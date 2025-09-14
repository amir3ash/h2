package wire

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"
)

var preface = []byte(http2.ClientPreface)

type Server struct {
	l *listener
	// OnConnect func(*Connection)
	conf *conf
}

func NewServer(opts ...serverOpts) *Server {
	conf := &conf{}
	for _, f := range opts {
		f(conf)
	}
	return &Server{l: newListener(conf.useCustomTcp), conf: conf}
}

func (srv Server) ServeTLS(l net.Listener, certFile, keyFile string) error {
	config := tls.Config{NextProtos: []string{"h2"}}
	var err error
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsListener := tls.NewListener(l, &config)
	return srv.Serve(tlsListener)
}

func (srv Server) Serve(l net.Listener) error {
	var tempDelay time.Duration // how long to sleep on accept failure

	ctx := context.WithValue(context.TODO(), http.ServerContextKey, srv)

	for {
		rw, err := l.Accept()
		if err != nil {
			// if srv.shuttingDown() {
			// 	return http.ErrServerClosed
			// }
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				// srv.logf("http: Accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		// connCtx := ctx
		// if cc := srv.ConnContext; cc != nil {
		// 	connCtx = cc(connCtx, rw)
		// 	if connCtx == nil {
		// 		panic("ConnContext returned nil")
		// 	}
		// }
		ctx = context.WithValue(ctx, http.LocalAddrContextKey, rw.LocalAddr())

		go func(ctx context.Context) {
			var closeConn bool
			defer func() {
				if closeConn {
					rw.Close()
				}
			}()

			if tlsConn, ok := rw.(*tls.Conn); ok {
				tlsTO := 700 * time.Millisecond // tlsHandshakeTimeout()
				if tlsTO > 0 {
					dl := time.Now().Add(tlsTO)
					rw.SetReadDeadline(dl)
					rw.SetWriteDeadline(dl)
				}
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					// If the handshake failed due to the client not speaking
					// TLS, assume they're speaking plaintext HTTP and write a
					// 400 response on the TLS conn's underlying net.Conn.
					var reason string
					if re, ok := err.(tls.RecordHeaderError); ok && re.Conn != nil && tlsRecordHeaderLooksLikeHTTP(re.RecordHeader) {
						io.WriteString(re.Conn, "HTTP/1.0 400 Bad Request\r\n\r\nClient sent an HTTP request to an HTTPS server.\n")
						re.Conn.Close()
						reason = "client sent an HTTP request to an HTTPS server"
					} else {
						reason = err.Error()
					}
					log.Printf("http: TLS handshake error from %s: %v", rw.RemoteAddr(), reason)
					return
				}
				// Restore Conn-level deadlines.
				if tlsTO > 0 {
					rw.SetReadDeadline(time.Time{})
					rw.SetWriteDeadline(time.Time{})
				}
				tlsState := new(tls.ConnectionState)
				*tlsState = tlsConn.ConnectionState()
				if proto := tlsState.NegotiatedProtocol; proto == "h2" {

					h2Conn, err := srv.newConnection(tlsConn)
					if err != nil {
						return
					}

					if err := srv.l.addConn(h2Conn); err != nil {
						slog.Error("can not add new conn", "connType", "tls", "err", err)
						return
					}

					closeConn = false
				}
				return

			} else if tcp, ok := rw.(*net.TCPConn); ok && srv.conf.useCustomTcp {
				tc := &tcpConn{}
				if err := tc.init(tcp); err != nil {
					slog.Error("can not init tcpConn", "err", err.Error())
					return
				}

				h2Conn, err := srv.newConnection(tc)
				if err != nil {
					slog.Error("can not create new connection", "err", err.Error())
					return
				}

				if err := srv.l.addConn(h2Conn); err != nil {
					slog.Error("can not add new conn", "connType", "tcpConn", "err", err)
					return
				}

				closeConn = false
				return
			} else {
				h2Conn, err := srv.newConnection(rw)
				if err != nil {
					return
				}

				if err := srv.l.addConn(h2Conn); err != nil {
					slog.Error("can not add new conn", "connType", "tls", "err", err)
					return
				}

				closeConn = false
				return
			}

		}(ctx)
	}
}

func (srv *Server) newConnection(conn net.Conn) (*Connection, error) {
	var sigWrite func(*PQ)
	if _, ok := conn.(*tcpConn); ok && useWriteLoop {
		sigWrite = srv.l.netWriter.addRunnable
	} else {
		sigWrite = func(q *PQ) {
			if q.numQueued <= 1 {
				q.writeCond.Signal()
			}
		}
	}

	h2Conn := newConnection(context.TODO(), conn, sigWrite)
	h2Conn.isServer = true

	if h2Conn.isServer && h2Conn.state < PrefaceRecieved {
		if err := checkEqualPreface(h2Conn.wireConn); err != nil {
			return nil, err
		}
		h2Conn.state = PrefaceRecieved

		if err := h2Conn.negotiate(); err != nil {
			return nil, err
		}
	}

	h2Conn.whenPeerInitatedStream = srv.conf.whenPeerInitatedStream
	return h2Conn, nil
}

// tlsRecordHeaderLooksLikeHTTP reports whether a TLS record header
// looks like it might've been a misdirected plaintext HTTP request.
func tlsRecordHeaderLooksLikeHTTP(hdr [5]byte) bool {
	switch string(hdr[:]) {
	case "GET /", "HEAD ", "POST ", "PUT /", "OPTIO":
		return true
	}

	return false
}

var ErrPrefaceWrong = errors.New("wrong http2 preface")
var ErrPrefaceTimeout = errors.New("http2 preface timeout")

func checkEqualPreface(conn net.Conn) error {
	data := make([]byte, len(preface))
	conn.SetReadDeadline(time.Now().Add(900 * time.Millisecond))
	defer conn.SetReadDeadline(time.Time{})

	for {
		if _, err := conn.Read(data); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return ErrPrefaceTimeout
			}

			if netErr, ok := err.(net.Error); !ok || !netErr.Temporary() {
				return err
			}

			time.Sleep(time.Millisecond)
			continue
		}

		break
	}

	if !bytes.Equal(data, preface) {
		// slog.Debug("recievd preface", "data", string(data))
		return ErrPrefaceWrong
	}
	return nil
}

type conf struct {
	whenPeerInitatedStream func(*Stream)
	useCustomTcp           bool
}
type serverOpts func(conf *conf)

func WhenPeerInitatedStream(f func(*Stream)) serverOpts {
	return func(conf *conf) {
		conf.whenPeerInitatedStream = f
	}
}
func UseCustomTcp(val bool) serverOpts {
	return func(conf *conf) {
		conf.useCustomTcp = val
	}
}
