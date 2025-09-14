package http8

// most of codes copied from net/http2.
import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/amir3ash/h2/pkg/internal"
	h2 "github.com/amir3ash/h2/pkg/wire"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
)

type Server struct {
	*h2.Server
	handler http.Handler
	// Http2 http.HTTP2Config
}
type ServerConf struct {
	UseCustomTcp bool
	Handler      http.Handler
}

func NewServer(conf *ServerConf) *Server {
	hs := Server{handler: http.DefaultServeMux}
	if conf.Handler != nil {
		hs.handler = conf.Handler
	}

	srv := h2.NewServer(
		h2.WhenPeerInitatedStream(hs.whenPeerInitatedStream),
		h2.UseCustomTcp(conf.UseCustomTcp),
	)

	hs.Server = srv
	return &hs
}

func (hs *Server) whenPeerInitatedStream(s *h2.Stream) {
	rw, req, err := hs.newWriterAndRequest(s)
	if err != nil {
		slog.Error("cant create req an resp", "err", err)
		return
	}
	// st.reqTrailer = req.Trailer
	// if st.reqTrailer != nil {
	// 	st.trailer = make(http.Header)
	// }
	body := req.Body.(*requestBody).pipe // may be nil

	handler := hs.handler.ServeHTTP
	// if f.Truncated {
	// 	// Their header list was too long. Send a 431 error.
	// 	handler = handleHeaderListTooLong
	// } else
	if err := checkValidHTTP2RequestHeaders(req.Header); err != nil {
		handler = new400Handler(err)
	}

	// The net/http package sets the read deadline from the
	// http.Server.ReadTimeout during the TLS handshake, but then
	// passes the connection off to us with the deadline already
	// set. Disarm it here after the request headers are read,
	// similar to how the http1 server works. Here it's
	// technically more like the http1 Server's ReadHeaderTimeout
	// (in Go 1.8), though. That's a more sane option anyway.
	// if sc.hs.ReadTimeout > 0 {
	// 	sc.conn.SetReadDeadline(time.Time{})
	// 	st.readDeadline = sc.srv.afterFunc(sc.hs.ReadTimeout, st.onReadTimeout)
	// }

	dataLen := int64(0)
	s.OnDataHook = func(data []byte, stream *h2.Stream) error {
		if req.ContentLength != -1 && dataLen+int64(len(data)) > req.ContentLength {
			stream.Close(http2.ErrCodeProtocol)
			body.CloseWithErr(fmt.Errorf("sender sent invalid content-length header"))
			return nil
		}

		dataLen += int64(len(data))
		_, err := body.Write(data)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if err := stream.Close(http2.ErrCodeCancel); err != nil {
					slog.Error("can not close stream", "err", err)
				}
			}
		}
		return nil
	}

	s.OnPearEndHook = func(stream *h2.Stream) error {
		req.Body.Close()
		return nil
	}

	go func() {
		didPanic := true
		defer func() {
			// rw.rws.stream.cancelCtx()
			if req.MultipartForm != nil {
				req.MultipartForm.RemoveAll()
			}
			if didPanic {
				e := recover()
				s.Close(http2.ErrCodeInternal)
				// Same as net/http:
				if e != nil && e != http.ErrAbortHandler {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					// sc.lof("http2: panic serving %v: %v\n%s", sc.conn.RemoteAddr(), e, buf)
					fmt.Printf("http2: panic serving: %v\n%s", e, buf)

				}
				return
			}
			rw.handlerDone()
		}()

		handler(rw, req)
		didPanic = false
	}()

}

type serverRequestParam struct {
	Method, Scheme, Authority, Path, Protocol string
	Header                                    http.Header
}

func (sc *Server) newWriterAndRequest(st *h2.Stream) (responseWriterNotifier, *http.Request, error) {
	rp := serverRequestParam{
		Method:    PseudoValue(st, "method"),
		Scheme:    PseudoValue(st, "scheme"),
		Authority: PseudoValue(st, "authority"),
		Path:      PseudoValue(st, "path"),
		Protocol:  PseudoValue(st, "protocol"),
	}

	// extended connect is disabled, so we should not see :protocol
	// if disableExtendedConnectProtocol && rp.Protocol != "" {
	// 	return nil, nil, sc.countError("bad_connect", streamError(st.ID(), ErrCodeProtocol))
	// }

	isConnect := rp.Method == "CONNECT"
	if isConnect {
		if rp.Protocol == "" && (rp.Path != "" || rp.Scheme != "" || rp.Authority == "") {
			return nil, nil, http2.StreamError{StreamID: st.ID(), Code: http2.ErrCodeProtocol}
		}
	} else if rp.Method == "" || rp.Path == "" || (rp.Scheme != "https" && rp.Scheme != "http") {
		// See 8.1.2.6 Malformed Requests and Responses:
		//
		// Malformed requests or responses that are detected
		// MUST be treated as a stream error (Section 5.4.2)
		// of type PROTOCOL_ERROR."
		//
		// 8.1.2.3 Request Pseudo-Header Fields
		// "All HTTP/2 requests MUST include exactly one valid
		// value for the :method, :scheme, and :path
		// pseudo-header fields"
		return nil, nil, http2.StreamError{StreamID: st.ID(), Code: http2.ErrCodeProtocol}
	}

	header := make(http.Header)
	rp.Header = header
	for _, hf := range st.Headers() {
		header.Add(http.CanonicalHeaderKey(hf.Name), hf.Value)
	}
	if rp.Authority == "" {
		rp.Authority = header.Get("Host")
	}
	if rp.Protocol != "" {
		header.Set(":protocol", rp.Protocol)
	}

	rw, req, err := sc.newWriterAndRequestNoBody(st, rp)
	if err != nil {
		return nil, nil, err
	}
	bodyOpen := st.State() < h2.HalfClosedRemoteState
	if bodyOpen {
		if vv, ok := rp.Header["Content-Length"]; ok {
			if cl, err := strconv.ParseUint(vv[0], 10, 63); err == nil {
				req.ContentLength = int64(cl)
			} else {
				req.ContentLength = 0
			}
		} else {
			req.ContentLength = -1
		}

		req.Body.(*requestBody).pipe = internal.NewBytesPipe()
	}
	return rw, req, nil
}

func (sc *Server) newWriterAndRequestNoBody(st *h2.Stream, rp serverRequestParam) (responseWriterNotifier, *http.Request, error) {
	var tlsState *tls.ConnectionState // nil if not scheme https
	if rp.Scheme == "https" {
		tlsState = st.Context().Value(h2.TlsConnectionStateKey{}).(*tls.ConnectionState)
	}

	res := newServerRequest(rp)
	if res.InvalidReason != "" {
		return nil, nil, http2.StreamError{StreamID: st.ID(), Code: http2.ErrCodeProtocol}
	}

	body := &requestBody{
		conn:          st.Connection(),
		stream:        st,
		needsContinue: res.NeedsContinue,
	}
	req := (&http.Request{
		Method:     rp.Method,
		URL:        res.URL,
		RemoteAddr: st.Connection().RemoteAddr().String(),
		Header:     rp.Header,
		RequestURI: res.RequestURI,
		Proto:      "HTTP/2.0",
		ProtoMajor: 2,
		ProtoMinor: 0,
		TLS:        tlsState,
		Host:       rp.Authority,
		Body:       body,
		Trailer:    res.Trailer,
	}).WithContext(st.Context())
	rw := sc.newRes(st, req)
	return rw, req, nil
}

// func (sc *HttpServer) newResponseWriter(st *h2.Stream, req *http.Request) *responseWriter {
// 	rws := responseWriterStatePool.Get().(*responseWriterState)
// 	bwSave := rws.bw
// 	*rws = responseWriterState{} // zero all the fields
// 	rws.conn = st.Connection()
// 	rws.bw = bwSave
// 	rws.bw.Reset(chunkWriter{rws})
// 	rws.stream = st
// 	rws.req = req
// 	return &responseWriter{rws: rws}
// }

// serverRequestResult is the result of NewServerRequest.
type serverRequestResult struct {
	// Various http.Request fields.
	URL        *url.URL
	RequestURI string
	Trailer    map[string][]string

	NeedsContinue bool // client provided an "Expect: 100-continue" header

	// If the request should be rejected, this is a short string suitable for passing
	// to the http2 package's CountError function.
	// It might be a bit odd to return errors this way rather than returing an error,
	// but this ensures we don't forget to include a CountError reason.
	InvalidReason string
}

func newServerRequest(rp serverRequestParam) serverRequestResult {
	needsContinue := httpguts.HeaderValuesContainsToken(rp.Header["Expect"], "100-continue")
	if needsContinue {
		delete(rp.Header, "Expect")
	}
	// Merge Cookie headers into one "; "-delimited value.
	if cookies := rp.Header["Cookie"]; len(cookies) > 1 {
		rp.Header["Cookie"] = []string{strings.Join(cookies, "; ")}
	}

	// Setup Trailers
	var trailer map[string][]string
	for _, v := range rp.Header["Trailer"] {
		for _, key := range strings.Split(v, ",") {
			key = textproto.CanonicalMIMEHeaderKey(textproto.TrimString(key))
			switch key {
			case "Transfer-Encoding", "Trailer", "Content-Length":
				// Bogus. (copy of http1 rules)
				// Ignore.
			default:
				if trailer == nil {
					trailer = make(map[string][]string)
				}
				trailer[key] = nil
			}
		}
	}
	delete(rp.Header, "Trailer")

	// "':authority' MUST NOT include the deprecated userinfo subcomponent
	// for "http" or "https" schemed URIs."
	// https://www.rfc-editor.org/rfc/rfc9113.html#section-8.3.1-2.3.8
	if strings.IndexByte(rp.Authority, '@') != -1 && (rp.Scheme == "http" || rp.Scheme == "https") {
		return serverRequestResult{
			InvalidReason: "userinfo_in_authority",
		}
	}

	var url_ *url.URL
	var requestURI string
	if rp.Method == "CONNECT" && rp.Protocol == "" {
		url_ = &url.URL{Host: rp.Authority}
		requestURI = rp.Authority // mimic HTTP/1 server behavior
	} else {
		var err error
		url_, err = url.ParseRequestURI(rp.Path)
		if err != nil {
			return serverRequestResult{
				InvalidReason: "bad_path",
			}
		}
		requestURI = rp.Path
	}

	return serverRequestResult{
		URL:           url_,
		NeedsContinue: needsContinue,
		RequestURI:    requestURI,
		Trailer:       trailer,
	}
}

// PseudoValue returns the given pseudo header field's value.
// The provided pseudo field should not contain the leading colon.
func PseudoValue(s *h2.Stream, pseudo string) string {
	for _, hf := range s.Headers() {
		if !hf.IsPseudo() {
			return ""
		}
		if hf.Name[1:] == pseudo {
			return hf.Value
		}
	}
	return ""
}

// From http://httpwg.org/specs/rfc7540.html#rfc.section.8.1.2.2
var connHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Connection",
	"Transfer-Encoding",
	"Upgrade",
}

// checkValidHTTP2RequestHeaders checks whether h is a valid HTTP/2 request,
// per RFC 7540 Section 8.1.2.2.
// The returned error is reported to users.
func checkValidHTTP2RequestHeaders(h http.Header) error {
	for _, k := range connHeaders {
		if _, ok := h[k]; ok {
			return fmt.Errorf("request header %q is not valid in HTTP/2", k)
		}
	}
	te := h["Te"]
	if len(te) > 0 && (len(te) > 1 || (te[0] != "trailers" && te[0] != "")) {
		return errors.New(`request header "TE" may only be "trailers" in HTTP/2`)
	}
	return nil
}

// bodyAllowedForStatus reports whether a given response status code
// permits a body. See RFC 7230, section 3.3.
func bodyAllowedForStatus(status int) bool {
	switch {
	case status >= 100 && status <= 199:
		return false
	case status == 204:
		return false
	case status == 304:
		return false
	}
	return true
}

func new400Handler(err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

type responseWriterNotifier interface {
	http.ResponseWriter

	handlerDone()
}
