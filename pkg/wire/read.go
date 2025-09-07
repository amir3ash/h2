package wire

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const useWriteLoop = false

// shared by [netWriter], [Server], [listener]
type connMap struct {
	mu    sync.RWMutex
	conns map[net.Conn]*Connection
}

func (cm *connMap) getH2Conn(conn net.Conn) (h2Conn *Connection, ok bool) {
	switch cv := conn.(type) {
	case *tls.Conn:
		conn = cv.NetConn()
	}
	if _, ok := conn.(*tcpConn); !ok {
		panic(fmt.Sprintf("internal: unknown conn %T", conn))
	}

	cm.mu.RLock()
	// fd := SocketFD(conn)
	// if fd == 0 {
	// 	return nil, false
	// }
	h2Conn, ok = cm.conns[conn]

	cm.mu.RUnlock()
	return
}

type listener struct {
	epollReader []*EpollReader
	conns       *connMap
	done        chan struct{}
	netListener net.Listener

	netWriter netWriter

	connectCh chan *Connection

	closeCh chan struct {
		err error
		h2  *Connection
	}

	useCustomTcp bool
}

func newListener(useCustomTcp bool) *listener {
	readPollers := make([]*EpollReader, 0, 2)
	if useCustomTcp {
		for range cap(readPollers) {
			epoll, err := NewPollerReader(128, 1*time.Millisecond)
			if err != nil {
				useCustomTcp = false
				slog.Error("epoll not supported", "err", err)
			}
			readPollers = append(readPollers, epoll)
		}
	}

	connMap := &connMap{conns: map[net.Conn]*Connection{}}

	l := &listener{
		epollReader: readPollers,
		conns:       connMap,

		useCustomTcp: useCustomTcp,

		connectCh: make(chan *Connection, 1),
		closeCh: make(chan struct {
			err error
			h2  *Connection
		}, 1),
	}

	if useCustomTcp {
		if useWriteLoop {
			l.netWriter = newNetWriter(connMap, 2)
			go l.netWriter.write()
		}
		go l.read()
	}

	go l.handleCloseConnect()
	return l
}

func (l *listener) startNomalConnection(h2 *Connection) {
	h2.wireConn.SetReadDeadline(time.Time{})

	go h2.que.startWriteLoop()

	for {
		if h2.state == Terminated {
			return
		}

		err := h2.ReadProcessFrame()
		if err != nil {
			slog.Error("ReadProcframes failed",
				slog.Int64("id", int64(h2.id)),
				slog.String("state", h2.state.String()),
				slog.String("err", err.Error()),
			)

			if ShouldCloseGracefully(err) {
				if h2.closeDone.Load() == nil {
					go h2.CloseWithError(err)
				}
			} else {
				err := h2.Terminate(err)
				if err != nil {
					slog.Error("closing conn failed", "err", err)
				}
			}
		}
	}
}

func (l *listener) handleCloseConnect() {
	// connect and close should be handled in one goroutine
	// to prevent race condition when adding/removing fd from epoll.

	for {
		select {
		case h2Conn := <-l.connectCh:
			l.handleConnect(h2Conn.wireConn.(*tcpConn), h2Conn)

		case p := <-l.closeCh:
			l.handleClose(p.h2, p.err)
		}
	}
}
func (l *listener) addConn(h2Conn *Connection) error {
	conn := h2Conn.wireConn

	if _, ok := conn.(*tcpConn); ok && l.useCustomTcp {
		l.connectCh <- h2Conn

	} else {
		go l.startNomalConnection(h2Conn)
	}

	return nil
}

func (l *listener) handleConnect(conn *tcpConn, h2 *Connection) {
	go h2.que.startWriteLoop()

	fd := SocketFD(conn)
	// slog.Info("starting to add fd", "fd", fd)

	l.conns.mu.Lock()
	l.conns.conns[conn] = h2
	l.conns.mu.Unlock()

	// idx := fd % len(l.netWriter.pollers)
	// if err := l.netWriter.pollers[idx].Add(fd, conn, h2); err != nil {
	// 	panic(err)
	// }

	// register epoll reader
	idx := fd % len(l.epollReader)
	if err := l.epollReader[idx].Add(h2); err != nil {
		slog.Error("can not add to epoll", "err", err)
	}

	h2.whenClosed = func(h2Conn *Connection) {
		l.handleClose(h2Conn, h2Conn.closeErr)
	}
}

func (l *listener) handleClose(h2 *Connection, closeErr error) {
	conn := h2.wireConn

	fd := l.epollReader[0].getFD(h2)
	if fd == 0 {
		done := h2.closeDone.Load()
		if done == nil {
			panic("zero fd but close not called")
		}

		select {
		case <-*done:
			// connection closed
		default:
			panic("zero fd but close not called")
		}

		return
	}
	// if err := l.epollReader[fd%len(l.epollReader)].Remove(h2); err != nil {
	// 	slog.Error("can not remove fd from read epoll", "fd", fd, "err", err, "netConn", conn)
	// }

	if useWriteLoop {
		if err := l.netWriter.pollers[fd%len(l.netWriter.pollers)].Remove(conn); err != nil {
			slog.Error("can not remove fd from netWriter's write epoll", "fd", fd, "err", err, "netConn", conn)
		}
	}
	l.conns.mu.Lock()
	delete(l.conns.conns, conn)
	l.conns.mu.Unlock()

	if h2.closeDone.Load() != nil {
		return // close already called
	}

	if ShouldCloseGracefully(closeErr) {
		go h2.CloseWithError(closeErr)
	} else {
		err := h2.Terminate(closeErr)
		if err != nil {
			slog.Error("closing conn failed", "err", err)
		}
	}
}

func (l *listener) read() {
	for i := range l.epollReader {
		go func(idx int) {
			conns := make([]*Connection, 128)

			for {
				select {
				case <-l.done:
					return
				default:
				}

				n, err := l.epollReader[idx].Wait(conns)
				if err != nil {
					slog.Error("can not wait reader poller")
				}

				for i := range n {
					h2Conn := conns[i]

					err = h2Conn.ReadProcessFrame()
					if err != nil {
						if _, ok := err.(ErrTemporaryTimeout); ok {
							slog.Warn("Temporary timeout readProcess frame", slog.Int64("id", int64(h2Conn.id)))
							continue
						}

						slog.Error("ReadProcessFrame failed", slog.Int64("id", int64(h2Conn.id)), slog.String("err", err.Error()))

						if !errors.Is(err, net.ErrClosed) {

							l.closeCh <- struct {
								err error
								h2  *Connection
							}{
								err: err,
								h2:  h2Conn,
							}

							continue
						}
					}
				}
			}
		}(i)
	}
}

// EPoll is a poll based connection implementation.
type EpollReader struct {
	fd int

	connBufferSize int
	lock           *sync.RWMutex
	conns          map[int32]*Connection
	connbuf        []*Connection

	timeoutMsec int
	events      []unix.EpollEvent
}

// NewPoller creates a new epoll poller.
func NewPollerReader(connBufferSize int, pollTimeout time.Duration) (*EpollReader, error) {
	return newPollerReaderWithBuffer(connBufferSize, pollTimeout)
}

// newPollerWithBuffer creates a new epoll poller with a buffer.
func newPollerReaderWithBuffer(count int, pollTimeout time.Duration) (*EpollReader, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &EpollReader{
		fd:             fd,
		connBufferSize: count,
		lock:           &sync.RWMutex{},
		conns:          make(map[int32]*Connection),
		connbuf:        make([]*Connection, count),
		timeoutMsec:    int(pollTimeout.Milliseconds()),
	}, nil
}

// Close closes the poller. If closeConns is true, it will close all the connections.
func (e *EpollReader) Close(closeConns bool) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if closeConns {
		for _, conn := range e.conns {
			conn.Close()
		}
	}

	e.conns = nil
	e.connbuf = e.connbuf[:0]

	return unix.Close(e.fd)
}

// Add adds a connection to the poller.
func (e *EpollReader) Add(conn *Connection) error {
	fd := e.getFD(conn)
	if e := syscall.SetNonblock(int(fd), true); e != nil {
		return errors.New("udev: unix.SetNonblock failed")
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLHUP | unix.EPOLLRDHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}
	e.conns[int32(fd)] = conn
	return nil
}

// Remove removes a connection from the poller.
func (e *EpollReader) Remove(conn *Connection) error {
	fd := e.getFD(conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.conns, int32(fd))

	return nil
}

// Wait waits for at most count events and returns the connections.
func (e *EpollReader) Wait(conns []*Connection) (n int, err error) {
	if len(e.events) != len(conns) {
		e.events = make([]unix.EpollEvent, len(conns))
	}

retry:
	n, err = unix.EpollWait(e.fd, e.events, e.timeoutMsec)
	if err != nil {
		if err == unix.EINTR {
			goto retry
		}
		return 0, err
	}

	e.lock.RLock()
	for i := 0; i < n; i++ {
		ev := e.events[i]
		if ev.Events != unix.EPOLLIN {
			slog.Warn("READER_POLL", slog.Int("fd", int(ev.Fd)), slog.Int64("events", int64(ev.Events)))
		}
		conn := e.conns[ev.Fd]
		if conn != nil {
			conns[i] = conn
			if ev.Events&unix.EPOLLRDHUP != 0 {
				delete(e.conns, ev.Fd)
				conn.wireConn.(*tcpConn).closed = true
			}
		} else {
			slog.Error("reader epoll wait can not get h2conn", slog.Int("fd", int(ev.Fd)))
		}
	}
	e.lock.RUnlock()

	return n, nil
}

func (*EpollReader) getFD(h2 *Connection) int {
	conn := h2.wireConn

	return SocketFD(conn)
}
