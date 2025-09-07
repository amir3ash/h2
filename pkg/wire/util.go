package wire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type findOrDie[T comparable] struct {
	lookFor map[T]chan struct{}
}

// Inserts arg to waiting list. Returns true if someone looked for it before.
func (fd *findOrDie[T]) insert(arg T) (hadWaiter bool) {
	if ch, ok := fd.lookFor[arg]; ok {
		close(ch)
		return true
	}
	return false
}

// Waits for arg to be inserted or deadline exceeded.
func (fd *findOrDie[T]) exist(ctx context.Context, arg T) error {
	fd.lookFor[arg] = make(chan struct{})

	// wait until inserted
	select {
	case <-ctx.Done():
		delete(fd.lookFor, arg)
		return ctx.Err()

	case <-fd.lookFor[arg]:
		delete(fd.lookFor, arg)
		return nil
	}
}

type framerPool struct {
	pool sync.Pool
}

func (fp *framerPool) GetWith(writeBuff *bytes.Buffer) *Framer {
	if fp.pool.New == nil {
		fp.pool.New = func() any {
			return NewFramer()
		}
	}

	f := fp.pool.Get().(*Framer)
	f.SetWriter(writeBuff)
	return f
}
func (fp *framerPool) Put(f *Framer) {
	f.SetWriter(nil)
	fp.pool.Put(f)
}

type bufferPool struct {
	pool sync.Pool
}

func (bp *bufferPool) Get() *bytes.Buffer {
	if bp.pool.New == nil {
		bp.pool.New = func() any {
			return new(bytes.Buffer)
		}
	}

	b := bp.pool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}
func (bp *bufferPool) Put(b *bytes.Buffer) {
	bp.pool.Put(b)
}

var _lastTime fastTime
var _started time.Time

// It returns last checked time.
//
// The time will be updated every 200ms. So it is not accurate.
func now_bufferd() fastTime {
	return _lastTime
}

func init() {
	go func() {
		for t := range time.Tick(200 * time.Millisecond) {
			_lastTime = fastTimeWith(t)
		}
	}()
	_started = time.Now()
}

var _epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
var _fastTime_to_unix = _epoch.UnixMilli()

type fastTime int64 // time in miliseconds sice 2020
func (t fastTime) Time() time.Time {
	return time.UnixMilli(int64(t) + _fastTime_to_unix)
}
func fastTimeWith(t time.Time) fastTime {
	return fastTime(t.UnixMilli() - _fastTime_to_unix)
}
func (t fastTime) before(t2 fastTime) bool {
	return t2 > t
}
func (t fastTime) add(d time.Duration) fastTime {
	t += fastTime(d.Milliseconds())
	return t
}

// A TCP implementation which returns [ErrTemporaryTimeout] in case of syscall.EAGAIN.
type tcpConn struct {
	conn   net.TCPConn
	fd     int
	closed bool
	// rDead, wDead uint64
	rLock, wLock atomic.Bool
	wDead, rDead time.Duration

	writeQ writeQ
}

// SyscallConn implements syscall.Conn.
func (t *tcpConn) SyscallConn() (syscall.RawConn, error) {
	return t.conn.SyscallConn()
}

// Close implements net.Conn.
func (t *tcpConn) Close() error {
	err := t.conn.Close()
	t.closed = true
	return err
}

// LocalAddr implements net.Conn.
func (t *tcpConn) LocalAddr() net.Addr {
	return t.conn.LocalAddr()
}

func (t *tcpConn) Read(b []byte) (n int, err error) {
	if !t.rLock.CompareAndSwap(false, true) {
		panic("concurent read")
	}
	defer t.rLock.Store(false)

	if t.closed {
		return t.conn.Read(b)
	}

	if err = t.checkReadDeadline(); err != nil {
		return 0, err
	}
	// slog.Debug("tcpConn Read")
	n, err = ignoringEINTRIO(syscall.Read, t.fd, b)
	if err != nil {
		n = 0
		if err == syscall.EAGAIN {
			// wait util reader poll notify
			// slog.Debug("err read again")
			return 0, ErrTemporaryTimeout{}
		}
	}

	return n, err
}

type ErrTemporaryTimeout struct{}

func (ErrTemporaryTimeout) Error() string {
	return "Temporary Timout"
}
func (ErrTemporaryTimeout) Timeout() bool   { return true }
func (ErrTemporaryTimeout) Temporary() bool { return true }

// RemoteAddr implements net.Conn.
func (tc *tcpConn) RemoteAddr() net.Addr {
	return tc.conn.RemoteAddr()
}

// SetDeadline implements net.Conn.
func (tc *tcpConn) SetDeadline(t time.Time) error {
	if err := tc.SetWriteDeadline(t); err != nil {
		return err
	}
	return tc.SetReadDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (tc *tcpConn) SetReadDeadline(t time.Time) error {
	if tc.closed {
		return tc.conn.SetReadDeadline(t)
	}

	tc.rDead = time.Since(_started)
	return nil
}

// SetWriteDeadline implements net.Conn.
func (tc *tcpConn) SetWriteDeadline(t time.Time) error {
	if !useWriteLoop || tc.closed {
		return tc.conn.SetWriteDeadline(t)
	}

	tc.wDead = time.Since(_started)
	return nil
}

const maxRW = 1 << 30

// Writes some or all of bytes. returns [ErrTemporaryTimeout] in case of partial write.
func (t *tcpConn) Write(b []byte) (n int, err error) {
	if !useWriteLoop {
		return t.conn.Write(b)
	}

	if !t.wLock.CompareAndSwap(false, true) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		panic(fmt.Sprintf("concurent write:\n%s", buf[:n]))
	}
	defer t.wLock.Store(false)

	if t.closed {
		return t.conn.Write(b)
	}

	if err = t.checkWriteDeadline(); err != nil {
		return 0, err
	}

	var nn int
	for {
		max := len(b)
		if max-nn > maxRW {
			max = nn + maxRW
		}
		n, err := ignoringEINTRIO(syscall.Write, t.fd, b[nn:max])
		if n > 0 {
			if n > max-nn {
				// This can reportedly happen when using
				// some VPN software. Issue #61060.
				// If we don't check this we will panic
				// with slice bounds out of range.
				// Use a more informative panic.
				panic("invalid return from write: got " + strconv.Itoa(n) + " from a write of " + strconv.Itoa(max-nn))
			}
			nn += n
		}
		if nn == len(b) {
			return nn, err
		}
		if err == syscall.EAGAIN {
			// if err = fd.pd.waitWrite(fd.isFile); err == nil {
			// 	continue
			// }
			return nn, ErrTemporaryTimeout{}
		}
		if err != nil {
			return nn, err
		}
		if n == 0 {
			return nn, io.ErrUnexpectedEOF
		}
	}
}

func (t *tcpConn) checkReadDeadline() error {
	if t.rDead == 0 {
		return nil
	}

	n := time.Since(_started)
	if t.rDead > n {
		return os.ErrDeadlineExceeded
	}
	return nil
}
func (t *tcpConn) checkWriteDeadline() error {
	if t.rDead == 0 {
		return nil
	}

	n := time.Since(_started)
	if t.wDead > n {
		return os.ErrDeadlineExceeded
	}
	return nil
}
func (t *tcpConn) init(tcp *net.TCPConn) error {
	t.conn = *tcp
	raw, err := tcp.SyscallConn()
	if err != nil {
		return  err
	}

	err = raw.Control(func(fd uintptr) {
		t.fd = int(fd)
	})
	if err != nil {
		return err
	}
	return nil
}
func (t *tcpConn) String() string {
	return fmt.Sprintf("*tcpConn{%p}", t)
}

var _ net.Conn = &tcpConn{}
var _ syscall.Conn = &tcpConn{}

func ignoringEINTRIO(fn func(fd int, p []byte) (n int, err error), fd int, p []byte) (n int, err error) {
	for i := 0; i < 10; i++ {
		n, err = fn(fd, p)
		if err != syscall.EINTR {
			return n, err
		}
	}
	return
}

type nopLocker struct{}

func (nopLocker) Lock()   {}
func (nopLocker) Unlock() {}
