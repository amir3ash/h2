package wire

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// implements [waitableQ]
type chanQ[T any] chan T

func (c chanQ[T]) pull() (item T, ok bool) {
	select {
	case item, ok = <-c:
		return item, ok
	default:
		var zero T
		return zero, false
	}
}
func (c chanQ[T]) pullWait() (item T) {
	item = <-c
	return item
}
func (c chanQ[T]) push(item T) {
	c <- item
	return
}
func (c chanQ[T]) len() int {
	return len(c)
}

type linkedItem[T any] struct {
	mu   sync.Mutex
	list [20]T
	head uint8
	tail uint8
	next *linkedItem[T]
}

func (li *linkedItem[T]) push(item T) bool {
	li.mu.Lock()

	if li.tail < uint8(cap(li.list)) { // has empty slot
		li.list[li.tail] = item
		li.tail++
		li.mu.Unlock()

		// TODO: try benchmarking this with ring buffer
		return true
	}

	li.mu.Unlock()
	return false
}

func (li *linkedItem[T]) pull() (item T, ok bool) {
	li.mu.Lock()
	var zero T
	if li.head < li.tail { // not empty
		item = li.list[li.head]
		li.list[li.head] = zero
		li.head++

		li.mu.Unlock()
		return item, true
	}

	li.mu.Unlock()
	return item, false
}
func (li *linkedItem[T]) clear() {
	li.head = 0
	li.tail = 0
	li.next = nil
}

type linkedQ[T any] struct {
	tailQ atomic.Pointer[linkedItem[T]] // just for first; others are nil
	headQ atomic.Pointer[linkedItem[T]]
	cond  *sync.Cond // just for first; others are nil
	// pool  sync.Pool
}

func newLinkedQ[T any]() *linkedQ[T] {
	li := linkedItem[T]{}
	lq := &linkedQ[T]{cond: sync.NewCond(&sync.Mutex{})}
	lq.headQ.Store(&li)
	lq.tailQ.Store(&li)
	// lq.pool = sync.Pool{New: func() any {
	// 	return &linkedItem[T]{}
	// }}
	return lq
}

func (lq *linkedQ[T]) pull() (item T, ok bool) {
	for {
		head := lq.headQ.Load()
		if head == nil {
			return item, false
		}

		if item, ok := head.pull(); ok {
			return item, ok
		}

		head.mu.Lock()
		if head.next == nil {
			head.mu.Unlock()
			return item, false
		}

		if lq.headQ.CompareAndSwap(head, head.next) {
			// clear(head.list[:])
			// lq.pool.Put(head)
		}
		head.mu.Unlock()
	}
}

func (lq *linkedQ[T]) pullWait() (item T) {
	lq.cond.L.Lock()
	for {
		item, ok := lq.pull()
		if !ok {
			lq.cond.Wait()
			continue
		}

		lq.cond.L.Unlock()
		return item
	}
}

func (lq *linkedQ[T]) push(item T) bool {
	// push item to last writeQ, if wg is not head of linked list
	for tailQ := lq.tailQ.Load(); ; tailQ = lq.tailQ.Load() {
		if tailQ.push(item) {
			lq.cond.Signal()
			return true
		}

		// q := lq.pool.Get().(*linkedItem[T])
		// q.clear() // TODO: race! when getting and putting from/to pool
		q := &linkedItem[T]{}
		if !q.push(item) {
			panic("internal: empty linkedQ.pushed failed")
		}
		if lq.tailQ.CompareAndSwap(tailQ, q) {
			tailQ.mu.Lock()
			tailQ.next = q
			tailQ.mu.Unlock()

			lq.cond.Signal()
			return true
		} else {
			// clear(q.list[:2])
			// lq.pool.Put(q)
		}
	}
}

func (lq *linkedQ[T]) len() int {
	n := 0
	for item := lq.headQ.Load(); item != nil; item = item.next {
		n += int(item.tail - item.head)
	}
	return n
}

type writeQ struct {
	*linkedQ[*PQ]
}

type netWriter struct {
	pollers []*EpollWriter
	queues  []writeQ
}

func newNetWriter(conns *connMap, num int) netWriter {
	writePollers := make([]*EpollWriter, 0, num)
	queues := make([]writeQ, 0, num)
	for range cap(writePollers) {
		epoll, err := NewPollerWriter(128, 1*time.Millisecond, conns)
		if err != nil {
			panic(err)
		}
		writePollers = append(writePollers, epoll)
	}

	for range cap(queues) {
		wq := writeQ{newLinkedQ[*PQ]()}
		queues = append(queues, wq)
	}

	return netWriter{
		pollers: writePollers,
		queues:  queues,
	}
}

func (nw netWriter) write() {
	for i := range nw.queues {
		go func(idx int) {
			writeQ := nw.queues[idx]

			for {
				q := writeQ.pullWait()

				if err, hasMore := q.write(); err != nil {
					slog.Error("write failed", "err", err)
					if err == io.ErrShortWrite {
						writeQ.push(q)
					}
					if _, ok := err.(ErrTemporaryTimeout); ok {
						writeQ.push(q)
					}
					continue

				} else if hasMore {
					if !writeQ.push(q) {
						panic("")
					}
				}
			}
		}(i)

		go func(idx int) {

			conns := make([]*Connection, 128)
			poller := nw.pollers[idx]
			writeQ := nw.queues[idx]

			for {
				n, err := poller.Wait(conns)
				if err != nil {
					slog.Debug("write poller's wait failed")
					continue
				}

				for i := range n {
					h2Conn := conns[i]
					slog.Info("netWrter pushing que", "id", h2Conn.id)
					if !writeQ.push(h2Conn.que) {
						panic("")
					}
				}
			}
		}(i)
	}
}

func (nw netWriter) addRunnable(q *PQ) {
	conn := q.wire.(*tcpConn)
	if conn.writeQ.linkedQ == nil {
		fd := SocketFD(conn)
		if fd == 0 {
			slog.Error("socket fd is zero")
			return
			panic("internal: socket fd is zero")
		}

		wq := nw.queues[fd%len(nw.queues)]
		conn.writeQ = wq
		q.wire = conn
		if !wq.push(q) {
			panic("internal: can not push to writeQ")
		}
	} else {
		if !conn.writeQ.push(q) {
			panic("internal: can not push to writeQ")
		}
	}
}

// EPoll is a poll based connection implementation.
type EpollWriter struct {
	fd int

	connBufferSize int
	lock           *sync.RWMutex
	h2Conns        map[int]*Connection
	conns          map[int]net.Conn

	timeoutMsec int
	conmap      *connMap
}

// NewPoller creates a new epoll poller.
func NewPollerWriter(connBufferSize int, pollTimeout time.Duration, conmap *connMap) (*EpollWriter, error) {
	return newPollerWithBuffer(connBufferSize, pollTimeout, conmap)
}

// newPollerWithBuffer creates a new epoll poller with a buffer.
func newPollerWithBuffer(count int, pollTimeout time.Duration, conmap *connMap) (*EpollWriter, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &EpollWriter{
		fd:             fd,
		connBufferSize: count,
		lock:           &sync.RWMutex{},
		h2Conns:        make(map[int]*Connection),
		conns:          make(map[int]net.Conn),
		conmap:         conmap,
		timeoutMsec:    int(pollTimeout.Milliseconds()),
	}, nil
}

// Close closes the poller. If closeConns is true, it will close all the connections.
func (e *EpollWriter) Close(closeConns bool) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if closeConns {
		for _, conn := range e.h2Conns {
			conn.Close()
		}
	}

	e.h2Conns = nil
	e.conmap = nil
	e.conns = nil
	return unix.Close(e.fd)
}

// Add adds a connection to the poller.
func (e *EpollWriter) Add(fd int, netConn net.Conn, h2Conn *Connection) error {
	if e := syscall.SetNonblock(int(fd), true); e != nil {
		return errors.New("udev: unix.SetNonblock failed")
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.POLLOUT | unix.POLLHUP | unix.EPOLLET, Fd: int32(fd)})
	if err != nil {
		return err
	}
	e.h2Conns[fd] = h2Conn
	e.conns[fd] = netConn
	return nil
}

// Remove removes a connection from the poller.
func (e *EpollWriter) Remove(conn net.Conn) error {
	fd := SocketFD(conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.h2Conns, fd)
	delete(e.conns, fd)

	return nil
}

// Wait waits for at most count events and returns the connections.
func (e *EpollWriter) Wait(h2Conns []*Connection) (n int, error error) {
	events := make([]unix.EpollEvent, 128)
retry:
	n, err := unix.EpollWait(e.fd, events, e.timeoutMsec)
	if err != nil {
		if err == unix.EINTR {
			goto retry
		}
		return 0, err
	}

	deleteds := make([]int32, 0, 128)

	e.lock.RLock()
	for i := range n {
		
		h2Conn := e.h2Conns[int(events[i].Fd)]
		if h2Conn != nil {
			h2Conns[i] = h2Conn
			if events[i].Events&unix.EPOLLHUP != 0 {
				delete(e.h2Conns, int(events[i].Fd))
				deleteds = append(deleteds, events[i].Fd)
			}
		}
	}
	e.lock.RUnlock()

	if len(deleteds) > 0 {
		e.conmap.mu.Lock()
		e.lock.Lock()
		for _, fd := range deleteds {
			delete(e.conmap.conns, e.conns[int(fd)])
			delete(e.conns, int(fd))
		}
		e.lock.Unlock()
		e.conmap.mu.Unlock()
	}
	return n, nil
}

func SocketFD(conn net.Conn) int {
	if con, ok := conn.(syscall.Conn); ok {
		raw, err := con.SyscallConn()
		if err != nil {
			return 0
		}
		sfd := 0
		raw.Control(func(fd uintptr) { // nolint: errcheck
			sfd = int(fd)
		})
		return sfd
	}
	return 0
}
