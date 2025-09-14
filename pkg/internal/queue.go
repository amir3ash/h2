package internal

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// implements [waitableQ]
type ChanQ[T any] chan T

func (c ChanQ[T]) Pull() (item T, ok bool) {
	select {
	case item, ok = <-c:
		return item, ok
	default:
		var zero T
		return zero, false
	}
}
func (c ChanQ[T]) pullWait() (item T) {
	item = <-c
	return item
}
func (c ChanQ[T]) Push(item T) {
	c <- item
	return
}
func (c ChanQ[T]) len() int {
	return len(c)
}

type LinkedItem[T any] struct {
	mu   sync.Mutex
	list [20]T
	head uint8
	tail uint8
	next *LinkedItem[T]
}

func (li *LinkedItem[T]) push(item T) bool {
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

func (li *LinkedItem[T]) pull() (item T, ok bool) {
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
func (li *LinkedItem[T]) clear() {
	li.head = 0
	li.tail = 0
	li.next = nil
}

type LinkedQ[T any] struct {
	tailQ atomic.Pointer[LinkedItem[T]] // just for first; others are nil
	headQ atomic.Pointer[LinkedItem[T]]
	cond  *sync.Cond // just for first; others are nil
	// pool  sync.Pool
}

func NewLinkedQ[T any]() *LinkedQ[T] {
	li := LinkedItem[T]{}
	lq := &LinkedQ[T]{cond: sync.NewCond(&sync.Mutex{})}
	lq.headQ.Store(&li)
	lq.tailQ.Store(&li)
	// lq.pool = sync.Pool{New: func() any {
	// 	return &linkedItem[T]{}
	// }}
	return lq
}

func (lq *LinkedQ[T]) Pull() (item T, ok bool) {
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

func (lq *LinkedQ[T]) PullWait() (item T) {
	lq.cond.L.Lock()
	for {
		item, ok := lq.Pull()
		if !ok {
			lq.cond.Wait()
			continue
		}

		lq.cond.L.Unlock()
		return item
	}
}

func (lq *LinkedQ[T]) Push(item T) bool {
	// push item to last writeQ, if wg is not head of linked list
	for tailQ := lq.tailQ.Load(); ; tailQ = lq.tailQ.Load() {
		if tailQ.push(item) {
			lq.cond.Signal()
			return true
		}

		// q := lq.pool.Get().(*linkedItem[T])
		// q.clear() // TODO: race! when getting and putting from/to pool
		q := &LinkedItem[T]{}
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

func (lq *LinkedQ[T]) len() int {
	n := 0
	for item := lq.headQ.Load(); item != nil; item = item.next {
		n += int(item.tail - item.head)
	}
	return n
}

var byteBufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func NewBytesPipe() *BytesPipe {
	return &BytesPipe{q: NewLinkedQ[*bytes.Buffer]()}
}

type BytesPipe struct {
	q         *LinkedQ[*bytes.Buffer]
	closed    bool
	err       error
	readTimer *time.Timer
	unread    *bytes.Buffer
}

func (b *BytesPipe) ReadData() (data *bytes.Buffer, err error) {
	for {
		if err = b.checkErr(); err != nil {
			return nil, err
		}

		cond := b.q.cond

		data, ok := b.q.Pull()
		if ok {
			return data, nil
		}

		cond.Wait()
	}
}
func (b *BytesPipe) Read(p []byte) (n int, err error) {
	if err = b.checkErr(); err != nil {
		return 0, err
	}

	if b.unread != nil {
		return b.unread.Read(p)
	}

	buff, err := b.ReadData()
	if err != nil {
		return 0, err
	}

	n, err = buff.Read(p)
	if buff.Len() == 0 {
		b.ReadDone(buff)
		return
	}

	b.unread = buff
	return
}
func (b *BytesPipe) SetReadDeadline(t time.Time) error {
	if err := b.checkErr(); err != nil {
		return err
	}

	if t.IsZero() { // clear deadline
		if b.readTimer != nil {
			b.readTimer.Stop()
		}
		return nil
	}

	d := time.Until(t)
	if d < 0 {
		b.CloseWithErr(os.ErrDeadlineExceeded)
		return nil
	}

	if b.readTimer != nil {
		b.readTimer.Reset(d)
		return nil
	}

	b.readTimer = time.AfterFunc(d, func() {
		b.CloseWithErr(os.ErrDeadlineExceeded)
	})

	return nil
}
func (b *BytesPipe) Write(p []byte) (n int, err error) {
	if err = b.checkErr(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}

	buf := byteBufferPool.Get().(*bytes.Buffer)

	n, err = buf.Write(p)
	if err != nil {
		return n, err
	}

	if !b.q.Push(buf) {
		panic("internal: can not push")
	}

	return
}
func (b *BytesPipe) Close() {
	b.CloseWithErr(io.EOF)
}
func (b *BytesPipe) CloseWithErr(err error) {
	b.err = err
	b.closed = true

	if b.readTimer != nil {
		b.readTimer.Stop()
	}
	b.q.cond.Signal()
}
func (*BytesPipe) ReadDone(buff *bytes.Buffer) {
	byteBufferPool.Put(buff)
}

func (b *BytesPipe) checkErr() error {
	if b.closed {
		return b.err
	}
	return nil
}
