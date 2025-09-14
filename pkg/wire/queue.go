package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type waitableQ[T any] interface {
	push(T)
	pull() (T, bool)
	// cap() int
	// len() int
}
type byteframe struct {
	stream  *Stream
	control bool
	data    *bytes.Buffer
	// priority  uint8
	// dependsOn uint32
	// exclusive bool

	forceFlush bool
}
type queueHeaderFrame struct {
	streamID  uint32
	headers   []hpack.HeaderField
	endStream bool
	priority  http2.PriorityParam
	stream    *Stream
}

type flushingFrame struct {
	*bytes.Buffer
	forceFlush bool
}

type priorityNode struct {
	frames    waitableQ[flushingFrame]
	stream    *Stream
	dependsOn *priorityNode
	exclusive bool
	weight    uint8 // 0-255;
	rank      uint8 // 0-15; 0 is more important
}

func (pn *priorityNode) calculateRank() {
	pn.rank = 3
}

type PQ struct {
	h2Conn       *Connection
	wire         net.Conn
	w            *bufio.Writer
	hpackEncoder *hpack.Encoder
	hpackBuff    bytes.Buffer
	idx          uint8
	idxCounter   uint8

	controlQ waitableQ[*bytes.Buffer]
	headerQ  *expandableRing[queueHeaderFrame]

	remaining     *bytes.Buffer
	exclusiveNode *priorityNode
	incrementIdx  bool

	nodesMu sync.Mutex
	nodes   map[uint32]*priorityNode
	dataQ   [4]*linkedQ[*priorityNode]

	addToRunnables func(*PQ)

	writeCond sync.Cond
	numQueued int32
	mu        sync.Mutex // guard against pushing/pulling frames concurrently
}

func (q *PQ) startWriteLoop() {
	var data *bytes.Buffer
	for {
		if q.h2Conn.state == Terminated {
			return
		}

		q.writeCond.L.Lock()

		data, _ = q.findNext()
		// wait until connection closed or a frame enqueued.
		for data == nil {
			if q.numQueued == 0 {
				q.writeCond.Wait()
			}
			if q.h2Conn.closeDone.Load() != nil {
				return // TODO: remianing queued frames (ex. data, header) will be ignored. fix it.
			}
			data, _ = q.findNext()
		}

		q.numQueued--
		if q.numQueued < 0 {
			panic("internal: negetive counter")
		}
		q.writeCond.L.Unlock()

		dataLen := data.Len()
		n, err := data.WriteTo(q.wire)
		if err != nil {
			bufPool.Put(data)
			q.h2Conn.Terminate(err)
			return
		}

		if n != int64(dataLen) {
			panic("invalid net.Conn")
		}

		bufPool.Put(data)
	}
}

func (q *PQ) findNext() (data *bytes.Buffer, forceFlush bool) {
	if q.incrementIdx {
		// try this pririty again on next funciton call
		// prority 1 consumes 7x bandwidth compared to priority 7
		if q.idx == q.idxCounter-1 {
			q.idx++
			if q.idx == uint8(cap(q.dataQ)) {
				q.idx = 0
			}
			q.idxCounter = 0
		} else {
			q.idxCounter++
		}
	}

	// pull control frames first
	if frame, ok := q.controlQ.pull(); ok {
		q.incrementIdx = false
		return frame, true
	}

	// pull header frame second
	if frame, ok := q.pullHeader(); ok {
		q.incrementIdx = false
		q.h2Conn.resetIdle()
		return frame, false
	}

	// checking for data frame
	end := q.idx
	var ok bool
	for {
		var node *priorityNode

		if q.exclusiveNode != nil {
			node = q.exclusiveNode
		}

		node, ok = q.dataQ[q.idx].pull()
		if ok {
			frame, ok := node.frames.pull()
			if !ok {
				panic("internal: node have not any frame")
			}

			q.incrementIdx = !node.exclusive
			if node.exclusive {
				q.exclusiveNode = node
			}

			q.h2Conn.resetIdle()
			return frame.Buffer, frame.forceFlush
		}
		// empty queue; try next
		q.idx++
		if q.idx == uint8(cap(q.dataQ)) {
			q.idx = 0
		}
		q.idxCounter = 0

		if q.idx == end {
			break
		}
	}
	return nil, false
}

func (q *PQ) createNode(stream *Stream, priority PriorityParam) {
	node := &priorityNode{
		stream:    stream,
		weight:    priority.Weight,
		exclusive: priority.Exclusive,
		dependsOn: nil,
		frames:    make(chanQ[flushingFrame], 1),
	}

	q.nodesMu.Lock()

	node.dependsOn = q.nodes[priority.StreamDep]
	node.calculateRank()
	q.nodes[stream.id] = node

	q.nodesMu.Unlock()
}

func (q *PQ) closeSteam(streamId uint32) {
	q.nodesMu.Lock()
	defer q.nodesMu.Unlock()

	// node := q.nodes[streamId]
	// q.dataQ[node.rank].remove(node)

	q.headerQ.remove(func(item queueHeaderFrame) bool {
		return item.streamID == streamId
	})

	delete(q.nodes, streamId)
	// TODO possible race condition
}
func (q *PQ) changePriority(streamID uint32, weight uint8, exclusive bool) {
	q.nodesMu.Lock()
	defer q.nodesMu.Unlock()

	node := q.nodes[streamID]
	// q.dataQ[node.rank].remove(node)

	node.weight = weight
	node.exclusive = exclusive

	node.calculateRank()

	// q.dataQ[node.rank].insert(node)
	// TODO posible race condition
}

func (q *PQ) enqueue(bf byteframe) (err error) {
	if bf.control {
		q.mu.Lock()
		q.controlQ.push(bf.data)
	} else {
		q.nodesMu.Lock()
		node := q.nodes[bf.stream.id]
		q.nodesMu.Unlock()
		if node == nil {
			return fmt.Errorf("node not exists")
		}

		node.frames.push(flushingFrame{bf.data, bf.forceFlush})
		q.mu.Lock()

		q.dataQ[node.rank].push(node)
	}

	q.numQueued++
	q.addToRunnables(q)
	q.mu.Unlock()

	return nil
}

func (q *PQ) write() (err error, hasMore bool) {
	const noMore = false
	var forceFlush bool

	if q.remaining == nil {
		q.remaining, forceFlush = q.findNext()
		if q.remaining == nil {
			return nil, noMore
		}
	}

	// if q.remaining.Len() > q.w.Available() {
	// 	err = q.w.Flush()
	// 	if err == nil {
	// 		n, err = q.remaining.WriteTo(q.wire)
	// 	}
	// } else {
	_, err = q.remaining.WriteTo(q.w)
	// }

	// q.h2Conn.streamsMu.Lock()
	streamsLen := len(q.h2Conn.streams)
	// q.h2Conn.streamsMu.Unlock()

	if err == nil && (streamsLen == 0 || forceFlush) {
		err = q.w.Flush()
	}
	// n, err := q.remaining.WriteTo(q.wire)

	if q.remaining.Len() == 0 {
		bufPool.Put(q.remaining)
		q.remaining, _ = q.findNext()

		return nil, q.remaining != nil
	}

	return err, true
}

func (q *PQ) WriteHeaders(stream *Stream, headers []hpack.HeaderField, priority PriorityParam, endStream bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.headerQ.push(queueHeaderFrame{
		streamID:  stream.id,
		headers:   headers,
		endStream: endStream,
		stream:    stream,
	})

	q.numQueued++
	q.addToRunnables(q)

	if endStream {
		return nil
	}

	// creating new priority node for the steam
	q.createNode(stream, priority)

	// TODO check it
	return nil
}

func (q *PQ) pullHeader() (frame *bytes.Buffer, ok bool) {
	h, ok := q.headerQ.pull()
	if !ok {
		return nil, false
	}

	for _, field := range h.headers {
		if err := q.hpackEncoder.WriteField(field); err != nil {
			panic(fmt.Sprintf("writing hpack field failed: %v", err))
		}
	}

	maxFrameSize := q.h2Conn.peerSettings.MAX_FRAME_SIZE
	headers := q.hpackBuff.Bytes()

	buff := bufPool.Get()
	framer := framers.GetWith(buff)

	data := headers[:min(len(headers), int(maxFrameSize))]
	headers = headers[len(data):]
	endHeaders := len(headers) == 0

	err := framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      h.streamID,
		BlockFragment: data,
		PadLength:     0,
		Priority:      h.priority,
		EndStream:     h.endStream && endHeaders,
		EndHeaders:    endHeaders,
	})
	if err != nil {
		q.hpackBuff.Reset()
		framers.Put(framer)
		panic(fmt.Sprintf("writing headers frame failed: %v", err))
	}

	for !endHeaders {

		data = headers[:min(len(headers), int(maxFrameSize))]
		headers = headers[len(data):]
		endHeaders = len(headers) == 0

		err = framer.WriteContinuation(h.streamID, endHeaders, data)
		if err != nil {
			q.hpackBuff.Reset()
			framers.Put(framer)
			panic(fmt.Sprintf("writing continuation frame failed: %v", err))
		}
	}

	if h.endStream {
		h.stream.endStream()
	} else {
		h.stream.startedWg.Done()
	}

	q.hpackBuff.Reset()
	framers.Put(framer)
	return buff, true
}

type ring[T any] struct {
	sync.Mutex
	buf        []T
	start, end uint16
}

func newRing[T any](max int) *ring[T] {
	return &ring[T]{buf: make([]T, max+1)}
}

func (r *ring[T]) push(item T) (ok bool) {
	r.Lock()
	if r.len() == r.cap() {
		r.Unlock()
		return false // its full
	}

	r.buf[r.end] = item
	r.end++
	if r.end == uint16(cap(r.buf)) {
		r.end = 0
	}

	r.Unlock()
	return true
}

func (r *ring[T]) pull() (item T, ok bool) {
	r.Lock()

	if r.isEmpty() {
		r.Unlock()
		return item, false
	}

	item = r.buf[r.start]
	// r.buf[r.start] = zero
	r.start++
	if r.start == uint16(cap(r.buf)) {
		r.start = 0
	}

	r.Unlock()
	return item, true
}

func (r *ring[T]) remove(del func(item T) bool) {
	r.Lock()
	found := false
	for i := r.start; i != r.end; {
		if !found && del(r.buf[i]) {
			found = true
		}

		prev := i
		i++
		if i == uint16(cap(r.buf)) {
			i = 0
		}

		if found {
			r.buf[prev] = r.buf[i]
		}

		// TODO: zero the last element
	}

	if found {
		r.end--
		r.end %= uint16(cap(r.buf))
	}
	r.Unlock()
}

func (r *ring[T]) len() int {
	l := int(r.end) - int(r.start)
	if l < 0 {
		l += cap(r.buf)
	}
	return l
}
func (r *ring[T]) isEmpty() bool {
	return r.end == r.start
}
func (r *ring[T]) cap() int {
	return cap(r.buf) - 1
}

func (r *ring[T]) String() string {
	return r.string(true)
}
func (r *ring[T]) string(lock bool) string {
	if lock {
		r.Lock()
		defer r.Unlock()
	}

	if r.len() == 0 {
		return fmt.Sprintf("ring[s=%d,e=%d]{}", r.start, r.end)
	}

	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "ring[s=%d,e=%d]{", r.start, r.end)
	for i := r.start; i != r.end-1; {
		fmt.Fprintf(buf, "%v, ", r.buf[i])
		i++
		i %= uint16(cap(r.buf))
	}
	fmt.Fprintf(buf, "%v}", r.buf[r.end-1])

	// s := fmt.Sprintf("ring[%v, %v]",
	// 	r.buf[r.start:min(int(r.start)+r.len(), cap(r.buf))],
	// 	r.buf[r.start:min(int(r.end), cap(r.buf))],
	// )

	return buf.String()
}

type expandableRing[T any] struct {
	ring[T]
}

func newExRing[T any]() *expandableRing[T] {
	return &expandableRing[T]{*newRing[T](4)}
}

func (r *expandableRing[T]) push(item T) {
	for {
		if ok := r.ring.push(item); ok {
			return
		}

		r.expand()
	}
}

func (r *expandableRing[T]) expand() {
	r.Lock()
	defer r.Unlock()

	oldCap := r.ring.cap()
	if r.ring.len() < oldCap {
		return
	}
	newCap := oldCap * 2
	new := newRing[T](newCap)

	end := uint16(0)
	for i := r.start; i != r.end; {
		new.buf[end] = r.buf[i]

		i++
		i %= uint16(cap(r.buf))
		end++
	}

	r.start = 0
	r.end = end
	r.ring.buf = new.buf
}
