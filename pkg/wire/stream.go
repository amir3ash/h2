package wire

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// see https://datatracker.ietf.org/doc/html/rfc9113#StreamStatesFigure
type streamState uint8

const (
	IdleState = streamState(iota)
	ReservedLocalState
	ReservedRemoteState
	OpenState
	HalfClosedLocalState
	HalfClosedRemoteState
	ClosedState
)

func (s streamState) String() string {
	res := [...]string{
		"IdleState",
		"ReservedLocalState",
		"ReservedRemoteState",
		"OpenState",
		"HalfClosedLocalState",
		"HalfClosedRemoteState",
		"ClosedState",
	}
	return res[s]
}

type Stream struct {
	conn  *Connection
	id    uint32
	state atomic.Uint32

	outflow   *outflow
	inflow    *inflow
	oFlowCond *sync.Cond

	closeErr error
	ctx      context.Context

	OnDataHook    func(data []byte, stream *Stream) error
	OnPearEndHook func(stream *Stream) error

	headers  []hpack.HeaderField
	trailers []hpack.HeaderField

	startedWg *sync.WaitGroup

	node *priorityNode
}

func (s *Stream) ID() uint32 {
	return s.id
}
func (s *Stream) Connection() *Connection {
	return s.conn
}

func (s *Stream) State() streamState {
	return streamState(s.state.Load())
}
func (s *Stream) swapState(state streamState) (oldState streamState) {
	old := s.state.Swap(uint32(state))
	return streamState(old)
}

func (s *Stream) casState(old, new streamState) bool {
	ok := s.state.CompareAndSwap(uint32(old), uint32(new))
	if !ok {
		panic(fmt.Errorf("casState race: streamID=%d, old1=%v, old2=%v", s.id, old, s.State()))
	}
	return ok
}

func (s *Stream) onPushPromise(frame *PushPromiseFrame) error {
	if s.State() != IdleState {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	return nil

}
func (s *Stream) onWindowUpdateFrame(frame *WindowUpdateFrame) error {
	if s.State() == IdleState {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if !s.outflow.add(int32(frame.Increment)) {
		return http2.StreamError{StreamID: s.id, Code: http2.ErrCodeFlowControl}
	}
	s.oFlowCond.Signal()
	return nil
}

func (s *Stream) onResetFrame(frame *RSTStreamFrame) error {
	// "RST_STREAM frames MUST NOT be sent for a stream in the "idle" state.
	// If a RST_STREAM frame identifying an idle stream is received,
	// the recipient MUST treat this as a connection error (Section 5.4.1) of
	// type PROTOCOL_ERROR."
	if s.State() == IdleState {
		slog.Error("PROTOCOL ERROR", "msg", "onResetFrame: idle stream")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	// TODO Close: stream
	s.close(http2.StreamError{StreamID: s.id, Code: frame.ErrCode})
	s.OnPearEndHook(s)
	return nil
}

func (s *Stream) close(err error) {
	if ss := s.State(); ss == ClosedState {
		return
		// panic("internal: can not close stream")
	}
	if ge, ok := err.(http2.GoAwayError); ok {
		if ge.LastStreamID < s.id {
			// the peer not proccessed the stream
		}
	}

	// TODO: notify upper layer that if it was idle
	// and the peer not proccesed the stream
	if oldState := s.swapState(ClosedState); oldState == ClosedState {
		return
	}

	s.closeErr = err
	s.endStream()

	removeQueuedFrames := err != nil
	s.conn.que.closeSteam(s.id, removeQueuedFrames)
}

func (s *Stream) endStream() {
	s.conn.removeStream(s.id)
	s.oFlowCond.Signal()
}

// It close the stream by sending a reset frame to the peer.
func (s *Stream) Close(code http2.ErrCode) error {
	if ss := s.State(); ss == ClosedState || ss == IdleState {
		return nil
	}

	s.close(http2.StreamError{StreamID: s.id, Code: code})

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteRSTStream(s.id, code); err != nil {
		return err
	}

	if err := s.conn.que.enqueue(byteframe{control: true, stream: s, data: buff}); err != nil {
		return err
	}
	return nil
}

func (s *Stream) onDataFrame(frame *DataFrame) (err error) {
	if ss := s.State(); ss != OpenState && ss != HalfClosedLocalState {
		return http2.StreamError{StreamID: s.id, Code: http2.ErrCodeStreamClosed}
	}
	if frame.Length == 0 {
		return nil
	}
	if !takeInflows(s.conn.inflow, s.inflow, frame.Length) {
		return http2.StreamError{StreamID: s.id, Code: http2.ErrCodeFlowControl}
	}

	if frame.StreamEnded() {
		if s.State() == OpenState {
			s.swapState(HalfClosedRemoteState)
		} else {
			s.swapState(ClosedState)
			s.endStream()
		}
		// TODO: should we check for reseved state?
	}

	// TODO: this runs in main goroutin; can it run in sperate goroutin?
	if err := s.OnDataHook(frame.Data(), s); err != nil {
		return err
	}

	if frame.StreamEnded() {
		s.OnPearEndHook(s)
	}

	if err := s.sendWindowUpdate(nil, int32(frame.Length)); err != nil {
		return err
	}
	if err := s.sendWindowUpdate(s, int32(frame.Length)); err != nil {
		return err
	}

	return nil
}

func (s *Stream) sendWindowUpdate(stream *Stream, n int32) error {
	var send int32

	streamID := uint32(0) // for connection-level flow-control frame
	if stream != nil {
		streamID = stream.id // for stream-level flow-control frame
		send = s.inflow.add(n)
	} else {
		send = s.conn.inflow.add(n)
	}

	if send == 0 {
		return nil
	}

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	// write flow contols frames
	if err := framer.WriteWindowUpdate(streamID, uint32(send)); err != nil {
		return err
	}

	if err := s.conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}
	return nil
}

func (s *Stream) onHeader(frame *MetaHeadersFrame) error {
	if frame.StreamEnded() {
		s.swapState(HalfClosedRemoteState)
	} else {
		s.swapState(OpenState)
	}

	if frame.HasPriority() && !frame.StreamEnded() {
		s.conn.que.createNode(s, frame.Priority)
	}

	s.headers = frame.Fields
	return nil
}

func (s *Stream) onTrailer(frame *MetaHeadersFrame) error {
	if !frame.StreamEnded() || len(frame.PseudoFields()) > 0 {
		return http2.StreamError{StreamID: s.id, Code: http2.ErrCodeProtocol}
	}

	if ss := s.State(); ss == OpenState || ss == ReservedRemoteState {
		s.swapState(HalfClosedRemoteState)
	} else {
		s.swapState(ClosedState)
	}

	s.trailers = frame.RegularFields()
	s.OnPearEndHook(s)
	return nil
}

func (s *Stream) WriteHeaders(headers []hpack.HeaderField, priority PriorityParam, endStream bool) error {
	if s.closeErr != nil {
		return s.closeErr
	}
	if ss := s.State(); ss != IdleState && ss != HalfClosedRemoteState && ss != OpenState {
		return fmt.Errorf("un normal state, stream=%d, state=%v", s.id, ss) // TODO: change this error
	}
	s.startedWg.Add(1)

	var oldState streamState
	if ss := s.State(); endStream {
		switch ss {
		case IdleState:
			s.casState(ss, HalfClosedLocalState)
		case HalfClosedRemoteState:
			s.casState(ss, ClosedState)
			// endStream will be called in PQ.pullHeader
		}
	} else if ss == IdleState {
		oldState = s.swapState(OpenState)
	}
	_ = oldState
	if oldState != IdleState && oldState != HalfClosedRemoteState && oldState != OpenState {
		panic("")
	}
	return s.conn.que.WriteHeaders(s, headers, priority, endStream)
}

// WriteAllOfData like io.Writer, blocks until it writes all data.
func (s *Stream) WriteAllOfData(data []byte, endStream bool) (n int, err error) {
	for {
		writen, err := s.WriteData(data[n:], endStream)
		n += writen
		if err != nil {
			return n, err
		}

		if n == len(data) {
			return n, nil
		}
		// wait until window control frame recived or stream closed
		s.oFlowCond.L.Lock()
		for f := s.outflow.available(); f == 0; f = s.outflow.available() {
			s.oFlowCond.Wait()
		}
		s.oFlowCond.L.Unlock()
	}
}

// WriteData writes data and returns number of written bytes and error.
//
// Maybe it doesn't write all data (ex. small window size, max frame size),
// in that case it will nil as error and number of bytes written n, which n < len(data).
func (s *Stream) WriteData(data []byte, endStream bool) (n int, err error) {
	if ss := s.State(); ss != OpenState && ss != ReservedLocalState && ss != HalfClosedRemoteState {
		return 0, fmt.Errorf("unnormal stream state %v", ss)
	}

	s.startedWg.Wait()
	var forceFlush bool
	// TODO: out flow control
	{
		n := int32(len(data))
		// Might need to split after applying limits.
		allowed := s.outflow.available()
		if n < allowed {
			allowed = n
		}
		if int32(s.conn.peerSettings.MAX_FRAME_SIZE) < allowed {
			allowed = int32(s.conn.peerSettings.MAX_FRAME_SIZE)
		}
		if allowed <= 0 && len(data) != 0 {
			return 0, nil // TODO: change this error
		}
		if len(data) > int(allowed) {
			data = data[:allowed]
			endStream = false
			forceFlush = true
		}

		s.outflow.take(allowed)
	}

	if endStream {
		if s.State() == HalfClosedRemoteState {
			s.swapState(ClosedState)
			s.endStream()
		} else {
			s.swapState(HalfClosedLocalState)
		}
	}

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteData(s.id, endStream, data); err != nil {
		return 0, err
	}

	if err := s.conn.que.enqueue(byteframe{stream: s, data: buff, forceFlush: forceFlush}); err != nil {
		return 0, err
	}

	return len(data), nil
}

// EndWithTrailers writes trailers (header frame after data) and closes the stream.
func (s *Stream) EndWithTrailers(trailers []hpack.HeaderField) error {
	if ss := s.State(); ss != OpenState && ss != ReservedLocalState && ss != HalfClosedRemoteState {
		panic("")
	}

	if s.State() == HalfClosedRemoteState {
		s.swapState(ClosedState)
	} else {
		s.swapState(HalfClosedLocalState)
		s.endStream()
	}

	// in closed state its nop, but what about half-closed-local
	const endStream = true
	return s.conn.que.WriteHeaders(s, trailers, PriorityParam{}, endStream)
}

func (s *Stream) Headers() []hpack.HeaderField {
	return s.headers
}
func (s *Stream) Trailers() []hpack.HeaderField {
	return s.trailers
}

// func (s *Stream) WriteRawFrame(frame *http2) error {
// 	s.conn.framer.WriteRawFrame()
// }

// func (s *Stream) Push(headers []hpack.HeaderField) error {
// 	// http://tools.ietf.org/html/rfc7540#section-6.6.
// 	// PUSH_PROMISE frames MUST only be sent on a peer-initiated stream that
// 	// is in either the "open" or "half-closed (remote)" state.
// 	if s.state != OpenState && s.state != HalfClosedRemoteState {
// 		// responseWriter.Push checks that the stream is peer-initiated.
// 		msg.done <- errStreamClosed
// 		return
// 	}

// 	if s.conn.goAwaySent {
// 		return http.ErrNotSupported
// 	}

// 	// http://tools.ietf.org/html/rfc7540#section-6.6.
// 	if !s.conn.peerSettings.SETTINGS_ENABLE_PUSH || s.id&1 == 1 {
// 		return http.ErrNotSupported
// 	}

// 	// PUSH_PROMISE frames must be sent in increasing order by stream ID, so
// 	// we allocate an ID for the promised stream lazily, when the PUSH_PROMISE
// 	// is written. Once the ID is allocated, we start the request handler.
// 	allocatePromisedID := func() (uint32, error) {
// 		// Check this again, just in case. Technically, we might have received
// 		// an updated SETTINGS by the time we got around to writing this frame.
// 		if !s.conn.peerSettings.SETTINGS_ENABLE_PUSH || s.conn.goAwaySent {
// 			return 0, http.ErrNotSupported
// 		}

// 		// http://tools.ietf.org/html/rfc7540#section-6.5.2.
// 		if s.conn.openConns.Load()+1 > s.conn.peerSettings.SETTINGS_MAX_CONCURRENT_STREAMS {
// 			return 0, http2.ErrPushLimitReached
// 		}

// 		// http://tools.ietf.org/html/rfc7540#section-5.1.1.
// 		// Streams initiated by the server MUST use even-numbered identifiers.
// 		// A server that is unable to establish a new stream identifier can send a GOAWAY
// 		// frame so that the client is forced to open a new connection for new streams.
// 		if s.conn.maxStreamID+2 >= 1<<31 {
// 			sc.startGracefulShutdownInternal()
// 			return 0, http2.ErrPushLimitReached
// 		}
// 		s.conn.maxStreamID += 2
// 		promisedID := s.conn.maxStreamID

// 		// http://tools.ietf.org/html/rfc7540#section-8.2.
// 		// Strictly speaking, the new stream should start in "reserved (local)", then
// 		// transition to "half closed (remote)" after sending the initial HEADERS, but
// 		// we start in "half closed (remote)" for simplicity.
// 		// See further comments at the definition of stateHalfClosedRemote.
// 		promised := sc.newStream(promisedID, msg.parent.id, stateHalfClosedRemote)
// 		rw, req, err := sc.newWriterAndRequestNoBody(promised, httpcommon.ServerRequestParam{
// 			Method:    msg.method,
// 			Scheme:    msg.url.Scheme,
// 			Authority: msg.url.Host,
// 			Path:      msg.url.RequestURI(),
// 			Header:    cloneHeader(msg.header), // clone since handler runs concurrently with writing the PUSH_PROMISE
// 		})
// 		if err != nil {
// 			// Should not happen, since we've already validated msg.url.
// 			panic(fmt.Sprintf("newWriterAndRequestNoBody(%+v): %v", msg.url, err))
// 		}

// 		sc.curHandlers++
// 		go sc.runHandler(rw, req, sc.handler.ServeHTTP)
// 		return promisedID, nil
// 	}

// 	s.conn.que.lock()
// 	defer s.conn.que.unlock()

// 	err := s.conn.framer.WritePushPromise(http2.PushPromiseParam{
// 		StreamID:  s.id,
// 		PromiseID: 0,
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	if err := s.conn.que.enqueue(byteframe{streamId: 99}); err != nil {
// 		return err
// 	}

// }
