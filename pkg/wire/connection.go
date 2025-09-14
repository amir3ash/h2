package wire

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const idleTimeout = 10 * time.Second
const idleEpsilon = idleTimeout / 4

var cliNum = atomic.Uint32{}
var bufPool = &bufferPool{}
var framers = &framerPool{}

var ErrIdleConnection = errors.New("idle connection timeout")

type connectionState uint8

const (
	_ = connectionState(iota)
	PrefaceRecieved
	SettingsSent
	SettingsRecieved
	GoAwaySent
	GoAwayRecieved
	Terminated
)

func (cs connectionState) String() string {
	ss := [...]string{
		"nil",
		"PrefaceRecieved",
		"SettingsSent",
		"SettingsRecieved",
		"GoAwaySent",
		"GoAwayRecieved",
		"Terminated",
	}
	return ss[cs]
}

// - - -- - - -- - ---- - -- - - - -- - - - - -- - - -- - - - -
type settings struct {
	HEADER_TABLE_SIZE      uint32
	ENABLE_PUSH            bool
	MAX_CONCURRENT_STREAMS uint32
	INITIAL_WINDOW_SIZE    int32
	MAX_FRAME_SIZE         uint32
	MAX_HEADER_LIST_SIZE   uint32

	others map[uint32]uint32
}

// default mySettings
var defaultSettings = settings{
	HEADER_TABLE_SIZE:      4096,
	MAX_CONCURRENT_STREAMS: 250,
	INITIAL_WINDOW_SIZE:    65535,
	MAX_FRAME_SIZE:         1 << 14,
	MAX_HEADER_LIST_SIZE:   4096,
}

type mySettings struct {
	settings

	mu          sync.Mutex
	newSettings [][]http2.Setting
}

func newMySettings() *mySettings {
	return &mySettings{settings: defaultSettings}
}

func (s *mySettings) writeSettings(newSettings []http2.Setting) (oldNoneAcked int) {
	s.mu.Lock()
	oldNoneAcked = len(s.newSettings)
	s.newSettings = append(s.newSettings, newSettings)
	s.mu.Unlock()
	return
}
func (s *mySettings) peerAcked(conn *Connection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.newSettings) == 0 {
		return errors.New("no settings sent before")
	}

	for _, v := range s.newSettings[0] {
		if err := conn.processMySettings(s, v); err != nil {
			return err
		}
	}

	copy(s.newSettings, s.newSettings[1:])
	s.newSettings = s.newSettings[:len(s.newSettings)-1]
	return nil
}
func (s *mySettings) noneAckedSettings() (n int) {
	s.mu.Lock()
	n = len(s.newSettings)
	s.mu.Unlock()
	return n
}

// A HTTP2 Connection
type Connection struct {
	id           uint32
	isServer     bool
	peerSettings settings
	mySettings   *mySettings

	state      connectionState
	readFramer *ReadFramer // for reads only
	wireConn   net.Conn
	ackedPings findOrDie[[8]byte]

	// hpackMu      sync.Mutex
	hpackEncoder *hpack.Encoder
	// hpackBuf     bytes.Buffer

	maxStreamID     uint32
	maxPeerStreamID uint32
	goAwaySent      bool
	closeErr        error
	closeDone       atomic.Pointer[chan struct{}] // non-nil means Close called

	openConns     atomic.Int32 // initiated by us
	openPeerConns atomic.Int32 // initiated by the peer
	streamsMu     sync.Mutex
	streams       map[uint32]*Stream
	openStreamsWG sync.WaitGroup

	inflow  *inflow
	outflow *outflow

	// rtt time.Duration

	idleTimer     *time.Timer
	lastResetIdle fastTime

	settingsHooks map[http2.SettingID]func(val uint32) // callbacks when settings arrived from the peer
	settingsChan  chan http2.Setting

	que                    *PQ
	whenPeerInitatedStream func(s *Stream)
	whenClosed             func(h2Conn *Connection)

	ctx       context.Context
	cancelCtx func()
}

func newConnection(ctx context.Context, conn net.Conn, signalWrite func(*PQ)) *Connection {
	queue := PQ{
		wire: conn,
		// w:            bufio.NewWriterSize(conn, 256),
		hpackEncoder: nil,
		controlQ:     newExRing[*bytes.Buffer](),
		headerQ:      newExRing[queueHeaderFrame](),
		nodes:        make(map[uint32]*priorityNode),
	}
	queue.writeCond = *sync.NewCond(&queue.mu)

	for i := range queue.dataQ {
		queue.dataQ[i] = newLinkedQ[*priorityNode]()
	}

	readerConn := conn

	encoder := hpack.NewEncoder(&queue.hpackBuff)
	framer := NewReadFramer(nil, readerConn)
	framer.ReadMetaHeaders = hpack.NewDecoder(defaultSettings.HEADER_TABLE_SIZE, nil)
	framer.SetReuseFrames()
	framer.SetMaxReadFrameSize(defaultSettings.MAX_FRAME_SIZE)
	_, framer.avoidIoLoop = readerConn.(*tcpConn)

	defaultPeerSettings := settings{
		HEADER_TABLE_SIZE:      4096,
		ENABLE_PUSH:            false,
		MAX_CONCURRENT_STREAMS: 100,
		INITIAL_WINDOW_SIZE:    65535,
		MAX_FRAME_SIZE:         1 << 14,
		MAX_HEADER_LIST_SIZE:   4096,
	}

	c := Connection{
		id:                     cliNum.Add(1),
		wireConn:               conn,
		readFramer:             framer,
		mySettings:             newMySettings(),
		peerSettings:           defaultPeerSettings,
		hpackEncoder:           encoder,
		streams:                make(map[uint32]*Stream, 1),
		que:                    &queue,
		whenPeerInitatedStream: func(s *Stream) { panic("default hook") },
		outflow:                &outflow{},
		inflow:                 &inflow{},
		whenClosed:             func(h2Conn *Connection) {},
	}

	c.ctx, c.cancelCtx = context.WithCancel(ctx)

	queue.h2Conn = &c
	queue.hpackEncoder = encoder
	queue.addToRunnables = signalWrite
	c.outflow.add(65535)
	c.inflow.init(65535)

	return &c
}

func (conn *Connection) Context() context.Context {
	return conn.ctx
}

// RemoteAddr returns the remote network address, if known.
func (conn *Connection) RemoteAddr() net.Addr {
	return conn.wireConn.RemoteAddr()
}

func (conn *Connection) NewIdleStream() (*Stream, error) {
	return conn.newStream(0, IdleState)
}

func (conn *Connection) getStream(id uint32) (s *Stream, ok bool) {
	conn.streamsMu.Lock()
	s, ok = conn.streams[id]
	conn.streamsMu.Unlock()
	return
}

func (conn *Connection) newStream(id uint32, ss streamState) (*Stream, error) {
	if conn.goAwaySent {
		return nil, fmt.Errorf("goAway frame sent")
	}

	stream := &Stream{
		conn:    conn,
		id:      id,
		outflow: &outflow{conn: conn.outflow},
		inflow:  &inflow{},

		OnDataHook:    func(data []byte, stream *Stream) error { return nil },
		OnPearEndHook: func(stream *Stream) error { return nil },
		startedWg:     &sync.WaitGroup{},
		oFlowCond:     sync.NewCond(nopLocker{}),
		ctx:           conn.ctx,
	}
	stream.outflow.add(conn.peerSettings.INITIAL_WINDOW_SIZE)
	stream.inflow.add(conn.mySettings.INITIAL_WINDOW_SIZE)

	stream.state.Store(uint32(ss))
	conn.openStreamsWG.Add(1)
	conn.streamsMu.Lock()
	conn.streams[id] = stream
	conn.streamsMu.Unlock()
	conn.resetIdle()

	return stream, nil
}

// called by [Stream.close]
func (conn *Connection) removeStream(id uint32) {
	conn.streamsMu.Lock()

	_, ok := conn.streams[id]
	if !ok {
		slog.Error("internal: stream not exists")
		conn.streamsMu.Unlock()
		return
	}

	delete(conn.streams, id)
	conn.streamsMu.Unlock()

	// TODO: set conncection state to idle
	conn.resetIdle()

	if conn.isServer == (id%2 == 0) {
		conn.openConns.Add(-1)
	} else {
		conn.openPeerConns.Add(-1)
	}
	conn.openStreamsWG.Done()

	// TODO: Return any buffered unread bytes worth of conn-level flow control.

}

func (conn *Connection) negotiate() error {
	if cs := conn.state; cs != PrefaceRecieved && cs != SettingsRecieved {
		panic("internal: bad negotiatiton time")
	}

	settings := make([]http2.Setting, 0, 2)
	// settings = append(settings, http2.Setting{http2.SettingEnablePush, 1})
	settings = append(settings, http2.Setting{http2.SettingInitialWindowSize, uint32(conn.mySettings.INITIAL_WINDOW_SIZE)})
	settings = append(settings, http2.Setting{http2.SettingMaxFrameSize, conn.mySettings.MAX_FRAME_SIZE})

	if err := conn.writeSettingsFrame(settings); err != nil {
		return err
	}

	if conn.state == PrefaceRecieved {
		conn.state = SettingsSent
	}

	i := 1
	conn.idleTimer = time.AfterFunc(idleTimeout, func() {
		if conn.closeDone.Load() != nil {
			return
		}

		_, permanentErr := conn.wireConn.Write([]byte{})

		slog.Error("idle connection",
			"id", conn.id,
			"run", i,
			"lenStreams", len(conn.streams),
			"writeErr", permanentErr,
			"headerQLen", conn.que.headerQ.len(),
		)
		if len(conn.streams) > 0 && permanentErr == nil {
			conn.resetIdle()
			i++
			conn.que.addToRunnables(conn.que)
			return
		}

		conn.Terminate(ErrIdleConnection)
	})

	return nil
}

func (conn *Connection) resetIdle() {
	if now := now_bufferd(); conn.lastResetIdle.add(idleEpsilon).before(now) {
		conn.idleTimer.Reset(idleTimeout)
		conn.lastResetIdle = now
	}
}

func (conn *Connection) onFrame(frame Frame) error {
	var err error

	// First frame received must be SETTINGS.
	if conn.state < SettingsRecieved {
		if frame.Header().Type != http2.FrameSettings {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		conn.state = SettingsRecieved
	}

	// Discard frames for streams initiated after the identified last
	// stream sent in a GOAWAY, or all frames after sending an error.
	// We still need to return connection-level flow control for DATA frames.
	// RFC 9113 Section 6.8.
	// if sc.inGoAway && (sc.goAwayCode != ErrCodeNo || frame.Header().StreamID > conn.maxPeerStreamID) {

	// 	if f, ok := frame.(*http2.DataFrame); ok {
	// 		if !conn.inflow.take(f.Length) {
	// 			return http2.StreamError{StreamID: f.Header().StreamID, Code: http2.ErrCodeFlowControl}
	// 		}
	// 		sc.sendWindowUpdate(nil, int(f.Length)) // conn-level
	// 	}
	// 	return nil
	// }

	if h, ok := frame.(*MetaHeadersFrame); ok {
		err = conn.onHeader(h)
		return err
	}

	switch frame.Header().Type {
	case http2.FrameSettings:
		err = conn.onSettingFrame(frame.(*SettingsFrame))
	case http2.FrameData:
		err = conn.onData(frame.(*DataFrame))
	case http2.FramePriority:
		// ignore
	case http2.FrameRSTStream:
		err = conn.onResetStreamFrame(frame.(*RSTStreamFrame))
	case http2.FrameGoAway:
		err = conn.onGoAwayFrame(frame.(*GoAwayFrame))
	case http2.FramePing:
		err = conn.onPingFrame(frame.(*PingFrame))
	case http2.FramePushPromise:
		err = conn.onPushPromise(frame.(*PushPromiseFrame))
	case http2.FrameWindowUpdate:
		err = conn.onWindowUpdateFrame(frame.(*WindowUpdateFrame))
	}

	return err
}

func (conn *Connection) onSettingFrame(frame *SettingsFrame) error {
	// streamID MUST be zero
	if frame.StreamID != 0 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if frame.IsAck() {
		if frame.Length != 0 {
			return http2.ConnectionError(http2.ErrCodeFrameSize)
		}

		if err := conn.mySettings.peerAcked(conn); err != nil {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		return nil
	}

	// settings frame length MUST be multiple of 6 octets
	if frame.Length%6 != 0 {
		return http2.ConnectionError(http2.ErrCodeFrameSize)
	}

	if frame.NumSettings() > 64 || frame.HasDuplicates() {
		// This isn't actually in the spec, but hang up on
		// suspiciously large settings frames or those with
		// duplicate entries.
		slog.Error("PROTOCOL ERROR", "msg", "duplicate settings")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if err := frame.ForeachSetting(conn.processPeerSetting); err != nil {
		slog.Error("PROTOCOL ERROR", "msg", "onSetting: setting not valid", "err", err)
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteSettingsAck(); err != nil {
		return err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}
	return nil
}
func (conn *Connection) WriteSettings(settings []http2.Setting) error {
	return conn.writeSettingsFrame(settings)
}
func (conn *Connection) writeSettingsFrame(settings []http2.Setting) error {
	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteSettings(settings...); err != nil {
		return err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}

	oldNoneAcked := conn.mySettings.writeSettings(settings)
	time.AfterFunc(1000*time.Millisecond, func() {
		if new := conn.mySettings.noneAckedSettings(); new > oldNoneAcked {
			err := http2.ConnectionError(http2.ErrCodeSettingsTimeout)
			if err := conn.CloseWithError(err); err != nil {
				// TODO: log it
			}
		}
	})

	return nil
}

func (conn *Connection) processMySettings(settings *mySettings, s http2.Setting) error {
	if err := s.Valid(); err != nil {
		return err
	}

	switch s.ID {
	case http2.SettingHeaderTableSize:
		conn.readFramer.ReadMetaHeaders.SetMaxDynamicTableSize(s.Val)
	case http2.SettingEnablePush:
		settings.ENABLE_PUSH = s.Val != 0
	case http2.SettingMaxConcurrentStreams:
		settings.MAX_CONCURRENT_STREAMS = s.Val
	case http2.SettingInitialWindowSize:
		// "A SETTINGS frame can alter the initial flow control window
		// size for all current streams. When the value of
		// SETTINGS_INITIAL_WINDOW_SIZE changes, a receiver MUST
		// adjust the size of all stream flow control windows that it
		// maintains by the difference between the new value and the
		// old value."
		old := settings.INITIAL_WINDOW_SIZE
		settings.INITIAL_WINDOW_SIZE = int32(s.Val)
		growth := int32(s.Val) - old // may be negative

		conn.streamsMu.Lock()
		defer conn.streamsMu.Unlock()

		for _, stream := range conn.streams {
			if n := stream.inflow.add(growth); n > 0 {
				// TODO: fix this
				// 6.9.2 Initial Flow Control Window Size
				// "An endpoint MUST treat a change to
				// SETTINGS_INITIAL_WINDOW_SIZE that causes any flow
				// control window to exceed the maximum size as a
				// connection error (Section 5.4.1) of type
				// FLOW_CONTROL_ERROR."
				// return http2.ConnectionError(http2.ErrCodeFlowControl)
			}
		}

	case http2.SettingMaxFrameSize:
		settings.MAX_FRAME_SIZE = s.Val // the maximum valid s.Val is < 2^31
		conn.readFramer.SetMaxReadFrameSize(s.Val)
	case http2.SettingMaxHeaderListSize:
		settings.MAX_HEADER_LIST_SIZE = s.Val
		conn.readFramer.MaxHeaderListSize = s.Val

	default:
		settings.others[uint32(s.ID)] = s.Val
		// do nothing; "An endpoint that receives a SETTINGS
		// frame with any unknown or unsupported identifier MUST
		// ignore that setting."
	}

	return nil
}

func (conn *Connection) processPeerSetting(s http2.Setting) error {
	if err := s.Valid(); err != nil {
		return err
	}

	switch s.ID {
	case http2.SettingHeaderTableSize:
		conn.hpackEncoder.SetMaxDynamicTableSize(s.Val)
	case http2.SettingEnablePush:
		conn.peerSettings.ENABLE_PUSH = s.Val != 0
	case http2.SettingMaxConcurrentStreams:
		conn.peerSettings.MAX_CONCURRENT_STREAMS = s.Val
	case http2.SettingInitialWindowSize:
		// "A SETTINGS frame can alter the initial flow control window
		// size for all current streams. When the value of
		// SETTINGS_INITIAL_WINDOW_SIZE changes, a receiver MUST
		// adjust the size of all stream flow control windows that it
		// maintains by the difference between the new value and the
		// old value."
		old := conn.peerSettings.INITIAL_WINDOW_SIZE
		conn.peerSettings.INITIAL_WINDOW_SIZE = int32(s.Val)
		growth := int32(s.Val) - old // may be negative

		for _, stream := range conn.streams {
			if !stream.outflow.add(growth) {
				// 6.9.2 Initial Flow Control Window Size
				// "An endpoint MUST treat a change to
				// SETTINGS_INITIAL_WINDOW_SIZE that causes any flow
				// control window to exceed the maximum size as a
				// connection error (Section 5.4.1) of type
				// FLOW_CONTROL_ERROR."
				return http2.ConnectionError(http2.ErrCodeFlowControl)
			}
		}

	case http2.SettingMaxFrameSize:
		conn.peerSettings.MAX_FRAME_SIZE = s.Val // the maximum valid s.Val is < 2^31
	case http2.SettingMaxHeaderListSize:
		conn.peerSettings.MAX_HEADER_LIST_SIZE = s.Val
	default:
		if _, ok := conn.settingsHooks[s.ID]; ok {
			conn.settingsChan <- s // run hooks in another gorouting
		}
		// do nothing; "An endpoint that receives a SETTINGS
		// frame with any unknown or unsupported identifier MUST
		// ignore that setting."
	}

	return nil
}

// runs in seperate goroutin
func (conn *Connection) runSettingsHooks() {
	// TODO: now: 1 gorouting per connection;
	// TODO: seperate this from [Connection]
	for s := range conn.settingsChan {
		if hook, ok := conn.settingsHooks[s.ID]; ok {
			hook(s.Val)
		}
	}
}

func (conn *Connection) onPingFrame(frame *PingFrame) error {
	if frame.StreamID != 0 || frame.Length != 8 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if frame.IsAck() {
		if sentBefore := conn.ackedPings.insert(frame.Data); !sentBefore {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
	}

	// write ack
	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WritePing(true, frame.Data); err != nil {
		return err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}
	return nil
}

// It sends a ping frame with random data to the peer.
// Then waits until reciving the ack or ctx.Done. Returns round trip time.
func (conn *Connection) PingPeer(ctx context.Context) (rtt time.Duration, err error) {
	data := [8]byte{}
	if _, err := rand.Read(data[:]); err != nil {
		return 0, err
	}

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	sent := time.Now()
	if err := framer.WritePing(false, data); err != nil {
		return 0, err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return 0, err
	}

	// wait for ack or return ctx's Err
	if err := conn.ackedPings.exist(ctx, data); err != nil {
		return 0, err
	}

	return time.Since(sent), nil
}

func (conn *Connection) onGoAwayFrame(frame *GoAwayFrame) error {
	if frame.StreamID != 0 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	conn.goAwaySent = true
	conn.state = GoAwayRecieved
	conn.closeErr = http2.GoAwayError{ErrCode: frame.ErrCode, LastStreamID: frame.LastStreamID}

	if frame.LastStreamID != conn.maxStreamID {
		// TODO: close streamIds higher than max; becouse
		// peer will ignore frames with higher frame ids
		for i := frame.LastStreamID; i < conn.maxPeerStreamID; i++ {
			stream, ok := conn.getStream(i)
			if ok {
				stream.close(conn.closeErr)
			}
		}
	}

	if frame.LastStreamID == 0 {
		if err := conn.Terminate(conn.closeErr); err != nil {
			return err
		}
	}

	// TODO: should we send another GoAway frame for ack?
	// TODO: close wireConn
	return nil
}

// low level; use [Connection.CloseWithError]
func (conn *Connection) WriteGoAway(code http2.ErrCode, debugData []byte) error {
	conn.goAwaySent = true

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteGoAway(conn.maxPeerStreamID, code, debugData); err != nil {
		return err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}

	return nil
}

// calls CloseWithError with nil error.
func (conn *Connection) Close() error {
	return conn.CloseWithError(nil)
}

// It close the connection and unwraps the err and send goaway frame.
// In case of goaway error, it waits until all stream to be closed gracefully.
func (conn *Connection) CloseWithError(err error) error {
waitDone:
	if closeDone := conn.closeDone.Load(); closeDone != nil {
		<-*closeDone
		return conn.closeErr
	}

	done := make(chan struct{})
	if !conn.closeDone.CompareAndSwap(nil, &done) {
		goto waitDone
	}
	defer close(done)
	conn.cancelCtx()

	var code http2.ErrCode = http2.ErrCodeNo

	switch ev := err.(type) {
	case nil:
	case http2.ConnectionError:
		code = http2.ErrCode(ev)
	case http2.StreamError:
		code = ev.Code
	default:
		code = http2.ErrCodeInternal
		if err == http2.ErrFrameTooLarge {
			code = http2.ErrCodeFrameSize
		}
	}

	conn.closeErr = err
	conn.state = GoAwaySent
	slog.Error("connection closed and write goAway", "id", conn.id, "code", code, "lenStreams", len(conn.streams), "err", err)

	if err := conn.WriteGoAway(code, nil); err != nil {
		conn.whenClosed(conn)

		conn.wireConn.SetDeadline(time.Now().Add(time.Second))
		return conn.wireConn.Close()
	}

	// wait minimum 1 rtt to let peer proccess the frame
	// and avoiding closeing TCP connection with RST flag
	// and let TCP buffer to be empty.
	time.Sleep(1 * time.Second)
	// TODO: close Idle streams

	if ShouldCloseGracefully(err) {
		// wait for all streams to be closed
		conn.openStreamsWG.Wait()
	}

	return conn.Terminate(err)
}

// Terminate closes the connection without sending goaway frame
// and without waiting for open streams to be closed.
func (conn *Connection) Terminate(err error) error {
	conn.closeErr = err
	conn.state = Terminated

	if conn.closeDone.Load() == nil {
		done := make(chan struct{})
		conn.closeDone.CompareAndSwap(nil, &done)
		close(done)
	}

	conn.cancelCtx()
	conn.idleTimer.Stop()

	conn.whenClosed(conn)
	conn.que.writeCond.Signal()

	conn.wireConn.SetDeadline(time.Now().Add(2 * time.Millisecond))
	err = conn.wireConn.Close()

	return err
}

func (conn *Connection) onWindowUpdateFrame(frame *WindowUpdateFrame) error {
	if frame.Increment == 0 {
		if frame.StreamID == 0 {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		return http2.StreamError{StreamID: frame.StreamID, Code: http2.ErrCodeProtocol}
	}

	if frame.StreamID > conn.maxPeerStreamID {
		slog.Error("PROTOCOL ERROR", "msg", "onWindowUpdate: too big stream id")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if frame.StreamID != 0 {
		s, found := conn.getStream(frame.StreamID)
		if !found {
			// "WINDOW_UPDATE can be sent by a peer that has sent a
			// frame bearing the END_STREAM flag. This means that a
			// receiver could receive a WINDOW_UPDATE frame on a "half
			// closed (remote)" or "closed" stream. A receiver MUST
			// NOT treat this as an error, see Section 5.1."
			return nil
		}
		if s.State() == IdleState {
			// Section 5.1: "Receiving any frame other than HEADERS
			// or PRIORITY on a stream in this state MUST be
			// treated as a connection error (Section 5.4.1) of
			// type PROTOCOL_ERROR."
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}

		return s.onWindowUpdateFrame(frame)
	}

	// connection level
	if !conn.outflow.add(int32(frame.Increment)) {
		return conn.WriteGoAway(http2.ErrCodeFlowControl, nil)
	}

	return nil
}

func (conn *Connection) onResetStreamFrame(frame *RSTStreamFrame) error {
	if frame.Length != 4 {
		return http2.ConnectionError(http2.ErrCodeFrameSize)
	}

	if frame.StreamID == 0 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if frame.StreamID > conn.maxPeerStreamID {
		slog.Error("PROTOCOL ERROR", "msg", "onReset: too big stream id")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if stream, found := conn.getStream(frame.StreamID); !found {
		// slog.Error("PROTOCOL ERROR", "msg", "onReset: stream not found")
		// return http2.ConnectionError(http2.ErrCodeProtocol)
		return nil
	} else {
		return stream.onResetFrame(frame)
	}
}

func (conn *Connection) onPushPromise(frame *PushPromiseFrame) error {
	if !conn.mySettings.ENABLE_PUSH {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	// initiated by server. Must be even
	if frame.PromiseID&1 == 0 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	stream, found := conn.getStream(frame.StreamID)
	if !found {

	}

	if ss := stream.State(); ss != OpenState && ss != HalfClosedLocalState {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	for {
		peerConns := conn.openPeerConns.Load()
		if peerConns+1 > int32(conn.mySettings.MAX_CONCURRENT_STREAMS) {

		}
		if conn.maxPeerStreamID < frame.PromiseID {
			return http2.ConnectionError(http2.ErrCodeProtocol)
		}
		if conn.openPeerConns.CompareAndSwap(peerConns, peerConns+1) {
			break
		}
	}

	if newStream, err := conn.newStream(frame.PromiseID, ReservedRemoteState); err != nil {
		return err
	} else {
		conn.whenPeerInitatedStream(stream)

		return newStream.onPushPromise(frame)
	}
}

func (conn *Connection) onHeader(frame *MetaHeadersFrame) error {
	// Streams initiated by a client MUST use odd-numbered stream
	// identifiers.
	if frame.StreamID&1 == 0 || conn.state < SettingsRecieved {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	if frame.StreamID == frame.Priority.StreamDep {
		// Section 5.3.1: "A stream cannot depend on itself. An endpoint MUST treat
		// this as a stream error (Section 5.4.2) of type PROTOCOL_ERROR."
		// Section 5.3.3 says that a stream can depend on one of its dependencies,
		// so it's only self-dependencies that are forbidden.
		return http2.StreamError{StreamID: frame.StreamID, Code: http2.ErrCodeProtocol}
	}

	// check MaxHeaderListSize limit
	if frame.Truncated {
		// TODO: check posible race when sending new settings
		return http2.StreamError{StreamID: frame.StreamID, Code: http2.ErrCodeFrameSize}
	}

	stream, found := conn.getStream(frame.StreamID)
	if found {
		// process trailer header
		ss := stream.State()
		if ss == HalfClosedRemoteState {
			return http2.StreamError{StreamID: stream.id, Code: http2.ErrCodeStreamClosed}
		}

		if ss == ClosedState || ss == HalfClosedLocalState {
			return nil
		}

		return stream.onTrailer(frame)
	}

	// creating new stream

	if conn.maxPeerStreamID >= frame.StreamID {
		slog.Error("PROTOCOL ERROR", "msg", "on header: stream id is big")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}
	conn.maxPeerStreamID = frame.StreamID

	for {
		peerConns := conn.openPeerConns.Load()
		if peerConns+1 > int32(conn.mySettings.MAX_CONCURRENT_STREAMS) {
			if conn.mySettings.noneAckedSettings() == 0 {
				slog.Error("PROTOCOL ERROR", "msg", "max concurent streams enriched")
				return http2.ConnectionError(http2.ErrCodeProtocol)
			} else {
				slog.Error("REFUSED STRAEM", "msg", "max concurent streams enriched", "type", "codeRefusedStream")
				return http2.StreamError{StreamID: stream.id, Code: http2.ErrCodeRefusedStream}
			}
		}

		if conn.openPeerConns.CompareAndSwap(peerConns, peerConns+1) {
			break
		}
	}

	stream, err := conn.newStream(frame.StreamID, IdleState)
	if err != nil {
		slog.Error("STREAM_ERROR", "msg", "stream created with error", "err", err, "type", "codeRefusedStream")
		return http2.StreamError{StreamID: frame.StreamID, Code: http2.ErrCodeRefusedStream}
	}

	err = stream.onHeader(frame)
	if err != nil {
		return err
	}

	conn.whenPeerInitatedStream(stream)
	if frame.StreamEnded() {
		stream.OnPearEndHook(stream)
	}
	return err
}

func (conn *Connection) onData(frame *DataFrame) error {
	if frame.StreamID == 0 {
		slog.Error("PROTOCOL ERROR", "msg", "on data frame stream is 0")
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	// If the length of the padding is the length of the frame payload or
	// greater, the recipient MUST treat this as a connection error
	// (Section 5.4.1) of type PROTOCOL_ERROR.
	if len(frame.Data()) == 0 && frame.Length != 0 {
		return http2.ConnectionError(http2.ErrCodeProtocol)
	}

	// "If a DATA frame is received whose stream is not in the "open" or
	// "half-closed (local)" state, the recipient MUST respond with a
	// stream error (Section 5.4.2) of type STREAM_CLOSED."
	stream, found := conn.getStream(frame.StreamID)
	if !found {
		return http2.ConnectionError(http2.ErrCodeStreamClosed)
	}

	if ss := stream.State(); ss != OpenState && ss != HalfClosedLocalState {
		// But still enforce their connection-level flow control,
		// and return any flow control bytes since we're not going
		// to consume them.
		if err := conn.increaseInflowOnErr(stream.id, frame.Length); err != nil {
			return err
		}

		return http2.StreamError{StreamID: frame.StreamID, Code: http2.ErrCodeStreamClosed}
	}

	if err := stream.onDataFrame(frame); err != nil {
		if err := conn.increaseInflowOnErr(stream.id, frame.Length); err != nil {
			return err
		}

		return err
	}

	return nil
}

// free the connection's input flow control in case of stream error
func (conn *Connection) increaseInflowOnErr(streamId uint32, n uint32) error {
	if streamId == 0 {
		panic("internal: zero stream")
	}
	if !conn.inflow.take(n) {
		return http2.StreamError{StreamID: streamId, Code: http2.ErrCodeFlowControl}
	}

	buff := bufPool.Get()
	framer := framers.GetWith(buff)
	defer framers.Put(framer)

	if err := framer.WriteWindowUpdate(0, n); err != nil {
		return err
	}

	if err := conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
		return err
	}

	return nil
}

// reads new frame from framer
func (conn *Connection) ReadProcessFrame() error {
	if conn.state >= Terminated {
		if conn.closeErr != nil {
			return conn.closeErr
		}
		return fmt.Errorf("connection closed")
	}

	frame, err := conn.readFramer.ReadFrame()
	if err != nil {
		return err
	}

	// slog.Debug("read  frame", "h2state", conn.state, "stream", frame.Header().StreamID,
	// 	"type", frame.Header().Type, "conn", fmt.Sprintf("%p", conn))

	err = conn.onFrame(frame)
	if ev, ok := err.(http2.StreamError); ok {
		buff := bufPool.Get()
		framer := framers.GetWith(buff)
		defer framers.Put(framer)

		if err = framer.WriteRSTStream(ev.StreamID, ev.Code); err != nil {
			return err
		}

		if err = conn.que.enqueue(byteframe{control: true, data: buff}); err != nil {
			return err
		}

		return nil
	}

	return err
}

func ShouldCloseGracefully(err error) bool {
	if err == http2.ErrFrameTooLarge {
		return true
	}

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return false
	}

	switch err.(type) {
	case http2.ConnectionError:
		return false // TODO: recheck ShouldCloseGracefully
	case http2.GoAwayError:
		return true
	}
	return false
}
