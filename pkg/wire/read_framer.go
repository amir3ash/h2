// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var (
	VerboseLogs    bool
	logFrameWrites bool
	logFrameReads  bool
	inTests        bool

	// Enabling extended CONNECT by causes browsers to attempt to use
	// WebSockets-over-HTTP/2. This results in problems when the server's websocket
	// package doesn't support extended CONNECT.
	//
	// Disable extended CONNECT by default for now.
	//
	// Issue #71128.
	disableExtendedConnectProtocol = true
)

func init() {
	e := os.Getenv("GODEBUG")
	if strings.Contains(e, "http2debug=1") {
		VerboseLogs = true
	}
	if strings.Contains(e, "http2debug=2") {
		VerboseLogs = true
		logFrameWrites = true
		logFrameReads = true
	}
	if strings.Contains(e, "http2xconnect=1") {
		disableExtendedConnectProtocol = false
	}
}

var frameParsers = map[http2.FrameType]frameParser{
	http2.FrameData:         parseDataFrame,
	http2.FrameHeaders:      parseHeadersFrame,
	http2.FramePriority:     parsePriorityFrame,
	http2.FrameRSTStream:    parseRSTStreamFrame,
	http2.FrameSettings:     parseSettingsFrame,
	http2.FramePushPromise:  parsePushPromise,
	http2.FramePing:         parsePingFrame,
	http2.FrameGoAway:       parseGoAwayFrame,
	http2.FrameWindowUpdate: parseWindowUpdateFrame,
	http2.FrameContinuation: parseContinuationFrame,
}

func typeFrameParser(t http2.FrameType) frameParser {
	if f := frameParsers[t]; f != nil {
		return f
	}
	return parseUnknownFrame
}

func invalidHTTP1LookingFrameHeader() FrameHeader {
	fh, _ := readFrameHeader(make([]byte, frameHeaderLen), strings.NewReader("HTTP/1.1 "))
	return fh
}

// ReadFrameHeader reads 9 bytes from r and returns a FrameHeader.
// Most users should use ReadFramer.ReadFrame instead.
func ReadFrameHeader(r io.Reader) (FrameHeader, error) {
	bufp := fhBytes.Get().(*[]byte)
	defer fhBytes.Put(bufp)
	return readFrameHeader(*bufp, r)
}

func readFrameHeader(buf []byte, r io.Reader) (FrameHeader, error) {
	_, err := io.ReadFull(r, buf[:frameHeaderLen])
	if err != nil {
		return FrameHeader{}, err
	}
	return FrameHeader{
		FrameHeader: http2.FrameHeader{
			Length:   (uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])),
			Type:     http2.FrameType(buf[3]),
			Flags:    http2.Flags(buf[4]),
			StreamID: binary.BigEndian.Uint32(buf[5:]) & (1<<31 - 1),
		},
		valid: true,
	}, nil
}

// A ReadFramer reads and writes Frames.
type ReadFramer struct {
	r         io.Reader
	lastFrame frameInvalidate
	errDetail error

	headersReaden bool
	header        FrameHeader
	rbuf          []byte
	avoidIoLoop   bool

	// countError is a non-nil func that's called on a frame parse
	// error with some unique error path token. It's initialized
	// from Transport.CountError or Server.CountError.
	countError func(errToken string)

	// lastHeaderStream is non-zero if the last frame was an
	// unfinished HEADERS/CONTINUATION.
	lastHeaderStream uint32

	maxReadSize uint32
	headerBuf   [frameHeaderLen]byte

	// TODO: let getReadBuf be configurable, and use a less memory-pinning
	// allocator in server.go to minimize memory pinned for many idle conns.
	// Will probably also need to make frame invalidation have a hook too.
	getReadBuf func(size uint32) []byte
	readBuf    []byte // cache for default getReadBuf

	maxWriteSize uint32 // zero means unlimited; TODO: implement

	w    io.Writer
	wbuf []byte

	// AllowIllegalWrites permits the ReadFramer's Write methods to
	// write frames that do not conform to the HTTP/2 spec. This
	// permits using the ReadFramer to test other HTTP/2
	// implementations' conformance to the spec.
	// If false, the Write methods will prefer to return an error
	// rather than comply.
	AllowIllegalWrites bool

	// AllowIllegalReads permits the ReadFramer's ReadFrame method
	// to return non-compliant frames or frame orders.
	// This is for testing and permits using the ReadFramer to test
	// other HTTP/2 implementations' conformance to the spec.
	// It is not compatible with ReadMetaHeaders.
	AllowIllegalReads bool

	// ReadMetaHeaders if non-nil causes ReadFrame to merge
	// HEADERS and CONTINUATION frames together and return
	// MetaHeadersFrame instead.
	ReadMetaHeaders *hpack.Decoder

	// MaxHeaderListSize is the http2 MAX_HEADER_LIST_SIZE.
	// It's used only if ReadMetaHeaders is set; 0 means a sane default
	// (currently 16MB)
	// If the limit is hit, MetaHeadersFrame.Truncated is set true.
	MaxHeaderListSize uint32

	// TODO: track which type of frame & with which flags was sent
	// last. Then return an error (unless AllowIllegalWrites) if
	// we're in the middle of a header block and a
	// non-Continuation or Continuation on a different stream is
	// attempted to be written.

	logReads, logWrites bool

	debugFramer       *ReadFramer // only use for logging written writes
	debugFramerBuf    *bytes.Buffer
	debugReadLoggerf  func(string, ...interface{})
	debugWriteLoggerf func(string, ...interface{})

	frameCache *frameCache // nil if frames aren't reused (default)
}

func (fr *ReadFramer) maxHeaderListSize() uint32 {
	if fr.MaxHeaderListSize == 0 {
		return 16 << 20 // sane default, per docs
	}
	return fr.MaxHeaderListSize
}

func (f *ReadFramer) startWrite(ftype FrameType, flags http2.Flags, streamID uint32) {
	// Write the FrameHeader.
	f.wbuf = append(f.wbuf[:0],
		0, // 3 bytes of length, filled in in endWrite
		0,
		0,
		byte(ftype),
		byte(flags),
		byte(streamID>>24),
		byte(streamID>>16),
		byte(streamID>>8),
		byte(streamID))
}

func (f *ReadFramer) endWrite() error {
	// Now that we know the final size, fill in the FrameHeader in
	// the space previously reserved for it. Abuse append.
	length := len(f.wbuf) - frameHeaderLen
	if length >= (1 << 24) {
		return ErrFrameTooLarge
	}
	_ = append(f.wbuf[:0],
		byte(length>>16),
		byte(length>>8),
		byte(length))
	if f.logWrites {
		f.logWrite()
	}

	n, err := f.w.Write(f.wbuf)
	if err == nil && n != len(f.wbuf) {
		err = io.ErrShortWrite
	}
	return err
}

func (f *ReadFramer) logWrite() {
	if f.debugFramer == nil {
		f.debugFramerBuf = new(bytes.Buffer)
		f.debugFramer = NewReadFramer(nil, f.debugFramerBuf)
		f.debugFramer.logReads = false // we log it ourselves, saying "wrote" below
		// Let us read anything, even if we accidentally wrote it
		// in the wrong order:
		f.debugFramer.AllowIllegalReads = true
	}
	f.debugFramerBuf.Write(f.wbuf)
	fr, err := f.debugFramer.ReadFrame()
	if err != nil {
		f.debugWriteLoggerf("http2: ReadFramer %p: failed to decode just-written frame", f)
		return
	}
	f.debugWriteLoggerf("http2: ReadFramer %p: wrote %v", f, summarizeFrame(fr))
}

func (f *ReadFramer) writeByte(v byte)     { f.wbuf = append(f.wbuf, v) }
func (f *ReadFramer) writeBytes(v []byte)  { f.wbuf = append(f.wbuf, v...) }
func (f *ReadFramer) writeUint16(v uint16) { f.wbuf = append(f.wbuf, byte(v>>8), byte(v)) }
func (f *ReadFramer) writeUint32(v uint32) {
	f.wbuf = append(f.wbuf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// SetReuseFrames allows the ReadFramer to reuse Frames.
// If called on a ReadFramer, Frames returned by calls to ReadFrame are only
// valid until the next call to ReadFrame.
func (fr *ReadFramer) SetReuseFrames() {
	if fr.frameCache != nil {
		return
	}
	fr.frameCache = &frameCache{}
}

// NewFramer returns a ReadFramer that writes frames to w and reads them from r.
func NewReadFramer(w io.Writer, r io.Reader) *ReadFramer {
	fr := &ReadFramer{
		w:                 w,
		r:                 r,
		countError:        func(string) {},
		logReads:          logFrameReads,
		logWrites:         logFrameWrites,
		debugReadLoggerf:  log.Printf,
		debugWriteLoggerf: log.Printf,
	}
	fr.getReadBuf = func(size uint32) []byte {
		if cap(fr.readBuf) >= int(size) {
			return fr.readBuf[:size]
		}
		fr.readBuf = make([]byte, size)
		return fr.readBuf
	}
	fr.SetMaxReadFrameSize(maxFrameSize)
	return fr
}

// SetMaxReadFrameSize sets the maximum size of a frame
// that will be read by a subsequent call to ReadFrame.
// It is the caller's responsibility to advertise this
// limit with a SETTINGS frame.
func (fr *ReadFramer) SetMaxReadFrameSize(v uint32) {
	if v > maxFrameSize {
		v = maxFrameSize
	}
	fr.maxReadSize = v
}

// ErrorDetail returns a more detailed error of the last error
// returned by ReadFramer.ReadFrame. For instance, if ReadFrame
// returns a StreamError with code PROTOCOL_ERROR, ErrorDetail
// will say exactly what was invalid. ErrorDetail is not guaranteed
// to return a non-nil value and like the rest of the http2 package,
// its return value is not protected by an API compatibility promise.
// ErrorDetail is reset after the next call to ReadFrame.
func (fr *ReadFramer) ErrorDetail() error {
	return fr.errDetail
}

// terminalReadFrameError reports whether err is an unrecoverable
// error from ReadFrame and no other frames should be read.
func terminalReadFrameError(err error) bool {
	if _, ok := err.(http2.StreamError); ok {
		return false
	}
	return err != nil
}

func (fr *ReadFramer) readFull(r io.Reader, buf []byte, isHeader bool) (n int, err error) {
	minLen := len(buf)
	// if len(buf) < min {
	// 	return 0, io.ErrShortBuffer
	// }
	rbuf := fr.rbuf
	if s := minLen - cap(rbuf); s > 0 {
		rbuf = append(rbuf[:cap(rbuf)], make([]byte, s)...)
		fr.rbuf = rbuf
	}
	rbuf = rbuf[:minLen]

	for n < minLen && err == nil {
		var nn int
		a := rbuf[n:]
		nn, err = r.Read(a)
		n += nn

		if _, ok := err.(ErrTemporaryTimeout); ok {
			slog.Debug("readFull: " + err.Error())
			return 0, err
		}
	}
	if n >= minLen {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	if copy(buf, rbuf) != minLen {
		panic("")
	}

	fr.rbuf = fr.rbuf[:0]
	return
}

// ReadFrame reads a single frame. The returned Frame is only valid
// until the next call to ReadFrame.
//
// If the frame is larger than previously set with SetMaxReadFrameSize, the
// returned error is ErrFrameTooLarge. Other errors may be of type
// ConnectionError, StreamError, or anything else from the underlying
// reader.
//
// If ReadFrame returns an error and a non-nil Frame, the Frame's StreamID
// indicates the stream responsible for the error.
func (fr *ReadFramer) ReadFrame() (Frame, error) {
	if fr.avoidIoLoop {
		return fr.readFrame_avoidLoop()
	}
	return fr.readFrame_std()
}
func (fr *ReadFramer) readFrame_std() (Frame, error) {
	fr.errDetail = nil
	if fr.lastFrame != nil {
		fr.lastFrame.invalidate()
	}
	fh, err := readFrameHeader(fr.headerBuf[:], fr.r)
	if err != nil {
		return nil, err
	}
	if fh.Length > fr.maxReadSize {
		if fh == invalidHTTP1LookingFrameHeader() {
			return nil, fmt.Errorf("http2: failed reading the frame payload: %w, note that the frame header looked like an HTTP/1.1 header", err)
		}
		return nil, ErrFrameTooLarge
	}
	payload := fr.getReadBuf(fh.Length)
	if _, err := io.ReadFull(fr.r, payload); err != nil {
		if fh == invalidHTTP1LookingFrameHeader() {
			return nil, fmt.Errorf("http2: failed reading the frame payload: %w, note that the frame header looked like an HTTP/1.1 header", err)
		}
		return nil, err
	}
	f, err := typeFrameParser(fh.Type)(fr.frameCache, fh, fr.countError, payload)
	if err != nil {
		if ce, ok := err.(connError); ok {
			return nil, fr.connError(ce.Code, ce.Reason)
		}
		return nil, err
	}
	if err := fr.checkFrameOrder(f); err != nil {
		return nil, err
	}
	if fr.logReads {
		fr.debugReadLoggerf("http2: Framer %p: read %v", fr, summarizeFrame(f))
	}
	if fh.Type == http2.FrameHeaders && fr.ReadMetaHeaders != nil {
		return fr.readMetaFrame(f.(*HeadersFrame))
	}
	return f, nil
}

func (fr *ReadFramer) readFrame_avoidLoop() (Frame, error) {
	fr.errDetail = nil
	if fr.lastFrame != nil {
		fr.lastFrame.invalidate()
	}

	var err error
	if !fr.headersReaden {
		fr.header, err = fr.readFrameHeader_avoidLoop(fr.headerBuf[:], fr.r)
		if err != nil {
			return nil, err
		}
		if fr.header.Length > fr.maxReadSize {
			if fr.header == invalidHTTP1LookingFrameHeader() {
				return nil, fmt.Errorf("http2: failed reading the frame payload: %w, note that the frame header looked like an HTTP/1.1 header", err)
			}
			slog.Info("frame too large in ReadFramer", "len", fr.header.Length, "max", fr.maxReadSize)
			return nil, ErrFrameTooLarge
		}
		fr.headersReaden = true
	}

	payload := fr.getReadBuf(fr.header.Length)
	if _, err := fr.readFull(fr.r, payload, false); err != nil {
		if fr.header == invalidHTTP1LookingFrameHeader() {
			return nil, fmt.Errorf("http2: failed reading the frame payload: %w, note that the frame header looked like an HTTP/1.1 header", err)
		}
		return nil, err
	}
	fr.headersReaden = false

	f, err := typeFrameParser(fr.header.Type)(fr.frameCache, fr.header, fr.countError, payload)
	if err != nil {
		if ce, ok := err.(connError); ok {
			return nil, fr.connError(ce.Code, ce.Reason)
		}
		return nil, err
	}
	if err := fr.checkFrameOrder(f); err != nil {
		return nil, err
	}
	if fr.logReads {
		fr.debugReadLoggerf("http2: ReadFramer %p: read %v", fr, summarizeFrame(f))
	}
	if fr.header.Type == http2.FrameHeaders && fr.ReadMetaHeaders != nil {
		return fr.readMetaFrame(f.(*HeadersFrame))
	}

	return f, nil
}

func (fr *ReadFramer) readFrameHeader_avoidLoop(buf []byte, r io.Reader) (FrameHeader, error) {
	_, err := fr.readFull(r, buf[:frameHeaderLen], true)
	if err != nil {
		slog.Warn("read frame header: readFull", "err", err)
		return FrameHeader{}, err
	}
	return FrameHeader{
		FrameHeader: http2.FrameHeader{
			Length:   (uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])),
			Type:     http2.FrameType(buf[3]),
			Flags:    http2.Flags(buf[4]),
			StreamID: binary.BigEndian.Uint32(buf[5:]) & (1<<31 - 1),
		},
		valid: true,
	}, nil
}

// connError returns ConnectionError(code) but first
// stashes away a public reason to the caller can optionally relay it
// to the peer before hanging up on them. This might help others debug
// their implementations.
func (fr *ReadFramer) connError(code http2.ErrCode, reason string) error {
	fr.errDetail = errors.New(reason)
	slog.Error("ReadFramer.connError", "code", code, "reason", reason)
	return http2.ConnectionError(code)
}

// checkFrameOrder reports an error if f is an invalid frame to return
// next from ReadFrame. Mostly it checks whether HEADERS and
// CONTINUATION frames are contiguous.
func (fr *ReadFramer) checkFrameOrder(f frameInvalidate) error {
	last := fr.lastFrame
	fr.lastFrame = f
	if fr.AllowIllegalReads {
		return nil
	}

	fh := f.Header()
	if fr.lastHeaderStream != 0 {
		if fh.Type != http2.FrameContinuation {
			return fr.connError(http2.ErrCodeProtocol,
				fmt.Sprintf("got %s for stream %d; expected CONTINUATION following %s for stream %d",
					fh.Type, fh.StreamID,
					last.Header().Type, fr.lastHeaderStream))
		}
		if fh.StreamID != fr.lastHeaderStream {
			return fr.connError(http2.ErrCodeProtocol,
				fmt.Sprintf("got CONTINUATION for stream %d; expected stream %d",
					fh.StreamID, fr.lastHeaderStream))
		}
	} else if fh.Type == http2.FrameContinuation {
		return fr.connError(http2.ErrCodeProtocol, fmt.Sprintf("unexpected CONTINUATION for stream %d", fh.StreamID))
	}

	switch fh.Type {
	case http2.FrameHeaders, http2.FrameContinuation:
		if fh.Flags.Has(http2.FlagHeadersEndHeaders) {
			fr.lastHeaderStream = 0
		} else {
			fr.lastHeaderStream = fh.StreamID
		}
	}

	return nil
}

// WriteData writes a DATA frame.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility not to violate the maximum frame size
// and to not call other Write methods concurrently.
func (f *ReadFramer) WriteData(streamID uint32, endStream bool, data []byte) error {
	return f.WriteDataPadded(streamID, endStream, data, nil)
}

// WriteDataPadded writes a DATA frame with optional padding.
//
// If pad is nil, the padding bit is not sent.
// The length of pad must not exceed 255 bytes.
// The bytes of pad must all be zero, unless f.AllowIllegalWrites is set.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility not to violate the maximum frame size
// and to not call other Write methods concurrently.
func (f *ReadFramer) WriteDataPadded(streamID uint32, endStream bool, data, pad []byte) error {
	if err := f.startWriteDataPadded(streamID, endStream, data, pad); err != nil {
		return err
	}
	return f.endWrite()
}

// startWriteDataPadded is WriteDataPadded, but only writes the frame to the ReadFramer's internal buffer.
// The caller should call endWrite to flush the frame to the underlying writer.
func (f *ReadFramer) startWriteDataPadded(streamID uint32, endStream bool, data, pad []byte) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	if len(pad) > 0 {
		if len(pad) > 255 {
			return errPadLength
		}
		if !f.AllowIllegalWrites {
			for _, b := range pad {
				if b != 0 {
					// "Padding octets MUST be set to zero when sending."
					return errPadBytes
				}
			}
		}
	}
	var flags http2.Flags
	if endStream {
		flags |= http2.FlagDataEndStream
	}
	if pad != nil {
		flags |= http2.FlagDataPadded
	}
	f.startWrite(FrameData, flags, streamID)
	if pad != nil {
		f.wbuf = append(f.wbuf, byte(len(pad)))
	}
	f.wbuf = append(f.wbuf, data...)
	f.wbuf = append(f.wbuf, pad...)
	return nil
}

// WriteSettings writes a SETTINGS frame with zero or more settings
// specified and the ACK bit not set.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WriteSettings(settings ...http2.Setting) error {
	f.startWrite(FrameSettings, 0, 0)
	for _, s := range settings {
		f.writeUint16(uint16(s.ID))
		f.writeUint32(s.Val)
	}
	return f.endWrite()
}

// WriteSettingsAck writes an empty SETTINGS frame with the ACK bit set.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WriteSettingsAck() error {
	f.startWrite(FrameSettings, http2.FlagSettingsAck, 0)
	return f.endWrite()
}

func (f *ReadFramer) WritePing(ack bool, data [8]byte) error {
	var flags http2.Flags
	if ack {
		flags = http2.FlagPingAck
	}
	f.startWrite(FramePing, flags, 0)
	f.writeBytes(data[:])
	return f.endWrite()
}

func parseGoAwayFrame(_ *frameCache, fh FrameHeader, countError func(string), p []byte) (frameInvalidate, error) {
	if fh.StreamID != 0 {
		countError("frame_goaway_has_stream")
		return nil, http2.ConnectionError(http2.ErrCodeProtocol)
	}
	if len(p) < 8 {
		countError("frame_goaway_short")
		return nil, http2.ConnectionError(http2.ErrCodeFrameSize)
	}
	return &GoAwayFrame{
		FrameHeader:  fh,
		LastStreamID: binary.BigEndian.Uint32(p[:4]) & (1<<31 - 1),
		ErrCode:      http2.ErrCode(binary.BigEndian.Uint32(p[4:8])),
		debugData:    p[8:],
	}, nil
}

func (f *ReadFramer) WriteGoAway(maxStreamID uint32, code http2.ErrCode, debugData []byte) error {
	f.startWrite(FrameGoAway, 0, 0)
	f.writeUint32(maxStreamID & (1<<31 - 1))
	f.writeUint32(uint32(code))
	f.writeBytes(debugData)
	return f.endWrite()
}

// WriteWindowUpdate writes a WINDOW_UPDATE frame.
// The increment value must be between 1 and 2,147,483,647, inclusive.
// If the Stream ID is zero, the window update applies to the
// connection as a whole.
func (f *ReadFramer) WriteWindowUpdate(streamID, incr uint32) error {
	// "The legal range for the increment to the flow control window is 1 to 2^31-1 (2,147,483,647) octets."
	if (incr < 1 || incr > 2147483647) && !f.AllowIllegalWrites {
		return errors.New("illegal window increment value")
	}
	f.startWrite(FrameWindowUpdate, 0, streamID)
	f.writeUint32(incr)
	return f.endWrite()
}

// WriteHeaders writes a single HEADERS frame.
//
// This is a low-level header writing method. Encoding headers and
// splitting them into any necessary CONTINUATION frames is handled
// elsewhere.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WriteHeaders(p HeadersFrameParam) error {
	if !validStreamID(p.StreamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags http2.Flags
	if p.PadLength != 0 {
		flags |= http2.FlagHeadersPadded
	}
	if p.EndStream {
		flags |= http2.FlagHeadersEndStream
	}
	if p.EndHeaders {
		flags |= http2.FlagHeadersEndHeaders
	}
	if !p.Priority.IsZero() {
		flags |= http2.FlagHeadersPriority
	}
	f.startWrite(FrameHeaders, flags, p.StreamID)
	if p.PadLength != 0 {
		f.writeByte(p.PadLength)
	}
	if !p.Priority.IsZero() {
		v := p.Priority.StreamDep
		if !validStreamIDOrZero(v) && !f.AllowIllegalWrites {
			return errDepStreamID
		}
		if p.Priority.Exclusive {
			v |= 1 << 31
		}
		f.writeUint32(v)
		f.writeByte(p.Priority.Weight)
	}
	f.wbuf = append(f.wbuf, p.BlockFragment...)
	f.wbuf = append(f.wbuf, padZeros[:p.PadLength]...)
	return f.endWrite()
}

// WritePriority writes a PRIORITY frame.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WritePriority(streamID uint32, p PriorityParam) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	if !validStreamIDOrZero(p.StreamDep) {
		return errDepStreamID
	}
	f.startWrite(FramePriority, 0, streamID)
	v := p.StreamDep
	if p.Exclusive {
		v |= 1 << 31
	}
	f.writeUint32(v)
	f.writeByte(p.Weight)
	return f.endWrite()
}

// WriteRSTStream writes a RST_STREAM frame.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WriteRSTStream(streamID uint32, code http2.ErrCode) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	f.startWrite(FrameRSTStream, 0, streamID)
	f.writeUint32(uint32(code))
	return f.endWrite()
}

// WriteContinuation writes a CONTINUATION frame.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WriteContinuation(streamID uint32, endHeaders bool, headerBlockFragment []byte) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags http2.Flags
	if endHeaders {
		flags |= http2.FlagContinuationEndHeaders
	}
	f.startWrite(FrameContinuation, flags, streamID)
	f.wbuf = append(f.wbuf, headerBlockFragment...)
	return f.endWrite()
}

// WritePushPromise writes a single PushPromise Frame.
//
// As with Header Frames, This is the low level call for writing
// individual frames. Continuation frames are handled elsewhere.
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility to not call other Write methods concurrently.
func (f *ReadFramer) WritePushPromise(p PushPromiseParam) error {
	if !validStreamID(p.StreamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags http2.Flags
	if p.PadLength != 0 {
		flags |= http2.FlagPushPromisePadded
	}
	if p.EndHeaders {
		flags |= http2.FlagPushPromiseEndHeaders
	}
	f.startWrite(FramePushPromise, flags, p.StreamID)
	if p.PadLength != 0 {
		f.writeByte(p.PadLength)
	}
	if !validStreamID(p.PromiseID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	f.writeUint32(p.PromiseID)
	f.wbuf = append(f.wbuf, p.BlockFragment...)
	f.wbuf = append(f.wbuf, padZeros[:p.PadLength]...)
	return f.endWrite()
}

// WriteRawFrame writes a raw frame. This can be used to write
// extension frames unknown to this package.
func (f *ReadFramer) WriteRawFrame(t FrameType, flags http2.Flags, streamID uint32, payload []byte) error {
	f.startWrite(t, flags, streamID)
	f.writeBytes(payload)
	return f.endWrite()
}

func (fr *ReadFramer) maxHeaderStringLen() int {
	v := int(fr.maxHeaderListSize())
	if v < 0 {
		// If maxHeaderListSize overflows an int, use no limit (0).
		return 0
	}
	return v
}

// readMetaFrame returns 0 or more CONTINUATION frames from fr and
// merge them into the provided hf and returns a MetaHeadersFrame
// with the decoded hpack values.
func (fr *ReadFramer) readMetaFrame(hf *HeadersFrame) (Frame, error) {
	if fr.AllowIllegalReads {
		return nil, errors.New("illegal use of AllowIllegalReads with ReadMetaHeaders")
	}
	mh := &MetaHeadersFrame{
		HeadersFrame: hf,
	}
	var remainSize = fr.maxHeaderListSize()
	var sawRegular bool

	var invalid error // pseudo header field errors
	hdec := fr.ReadMetaHeaders
	hdec.SetEmitEnabled(true)
	hdec.SetMaxStringLength(fr.maxHeaderStringLen())
	hdec.SetEmitFunc(func(hf hpack.HeaderField) {
		if http2.VerboseLogs && fr.logReads {
			fr.debugReadLoggerf("http2: decoded hpack field %+v", hf)
		}
		if !httpguts.ValidHeaderFieldValue(hf.Value) {
			// Don't include the value in the error, because it may be sensitive.
			invalid = headerFieldValueError(hf.Name)
		}
		isPseudo := strings.HasPrefix(hf.Name, ":")
		if isPseudo {
			if sawRegular {
				invalid = errPseudoAfterRegular
			}
		} else {
			sawRegular = true
			if !validWireHeaderFieldName(hf.Name) {
				invalid = headerFieldNameError(hf.Name)
			}
		}

		if invalid != nil {
			hdec.SetEmitEnabled(false)
			return
		}

		size := hf.Size()
		if size > remainSize {
			hdec.SetEmitEnabled(false)
			mh.Truncated = true
			remainSize = 0
			return
		}
		remainSize -= size

		mh.Fields = append(mh.Fields, hf)
	})
	// Lose reference to MetaHeadersFrame:
	defer hdec.SetEmitFunc(func(hf hpack.HeaderField) {})

	var hc headersOrContinuation = hf
	for {
		frag := hc.HeaderBlockFragment()

		// Avoid parsing large amounts of headers that we will then discard.
		// If the sender exceeds the max header list size by too much,
		// skip parsing the fragment and close the connection.
		//
		// "Too much" is either any CONTINUATION frame after we've already
		// exceeded the max header list size (in which case remainSize is 0),
		// or a frame whose encoded size is more than twice the remaining
		// header list bytes we're willing to accept.
		if int64(len(frag)) > int64(2*remainSize) {
			if http2.VerboseLogs {
				log.Printf("http2: header list too large")
			}
			// It would be nice to send a RST_STREAM before sending the GOAWAY,
			// but the structure of the server's frame writer makes this difficult.
			return mh, http2.ConnectionError(http2.ErrCodeProtocol)
		}

		// Also close the connection after any CONTINUATION frame following an
		// invalid header, since we stop tracking the size of the headers after
		// an invalid one.
		if invalid != nil {
			if http2.VerboseLogs {
				log.Printf("http2: invalid header: %v", invalid)
			}
			// It would be nice to send a RST_STREAM before sending the GOAWAY,
			// but the structure of the server's frame writer makes this difficult.
			return mh, http2.ConnectionError(http2.ErrCodeProtocol)
		}

		if _, err := hdec.Write(frag); err != nil {
			return mh, http2.ConnectionError(http2.ErrCodeCompression)
		}

		if hc.HeadersEnded() {
			break
		}
		if f, err := fr.ReadFrame(); err != nil {
			return nil, err
		} else {
			hc = f.(*ContinuationFrame) // guaranteed by checkFrameOrder
		}
	}

	mh.HeadersFrame.headerFragBuf = nil
	mh.HeadersFrame.invalidate()

	if err := hdec.Close(); err != nil {
		return mh, http2.ConnectionError(http2.ErrCodeCompression)
	}
	if invalid != nil {
		fr.errDetail = invalid
		if http2.VerboseLogs {
			log.Printf("http2: invalid header: %v", invalid)
		}
		return nil, http2.StreamError{mh.StreamID, http2.ErrCodeProtocol, invalid}
	}
	if err := mh.checkPseudos(); err != nil {
		fr.errDetail = err
		if http2.VerboseLogs {
			log.Printf("http2: invalid pseudo headers: %v", err)
		}
		return nil, http2.StreamError{mh.StreamID, http2.ErrCodeProtocol, err}
	}
	return mh, nil
}
