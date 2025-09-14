package http8

import (
	"io"
	"log/slog"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amir3ash/h2/pkg/internal"
	h2 "github.com/amir3ash/h2/pkg/wire"
	"golang.org/x/net/http/httpguts"
	"golang.org/x/net/http2/hpack"
)

var _ http.ResponseWriter = &res{}

// requestBody is the Handler's Request.Body type.
// Read and Close may be called concurrently.
type requestBody struct {
	// _             incomparable
	stream        *h2.Stream
	conn          *h2.Connection
	closeOnce     sync.Once           // for use by Close only
	sawEOF        bool                // for use by Read only
	pipe          *internal.BytesPipe // non-nil if we have an HTTP entity message body
	needsContinue bool                // need to send a 100-continue
}

func (b *requestBody) Close() error {
	b.closeOnce.Do(func() {
		if b.pipe != nil {
			// b.pipe.BreakWithError(errClosedBody)
			b.pipe.Close()
		}
	})
	return nil
}

func (b *requestBody) Read(p []byte) (n int, err error) {
	if b.needsContinue {
		b.needsContinue = false
		// b.conn.write100ContinueHeaders(b.stream)
	}
	if b.pipe == nil || b.sawEOF {
		return 0, io.EOF
	}
	n, err = b.pipe.Read(p)
	if err == io.EOF {
		b.sawEOF = true
	}
	// if b.conn == nil && inTests {
	// 	return
	// }
	// TODO: implement inclraese winow size after handler read
	// b.conn.noteBodyReadFromHandler(b.stream, n, err)
	return
}

func (sc *Server) newRes(st *h2.Stream, req *http.Request) *res {
	rw := resPool.Get().(*res)
	bufSave := rw.smallResponseBuf[:0]

	*rw = res{
		stream:           st,
		smallResponseBuf: bufSave,
		isHead:           req.Method == "HEAD",
		header:           make(http.Header),
		logger:           slog.Default(),
	}
	return rw
}

type res struct {
	stream *h2.Stream

	header      http.Header
	trailersMap map[string]struct{}
	status      int // status code passed to WriteHeader

	// for responses smaller than maxSmallResponseSize, we buffer calls to Write,
	// and automatically add the Content-Length header
	smallResponseBuf []byte

	contentLen     int64 // if handler set valid Content-Length header
	numWritten     int64 // bytes written
	headerComplete bool  // set once WriteHeader is called with a status code >= 200
	headerWritten  bool  // set once the response header has been serialized to the stream
	isHead         bool
	trailerWritten bool // set once the response trailers has been serialized to the stream

	hijacked bool
	logger   *slog.Logger

	handlerFinished bool
}

var _ http.ResponseController

// Header implements http.ResponseWriter.
func (r *res) Header() http.Header {
	switch ss := r.stream.State(); ss {
	case h2.OpenState, h2.HalfClosedRemoteState:
		break
	case h2.ClosedState:
		panic("WriteHeader called after Handler finished")
	default:
		panic("unnormal stream state " + ss.String())
	}

	return r.header
}

// WriteHeader implements http.ResponseWriter.
func (r *res) WriteHeader(statusCode int) {
	if r.headerComplete {
		return
	}

	if r.stream.State() == h2.ClosedState {
		panic("WriteHeader called after Handler finished")
	}

	if statusCode < 100 || statusCode > 999 {
		panic("invalid WriteHeader code " + strconv.Itoa(statusCode))
	}
	r.status = statusCode
	// We're done with headers once we write a status >= 200.
	r.headerComplete = true
	// Add Date header.
	// This is what the standard library does.
	// Can be disabled by setting the Date header to nil.
	if _, ok := r.header["Date"]; !ok {
		r.header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Content-Length checking
	// use ParseUint instead of ParseInt, as negative values are invalid
	if clen := r.header.Get("Content-Length"); clen != "" {
		if cl, err := strconv.ParseUint(clen, 10, 63); err == nil {
			r.contentLen = int64(cl)
		} else {
			// emit a warning for malformed Content-Length and remove it
			// logger := r.logger
			// if logger == nil {
			// 	logger = slog.Default()
			// }
			slog.Error("Malformed Content-Length", "value", clen)
			r.header.Del("Content-Length")
		}
	}

}

// Write implements http.ResponseWriter.
func (w *res) Write(p []byte) (int, error) {
	bodyAllowed := bodyAllowedForStatus(w.status)
	if !w.headerComplete {
		w.sniffContentType(p)
		w.WriteHeader(http.StatusOK)
		bodyAllowed = true
	}
	if !bodyAllowed {
		return 0, http.ErrBodyNotAllowed
	}

	w.numWritten += int64(len(p))
	if w.contentLen != 0 && w.numWritten > w.contentLen {
		return 0, http.ErrContentLength
	}

	if w.isHead {
		return len(p), nil
	}

	if !w.headerWritten {
		// Buffer small responses.
		// This allows us to automatically set the Content-Length field.
		const maxSmallResponseSize = 4096
		if len(w.smallResponseBuf)+len(p) < maxSmallResponseSize {
			w.smallResponseBuf = append(w.smallResponseBuf, p...)
			return len(p), nil
		}
	}
	return w.doWrite(p)
}
func (w *res) doWrite(p []byte) (int, error) {
	l := uint64(len(w.smallResponseBuf) + len(p))

	if !w.headerWritten {
		w.sniffContentType(w.smallResponseBuf)
		if err := w.writeHeader(w.status, l == 0); err != nil {
			return 0, maybeReplaceError(err)
		}
		w.headerWritten = true
	}

	endStream := w.handlerFinished && len(w.trailersMap) == 0

	if len(w.smallResponseBuf) > 0 {
		endStream := endStream && len(p) == 0
		if _, err := w.stream.WriteAllOfData(w.smallResponseBuf, endStream); err != nil {
			return 0, maybeReplaceError(err)
		}
		w.smallResponseBuf = w.smallResponseBuf[:0]

		if endStream {
			return 0, nil
		}
	}

	var n int
	var err error

	n, err = w.stream.WriteAllOfData(p, endStream)
	if err != nil {
		return n, maybeReplaceError(err)
	}
	return n, nil
}

func (w *res) writeHeader(status int, zeroLenData bool) error {
	headers := make([]hpack.HeaderField, 0, 1)
	headers = append(headers, hpack.HeaderField{Name: ":status", Value: strconv.Itoa(status)})

	// Handle trailer fields
	if vals, ok := w.header["Trailer"]; ok {
		for _, val := range vals {
			for _, trailer := range strings.Split(val, ",") {
				// We need to convert to the canonical header key value here because this will be called when using
				// headers.Add or headers.Set.
				trailer = textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(trailer))
				w.declareTrailer(trailer)
			}
		}
	}

	for k, v := range w.header {
		if _, excluded := w.trailersMap[k]; excluded {
			continue
		}
		// Ignore "Trailer:" prefixed headers
		if strings.HasPrefix(k, http.TrailerPrefix) {
			continue
		}
		for index := range v {
			headers = append(headers, hpack.HeaderField{Name: strings.ToLower(k), Value: v[index]})
		}
	}

	endStream := (w.handlerFinished && len(w.trailersMap) == 0 && zeroLenData) || w.isHead
	err := w.stream.WriteHeaders(headers, h2.PriorityParam{}, endStream)
	return err
}

func (w *res) FlushError() error {
	if !w.headerComplete {
		w.WriteHeader(http.StatusOK)
	}
	_, err := w.doWrite(nil)
	return err
}

func (w *res) flushTrailers() {
	if w.trailerWritten {
		return
	}
	if err := w.writeTrailers(); err != nil {
		w.logger.Debug("could not write trailers", "error", err)
	}
}

func (w *res) Flush() {
	if err := w.FlushError(); err != nil {
		if w.logger != nil {
			w.logger.Debug("could not flush to stream", "error", err)
		}
	}
}
func (w *res) handlerDone() {
	w.handlerFinished = true
	w.Flush()
	w.flushTrailers()
	resPool.Put(w)
}

// declareTrailer adds a trailer to the trailer list, while also validating that the trailer has a
// valid name.
func (w *res) declareTrailer(k string) {
	if !httpguts.ValidTrailerHeader(k) {
		// Forbidden by RFC 9110, section 6.5.1.
		w.logger.Debug("ignoring invalid trailer", slog.String("header", k))
		return
	}
	if w.trailersMap == nil {
		w.trailersMap = make(map[string]struct{})
	}
	w.trailersMap[k] = struct{}{}
}

// hasNonEmptyTrailers checks to see if there are any trailers with an actual
// value set. This is possible by adding trailers to the "Trailers" header
// but never actually setting those names as trailers in the course of handling
// the request. In that case, this check may save us some allocations.
func (w *res) hasNonEmptyTrailers() bool {
	for trailer := range w.trailersMap {
		if _, ok := w.header[trailer]; ok {
			return true
		}
	}
	return false
}

// writeTrailers will write trailers to the stream if there are any.
func (w *res) writeTrailers() error {
	// promote headers added via "Trailer:" convention as trailers, these can be added after
	// streaming the status/headers have been written.
	for k := range w.header {
		// Handle "Trailer:" prefix
		if strings.HasPrefix(k, http.TrailerPrefix) {
			w.declareTrailer(k)
		}
	}

	if !w.hasNonEmptyTrailers() {
		return nil
	}

	trailers := make([]hpack.HeaderField, 0, len(w.trailersMap))
	for trailer := range w.trailersMap {
		trailerName := strings.ToLower(strings.TrimPrefix(trailer, http.TrailerPrefix))
		if vals, ok := w.header[trailer]; ok {
			for _, val := range vals {
				trailers = append(trailers, hpack.HeaderField{Name: trailerName, Value: val})
			}
		}
	}

	err := w.stream.EndWithTrailers(trailers)
	w.trailerWritten = true
	return err
}

func (w *res) sniffContentType(p []byte) {
	// If no content type, apply sniffing algorithm to body.
	// We can't use `w.header.Get` here since if the Content-Type was set to nil, we shouldn't do sniffing.
	_, haveType := w.header["Content-Type"]

	// If the Content-Encoding was set and is non-blank, we shouldn't sniff the body.
	hasCE := w.header.Get("Content-Encoding") != ""
	if !hasCE && !haveType && len(p) > 0 {
		w.header.Set("Content-Type", http.DetectContentType(p))
	}
}

func maybeReplaceError(err error) error {
	return err
}

var resPool = sync.Pool{
	New: func() any {
		rws := &res{}
		rws.smallResponseBuf = make([]byte, 0, 4096)
		return rws
	},
}
