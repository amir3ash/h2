// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Flow control

package wire

import (
	"sync"
	"sync/atomic"
)

// inflowMinRefresh is the minimum number of bytes we'll send for a
// flow control window update.
const inflowMinRefresh = 4 << 10

// "A sender MUST NOT allow a flow-control window to exceed 2^31-1 octets."
// RFC 7540 Section 6.9.1.
const maxWindow = 1<<31 - 1

type flow uint64 // [ 2bit pad | 31bit unset | 31bit available ]

func (f flow) available() int32 {
	return int32(f & maxWindow)
}
func (f flow) unset() int32 {
	return int32(f >> 31)
}
func (f flow) setAvailable(n int32) flow {
	return (f & (maxWindow << 31)) | flow(n)
}
func (f flow) setUnset(n int32) flow {
	return (flow(n) << 31) | (f & maxWindow)
}
func (f flow) uint64() uint64 {
	return uint64(f)
}

// inflow accounts for an inbound flow control window.
// It tracks both the latest window sent to the peer (used for enforcement)
// and the accumulated unsent window.
type inflow struct {
	// mu     sync.Mutex
	// avail  int32
	// unsent int32

	atom atomic.Uint64
}

// init sets the initial window.
func (f *inflow) init(n int32) {
	f.atom.Store(uint64(n))
}

// add adds n bytes to the window, with a maximum window size of max,
// indicating that the peer can now send us more data.
// For example, the user read from a {Request,Response} body and consumed
// some of the buffered data, so the peer can now send more.
// It returns the number of bytes to send in a WINDOW_UPDATE frame to the peer.
// Window updates are accumulated and sent when the unsent capacity
// is at least inflowMinRefresh or will at least double the peer's available window.
func (f *inflow) add(n int32) (connAdd int32) {
	if n < 0 {
		panic("negative update")
	}

begin:
	atom := flow(f.atom.Load())
	newAtom := atom.setUnset(n + atom.unset())
	newUnsent := newAtom.unset()
	avail := newAtom.available()

	if int64(newUnsent)+int64(avail) > maxWindow {
		panic("flow control update exceeds maximum window size")
	}

	if newUnsent < inflowMinRefresh && newUnsent < avail {
		// If there aren't at least inflowMinRefresh bytes of window to send,
		// and this update won't at least double the window, buffer the update for later.
		if !f.atom.CompareAndSwap(atom.uint64(), newAtom.uint64()) {
			goto begin
		}
		return 0
	}

	newAtom = atom.setAvailable(atom.available() + newUnsent)
	newAtom = newAtom.setUnset(0)

	if !f.atom.CompareAndSwap(atom.uint64(), newAtom.uint64()) {
		goto begin
	}
	return newUnsent
}

// take attempts to take n bytes from the peer's flow control window.
// It reports whether the window has available capacity.
func (f *inflow) take(n uint32) bool {
	for {
		atom := flow(f.atom.Load())
		if n > uint32(atom.available()) {
			return false
		}

		newAtom := atom.setAvailable(atom.available() - int32(n))
		if f.atom.CompareAndSwap(atom.uint64(), newAtom.uint64()) {
			return true
		}
	}
}

func (f *inflow) unsent() int32 {
	atom := flow(f.atom.Load())
	return atom.unset()
}
func (f *inflow) available() int32 {
	atom := flow(f.atom.Load())
	return atom.available()
}

// takeInflows attempts to take n bytes from two inflows,
// typically connection-level and stream-level flows.
// It reports whether both windows have available capacity.
func takeInflows(conn, stream *inflow, n uint32) bool {
	for {
		atomC := flow(conn.atom.Load())   // conn-level
		atomS := flow(stream.atom.Load()) // stream-level

		if n > uint32(atomC.available()) || n > uint32(atomS.available()) {
			return false
		}

		newAtomC := atomC.setAvailable(atomC.available() - int32(n))
		if !conn.atom.CompareAndSwap(atomC.uint64(), newAtomC.uint64()) {
			continue
		}

		newAtomS := atomS.setAvailable(atomS.available() - int32(n))
		if !stream.atom.CompareAndSwap(atomS.uint64(), newAtomS.uint64()) {
			panic("internal: race while taking")
		}
		return true
	}
}

// TODO: check for race condition
// outflow is the outbound flow control window's size.
type outflow struct {
	// _ incomparable

	// n is the number of DATA bytes we're allowed to send.
	// An outflow is kept both on a conn and a per-stream.
	n  int32
	mu sync.Mutex
	// conn points to the shared connection-level outflow that is
	// shared by all streams on that conn. It is nil for the outflow
	// that's on the conn directly.
	conn *outflow
}

func (f *outflow) setConnFlow(cf *outflow) { f.conn = cf }

func (f *outflow) available() int32 {
	f.mu.Lock()
	n := f.n
	if f.conn != nil {
		f.conn.mu.Lock()
		if f.conn.n < n {
			n = f.conn.n
		}
		f.conn.mu.Unlock()
	}
	f.mu.Unlock()
	return n
}

func (f *outflow) take(n int32) {
	if n > f.available() {
		panic("internal error: took too much")
	}
	f.n -= n
	if f.conn != nil {
		f.conn.n -= n
	}
}

// add adds n bytes (positive or negative) to the flow control window.
// It returns false if the sum would exceed 2^31-1.
func (f *outflow) add(n int32) bool {
	sum := f.n + n
	if (sum > n) == (f.n > 0) {
		f.n = sum
		return true
	}
	return false
}
