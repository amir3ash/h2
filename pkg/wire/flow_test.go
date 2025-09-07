// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Flow control

package wire

import (
	"math/rand"
	"testing"
)

func TestFlow(t *testing.T) {
	t.Run("available", func(t *testing.T) {
		tests := []int32{0, 1, 1 << 16, 1<<31 - 1}
		for _, v := range tests {
			f := flow(rand.Uint64() & (1<<62 - 1))
			oldUnset := f.unset()
			f = f.setAvailable(v)

			if got := f.available(); got != v {
				t.Errorf("unequal availablity, got=%b, expected=%d", got, v)
			}

			if oldUnset != f.unset() {
				t.Errorf("unset modified, old=%b, now=%b", oldUnset, f.unset())
			}
		}
	})

	t.Run("unset", func(t *testing.T) {
		tests := []int32{0, 1, 1 << 16, 1<<31 - 1}
		for _, v := range tests {
			f := flow(rand.Uint64() & (1<<62 - 1))
			oldAvailable := f.available()
			f = f.setUnset(v)

			if got := f.unset(); got != v {
				t.Errorf("unequal unset, got=%d, expected=%d", got, v)
			}

			if oldAvailable != f.available() {
				t.Error("available modified")
			}
		}
	})
}

func Test_inflow_add(t *testing.T) {
	windowSize := uint32(65535)
	maxAllowd := int32(1<<31 - windowSize - 1)

	tests := []struct {
		name        string
		n           int32
		wantConnAdd int32
		expectPanic bool
	}{
		{"normal", 1 << 13, 1 << 13, false},
		{"min-refresh", 4095, 0, false},
		{"zero", 0, 0, false},
		{"max", 1<<31 - 1, 1<<31 - 1, true},
		{"max-allowed", maxAllowd, maxAllowd, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				e := recover()
				if (e == nil) == tt.expectPanic {
					t.Errorf("expectPanic=%v but err=%v", tt.expectPanic, e)
				}
			}()

			f := &inflow{}
			f.init(int32(windowSize))

			if gotConnAdd := f.add(tt.n); gotConnAdd != tt.wantConnAdd {
				t.Errorf("inflow.add() = %v, want %v", gotConnAdd, tt.wantConnAdd)
			}

		})
	}
}

func Test_inflow_take(t *testing.T) {
	tests := []struct {
		name          string
		init          int32
		takeN         uint32
		want          bool
		wantAvailable int32
	}{
		{"zero-from-zero", 0, 0, true, 0},
		{"equal", 65535, 65535, true, 0},
		{"take-extra", 4000, 4001, false, 4000},
		{"normal", 500, 100, true, 400},
		{"max-from-zero", 0, 1<<32 - 1, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &inflow{}
			f.init(tt.init)

			if got := f.take(tt.takeN); got != tt.want {
				t.Errorf("inflow.take() = %v, want %v", got, tt.want)
			}

			if available := flow(f.atom.Load()).available(); available != tt.wantAvailable {
				t.Errorf("available = %d, expected = %d", available, tt.wantAvailable)
			}
		})
	}
}

func TestInFlowTake(t *testing.T) {
	var f inflow
	f.init(100)
	if !f.take(40) {
		t.Fatalf("f.take(40) from 100: got false, want true")
	}
	if !f.take(40) {
		t.Fatalf("f.take(40) from 60: got false, want true")
	}
	if f.take(40) {
		t.Fatalf("f.take(40) from 20: got true, want false")
	}
	if !f.take(20) {
		t.Fatalf("f.take(20) from 20: got false, want true")
	}
}

func TestInflowAddSmall(t *testing.T) {
	var f inflow
	f.init(0)
	// Adding even a small amount when there is no flow causes an immediate send.
	if got, want := f.add(1), int32(1); got != want {
		t.Fatalf("f.add(1) to 1 = %v, want %v", got, want)
	}
}

func TestInflowAdd(t *testing.T) {
	var f inflow
	f.init(10 * inflowMinRefresh)
	if got, want := f.add(inflowMinRefresh-1), int32(0); got != want {
		t.Fatalf("f.add(minRefresh - 1) = %v, want %v", got, want)
	}
	if got, want := f.add(1), int32(inflowMinRefresh); got != want {
		t.Fatalf("f.add(minRefresh) = %v, want %v", got, want)
	}
}

func TestTakeInflows(t *testing.T) {
	var a, b inflow
	a.init(10)
	b.init(20)
	if !takeInflows(&a, &b, 5) {
		t.Fatalf("takeInflows(a, b, 5) from 10, 20: got false, want true")
	}
	if takeInflows(&a, &b, 6) {
		t.Fatalf("takeInflows(a, b, 6) from 5, 15: got true, want false")
	}
	if !takeInflows(&a, &b, 5) {
		t.Fatalf("takeInflows(a, b, 5) from 5, 15: got false, want true")
	}
}

func TestOutFlow(t *testing.T) {
	var st outflow
	var conn outflow
	st.add(3)
	conn.add(2)

	if got, want := st.available(), int32(3); got != want {
		t.Errorf("available = %d; want %d", got, want)
	}
	st.setConnFlow(&conn)
	if got, want := st.available(), int32(2); got != want {
		t.Errorf("after parent setup, available = %d; want %d", got, want)
	}

	st.take(2)
	if got, want := conn.available(), int32(0); got != want {
		t.Errorf("after taking 2, conn = %d; want %d", got, want)
	}
	if got, want := st.available(), int32(0); got != want {
		t.Errorf("after taking 2, stream = %d; want %d", got, want)
	}
}

func TestOutFlowAdd(t *testing.T) {
	var f outflow
	if !f.add(1) {
		t.Fatal("failed to add 1")
	}
	if !f.add(-1) {
		t.Fatal("failed to add -1")
	}
	if got, want := f.available(), int32(0); got != want {
		t.Fatalf("size = %d; want %d", got, want)
	}
	if !f.add(1<<31 - 1) {
		t.Fatal("failed to add 2^31-1")
	}
	if got, want := f.available(), int32(1<<31-1); got != want {
		t.Fatalf("size = %d; want %d", got, want)
	}
	if f.add(1) {
		t.Fatal("adding 1 to max shouldn't be allowed")
	}
}

func TestOutFlowAddOverflow(t *testing.T) {
	var f outflow
	if !f.add(0) {
		t.Fatal("failed to add 0")
	}
	if !f.add(-1) {
		t.Fatal("failed to add -1")
	}
	if !f.add(0) {
		t.Fatal("failed to add 0")
	}
	if !f.add(1) {
		t.Fatal("failed to add 1")
	}
	if !f.add(1) {
		t.Fatal("failed to add 1")
	}
	if !f.add(0) {
		t.Fatal("failed to add 0")
	}
	if !f.add(-3) {
		t.Fatal("failed to add -3")
	}
	if got, want := f.available(), int32(-2); got != want {
		t.Fatalf("size = %d; want %d", got, want)
	}
	if !f.add(1<<31 - 1) {
		t.Fatal("failed to add 2^31-1")
	}
	if got, want := f.available(), int32(1+-3+(1<<31-1)); got != want {
		t.Fatalf("size = %d; want %d", got, want)
	}

}

func FuzzInflow(f *testing.F) {
	checkEqual := func(t *testing.T, f *inflow, expected *inflowTest) {
		t.Helper()

		if f.unsent() != expected.unsent {
			t.Errorf("unsets are not equal, got %d, expected=%d", f.unsent(), expected.unsent)
		}

		if f.available() != expected.avail {
			t.Errorf("availables are not equal, got %d, expected=%d", f.available(), expected.avail)
		}
	}

	f.Fuzz(func(t *testing.T, arg uint64, n int32, take uint32) {
		if n < 0 {
			n = -n
		}

		f := &inflow{}
		f.atom.Store(arg & (1<<62 - 1))

		test := &inflowTest{avail: f.available(), unsent: f.unsent()}

		gotAdd := f.add(n)
		expectedAdd := test.add(int(n))

		if gotAdd != expectedAdd {
			t.Errorf("inflow.add() returns %d, wanted=%d", gotAdd, expectedAdd)
		}

		checkEqual(t, f, test)

		gotTake := f.take(take)
		wantTake := test.take(take)
		if gotTake != wantTake {
			t.Errorf("unequal takes, got=%v, want=%v", gotTake, wantTake)
		}

		checkEqual(t, f, test)
	})
}

// inflow accounts for an inbound flow control window.
// It tracks both the latest window sent to the peer (used for enforcement)
// and the accumulated unsent window.
type inflowTest struct {
	avail  int32
	unsent int32
}

// init sets the initial window.
func (f *inflowTest) init(n int32) {
	f.avail = n
}

// add adds n bytes to the window, with a maximum window size of max,
// indicating that the peer can now send us more data.
// For example, the user read from a {Request,Response} body and consumed
// some of the buffered data, so the peer can now send more.
// It returns the number of bytes to send in a WINDOW_UPDATE frame to the peer.
// Window updates are accumulated and sent when the unsent capacity
// is at least inflowMinRefresh or will at least double the peer's available window.
func (f *inflowTest) add(n int) (connAdd int32) {
	if n < 0 {
		panic("negative update")
	}
	unsent := int64(f.unsent) + int64(n)
	// "A sender MUST NOT allow a flow-control window to exceed 2^31-1 octets."
	// RFC 7540 Section 6.9.1.
	const maxWindow = 1<<31 - 1
	if unsent+int64(f.avail) > maxWindow {
		panic("flow control update exceeds maximum window size")
	}
	f.unsent = int32(unsent)
	if f.unsent < inflowMinRefresh && f.unsent < f.avail {
		// If there aren't at least inflowMinRefresh bytes of window to send,
		// and this update won't at least double the window, buffer the update for later.
		return 0
	}
	f.avail += f.unsent
	f.unsent = 0
	return int32(unsent)
}

// take attempts to take n bytes from the peer's flow control window.
// It reports whether the window has available capacity.
func (f *inflowTest) take(n uint32) bool {
	if n > uint32(f.avail) {
		return false
	}
	f.avail -= int32(n)
	return true
}
