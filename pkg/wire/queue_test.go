package wire

import (
	"bufio"
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestRing(t *testing.T) {
	t.Run("cap", func(t *testing.T) {
		rb := newRing[int](0)
		if c := rb.cap(); c != 0 {
			t.Errorf("cap must be zero, got %d", c)
		}

		rb = newRing[int](4)
		if c := rb.cap(); c != 4 {
			t.Errorf("invalid capacity, got: %d, expected: %d", c, 4)
		}
	})
	t.Run("empty-ring-pull", func(t *testing.T) {
		rb := newRing[int](5)

		_, ok := rb.pull()
		if ok {
			t.Error("ring is not empty")
		}
	})

	t.Run("full-ring-push", func(t *testing.T) {
		rb := newRing[int](5)

		if ok := rb.push(1); !ok {
			t.Error("can not push to ring")
		}

		if _, ok := rb.pull(); !ok {
			t.Errorf("can not pull")
		}

		for range rb.cap() {
			if ok := rb.push(1); !ok {
				t.Errorf("can not push to ring, len=%d", rb.len())
			}
		}

		if pushed := rb.push(1); pushed {
			t.Errorf("must not push to ring, len=%d", rb.len())
		}

	})

	t.Run("push-pull", func(t *testing.T) {
		rb := newRing[int](5)
		if ok := rb.push(1); !ok {
			t.Error("it should push, but failed")
		}

		got, ok := rb.pull()
		if !ok {
			t.Error("ring is empty")
		}
		if got != 1 {
			t.Errorf("ring.pull returns %v, expected: 1", got)
		}
		if l := rb.len(); l != 0 {
			t.Errorf("ring.len is %d, expected: zero", l)
		}

		for i := range rb.cap() {
			if !rb.push(i + 1) {
				t.Fatal("can not push")
			}
		}

		for i := range rb.cap() {
			got, ok := rb.pull()
			if !ok {
				t.Fatal("empty")
			}

			if expected := i + 1; got != expected {
				t.Errorf("not equel, got %v, expected: %v", got, expected)
			}
		}
	})

	t.Run("concurent-push-pull", func(t *testing.T) {
		rb := newRing[int](1023)
		wg := sync.WaitGroup{}

		// pushing
		for range rb.cap() {
			wg.Add(1)

			go func() {
				if ok := rb.push(1); !ok {
					t.Error("it should push, but failed")
				}
				wg.Done()
			}()
		}
		wg.Wait()

		// pulling
		for range rb.cap() {
			wg.Add(1)
			go func() {
				got, ok := rb.pull()
				if !ok {
					t.Error("ring is empty")
				}
				if expected := 1; got != expected {
					t.Errorf("ring.pull returns %v, expected: %v", got, expected)
				}
				wg.Done()
			}()
		}

		wg.Wait()

		if l := rb.len(); l != 0 {
			t.Errorf("ring.len is %d, expected: zero", l)
		}
	})

	t.Run("len", func(t *testing.T) {
		rb := newRing[int](rand.Intn(20) + 5)
		max := cap(rb.buf) - 1
		len := 0

		for i := max; i >= 0; i-- {
			for range i {
				if !rb.push(1) {
					t.Fatal("can not push")
				}
				len++
			}

			for range i - 1 {
				if _, ok := rb.pull(); !ok {
					t.Fatal("can not pull")
				}
				len--
			}

			if len != rb.len() {
				t.Errorf("unexpected len, len=%d, expected=%d", rb.len(), len)
			}
		}

		if rb.len() != max {
			// every run, we pulled one lower than push; no. of runs is max
			t.Errorf("len must be %d, len=%d", max, rb.len())
		}
	})

	t.Run("String", func(t *testing.T) {
		rb := newRing[int](50)
		for i := range 5 {
			rb.push(i)
		}
		if rb.len() != 5 {
			t.Fatal()
		}

		expected := "ring[s=0,e=5]{0, 1, 2, 3, 4}"
		if got := rb.String(); got != expected {
			t.Errorf("invalide Stringer, got: \"%s\", expected: \"%s\"", got, expected)
		}
	})
	t.Run("String-zero", func(t *testing.T) {
		rb := newRing[int](50)

		expected := "ring[s=0,e=0]{}"
		if got := rb.String(); got != expected {
			t.Errorf("invalide Stringer, got: \"%s\", expected: \"%s\"", got, expected)
		}
	})
}

func TestPQ(t *testing.T) {
	newPQ := func() *PQ {
		pq := PQ{
			wire:         nil,
			w:            bufio.NewWriterSize(nil, 256),
			hpackEncoder: nil,
			controlQ:     newExRing[*bytes.Buffer](),
			headerQ:      newExRing[queueHeaderFrame](),
			nodes:        make(map[uint32]*priorityNode),
		}
		pq.addToRunnables = func(p *PQ) {}
		for i := range pq.dataQ {
			lq := newLinkedQ[*priorityNode]()
			pq.dataQ[i] = lq
		}
		return &pq
	}

	randomData := func() *bytes.Buffer {
		b := bufPool.Get()
		b.Write([]byte("testData"))
		return b
	}

	t.Run("enqueue-many-control-frames", func(t *testing.T) {
		pq := newPQ()
		for range 1024 {
			if err := pq.enqueue(byteframe{data: randomData(), control: true}); err != nil {
				t.Errorf("can not enqueue: %v", err)
			}
		}

		err := pq.enqueue(byteframe{data: randomData(), control: true})
		if err != bufio.ErrBufferFull {
			t.Errorf("PQ.enqueue returns %v, expected: %v", err, bufio.ErrBufferFull)
		}
	})

	t.Run("enqueue-many-control-frames-concurent", func(t *testing.T) {
		pq := newPQ()
		wg := sync.WaitGroup{}
		for range 1024 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := pq.enqueue(byteframe{data: randomData(), control: true}); err != nil {
					t.Errorf("can not enqueue: %v", err)
				}
			}()
		}

		wg.Wait()
		err := pq.enqueue(byteframe{data: randomData(), control: true})
		if err != bufio.ErrBufferFull {
			t.Errorf("PQ.enqueue returns %v, expected: %v", err, bufio.ErrBufferFull)
		}
	})

	t.Run("enqueue-many-data-frames", func(t *testing.T) {
		pq := newPQ()
		s := &Stream{}
		pq.createNode(s, PriorityParam{})
		// node := pq.nodes[s.id]
		for range 1024 {
			if err := pq.enqueue(byteframe{data: randomData(), stream: s}); err != nil {
				t.Errorf("can not enqueue: %v", err)
			}
		}

		err := pq.enqueue(byteframe{data: randomData(), stream: s})
		if err != bufio.ErrBufferFull {
			t.Errorf("PQ.enqueue returns %v, expected: %v", err, bufio.ErrBufferFull)
		}
	})

	t.Run("enqueue-many-data-frames-concurent", func(t *testing.T) {
		pq := newPQ()
		s := &Stream{}
		pq.createNode(s, PriorityParam{})
		// node := pq.nodes[s.id]
		wg := sync.WaitGroup{}
		for range 1024 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := pq.enqueue(byteframe{data: randomData(), stream: s}); err != nil {
					t.Errorf("can not enqueue: %v", err)
				}
			}()
		}

		wg.Wait()
		err := pq.enqueue(byteframe{data: randomData(), stream: s})
		if err != bufio.ErrBufferFull {
			t.Errorf("PQ.enqueue returns %v, expected: %v", err, bufio.ErrBufferFull)
		}
	})

	t.Run("findNext", func(t *testing.T) {
		pq := newPQ()
		pq.h2Conn = &Connection{
			idleTimer: time.AfterFunc(0, func() {}),
		}
		s := &Stream{}
		pq.createNode(s, PriorityParam{})
		// node := pq.nodes[s.id]
		for range 5 {
			t.Log("a")
			if err := pq.enqueue(byteframe{data: randomData(), stream: s}); err != nil {
				t.Fatalf("can not enqueue: %v", err)
			}
		}

		for range 5 {
			if buf, _ := pq.findNext(); buf == nil || buf.Len() == 0 {
				t.Fatal("nil buffer")
			}
		}
		if buf, _ := pq.findNext(); buf != nil {
			t.Fatal("none nil")
		}
		// t.Fail()
	})
}

func TestExpandableRing(t *testing.T) {
	expectLen := func(t *testing.T, r *expandableRing[int], expectedLen int, msg string) (ok bool) {
		t.Helper()
		if l := r.len(); l != expectedLen {
			t.Errorf("invalid len: %s: want=%d, got=%d", msg, expectedLen, l)
			return false
		}
		return true
	}

	t.Run("cap", func(t *testing.T) {
		rb := newExRing[int]()
		if c := rb.cap(); c != 4 {
			t.Errorf("invalid initialized capacity, got: %d, expected: %d", c, 4)
		}
	})
	t.Run("empty-ring-pull", func(t *testing.T) {
		rb := newExRing[int]()

		_, ok := rb.pull()
		if ok {
			t.Error("ring is not empty")
		}
	})

	t.Run("full-ring-push", func(t *testing.T) {
		rb := newExRing[int]()

		rb.push(1)
		expectLen(t, rb, 1, "it must push")

		if _, ok := rb.pull(); !ok {
			t.Errorf("can not pull")
		}

		for range rb.cap() {
			rb.push(1)
		}

		expectLen(t, rb, 4, "initialized cap must be 4")

		rb.push(1)
		expectLen(t, rb, 5, "must push to ring after expand")

	})

	t.Run("push-pull", func(t *testing.T) {
		rb := newExRing[int]()
		rb.push(1)

		expectLen(t, rb, 1, "it should push, but failed")

		got, ok := rb.pull()
		if !ok {
			t.Error("ring is empty")
		}
		if got != 1 {
			t.Errorf("ring.pull returns %v, expected: 1", got)
		}
		if l := rb.len(); l != 0 {
			t.Errorf("ring.len is %d, expected: zero", l)
		}

		n := rand.Intn(5000) + 10
		for i := range n {
			rb.push(i + 1)
		}
		expectLen(t, rb, n, "it should push all integers")

		for i := range n {
			got, ok := rb.pull()
			if !ok {
				t.Fatal("empty")
			}

			if expected := i + 1; got != expected {
				t.Errorf("not equel, got %v, expected: %v", got, expected)
			}
		}
	})

	t.Run("concurent-push-pull", func(t *testing.T) {
		rb := newExRing[int]()
		wg := sync.WaitGroup{}

		// pushing
		for range 1025 {
			wg.Add(1)

			go func() {
				rb.push(1)

				wg.Done()
			}()
		}
		wg.Wait()

		expectLen(t, rb, 1025, "it should push concurently")

		// pulling
		for range 1025 {
			wg.Add(1)
			go func() {
				got, ok := rb.pull()
				if !ok {
					t.Error("ring is empty")
				}
				if expected := 1; got != expected {
					t.Errorf("ring.pull returns %v, expected: %v", got, expected)
				}
				wg.Done()
			}()
		}

		wg.Wait()

		if l := rb.len(); l != 0 {
			t.Errorf("ring.len is %d, expected: zero", l)
		}
	})

	t.Run("len", func(t *testing.T) {
		rb := newExRing[int]()
		max := rand.Intn(20) + 5
		len := 0

		for i := max; i >= 0; i-- {
			for range i {
				rb.push(1)
				len++
			}

			for range i - 1 {
				if _, ok := rb.pull(); !ok {
					t.Fatal("can not pull")
				}
				len--
			}

			if len != rb.len() {
				t.Errorf("unexpected len, len=%d, expected=%d", rb.len(), len)
			}
		}

		if rb.len() != max {
			// every run, we pulled one lower than push; no. of runs is max
			t.Errorf("len must be %d, len=%d", max, rb.len())
		}
	})
}
