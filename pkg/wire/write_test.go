package wire

import (
	"sync"
	"testing"
	"time"
)

func TestLinkedQ(t *testing.T) {
	t.Run("many-push-pull", func(t *testing.T) {
		lq := newLinkedQ[int]()
		for range 420 {
			if !lq.push(1) {
				t.Error("it should push to linkedQ")
			}
		}

		for range 420 {
			item, ok := lq.pull()
			if !ok {
				t.Error("it should pull from linkedQ")
			}

			if expected := 1; item != expected {
				t.Errorf("invalid item, got %v, expected %v", item, expected)
			}
		}

		if _, notEmpty := lq.pull(); notEmpty {
			t.Error("linkedQ should be empty")
		}
	})

	t.Run("push-pullWait", func(t *testing.T) {
		lq := newLinkedQ[int]()
		for range 420 {
			if !lq.push(1) {
				t.Error("it should push to linkedQ")
			}
		}

		for range 420 {
			item := lq.pullWait()

			if expected := 1; item != expected {
				t.Errorf("invalid item, got %v, expected %v", item, expected)
			}
		}

		if _, notEmpty := lq.pull(); notEmpty {
			t.Error("linkedQ should be empty")
		}
	})

	t.Run("many-push-one-pullWait", func(t *testing.T) {
		lq := newLinkedQ[int]()

		for range 1024 {
			go func() {
				if !lq.push(1) {
					t.Error("it should push to linkedQ")
				}
			}()
		}

		for range 1024 {
			item := lq.pullWait()

			if expected := 1; item != expected {
				t.Errorf("invalid item, got %v, expected %v", item, expected)
			}
		}

		if _, notEmpty := lq.pull(); notEmpty {
			t.Error("linkedQ should be empty")
		}
	})

	t.Run("concurent-push-pullWait", func(t *testing.T) {
		lq := newLinkedQ[int]()

		for range 1024 {
			go func() {
				if !lq.push(1) {
					t.Error("it should push to linkedQ")
				}
			}()
		}

		wg := sync.WaitGroup{}
		for range 1024 {
			wg.Add(1)
			go func() {
				item := lq.pullWait()

				if expected := 1; item != expected {
					t.Errorf("invalid item, got %v, expected %v", item, expected)
				}
				wg.Done()
			}()
		}

		wg.Wait()
		if _, notEmpty := lq.pull(); notEmpty {
			t.Error("linkedQ should be empty")
		}
	})

	t.Run("len", func(t *testing.T) {
		lq := newLinkedQ[int]()

		for i := range 256 {
			if !lq.push(1) {
				t.Error("it should push to linkedQ")
			}

			expLen := i + 1
			if l := lq.len(); l != expLen {
				t.Errorf("invalid, len=%d, expected=%d", l, expLen)
			}
		}
	})
}

func TestWriteQ(t *testing.T) {
	// w := writeQ{make(chanWrite, 300)}

	// ok := w.push(nil)
	// if !ok {
	// 	t.Fatal()
	// }

	// if l := w.len(); l != 1 {
	// 	t.Errorf("len=%d", l)
	// }

	ch := make(chan bool, 5)
	ch <- true

	_ = len(ch)
}

func TestFastTime(t *testing.T) {
	t.Run("epoch", func(t *testing.T) {
		expectEpoch := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		if !expectEpoch.Equal(_epoch) {
			t.Errorf("unexpected epoch")
		}
	})

	t.Run("fastTimeWith", func(t *testing.T) {
		date := time.Date(2029, 8, 7, 6, 5, 4, 3, time.UTC)
		since2020Ms := date.Sub(_epoch).Milliseconds()

		ft := fastTimeWith(date)
		if expected := since2020Ms; int64(ft) != expected {
			t.Errorf("durations not equals: got: %v, expected: %v", ft, expected)
		}
	})

	t.Run("Time", func(t *testing.T) {
		expectedTime := time.Now().Truncate(time.Millisecond)
		ft := fastTimeWith(expectedTime)

		got := ft.Time()
		if !got.Equal(expectedTime) {
			t.Errorf("not equal: got %v, expected %v", got, expectedTime)
		}

		if newFt := fastTimeWith(got); newFt != ft {
			t.Errorf("can not convert back to fast time, got %d, expected: %d", newFt, ft)
		}
	})
}
