package dispatcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type wallClock struct{}

func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type harness struct {
	d      *Dispatcher[string]
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newHarness(t *testing.T, concurrency int, run func(string) (time.Duration, bool)) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{cancel: cancel}
	h.d = New(Config[string]{
		Ctx:         ctx,
		Concurrency: concurrency,
		Clock:       wallClock{},
		Spawn:       func(fn func()) { h.wg.Add(1); go func() { defer h.wg.Done(); fn() }() },
		Run:         run,
	})
	t.Cleanup(func() {
		cancel()
		h.wg.Wait()
	})
	return h
}

// TestSingleFlight: concurrent dispatches of one key run exactly one
// worker at a time, and every runnable pass happens on one goroutine.
func TestSingleFlight(t *testing.T) {
	var (
		concurrent atomic.Int32
		maxSeen    atomic.Int32
		runs       atomic.Int32
	)
	h := newHarness(t, 8, func(string) (time.Duration, bool) {
		c := concurrent.Add(1)
		for {
			m := maxSeen.Load()
			if c <= m || maxSeen.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		concurrent.Add(-1)
		runs.Add(1)
		return 0, false
	})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.d.Dispatch("k", 0) }()
	}
	wg.Wait()
	h.cancel()
	h.wg.Wait()
	if maxSeen.Load() != 1 {
		t.Fatalf("max concurrent workers for one key = %d", maxSeen.Load())
	}
	if runs.Load() == 0 {
		t.Fatal("no runs happened")
	}
}

// TestRepokeNeverLosesWake is the lost-wake regression at primitive
// level: a dispatch arriving while the worker is mid-run must produce
// another run after it exits.
func TestRepokeNeverLosesWake(t *testing.T) {
	var (
		runs    atomic.Int32
		inRun   = make(chan struct{})
		release = make(chan struct{})
	)
	h := newHarness(t, 1, func(string) (time.Duration, bool) {
		if runs.Add(1) == 1 {
			inRun <- struct{}{}
			<-release
		}
		return 0, false
	})
	h.d.Dispatch("k", 0)
	<-inRun
	h.d.Dispatch("k", 0) // suppressed: must be re-poked
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("runs = %d, want the suppressed dispatch honored", runs.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRepokeOverridesDelay: a worker asking for a long redispatch delay
// is redispatched immediately when a poke was suppressed during its run.
func TestRepokeOverridesDelay(t *testing.T) {
	var (
		runs    atomic.Int32
		inRun   = make(chan struct{})
		release = make(chan struct{})
	)
	h := newHarness(t, 1, func(string) (time.Duration, bool) {
		if runs.Add(1) == 1 {
			inRun <- struct{}{}
			<-release
			return time.Hour, true // wants a long retry wait
		}
		return 0, false
	})
	h.d.Dispatch("k", 0)
	<-inRun
	h.d.Dispatch("k", 0)
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("re-poke did not override the hour-long redispatch delay")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestWakeCutsDelayShort: a delayed dispatch runs promptly on Wake.
func TestWakeCutsDelayShort(t *testing.T) {
	ran := make(chan struct{})
	h := newHarness(t, 1, func(string) (time.Duration, bool) {
		close(ran)
		return 0, false
	})
	h.d.Dispatch("k", time.Hour)
	deadline := time.Now().Add(5 * time.Second)
	for h.d.Delayed() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("worker never armed its delay")
		}
		time.Sleep(time.Millisecond)
	}
	if !h.d.IsActive("k") || h.d.Active() != 1 {
		t.Fatalf("delayed worker not active: %v %d", h.d.IsActive("k"), h.d.Active())
	}
	h.d.Wake("k")
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("wake did not cut the delay short")
	}
}

// TestConcurrencyBound: distinct keys never exceed the configured
// global concurrency.
func TestConcurrencyBound(t *testing.T) {
	const bound = 3
	var (
		concurrent atomic.Int32
		maxSeen    atomic.Int32
	)
	h := newHarness(t, bound, func(string) (time.Duration, bool) {
		c := concurrent.Add(1)
		for {
			m := maxSeen.Load()
			if c <= m || maxSeen.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		concurrent.Add(-1)
		return 0, false
	})
	for i := 0; i < 24; i++ {
		h.d.Dispatch(string(rune('a'+i)), 0)
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.d.Active() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("workers never drained: %d active", h.d.Active())
		}
		time.Sleep(time.Millisecond)
	}
	if m := maxSeen.Load(); m > bound {
		t.Fatalf("max concurrency = %d, bound %d", m, bound)
	}
}

// TestShutdownStopsWork: a done context stops redispatches and lets
// delayed workers exit without running.
func TestShutdownStopsWork(t *testing.T) {
	var runs atomic.Int32
	h := newHarness(t, 1, func(string) (time.Duration, bool) {
		runs.Add(1)
		return time.Millisecond, true // would loop forever
	})
	h.d.Dispatch("loop", 0)
	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("worker never ran")
		}
		time.Sleep(time.Millisecond)
	}
	h.d.Dispatch("parked", time.Hour)
	h.cancel()
	h.wg.Wait() // all workers exit, the hour-delayed one included
	if h.d.Active() != 0 {
		t.Fatalf("active = %d after shutdown", h.d.Active())
	}
}
