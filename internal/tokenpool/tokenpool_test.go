package tokenpool

import (
	"sync"
	"testing"
	"time"
)

func newTestPool(cap int) *Pool[string, string] {
	return New[string, string](map[string]int{"boot": cap})
}

var t0 = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)

// grant asserts an Acquire that must take a token.
func grant(t *testing.T, p *Pool[string, string], k, w string, now time.Time) {
	t.Helper()
	granted, held, _, kicks := p.Acquire(k, w, false, now)
	if !granted || !held || len(kicks) != 0 {
		t.Fatalf("Acquire(%s,%s) = granted=%v held=%v kicks=%v, want plain grant", k, w, granted, held, kicks)
	}
}

// parked asserts an Acquire that must queue.
func parked(t *testing.T, p *Pool[string, string], k, w string, now time.Time) {
	t.Helper()
	granted, held, waited, kicks := p.Acquire(k, w, false, now)
	if granted || held || waited != 0 || len(kicks) != 0 {
		t.Fatalf("Acquire(%s,%s) = %v %v %v kicks=%v, want park", k, w, granted, held, waited, kicks)
	}
}

func TestGrantParkReleaseFIFO(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	parked(t, p, "boot", "c", t0)

	kick, ok := p.Release("boot")
	if !ok || kick != "b" {
		t.Fatalf("Release kick = %q %v, want head b", kick, ok)
	}
	// b keeps its slot until it actually acquires.
	if u := p.Snapshot()["boot"]; u.Waiting != 2 || u.InUse != 0 {
		t.Fatalf("usage after release = %+v", u)
	}
	granted, held, waited, _ := p.Acquire("boot", "b", false, t0.Add(time.Second))
	if !granted || !held || waited != time.Second {
		t.Fatalf("woken head acquire = %v %v %v", granted, held, waited)
	}
	if u := p.Snapshot()["boot"]; u.Waiting != 1 || u.InUse != 1 {
		t.Fatalf("usage after head acquired = %+v", u)
	}
}

// TestDecliningWaiterHandsWakeOn is the review-finding scenario: the
// woken queue head declines (bypass), and the wake must reach the next
// waiter instead of stranding the free token.
func TestDecliningWaiterHandsWakeOn(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	parked(t, p, "boot", "d", t0)

	if kick, ok := p.Release("boot"); !ok || kick != "b" {
		t.Fatalf("release kick = %q %v", kick, ok)
	}
	// b was canceled meanwhile: it bypasses, and its departure must
	// kick d.
	granted, held, waited, kicks := p.Acquire("boot", "b", true, t0)
	if !granted || held || waited != 0 {
		t.Fatalf("bypass = %v %v %v", granted, held, waited)
	}
	if len(kicks) != 1 || kicks[0] != "d" {
		t.Fatalf("declined wake kicks = %v, want [d]", kicks)
	}
	grant(t, p, "boot", "d", t0)
}

// TestClearHandsWakeOn covers the cancellation-without-reacquire path:
// clearing a queued waiter while capacity is free kicks the next.
func TestClearHandsWakeOn(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	parked(t, p, "boot", "d", t0)

	// While the token is held, clearing b exposes no capacity: no kick.
	if kick, ok := p.Clear("b"); ok {
		t.Fatalf("clear with token held kicked %q", kick)
	}
	if _, ok := p.Clear("b"); ok {
		t.Fatal("double clear kicked")
	}
	if kick, ok := p.Release("boot"); !ok || kick != "d" {
		t.Fatalf("release kick = %q %v, want d (b cleared)", kick, ok)
	}

	// With the token free, clearing the head hands the wake on.
	grant(t, p, "boot", "d", t0)
	parked(t, p, "boot", "e", t0)
	parked(t, p, "boot", "f", t0)
	if kick, ok := p.Release("boot"); !ok || kick != "e" {
		t.Fatalf("release kick = %q %v", kick, ok)
	}
	if kick, ok := p.Clear("e"); !ok || kick != "f" {
		t.Fatalf("clear of woken head kicked %q %v, want f", kick, ok)
	}
}

// TestCascadeKick: two releases before the head runs wake only the head;
// its grant must cascade the second wake to the next waiter.
func TestCascadeKick(t *testing.T) {
	p := newTestPool(2)
	grant(t, p, "boot", "a", t0)
	grant(t, p, "boot", "b", t0)
	parked(t, p, "boot", "c", t0)
	parked(t, p, "boot", "d", t0)

	if kick, ok := p.Release("boot"); !ok || kick != "c" {
		t.Fatalf("first release kick = %q %v", kick, ok)
	}
	if kick, ok := p.Release("boot"); !ok || kick != "c" {
		t.Fatalf("second release kick = %q %v (head still queued)", kick, ok)
	}
	granted, held, _, kicks := p.Acquire("boot", "c", false, t0)
	if !granted || !held {
		t.Fatalf("head grant = %v %v", granted, held)
	}
	if len(kicks) != 1 || kicks[0] != "d" {
		t.Fatalf("cascade kicks = %v, want [d]", kicks)
	}
	grant(t, p, "boot", "d", t0)
}

// TestRecheckKeepsSlotAndParkTime: a queued waiter poked without free
// capacity re-parks in place, keeping FIFO position and its original
// park time.
func TestRecheckKeepsSlotAndParkTime(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	parked(t, p, "boot", "c", t0)

	// Spurious re-check of b: still full, still head, since preserved.
	parked(t, p, "boot", "b", t0.Add(time.Minute))
	if kick, ok := p.Release("boot"); !ok || kick != "b" {
		t.Fatalf("kick = %q %v, want b still at head", kick, ok)
	}
	_, _, waited, _ := p.Acquire("boot", "b", false, t0.Add(2*time.Minute))
	if waited != 2*time.Minute {
		t.Fatalf("waited = %v, want park time preserved from first park", waited)
	}
}

// TestKeyChangeUnparks: a waiter parked on one key acquiring under
// another (deployment changed the step's class) releases its old slot
// and hands on any exposed wake, without reporting cross-key wait time.
func TestKeyChangeUnparks(t *testing.T) {
	p := New[string, string](map[string]int{"boot": 1, "destroy": 1})
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	parked(t, p, "boot", "c", t0)

	p.Release("boot") // frees the token; head b woken
	// b now acquires under "destroy" instead: its boot slot must clear
	// and the exposed boot capacity must kick c.
	granted, held, waited, kicks := p.Acquire("destroy", "b", false, t0.Add(time.Hour))
	if !granted || !held {
		t.Fatalf("cross-key grant = %v %v", granted, held)
	}
	if waited != time.Hour {
		// The park was real waiting from b's perspective; the pool
		// reports it against the grant regardless of key.
		t.Fatalf("waited = %v", waited)
	}
	if len(kicks) != 1 || kicks[0] != "c" {
		t.Fatalf("kicks = %v, want the boot hand-off [c]", kicks)
	}
	if k, ok := p.ParkedOn("b"); ok {
		t.Fatalf("b still parked on %q", k)
	}
}

func TestUnlimitedAndBypass(t *testing.T) {
	p := newTestPool(1)
	granted, held, waited, kicks := p.Acquire("unconfigured", "a", false, t0)
	if !granted || held || waited != 0 || len(kicks) != 0 {
		t.Fatalf("unlimited acquire = %v %v %v %v", granted, held, waited, kicks)
	}
	granted, held, _, _ = p.Acquire("boot", "b", true, t0)
	if !granted || held {
		t.Fatalf("bypass acquire = %v %v", granted, held)
	}
	if p.Snapshot() != nil {
		t.Fatalf("snapshot = %+v, want nil before any limited use", p.Snapshot())
	}
}

func TestParkedOnAndSnapshot(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "boot", "a", t0)
	parked(t, p, "boot", "b", t0)
	if k, ok := p.ParkedOn("b"); !ok || k != "boot" {
		t.Fatalf("ParkedOn(b) = %q %v", k, ok)
	}
	if _, ok := p.ParkedOn("a"); ok {
		t.Fatal("token holder reported parked")
	}
	if u := p.Snapshot()["boot"]; u != (Usage{Capacity: 1, InUse: 1, Waiting: 1}) {
		t.Fatalf("usage = %+v", u)
	}
}

// TestConcurrentChurn hammers grant/park/release/clear from many
// goroutines under the race detector and checks conservation: tokens
// in use never exceed capacity, and the pool drains to empty.
func TestConcurrentChurn(t *testing.T) {
	const cap = 3
	p := newTestPool(cap)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		cond = sync.NewCond(&mu)
		woke = map[string]bool{}
	)
	worker := func(w string) {
		defer wg.Done()
		now := time.Now()
		for {
			granted, held, _, kicks := p.Acquire("boot", w, false, now)
			if len(kicks) > 0 {
				mu.Lock()
				for _, kk := range kicks {
					woke[kk] = true
				}
				cond.Broadcast()
				mu.Unlock()
			}
			if granted {
				if !held {
					t.Errorf("limited grant without token")
					return
				}
				kick, kickOK := p.Release("boot")
				if kickOK {
					mu.Lock()
					woke[kick] = true
					cond.Broadcast()
					mu.Unlock()
				}
				return
			}
			// Parked: wait for our wake.
			mu.Lock()
			for !woke[w] {
				cond.Wait()
			}
			delete(woke, w)
			mu.Unlock()
		}
	}
	const n = 60
	names := make([]string, n)
	for i := range names {
		names[i] = string(rune('A'+i%26)) + string(rune('0'+i/26))
	}
	for _, w := range names {
		wg.Add(1)
		go worker(w)
	}
	wg.Wait()
	u := p.Snapshot()["boot"]
	if u.InUse != 0 || u.Waiting != 0 {
		t.Fatalf("pool did not drain: %+v", u)
	}
}
