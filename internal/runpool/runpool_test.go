package runpool

import (
	"sync"
	"testing"
)

func newTestPool(cap int) *Pool[string, string, int] {
	return New[string, string, int](map[string]int{"m": cap})
}

func grant(t *testing.T, p *Pool[string, string, int], w string, order int) (kick string, ok bool) {
	t.Helper()
	granted, kick, ok := p.Acquire("m", w, order)
	if !granted {
		t.Fatalf("Acquire(%s) parked, want grant", w)
	}
	return kick, ok
}

func park(t *testing.T, p *Pool[string, string, int], w string, order int) {
	t.Helper()
	granted, kick, ok := p.Acquire("m", w, order)
	if granted || ok {
		t.Fatalf("Acquire(%s) = granted=%v kick=%q, want park", w, granted, kick)
	}
}

// TestLineOrderIsTheKey: tokens go out in order-key order however the
// members ask, and a release wakes the head of the line.
func TestLineOrderIsTheKey(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "a", 1)
	park(t, p, "c", 3)
	park(t, p, "b", 2)
	if u := p.Snapshot()["m"]; u.InUse != 1 || u.Waiting != 2 {
		t.Fatalf("usage = %+v", u)
	}
	kick, ok := p.Release("a")
	if !ok || kick != "b" {
		t.Fatalf("Release kick = %q %v, want b", kick, ok)
	}
	// c asks first after the release: strict order parks it behind b.
	park(t, p, "c", 3)
	if kick, ok := grant(t, p, "b", 2); ok {
		t.Fatalf("b's grant kicked %q with the class full", kick)
	}
	kick, ok = p.Release("b")
	if !ok || kick != "c" {
		t.Fatalf("Release kick = %q %v, want c", kick, ok)
	}
	grant(t, p, "c", 3)
}

// TestGrantCascades: with capacity for more than the head, a grant kicks
// the next in line, and a release with room wakes only the head.
func TestGrantCascades(t *testing.T) {
	p := newTestPool(3)
	grant(t, p, "a", 1)
	grant(t, p, "b", 2)
	grant(t, p, "c", 3)
	park(t, p, "d", 4)
	park(t, p, "e", 5)
	p.Release("a")
	p.Release("b")
	// d and e both fit now; d's grant must hand the wake to e.
	kick, ok := grant(t, p, "d", 4)
	if !ok || kick != "e" {
		t.Fatalf("cascade kick = %q %v, want e", kick, ok)
	}
	if kick, ok := grant(t, p, "e", 5); ok {
		t.Fatalf("last grant kicked %q", kick)
	}
}

// TestHolderAcquireIsIdempotent: a holder asking again is granted
// without a second token.
func TestHolderAcquireIsIdempotent(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "a", 1)
	grant(t, p, "a", 1)
	if u := p.Snapshot()["m"]; u.InUse != 1 {
		t.Fatalf("InUse = %d after a repeated acquire", u.InUse)
	}
	if !p.Holds("a") {
		t.Fatal("Holds(a) = false")
	}
}

// TestHoldOvershoots: recovery holds ignore capacity, and nothing is
// granted until use falls below it.
func TestHoldOvershoots(t *testing.T) {
	p := newTestPool(1)
	p.Admit("m", "x", 9, 0)
	p.Hold("m", "a")
	p.Hold("m", "b")
	p.Hold("m", "a") // idempotent
	if u := p.Snapshot()["m"]; u.InUse != 2 || u.Pending != 1 {
		t.Fatalf("usage = %+v", u)
	}
	park(t, p, "x", 9)
	if kick, ok := p.Release("a"); ok {
		t.Fatalf("Release with use still at capacity kicked %q", kick)
	}
	kick, ok := p.Release("b")
	if !ok || kick != "x" {
		t.Fatalf("Release kick = %q %v, want x", kick, ok)
	}
	grant(t, p, "x", 9)
	if u := p.Snapshot()["m"]; u.Pending != 0 || u.InUse != 1 {
		t.Fatalf("usage = %+v", u)
	}
}

// TestPendingCountsAdmittedNotStarted: Admit counts, a grant and a
// release of a non-holder uncount, Clear keeps the count.
func TestPendingCountsAdmittedNotStarted(t *testing.T) {
	p := newTestPool(1)
	p.Admit("m", "a", 1, 0)
	p.Admit("m", "b", 2, 0)
	p.Admit("m", "b", 2, 0)
	if n := p.Pending("m"); n != 2 {
		t.Fatalf("Pending = %d, want 2", n)
	}
	grant(t, p, "a", 1)
	park(t, p, "b", 2)
	if n := p.Pending("m"); n != 1 {
		t.Fatalf("Pending after grant = %d, want 1", n)
	}
	if kick, ok := p.Clear("b"); ok {
		t.Fatalf("Clear with the class full kicked %q", kick)
	}
	if n := p.Pending("m"); n != 1 {
		t.Fatalf("Pending after Clear = %d, want 1 (still pending)", n)
	}
	if _, parked := p.ParkedOn("b"); parked {
		t.Fatal("b still parked after Clear")
	}
	if kick, ok := p.Release("b"); ok {
		t.Fatalf("Release of a non-holder kicked %q", kick)
	}
	if n := p.Pending("m"); n != 0 {
		t.Fatalf("Pending after Release = %d, want 0", n)
	}
}

// TestClearHandsTheWakeOn: a woken head that leaves the line hands the
// wake to the next waiter rather than stranding free capacity.
func TestClearHandsTheWakeOn(t *testing.T) {
	p := newTestPool(1)
	grant(t, p, "a", 1)
	park(t, p, "b", 2)
	park(t, p, "c", 3)
	if kick, _ := p.Release("a"); kick != "b" {
		t.Fatalf("kick = %q, want b", kick)
	}
	kick, ok := p.Clear("b")
	if !ok || kick != "c" {
		t.Fatalf("Clear kick = %q %v, want c", kick, ok)
	}
}

// TestUnlimitedKeyIsANoOp: keys without capacity grant and record nothing.
func TestUnlimitedKeyIsANoOp(t *testing.T) {
	p := newTestPool(1)
	p.Admit("free", "a", 1, 0)
	granted, _, ok := p.Acquire("free", "a", 1)
	if !granted || ok {
		t.Fatalf("Acquire on unlimited = %v %v", granted, ok)
	}
	p.Hold("free", "b")
	if p.Holds("b") || p.Pending("free") != 0 {
		t.Fatal("unlimited key recorded state")
	}
	if _, ok := p.Snapshot()["free"]; ok {
		t.Fatal("unlimited key in Snapshot")
	}
}

// TestConcurrentUse: the invariant under contention is use <= capacity
// and every member ends released.
func TestConcurrentUse(t *testing.T) {
	const cap, members = 3, 64
	p := newTestPool(cap)
	var wg sync.WaitGroup
	for i := 0; i < members; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := string(rune('a' + i%26))
			w += string(rune('0' + i/26))
			for {
				granted, _, _ := p.Acquire("m", w, i)
				if u := p.Snapshot()["m"]; u.InUse > cap {
					t.Errorf("InUse %d > cap %d", u.InUse, cap)
				}
				if granted {
					break
				}
			}
			p.Release(w)
		}(i)
	}
	wg.Wait()
	if u := p.Snapshot()["m"]; u.InUse != 0 || u.Waiting != 0 || u.Pending != 0 {
		t.Fatalf("usage after all released = %+v", u)
	}
}

// TestAdmitCap: the pending cap refuses the admission past it, a
// repeated admission of a pending member is not a new one, and a start
// or a release frees a place.
func TestAdmitCap(t *testing.T) {
	p := newTestPool(1)
	if !p.Admit("m", "a", 1, 2) || !p.Admit("m", "b", 2, 2) {
		t.Fatal("admissions under the cap refused")
	}
	if !p.Admit("m", "b", 2, 2) {
		t.Fatal("repeated admission of a pending member refused")
	}
	if p.Admit("m", "c", 3, 2) {
		t.Fatal("admission at the cap accepted")
	}
	grant(t, p, "a", 1)
	if !p.Admit("m", "c", 3, 2) {
		t.Fatal("admission after a start refused")
	}
	if p.Admit("m", "d", 4, 2) {
		t.Fatal("admission at the cap accepted")
	}
	p.Release("b")
	if !p.Admit("m", "d", 4, 2) {
		t.Fatal("admission after a release refused")
	}
}

// TestAdmittedMemberHoldsItsPlaceBeforeAsking: a member admitted ahead
// keeps its place even when a later one asks first.
func TestAdmittedMemberHoldsItsPlaceBeforeAsking(t *testing.T) {
	p := newTestPool(1)
	p.Admit("m", "a", 1, 0)
	p.Admit("m", "b", 2, 0)
	park(t, p, "b", 2)
	if kick, ok := grant(t, p, "a", 1); ok {
		t.Fatalf("grant at capacity kicked %q", kick)
	}
	if u := p.Snapshot()["m"]; u.InUse != 1 || u.Waiting != 1 || u.Pending != 1 {
		t.Fatalf("usage = %+v", u)
	}
	kick, ok := p.Release("a")
	if !ok || kick != "b" {
		t.Fatalf("Release kick = %q %v, want b", kick, ok)
	}
}
