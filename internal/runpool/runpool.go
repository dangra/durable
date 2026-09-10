// Package runpool provides a keyed counting pool of held tokens with an
// ordered line of members — the mechanism behind the engine's run
// classes. A member is admitted into a key's line at a caller-supplied
// place, takes the token once, and holds it until it is released. It
// holds only mechanism: what a key means, what the order key is, and
// what a kick does are caller policy.
//
// The line is ordered by the place, never by arrival: a member is
// granted only when it and every member ahead of it fit in the free
// capacity, whether those ahead have asked yet or not, so tokens go out
// in line order however the members happen to ask. The pool never
// dispatches wakes itself. Operations that can expose free capacity
// return the head of the line to wake, and the caller must deliver that
// kick; a member keeps its place until it acquires, so a redundant kick
// is harmless and a lost one is not.
//
// A key's pending members — admitted and not holding, in line or set
// aside — are counted, and Admit can refuse past a cap: the check and
// the count are one step.
package runpool

import (
	"cmp"
	"slices"
	"sync"
)

// Pool tracks, per key, the tokens held and the members in line. K
// identifies a class, W a member, O the member's place in line. The
// zero value is unusable — construct with New.
type Pool[K comparable, W comparable, O cmp.Ordered] struct {
	mu       sync.Mutex
	capacity map[K]int
	classes  map[K]*class[W, O]
	holders  map[W]K
	// pending maps every admitted, non-holding member to its key; a
	// member is in its class's line unless set aside by Clear.
	pending   map[W]K
	sidelined map[W]struct{}
	// parked marks the pending members that asked and were refused.
	parked map[W]struct{}
}

type class[W comparable, O cmp.Ordered] struct {
	capacity  int
	inUse     int
	line      []waiter[W, O] // sorted by order
	sidelined int
}

type waiter[W comparable, O cmp.Ordered] struct {
	w     W
	order O
}

// Usage is a point-in-time snapshot of one key.
type Usage struct {
	Capacity int
	// InUse counts the tokens held.
	InUse int
	// Waiting counts the members that asked and were refused.
	Waiting int
	// Pending counts the members admitted and not holding: in line, the
	// waiting ones included, or set aside.
	Pending int
}

// New builds a Pool limiting each key in capacity to its value. Keys
// absent from capacity are unlimited: every operation on them is a
// no-op that grants. The map is copied.
func New[K comparable, W comparable, O cmp.Ordered](capacity map[K]int) *Pool[K, W, O] {
	p := &Pool[K, W, O]{
		capacity:  make(map[K]int, len(capacity)),
		classes:   make(map[K]*class[W, O]),
		holders:   make(map[W]K),
		pending:   make(map[W]K),
		sidelined: make(map[W]struct{}),
		parked:    make(map[W]struct{}),
	}
	for k, c := range capacity {
		p.capacity[k] = c
	}
	return p
}

func (p *Pool[K, W, O]) classFor(k K) (*class[W, O], bool) {
	cap, limited := p.capacity[k]
	if !limited {
		return nil, false
	}
	c := p.classes[k]
	if c == nil {
		c = &class[W, O]{capacity: cap}
		p.classes[k] = c
	}
	return c, true
}

// Admit puts w in k's line at order unless k already has maxPending
// pending members (maxPending <= 0 is unbounded), and reports whether it
// did. Idempotent for a member already pending; a holder and an
// unlimited key are admitted without record.
func (p *Pool[K, W, O]) Admit(k K, w W, order O, maxPending int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, limited := p.classFor(k)
	if !limited {
		return true
	}
	if _, holds := p.holders[w]; holds {
		return true
	}
	if _, ok := p.pending[w]; ok {
		return true
	}
	if maxPending > 0 && c.pendingCount() >= maxPending {
		return false
	}
	p.pending[w] = k
	c.insert(w, order)
	return true
}

// Acquire asks for k's token on behalf of w, whose place in line is
// order (used only when w is not in line yet). A holder is granted
// again without change. Otherwise w is granted exactly when it and
// every member ahead of it fit in the free capacity; a grant takes w
// out of the line. kick names the member the caller must wake when the
// grant left capacity for the new head of the line.
func (p *Pool[K, W, O]) Acquire(k K, w W, order O) (granted bool, kick W, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, limited := p.classFor(k)
	if !limited {
		return true, kick, false
	}
	if _, holds := p.holders[w]; holds {
		return true, kick, false
	}
	if _, pending := p.pending[w]; !pending {
		p.pending[w] = k
	}
	idx := c.index(w)
	if idx < 0 {
		// Not in line: never admitted, or set aside.
		if _, aside := p.sidelined[w]; aside {
			delete(p.sidelined, w)
			c.sidelined--
		}
		idx = c.insert(w, order)
	}
	if c.inUse+idx >= c.capacity {
		p.parked[w] = struct{}{}
		return false, kick, false
	}
	c.line = slices.Delete(c.line, idx, idx+1)
	delete(p.parked, w)
	delete(p.pending, w)
	c.inUse++
	p.holders[w] = k
	kick, ok = c.head()
	return true, kick, ok
}

// Hold makes w a holder of k's token regardless of capacity — for
// recovery, where a member that had started before a restart holds by
// right and a lowered capacity is honored by granting nothing until
// use falls below it. Idempotent; a no-op for an unlimited key.
func (p *Pool[K, W, O]) Hold(k K, w W) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, limited := p.classFor(k)
	if !limited {
		return
	}
	if _, holds := p.holders[w]; holds {
		return
	}
	p.forget(c, w)
	c.inUse++
	p.holders[w] = k
}

// Release ends w's membership: a holder returns its token, a pending
// member leaves the line or its sideline. kick names the head of the
// line when the departure left capacity for it.
func (p *Pool[K, W, O]) Release(w W) (kick W, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if k, holds := p.holders[w]; holds {
		delete(p.holders, w)
		c := p.classes[k]
		c.inUse--
		return c.head()
	}
	if k, pending := p.pending[w]; pending {
		c := p.classes[k]
		p.forget(c, w)
		return c.head()
	}
	return kick, false
}

// Clear sets w aside: out of the line, still pending, free to Acquire
// again later at a place of its own. For a member that cannot proceed
// for now and must not hold the line. kick as for Release.
func (p *Pool[K, W, O]) Clear(w W) (kick W, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, pending := p.pending[w]
	if !pending {
		return kick, false
	}
	if _, aside := p.sidelined[w]; aside {
		return kick, false
	}
	c := p.classes[k]
	if i := c.index(w); i >= 0 {
		c.line = slices.Delete(c.line, i, i+1)
	}
	delete(p.parked, w)
	p.sidelined[w] = struct{}{}
	c.sidelined++
	return c.head()
}

// forget drops every pending trace of w. Callers hold p.mu.
func (p *Pool[K, W, O]) forget(c *class[W, O], w W) {
	if _, pending := p.pending[w]; !pending {
		return
	}
	delete(p.pending, w)
	delete(p.parked, w)
	if _, aside := p.sidelined[w]; aside {
		delete(p.sidelined, w)
		c.sidelined--
		return
	}
	if i := c.index(w); i >= 0 {
		c.line = slices.Delete(c.line, i, i+1)
	}
}

func (c *class[W, O]) pendingCount() int { return len(c.line) + c.sidelined }

// index returns w's place in the line, or -1.
func (c *class[W, O]) index(w W) int {
	return slices.IndexFunc(c.line, func(x waiter[W, O]) bool { return x.w == w })
}

// insert puts w in the line at its order and returns its index.
func (c *class[W, O]) insert(w W, order O) int {
	i, _ := slices.BinarySearchFunc(c.line, order, func(x waiter[W, O], o O) int { return cmp.Compare(x.order, o) })
	c.line = slices.Insert(c.line, i, waiter[W, O]{w: w, order: order})
	return i
}

// head names the member to wake when the class has free capacity.
// Callers hold p.mu.
func (c *class[W, O]) head() (W, bool) {
	var zero W
	if c.inUse < c.capacity && len(c.line) > 0 {
		return c.line[0].w, true
	}
	return zero, false
}

// Holds reports whether w holds a token.
func (p *Pool[K, W, O]) Holds(w W) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.holders[w]
	return ok
}

// ParkedOn reports the key w asked for and was refused, if any.
func (p *Pool[K, W, O]) ParkedOn(w W) (K, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, parked := p.parked[w]; parked {
		return p.pending[w], true
	}
	var zero K
	return zero, false
}

// Pending reports k's pending count; zero for an unlimited key.
func (p *Pool[K, W, O]) Pending(k K) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c := p.classes[k]; c != nil {
		return c.pendingCount()
	}
	return 0
}

// Snapshot reports per-key occupancy for keys used since construction.
func (p *Pool[K, W, O]) Snapshot() map[K]Usage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.classes) == 0 {
		return nil
	}
	out := make(map[K]Usage, len(p.classes))
	for k, c := range p.classes {
		waiting := 0
		for _, x := range c.line {
			if _, parked := p.parked[x.w]; parked {
				waiting++
			}
		}
		out[k] = Usage{Capacity: c.capacity, InUse: c.inUse, Waiting: waiting, Pending: c.pendingCount()}
	}
	return out
}
