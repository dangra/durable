// Package tokenpool provides a keyed FIFO-fair counting semaphore with
// declining waiters — the mechanism behind the engine's concurrency
// classes. It holds only mechanism: who may bypass the gate, and what a
// wake means, is caller policy.
//
// The pool never dispatches wakes itself. Operations that can expose
// free capacity return the next waiter to wake, and the caller must
// deliver that kick: a woken waiter keeps its FIFO slot until it
// actually takes a token, so a waiter that declines its wake (Acquire
// with bypass, Clear) hands the wake on through the returned kick, and a
// free token can never strand the queue.
package tokenpool

import (
	"slices"
	"sync"
	"time"
)

// Pool tracks token usage and parked waiters per key. K identifies a
// token class, W a waiter; a waiter is parked on at most one key at a
// time. The zero value is unusable — construct with New.
type Pool[K comparable, W comparable] struct {
	mu       sync.Mutex
	capacity map[K]int
	classes  map[K]*class[W]
	parked   map[W]park[K]
}

type class[W comparable] struct {
	capacity int
	inUse    int
	waiters  []W // FIFO
}

// park records the key a waiter is parked on and since when.
type park[K comparable] struct {
	key   K
	since time.Time
}

// Usage is a point-in-time snapshot of one key's token occupancy.
type Usage struct {
	Capacity int
	InUse    int
	Waiting  int
}

// New builds a Pool limiting each key in capacity to its value. Keys
// absent from capacity are unlimited: Acquire proceeds without a token.
// The map is copied.
func New[K comparable, W comparable](capacity map[K]int) *Pool[K, W] {
	p := &Pool[K, W]{
		capacity: make(map[K]int, len(capacity)),
		classes:  make(map[K]*class[W]),
		parked:   make(map[W]park[K]),
	}
	for k, c := range capacity {
		p.capacity[k] = c
	}
	return p
}

// Acquire gates waiter w on key k. granted=false parks w as k's FIFO
// tail (or keeps its existing slot and park time on a re-check).
// bypass=true — or an unlimited key — proceeds without taking a token
// (held=false). waited is how long w had been parked before this token
// grant, and stays zero on token-less proceeds so a bypass never reports
// a phantom grant.
//
// kicks names every waiter the caller must wake: an outcome that removes
// w's park can expose free capacity on w's prior key, and a grant with
// capacity still free cascades the wake down k's queue — a cross-key
// grant can owe both at once.
func (p *Pool[K, W]) Acquire(k K, w W, bypass bool, now time.Time) (granted, held bool, waited time.Duration, kicks []W) {
	p.mu.Lock()
	prior, wasParked := p.parked[w]

	// unpark removes w's park state (FIFO slot included). Its key's
	// exposed capacity is handed on — a declined wake (bypass, key
	// change) must not strand the queue — except when the caller is
	// about to consume that same key's capacity itself (samePending):
	// the post-grant cascade below is then authoritative.
	unpark := func(samePending bool) {
		if !wasParked {
			return
		}
		delete(p.parked, w)
		if c := p.classes[prior.key]; c != nil {
			c.waiters = slices.DeleteFunc(c.waiters, func(x W) bool { return x == w })
			if !samePending {
				if x, ok := nextWaiter(c); ok {
					kicks = append(kicks, x)
				}
			}
		}
	}

	if bypass {
		unpark(false)
		p.mu.Unlock()
		return true, false, 0, kicks
	}
	cap, limited := p.capacity[k]
	if !limited {
		unpark(false)
		p.mu.Unlock()
		return true, false, 0, kicks
	}
	c := p.classes[k]
	if c == nil {
		c = &class[W]{capacity: cap}
		p.classes[k] = c
	}
	if c.inUse < c.capacity {
		unpark(wasParked && prior.key == k)
		c.inUse++
		// Cascade: with capacity still free and others queued (two
		// releases before the head ran), the head alone was woken —
		// wake the next in line too.
		if x, ok := nextWaiter(c); ok {
			kicks = append(kicks, x)
		}
		p.mu.Unlock()
		if wasParked {
			waited = now.Sub(prior.since)
		}
		return true, true, waited, kicks
	}
	if !slices.Contains(c.waiters, w) {
		c.waiters = append(c.waiters, w)
	}
	since := now
	if wasParked {
		since = prior.since // keep the original park time across re-checks
	}
	p.parked[w] = park[K]{key: k, since: since}
	p.mu.Unlock()
	return false, false, 0, nil
}

// Release returns a token of k and names the head waiter to wake, if
// any; the caller must deliver the kick. The woken waiter keeps its FIFO
// slot until it actually acquires: if it declines (bypassing or cleared
// meanwhile), its unpark hands the wake to the next in line.
func (p *Pool[K, W]) Release(k K) (kick W, ok bool) {
	p.mu.Lock()
	if c := p.classes[k]; c != nil {
		c.inUse--
		kick, ok = nextWaiter(c)
	}
	p.mu.Unlock()
	return kick, ok
}

// Clear removes every trace of w's park — for waiters that resolve
// without passing through Acquire again — and names the next waiter to
// wake if the departure exposed free capacity; the caller must deliver
// the kick.
func (p *Pool[K, W]) Clear(w W) (kick W, ok bool) {
	p.mu.Lock()
	pk, parked := p.parked[w]
	if !parked {
		p.mu.Unlock()
		return kick, false
	}
	delete(p.parked, w)
	if c := p.classes[pk.key]; c != nil {
		c.waiters = slices.DeleteFunc(c.waiters, func(x W) bool { return x == w })
		kick, ok = nextWaiter(c)
	}
	p.mu.Unlock()
	return kick, ok
}

// ParkedOn reports the key w is currently parked on, if any.
func (p *Pool[K, W]) ParkedOn(w W) (K, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pk, ok := p.parked[w]
	return pk.key, ok
}

// Snapshot reports per-key token occupancy for keys used since
// construction.
func (p *Pool[K, W]) Snapshot() map[K]Usage {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.classes) == 0 {
		return nil
	}
	out := make(map[K]Usage, len(p.classes))
	for k, c := range p.classes {
		out[k] = Usage{Capacity: c.capacity, InUse: c.inUse, Waiting: len(c.waiters)}
	}
	return out
}

// nextWaiter names the head waiter to wake when the class has free
// capacity. Callers hold p.mu.
func nextWaiter[W comparable](c *class[W]) (W, bool) {
	if c.inUse < c.capacity && len(c.waiters) > 0 {
		return c.waiters[0], true
	}
	var zero W
	return zero, false
}
