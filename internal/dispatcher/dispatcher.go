// Package dispatcher provides a keyed single-flight worker scheduler
// with re-poke — the mechanism behind the engine's run dispatching. At
// most one worker exists per key, global concurrency is bounded, a
// worker may start after a delay that a Wake can cut short, and a
// dispatch suppressed by the one-worker guard is recorded as a re-poke
// and honored when the current worker exits — so a wake arriving in the
// window between a worker's last decision and its exit is never lost.
//
// The dispatcher holds only mechanism: what a key means and what its
// worker does is caller policy, supplied as the Run callback.
package dispatcher

import (
	"context"
	"sync"
	"time"

	"github.com/dangra/durable/internal/watchset"
)

// Clock is the subset of the engine clock the dispatcher needs.
type Clock interface {
	After(d time.Duration) <-chan time.Time
}

// Config assembles a Dispatcher.
type Config[K comparable] struct {
	// Ctx bounds every worker: a done context stops new work and lets
	// waiting workers exit.
	Ctx context.Context
	// Concurrency bounds globally concurrent Run invocations.
	Concurrency int
	// Clock times dispatch delays.
	Clock Clock
	// Spawn starts a worker goroutine; the caller supplies it so worker
	// lifetimes join the caller's shutdown tracking (e.g. WaitGroup.Go).
	Spawn func(func())
	// Run processes one key until it needs to stop: it returns the
	// delay before the next dispatch and whether one is wanted at all.
	Run func(k K) (redispatchIn time.Duration, again bool)
}

// Dispatcher runs at most one worker per key. The zero value is
// unusable — construct with New.
type Dispatcher[K comparable] struct {
	cfg   Config[K]
	sem   chan struct{}
	wakes watchset.Signals[K]

	mu     sync.Mutex
	active map[K]struct{}
	repoke map[K]struct{}
}

// New builds a Dispatcher from cfg.
func New[K comparable](cfg Config[K]) *Dispatcher[K] {
	return &Dispatcher[K]{
		cfg:    cfg,
		sem:    make(chan struct{}, cfg.Concurrency),
		active: make(map[K]struct{}),
		repoke: make(map[K]struct{}),
	}
}

// Dispatch schedules a worker for k after delay. If k already has a
// worker, the dispatch is recorded as a re-poke instead and honored —
// immediately, overriding any redispatch delay the worker asks for,
// since a poke may be urgent — when that worker exits.
func (d *Dispatcher[K]) Dispatch(k K, delay time.Duration) {
	d.mu.Lock()
	if _, dup := d.active[k]; dup {
		d.repoke[k] = struct{}{}
		d.mu.Unlock()
		return
	}
	d.active[k] = struct{}{}
	d.mu.Unlock()

	d.cfg.Spawn(func() {
		if delay > 0 {
			wake := d.wakes.Arm(k)
			select {
			case <-d.cfg.Clock.After(delay):
			case <-wake:
			case <-d.cfg.Ctx.Done():
				d.wakes.Disarm(k)
				d.clearActive(k)
				return
			}
			d.wakes.Disarm(k)
		}
		select {
		case d.sem <- struct{}{}:
		case <-d.cfg.Ctx.Done():
			d.clearActive(k)
			return
		}
		redispatchIn, again := d.cfg.Run(k)
		<-d.sem
		// A suppressed dispatch is urgent: redispatch immediately even
		// over a requested delay — the next Run re-derives any
		// remaining wait.
		if d.clearActive(k) {
			redispatchIn, again = 0, true
		}
		if again && d.cfg.Ctx.Err() == nil {
			d.Dispatch(k, redispatchIn)
		}
	})
}

// Wake cuts short the delay of k's dispatched-but-waiting worker, if
// any. It does not create a worker: pair it with Dispatch when the key
// might not have one.
func (d *Dispatcher[K]) Wake(k K) {
	d.wakes.Fire(k)
}

// clearActive releases k's worker slot and reports whether a dispatch
// was suppressed while it was held (the worker owes a re-poke).
func (d *Dispatcher[K]) clearActive(k K) (repoke bool) {
	d.mu.Lock()
	delete(d.active, k)
	_, repoke = d.repoke[k]
	delete(d.repoke, k)
	d.mu.Unlock()
	return repoke
}

// IsActive reports whether k currently has a worker (running or in a
// delayed dispatch).
func (d *Dispatcher[K]) IsActive(k K) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.active[k]
	return ok
}

// Active reports the number of keys with a live worker.
func (d *Dispatcher[K]) Active() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.active)
}

// Delayed reports the number of workers waiting out a dispatch delay.
func (d *Dispatcher[K]) Delayed() int {
	return d.wakes.Len()
}
