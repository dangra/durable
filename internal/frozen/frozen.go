// Package frozen provides a map that is written during a configuration
// phase and read, without locking, forever after: Put until Freeze, then
// Get. It turns "nobody writes this after startup" from a comment into a
// runtime assertion — a Put after Freeze panics at the offending call
// site — while keeping the post-freeze read path a single atomic load
// ahead of a plain map lookup.
//
// Before Freeze, reads and writes are serialized by a mutex, so a
// configuration phase may interleave them freely. Freeze publishes every
// earlier write: a Get that observes the frozen flag happens-after the
// last Put, by the synchronization the atomic flag provides.
package frozen

import (
	"maps"
	"sync"
	"sync/atomic"
)

// Map is a write-then-read-only map. The zero value is ready to use.
type Map[K comparable, V any] struct {
	mu     sync.Mutex
	frozen atomic.Bool
	m      map[K]V
}

// Put stores v under k. It panics once the map is frozen.
func (f *Map[K, V]) Put(k K, v V) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.frozen.Load() {
		panic("frozen: Put after Freeze")
	}
	if f.m == nil {
		f.m = make(map[K]V)
	}
	f.m[k] = v
}

// Get returns the value under k and whether one is present. After Freeze
// it takes no lock.
func (f *Map[K, V]) Get(k K) (V, bool) {
	if f.frozen.Load() {
		v, ok := f.m[k]
		return v, ok
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[k]
	return v, ok
}

// Len is the number of entries.
func (f *Map[K, V]) Len() int {
	if f.frozen.Load() {
		return len(f.m)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.m)
}

// Range calls fn for each entry, in unspecified order, until fn returns
// false. fn may use the map: before Freeze, Range iterates a snapshot
// taken under the lock, so a Put from fn lands but is not visited.
func (f *Map[K, V]) Range(fn func(K, V) bool) {
	var m map[K]V
	if f.frozen.Load() {
		m = f.m
	} else {
		f.mu.Lock()
		m = maps.Clone(f.m)
		f.mu.Unlock()
	}
	for k, v := range m {
		if !fn(k, v) {
			return
		}
	}
}

// Freeze ends the write phase: every Put so far is visible to every later
// lock-free Get, and any further Put panics. Freezing twice is a no-op.
func (f *Map[K, V]) Freeze() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frozen.Store(true)
}

// Frozen reports whether Freeze has been called.
func (f *Map[K, V]) Frozen() bool { return f.frozen.Load() }
