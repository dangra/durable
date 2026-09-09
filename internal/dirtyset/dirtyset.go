// Package dirtyset provides a keyed, sticky, test-and-clear flag: the
// polled form of an auto-reset event. Mark says "your copy of k may be
// stale"; Take asks whether anyone said so since the last Take, and
// clears it. A mark carries no payload and no count — marks between two
// Takes coalesce into one — so the holder's only response is to reload,
// which is what keeps it one bit.
//
// A mark latches: unlike a fire with nobody armed, it is remembered until
// taken. The missed-update discipline is the polled one: Take FIRST, then
// read — a write that lands after the Take is seen at the next Take, and
// a write before it is already in what the read returns, provided
// writers mark after they write.
package dirtyset

import "sync"

// Set is a set of marked keys. The zero value is ready to use, and it is
// safe for concurrent use.
type Set[K comparable] struct {
	mu     sync.Mutex
	marked map[K]struct{}
}

// Mark flags k. Marking a marked key is a no-op.
func (s *Set[K]) Mark(k K) {
	s.mu.Lock()
	if s.marked == nil {
		s.marked = make(map[K]struct{})
	}
	s.marked[k] = struct{}{}
	s.mu.Unlock()
}

// Take clears k's mark and reports whether it was set.
func (s *Set[K]) Take(k K) bool {
	s.mu.Lock()
	_, ok := s.marked[k]
	delete(s.marked, k)
	s.mu.Unlock()
	return ok
}
