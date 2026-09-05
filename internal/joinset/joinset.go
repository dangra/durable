// Package joinset provides a keyed join: waiters register on a set of
// keys with a threshold of how many must complete; keys complete
// monotonically, and completing one marks it on every waiter registered
// for it and reports the waiters that just crossed their threshold. It
// is the mechanism behind the engine's multi-target parks. It holds only
// mechanism: what a key is, how its real state is observed, and what a
// kick means are caller policy.
//
// The missed-completion discipline is the caller's: register FIRST, then
// observe each key's current state (marking the done ones), and only
// then wait. A completion that raced the registration is caught by the
// observation; one after it, by Complete.
//
// The set never dispatches wakes itself: Complete returns the waiters to
// kick, and the caller delivers. A waiter is kicked at most once, when a
// completion takes it from below its threshold to at or above it; a
// waiter the caller marked past its threshold is the caller's to act on.
package joinset

import (
	"sync"
	"time"
)

// Set tracks, per waiter, its keys, its threshold, and which keys are
// done. K identifies a key, W a waiter; a waiter is registered at most
// once at a time, and its keys are fixed until Remove. The zero value is
// ready to use.
type Set[K, W comparable] struct {
	mu      sync.Mutex
	waiters map[W]*waiter[K]
	byKey   map[K]map[W]struct{}
}

type waiter[K comparable] struct {
	keys  []K
	need  int
	done  map[K]struct{}
	since time.Time
	// kicked records that Complete already reported the waiter, so a
	// later completion beyond the threshold does not report it again.
	kicked bool
}

// Register adds w waiting on keys until at least need of them are done,
// stamped with now, and reports whether the registration is new. An
// already-registered waiter is left exactly as it was — its keys, its
// done set, and its stamp — so a caller re-entering after a kick can
// tell a fresh park from a re-run. Duplicate keys collapse; need is
// clamped to [1, len(keys)] (0 for no keys, which is then never
// satisfied by Complete).
func (s *Set[K, W]) Register(w W, keys []K, need int, now time.Time) (fresh bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiters == nil {
		s.waiters = make(map[W]*waiter[K])
		s.byKey = make(map[K]map[W]struct{})
	}
	if _, exists := s.waiters[w]; exists {
		return false
	}
	wt := &waiter[K]{done: make(map[K]struct{}), since: now}
	for _, k := range keys {
		members := s.byKey[k]
		if members == nil {
			members = make(map[W]struct{})
			s.byKey[k] = members
		}
		if _, dup := members[w]; dup {
			continue
		}
		members[w] = struct{}{}
		wt.keys = append(wt.keys, k)
	}
	wt.need = min(max(need, 1), len(wt.keys))
	s.waiters[w] = wt
	return true
}

// Mark records that the caller observed key k done for waiter w. It is a
// no-op for an unregistered waiter or a key outside its set, and never
// kicks: the caller that observed the key checks Done itself.
func (s *Set[K, W]) Mark(w W, k K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wt := s.waiters[w]
	if wt == nil {
		return
	}
	if _, member := s.byKey[k][w]; !member {
		return
	}
	wt.done[k] = struct{}{}
}

// Complete records key k done for every waiter registered on it and
// returns the waiters this completion satisfied: those whose done count
// reached their threshold now, and were not already reported. Order is
// unspecified.
func (s *Set[K, W]) Complete(k K) (kicks []W) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.byKey[k] {
		wt := s.waiters[w]
		wt.done[k] = struct{}{}
		if !wt.kicked && wt.need > 0 && len(wt.done) >= wt.need {
			wt.kicked = true
			kicks = append(kicks, w)
		}
	}
	return kicks
}

// Done returns w's done keys in registration order, and whether w is
// registered.
func (s *Set[K, W]) Done(w W) (keys []K, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wt := s.waiters[w]
	if wt == nil {
		return nil, false
	}
	for _, k := range wt.keys {
		if _, d := wt.done[k]; d {
			keys = append(keys, k)
		}
	}
	return keys, true
}

// Remove drops w's registration and returns its Register stamp; ok is
// false when w was not registered.
func (s *Set[K, W]) Remove(w W) (since time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wt := s.waiters[w]
	if wt == nil {
		return time.Time{}, false
	}
	delete(s.waiters, w)
	for _, k := range wt.keys {
		members := s.byKey[k]
		delete(members, w)
		if len(members) == 0 {
			delete(s.byKey, k)
		}
	}
	return wt.since, true
}

// Len is the number of registered waiters.
func (s *Set[K, W]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters)
}
