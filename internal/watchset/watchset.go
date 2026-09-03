// Package watchset provides the keyed wait primitives the engine's
// wake-up plumbing is built on: a one-shot broadcast Set and a
// single-slot Signals. Both hold only mechanism — what a key means, and
// when to notify or fire it, is caller policy.
//
// Neither primitive stores state about the thing being awaited, so both
// are subject to the missed-wakeup discipline: register FIRST, then
// re-check the watched state, and only then block. A notification that
// raced the registration is then caught by the re-check, and one that
// arrives after it closes or fills the returned channel.
package watchset

import "sync"

// Set is a keyed one-shot broadcast. Watch registers a channel closed at
// the key's next Notify; Notify wakes and clears every current watcher
// of the key, so a Watch after a Notify waits for the next one.
//
// The zero value is ready to use.
type Set[K comparable] struct {
	mu       sync.Mutex
	watchers map[K][]chan struct{}
}

// Watch registers a watcher for k and returns the channel closed at k's
// next Notify, plus a cancel that unregisters the watcher if it has not
// fired (idempotent, and a no-op after Notify). Callers that may return
// without being notified must cancel, or the registration lingers until
// the key's next Notify.
func (s *Set[K]) Watch(k K) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	if s.watchers == nil {
		s.watchers = make(map[K][]chan struct{})
	}
	s.watchers[k] = append(s.watchers[k], ch)
	s.mu.Unlock()
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		chs := s.watchers[k]
		for i, c := range chs {
			if c == ch {
				s.watchers[k] = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(s.watchers[k]) == 0 {
			delete(s.watchers, k)
		}
	}
	return ch, cancel
}

// Notify closes and clears all current watchers of k.
func (s *Set[K]) Notify(k K) {
	s.mu.Lock()
	chs := s.watchers[k]
	delete(s.watchers, k)
	s.mu.Unlock()
	for _, ch := range chs {
		close(ch)
	}
}

// Signals holds at most one armed waiter per key: Arm registers (and
// displaces any previous registration for the key), Fire delivers
// without blocking — buffered, so a Fire between Arm and the receive
// still wakes — and a Fire with nothing armed is a no-op.
//
// The zero value is ready to use.
type Signals[K comparable] struct {
	mu    sync.Mutex
	armed map[K]chan struct{}
}

// Arm registers the key's waiter and returns the channel Fire delivers
// on. Pair with Disarm once the wait is over.
func (s *Signals[K]) Arm(k K) <-chan struct{} {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	if s.armed == nil {
		s.armed = make(map[K]chan struct{})
	}
	s.armed[k] = ch
	s.mu.Unlock()
	return ch
}

// Disarm unregisters the key's waiter, if any.
func (s *Signals[K]) Disarm(k K) {
	s.mu.Lock()
	delete(s.armed, k)
	s.mu.Unlock()
}

// Fire wakes the key's armed waiter without blocking. Firing an unarmed
// key is a no-op, and fires while the single buffered slot is still
// undelivered coalesce into it; once the waiter has drained the slot, a
// later Fire delivers again.
func (s *Signals[K]) Fire(k K) {
	s.mu.Lock()
	ch := s.armed[k]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Len reports the number of currently armed keys.
func (s *Signals[K]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.armed)
}
