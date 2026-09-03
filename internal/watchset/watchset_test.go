package watchset

import (
	"sync"
	"testing"
	"time"
)

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestSetNotifyClosesAllWatchers(t *testing.T) {
	var s Set[string]
	ch1, _ := s.Watch("a")
	ch2, _ := s.Watch("a")
	chOther, _ := s.Watch("b")
	s.Notify("a")
	if !closed(ch1) || !closed(ch2) {
		t.Fatal("watchers of the notified key not closed")
	}
	if closed(chOther) {
		t.Fatal("watcher of a different key closed")
	}
}

func TestSetWatchAfterNotifyWaitsForNext(t *testing.T) {
	var s Set[string]
	s.Notify("a") // nothing registered: no-op
	ch, _ := s.Watch("a")
	if closed(ch) {
		t.Fatal("watch after notify fired immediately")
	}
	s.Notify("a")
	if !closed(ch) {
		t.Fatal("second notify did not fire the new watcher")
	}
}

func TestSetCancelUnregisters(t *testing.T) {
	var s Set[string]
	ch1, cancel1 := s.Watch("a")
	ch2, _ := s.Watch("a")
	cancel1()
	cancel1() // idempotent
	s.Notify("a")
	if closed(ch1) {
		t.Fatal("canceled watcher was notified")
	}
	if !closed(ch2) {
		t.Fatal("surviving watcher was not notified")
	}
	cancel1() // no-op after notify cleared the key
}

// TestSetRegisterThenRecheck exercises the missed-wakeup discipline the
// package documents: a watcher registered before observing stale state
// always sees the notify that raced it.
func TestSetRegisterThenRecheck(t *testing.T) {
	var (
		s    Set[int]
		mu   sync.Mutex
		done bool
	)
	const rounds = 200
	for i := 0; i < rounds; i++ {
		mu.Lock()
		done = false
		mu.Unlock()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			mu.Lock()
			done = true
			mu.Unlock()
			s.Notify(i)
		}()
		go func() {
			defer wg.Done()
			ch, cancel := s.Watch(i) // register first...
			mu.Lock()
			d := done // ...then re-check
			mu.Unlock()
			if d {
				cancel()
				return
			}
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Error("missed wakeup")
			}
		}()
		wg.Wait()
	}
}

func TestSignalsFireWakesArmed(t *testing.T) {
	var s Signals[string]
	ch := s.Arm("a")
	s.Fire("a")
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("armed waiter not woken")
	}
	s.Disarm("a")
	if s.Len() != 0 {
		t.Fatalf("Len = %d after disarm", s.Len())
	}
}

func TestSignalsFireBeforeReceiveStillWakes(t *testing.T) {
	var s Signals[string]
	ch := s.Arm("a")
	s.Fire("a")
	s.Fire("a") // repeat fire collapses into the buffered slot
	select {
	case <-ch:
	default:
		t.Fatal("buffered fire lost")
	}
}

func TestSignalsFireUnarmedIsNoop(t *testing.T) {
	var s Signals[string]
	s.Fire("a")
	if s.Len() != 0 {
		t.Fatalf("Len = %d", s.Len())
	}
}

func TestSignalsRearmDisplaces(t *testing.T) {
	var s Signals[string]
	old := s.Arm("a")
	cur := s.Arm("a")
	s.Fire("a")
	select {
	case <-cur:
	case <-time.After(time.Second):
		t.Fatal("current registration not woken")
	}
	select {
	case <-old:
		t.Fatal("displaced registration woken")
	default:
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d", s.Len())
	}
}
