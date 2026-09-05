package joinset

import (
	"slices"
	"sync"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func sorted[W ~string](ws []W) []W {
	out := slices.Clone(ws)
	slices.Sort(out)
	return out
}

func TestAllOfKicksOnLastKey(t *testing.T) {
	var s Set[string, string]
	if !s.Register("p", []string{"a", "b", "c"}, 3, t0) {
		t.Fatal("first Register should be fresh")
	}
	if kicks := s.Complete("a"); len(kicks) != 0 {
		t.Fatalf("kicks after a = %v; want none", kicks)
	}
	if kicks := s.Complete("b"); len(kicks) != 0 {
		t.Fatalf("kicks after b = %v; want none", kicks)
	}
	if done, ok := s.Done("p"); !ok || !slices.Equal(done, []string{"a", "b"}) {
		t.Fatalf("Done = %v, %v; want [a b]", done, ok)
	}
	if kicks := s.Complete("c"); !slices.Equal(kicks, []string{"p"}) {
		t.Fatalf("kicks after c = %v; want [p]", kicks)
	}
	// Completing again past the threshold reports nothing more.
	if kicks := s.Complete("c"); len(kicks) != 0 {
		t.Fatalf("kicks after repeated c = %v; want none", kicks)
	}
	if done, _ := s.Done("p"); !slices.Equal(done, []string{"a", "b", "c"}) {
		t.Fatalf("Done = %v; want all, in registration order", done)
	}
}

func TestAnyOfKicksOnceOnFirstKey(t *testing.T) {
	var s Set[string, string]
	s.Register("p", []string{"a", "b", "c"}, 1, t0)
	if kicks := s.Complete("b"); !slices.Equal(kicks, []string{"p"}) {
		t.Fatalf("kicks after b = %v; want [p]", kicks)
	}
	if kicks := s.Complete("a"); len(kicks) != 0 {
		t.Fatalf("kicks after a = %v; want none: already reported", kicks)
	}
	if done, _ := s.Done("p"); !slices.Equal(done, []string{"a", "b"}) {
		t.Fatalf("Done = %v; want [a b] in registration order, not completion order", done)
	}
}

func TestMarkCountsButNeverKicks(t *testing.T) {
	var s Set[string, string]
	s.Register("p", []string{"a", "b"}, 2, t0)
	s.Mark("p", "a")
	s.Mark("p", "zzz") // not one of p's keys
	s.Mark("q", "a")   // not registered
	if done, _ := s.Done("p"); !slices.Equal(done, []string{"a"}) {
		t.Fatalf("Done = %v; want [a]", done)
	}
	// The caller marked p to its threshold itself; Complete on the last
	// key crosses nothing new... but p was never reported, and the
	// completion does take it to the threshold: it is reported now.
	s.Mark("p", "b")
	if kicks := s.Complete("b"); !slices.Equal(kicks, []string{"p"}) {
		t.Fatalf("kicks = %v; want [p]: never reported before", kicks)
	}
}

func TestSharedKeyFansOut(t *testing.T) {
	var s Set[string, string]
	s.Register("p1", []string{"a", "b"}, 2, t0)
	s.Register("p2", []string{"a"}, 1, t0)
	s.Register("p3", []string{"a", "c"}, 1, t0)
	kicks := sorted(s.Complete("a"))
	if !slices.Equal(kicks, []string{"p2", "p3"}) {
		t.Fatalf("kicks after a = %v; want [p2 p3]", kicks)
	}
	if kicks := s.Complete("b"); !slices.Equal(kicks, []string{"p1"}) {
		t.Fatalf("kicks after b = %v; want [p1]", kicks)
	}
	if kicks := s.Complete("nobody-waits"); len(kicks) != 0 {
		t.Fatalf("kicks for an unwatched key = %v", kicks)
	}
}

func TestRegisterTwiceKeepsTheFirst(t *testing.T) {
	var s Set[string, string]
	s.Register("p", []string{"a", "b"}, 2, t0)
	s.Complete("a")
	if s.Register("p", []string{"x"}, 1, t0.Add(time.Hour)) {
		t.Fatal("second Register should not be fresh")
	}
	done, _ := s.Done("p")
	if !slices.Equal(done, []string{"a"}) {
		t.Fatalf("Done = %v; want the original registration's [a]", done)
	}
	if since, ok := s.Remove("p"); !ok || !since.Equal(t0) {
		t.Fatalf("Remove = %v, %v; want the original stamp", since, ok)
	}
}

func TestRemoveCleansTheIndex(t *testing.T) {
	var s Set[string, string]
	s.Register("p", []string{"a", "b"}, 2, t0)
	s.Register("q", []string{"a"}, 1, t0)
	if since, ok := s.Remove("p"); !ok || !since.Equal(t0) {
		t.Fatalf("Remove p = %v, %v", since, ok)
	}
	if _, ok := s.Remove("p"); ok {
		t.Fatal("second Remove should report not registered")
	}
	if _, ok := s.Done("p"); ok {
		t.Fatal("Done after Remove should report not registered")
	}
	if kicks := s.Complete("a"); !slices.Equal(kicks, []string{"q"}) {
		t.Fatalf("kicks after a = %v; want only q", kicks)
	}
	if kicks := s.Complete("b"); len(kicks) != 0 {
		t.Fatalf("kicks after b = %v; p is gone", kicks)
	}
	// q is satisfied but stays registered — and indexed — until the caller
	// removes it: a satisfied waiter is the caller's to act on.
	if s.Len() != 1 || len(s.byKey) != 1 || len(s.byKey["a"]) != 1 {
		t.Fatalf("Len = %d, byKey = %v; want q alone, still indexed under a", s.Len(), s.byKey)
	}
	s.Remove("q")
	if s.Len() != 0 || len(s.byKey) != 0 {
		t.Fatalf("Len = %d, byKey = %v; want an empty set and index", s.Len(), s.byKey)
	}
}

func TestDuplicateKeysAndClamping(t *testing.T) {
	var s Set[string, string]
	s.Register("p", []string{"a", "a", "b"}, 5, t0) // need clamps to 2
	if kicks := s.Complete("a"); len(kicks) != 0 {
		t.Fatalf("kicks after a = %v; want none", kicks)
	}
	if kicks := s.Complete("b"); !slices.Equal(kicks, []string{"p"}) {
		t.Fatalf("kicks after b = %v; want [p]", kicks)
	}
	if done, _ := s.Done("p"); !slices.Equal(done, []string{"a", "b"}) {
		t.Fatalf("Done = %v; want [a b] without the duplicate", done)
	}
	s.Register("z", []string{"a", "b"}, 0, t0) // need clamps to 1
	if kicks := s.Complete("b"); !slices.Equal(kicks, []string{"z"}) {
		t.Fatalf("kicks after b for z = %v; want [z]", kicks)
	}
	s.Register("e", nil, 1, t0) // no keys: registered, never satisfied
	if s.Len() != 3 {
		t.Fatalf("Len = %d; want 3", s.Len())
	}
	if kicks := s.Complete("a"); len(kicks) != 0 {
		t.Fatalf("kicks = %v; e has no keys", kicks)
	}
}

// Every waiter is reported exactly once across concurrent completions,
// registrations, and removals.
func TestConcurrentCompletionsReportEachWaiterOnce(t *testing.T) {
	var s Set[int, int]
	const waiters, keys = 200, 50
	for w := 0; w < waiters; w++ {
		ks := make([]int, 0, keys)
		for k := w % keys; len(ks) < 5; k = (k + 7) % keys {
			ks = append(ks, k)
		}
		s.Register(w, ks, len(ks), t0)
	}
	var (
		mu     sync.Mutex
		kicked = map[int]int{}
		wg     sync.WaitGroup
	)
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for _, w := range s.Complete(k) {
				mu.Lock()
				kicked[w]++
				mu.Unlock()
			}
		}(k)
	}
	wg.Wait()
	if len(kicked) != waiters {
		t.Fatalf("kicked %d waiters; want %d", len(kicked), waiters)
	}
	for w, n := range kicked {
		if n != 1 {
			t.Fatalf("waiter %d kicked %d times", w, n)
		}
	}
}
