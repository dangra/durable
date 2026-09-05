package joinset

import (
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStressConcurrentOps hammers one Set from many goroutines — parkers
// registering, marking, and removing; completers finishing keys; readers
// polling Done and Len — and checks the properties the engine relies on
// under contention: a registration is kicked at most once, only once its
// threshold is met, Done never names a key outside the registration or
// shrinks while registered, and the index is exactly the registrations
// once everything has quiesced. Waiter ids are unique per registration,
// so a re-registered parker is a new waiter to the accounting.
func TestStressConcurrentOps(t *testing.T) {
	const (
		keySpace = 64
		parkers  = 32
		rounds   = 400
	)
	if testing.Short() {
		t.Skip("stress")
	}
	var (
		s      Set[int, int64]
		nextID atomic.Int64
		mu     sync.Mutex
		// live registrations by waiter id: what the accounting knows.
		regs   = map[int64]*registration{}
		kicked = map[int64]int{}
	)
	seen := func(id int64) *registration {
		mu.Lock()
		defer mu.Unlock()
		return regs[id]
	}

	var wg, pwg sync.WaitGroup // everyone; parkers alone
	completers := max(runtime.GOMAXPROCS(0)/2, 2)
	stop := make(chan struct{})

	// Completers: finish random keys for as long as the parkers run.
	for c := 0; c < completers; c++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 99))
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, w := range s.Complete(r.IntN(keySpace)) {
					reg := seen(w)
					if reg == nil {
						// Removed concurrently between Complete and here;
						// still must have been a registration.
						mu.Lock()
						kicked[w]++
						mu.Unlock()
						continue
					}
					done, ok := s.Done(w)
					mu.Lock()
					kicked[w]++
					mu.Unlock()
					if ok && len(done) < reg.need {
						t.Errorf("waiter %d kicked with %d done, need %d", w, len(done), reg.need)
					}
				}
			}
		}(uint64(c + 1))
	}

	// Parkers: register on a random key set, mark a few, poll Done for
	// monotonicity and containment, then remove; repeat.
	for p := 0; p < parkers; p++ {
		pwg.Add(1)
		go func(seed uint64) {
			defer pwg.Done()
			r := rand.New(rand.NewPCG(seed, 7))
			for round := 0; round < rounds; round++ {
				n := 1 + r.IntN(6)
				keys := make([]int, 0, n)
				for i := 0; i < n; i++ {
					keys = append(keys, r.IntN(keySpace))
				}
				need := 1 + r.IntN(n)
				id := nextID.Add(1)
				reg := &registration{keys: uniq(keys), need: min(need, len(uniq(keys)))}
				mu.Lock()
				regs[id] = reg
				mu.Unlock()
				if !s.Register(id, keys, need, time.Now()) {
					t.Errorf("fresh id %d not fresh", id)
				}
				if s.Register(id, keys, need, time.Now()) {
					t.Errorf("re-Register of %d reported fresh", id)
				}
				prev := 0
				for i := 0; i < 8; i++ {
					if r.IntN(3) == 0 {
						s.Mark(id, keys[r.IntN(len(keys))])
					}
					done, ok := s.Done(id)
					if !ok {
						t.Errorf("waiter %d vanished while registered", id)
						break
					}
					if len(done) < prev {
						t.Errorf("waiter %d Done shrank %d -> %d", id, prev, len(done))
					}
					prev = len(done)
					for _, k := range done {
						if !slices.Contains(reg.keys, k) {
							t.Errorf("waiter %d Done names %d, not one of %v", id, k, reg.keys)
						}
					}
					if sorted := slices.Sorted(slices.Values(done)); len(slices.Compact(sorted)) != len(done) {
						t.Errorf("waiter %d Done has duplicates: %v", id, done)
					}
					runtime.Gosched()
				}
				if _, ok := s.Remove(id); !ok {
					t.Errorf("Remove(%d) found nothing", id)
				}
				if _, ok := s.Remove(id); ok {
					t.Errorf("second Remove(%d) found something", id)
				}
				mu.Lock()
				delete(regs, id)
				mu.Unlock()
			}
		}(uint64(p + 1))
	}

	// Readers: Len must never exceed the parker count, and never be
	// negative in disguise.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := s.Len(); n < 0 || n > parkers {
				t.Errorf("Len = %d with %d parkers", n, parkers)
			}
			runtime.Gosched()
		}
	}()

	// Let the parkers finish, then stop the completers and reader.
	pwg.Wait()
	close(stop)
	wg.Wait()

	for id, n := range kicked {
		if n != 1 {
			t.Errorf("waiter %d kicked %d times", id, n)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiters) != 0 || len(s.byKey) != 0 {
		t.Fatalf("set not empty after every parker removed itself: %d waiters, %d index keys", len(s.waiters), len(s.byKey))
	}
	if len(kicked) == 0 {
		t.Fatal("no waiter was ever kicked; the stress did not exercise completions")
	}
}

type registration struct {
	keys []int
	need int
}

func uniq(keys []int) []int {
	var out []int
	for _, k := range keys {
		if !slices.Contains(out, k) {
			out = append(out, k)
		}
	}
	return out
}
