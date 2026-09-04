package tokenpool

import (
	"testing"
	"time"
)

// FuzzOps drives arbitrary Acquire/Release/Clear sequences over a small
// key and waiter space, checking after every operation that the pool's
// observable state stays consistent: token counts match an exact
// shadow model, capacity is never exceeded, parked waiters and FIFO
// occupancy agree, and every returned kick names a currently-parked
// waiter. Releases are issued only for held tokens — releasing an
// unheld token is a documented panic, tested separately.
func FuzzOps(f *testing.F) {
	f.Add([]byte{0, 0, 0, 7, 2, 0, 0, 14, 3, 7})
	f.Add([]byte{0, 1, 0, 2, 0, 3, 2, 1, 3, 2, 0, 4, 1, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		keys := []string{"one", "two", "unlimited"}
		waiters := []string{"a", "b", "c", "d", "e", "f"}
		p := New[string, string](map[string]int{"one": 1, "two": 2})
		held := map[string]int{}      // key -> tokens the model believes are held
		holder := map[string]string{} // waiter -> key it holds a token of
		now := time.Unix(0, 0)

		check := func(kicks []string) {
			t.Helper()
			snap := p.Snapshot()
			if _, ok := snap["unlimited"]; ok {
				t.Fatal("unlimited key acquired a class entry")
			}
			parked := 0
			for _, w := range waiters {
				if k, ok := p.ParkedOn(w); ok {
					parked++
					if u := snap[k]; u.Waiting < 1 {
						t.Fatalf("waiter %q parked on %q but Waiting=%d", w, k, u.Waiting)
					}
				}
			}
			waiting := 0
			for k, u := range snap {
				if u.InUse < 0 || u.InUse > u.Capacity {
					t.Fatalf("key %q usage out of range: %+v", k, u)
				}
				if u.InUse != held[k] {
					t.Fatalf("key %q InUse=%d, model holds %d", k, u.InUse, held[k])
				}
				waiting += u.Waiting
			}
			if waiting != parked {
				t.Fatalf("FIFO occupancy %d != parked waiters %d", waiting, parked)
			}
			for _, kick := range kicks {
				if _, ok := p.ParkedOn(kick); !ok {
					t.Fatalf("kick %q names a waiter that is not parked", kick)
				}
			}
		}

		for i := 0; i+1 < len(data); i += 2 {
			op, arg := data[i]%4, data[i+1]
			w := waiters[int(arg)%len(waiters)]
			k := keys[int(arg/6)%len(keys)]
			now = now.Add(time.Millisecond)
			switch op {
			case 0, 1: // acquire (op 1 with bypass)
				if _, isHolder := holder[w]; isHolder {
					continue // one operation per waiter at a time, as in the engine
				}
				bypass := op == 1
				granted, heldTok, waited, kicks := p.Acquire(k, w, bypass, now)
				if heldTok {
					if !granted {
						t.Fatal("held without granted")
					}
					held[k]++
					holder[w] = k
					if _, stillParked := p.ParkedOn(w); stillParked {
						t.Fatalf("waiter %q granted a token while still parked", w)
					}
				}
				if !heldTok && waited != 0 {
					t.Fatalf("token-less proceed reported waited=%v", waited)
				}
				check(kicks)
			case 2: // release one of w's held tokens, if any
				if k, ok := holder[w]; ok {
					kick, kicked := p.Release(k)
					held[k]--
					delete(holder, w)
					if kicked {
						check([]string{kick})
					} else {
						check(nil)
					}
				}
			case 3: // clear w's park
				kick, kicked := p.Clear(w)
				if kicked {
					check([]string{kick})
				} else {
					check(nil)
				}
			}
		}
	})
}
