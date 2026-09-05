package joinset

import (
	"slices"
	"testing"
	"time"
)

// FuzzOps drives arbitrary Register/Mark/Complete/Remove sequences over a
// small key and waiter space against an exact shadow model, checking
// after every operation that the set's observable state matches: Done
// per waiter, Len, and — the property the engine relies on — that
// Complete kicks exactly the waiters the model says just crossed their
// threshold, each at most once per registration. The reverse index is
// checked for consistency with the registrations too.
func FuzzOps(f *testing.F) {
	f.Add([]byte{0, 0, 7, 2, 0, 1, 3, 2, 5, 2, 6, 3, 0})
	f.Add([]byte{0, 3, 0, 1, 12, 2, 0, 2, 1, 2, 2, 3, 4, 0, 0, 3, 0})
	f.Add([]byte{0, 63, 5, 1, 2, 0, 2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 3, 0, 0, 63, 5})
	f.Fuzz(func(t *testing.T, data []byte) {
		keys := []string{"a", "b", "c", "d", "e", "f"}
		waiters := []string{"p", "q", "r", "s"}
		var s Set[string, string]
		now := time.Unix(1_700_000_000, 0)

		type model struct {
			keys   []string
			need   int
			done   map[string]bool
			kicked bool
			since  time.Time
		}
		reg := map[string]*model{}

		check := func(op string, kicks []string, wantKicks []string) {
			t.Helper()
			slices.Sort(kicks)
			slices.Sort(wantKicks)
			if !slices.Equal(kicks, wantKicks) {
				t.Fatalf("%s: kicks = %v, model wants %v", op, kicks, wantKicks)
			}
			if got := s.Len(); got != len(reg) {
				t.Fatalf("%s: Len = %d, model has %d", op, got, len(reg))
			}
			for _, w := range waiters {
				done, ok := s.Done(w)
				m := reg[w]
				if ok != (m != nil) {
					t.Fatalf("%s: Done(%q) registered=%v, model %v", op, w, ok, m != nil)
				}
				if m == nil {
					continue
				}
				var want []string
				for _, k := range m.keys {
					if m.done[k] {
						want = append(want, k)
					}
				}
				if !slices.Equal(done, want) {
					t.Fatalf("%s: Done(%q) = %v, model %v", op, w, done, want)
				}
			}
			// Index consistency, from inside the package.
			s.mu.Lock()
			defer s.mu.Unlock()
			for k, members := range s.byKey {
				if len(members) == 0 {
					t.Fatalf("%s: empty index entry for key %q", op, k)
				}
				for w := range members {
					m := reg[w]
					if m == nil || !slices.Contains(m.keys, k) {
						t.Fatalf("%s: index lists %q under %q, model does not", op, w, k)
					}
				}
			}
			for w, m := range reg {
				for _, k := range m.keys {
					if _, ok := s.byKey[k][w]; !ok {
						t.Fatalf("%s: model has %q on %q, index does not", op, w, k)
					}
				}
			}
		}

		for i := 0; i+1 < len(data); i += 2 {
			op, arg := data[i], data[i+1]
			w := waiters[int(arg)%len(waiters)]
			switch op % 4 {
			case 0: // Register: bits of arg select keys, its high bits the threshold.
				var ks []string
				for b, k := range keys {
					if arg&(1<<b) != 0 {
						ks = append(ks, k)
					}
				}
				// Some duplicates, sometimes.
				if arg%5 == 0 && len(ks) > 0 {
					ks = append(ks, ks[0])
				}
				need := int(arg>>6) + int(op>>2)%4 - 1 // may be <1 or >len
				now = now.Add(time.Second)
				fresh := s.Register(w, ks, need, now)
				if m := reg[w]; m != nil {
					if fresh {
						t.Fatalf("Register(%q) fresh while registered", w)
					}
					check("Register(dup)", nil, nil)
					continue
				}
				if !fresh {
					t.Fatalf("Register(%q) not fresh while unregistered", w)
				}
				var uniq []string
				for _, k := range ks {
					if !slices.Contains(uniq, k) {
						uniq = append(uniq, k)
					}
				}
				reg[w] = &model{keys: uniq, need: min(max(need, 1), len(uniq)), done: map[string]bool{}, since: now}
				check("Register", nil, nil)
			case 1: // Mark
				k := keys[int(arg>>2)%len(keys)]
				s.Mark(w, k)
				if m := reg[w]; m != nil && slices.Contains(m.keys, k) {
					m.done[k] = true
				}
				check("Mark", nil, nil)
			case 2: // Complete
				k := keys[int(arg)%len(keys)]
				var want []string
				for w, m := range reg {
					if !slices.Contains(m.keys, k) {
						continue
					}
					m.done[k] = true
					if !m.kicked && m.need > 0 && len(m.done) >= m.need {
						m.kicked = true
						want = append(want, w)
					}
				}
				check("Complete", s.Complete(k), want)
			case 3: // Remove
				since, ok := s.Remove(w)
				m := reg[w]
				if ok != (m != nil) {
					t.Fatalf("Remove(%q) ok=%v, model registered=%v", w, ok, m != nil)
				}
				if ok && !since.Equal(m.since) {
					t.Fatalf("Remove(%q) since=%v, model %v", w, since, m.since)
				}
				delete(reg, w)
				check("Remove", nil, nil)
			}
		}
	})
}
