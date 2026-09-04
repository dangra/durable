package watchset

import "testing"

// FuzzOps drives arbitrary single-goroutine op sequences over Set and
// Signals against exact shadow models: every watcher channel must be
// closed if and only if a Notify fired while it was registered, and a
// Signals channel must hold a pending value exactly when a Fire landed
// on the armed, undelivered slot.
func FuzzOps(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0, 0, 1, 2, 0, 1, 1, 3, 0, 4, 0, 5, 0})
	f.Add([]byte{0, 2, 0, 2, 1, 2, 2, 0, 0, 1, 1, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		keys := []string{"x", "y", "z"}

		type watcher struct {
			ch       <-chan struct{}
			cancel   func()
			key      string
			expected bool // must be closed
			canceled bool
		}
		var set Set[string]
		var watchers []*watcher

		type signal struct {
			ch      <-chan struct{}
			pending bool
		}
		var sigs Signals[string]
		armed := map[string]*signal{}

		closed := func(ch <-chan struct{}) bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		}
		verify := func() {
			t.Helper()
			for i, w := range watchers {
				if got := closed(w.ch); got != w.expected {
					t.Fatalf("watcher %d (key %q, canceled=%v): closed=%v, want %v",
						i, w.key, w.canceled, got, w.expected)
				}
			}
			wantLen := 0
			for _, s := range armed {
				if s != nil {
					wantLen++
				}
			}
			if got := sigs.Len(); got != wantLen {
				t.Fatalf("Signals.Len = %d, model has %d armed", got, wantLen)
			}
		}

		for i := 0; i+1 < len(data); i += 2 {
			op, arg := data[i]%6, data[i+1]
			k := keys[int(arg)%len(keys)]
			switch op {
			case 0: // watch
				ch, cancel := set.Watch(k)
				watchers = append(watchers, &watcher{ch: ch, cancel: cancel, key: k})
			case 1: // notify: every live watcher of k must close
				set.Notify(k)
				for _, w := range watchers {
					if w.key == k && !w.canceled {
						w.expected = true
					}
				}
			case 2: // cancel one watcher by index (idempotent, any state)
				if len(watchers) > 0 {
					w := watchers[int(arg)%len(watchers)]
					w.cancel()
					if !w.expected {
						w.canceled = true // not yet fired: never fires now
					}
				}
			case 3: // arm (displaces any previous registration of k)
				ch := sigs.Arm(k)
				armed[k] = &signal{ch: ch}
			case 4: // fire: pending exactly when armed and undelivered
				sigs.Fire(k)
				if s := armed[k]; s != nil {
					s.pending = true // coalesces if already pending
				}
			case 5: // drain or disarm, alternating on the arg
				if s := armed[k]; s != nil {
					if arg%2 == 0 {
						select {
						case <-s.ch:
							if !s.pending {
								t.Fatalf("received on %q with nothing pending", k)
							}
							s.pending = false
						default:
							if s.pending {
								t.Fatalf("pending fire on %q not receivable", k)
							}
						}
					} else {
						sigs.Disarm(k)
						armed[k] = nil
					}
				}
			}
			verify()
		}
	})
}
