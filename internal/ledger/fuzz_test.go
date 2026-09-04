package ledger

import (
	"bytes"
	"fmt"
	"testing"
)

// decodeCase builds a topology and facts from fuzz bytes: byte 0 sizes
// the topology (1-8 steps), one flag byte per step (unwind, retired),
// then one forward-state and one unwind-state byte per step. State
// value 4 decodes to a corrupt out-of-range OpState so the defensive
// branches stay exercised.
func decodeCase(data []byte) (Topology, Facts) {
	r := bytes.NewReader(data)
	next := func() byte {
		b, err := r.ReadByte()
		if err != nil {
			return 0
		}
		return b
	}
	n := int(next())%8 + 1
	topo := make(Topology, n)
	for i := range topo {
		flags := next()
		topo[i] = Step{
			ID:      fmt.Sprintf("S%d", i),
			Unwind:  flags&1 != 0,
			Retired: flags&2 != 0,
		}
	}
	states := func() map[string]OpState {
		m := make(map[string]OpState, n+1)
		for i := 0; i < n; i++ {
			switch v := next() % 6; v {
			case 4:
				m[fmt.Sprintf("S%d", i)] = OpState(99) // corrupt
			case 5: // absent: exercise missing-entry handling
			default:
				m[fmt.Sprintf("S%d", i)] = OpState(v)
			}
		}
		// A fact for a step no longer in the topology (renamed/removed).
		if next()%2 == 0 {
			m["GONE"] = OpState(next() % 4)
		}
		return m
	}
	return topo, Facts{Forward: states(), Unwind: states()}
}

func checkDecision(t *testing.T, topo Topology, dec Decision, unwindPhase bool, f Facts) {
	t.Helper()
	switch dec.Kind {
	case KindRunForward:
		if _, ok := topo.index(dec.Step); !ok {
			t.Fatalf("RunForward selected step %q absent from topology", dec.Step)
		}
	case KindRunUnwind:
		i, ok := topo.index(dec.Step)
		if !ok {
			t.Fatalf("RunUnwind selected step %q absent from topology", dec.Step)
		}
		if !topo[i].Unwind {
			t.Fatalf("RunUnwind selected step %q that does not declare unwind", dec.Step)
		}
		if f.Forward[dec.Step] != OpSucceeded {
			t.Fatalf("RunUnwind selected step %q whose forward op is %d", dec.Step, f.Forward[dec.Step])
		}
	case KindForwardComplete, KindUnwindComplete:
	case KindInvalid:
		if dec.Reason == "" {
			t.Fatal("KindInvalid without a reason")
		}
	default:
		t.Fatalf("unknown decision kind %d", dec.Kind)
	}
	_ = unwindPhase
}

// FuzzReconcile checks, for arbitrary topologies and (possibly corrupt)
// facts: decisions are well-formed, reconciliation is deterministic, and
// repeatedly applying the decision terminates — the engine's reconcile
// loop can never spin forever, whatever the persisted facts claim.
func FuzzReconcile(f *testing.F) {
	f.Add([]byte{3, 1, 0, 2, 2, 3, 1, 0, 0, 0, 1, 2, 3})
	f.Add([]byte{8, 1, 1, 1, 1, 1, 1, 1, 1})
	f.Add([]byte{1, 0, 4, 4, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		topo, facts := decodeCase(data)

		if a, b := NextForward(topo, facts), NextForward(topo, facts); a != b {
			t.Fatalf("NextForward nondeterministic: %+v vs %+v", a, b)
		}
		if a, b := NextUnwind(topo, facts), NextUnwind(topo, facts); a != b {
			t.Fatalf("NextUnwind nondeterministic: %+v vs %+v", a, b)
		}

		// Forward phase: resolve every selected operation as success;
		// must reach ForwardComplete or Invalid within the bound.
		fwd := Facts{Forward: cloneStates(facts.Forward), Unwind: cloneStates(facts.Unwind)}
		bound := 2*len(topo) + 2
		for i := 0; ; i++ {
			if i > bound {
				t.Fatalf("forward reconciliation did not terminate within %d steps", bound)
			}
			dec := NextForward(topo, fwd)
			checkDecision(t, topo, dec, false, fwd)
			if dec.Kind != KindRunForward {
				break
			}
			if fwd.Forward[dec.Step] == OpSucceeded {
				t.Fatalf("forward re-selected already-succeeded step %q", dec.Step)
			}
			fwd.Forward[dec.Step] = OpSucceeded
		}

		// Unwind phase: same termination property.
		uw := Facts{Forward: cloneStates(facts.Forward), Unwind: cloneStates(facts.Unwind)}
		for i := 0; ; i++ {
			if i > bound {
				t.Fatalf("unwind reconciliation did not terminate within %d steps", bound)
			}
			dec := NextUnwind(topo, uw)
			checkDecision(t, topo, dec, true, uw)
			if dec.Kind != KindRunUnwind {
				break
			}
			if st := uw.Unwind[dec.Step]; st == OpSucceeded || st == OpFailed {
				t.Fatalf("unwind re-selected already-resolved step %q", dec.Step)
			}
			uw.Unwind[dec.Step] = OpSucceeded
		}
	})
}

func cloneStates(m map[string]OpState) map[string]OpState {
	out := make(map[string]OpState, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
