package ledger

import "testing"

func step(id string) Step                { return Step{ID: id} }
func unwindable(id string) Step          { return Step{ID: id, Unwind: true} }
func retired(id string) Step             { return Step{ID: id, Retired: true} }
func retiredUnwindable(id string) Step   { return Step{ID: id, Unwind: true, Retired: true} }
func forward(m map[string]OpState) Facts { return Facts{Forward: m} }

func TestNextForward(t *testing.T) {
	tests := []struct {
		name  string
		topo  Topology
		facts Facts
		want  Decision
	}{
		{
			name: "empty ledger starts at first step",
			topo: Topology{step("A"), step("B")},
			want: Decision{Kind: KindRunForward, Step: "A"},
		},
		{
			name:  "advances past successful steps",
			topo:  Topology{step("A"), step("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "C"},
		},
		{
			name:  "all steps succeeded completes forward",
			topo:  Topology{step("A"), step("B")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindForwardComplete},
		},
		{
			// Invariant 23/24: an unresolved operation pins the Run.
			name:  "unresolved operation pins the run",
			topo:  Topology{step("A"), step("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpUnresolved}),
			want:  Decision{Kind: KindRunForward, Step: "B"},
		},
		{
			// Invariant 25: a step inserted before a pinned step's position
			// does not run while the pin holds.
			name:  "insertion before pinned step still continues pinned step",
			topo:  Topology{step("A"), step("X"), step("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpUnresolved}),
			want:  Decision{Kind: KindRunForward, Step: "B"},
		},
		{
			// Invariant 25 continued: after the pinned step resolves, the
			// insertion lies behind the frontier and never executes.
			name:  "insertion before resolved pinned step is skipped",
			topo:  Topology{step("A"), step("X"), step("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "C"},
		},
		{
			// Invariant 26: insertion after the frontier may execute.
			name:  "insertion after frontier executes",
			topo:  Topology{step("A"), step("B"), step("X"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "X"},
		},
		{
			// Invariant 28: reordering never moves execution backward.
			name:  "reorder moving unexecuted step behind frontier skips it",
			topo:  Topology{step("A"), step("C"), step("B"), step("D")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "D"},
		},
		{
			// Invariant 29/31: a retired, never-started step is bypassed
			// without fabricating success.
			name:  "retired unstarted step is bypassed",
			topo:  Topology{step("A"), retired("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "C"},
		},
		{
			// Invariant 30: an already-started step continues after
			// retirement.
			name:  "retired unresolved step continues",
			topo:  Topology{step("A"), retired("B"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpUnresolved}),
			want:  Decision{Kind: KindRunForward, Step: "B"},
		},
		{
			name: "trailing retired steps complete forward",
			topo: Topology{step("A"), retired("B")},
			facts: forward(map[string]OpState{
				"A": OpSucceeded,
			}),
			want: Decision{Kind: KindForwardComplete},
		},
		{
			// Removed successful steps are ignored for the frontier.
			name:  "successful step removed from topology is ignored",
			topo:  Topology{step("A"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpSucceeded}),
			want:  Decision{Kind: KindRunForward, Step: "C"},
		},
		{
			// Invariant 33 / spec 03-evolution "Removing an unresolved
			// Step": the run cannot be safely continued.
			name:  "unresolved step removed from topology is invalid",
			topo:  Topology{step("A"), step("C")},
			facts: forward(map[string]OpState{"A": OpSucceeded, "B": OpUnresolved}),
			want:  Decision{Kind: KindInvalid},
		},
		{
			name:  "permanent failure fact in forward phase is invalid",
			topo:  Topology{step("A")},
			facts: forward(map[string]OpState{"A": OpFailed}),
			want:  Decision{Kind: KindInvalid},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextForward(tt.topo, tt.facts)
			if got.Kind != tt.want.Kind || got.Step != tt.want.Step {
				t.Fatalf("NextForward = {Kind:%d Step:%q Reason:%q}, want {Kind:%d Step:%q}",
					got.Kind, got.Step, got.Reason, tt.want.Kind, tt.want.Step)
			}
		})
	}
}

func TestNextUnwind(t *testing.T) {
	tests := []struct {
		name  string
		topo  Topology
		facts Facts
		want  Decision
	}{
		{
			// Invariant 41: reverse current pipeline order.
			name: "unwinds last eligible successful step first",
			topo: Topology{unwindable("A"), unwindable("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded, "D": OpFailed},
			},
			want: Decision{Kind: KindRunUnwind, Step: "B"},
		},
		{
			// Invariant 35: unwind requires successful forward completion;
			// the failing step itself is not unwound (invariant 18).
			name: "failed and unstarted steps are not unwound",
			topo: Topology{unwindable("A"), unwindable("B"), unwindable("C")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "C": OpFailed},
			},
			want: Decision{Kind: KindRunUnwind, Step: "A"},
		},
		{
			// Invariant 37: unwind requires current unwind=true.
			name: "steps without unwind capability are skipped",
			topo: Topology{step("A"), unwindable("B"), step("C")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded, "C": OpSucceeded},
			},
			want: Decision{Kind: KindRunUnwind, Step: "B"},
		},
		{
			// Spec 03-evolution "Unwind ordering": reverse current order
			// after a reorder. Successful A, B, C under topology
			// A -> C -> B -> D unwinds B first.
			name: "reverse order follows current topology after reorder",
			topo: Topology{unwindable("A"), unwindable("C"), unwindable("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded, "C": OpSucceeded},
			},
			want: Decision{Kind: KindRunUnwind, Step: "B"},
		},
		{
			// Invariant 40: a removed step does not unwind.
			name: "removed successful step is skipped",
			topo: Topology{unwindable("A"), unwindable("C"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded, "C": OpSucceeded},
			},
			want: Decision{Kind: KindRunUnwind, Step: "C"},
		},
		{
			// Invariant 38/39: retirement does not disable unwind for steps
			// that executed; a retired-and-bypassed step never unwinds.
			name: "retired executed step unwinds, bypassed one does not",
			topo: Topology{retiredUnwindable("A"), retiredUnwindable("B"), unwindable("C"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "C": OpSucceeded},
			},
			want: Decision{Kind: KindRunUnwind, Step: "C"},
		},
		{
			name: "resolved unwind advances backward",
			topo: Topology{unwindable("A"), unwindable("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded},
				Unwind:  map[string]OpState{"B": OpSucceeded},
			},
			want: Decision{Kind: KindRunUnwind, Step: "A"},
		},
		{
			// Invariant 45: permanent unwind failure does not stop unwind.
			name: "permanent unwind failure continues backward",
			topo: Topology{unwindable("A"), unwindable("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded},
				Unwind:  map[string]OpState{"B": OpFailed},
			},
			want: Decision{Kind: KindRunUnwind, Step: "A"},
		},
		{
			// Invariant 42/43: monotonic backward frontier. B inserted (or
			// made eligible) at a position already crossed by resolved C is
			// skipped.
			name: "newly eligible step behind unwind frontier is skipped",
			topo: Topology{unwindable("A"), unwindable("B"), unwindable("C"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded, "C": OpSucceeded},
				Unwind:  map[string]OpState{"C": OpSucceeded, "A": OpSucceeded},
			},
			want: Decision{Kind: KindUnwindComplete},
		},
		{
			name: "unresolved unwind operation resumes",
			topo: Topology{unwindable("A"), unwindable("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded},
				Unwind:  map[string]OpState{"B": OpUnresolved},
			},
			want: Decision{Kind: KindRunUnwind, Step: "B"},
		},
		{
			// Losing eligibility mid-operation (unwind disabled) drops the
			// unresolved operation.
			name: "unresolved unwind loses eligibility when unwind disabled",
			topo: Topology{unwindable("A"), step("B"), step("D")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded},
				Unwind:  map[string]OpState{"B": OpUnresolved},
			},
			want: Decision{Kind: KindRunUnwind, Step: "A"},
		},
		{
			name: "no eligible work completes unwind",
			topo: Topology{step("A"), step("B")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpFailed},
			},
			want: Decision{Kind: KindUnwindComplete},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextUnwind(tt.topo, tt.facts)
			if got.Kind != tt.want.Kind || got.Step != tt.want.Step {
				t.Fatalf("NextUnwind = {Kind:%d Step:%q Reason:%q}, want {Kind:%d Step:%q}",
					got.Kind, got.Step, got.Reason, tt.want.Kind, tt.want.Step)
			}
		})
	}
}

// TestDefensiveInvalids covers the fact-corruption branches: states no
// well-behaved engine produces must reconcile to KindInvalid, never to
// executable work.
func TestDefensiveInvalids(t *testing.T) {
	tests := []struct {
		name    string
		forward bool
		topo    Topology
		facts   Facts
	}{
		{
			name:    "multiple unresolved forward operations",
			forward: true,
			topo:    Topology{step("A"), step("B")},
			facts:   forward(map[string]OpState{"A": OpUnresolved, "B": OpUnresolved}),
		},
		{
			name:    "corrupt forward state beyond the frontier",
			forward: true,
			topo:    Topology{step("A"), step("B")},
			facts:   forward(map[string]OpState{"A": OpSucceeded, "B": OpState(99)}),
		},
		{
			name: "multiple unresolved unwind operations",
			topo: Topology{unwindable("A"), unwindable("B")},
			facts: Facts{
				Forward: map[string]OpState{"A": OpSucceeded, "B": OpSucceeded},
				Unwind:  map[string]OpState{"A": OpUnresolved, "B": OpUnresolved},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Decision
			if tt.forward {
				got = NextForward(tt.topo, tt.facts)
			} else {
				got = NextUnwind(tt.topo, tt.facts)
			}
			if got.Kind != KindInvalid || got.Reason == "" {
				t.Fatalf("decision = {Kind:%d Reason:%q}, want KindInvalid with a reason", got.Kind, got.Reason)
			}
		})
	}
}

// TestViolationRederivationFallback pins the total-function guards of
// the sorted violation re-derivations: called without an actual
// violation (which the fast scans never do), they still return a
// well-formed KindInvalid rather than misbehaving.
func TestViolationRederivationFallback(t *testing.T) {
	clean := Facts{
		Forward: map[string]OpState{"A": OpSucceeded},
		Unwind:  map[string]OpState{"A": OpSucceeded},
	}
	if got := forwardViolation(clean); got.Kind != KindInvalid || got.Reason == "" {
		t.Fatalf("forwardViolation fallback = %+v", got)
	}
	if got := unwindViolation(clean); got.Kind != KindInvalid || got.Reason == "" {
		t.Fatalf("unwindViolation fallback = %+v", got)
	}
}
