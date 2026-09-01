// Package ledger implements pure reconciliation of persisted execution
// facts against the current pipeline topology: the monotonic forward and
// unwind frontiers, unresolved-operation pinning, retirement skips, and
// unwind eligibility.
//
// It performs no I/O and holds no state; the engine converts durable
// records into Facts and acts on the returned Decision. Step identities are
// plain strings to keep this package free of dependencies.
package ledger

// OpState is the resolution state of one logical operation.
type OpState uint8

const (
	OpNone OpState = iota
	OpUnresolved
	OpSucceeded
	OpFailed
)

// Step is one entry of the current pipeline topology.
type Step struct {
	ID      string
	Unwind  bool
	Retired bool
}

// Topology is the current registered pipeline definition, in order.
type Topology []Step

func (t Topology) index(id string) (int, bool) {
	for i, s := range t {
		if s.ID == id {
			return i, true
		}
	}
	return -1, false
}

// Facts are the Run's durable execution facts relevant to reconciliation.
type Facts struct {
	Forward map[string]OpState
	Unwind  map[string]OpState
}

// Kind classifies the next applicable action for a Run.
type Kind uint8

const (
	// KindRunForward executes the forward operation of Decision.Step.
	KindRunForward Kind = iota + 1
	// KindForwardComplete means forward execution reached the end of the
	// applicable current topology: reduce and commit terminal success.
	KindForwardComplete
	// KindRunUnwind executes the unwind operation of Decision.Step.
	KindRunUnwind
	// KindUnwindComplete means no eligible unwind work remains: commit
	// terminal failure.
	KindUnwindComplete
	// KindInvalid means the Run cannot be safely reconciled against the
	// current topology.
	KindInvalid
)

// Decision is the outcome of reconciliation.
type Decision struct {
	Kind   Kind
	Step   string
	Reason string
}

func invalid(reason string) Decision {
	return Decision{Kind: KindInvalid, Reason: reason}
}

// NextForward reconciles the next applicable forward operation.
//
// An unresolved forward operation pins the Run: it is always selected, and
// no other forward work is considered until it resolves. Otherwise the
// forward frontier is the maximum current-topology position among
// successfully executed Steps; the next operation is the first non-retired,
// never-started Step after it. Successful Steps absent from the current
// topology are ignored.
func NextForward(topo Topology, f Facts) Decision {
	unresolved := ""
	for id, st := range f.Forward {
		switch st {
		case OpUnresolved:
			if unresolved != "" {
				return invalid("multiple unresolved forward operations: " + unresolved + ", " + id)
			}
			unresolved = id
		case OpFailed:
			return invalid("forward facts contain a permanent failure for step " + id + " but the run is in the forward phase")
		}
	}
	if unresolved != "" {
		if _, ok := topo.index(unresolved); !ok {
			return invalid("unresolved forward step " + unresolved + " is not present in the current pipeline definition")
		}
		return Decision{Kind: KindRunForward, Step: unresolved}
	}

	frontier := -1
	for i, s := range topo {
		if f.Forward[s.ID] == OpSucceeded && i > frontier {
			frontier = i
		}
	}
	for i := frontier + 1; i < len(topo); i++ {
		s := topo[i]
		if s.Retired {
			continue
		}
		if f.Forward[s.ID] != OpNone {
			// Succeeded after the frontier is impossible by construction;
			// anything else here means inconsistent facts.
			return invalid("inconsistent forward facts for step " + s.ID)
		}
		return Decision{Kind: KindRunForward, Step: s.ID}
	}
	return Decision{Kind: KindForwardComplete}
}

// NextUnwind reconciles the next applicable unwind operation.
//
// The unwind frontier is the minimum current-topology position among Steps
// whose unwind operation has resolved; positions at or beyond it are
// already traversed and never execute. Below the frontier, Steps unwind in
// reverse current-topology order when they successfully executed forward,
// currently declare unwind, and have not resolved. Steps that lose
// eligibility (removed, or unwind disabled) are skipped even if an unwind
// attempt had started.
func NextUnwind(topo Topology, f Facts) Decision {
	seen := ""
	for id, st := range f.Unwind {
		if st == OpUnresolved {
			if seen != "" {
				return invalid("multiple unresolved unwind operations: " + seen + ", " + id)
			}
			seen = id
		}
	}

	frontier := len(topo)
	for i, s := range topo {
		if st := f.Unwind[s.ID]; st == OpSucceeded || st == OpFailed {
			if i < frontier {
				frontier = i
			}
		}
	}
	for i := frontier - 1; i >= 0; i-- {
		s := topo[i]
		if !s.Unwind {
			continue
		}
		if f.Forward[s.ID] != OpSucceeded {
			continue
		}
		switch f.Unwind[s.ID] {
		case OpNone, OpUnresolved:
			return Decision{Kind: KindRunUnwind, Step: s.ID}
		}
	}
	return Decision{Kind: KindUnwindComplete}
}
