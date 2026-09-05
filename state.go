package durable

import "google.golang.org/protobuf/proto"

// StateSource is a source of committed Step State bytes: an Invocation or
// a ReduceView. Its methods are the plumbing beneath LookupState; handlers
// never call them. The engine's invocations and views implement it, as
// does durabletest's fake.
type StateSource interface {
	// StateBytes returns the committed State of the Step as persisted, or
	// ok=false when the Run has none for it. The bytes are owned by the
	// source and must not be modified; LookupState only decodes them,
	// and the decoded message is the caller-owned copy.
	StateBytes(StepID) ([]byte, bool)

	// ReportViolation records a runtime contract violation observed while
	// reading durable data — committed State or Input that cannot be
	// decoded under the current schema. The engine invalidates the Run
	// for the current deployment once the attempt returns; the first
	// report wins.
	ReportViolation(error)
}

// LookupState returns the committed State of the referenced Step, or
// ok=false when no committed State exists for this Run (the Step has not
// executed, was retired before this Run entered it, was inserted behind the
// forward frontier, was removed, or never successfully completed).
//
// The returned value is a defensive caller-owned copy; mutating it does not
// affect persisted data or other lookups.
//
// LookupState is the shared lookup model behind the generated State methods
// on Invocation and Pipeline marker types; application code normally calls
// those instead.
func LookupState[T proto.Message](src StateSource, ref StateStepRef[T]) (T, bool) {
	var zero T
	b, ok := src.StateBytes(ref.Step)
	if !ok {
		return zero, false
	}
	msg := ref.New()
	if err := proto.Unmarshal(b, msg); err != nil {
		// Committed State that cannot be decoded under the current schema
		// is a runtime contract violation: the Run becomes invalid for the
		// current deployment.
		src.ReportViolation(&stateDecodeError{step: ref.Step, err: err})
		return zero, false
	}
	return msg, true
}

type stateDecodeError struct {
	step StepID
	err  error
}

func (e *stateDecodeError) Error() string {
	return "cannot decode committed state of step " + string(e.step) + ": " + e.err.Error()
}

func (e *stateDecodeError) Unwrap() error { return e.err }
