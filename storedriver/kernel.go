package storedriver

import "time"

// The identity and outcome vocabulary shared by the Store contract and
// the durable user API. The root durable package aliases these types, so
// user code refers to them as durable.RunID, durable.Phase, and so on;
// they are defined here because the durable records below are built from
// them.

// PipelineID identifies a durable Pipeline.
type PipelineID string

// ResourceID identifies the logical resource a Run operates on.
type ResourceID string

// RunID identifies one exact execution of one Pipeline against one resource.
type RunID string

// StepID identifies durable Step semantics: forward behavior, unwind
// behavior, and Step State schema.
type StepID string

// ID returns the StepID itself, satisfying durable.StepIdentifier so a
// bare StepID is accepted wherever a generated step reference is.
func (s StepID) ID() StepID { return s }

// Phase is the execution phase of a Run.
type Phase uint8

const (
	PhaseForward Phase = iota + 1
	PhaseUnwind
	PhaseDone
)

func (p Phase) String() string {
	switch p {
	case PhaseForward:
		return "forward"
	case PhaseUnwind:
		return "unwind"
	case PhaseDone:
		return "done"
	default:
		return "unknown"
	}
}

// AwaitMode is how a multi-target park resolves: when every target is
// terminal, or when the first one is.
type AwaitMode uint8

const (
	AwaitModeAll AwaitMode = iota + 1
	AwaitModeAny
)

func (m AwaitMode) String() string {
	switch m {
	case AwaitModeAll:
		return "all"
	case AwaitModeAny:
		return "any"
	default:
		return "unknown"
	}
}

// Outcome is the terminal business outcome of a Run.
type Outcome uint8

const (
	OutcomeSuccess Outcome = iota + 1
	OutcomeFailure
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	default:
		return "unknown"
	}
}

// FailureKind attributes a permanent failure. It is purely informational:
// the engine's scheduling, retry, and unwind behavior never depend on it.
type FailureKind uint8

const (
	// FailureKindSystem attributes the failure to infrastructure or
	// environment: retrying elsewhere or later might have helped, paging
	// someone is appropriate. It is the zero value and the default.
	FailureKindSystem FailureKind = iota
	// FailureKindUser attributes the failure to the request or intent
	// itself: no amount of infrastructure health would have made it
	// succeed.
	FailureKindUser
	// FailureKindCanceled marks a RootFailure established by Run
	// cancellation. It is created by the engine; handlers should not
	// attribute their own failures with it.
	FailureKindCanceled
)

func (k FailureKind) String() string {
	switch k {
	case FailureKindUser:
		return "user"
	case FailureKindCanceled:
		return "canceled"
	default:
		return "system"
	}
}

// FailureRecord is the durable representation of one permanent operation
// failure. Arbitrary Go error chains are intentionally flattened: only the
// execution location, attempt, phase, timestamp, and human-readable message
// are preserved.
type FailureRecord struct {
	StepID  StepID
	Phase   Phase
	Attempt uint64
	Message string
	At      time.Time

	// Kind and Reason are informational attribution; they never affect
	// engine behavior. Kind defaults to FailureKindSystem; Reason is a
	// low-cardinality slug, empty when none was provided.
	Kind   FailureKind
	Reason string
}

// RootFailure is the permanent forward failure that established the Run's
// transition from forward execution to unwind.
type RootFailure struct {
	FailureRecord
}

// UnwindFailure is a permanent failure of one unwind operation. It does not
// stop the remaining unwind.
type UnwindFailure struct {
	FailureRecord
}
