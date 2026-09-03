package durable

import "time"

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

// RunState is the scheduler-visible state of a Run, distinct from Phase.
type RunState uint8

const (
	RunStateRunnable RunState = iota + 1
	RunStateRunning
	RunStateWaitingRetry
	// RunStateScheduled means the Run was accepted with a delayed start
	// and its first operation is not yet eligible.
	RunStateScheduled
	// RunStateAwaiting means the Run's in-flight operation is parked via
	// AwaitRun until another Run terminates.
	RunStateAwaiting
	// RunStateInvalid means the current application deployment cannot
	// safely continue the nonterminal Run. It is an operational runtime
	// condition, not a terminal business outcome; a corrected deployment
	// may make the Run runnable again.
	RunStateInvalid
	RunStateDone
)

func (s RunState) String() string {
	switch s {
	case RunStateRunnable:
		return "runnable"
	case RunStateRunning:
		return "running"
	case RunStateWaitingRetry:
		return "waiting-retry"
	case RunStateScheduled:
		return "scheduled"
	case RunStateAwaiting:
		return "awaiting"
	case RunStateInvalid:
		return "invalid"
	case RunStateDone:
		return "done"
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

// Result is the terminal result of a Run. An invalid nonterminal Run does
// not produce a Result.
type Result struct {
	Outcome Outcome

	RootFailure    *RootFailure
	UnwindFailures []UnwindFailure
}

func (r Result) Succeeded() bool { return r.Outcome == OutcomeSuccess }

func (r Result) Failed() bool { return r.Outcome == OutcomeFailure }

// Canceled reports whether the Run terminated because of a cancellation
// request. A canceled Run is a failed Run whose RootFailure carries
// FailureKindCanceled; a Run whose operation permanently failed on its own
// while a cancellation was pending reports Failed but not Canceled.
func (r Result) Canceled() bool {
	return r.RootFailure != nil && r.RootFailure.Kind == FailureKindCanceled
}

// Status is a point-in-time observation of a Run.
type Status struct {
	PipelineID PipelineID
	ResourceID ResourceID
	RunID      RunID

	Phase Phase
	State RunState

	// StepID and Attempt describe the current operation, when one exists.
	StepID  StepID
	Attempt uint64

	NextAttemptAt time.Time

	// LastError, LastReason, and LastErrorAt describe the most recent
	// ordinary-error attempt of the current unresolved operation; empty
	// once it resolves.
	LastError   string
	LastReason  string
	LastErrorAt time.Time

	// Outcome is set only for terminal Runs.
	Outcome *Outcome

	// InvalidReason is set when State is RunStateInvalid.
	InvalidReason string

	// CancelRequested and CancelCause reflect a pending cancellation
	// request on a nonterminal Run.
	CancelRequested bool
	CancelCause     string

	// AwaitingRunID is set when State is RunStateAwaiting: the Run whose
	// termination this Run is parked on.
	AwaitingRunID RunID
}
