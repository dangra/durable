package engine

import "github.com/dangra/durable"

import "time"

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
	// RunStateThrottled means the Run's next operation is parked waiting
	// for capacity in its concurrency class.
	RunStateThrottled
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
	case RunStateThrottled:
		return "throttled"
	case RunStateInvalid:
		return "invalid"
	case RunStateDone:
		return "done"
	default:
		return "unknown"
	}
}

// Result is the terminal result of a Run. An invalid nonterminal Run does
// not produce a Result.
type Result struct {
	Outcome durable.Outcome

	RootFailure    *durable.RootFailure
	UnwindFailures []durable.UnwindFailure
}

func (r Result) Succeeded() bool { return r.Outcome == durable.OutcomeSuccess }

func (r Result) Failed() bool { return r.Outcome == durable.OutcomeFailure }

// Canceled reports whether the Run terminated because of a cancellation
// request. A canceled Run is a failed Run whose RootFailure carries
// FailureKindCanceled; a Run whose operation permanently failed on its own
// while a cancellation was pending reports Failed but not Canceled.
func (r Result) Canceled() bool {
	return r.RootFailure != nil && r.RootFailure.Kind == durable.FailureKindCanceled
}

// Status is a point-in-time observation of a Run.
type Status struct {
	PipelineID durable.PipelineID
	ResourceID durable.ResourceID
	RunID      durable.RunID

	Phase durable.Phase
	State RunState

	// StepID and Attempt describe the current operation, when one exists.
	StepID  durable.StepID
	Attempt uint64

	NextAttemptAt time.Time

	// LastError, LastReason, and LastErrorAt describe the most recent
	// ordinary-error attempt of the current unresolved operation; empty
	// once it resolves.
	LastError   string
	LastReason  string
	LastErrorAt time.Time

	// Outcome is set only for terminal Runs.
	Outcome *durable.Outcome

	// InvalidReason is set when State is RunStateInvalid.
	InvalidReason string

	// CancelRequested and CancelCause reflect a pending cancellation
	// request on a nonterminal Run.
	CancelRequested bool
	CancelCause     string

	// AwaitingRunIDs, AwaitMode, and AwaitDeadline describe the park when
	// State is RunStateAwaiting: the Runs whose termination this Run is
	// parked on, how the park resolves, and when it expires (zero for a
	// park without WithAwaitTimeout).
	AwaitingRunIDs []durable.RunID
	AwaitMode      durable.AwaitMode
	AwaitDeadline  time.Time

	// ThrottledClass is set when State is RunStateThrottled: the
	// concurrency class the Run is waiting for capacity in.
	ThrottledClass string
}
