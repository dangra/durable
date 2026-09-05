package durable

import (
	"errors"
	"fmt"

	"github.com/dangra/durable/kernel"
)

// ErrEngineStopping is the context cancellation cause (context.Cause)
// attempt contexts carry when the Engine is shutting down. Shutdown is
// not cancellation: return ctx.Err() and the Run resumes under the next
// Engine.
var ErrEngineStopping = errors.New("durable: engine stopping")

// PreemptedError is the context cancellation cause (context.Cause) an
// attempt context carries when the engine preempts it for a Run
// cancellation request; Cause is the cancellation request's cause.
// Returning ctx.Err() remains the cooperative default (the re-executed
// attempt observes Invocation.CancelRequested). A handler or middleware
// that instead yields immediately returns a Fail wrapping this error;
// when engine-side evidence confirms the preemption (or the cancel
// request is already visible), the resulting RootFailure is attributed
// FailureKindCanceled with the cancellation's cause — see
// FailFastOnCancel.
type PreemptedError struct {
	Cause string
}

func (e *PreemptedError) Error() string {
	if e.Cause == "" {
		return "durable: attempt preempted by cancellation"
	}
	return "durable: attempt preempted by cancellation: " + e.Cause
}

// ErrRunNotFound is returned when no Run exists for a RunID — by a
// Pipeline's Run lookup, and by Cancel. A handler looking up a child it
// scheduled checks it.
var ErrRunNotFound = kernel.ErrRunNotFound

// ErrRunTerminal is returned by Cancel when the Run already has a committed
// terminal outcome.
var ErrRunTerminal = kernel.ErrRunTerminal

// ScheduleConflictError is returned by Schedule when a nonterminal Run
// already occupies the resource slot: a Run of the same pipeline with
// different Input, or a Run of another pipeline in the same exclusion
// group. RunID and PipelineID identify the blocking Run so the caller can
// route to its handle — or, from inside a handler, park on it with
// AwaitRun.
type ScheduleConflictError struct {
	RunID      RunID
	PipelineID PipelineID
}

func (e *ScheduleConflictError) Error() string {
	return fmt.Sprintf("durable: schedule conflict: active run %s of pipeline %q occupies the slot", e.RunID, e.PipelineID)
}
