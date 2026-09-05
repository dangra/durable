package durable

import "errors"

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
