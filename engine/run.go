package engine

import (
	"context"
	"github.com/dangra/durable"
	"github.com/dangra/durable/store/driver"
)

// Run is an in-process handle to one execution. The handle itself is not
// durable state; recover one after restart through Pipeline.GetRun.
type Run struct {
	id     durable.RunID
	engine *Engine
}

// ID returns the RunID.
func (r Run) ID() durable.RunID { return r.id }

// Wait blocks until the Run is terminal and returns its Result.
//
// A non-nil error means operational inability to produce a terminal Result:
// caller context cancellation, lookup failure, engine shutdown, or Run
// invalidity (*InvalidRunError). Pipeline semantic failure is represented
// by Result.Outcome == OutcomeFailure, not by an error.
//
// Inside a handler Wait never blocks: called with the attempt context (or
// one derived from it) it returns the Result if the Run is already
// terminal and ErrRunInProgress otherwise. A handler blocking on another
// Run holds a worker slot and can deadlock the pool; AwaitRun is the
// mechanism for cross-run waiting.
func (r Run) Wait(ctx context.Context) (Result, error) {
	e := r.engine
	base, ok := e.baseContext()
	if !ok {
		return Result{}, ErrNotStarted
	}
	for {
		// Register first, then read once: a notification between the
		// read and the sleep then closes ch. A Run found terminal pays
		// a registration and its cancel, an order of magnitude less
		// than the read a check before registering would cost.
		ch, cancel := e.waiters.Watch(r.id)
		rec, err := e.store.GetRunHead(ctx, r.id)
		if err != nil {
			cancel()
			return Result{}, err
		}
		if rec.Terminal() {
			cancel()
			return resultOf(rec), nil
		}
		if ie := e.invalidFor(r.id); ie != nil {
			cancel()
			return Result{}, ie
		}
		if inAttempt(ctx) {
			cancel()
			return Result{}, ErrRunInProgress
		}
		select {
		case <-ch:
		case <-ctx.Done():
			cancel()
			return Result{}, ctx.Err()
		case <-base.Done():
			cancel()
			return Result{}, base.Err()
		}
	}
}

// Cancel durably requests cancellation of the Run. The first request wins;
// canceling an already-canceling Run is a no-op returning nil.
//
// Cancellation reuses unwind: the Run stops selecting new forward work, a
// Failure with FailureKindCanceled is established, successfully
// executed Steps unwind normally, and the Run terminates with
// OutcomeFailure. A started operation is never abandoned: its in-flight
// attempt context is preempted once — carrying a *PreemptedError with
// this cause as its context.Cause — and it continues (observing
// Invocation.CancelRequested) until it resolves. FailFastOnCancel opts
// preemption-safe handlers out of that cooperative loop.
//
// A terminal Run returns durable.ErrRunTerminal; a missing Run durable.ErrRunNotFound. The
// request survives restart, and on an invalid Run it takes effect when a
// corrected deployment makes the Run reconcilable again.
func (r Run) Cancel(ctx context.Context, cause string) error {
	e := r.engine
	if !e.isStarted() {
		return ErrNotStarted
	}
	accepted, err := e.store.RequestCancel(ctx, r.id, driver.CancelRequest{Cause: e.boundText(cause), At: e.clock.Now()})
	if err != nil {
		return err
	}
	if accepted && e.debugLog() {
		e.logger.Debug("durable: cancel requested", "run", string(r.id), "cause", cause)
	}
	e.preemptAttempt(r.id, e.boundText(cause))
	e.disp.Wake(r.id)
	e.disp.Dispatch(r.id, 0)
	return nil
}

// Status returns a point-in-time observation of the Run. It reads the
// Run's head: no blob is touched, so polling is cheap however large the
// Input and States are.
func (r Run) Status(ctx context.Context) (Status, error) {
	e := r.engine
	rec, err := e.store.GetRunHead(ctx, r.id)
	if err != nil {
		return Status{}, err
	}
	st := Status{
		PipelineID:  rec.PipelineID,
		ResourceID:  rec.ResourceID,
		RunID:       rec.RunID,
		Phase:       rec.Phase,
		LastError:   rec.LastError,
		LastReason:  rec.LastReason,
		LastErrorAt: rec.LastErrorAt,
	}
	if rec.Cancel != nil {
		st.CancelRequested = true
		st.CancelCause = rec.Cancel.Cause
	}
	for id, sr := range rec.Steps {
		if sr.Forward.Status == driver.OpUnresolved {
			st.StepID, st.Attempt = id, sr.Forward.Attempts
		}
		if sr.Unwind.Status == driver.OpUnresolved {
			st.StepID, st.Attempt = id, sr.Unwind.Attempts
		}
	}
	if rec.Failure != nil {
		rf := *rec.Failure
		st.Failure = &rf
	}
	switch {
	case rec.Terminal():
		st.State = RunStateDone
		st.Outcome = rec.Outcome
	default:
		if ie := e.invalidFor(r.id); ie != nil {
			st.State = RunStateInvalid
			st.InvalidReason = ie.Reason
		} else if rec.Awaiting != nil && rec.Cancel == nil {
			st.State = RunStateAwaiting
			st.AwaitingRunIDs = append([]durable.RunID(nil), rec.Awaiting.Targets...)
			st.AwaitMode = rec.Awaiting.Mode
			st.AwaitDeadline = rec.Awaiting.Deadline
		} else if class, ok := e.pool.ParkedOn(r.id); ok {
			st.State = RunStateThrottled
			st.ThrottledClass = class
		} else if !rec.NextAttemptAt.IsZero() && e.clock.Now().Before(rec.NextAttemptAt) {
			if started(rec) {
				st.State = RunStateWaitingRetry
			} else {
				st.State = RunStateScheduled
			}
			st.NextAttemptAt = rec.NextAttemptAt
		} else if e.disp != nil && e.disp.IsActive(r.id) {
			st.State = RunStateRunning
		} else {
			st.State = RunStateRunnable
		}
	}
	return st, nil
}

// Annotations returns a caller-owned copy of the Run's immutable
// acceptance-time annotations, nil when none were supplied.
func (r Run) Annotations(ctx context.Context) (map[string]string, error) {
	rec, err := r.engine.store.GetRunHead(ctx, r.id)
	if err != nil {
		return nil, err
	}
	if len(rec.Annotations) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(rec.Annotations))
	for k, v := range rec.Annotations {
		out[k] = v
	}
	return out, nil
}

// InputBytes returns the Run's immutable serialized Pipeline Input (nil
// for an Input-less pipeline). The slice is shared with the store and
// must not be modified. It is intended for generated code, which wraps
// it with a typed Input accessor; the typed accessor returns a
// caller-owned message via a fresh unmarshal.
//
// The Input is released when the Run reaches its terminal outcome: it
// has been folded into the Output by then, and the store keeps only the
// Output. InputBytes on a terminal Run returns durable.ErrRunTerminal.
func (r Run) InputBytes(ctx context.Context) ([]byte, error) {
	rec, err := r.engine.store.GetRun(ctx, r.id)
	if err != nil {
		return nil, err
	}
	if rec.Terminal() {
		return nil, durable.ErrRunTerminal
	}
	return rec.Input, nil
}

// OutputBytes returns the committed terminal output of a Run: the
// Pipeline Output of a successful Run, or the failure Output of a failed
// Run whose pipeline declares one; nil otherwise. The slice is shared
// with the store and must not be modified. It is intended for generated
// code, which wraps it with typed Output and FailureOutput accessors
// keyed on the outcome.
func (r Run) OutputBytes(ctx context.Context) ([]byte, error) {
	rec, err := r.engine.store.GetRun(ctx, r.id)
	if err != nil {
		return nil, err
	}
	return rec.Output, nil
}

// started reports whether any operation attempt was ever reserved for the
// Run, distinguishing a delayed start from a retry wait.
func started(rec *driver.RunRecord) bool {
	for _, sr := range rec.Steps {
		if sr.Forward.Attempts > 0 || sr.Unwind.Attempts > 0 {
			return true
		}
	}
	return false
}

func resultOf(rec *driver.RunRecord) Result {
	res := Result{Outcome: *rec.Outcome}
	if rec.Failure != nil {
		rf := *rec.Failure
		res.Failure = &rf
	}
	return res
}
