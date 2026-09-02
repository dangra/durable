package durable

import "context"

// Run is an in-process handle to one execution. The handle itself is not
// durable state; recover one after restart through Pipeline.Run.
type Run struct {
	id     RunID
	engine *Engine
}

// ID returns the RunID.
func (r Run) ID() RunID { return r.id }

// Wait blocks until the Run is terminal and returns its Result.
//
// A non-nil error means operational inability to produce a terminal Result:
// caller context cancellation, lookup failure, engine shutdown, or Run
// invalidity (*InvalidRunError). Pipeline semantic failure is represented
// by Result.Outcome == OutcomeFailure, not by an error.
func (r Run) Wait(ctx context.Context) (Result, error) {
	e := r.engine
	base, ok := e.baseContext()
	if !ok {
		return Result{}, ErrEngineNotStarted
	}
	for {
		rec, err := e.store.GetRun(ctx, r.id)
		if err != nil {
			return Result{}, err
		}
		if rec.Terminal() {
			return resultOf(rec), nil
		}
		if ie := e.invalidFor(r.id); ie != nil {
			return Result{}, ie
		}
		ch := e.waiterChan(r.id)
		// Re-check after registering so a notification between the read
		// and the registration is not missed.
		rec, err = e.store.GetRun(ctx, r.id)
		if err != nil {
			return Result{}, err
		}
		if rec.Terminal() {
			return resultOf(rec), nil
		}
		if ie := e.invalidFor(r.id); ie != nil {
			return Result{}, ie
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-base.Done():
			return Result{}, base.Err()
		}
	}
}

// Cancel durably requests cancellation of the Run. The first request wins;
// canceling an already-canceling Run is a no-op returning nil.
//
// Cancellation reuses unwind: the Run stops selecting new forward work, a
// RootFailure with FailureKindCanceled is established, successfully
// executed Steps unwind normally, and the Run terminates with
// OutcomeFailure. A started operation is never abandoned: its in-flight
// attempt context is preempted once, and it continues (observing
// Invocation.CancelRequested) until it resolves.
//
// A terminal Run returns ErrRunTerminal; a missing Run ErrRunNotFound. The
// request survives restart, and on an invalid Run it takes effect when a
// corrected deployment makes the Run reconcilable again.
func (r Run) Cancel(ctx context.Context, cause string) error {
	e := r.engine
	if !e.isStarted() {
		return ErrEngineNotStarted
	}
	if _, err := e.store.RequestCancel(ctx, r.id, CancelRequest{Cause: cause, At: e.clock.Now()}); err != nil {
		return err
	}
	e.preemptAttempt(r.id)
	e.wakeRun(r.id)
	e.dispatch(r.id, 0)
	return nil
}

// Status returns a point-in-time observation of the Run.
func (r Run) Status(ctx context.Context) (Status, error) {
	e := r.engine
	rec, err := e.store.GetRun(ctx, r.id)
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
		if sr.ForwardStatus == OpUnresolved {
			st.StepID, st.Attempt = id, sr.ForwardAttempts
		}
		if sr.UnwindStatus == OpUnresolved {
			st.StepID, st.Attempt = id, sr.UnwindAttempts
		}
	}
	switch {
	case rec.Terminal():
		st.State = RunStateDone
		st.Outcome = rec.Outcome
	default:
		if ie := e.invalidFor(r.id); ie != nil {
			st.State = RunStateInvalid
			st.InvalidReason = ie.Reason
		} else if !rec.NextAttemptAt.IsZero() && e.clock.Now().Before(rec.NextAttemptAt) {
			if started(rec) {
				st.State = RunStateWaitingRetry
			} else {
				st.State = RunStateScheduled
			}
			st.NextAttemptAt = rec.NextAttemptAt
		} else if e.isActive(r.id) {
			st.State = RunStateRunning
		} else {
			st.State = RunStateRunnable
		}
	}
	return st, nil
}

// OutputBytes returns the committed Pipeline Output of a terminal
// successful Run. It is intended for generated code, which wraps it with a
// typed Output accessor.
func (r Run) OutputBytes(ctx context.Context) ([]byte, error) {
	rec, err := r.engine.store.GetRun(ctx, r.id)
	if err != nil {
		return nil, err
	}
	return rec.Output, nil
}

// started reports whether any operation attempt was ever reserved for the
// Run, distinguishing a delayed start from a retry wait.
func started(rec *RunRecord) bool {
	for _, sr := range rec.Steps {
		if sr.ForwardAttempts > 0 || sr.UnwindAttempts > 0 {
			return true
		}
	}
	return false
}

func resultOf(rec *RunRecord) Result {
	res := Result{
		Outcome:        *rec.Outcome,
		UnwindFailures: append([]UnwindFailure(nil), rec.UnwindFailures...),
	}
	if rec.RootFailure != nil {
		rf := *rec.RootFailure
		res.RootFailure = &rf
	}
	return res
}
