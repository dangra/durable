package durable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable/internal/ledger"
)

// RetryPolicy configures exponential backoff for ordinary (retryable)
// errors. Ordinary errors are retried indefinitely; there is no max-attempt
// exhaustion concept in v1.
type RetryPolicy struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
}

var defaultRetryPolicy = RetryPolicy{
	Initial:    100 * time.Millisecond,
	Max:        30 * time.Second,
	Multiplier: 2,
}

// Option configures an Engine.
type Option func(*Engine)

// WithConcurrency bounds the number of concurrently executing operations
// across all Runs. The default is 16.
func WithConcurrency(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.concurrency = n
		}
	}
}

// WithRetryPolicy sets the engine-level retry policy.
func WithRetryPolicy(p RetryPolicy) Option {
	return func(e *Engine) {
		if p.Initial > 0 {
			e.retry = p
		}
	}
}

// WithClock sets the Clock used for retry timing, wakeups, recovery
// backoff, and failure timestamps.
func WithClock(c Clock) Option {
	return func(e *Engine) {
		if c != nil {
			e.clock = c
		}
	}
}

// WithLogger sets the structured logger for operational diagnostics.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.logger = l
		}
	}
}

// WithRecoveryBackoff delays the first execution of unresolved operations
// discovered at startup, protecting against crash loops.
func WithRecoveryBackoff(d time.Duration) Option {
	return func(e *Engine) {
		if d >= 0 {
			e.recoveryBackoff = d
		}
	}
}

// Engine executes Runs against a Store. Exactly one Engine may execute
// against a Store at a time in v1.
type Engine struct {
	store           Store
	clock           Clock
	logger          *slog.Logger
	retry           RetryPolicy
	concurrency     int
	recoveryBackoff time.Duration
	middleware      []Middleware

	mu            sync.Mutex
	pipelines     map[PipelineID]*Definition
	stepOwner     map[StepID]PipelineID
	started       bool
	active        map[RunID]struct{}
	invalid       map[RunID]*InvalidRunError
	waiters       map[RunID][]chan struct{}
	attemptCancel map[RunID]context.CancelFunc
	wakes         map[RunID]chan struct{}

	baseCtx context.Context
	cancel  context.CancelFunc
	sem     chan struct{}
	wg      sync.WaitGroup
}

// NewEngine constructs an Engine in the configuring state. Definitions bind
// and handlers register before Start; after Start, registration freezes and
// scheduling is accepted.
func NewEngine(store Store, opts ...Option) *Engine {
	e := &Engine{
		store:       store,
		clock:       wallClock{},
		logger:      slog.Default(),
		retry:       defaultRetryPolicy,
		concurrency: 16,
		pipelines:   make(map[PipelineID]*Definition),
		stepOwner:   make(map[StepID]PipelineID),
		active:        make(map[RunID]struct{}),
		invalid:       make(map[RunID]*InvalidRunError),
		waiters:       make(map[RunID][]chan struct{}),
		attemptCancel: make(map[RunID]context.CancelFunc),
		wakes:         make(map[RunID]chan struct{}),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *Engine) register(d *Definition) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		return ErrEngineStarted
	}
	if _, dup := e.pipelines[d.ID()]; dup {
		return fmt.Errorf("durable: pipeline %q bound twice", d.ID())
	}
	for id := range d.steps {
		if owner, dup := e.stepOwner[id]; dup {
			return fmt.Errorf("durable: step %q declared by pipelines %q and %q; one durable step belongs to exactly one active pipeline", id, owner, d.ID())
		}
	}
	e.pipelines[d.ID()] = d
	for id := range d.steps {
		e.stepOwner[id] = d.ID()
	}
	return nil
}

// Start freezes registration, recovers nonterminal Runs, and begins
// accepting scheduling. Engine-wide problems fail startup; a problem
// isolated to one persisted Run never does — such Runs are logged, marked
// invalid, and ignored until a corrected deployment reconciles them.
//
// ctx governs startup work only; execution lifetime is engine-owned until
// Stop.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return ErrEngineStarted
	}
	e.started = true
	e.baseCtx, e.cancel = context.WithCancel(context.Background())
	e.sem = make(chan struct{}, e.concurrency)
	e.mu.Unlock()

	recs, err := e.store.ListNonterminal(ctx)
	if err != nil {
		return fmt.Errorf("durable: discovering nonterminal runs: %w", err)
	}
	for _, rec := range recs {
		delay := time.Duration(0)
		if hasUnresolvedOp(rec) {
			delay = e.recoveryBackoff
		}
		e.dispatch(rec.RunID, delay)
	}
	return nil
}

// Stop gracefully shuts down: it stops scheduling new operations, cancels
// active handler contexts, and leaves unresolved Runs nonterminal. Shutdown
// does not create Pipeline failure; a future Engine resumes the Runs.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if !e.started || e.cancel == nil {
		e.mu.Unlock()
		return ErrEngineNotStarted
	}
	cancel := e.cancel
	e.mu.Unlock()

	cancel()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) isStarted() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.started
}

// baseContext returns the engine-owned execution context, or ok=false when
// the engine has not started.
func (e *Engine) baseContext() (context.Context, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.baseCtx, e.baseCtx != nil
}

func hasUnresolvedOp(rec *RunRecord) bool {
	for _, sr := range rec.Steps {
		if sr.ForwardStatus == OpUnresolved || sr.UnwindStatus == OpUnresolved {
			return true
		}
	}
	return false
}

// dispatch schedules processing of a Run after delay. At most one worker
// exists per Run at a time.
func (e *Engine) dispatch(id RunID, delay time.Duration) {
	e.mu.Lock()
	if _, dup := e.active[id]; dup {
		e.mu.Unlock()
		return
	}
	e.active[id] = struct{}{}
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if delay > 0 {
			wake := e.armWake(id)
			select {
			case <-e.clock.After(delay):
			case <-wake:
			case <-e.baseCtx.Done():
				e.disarmWake(id)
				e.clearActive(id)
				return
			}
			e.disarmWake(id)
		}
		select {
		case e.sem <- struct{}{}:
		case <-e.baseCtx.Done():
			e.clearActive(id)
			return
		}
		redispatchIn, again := e.processRun(id)
		<-e.sem
		e.clearActive(id)
		if again && e.baseCtx.Err() == nil {
			e.dispatch(id, redispatchIn)
		}
	}()
}

// armWake registers a wake channel a delayed dispatch waits on, so a
// cancellation (or other urgent signal) can cut a retry or start delay
// short. At most one dispatch waits per Run.
func (e *Engine) armWake(id RunID) chan struct{} {
	ch := make(chan struct{}, 1)
	e.mu.Lock()
	e.wakes[id] = ch
	e.mu.Unlock()
	return ch
}

func (e *Engine) disarmWake(id RunID) {
	e.mu.Lock()
	delete(e.wakes, id)
	e.mu.Unlock()
}

// wakeRun cuts short a delayed dispatch wait for the Run, if any.
func (e *Engine) wakeRun(id RunID) {
	e.mu.Lock()
	ch := e.wakes[id]
	e.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// attemptContext derives the per-attempt handler context and registers its
// cancel so a cancellation request can preempt the in-flight attempt.
func (e *Engine) attemptContext(id RunID) (context.Context, func()) {
	ctx, cancel := context.WithCancel(e.baseCtx)
	e.mu.Lock()
	e.attemptCancel[id] = cancel
	e.mu.Unlock()
	return ctx, func() {
		e.mu.Lock()
		delete(e.attemptCancel, id)
		e.mu.Unlock()
		cancel()
	}
}

// preemptAttempt cancels the Run's in-flight attempt context, if any. The
// interrupted attempt resolves through normal handler result semantics.
func (e *Engine) preemptAttempt(id RunID) {
	e.mu.Lock()
	cancel := e.attemptCancel[id]
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *Engine) clearActive(id RunID) {
	e.mu.Lock()
	delete(e.active, id)
	e.mu.Unlock()
}

func (e *Engine) isActive(id RunID) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.active[id]
	return ok
}

// processRun advances one Run until it becomes terminal, invalid, must wait
// for retry, or shutdown begins. It returns a redispatch delay when the Run
// needs a future wakeup.
func (e *Engine) processRun(id RunID) (time.Duration, bool) {
	for {
		if e.baseCtx.Err() != nil {
			return 0, false
		}
		rec, err := e.store.GetRun(e.baseCtx, id)
		if err != nil {
			if errors.Is(err, ErrRunNotFound) {
				e.logger.Error("durable: dispatched run not found", "run", id)
				return 0, false
			}
			e.logger.Error("durable: store read failed", "run", id, "error", err)
			return time.Second, true
		}
		if rec.Terminal() {
			e.notify(id)
			return 0, false
		}

		e.mu.Lock()
		def := e.pipelines[rec.PipelineID]
		e.mu.Unlock()
		if def == nil {
			e.markInvalid(rec, "", fmt.Sprintf("pipeline %q is not registered with the current deployment", rec.PipelineID))
			return 0, false
		}

		facts := factsOf(rec)
		var dec ledger.Decision
		switch rec.Phase {
		case PhaseForward:
			dec = ledger.NextForward(def.topo, facts)
		case PhaseUnwind:
			dec = ledger.NextUnwind(def.topo, facts)
		default:
			e.markInvalid(rec, "", fmt.Sprintf("nonterminal run in unexpected phase %v", rec.Phase))
			return 0, false
		}

		switch dec.Kind {
		case ledger.KindInvalid:
			e.markInvalid(rec, StepID(dec.Step), dec.Reason)
			return 0, false

		case ledger.KindRunForward:
			// A pending cancellation stops new forward work; a started
			// operation is never abandoned and continues until it
			// resolves.
			if rec.Cancel != nil && !forwardStarted(rec, StepID(dec.Step)) {
				if !e.applyCancel(rec) {
					return time.Second, true
				}
				continue
			}
			if delay, wait := e.retryGate(rec); wait {
				return delay, true
			}
			done, delay, again := e.runForward(rec, def, StepID(dec.Step))
			if !done {
				return delay, again
			}

		case ledger.KindForwardComplete:
			// A Run remains cancelable until terminal success commits.
			if rec.Cancel != nil {
				if !e.applyCancel(rec) {
					return time.Second, true
				}
				continue
			}
			if !e.reduceAndComplete(rec, def) {
				return 0, false
			}

		case ledger.KindRunUnwind:
			if delay, wait := e.retryGate(rec); wait {
				return delay, true
			}
			done, delay, again := e.runUnwind(rec, def, StepID(dec.Step))
			if !done {
				return delay, again
			}

		case ledger.KindUnwindComplete:
			oc := OutcomeFailure
			rec.Outcome = &oc
			rec.Phase = PhaseDone
			if !e.update(rec) {
				return time.Second, true
			}
			e.notify(id)
			return 0, false
		}
	}
}

// forwardStarted reports whether the Step's forward operation has ever
// reserved an attempt for this Run.
func forwardStarted(rec *RunRecord, stepID StepID) bool {
	sr, ok := rec.Steps[stepID]
	return ok && sr.ForwardAttempts > 0
}

// applyCancel establishes the cancellation RootFailure and transitions the
// Run to unwind.
func (e *Engine) applyCancel(rec *RunRecord) bool {
	cause := rec.Cancel.Cause
	if cause == "" {
		cause = "canceled"
	}
	rec.RootFailure = &RootFailure{FailureRecord: FailureRecord{
		Phase:   PhaseForward,
		Message: cause,
		At:      e.clock.Now(),
		Kind:    FailureKindCanceled,
	}}
	rec.Phase = PhaseUnwind
	rec.NextAttemptAt = time.Time{}
	return e.update(rec)
}

// recordLastError captures an ordinary-error attempt on the record; it
// rides the same durable write as NextAttemptAt.
func recordLastError(rec *RunRecord, err error, now time.Time) {
	rec.LastError = err.Error()
	rec.LastReason = reasonOf(err)
	rec.LastErrorAt = now
}

// clearLastError resets the last-error fields when the current operation
// resolves.
func clearLastError(rec *RunRecord) {
	rec.LastError, rec.LastReason = "", ""
	rec.LastErrorAt = time.Time{}
}

// retryGate returns a wait when the Run's next attempt is not yet eligible.
func (e *Engine) retryGate(rec *RunRecord) (time.Duration, bool) {
	if rec.NextAttemptAt.IsZero() {
		return 0, false
	}
	now := e.clock.Now()
	if now.Before(rec.NextAttemptAt) {
		return rec.NextAttemptAt.Sub(now), true
	}
	return 0, false
}

// runForward reserves an attempt and executes one forward operation.
// done=true means the loop should reconcile again immediately.
func (e *Engine) runForward(rec *RunRecord, def *Definition, stepID StepID) (done bool, delay time.Duration, again bool) {
	sc := def.step(stepID)
	sr := rec.Step(stepID)

	// Durably reserve the attempt before invoking application code.
	sr.ForwardStatus = OpUnresolved
	sr.ForwardAttempts++
	rec.NextAttemptAt = time.Time{}
	if !e.update(rec) {
		return false, time.Second, true
	}

	inv := e.invocation(rec, def, stepID, sr.ForwardAttempts, PhaseForward)
	state, err := e.invokeForward(sc, inv)

	if v := inv.takeViolation(); v != nil {
		e.markInvalid(rec, stepID, v.Error())
		return false, 0, false
	}

	now := e.clock.Now()
	switch pe, permanent := asPermanent(err); {
	case err == nil:
		var stateBytes []byte
		if sc.HasState {
			if state == nil || !state.ProtoReflect().IsValid() {
				e.markInvalid(rec, stepID, "state-producing handler returned nil state with nil error")
				return false, 0, false
			}
			b, merr := proto.Marshal(state)
			if merr != nil {
				e.markInvalid(rec, stepID, fmt.Sprintf("cannot serialize successful state: %v", merr))
				return false, 0, false
			}
			stateBytes = b
		}
		// State commit and forward success are one durable transition.
		sr.ForwardStatus = OpSucceeded
		sr.State = stateBytes
		clearLastError(rec)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return true, 0, false

	case permanent:
		sr.ForwardStatus = OpFailed
		rec.RootFailure = &RootFailure{FailureRecord: FailureRecord{
			StepID:  stepID,
			Phase:   PhaseForward,
			Attempt: sr.ForwardAttempts,
			Message: pe.err.Error(),
			At:      now,
			Kind:    pe.failureKind(),
			Reason:  pe.failureReason(),
		}}
		rec.Phase = PhaseUnwind
		clearLastError(rec)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return true, 0, false

	default:
		d := e.backoff(sr.ForwardAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return false, d, true
	}
}

// runUnwind reserves an attempt and executes one unwind operation.
func (e *Engine) runUnwind(rec *RunRecord, def *Definition, stepID StepID) (done bool, delay time.Duration, again bool) {
	sc := def.step(stepID)
	sr := rec.Step(stepID)

	sr.UnwindStatus = OpUnresolved
	sr.UnwindAttempts++
	rec.NextAttemptAt = time.Time{}
	if !e.update(rec) {
		return false, time.Second, true
	}

	inv := e.invocation(rec, def, stepID, sr.UnwindAttempts, PhaseUnwind)
	failure := Failure{
		UnwindFailures: append([]UnwindFailure(nil), rec.UnwindFailures...),
	}
	if rec.RootFailure != nil {
		failure.Root = *rec.RootFailure
	}
	err := e.invokeUnwind(sc, inv, failure)

	if v := inv.takeViolation(); v != nil {
		e.markInvalid(rec, stepID, v.Error())
		return false, 0, false
	}

	now := e.clock.Now()
	switch pe, permanent := asPermanent(err); {
	case err == nil:
		sr.UnwindStatus = OpSucceeded
		clearLastError(rec)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return true, 0, false

	case permanent:
		sr.UnwindStatus = OpFailed
		rec.UnwindFailures = append(rec.UnwindFailures, UnwindFailure{FailureRecord: FailureRecord{
			StepID:  stepID,
			Phase:   PhaseUnwind,
			Attempt: sr.UnwindAttempts,
			Message: pe.err.Error(),
			At:      now,
			Kind:    pe.failureKind(),
			Reason:  pe.failureReason(),
		}})
		clearLastError(rec)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return true, 0, false

	default:
		d := e.backoff(sr.UnwindAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.update(rec) {
			return false, time.Second, true
		}
		return false, d, true
	}
}

// reduceAndComplete runs the Reducer (if any) and commits terminal success.
// It returns false when the Run became invalid or the store failed.
func (e *Engine) reduceAndComplete(rec *RunRecord, def *Definition) bool {
	if def.cfg.Reduce != nil {
		view := &ReduceView{
			input:    rec.Input,
			newInput: def.cfg.NewInput,
			states:   committedStates(rec),
		}
		out, rerr := e.invokeReduce(def, view)
		if rerr != nil {
			e.markInvalid(rec, "", rerr.Error())
			return false
		}
		if v := view.takeViolation(); v != nil {
			e.markInvalid(rec, "", v.Error())
			return false
		}
		if out == nil || !out.ProtoReflect().IsValid() {
			e.markInvalid(rec, "", "reducer returned nil output")
			return false
		}
		b, merr := proto.Marshal(out)
		if merr != nil {
			e.markInvalid(rec, "", fmt.Sprintf("cannot serialize pipeline output: %v", merr))
			return false
		}
		rec.Output = b
	}
	oc := OutcomeSuccess
	rec.Outcome = &oc
	rec.Phase = PhaseDone
	if !e.update(rec) {
		return false
	}
	e.notify(rec.RunID)
	return true
}

func (e *Engine) invocation(rec *RunRecord, def *Definition, stepID StepID, attempt uint64, phase Phase) *Invocation {
	return &Invocation{
		pipelineID:      rec.PipelineID,
		resourceID:      rec.ResourceID,
		runID:           rec.RunID,
		stepID:          stepID,
		attempt:         attempt,
		phase:           phase,
		input:           rec.Input,
		newInput:        def.cfg.NewInput,
		states:          committedStates(rec),
		cancelRequested: rec.Cancel != nil,
	}
}

func committedStates(rec *RunRecord) map[StepID][]byte {
	states := make(map[StepID][]byte)
	for id, sr := range rec.Steps {
		if sr.ForwardStatus == OpSucceeded && sr.State != nil {
			states[id] = sr.State
		}
	}
	return states
}

func (e *Engine) invokeForward(sc *StepConfig, inv *Invocation) (state proto.Message, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
			e.logger.Error("durable: forward handler panic",
				"run", inv.runID, "step", inv.stepID, "attempt", inv.attempt,
				"panic", p, "stack", string(debug.Stack()))
		}
	}()
	ctx, done := e.attemptContext(inv.runID)
	defer done()
	return e.wrap(Handler(sc.Run))(ctx, inv)
}

func (e *Engine) invokeUnwind(sc *StepConfig, inv *Invocation, failure Failure) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("unwind handler panic: %v", p)
			e.logger.Error("durable: unwind handler panic",
				"run", inv.runID, "step", inv.stepID, "attempt", inv.attempt,
				"panic", p, "stack", string(debug.Stack()))
		}
	}()
	h := e.wrap(func(ctx context.Context, in *Invocation) (proto.Message, error) {
		return nil, sc.UnwindFunc(ctx, in, failure)
	})
	ctx, done := e.attemptContext(inv.runID)
	defer done()
	_, err = h(ctx, inv)
	return err
}

func (e *Engine) invokeReduce(def *Definition, view *ReduceView) (out proto.Message, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("reducer panic: %v", p)
			e.logger.Error("durable: reducer panic",
				"pipeline", def.ID(), "panic", p, "stack", string(debug.Stack()))
		}
	}()
	return def.cfg.Reduce(view), nil
}

func (e *Engine) update(rec *RunRecord) bool {
	rec.UpdatedAt = e.clock.Now()
	if err := e.store.UpdateRun(e.baseCtx, rec); err != nil {
		if e.baseCtx.Err() == nil {
			e.logger.Error("durable: store update failed", "run", rec.RunID, "error", err)
		}
		return false
	}
	return true
}

func (e *Engine) backoff(attempts uint64) time.Duration {
	p := e.retry
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	d := float64(p.Initial) * math.Pow(p.Multiplier, float64(attempts-1))
	if max := float64(p.Max); p.Max > 0 && d > max {
		d = max
	}
	// Jitter in [d/2, d).
	d = d/2 + mrand.Float64()*d/2
	return time.Duration(d)
}

// markInvalid records that the current deployment cannot safely continue
// the Run. Invalidity is deployment-relative and derived, not persisted:
// the Run is logged, surfaced through Status and Wait, and ignored for
// execution until a corrected deployment reconciles it.
func (e *Engine) markInvalid(rec *RunRecord, stepID StepID, reason string) {
	ie := &InvalidRunError{RunID: rec.RunID, PipelineID: rec.PipelineID, Reason: reason}
	e.mu.Lock()
	e.invalid[rec.RunID] = ie
	e.mu.Unlock()
	e.logger.Error("durable: run invalid for current deployment",
		"run", rec.RunID,
		"pipeline", rec.PipelineID,
		"resource", rec.ResourceID,
		"phase", rec.Phase.String(),
		"step", stepID,
		"reason", reason)
	e.notify(rec.RunID)
}

func (e *Engine) invalidFor(id RunID) *InvalidRunError {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.invalid[id]
}

// waiterChan registers a channel closed at the next terminal/invalid
// notification for the Run.
func (e *Engine) waiterChan(id RunID) chan struct{} {
	ch := make(chan struct{})
	e.mu.Lock()
	e.waiters[id] = append(e.waiters[id], ch)
	e.mu.Unlock()
	return ch
}

func (e *Engine) notify(id RunID) {
	e.mu.Lock()
	chs := e.waiters[id]
	delete(e.waiters, id)
	e.mu.Unlock()
	for _, ch := range chs {
		close(ch)
	}
}

func factsOf(rec *RunRecord) ledger.Facts {
	f := ledger.Facts{
		Forward: make(map[string]ledger.OpState, len(rec.Steps)),
		Unwind:  make(map[string]ledger.OpState, len(rec.Steps)),
	}
	for id, sr := range rec.Steps {
		f.Forward[string(id)] = opState(sr.ForwardStatus)
		f.Unwind[string(id)] = opState(sr.UnwindStatus)
	}
	return f
}

func opState(s OpStatus) ledger.OpState {
	switch s {
	case OpUnresolved:
		return ledger.OpUnresolved
	case OpSucceeded:
		return ledger.OpSucceeded
	case OpFailed:
		return ledger.OpFailed
	default:
		return ledger.OpNone
	}
}

func newRunID(now time.Time) RunID {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("durable: reading random bytes: %v", err))
	}
	return RunID(fmt.Sprintf("%016x%s", now.UnixNano(), hex.EncodeToString(b[:])))
}
