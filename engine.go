package durable

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
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

// WithConcurrencyClass sets the capacity of a named concurrency class:
// at most capacity operations of steps declaring the class execute
// simultaneously. A class declared in step options but never configured
// here is unlimited (the Engine warns at Start). Class tokens are
// execution-scoped and in-memory: they are held only while an operation's
// handler runs, never across retry waits, parks, or restarts.
func WithConcurrencyClass(name string, capacity int) Option {
	return func(e *Engine) {
		if name != "" && capacity > 0 {
			e.classCapacity[name] = capacity
		}
	}
}

// RetentionPolicy configures reaping of terminal Runs. Retention is off by
// default: without WithRetention, terminal Runs accumulate indefinitely.
type RetentionPolicy struct {
	// TerminalAfter is how long after its terminal commit a Run is kept.
	// It must be positive to enable retention.
	TerminalAfter time.Duration
	// Interval is the jittered sweep cadence; default 10 minutes.
	Interval time.Duration
}

// WithRetention enables background reaping of terminal Runs. Only terminal
// Runs are ever reaped — nonterminal Runs, invalid ones included, are
// never touched regardless of age. The first sweep runs at Start.
func WithRetention(p RetentionPolicy) Option {
	return func(e *Engine) {
		if p.TerminalAfter > 0 {
			if p.Interval <= 0 {
				p.Interval = 10 * time.Minute
			}
			e.retention = p
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
	retention       RetentionPolicy
	middleware      []Middleware

	classCapacity map[string]int

	mu            sync.Mutex
	classes       map[string]*concClass
	throttled     map[RunID]string
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
		store:         store,
		clock:         wallClock{},
		logger:        slog.Default(),
		retry:         defaultRetryPolicy,
		concurrency:   16,
		pipelines:     make(map[PipelineID]*Definition),
		stepOwner:     make(map[StepID]PipelineID),
		active:        make(map[RunID]struct{}),
		invalid:       make(map[RunID]*InvalidRunError),
		waiters:       make(map[RunID][]chan struct{}),
		attemptCancel: make(map[RunID]context.CancelFunc),
		wakes:         make(map[RunID]chan struct{}),
		classCapacity: make(map[string]int),
		classes:       make(map[string]*concClass),
		throttled:     make(map[RunID]string),
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

	if e.retention.TerminalAfter > 0 {
		e.wg.Add(1)
		go e.retentionLoop()
	} else {
		e.logger.Info("durable: retention disabled; terminal runs accumulate")
	}

	e.mu.Lock()
	for _, d := range e.pipelines {
		for _, sc := range d.steps {
			if sc.ConcurrencyClass != "" {
				if _, ok := e.classCapacity[sc.ConcurrencyClass]; !ok {
					e.logger.Warn("durable: concurrency class has no configured capacity; unlimited",
						"class", sc.ConcurrencyClass, "step", sc.ID)
				}
			}
		}
	}
	e.mu.Unlock()
	return nil
}

// concClass is the in-memory token pool of one concurrency class.
type concClass struct {
	capacity int
	inUse    int
	waiters  []RunID // FIFO
}

// acquireClass gates an operation on its step's concurrency class.
// proceed=false parks the Run as throttled (woken FIFO on release). A
// pending cancellation bypasses the gate so the Run can resolve; bypassed
// and classless acquisitions hold no token (held=false).
func (e *Engine) acquireClass(rec *RunRecord, class string) (proceed, held bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.throttled, rec.RunID)
	if class == "" || rec.Cancel != nil {
		return true, false
	}
	cap, limited := e.classCapacity[class]
	if !limited {
		return true, false
	}
	c := e.classes[class]
	if c == nil {
		c = &concClass{capacity: cap}
		e.classes[class] = c
	}
	if c.inUse < c.capacity {
		c.inUse++
		return true, true
	}
	if slices.Contains(c.waiters, rec.RunID) {
		e.throttled[rec.RunID] = class
		return false, false
	}
	c.waiters = append(c.waiters, rec.RunID)
	e.throttled[rec.RunID] = class
	return false, false
}

// releaseClass returns a token and wakes the next throttled Run, if any.
func (e *Engine) releaseClass(class string) {
	e.mu.Lock()
	c := e.classes[class]
	var next RunID
	if c != nil {
		c.inUse--
		if len(c.waiters) > 0 {
			next = c.waiters[0]
			c.waiters = c.waiters[1:]
		}
	}
	e.mu.Unlock()
	if next != "" {
		e.dispatch(next, 0)
	}
}

// throttledClass reports the class a Run is currently parked on, if any.
func (e *Engine) throttledClass(id RunID) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	class, ok := e.throttled[id]
	return class, ok
}

// retentionLoop sweeps terminal Runs older than the policy on a jittered
// interval, starting immediately.
func (e *Engine) retentionLoop() {
	defer e.wg.Done()
	for {
		e.sweepRetention()
		d := e.retention.Interval
		d = d/2 + d/4 + time.Duration(mrand.Int64N(int64(d/2))) // [0.75d, 1.25d)
		select {
		case <-e.clock.After(d):
		case <-e.baseCtx.Done():
			return
		}
	}
}

func (e *Engine) sweepRetention() {
	const batch = 256
	cutoff := e.clock.Now().Add(-e.retention.TerminalAfter)
	total := 0
	for {
		n, err := e.store.ReapTerminal(e.baseCtx, cutoff, batch)
		if err != nil {
			if e.baseCtx.Err() == nil {
				e.logger.Error("durable: retention sweep failed", "error", err)
			}
			return
		}
		total += n
		if n < batch {
			break
		}
	}
	if total > 0 {
		e.logger.Info("durable: reaped terminal runs", "count", total, "terminal_before", cutoff)
	}
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

	e.wg.Go(func() {
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
	})
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
			if e.awaitGate(rec) {
				return 0, false
			}
			if delay, wait := e.retryGate(rec); wait {
				return delay, true
			}
			class := def.step(StepID(dec.Step)).ConcurrencyClass
			proceed, held := e.acquireClass(rec, class)
			if !proceed {
				return 0, false
			}
			done, delay, again := e.runForward(rec, def, StepID(dec.Step))
			if held {
				e.releaseClass(class)
			}
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
			if e.awaitGate(rec) {
				return 0, false
			}
			if delay, wait := e.retryGate(rec); wait {
				return delay, true
			}
			class := def.step(StepID(dec.Step)).ConcurrencyClass
			proceed, held := e.acquireClass(rec, class)
			if !proceed {
				return 0, false
			}
			done, delay, again := e.runUnwind(rec, def, StepID(dec.Step))
			if held {
				e.releaseClass(class)
			}
			if !done {
				return delay, again
			}

		case ledger.KindUnwindComplete:
			oc := OutcomeFailure
			rec.Outcome = &oc
			rec.Phase = PhaseDone
			if !e.apply(rec, Transition{Cursor: idleCursor(rec), Outcome: rec.Outcome}) {
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
	rec.RootFailure = &RootFailure{
		Phase:   PhaseForward,
		Message: cause,
		At:      e.clock.Now(),
		Kind:    FailureKindCanceled}
	rec.Phase = PhaseUnwind
	rec.NextAttemptAt = time.Time{}
	return e.apply(rec, Transition{Cursor: idleCursor(rec), RootFailure: rec.RootFailure})
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

// awaitGate parks the Run when its in-flight operation awaits another
// Run that is still nonterminal: it registers a completion watcher and
// returns true (no redispatch — the watcher wakes the Run). A pending
// cancellation bypasses the park so the operation can resolve.
func (e *Engine) awaitGate(rec *RunRecord) bool {
	if rec.AwaitingRunID == "" || rec.Cancel != nil {
		return false
	}
	target := rec.AwaitingRunID
	if cycle, err := e.awaitCycle(rec.RunID, target); err == nil && cycle {
		e.markInvalid(rec, "", fmt.Sprintf("await cycle: awaiting run %s would deadlock back to this run", target))
		return true
	}
	// Register before checking terminality so a completion between the
	// check and the registration cannot be missed.
	ch := e.waiterChan(target)
	trec, err := e.store.GetRun(e.baseCtx, target)
	switch {
	case errors.Is(err, ErrRunNotFound):
		// Resolve immediately: the target never existed or was reaped.
		e.removeWaiter(target, ch)
		return false
	case err != nil:
		e.removeWaiter(target, ch)
		if e.baseCtx.Err() == nil {
			e.logger.Error("durable: await target read failed", "run", rec.RunID, "target", target, "error", err)
		}
		return false
	case trec.Terminal():
		e.removeWaiter(target, ch)
		return false
	}
	parked := rec.RunID
	e.wg.Go(func() {
		select {
		case <-ch:
			e.dispatch(parked, 0)
		case <-e.baseCtx.Done():
		}
	})
	return true
}

// parkAwait records an AwaitRun resolution: the operation stays
// unresolved, the awaited Run is durably noted on the cursor, and the next
// reconciliation parks through awaitGate (or proceeds immediately if the
// target is already terminal or missing).
func (e *Engine) parkAwait(rec *RunRecord, stepID StepID, attempts uint64, target RunID) (bool, time.Duration, bool) {
	if target == "" {
		e.markInvalid(rec, stepID, "AwaitRun with empty RunID")
		return false, 0, false
	}
	rec.AwaitingRunID = target
	if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, attempts)}) {
		return false, time.Second, true
	}
	// Detect cycles after persisting the park: with every parker checking
	// after its own durable write, the later writer always sees the full
	// chain, so a concurrently-formed cycle cannot escape detection.
	cycle, err := e.awaitCycle(rec.RunID, target)
	if err != nil {
		return false, time.Second, true
	}
	if cycle {
		e.markInvalid(rec, stepID, fmt.Sprintf("await cycle: awaiting run %s would deadlock back to this run", target))
		return false, 0, false
	}
	return true, 0, false
}

// awaitCycle walks the await chain from target looking for self.
func (e *Engine) awaitCycle(self, target RunID) (bool, error) {
	cur := target
	for hops := 0; cur != "" && hops < 64; hops++ {
		if cur == self {
			return true, nil
		}
		rec, err := e.store.GetRun(e.baseCtx, cur)
		if errors.Is(err, ErrRunNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if rec.Terminal() {
			return false, nil
		}
		cur = rec.AwaitingRunID
	}
	return false, nil
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
	awaited := rec.AwaitingRunID
	rec.AwaitingRunID = ""
	sr.ForwardStatus = OpUnresolved
	sr.ForwardAttempts++
	rec.NextAttemptAt = time.Time{}
	if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.ForwardAttempts)}) {
		return false, time.Second, true
	}

	inv := e.invocation(rec, def, stepID, sr.ForwardAttempts, PhaseForward)
	inv.awaitedRunID = awaited
	state, err := e.invokeForward(sc, inv)

	if v := inv.takeViolation(); v != nil {
		e.markInvalid(rec, stepID, v.Error())
		return false, 0, false
	}

	if aw, ok := asAwait(err); ok {
		return e.parkAwait(rec, stepID, sr.ForwardAttempts, aw.target)
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
		if !e.apply(rec, Transition{Cursor: idleCursor(rec), Steps: []StepWrite{{StepID: stepID, Record: *sr}}}) {
			return false, time.Second, true
		}
		return true, 0, false

	case permanent:
		sr.ForwardStatus = OpFailed
		rec.RootFailure = &RootFailure{
			StepID:  stepID,
			Phase:   PhaseForward,
			Attempt: sr.ForwardAttempts,
			Message: pe.err.Error(),
			At:      now,
			Kind:    pe.failureKind(),
			Reason:  pe.failureReason()}
		rec.Phase = PhaseUnwind
		clearLastError(rec)
		if !e.apply(rec, Transition{Cursor: idleCursor(rec), Steps: []StepWrite{{StepID: stepID, Record: *sr}}, RootFailure: rec.RootFailure}) {
			return false, time.Second, true
		}
		return true, 0, false

	default:
		d := e.backoff(sr.ForwardAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.ForwardAttempts)}) {
			return false, time.Second, true
		}
		return false, d, true
	}
}

// runUnwind reserves an attempt and executes one unwind operation.
func (e *Engine) runUnwind(rec *RunRecord, def *Definition, stepID StepID) (done bool, delay time.Duration, again bool) {
	sc := def.step(stepID)
	sr := rec.Step(stepID)

	awaited := rec.AwaitingRunID
	rec.AwaitingRunID = ""
	sr.UnwindStatus = OpUnresolved
	sr.UnwindAttempts++
	rec.NextAttemptAt = time.Time{}
	if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.UnwindAttempts)}) {
		return false, time.Second, true
	}

	inv := e.invocation(rec, def, stepID, sr.UnwindAttempts, PhaseUnwind)
	inv.awaitedRunID = awaited
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

	if aw, ok := asAwait(err); ok {
		return e.parkAwait(rec, stepID, sr.UnwindAttempts, aw.target)
	}

	now := e.clock.Now()
	switch pe, permanent := asPermanent(err); {
	case err == nil:
		sr.UnwindStatus = OpSucceeded
		clearLastError(rec)
		if !e.apply(rec, Transition{Cursor: idleCursor(rec), Steps: []StepWrite{{StepID: stepID, Record: *sr}}}) {
			return false, time.Second, true
		}
		return true, 0, false

	case permanent:
		sr.UnwindStatus = OpFailed
		rec.UnwindFailures = append(rec.UnwindFailures, UnwindFailure{
			StepID:  stepID,
			Phase:   PhaseUnwind,
			Attempt: sr.UnwindAttempts,
			Message: pe.err.Error(),
			At:      now,
			Kind:    pe.failureKind(),
			Reason:  pe.failureReason()})
		clearLastError(rec)
		uf := rec.UnwindFailures[len(rec.UnwindFailures)-1]
		if !e.apply(rec, Transition{Cursor: idleCursor(rec), Steps: []StepWrite{{StepID: stepID, Record: *sr}}, UnwindFailure: &uf}) {
			return false, time.Second, true
		}
		return true, 0, false

	default:
		d := e.backoff(sr.UnwindAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.UnwindAttempts)}) {
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
	if !e.apply(rec, Transition{Cursor: idleCursor(rec), Output: rec.Output, Outcome: rec.Outcome}) {
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

// activeCursor builds the Cursor for a Run whose operation on stepID is
// in flight; idleCursor for a Run with none.
func activeCursor(rec *RunRecord, stepID StepID, attempts uint64) Cursor {
	return Cursor{
		Phase:         rec.Phase,
		StepID:        stepID,
		Attempts:      attempts,
		AwaitingRunID: rec.AwaitingRunID,
		NextAttemptAt: rec.NextAttemptAt,
		LastError:     rec.LastError,
		LastReason:    rec.LastReason,
		LastErrorAt:   rec.LastErrorAt,
	}
}

func idleCursor(rec *RunRecord) Cursor { return activeCursor(rec, "", 0) }

// apply performs one atomic durable transition. Any unresolved operation
// in rec covered by neither the cursor nor an explicit step write (an
// unwind operation displaced by topology change) is flushed as a step row
// so its attempt count survives.
func (e *Engine) apply(rec *RunRecord, t Transition) bool {
	rec.UpdatedAt = e.clock.Now()
	t.Cursor.UpdatedAt = rec.UpdatedAt
	for id, sr := range rec.Steps {
		if id == t.Cursor.StepID {
			continue
		}
		if sr.ForwardStatus != OpUnresolved && sr.UnwindStatus != OpUnresolved {
			continue
		}
		covered := false
		for _, sw := range t.Steps {
			if sw.StepID == id {
				covered = true
				break
			}
		}
		if !covered {
			t.Steps = append(t.Steps, StepWrite{StepID: id, Record: *sr})
		}
	}
	if err := e.store.ApplyTransition(e.baseCtx, rec.RunID, t); err != nil {
		if e.baseCtx.Err() == nil {
			e.logger.Error("durable: store transition failed", "run", rec.RunID, "error", err)
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

// removeWaiter unregisters a waiter channel that will not be waited on.
func (e *Engine) removeWaiter(id RunID, ch chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	chs := e.waiters[id]
	for i, c := range chs {
		if c == ch {
			e.waiters[id] = append(chs[:i], chs[i+1:]...)
			break
		}
	}
	if len(e.waiters[id]) == 0 {
		delete(e.waiters, id)
	}
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

// runIDEntropy makes concurrent Schedule calls collision-free and orders
// RunIDs created within the same millisecond.
var runIDEntropy = &ulid.LockedMonotonicReader{MonotonicReader: ulid.Monotonic(rand.Reader, 0)}

// newRunID generates a ULID RunID: time-prefixed and lexicographically
// creation-ordered. This is an implementation convenience for debugging,
// key layout, and tooling — RunIDs remain opaque strings, no API compares
// them, and CreatedAt stays authoritative for ordering.
func newRunID(now time.Time) RunID {
	if now.Before(time.Unix(0, 0)) {
		now = time.Unix(0, 0)
	}
	id, err := ulid.New(ulid.Timestamp(now), runIDEntropy)
	if err != nil {
		panic(fmt.Sprintf("durable: generating run id: %v", err))
	}
	return RunID(id.String())
}
