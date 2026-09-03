package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable/internal/dispatcher"
	"github.com/dangra/durable/internal/ledger"
	"github.com/dangra/durable/internal/tokenpool"
	"github.com/dangra/durable/internal/watchset"
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

// WithLogger sets the structured logger for engine lifecycle events and
// operational diagnostics; the default is slog.Default().
//
// The Engine logs per-attempt progress (scheduled, retrying, succeeded,
// awaiting, throttled) at Debug; once-per-run milestones (terminal
// outcome, unwind start, cancellation accepted) at Info; permanent unwind
// failures at Warn; and anomalies (handler panics, store errors, invalid
// runs) at Error. Lifecycle lines carry the canonical keys pipeline,
// resource, and run; operation-scoped lines add step, phase, and attempt;
// failure causes appear under error. Anomaly diagnostics carry at least
// run. Invocation.Logger pre-attaches the same canonical keys for handler
// code.
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
	observers       []Observer

	classCapacity map[string]int

	// pool gates operations on their step's concurrency class; it is
	// internally locked, independent of mu.
	pool *tokenpool.Pool[string, RunID]

	mu            sync.Mutex
	awaitParked   map[RunID]time.Time
	started       bool
	invalid       map[RunID]*InvalidRunError
	attemptCancel map[RunID]context.CancelFunc

	// pipelines and stepOwner are written only before Start, under mu
	// (register rejects later binds); after the Start freeze they are
	// read without locking.
	pipelines map[PipelineID]*Definition
	stepOwner map[StepID]PipelineID

	// waiters broadcasts a Run's terminal/invalid notification to
	// Run.Wait callers and await watchers; it is internally locked,
	// independent of mu.
	waiters watchset.Set[RunID]

	// disp runs at most one worker per Run with bounded concurrency; it
	// exists once the engine has started.
	disp *dispatcher.Dispatcher[RunID]

	baseCtx context.Context
	cancel  context.CancelFunc
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
		invalid:       make(map[RunID]*InvalidRunError),
		attemptCancel: make(map[RunID]context.CancelFunc),
		classCapacity: make(map[string]int),
		awaitParked:   make(map[RunID]time.Time),
	}
	for _, o := range opts {
		o(e)
	}
	e.pool = tokenpool.New[string, RunID](e.classCapacity)
	// A StoreOp subscription observes every Store call, so the wrap must
	// cover the engine's own store handle.
	if e.hasStoreObserver() {
		e.store = &observedStore{inner: e.store, engine: e}
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
	e.disp = dispatcher.New(dispatcher.Config[RunID]{
		Ctx:         e.baseCtx,
		Concurrency: e.concurrency,
		Clock:       e.clock,
		Spawn:       e.wg.Go,
		Run:         e.processRun,
	})
	e.mu.Unlock()

	recs, err := e.store.ListNonterminal(ctx)
	if err != nil {
		return fmt.Errorf("durable: discovering nonterminal runs: %w", err)
	}
	if len(recs) > 0 {
		e.logger.Info("durable: recovering nonterminal runs", "count", len(recs))
	}
	for _, rec := range recs {
		delay := time.Duration(0)
		if hasUnresolvedOp(rec) {
			delay = e.recoveryBackoff
		}
		e.disp.Dispatch(rec.RunID, delay)
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

// acquireClass gates an operation on its step's concurrency class via
// the token pool. proceed=false parks the Run as throttled (woken FIFO
// on release); a pending cancellation — or a classless step — bypasses
// the gate so the Run can resolve, holding no token. waited is how long
// the Run had been parked before an actual token grant. Kicks owed by
// the pool (declined or cascaded wakes) are dispatched here.
func (e *Engine) acquireClass(rec *RunRecord, class string) (proceed, held bool, waited time.Duration) {
	bypass := class == "" || rec.Cancel != nil
	granted, held, waited, kicks := e.pool.Acquire(class, rec.RunID, bypass, e.clock.Now())
	for _, id := range kicks {
		e.disp.Dispatch(id, 0)
	}
	return granted, held, waited
}

// releaseClass returns a token and wakes the head waiter, if any.
func (e *Engine) releaseClass(class string) {
	if kick, ok := e.pool.Release(class); ok {
		e.disp.Dispatch(kick, 0)
	}
}

// clearClassWait removes a Run's concurrency-class park for paths that
// resolve it without passing through acquireClass (cancellation,
// invalidity), waking the next waiter if its departure exposed capacity.
func (e *Engine) clearClassWait(id RunID) {
	if kick, ok := e.pool.Clear(id); ok {
		e.disp.Dispatch(kick, 0)
	}
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
			break // batches already emitted below stay reported
		}
		// Emit per batch: a later batch erroring must not lose the
		// event for runs this batch already durably deleted.
		if n > 0 {
			total += n
			e.emitRunsReaped(n)
		}
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
			e.waiters.Notify(id)
			return 0, false
		}

		// Lock-free read: pipelines is frozen at Start (register rejects
		// later binds under mu), and every worker descends from a
		// mu-synchronized point after that freeze — Start's recovery
		// dispatches, or a Schedule that passed isStarted. The last
		// write happens-before every read here.
		def := e.pipelines[rec.PipelineID]
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
			proceed, held, waited := e.acquireClass(rec, class)
			if !proceed {
				if e.debugLog() {
					e.logger.Debug("durable: run throttled",
						"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
						"run", string(rec.RunID), "step", dec.Step, "class", class)
				}
				return 0, false
			}
			if waited > 0 {
				e.emitClassWait(ClassWaitEvent{
					PipelineID: rec.PipelineID, ResourceID: rec.ResourceID,
					RunID: rec.RunID, Class: class, Duration: waited})
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
			proceed, held, waited := e.acquireClass(rec, class)
			if !proceed {
				if e.debugLog() {
					e.logger.Debug("durable: run throttled",
						"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
						"run", string(rec.RunID), "step", dec.Step, "class", class)
				}
				return 0, false
			}
			if waited > 0 {
				e.emitClassWait(ClassWaitEvent{
					PipelineID: rec.PipelineID, ResourceID: rec.ResourceID,
					RunID: rec.RunID, Class: class, Duration: waited})
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
			e.completeRun(rec)
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
	if !e.apply(rec, Transition{Cursor: idleCursor(rec), RootFailure: rec.RootFailure}) {
		return false
	}
	// A canceled Run may have been parked on a class without ever
	// re-entering acquireClass; drop its park state so Stats stays
	// accurate and no stale FIFO entry eats a future wake.
	e.clearClassWait(rec.RunID)
	e.logger.Info("durable: cancellation accepted; unwinding",
		"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
		"run", string(rec.RunID), "cause", cause)
	e.emitRunUnwinding(RunFailureEvent{
		PipelineID: rec.PipelineID, ResourceID: rec.ResourceID, RunID: rec.RunID,
		Kind: FailureKindCanceled, Message: cause})
	return true
}

// completeRun records a Run's terminal commit on both observability
// planes — the level-policy log line and the RunTerminal observer event
// — and releases its waiters. Every terminal path goes through this one
// seam; rec must already carry the committed outcome and final
// UpdatedAt.
func (e *Engine) completeRun(rec *RunRecord) {
	e.logger.Info("durable: run complete",
		"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
		"run", string(rec.RunID), "outcome", rec.Outcome.String(),
		"elapsed", rec.UpdatedAt.Sub(rec.CreatedAt))
	e.emitRunTerminal(rec)
	e.waiters.Notify(rec.RunID)
}

// attemptResolved is the single seam through which a resolved, retried,
// or permanently failed operation attempt reaches both observability
// planes: the log line and the AttemptDone event are produced from the
// same facts, so a future resolution path cannot serve one and drift on
// the other. For AttemptFailed, attribution comes off the record — the
// RootFailure forward, the newest UnwindFailure during unwind.
func (e *Engine) attemptResolved(rec *RunRecord, stepID StepID, phase Phase, attempt uint64, elapsed time.Duration, result AttemptResult, err error, retryIn time.Duration, panicked bool) {
	switch result {
	case AttemptSucceeded:
		if e.debugLog() {
			e.logger.Debug("durable: operation succeeded",
				"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
				"run", string(rec.RunID), "step", string(stepID), "phase", phase.String(),
				"attempt", attempt, "elapsed", elapsed)
		}
	case AttemptRetrying:
		if e.debugLog() {
			e.logger.Debug("durable: operation failed; will retry",
				"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
				"run", string(rec.RunID), "step", string(stepID), "phase", phase.String(),
				"attempt", attempt, "error", err, "next_attempt_at", rec.NextAttemptAt)
		}
	case AttemptFailed:
		if phase == PhaseForward {
			e.logger.Info("durable: run failed; unwinding",
				"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
				"run", string(rec.RunID), "step", string(stepID), "attempt", attempt,
				"error", err, "kind", rec.RootFailure.Kind.String(), "reason", rec.RootFailure.Reason)
		} else {
			uf := rec.UnwindFailures[len(rec.UnwindFailures)-1]
			// Warn, not Info: a permanently failed unwind means
			// compensation did not happen — external resources may be
			// leaked.
			e.logger.Warn("durable: unwind step failed permanently",
				"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
				"run", string(rec.RunID), "step", string(stepID), "attempt", attempt,
				"error", err, "kind", uf.Kind.String(), "reason", uf.Reason)
		}
	}
	e.emitAttemptDone(AttemptEvent{
		PipelineID: rec.PipelineID, ResourceID: rec.ResourceID, RunID: rec.RunID,
		StepID: stepID, Phase: phase, Attempt: attempt,
		Duration: elapsed, Result: result, Err: err, RetryIn: retryIn, Panicked: panicked})
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
		if rec.AwaitingRunID != "" {
			// A cancellation is bypassing an existing park: the Run
			// proceeds now, so it is no longer awaiting. The stale
			// watcher goroutine sees the missing entry and stays silent.
			e.mu.Lock()
			delete(e.awaitParked, rec.RunID)
			e.mu.Unlock()
		}
		return false
	}
	target := rec.AwaitingRunID
	if cycle, err := e.awaitCycle(rec.RunID, target); err == nil && cycle {
		e.markInvalid(rec, "", fmt.Sprintf("await cycle: awaiting run %s would deadlock back to this run", target))
		return true
	}
	// Register before checking terminality so a completion between the
	// check and the registration cannot be missed.
	ch, cancelWatch := e.waiters.Watch(target)
	trec, err := e.store.GetRun(e.baseCtx, target)
	switch {
	case errors.Is(err, ErrRunNotFound):
		// Resolve immediately: the target never existed or was reaped.
		cancelWatch()
		e.resolveAwaitPark(rec, target, true)
		return false
	case err != nil:
		cancelWatch()
		if e.baseCtx.Err() == nil {
			e.logger.Error("durable: await target read failed", "run", rec.RunID, "target", target, "error", err)
		}
		e.resolveAwaitPark(rec, target, false)
		return false
	case trec.Terminal():
		cancelWatch()
		e.resolveAwaitPark(rec, target, true)
		return false
	}
	parked := rec.RunID
	e.mu.Lock()
	// Preserve an existing park time: a spurious wake (the target marked
	// invalid, notify firing without terminality) re-parks here, and the
	// Run has been logically awaiting since its first park.
	if _, already := e.awaitParked[parked]; !already {
		e.awaitParked[parked] = e.clock.Now()
	}
	e.mu.Unlock()
	// The watcher only pokes: whether the wake is real — the target
	// actually terminal or gone — is decided by the re-run of this gate,
	// which also emits WaiterWoken. notify also fires for invalid runs,
	// and that wake must neither emit nor reset the park.
	e.wg.Go(func() {
		select {
		case <-ch:
			e.disp.Dispatch(parked, 0)
		case <-e.baseCtx.Done():
			cancelWatch()
		}
	})
	return true
}

// resolveAwaitPark closes out a Run's AwaitRun park, if one is recorded:
// the park entry is dropped, and a genuine resolution (the target
// terminal or missing, not a store read error) emits WaiterWoken with
// the full first-park-to-resolution duration.
func (e *Engine) resolveAwaitPark(rec *RunRecord, target RunID, resolved bool) {
	e.mu.Lock()
	since, present := e.awaitParked[rec.RunID]
	delete(e.awaitParked, rec.RunID)
	e.mu.Unlock()
	if present && resolved {
		e.emitWaiterWoken(WakeEvent{
			PipelineID: rec.PipelineID, ResourceID: rec.ResourceID,
			RunID: rec.RunID, Target: target, Duration: e.clock.Now().Sub(since)})
	}
}

// parkAwait records an AwaitRun resolution: the operation stays
// unresolved, the awaited Run is durably noted on the cursor, and the next
// reconciliation parks through awaitGate (or proceeds immediately if the
// target is already terminal or missing).
func (e *Engine) parkAwait(rec *RunRecord, stepID StepID, attempts uint64, target RunID, elapsed time.Duration) (bool, time.Duration, bool) {
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
	if e.debugLog() {
		e.logger.Debug("durable: run awaiting",
			"pipeline", string(rec.PipelineID), "resource", string(rec.ResourceID),
			"run", string(rec.RunID), "step", string(stepID), "target", string(target))
	}
	e.emitAttemptDone(AttemptEvent{
		PipelineID: rec.PipelineID, ResourceID: rec.ResourceID, RunID: rec.RunID,
		StepID: stepID, Phase: rec.Phase, Attempt: attempts,
		Duration: elapsed, Result: AttemptAwaiting})
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
	opStart := e.clock.Now()
	state, panicked, err := e.invokeForward(sc, inv)

	if v := inv.takeViolation(); v != nil {
		e.markInvalid(rec, stepID, v.Error())
		return false, 0, false
	}

	if aw, ok := asAwait(err); ok {
		return e.parkAwait(rec, stepID, sr.ForwardAttempts, aw.target, e.clock.Now().Sub(opStart))
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
		e.attemptResolved(rec, stepID, PhaseForward, sr.ForwardAttempts, now.Sub(opStart), AttemptSucceeded, nil, 0, false)
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
		e.attemptResolved(rec, stepID, PhaseForward, sr.ForwardAttempts, now.Sub(opStart), AttemptFailed, pe.err, 0, false)
		e.emitRunUnwinding(RunFailureEvent{
			PipelineID: rec.PipelineID, ResourceID: rec.ResourceID, RunID: rec.RunID,
			StepID: stepID, Kind: rec.RootFailure.Kind, Reason: rec.RootFailure.Reason,
			Message: rec.RootFailure.Message})
		return true, 0, false

	default:
		d := e.backoff(sr.ForwardAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.ForwardAttempts)}) {
			return false, time.Second, true
		}
		e.attemptResolved(rec, stepID, PhaseForward, sr.ForwardAttempts, now.Sub(opStart), AttemptRetrying, err, d, panicked)
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
	opStart := e.clock.Now()
	failure := Failure{
		UnwindFailures: append([]UnwindFailure(nil), rec.UnwindFailures...),
	}
	if rec.RootFailure != nil {
		failure.Root = *rec.RootFailure
	}
	panicked, err := e.invokeUnwind(sc, inv, failure)

	if v := inv.takeViolation(); v != nil {
		e.markInvalid(rec, stepID, v.Error())
		return false, 0, false
	}

	if aw, ok := asAwait(err); ok {
		return e.parkAwait(rec, stepID, sr.UnwindAttempts, aw.target, e.clock.Now().Sub(opStart))
	}

	now := e.clock.Now()
	switch pe, permanent := asPermanent(err); {
	case err == nil:
		sr.UnwindStatus = OpSucceeded
		clearLastError(rec)
		if !e.apply(rec, Transition{Cursor: idleCursor(rec), Steps: []StepWrite{{StepID: stepID, Record: *sr}}}) {
			return false, time.Second, true
		}
		e.attemptResolved(rec, stepID, PhaseUnwind, sr.UnwindAttempts, now.Sub(opStart), AttemptSucceeded, nil, 0, false)
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
		e.attemptResolved(rec, stepID, PhaseUnwind, sr.UnwindAttempts, now.Sub(opStart), AttemptFailed, pe.err, 0, false)
		return true, 0, false

	default:
		d := e.backoff(sr.UnwindAttempts)
		rec.NextAttemptAt = now.Add(d)
		recordLastError(rec, err, now)
		if !e.apply(rec, Transition{Cursor: activeCursor(rec, stepID, sr.UnwindAttempts)}) {
			return false, time.Second, true
		}
		e.attemptResolved(rec, stepID, PhaseUnwind, sr.UnwindAttempts, now.Sub(opStart), AttemptRetrying, err, d, panicked)
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
	e.completeRun(rec)
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
		baseLogger:      e.logger,
	}
}

// debugLog reports whether the logger records Debug. Per-attempt log sites
// guard on it so a disabled level costs no argument boxing in hot paths.
func (e *Engine) debugLog() bool {
	return e.logger.Enabled(context.Background(), slog.LevelDebug)
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

func (e *Engine) invokeForward(sc *StepConfig, inv *Invocation) (state proto.Message, panicked bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			panicked = true
			err = fmt.Errorf("handler panic: %v", p)
			e.logger.Error("durable: forward handler panic",
				"run", inv.runID, "step", inv.stepID, "attempt", inv.attempt,
				"panic", p, "stack", string(debug.Stack()))
		}
	}()
	ctx, done := e.attemptContext(inv.runID)
	defer done()
	state, err = e.wrap(Handler(sc.Run))(ctx, inv)
	return state, false, err
}

func (e *Engine) invokeUnwind(sc *StepConfig, inv *Invocation, failure Failure) (panicked bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			panicked = true
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
	return false, err
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
	// Idempotent per (run, reason): re-dispatches of an already-invalid
	// Run (a Cancel, a repoke) re-derive the same conclusion every pass
	// and must not re-log or re-fire RunInvalid.
	if prev, ok := e.invalid[rec.RunID]; ok && prev.Reason == reason {
		e.mu.Unlock()
		return
	}
	e.invalid[rec.RunID] = ie
	e.mu.Unlock()
	e.clearClassWait(rec.RunID)
	e.logger.Error("durable: run invalid for current deployment",
		"run", rec.RunID,
		"pipeline", rec.PipelineID,
		"resource", rec.ResourceID,
		"phase", rec.Phase.String(),
		"step", stepID,
		"reason", reason)
	e.emitRunInvalid(RunFailureEvent{
		PipelineID: rec.PipelineID, ResourceID: rec.ResourceID, RunID: rec.RunID,
		StepID: stepID, Reason: reason})
	e.waiters.Notify(rec.RunID)
}

func (e *Engine) invalidFor(id RunID) *InvalidRunError {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.invalid[id]
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
