package durable

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// Observer receives engine lifecycle events for metrics. Nil fields are
// skipped at the cost of a nil check per event. Use WithObserver
// (repeatable — observers compose) to install one.
//
// Callbacks run synchronously on engine goroutines immediately after the
// fact they report is durably committed: they must be fast and must not
// block. A callback that panics is recovered and logged at Error; it
// never affects the Run. Callbacks may call Engine.Stats.
//
// Events are in-memory facts about this process, not durable records: a
// crash between a commit and its callback loses the event, and a
// redispatched at-least-once attempt can fire again. Treat counters
// built from them as operational signal, not accounting — anything that
// must be exact reads the Store.
type Observer struct {
	// RunScheduled fires when Schedule accepts a new Run (created=true).
	RunScheduled func(RunEvent)
	// AttemptDone fires when one operation attempt resolves (success or
	// permanent failure), schedules a retry, or parks via AwaitRun.
	AttemptDone func(AttemptEvent)
	// RunUnwinding fires when a RootFailure is established — permanent
	// forward failure or an accepted cancellation — and unwind begins.
	RunUnwinding func(RunFailureEvent)
	// RunTerminal fires when a Run commits its terminal outcome.
	RunTerminal func(RunTerminalEvent)
	// RunInvalid fires when the current deployment marks a Run invalid.
	RunInvalid func(RunFailureEvent)
	// WaiterWoken fires when a Run parked via AwaitRun resolves its park
	// because the awaited Run reached terminality or was found missing;
	// Duration spans first park to resolution. A cancellation bypass or
	// a spurious poke (the target turning invalid) emits nothing.
	WaiterWoken func(WakeEvent)
	// ClassWait fires when a Run throttled on a concurrency class is
	// granted a token, reporting how long it waited for it. A canceled
	// Run that bypasses the gate without a token emits nothing.
	ClassWait func(ClassWaitEvent)
	// RunsReaped fires after each retention sweep that deleted anything.
	RunsReaped func(count int)
	// StoreOp fires after every Store call, reads included. Installing
	// it wraps the Store at NewEngine time.
	StoreOp func(StoreOpEvent)
}

// WithObserver installs an Observer. Multiple observers compose: each
// event fires on every installed observer, in installation order.
func WithObserver(o Observer) Option {
	return func(e *Engine) {
		e.observers = append(e.observers, o)
	}
}

// RunEvent identifies a Run at acceptance. StartAt is nonzero when the
// Run was scheduled with a delayed start. Annotations is shared with
// every observer of the event and must not be modified.
type RunEvent struct {
	PipelineID  PipelineID
	ResourceID  ResourceID
	RunID       RunID
	StartAt     time.Time
	Annotations map[string]string
}

// AttemptResult classifies how an operation attempt ended.
type AttemptResult uint8

const (
	// AttemptSucceeded resolved the operation successfully.
	AttemptSucceeded AttemptResult = iota + 1
	// AttemptRetrying left the operation unresolved; a retry is
	// scheduled RetryIn from now.
	AttemptRetrying
	// AttemptFailed resolved the operation as permanently failed.
	AttemptFailed
	// AttemptAwaiting parked the operation via AwaitRun.
	AttemptAwaiting
)

func (r AttemptResult) String() string {
	switch r {
	case AttemptSucceeded:
		return "succeeded"
	case AttemptRetrying:
		return "retrying"
	case AttemptFailed:
		return "failed"
	case AttemptAwaiting:
		return "awaiting"
	default:
		return "unknown"
	}
}

// AttemptEvent reports one resolved, retried, or parked operation attempt.
type AttemptEvent struct {
	PipelineID PipelineID
	ResourceID ResourceID
	RunID      RunID
	StepID     StepID
	Phase      Phase
	Attempt    uint64
	// Duration is the handler execution time of this attempt.
	Duration time.Duration
	Result   AttemptResult
	// Err is the handler error for AttemptRetrying and AttemptFailed.
	Err error
	// RetryIn is the scheduled backoff delay for AttemptRetrying.
	RetryIn time.Duration
	// Panicked marks an attempt whose handler panicked; the panic is
	// treated as an ordinary retryable error.
	Panicked bool
}

// RunFailureEvent reports a RootFailure being established (RunUnwinding)
// or a Run turning invalid for the current deployment (RunInvalid, where
// only Reason is populated besides the identity, and StepID when the
// problem is step-scoped).
type RunFailureEvent struct {
	PipelineID PipelineID
	ResourceID ResourceID
	RunID      RunID
	StepID     StepID
	Kind       FailureKind
	Reason     string
	Message    string
}

// RunTerminalEvent reports a Run's terminal commit. Kind and Reason
// carry RootFailure attribution for OutcomeFailure; Duration is
// acceptance-to-terminal. Annotations is shared with every observer of
// the event and must not be modified — it carries the acceptance-time
// metadata (tenant tags) metric adapters label by.
type RunTerminalEvent struct {
	PipelineID  PipelineID
	ResourceID  ResourceID
	RunID       RunID
	Outcome     Outcome
	Kind        FailureKind
	Reason      string
	Duration    time.Duration
	Annotations map[string]string
}

// WakeEvent reports an AwaitRun park resolving: Duration is how long
// RunID was parked on Target.
type WakeEvent struct {
	PipelineID PipelineID
	ResourceID ResourceID
	RunID      RunID
	Target     RunID
	Duration   time.Duration
}

// ClassWaitEvent reports a Run proceeding after being throttled on a
// concurrency class for Duration.
type ClassWaitEvent struct {
	PipelineID PipelineID
	ResourceID ResourceID
	RunID      RunID
	Class      string
	Duration   time.Duration
}

// StoreOpEvent reports one Store call: Op is the method name, and Write
// marks methods that durably write. Write is declared by the engine's
// store decorator method-by-method, so a Store method added later cannot
// reach observers unclassified — the decorator will not compile without
// an implementation stating it.
type StoreOpEvent struct {
	Op       string
	Write    bool
	Duration time.Duration
	Err      error
}

// emit invokes one observer callback, recovering panics so telemetry can
// never affect a Run.
func emit[E any](e *Engine, f func(E), ev E) {
	if f == nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			e.logger.Error("durable: observer panic",
				"panic", p, "event", fmt.Sprintf("%T", ev), "stack", string(debug.Stack()))
		}
	}()
	f(ev)
}

func (e *Engine) emitRunScheduled(ev RunEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunScheduled, ev)
	}
}

func (e *Engine) emitAttemptDone(ev AttemptEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].AttemptDone, ev)
	}
}

func (e *Engine) emitRunUnwinding(ev RunFailureEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunUnwinding, ev)
	}
}

func (e *Engine) emitRunTerminal(rec *RunRecord) {
	if len(e.observers) == 0 {
		return
	}
	ev := RunTerminalEvent{
		PipelineID:  rec.PipelineID,
		ResourceID:  rec.ResourceID,
		RunID:       rec.RunID,
		Outcome:     *rec.Outcome,
		Duration:    rec.UpdatedAt.Sub(rec.CreatedAt),
		Annotations: rec.Annotations,
	}
	if rec.RootFailure != nil {
		ev.Kind = rec.RootFailure.Kind
		ev.Reason = rec.RootFailure.Reason
	}
	for i := range e.observers {
		emit(e, e.observers[i].RunTerminal, ev)
	}
}

func (e *Engine) emitRunInvalid(ev RunFailureEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunInvalid, ev)
	}
}

func (e *Engine) emitWaiterWoken(ev WakeEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].WaiterWoken, ev)
	}
}

func (e *Engine) emitClassWait(ev ClassWaitEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].ClassWait, ev)
	}
}

func (e *Engine) emitRunsReaped(count int) {
	for i := range e.observers {
		emit(e, e.observers[i].RunsReaped, count)
	}
}

func (e *Engine) emitStoreOp(ev StoreOpEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].StoreOp, ev)
	}
}

// hasStoreObserver reports whether any installed observer subscribes to
// StoreOp, deciding whether NewEngine wraps the Store.
func (e *Engine) hasStoreObserver() bool {
	for i := range e.observers {
		if e.observers[i].StoreOp != nil {
			return true
		}
	}
	return false
}

// observedStore decorates the engine's Store with StoreOp events. Every
// Store method is implemented explicitly, no embedding: a method added
// to the interface later breaks this build instead of escaping
// observation.
type observedStore struct {
	inner  Store
	engine *Engine
}

func (s *observedStore) op(name string, write bool, start time.Time, err error) {
	s.engine.emitStoreOp(StoreOpEvent{Op: name, Write: write, Duration: s.engine.clock.Now().Sub(start), Err: err})
}

func (s *observedStore) CreateRun(ctx context.Context, rec *RunRecord) (*RunRecord, bool, error) {
	start := s.engine.clock.Now()
	existing, created, err := s.inner.CreateRun(ctx, rec)
	s.op("CreateRun", true, start, err)
	return existing, created, err
}

func (s *observedStore) GetRun(ctx context.Context, id RunID) (*RunRecord, error) {
	start := s.engine.clock.Now()
	rec, err := s.inner.GetRun(ctx, id)
	s.op("GetRun", false, start, err)
	return rec, err
}

func (s *observedStore) ApplyTransition(ctx context.Context, id RunID, t Transition) error {
	start := s.engine.clock.Now()
	err := s.inner.ApplyTransition(ctx, id, t)
	s.op("ApplyTransition", true, start, err)
	return err
}

func (s *observedStore) RequestCancel(ctx context.Context, id RunID, req CancelRequest) (bool, error) {
	start := s.engine.clock.Now()
	accepted, err := s.inner.RequestCancel(ctx, id, req)
	s.op("RequestCancel", true, start, err)
	return accepted, err
}

func (s *observedStore) ReapTerminal(ctx context.Context, before time.Time, limit int) (int, error) {
	start := s.engine.clock.Now()
	n, err := s.inner.ReapTerminal(ctx, before, limit)
	s.op("ReapTerminal", true, start, err)
	return n, err
}

func (s *observedStore) ListNonterminal(ctx context.Context) ([]*RunRecord, error) {
	start := s.engine.clock.Now()
	recs, err := s.inner.ListNonterminal(ctx)
	s.op("ListNonterminal", false, start, err)
	return recs, err
}

func (s *observedStore) ListRuns(ctx context.Context, pipeline PipelineID, resource ResourceID) ([]*RunRecord, error) {
	start := s.engine.clock.Now()
	recs, err := s.inner.ListRuns(ctx, pipeline, resource)
	s.op("ListRuns", false, start, err)
	return recs, err
}

func (s *observedStore) Close() error {
	start := s.engine.clock.Now()
	err := s.inner.Close()
	s.op("Close", false, start, err)
	return err
}

// ClassStats is the point-in-time state of one configured concurrency
// class.
type ClassStats struct {
	Capacity int
	InUse    int
	Waiting  int
}

// EngineStats is a point-in-time snapshot of engine occupancy, intended
// for poll-style gauge collection.
type EngineStats struct {
	// ActiveRuns is the number of Runs with a live worker (executing or
	// in a delayed dispatch).
	ActiveRuns int
	// AwaitingRuns is the number of Runs parked via AwaitRun.
	AwaitingRuns int
	// ThrottledRuns is the number of Runs parked on concurrency classes.
	ThrottledRuns int
	// DelayedRuns is the number of Runs waiting out a retry backoff or
	// delayed start.
	DelayedRuns int
	// InvalidRuns is the number of Runs marked invalid for the current
	// deployment.
	InvalidRuns int
	// Classes holds per-class token occupancy for classes that have
	// been used since Start.
	Classes map[string]ClassStats
}

// Stats returns a point-in-time snapshot of engine occupancy. It is safe
// for concurrent use, including from Observer callbacks.
func (e *Engine) Stats() EngineStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := EngineStats{
		AwaitingRuns: len(e.awaitParked),
		InvalidRuns:  len(e.invalid),
	}
	if e.disp != nil {
		st.ActiveRuns = e.disp.Active()
		st.DelayedRuns = e.disp.Delayed()
	}
	if usage := e.pool.Snapshot(); len(usage) > 0 {
		st.Classes = make(map[string]ClassStats, len(usage))
		for name, u := range usage {
			st.Classes[name] = ClassStats{Capacity: u.Capacity, InUse: u.InUse, Waiting: u.Waiting}
			st.ThrottledRuns += u.Waiting
		}
	}
	return st
}
