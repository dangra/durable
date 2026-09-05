package engine

import (
	"context"
	"fmt"
	"github.com/dangra/durable"
	"github.com/dangra/durable/observe"
	"github.com/dangra/durable/store/driver"
	"runtime/debug"
	"time"
)

// WithObserver installs an observe.Observer. Multiple observers compose: each
// event fires on every installed observer, in installation order.
func WithObserver(o observe.Observer) Option {
	return func(e *Engine) {
		e.observers = append(e.observers, o)
	}
}

// copyAnnotations builds the event-owned annotation map: observers must
// never be handed engine-owned state a mutating callback could corrupt.
func copyAnnotations(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
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

func (e *Engine) emitRunScheduled(ev observe.RunEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunScheduled, ev)
	}
}

func (e *Engine) emitAttemptDone(ev observe.AttemptEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].AttemptDone, ev)
	}
}

func (e *Engine) emitRunUnwinding(ev observe.RunFailureEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunUnwinding, ev)
	}
}

func (e *Engine) emitRunTerminal(rec *driver.RunRecord) {
	if len(e.observers) == 0 {
		return
	}
	ev := observe.RunTerminalEvent{
		PipelineID:  rec.PipelineID,
		ResourceID:  rec.ResourceID,
		RunID:       rec.RunID,
		Outcome:     *rec.Outcome,
		Duration:    rec.UpdatedAt.Sub(rec.CreatedAt),
		Annotations: copyAnnotations(rec.Annotations),
	}
	if rec.RootFailure != nil {
		ev.Kind = rec.RootFailure.Kind
		ev.Reason = rec.RootFailure.Reason
	}
	for i := range e.observers {
		emit(e, e.observers[i].RunTerminal, ev)
	}
}

func (e *Engine) emitRunInvalid(ev observe.RunFailureEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].RunInvalid, ev)
	}
}

func (e *Engine) emitWaiterWoken(ev observe.WakeEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].WaiterWoken, ev)
	}
}

func (e *Engine) emitClassWait(ev observe.ClassWaitEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].ClassWait, ev)
	}
}

func (e *Engine) emitRunsReaped(count int) {
	for i := range e.observers {
		emit(e, e.observers[i].RunsReaped, count)
	}
}

func (e *Engine) emitStoreOp(ev observe.StoreOpEvent) {
	for i := range e.observers {
		emit(e, e.observers[i].StoreOp, ev)
	}
}

// hasStoreObserver reports whether any installed observer subscribes to
// StoreOp, deciding whether New wraps the driver.Store.
func (e *Engine) hasStoreObserver() bool {
	for i := range e.observers {
		if e.observers[i].StoreOp != nil {
			return true
		}
	}
	return false
}

// observedStore decorates the engine's driver.Store with StoreOp events. Every
// driver.Store method is implemented explicitly, no embedding: a method added
// to the interface later breaks this build instead of escaping
// observation.
type observedStore struct {
	inner  driver.Store
	engine *Engine
}

func (s *observedStore) op(name string, write bool, start time.Time, err error) {
	s.engine.emitStoreOp(observe.StoreOpEvent{Op: name, Write: write, Duration: s.engine.clock.Now().Sub(start), Err: err})
}

func (s *observedStore) CreateRun(ctx context.Context, rec *driver.RunRecord) (*driver.RunRecord, bool, error) {
	start := s.engine.clock.Now()
	existing, created, err := s.inner.CreateRun(ctx, rec)
	s.op("CreateRun", true, start, err)
	return existing, created, err
}

func (s *observedStore) GetRun(ctx context.Context, id durable.RunID) (*driver.RunRecord, error) {
	start := s.engine.clock.Now()
	rec, err := s.inner.GetRun(ctx, id)
	s.op("GetRun", false, start, err)
	return rec, err
}

func (s *observedStore) ApplyTransition(ctx context.Context, id durable.RunID, t driver.Transition) error {
	start := s.engine.clock.Now()
	err := s.inner.ApplyTransition(ctx, id, t)
	s.op("ApplyTransition", true, start, err)
	return err
}

func (s *observedStore) RequestCancel(ctx context.Context, id durable.RunID, req driver.CancelRequest) (bool, error) {
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

func (s *observedStore) ListNonterminal(ctx context.Context) ([]*driver.RunRecord, error) {
	start := s.engine.clock.Now()
	recs, err := s.inner.ListNonterminal(ctx)
	s.op("ListNonterminal", false, start, err)
	return recs, err
}

func (s *observedStore) ListRuns(ctx context.Context, pipeline durable.PipelineID, resource durable.ResourceID) ([]*driver.RunRecord, error) {
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

// Stats returns a point-in-time snapshot of engine occupancy. It is safe
// for concurrent use, including from observe.Observer callbacks.
func (e *Engine) Stats() observe.EngineStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := observe.EngineStats{
		AwaitingRuns: e.joins.Len(),
		InvalidRuns:  len(e.invalid),
	}
	if e.disp != nil {
		st.ActiveRuns = e.disp.Active()
		st.DelayedRuns = e.disp.Delayed()
	}
	if usage := e.pool.Snapshot(); len(usage) > 0 {
		st.Classes = make(map[string]observe.ClassStats, len(usage))
		for name, u := range usage {
			st.Classes[name] = observe.ClassStats{Capacity: u.Capacity, InUse: u.InUse, Waiting: u.Waiting}
			st.ThrottledRuns += u.Waiting
		}
	}
	return st
}
