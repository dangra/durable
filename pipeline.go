package durable

import (
	"context"
	"fmt"
	"github.com/dangra/durable/observe"
	"github.com/dangra/durable/storedriver"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
)

// ScheduleOption configures acceptance of a Run. Options never contribute
// to duplicate-scheduling identity: Input equivalence alone decides whether
// an active Run matches.
type ScheduleOption func(*scheduleOptions)

type scheduleOptions struct {
	// start computes first execution eligibility from the acceptance time;
	// nil means immediately eligible.
	start func(now time.Time) time.Time
	// annotations accumulate across options; later keys win.
	annotations map[string]string
}

// WithAnnotations attaches caller-supplied propagation metadata to the
// Run at acceptance: a W3C traceparent, tenant tags, request ids. The
// map is copied; repeated options merge with later keys winning.
// Annotations are immutable for the life of the Run, readable by
// handlers through Invocation.Annotations and by callers through
// Run.Annotations, and are never part of duplicate-scheduling identity —
// on a dedup hit the active Run keeps its own annotations. Keys and
// values must be valid UTF-8 (the durable representation stores them as
// protobuf strings); Schedule rejects violations.
func WithAnnotations(annotations map[string]string) ScheduleOption {
	return func(o *scheduleOptions) {
		if o.annotations == nil {
			o.annotations = make(map[string]string, len(annotations))
		}
		for k, v := range annotations {
			o.annotations[k] = v
		}
	}
}

// StartAt delays the Run's first operation until t. The Run is created and
// occupies its resource slot immediately; the delay survives restart. If
// multiple start options are given, the last wins.
func StartAt(t time.Time) ScheduleOption {
	return func(o *scheduleOptions) {
		o.start = func(time.Time) time.Time { return t }
	}
}

// StartAfter delays the Run's first operation until d after acceptance,
// measured by the engine clock. Sugar for StartAt(now.Add(d)).
func StartAfter(d time.Duration) ScheduleOption {
	return func(o *scheduleOptions) {
		o.start = func(now time.Time) time.Time { return now.Add(d) }
	}
}

// Pipeline is a definition bound to an Engine. Generated typed pipeline
// handles wrap it.
type Pipeline struct {
	engine *Engine
	def    *Definition
}

// ID returns the PipelineID.
func (p *Pipeline) ID() PipelineID { return p.def.ID() }

// Schedule creates a Run for (pipeline, resource) or returns the active
// one.
//
// If no nonterminal Run occupies the slot, a Run is created and
// created=true. If one exists with equivalent Input (exact proto.Equal,
// including unknown fields), it is returned with created=false. Different
// Input yields a *ScheduleConflictError.
//
// For an Input-declaring pipeline, input must be non-nil; for an Input-less
// pipeline it must be nil (generated Schedule omits the argument). ctx
// governs only acceptance of the scheduling request: once accepted, Run
// execution belongs to the Engine.
func (p *Pipeline) Schedule(ctx context.Context, resource ResourceID, input proto.Message, opts ...ScheduleOption) (Run, bool, error) {
	e := p.engine
	if !e.isStarted() {
		return Run{}, false, ErrEngineNotStarted
	}
	if invalidID(string(resource)) {
		return Run{}, false, fmt.Errorf("durable: resource id %q must be NUL-free valid UTF-8", resource)
	}
	var inputBytes []byte
	switch {
	case p.def.cfg.NewInput != nil:
		if input == nil || !input.ProtoReflect().IsValid() {
			return Run{}, false, fmt.Errorf("durable: pipeline %q declares input; nil input is invalid", p.def.ID())
		}
		b, err := proto.Marshal(input)
		if err != nil {
			return Run{}, false, fmt.Errorf("durable: cannot serialize input: %w", err)
		}
		inputBytes = b
	case input != nil:
		return Run{}, false, fmt.Errorf("durable: pipeline %q declares no input", p.def.ID())
	}

	var so scheduleOptions
	// Engine-wide annotators seed the annotations from the caller's ctx
	// first, so the call's own options win on key conflicts.
	for _, a := range e.annotators {
		if m := a(ctx); len(m) > 0 {
			WithAnnotations(m)(&so)
		}
	}
	for _, o := range opts {
		o(&so)
	}
	for k, v := range so.annotations {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return Run{}, false, fmt.Errorf("durable: annotation %q must have valid UTF-8 key and value", k)
		}
	}

	now := e.clock.Now()
	rec := &storedriver.RunRecord{
		RunID:       newRunID(now),
		PipelineID:  p.def.ID(),
		ResourceID:  resource,
		Group:       p.def.slotGroup(),
		Annotations: so.annotations,
		Input:       inputBytes,
		Phase:       PhaseForward,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if so.start != nil {
		rec.NextAttemptAt = so.start(now)
	}
	existing, created, err := e.store.CreateRun(ctx, rec)
	if err != nil {
		return Run{}, false, err
	}
	if created {
		if e.debugLog() {
			args := []any{
				"pipeline", string(rec.PipelineID), "resource", string(resource),
				"run", string(rec.RunID)}
			if !rec.NextAttemptAt.IsZero() {
				args = append(args, "start_at", rec.NextAttemptAt)
			}
			e.logger.Debug("durable: run scheduled", args...)
		}
		e.emitRunScheduled(observe.RunEvent{
			PipelineID: rec.PipelineID, ResourceID: resource,
			RunID: rec.RunID, StartAt: rec.NextAttemptAt,
			Annotations: copyAnnotations(rec.Annotations)})
		e.disp.Dispatch(rec.RunID, 0)
		return Run{id: rec.RunID, engine: e}, true, nil
	}

	// A slot occupied by another pipeline in the exclusion group is always
	// a conflict: input equivalence is only meaningful within one pipeline.
	if existing.PipelineID != p.def.ID() {
		return Run{}, false, &ScheduleConflictError{RunID: existing.RunID, PipelineID: existing.PipelineID}
	}
	if p.def.cfg.NewInput == nil {
		return Run{id: existing.RunID, engine: e}, false, nil
	}
	existingInput := p.def.cfg.NewInput()
	if err := proto.Unmarshal(existing.Input, existingInput); err != nil {
		return Run{}, false, fmt.Errorf("durable: cannot decode input of active run %s: %w", existing.RunID, err)
	}
	if proto.Equal(existingInput, input) {
		return Run{id: existing.RunID, engine: e}, false, nil
	}
	return Run{}, false, &ScheduleConflictError{RunID: existing.RunID, PipelineID: existing.PipelineID}
}

// Run returns a handle to an existing Run, verifying it belongs to this
// pipeline. A mismatch returns *PipelineMismatchError.
func (p *Pipeline) Run(ctx context.Context, id RunID) (Run, error) {
	rec, err := p.engine.store.GetRun(ctx, id)
	if err != nil {
		return Run{}, err
	}
	if rec.PipelineID != p.def.ID() {
		return Run{}, &PipelineMismatchError{RunID: id, Expected: p.def.ID(), Actual: rec.PipelineID}
	}
	return Run{id: id, engine: p.engine}, nil
}

// ActiveRun returns a handle to this pipeline's nonterminal Run for a
// resource, if one exists. It is a read-only observation for wait/inspect
// flows (callers that know the resource but cannot reproduce the exact
// Input); claiming the slot atomically remains Schedule's job.
func (p *Pipeline) ActiveRun(ctx context.Context, resource ResourceID) (Run, bool, error) {
	recs, err := p.engine.store.ListRuns(ctx, p.def.ID(), resource)
	if err != nil {
		return Run{}, false, err
	}
	for _, rec := range recs {
		if !rec.Terminal() {
			return Run{id: rec.RunID, engine: p.engine}, true, nil
		}
	}
	return Run{}, false, nil
}

// Active returns handles for this pipeline's nonterminal Runs.
func (p *Pipeline) Active(ctx context.Context) ([]Run, error) {
	recs, err := p.engine.store.ListNonterminal(ctx)
	if err != nil {
		return nil, err
	}
	var runs []Run
	for _, rec := range recs {
		if rec.PipelineID == p.def.ID() {
			runs = append(runs, Run{id: rec.RunID, engine: p.engine})
		}
	}
	return runs, nil
}

// Runs returns handles for all Runs of this pipeline against a resource,
// oldest first.
func (p *Pipeline) Runs(ctx context.Context, resource ResourceID) ([]Run, error) {
	recs, err := p.engine.store.ListRuns(ctx, p.def.ID(), resource)
	if err != nil {
		return nil, err
	}
	runs := make([]Run, 0, len(recs))
	for _, rec := range recs {
		runs = append(runs, Run{id: rec.RunID, engine: p.engine})
	}
	return runs, nil
}
