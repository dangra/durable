package durable

import (
	"context"
	"fmt"
	"time"

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
	for _, o := range opts {
		o(&so)
	}

	now := e.clock.Now()
	rec := &RunRecord{
		RunID:      newRunID(now),
		PipelineID: p.def.ID(),
		ResourceID: resource,
		Input:      inputBytes,
		Phase:      PhaseForward,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if so.start != nil {
		rec.NextAttemptAt = so.start(now)
	}
	existing, created, err := e.store.CreateRun(ctx, rec)
	if err != nil {
		return Run{}, false, err
	}
	if created {
		e.dispatch(rec.RunID, 0)
		return Run{id: rec.RunID, engine: e}, true, nil
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
	return Run{}, false, &ScheduleConflictError{RunID: existing.RunID}
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
