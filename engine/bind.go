package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/dangra/durable"
	"github.com/dangra/durable/internal/ledger"
	"github.com/dangra/durable/pipelinedef"
	"google.golang.org/protobuf/proto"
)

// boundStep is a step as the engine invokes it: its description and its
// operations composed once at Bind — the engine's middleware around the
// pipeline's around the adapter.
type boundStep struct {
	*pipelinedef.Step
	forward durable.Handler
	unwind  durable.Handler // nil unless Step.Unwind
}

// boundDef is a validated definition as the engine executes it: the
// config, its steps by id with their composed operations, and the ledger
// topology derived from them.
type boundDef struct {
	cfg   pipelinedef.Config
	steps map[durable.StepID]*boundStep
	topo  ledger.Topology

	// excludes is the pipeline's exclusion set as this deployment sees
	// it — every bound pipeline sharing at least one of its Mutexes,
	// itself included — resolved at Start. It is passed to the store at
	// each admission and never persisted, so a later deployment's view
	// applies to new admissions at once and Runs in flight are never
	// touched.
	excludes []durable.PipelineID
}

func (d *boundDef) ID() durable.PipelineID { return d.cfg.ID }

func (d *boundDef) step(id durable.StepID) *boundStep { return d.steps[id] }

// Bind validates def and registers it with the Engine, returning the
// Pipeline handle Runs are scheduled through. It is allowed only before
// Start (ErrStarted afterwards). Bind is the single validator of a
// definition: an empty or malformed identifier, a pipeline with no steps
// or a duplicated step, a step with no Run adapter, an unwind
// declaration that disagrees with its adapter, or a nil middleware entry
// is reported here as an error — for generated code that indicates a
// code-generation bug. Bind composes the pipeline's operations: the
// Engine's middleware around the pipeline's own around each adapter,
// once, here.
func (e *Engine) Bind(def *pipelinedef.Definition) (*Pipeline, error) {
	if def == nil {
		return nil, errors.New("durable: Bind of a nil definition")
	}
	bd, err := bindDefinition(def.Config(), e.middleware)
	if err != nil {
		return nil, err
	}
	if err := e.register(bd); err != nil {
		return nil, err
	}
	return &Pipeline{engine: e, def: bd}, nil
}

func bindDefinition(cfg pipelinedef.Config, engineMW []durable.Middleware) (*boundDef, error) {
	if cfg.ID == "" {
		return nil, errors.New("durable: definition has empty PipelineID")
	}
	if invalidID(string(cfg.ID)) {
		return nil, fmt.Errorf("durable: pipeline id %q must be NUL-free valid UTF-8", cfg.ID)
	}
	for _, m := range cfg.Mutexes {
		if m == "" || invalidID(m) {
			return nil, fmt.Errorf("durable: pipeline %q: mutex %q must be a non-empty NUL-free valid UTF-8 name", cfg.ID, m)
		}
	}
	for i, mw := range cfg.Middleware {
		if mw == nil {
			return nil, fmt.Errorf("durable: pipeline %q: middleware %d is nil", cfg.ID, i)
		}
	}
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("durable: pipeline %q has no steps", cfg.ID)
	}
	d := &boundDef{
		cfg:   cfg,
		steps: make(map[durable.StepID]*boundStep, len(cfg.Steps)),
	}
	for i := range cfg.Steps {
		sc := &cfg.Steps[i]
		if sc.ID == "" {
			return nil, fmt.Errorf("durable: pipeline %q has a step with empty StepID", cfg.ID)
		}
		if invalidID(string(sc.ID)) {
			return nil, fmt.Errorf("durable: pipeline %q: step %q: identifiers must be NUL-free valid UTF-8", cfg.ID, sc.ID)
		}
		if _, dup := d.steps[sc.ID]; dup {
			return nil, fmt.Errorf("durable: pipeline %q declares step %q twice", cfg.ID, sc.ID)
		}
		if sc.Run == nil {
			return nil, fmt.Errorf("durable: pipeline %q: step %q has no Run adapter", cfg.ID, sc.ID)
		}
		if sc.Unwind != (sc.UnwindFunc != nil) {
			return nil, fmt.Errorf("durable: pipeline %q: step %q unwind declaration and adapter disagree", cfg.ID, sc.ID)
		}
		bs := &boundStep{Step: sc, forward: wrap(durable.Handler(sc.Run), engineMW, cfg.Middleware)}
		if sc.Unwind {
			uf := sc.UnwindFunc
			bs.unwind = wrap(func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, uf(ctx, inv)
			}, engineMW, cfg.Middleware)
		}
		d.steps[sc.ID] = bs
		d.topo = append(d.topo, ledger.Step{
			ID:      string(sc.ID),
			Unwind:  sc.Unwind,
			Retired: sc.Retired,
		})
	}
	return d, nil
}
