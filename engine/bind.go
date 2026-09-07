package engine

import (
	"errors"
	"fmt"

	"github.com/dangra/durable"
	"github.com/dangra/durable/internal/ledger"
	"github.com/dangra/durable/pipelinedef"
)

// boundDef is a validated definition as the engine executes it: the
// config, its steps by id, and the ledger topology derived from them.
type boundDef struct {
	cfg   pipelinedef.Config
	steps map[durable.StepID]*pipelinedef.Step
	topo  ledger.Topology

	// exclusive is the pipeline's exclusion group as this deployment sees
	// it — every bound pipeline sharing its ExclusionGroup, itself
	// included — resolved at Start. It is passed to the store at each
	// admission and never persisted, so a later deployment's view applies
	// to new admissions at once and Runs in flight are never touched.
	exclusive []durable.PipelineID
}

func (d *boundDef) ID() durable.PipelineID { return d.cfg.ID }

func (d *boundDef) step(id durable.StepID) *pipelinedef.Step { return d.steps[id] }

// Bind validates def and registers it with the Engine, returning the
// Pipeline handle Runs are scheduled through. It is allowed only before
// Start (ErrStarted afterwards). Bind is the single validator of a
// definition: an empty or malformed identifier, a pipeline with no steps
// or a duplicated step, a step with no Run adapter, or an unwind
// declaration that disagrees with its adapter is reported here as an
// error — for generated code that indicates a code-generation bug.
func (e *Engine) Bind(def *pipelinedef.Definition) (*Pipeline, error) {
	if def == nil {
		return nil, errors.New("durable: Bind of a nil definition")
	}
	bd, err := bindDefinition(def.Config())
	if err != nil {
		return nil, err
	}
	if err := e.register(bd); err != nil {
		return nil, err
	}
	return &Pipeline{engine: e, def: bd}, nil
}

func bindDefinition(cfg pipelinedef.Config) (*boundDef, error) {
	if cfg.ID == "" {
		return nil, errors.New("durable: definition has empty PipelineID")
	}
	if invalidID(string(cfg.ID)) {
		return nil, fmt.Errorf("durable: pipeline id %q must be NUL-free valid UTF-8", cfg.ID)
	}
	if invalidID(cfg.ExclusionGroup) {
		return nil, fmt.Errorf("durable: pipeline %q: exclusion group %q must be NUL-free valid UTF-8", cfg.ID, cfg.ExclusionGroup)
	}
	if len(cfg.Steps) == 0 {
		return nil, fmt.Errorf("durable: pipeline %q has no steps", cfg.ID)
	}
	d := &boundDef{
		cfg:   cfg,
		steps: make(map[durable.StepID]*pipelinedef.Step, len(cfg.Steps)),
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
		d.steps[sc.ID] = sc
		d.topo = append(d.topo, ledger.Step{
			ID:      string(sc.ID),
			Unwind:  sc.Unwind,
			Retired: sc.Retired,
		})
	}
	return d, nil
}
