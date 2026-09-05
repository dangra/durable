package engine

import (
	"context"
	"fmt"
	"github.com/dangra/durable"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable/internal/ledger"
)

// StepConfig describes one Step of a pipeline definition. It is the
// type-erased adapter surface produced by generated code.
type StepConfig struct {
	ID      durable.StepID
	Unwind  bool
	Retired bool
	// HasState reports whether the Step declaration has protobuf fields,
	// i.e. successful forward execution commits Step State.
	HasState bool

	// ConcurrencyClass optionally names the token pool bounding this
	// step's executing operations; empty inherits the pipeline default.
	ConcurrencyClass string

	// Run invokes the application forward handler. For a state-producing
	// Step it returns the State to commit; for a stateless Step it returns
	// (nil, err).
	Run func(context.Context, durable.Invocation) (proto.Message, error)

	// UnwindFunc invokes the application unwind handler. It is set exactly
	// when Unwind is true.
	UnwindFunc func(context.Context, durable.Invocation, durable.Failure) error
}

// DefinitionConfig is the type-erased pipeline description produced by
// generated code.
type DefinitionConfig struct {
	ID    durable.PipelineID
	Steps []StepConfig

	// ExclusionGroup optionally names a mutual-exclusion group: pipelines
	// sharing a group allow at most one nonterminal Run per ResourceID
	// across the whole group. Empty means the pipeline excludes only with
	// itself.
	ExclusionGroup string

	// ConcurrencyClass optionally sets the default concurrency class for
	// all of this pipeline's steps; a step's own class overrides it.
	ConcurrencyClass string

	// NewInput constructs an empty Input message; nil for an Input-less
	// pipeline.
	NewInput func() proto.Message

	// Reduce invokes the application Reducer; nil for an Output-less
	// pipeline. It must be pure.
	Reduce func(durable.ReduceView) proto.Message
}

// Definition is a registered-but-unbound pipeline definition: identity,
// current ordered Step topology, retirement flags, unwind capabilities,
// active handlers, and Reducer.
type Definition struct {
	cfg   DefinitionConfig
	steps map[durable.StepID]*StepConfig
	topo  ledger.Topology
}

// NewDefinition validates cfg and constructs a Definition. It is intended
// for generated code and panics on malformed configuration, which indicates
// a code-generation bug rather than a runtime condition.
func NewDefinition(cfg DefinitionConfig) *Definition {
	if cfg.ID == "" {
		panic("durable: definition has empty PipelineID")
	}
	if invalidID(string(cfg.ID)) {
		panic(fmt.Sprintf("durable: pipeline id %q must be NUL-free valid UTF-8", cfg.ID))
	}
	if invalidID(cfg.ExclusionGroup) {
		panic(fmt.Sprintf("durable: pipeline %q: exclusion group %q must be NUL-free valid UTF-8", cfg.ID, cfg.ExclusionGroup))
	}
	if len(cfg.Steps) == 0 {
		panic(fmt.Sprintf("durable: pipeline %q has no steps", cfg.ID))
	}
	d := &Definition{
		cfg:   cfg,
		steps: make(map[durable.StepID]*StepConfig, len(cfg.Steps)),
	}
	for i := range cfg.Steps {
		sc := &cfg.Steps[i]
		if sc.ID == "" {
			panic(fmt.Sprintf("durable: pipeline %q has a step with empty StepID", cfg.ID))
		}
		if invalidID(string(sc.ID)) {
			panic(fmt.Sprintf("durable: pipeline %q: step %q: identifiers must be NUL-free valid UTF-8", cfg.ID, sc.ID))
		}
		if _, dup := d.steps[sc.ID]; dup {
			panic(fmt.Sprintf("durable: pipeline %q declares step %q twice", cfg.ID, sc.ID))
		}
		if sc.Run == nil {
			panic(fmt.Sprintf("durable: step %q has no Run adapter", sc.ID))
		}
		if sc.Unwind != (sc.UnwindFunc != nil) {
			panic(fmt.Sprintf("durable: step %q unwind declaration and adapter disagree", sc.ID))
		}
		if sc.ConcurrencyClass == "" {
			sc.ConcurrencyClass = cfg.ConcurrencyClass
		}
		d.steps[sc.ID] = sc
		d.topo = append(d.topo, ledger.Step{
			ID:      string(sc.ID),
			Unwind:  sc.Unwind,
			Retired: sc.Retired,
		})
	}
	return d
}

// ID returns the PipelineID.
func (d *Definition) ID() durable.PipelineID { return d.cfg.ID }

// Bind registers the definition with an Engine. It is allowed only before
// Engine.Start.
func (d *Definition) Bind(e *Engine) (*Pipeline, error) {
	if err := e.register(d); err != nil {
		return nil, err
	}
	return &Pipeline{engine: e, def: d}, nil
}

func (d *Definition) step(id durable.StepID) *StepConfig { return d.steps[id] }

// slotGroup returns the namespaced exclusion scope for this pipeline's
// Runs. The namespaces keep an explicit group name from accidentally
// colliding with another pipeline's default per-pipeline scope.
func (d *Definition) slotGroup() string {
	if d.cfg.ExclusionGroup != "" {
		return "group/" + d.cfg.ExclusionGroup
	}
	return "pipeline/" + string(d.cfg.ID)
}
