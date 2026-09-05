// Package pipelinedef is the type-erased pipeline description generated
// code builds and the engine binds. Application code does not normally
// import it: protoc-gen-durable emits a typed constructor per pipeline
// that assembles a Definition from the application's step handlers, and
// the typed Definition's Bind hands it to an engine. Hand-rolled
// pipelines — tests, mostly — build the same Definition directly.
//
// A Definition is inert: constructing one validates nothing. Engine.Bind
// is the single validator and reports malformed definitions as errors.
package pipelinedef

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
)

// Step describes one Step of a pipeline.
type Step struct {
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

// Config is the type-erased pipeline description.
type Config struct {
	ID    durable.PipelineID
	Steps []Step

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

// Definition is an unbound pipeline definition: identity, ordered Step
// topology, retirement flags, unwind capabilities, handlers, and Reducer.
// It is a value to hand to Engine.Bind; it is not validated until then.
type Definition struct {
	cfg Config
}

// New wraps cfg. It copies the Steps slice so later mutation of cfg does
// not reach the Definition, and applies the pipeline-level concurrency
// class to steps that declare none.
func New(cfg Config) *Definition {
	cfg.Steps = append([]Step(nil), cfg.Steps...)
	for i := range cfg.Steps {
		if cfg.Steps[i].ConcurrencyClass == "" {
			cfg.Steps[i].ConcurrencyClass = cfg.ConcurrencyClass
		}
	}
	return &Definition{cfg: cfg}
}

// ID returns the PipelineID.
func (d *Definition) ID() durable.PipelineID { return d.cfg.ID }

// Config returns the description the Definition was built from, with the
// concurrency-class default applied. The engine reads it at Bind; the
// Steps slice is shared and must not be modified.
func (d *Definition) Config() Config { return d.cfg }

// StepRef constructs the reference to a stateless Step. Generated code
// exports one per stateless Step.
func StepRef(id durable.StepID) durable.StepRef { return durable.StepRef{Step: id} }

// StateStepRef constructs the typed reference to a state-producing Step.
// Generated code exports one per state-producing Step.
func StateStepRef[T proto.Message](id durable.StepID, new func() T) durable.StateStepRef[T] {
	return durable.StateStepRef[T]{Step: id, New: new}
}
