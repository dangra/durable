package engine_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

// Bind is the single validator of a definition: every malformed shape is
// an error at Bind, never a panic at construction.
func TestBindValidatesTheDefinition(t *testing.T) {
	run := func(context.Context, durable.Invocation) (proto.Message, error) { return nil, nil }
	unwind := func(context.Context, durable.Invocation, durable.Failure) error { return nil }
	ok := pipelinedef.Step{ID: "s/v1", Run: run}

	cases := []struct {
		name string
		cfg  pipelinedef.Config
		want string
	}{
		{"empty pipeline id", pipelinedef.Config{Steps: []pipelinedef.Step{ok}}, "empty PipelineID"},
		{"invalid pipeline id", pipelinedef.Config{ID: "p\x00", Steps: []pipelinedef.Step{ok}}, "NUL-free"},
		{"invalid group", pipelinedef.Config{ID: "p", ExclusionGroup: "\xff", Steps: []pipelinedef.Step{ok}}, "exclusion group"},
		{"no steps", pipelinedef.Config{ID: "p"}, "has no steps"},
		{"empty step id", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{{Run: run}}}, "empty StepID"},
		{"invalid step id", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{{ID: "s\x00", Run: run}}}, "NUL-free"},
		{"duplicate step", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{ok, ok}}, "twice"},
		{"no run adapter", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{{ID: "s/v1"}}}, "no Run adapter"},
		{"unwind without adapter", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{{ID: "s/v1", Run: run, Unwind: true}}}, "disagree"},
		{"adapter without unwind", pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{{ID: "s/v1", Run: run, UnwindFunc: unwind}}}, "disagree"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := engine.New(durabletest.NewMemStore(), engine.WithLogger(discardTestLogger()))
			_, err := e.Bind(pipelinedef.New(tc.cfg))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Bind error = %v; want it to mention %q", err, tc.want)
			}
		})
	}

	e := engine.New(durabletest.NewMemStore(), engine.WithLogger(discardTestLogger()))
	if _, err := e.Bind(nil); err == nil {
		t.Fatal("Bind(nil) must fail")
	}
	p, err := e.Bind(pipelinedef.New(pipelinedef.Config{ID: "p", Steps: []pipelinedef.Step{ok}}))
	if err != nil || p.ID() != "p" {
		t.Fatalf("well-formed definition: %v, %v", p, err)
	}
}

// New applies the pipeline-level concurrency class and detaches from the
// caller's Steps slice.
func TestDefinitionNormalizesAndCopies(t *testing.T) {
	steps := []pipelinedef.Step{
		{ID: "a/v1"},
		{ID: "b/v1", ConcurrencyClass: "own"},
	}
	def := pipelinedef.New(pipelinedef.Config{ID: "p", ConcurrencyClass: "default", Steps: steps})
	steps[0].ID = "mutated"
	got := def.Config().Steps
	if got[0].ID != "a/v1" {
		t.Fatal("New must copy the Steps slice")
	}
	if got[0].ConcurrencyClass != "default" || got[1].ConcurrencyClass != "own" {
		t.Fatalf("class defaulting: %+v", got)
	}
	if def.ID() != "p" {
		t.Fatalf("ID = %q", def.ID())
	}
}
