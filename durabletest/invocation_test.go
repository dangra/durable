package durabletest_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/pipelinedef"
)

var (
	nameStep  = pipelinedef.StateStepRef("name/v1", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	countStep = pipelinedef.StateStepRef("count/v1", func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} })
)

func TestFakeInvocationDefaults(t *testing.T) {
	inv := durabletest.NewInvocation(durabletest.InvocationConfig{})
	if inv.Phase() != durable.PhaseForward || inv.Attempt() != 1 {
		t.Fatalf("defaults: phase=%v attempt=%d", inv.Phase(), inv.Attempt())
	}
	if inv.InputMessage() != nil || inv.Annotations() != nil || inv.CancelRequested() {
		t.Fatal("zero config must present an input-less, unannotated, uncanceled attempt")
	}
	if _, ok := inv.Awaited(); ok {
		t.Fatal("zero config must be a first execution")
	}
	if _, ok := inv.AwaitedRunID(); ok {
		t.Fatal("zero config must be a first execution")
	}
	inv.Logger().Info("discarded") // must not panic with a nil logger
	if inv.Violation() != nil {
		t.Fatal("no violation expected")
	}
}

func TestFakeInvocationStateLookup(t *testing.T) {
	inv := durabletest.NewInvocation(durabletest.InvocationConfig{
		State: map[durable.StepID]proto.Message{
			nameStep.ID(): wrapperspb.String("alice"),
		},
	})
	got, ok := durable.LookupState(inv, nameStep)
	if !ok || got.GetValue() != "alice" {
		t.Fatalf("LookupState = %v, %v", got, ok)
	}
	got.Value = "mutated"
	if again, _ := durable.LookupState(inv, nameStep); again.GetValue() != "alice" {
		t.Fatal("LookupState must return a caller-owned copy")
	}
	if _, ok := durable.LookupState(inv, countStep); ok {
		t.Fatal("unset step must report no state")
	}
	if inv.Violation() != nil {
		t.Fatalf("unexpected violation: %v", inv.Violation())
	}

	// The fake satisfies ReduceView with the same data.
	var view durable.ReduceView = inv
	if v, ok := durable.LookupState(view, nameStep); !ok || v.GetValue() != "alice" {
		t.Fatal("ReduceView lookup must see the same state")
	}
}

func TestFakeInvocationCopiesAndMemory(t *testing.T) {
	wake := &durable.Wake{Targets: []durable.RunID{"child"}, Done: []durable.RunID{"child"}}
	inv := durabletest.NewInvocation(durabletest.InvocationConfig{
		PipelineID:  "p",
		ResourceID:  "r",
		RunID:       "run",
		StepID:      "s/v1",
		Attempt:     3,
		Phase:       durable.PhaseUnwind,
		Input:       wrapperspb.String("in"),
		Annotations: map[string]string{"traceparent": "00-abc"},
		Awaited:     wake,
	})
	if inv.PipelineID() != "p" || inv.ResourceID() != "r" || inv.RunID() != "run" || inv.StepID() != "s/v1" || inv.Attempt() != 3 || inv.Phase() != durable.PhaseUnwind {
		t.Fatal("identity accessors must echo the config")
	}
	in := inv.InputMessage().(*wrapperspb.StringValue)
	in.Value = "mutated"
	if inv.InputMessage().(*wrapperspb.StringValue).GetValue() != "in" {
		t.Fatal("InputMessage must return a copy")
	}
	ann := inv.Annotations()
	ann["traceparent"] = "mutated"
	if inv.Annotations()["traceparent"] != "00-abc" {
		t.Fatal("Annotations must return a copy")
	}
	w, ok := inv.Awaited()
	if !ok || len(w.Done) != 1 {
		t.Fatalf("Awaited = %+v, %v", w, ok)
	}
	w.Done[0] = "mutated"
	if wake.Done[0] != "child" {
		t.Fatal("Awaited must return a copy")
	}
	if id, ok := inv.AwaitedRunID(); !ok || id != "child" {
		t.Fatalf("AwaitedRunID = %q, %v", id, ok)
	}
	multi := durabletest.NewInvocation(durabletest.InvocationConfig{Awaited: &durable.Wake{Targets: []durable.RunID{"a", "b"}}})
	if _, ok := multi.AwaitedRunID(); ok {
		t.Fatal("AwaitedRunID must reject multi-target parks")
	}
}

func TestFakeInvocationOwnsItsConfig(t *testing.T) {
	cfg := durabletest.InvocationConfig{
		Input:       wrapperspb.String("in"),
		State:       map[durable.StepID]proto.Message{nameStep.ID(): wrapperspb.String("alice")},
		Annotations: map[string]string{"k": "v"},
		Awaited:     &durable.Wake{Targets: []durable.RunID{"child"}},
	}
	inv := durabletest.NewInvocation(cfg)
	cfg.Input.(*wrapperspb.StringValue).Value = "mutated"
	cfg.State[nameStep.ID()].(*wrapperspb.StringValue).Value = "mutated"
	cfg.Annotations["k"] = "mutated"
	cfg.Awaited.Targets[0] = "mutated"

	if inv.InputMessage().(*wrapperspb.StringValue).GetValue() != "in" {
		t.Fatal("Input must be copied at construction")
	}
	if v, _ := durable.LookupState(inv, nameStep); v.GetValue() != "alice" {
		t.Fatal("State must be captured at construction")
	}
	if inv.Annotations()["k"] != "v" {
		t.Fatal("Annotations must be copied at construction")
	}
	if id, _ := inv.AwaitedRunID(); id != "child" {
		t.Fatal("Awaited must be copied at construction")
	}
}

func TestFakeInvocationLoggerAndViolation(t *testing.T) {
	var buf bytes.Buffer
	inv := durabletest.NewInvocation(durabletest.InvocationConfig{
		RunID:  "run",
		StepID: "s/v1",
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	inv.Logger().Info("hello")
	if line := buf.String(); !strings.Contains(line, "run=run") || !strings.Contains(line, "step=s/v1") || !strings.Contains(line, "attempt=1") {
		t.Fatalf("logger must carry the canonical keys: %q", line)
	}

	first, second := errors.New("first"), errors.New("second")
	inv.ReportViolation(first)
	inv.ReportViolation(second)
	if inv.Violation() != first {
		t.Fatal("the first violation wins")
	}
}
