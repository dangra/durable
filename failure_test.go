// Failure attribution: kind and reason through the error chain, on the
// forward and unwind paths, and error-text sanitizing.
package durable_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
	"google.golang.org/protobuf/proto"
)

// classifiedError carries its own attribution, the way domain error types
// implement FailureKinder/FailureReasoner once so resolution sites stay
// plain Fail(err).
type classifiedError struct{ msg string }

func (e *classifiedError) Error() string                    { return e.msg }
func (e *classifiedError) FailureKind() durable.FailureKind { return durable.FailureKindUser }
func (e *classifiedError) FailureReason() string            { return "invalid-image" }

func failingRun(t *testing.T, id durable.PipelineID, fail error) durable.Result {
	t.Helper()
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: id,
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				return fail
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", res, err)
	}
	return res
}

func TestFailureAttribution(t *testing.T) {
	t.Run("defaults to system with no reason", func(t *testing.T) {
		res := failingRun(t, "attr-default", durable.Fail(errors.New("boom")))
		if res.RootFailure.Kind != durable.FailureKindSystem || res.RootFailure.Reason != "" {
			t.Fatalf("RootFailure = %+v, want system kind, empty reason", res.RootFailure)
		}
	})
	t.Run("explicit options", func(t *testing.T) {
		res := failingRun(t, "attr-opts", durable.Fail(errors.New("bad region"),
			durable.WithUserKind(), durable.WithReason("invalid-input")))
		if res.RootFailure.Kind != durable.FailureKindUser || res.RootFailure.Reason != "invalid-input" {
			t.Fatalf("RootFailure = %+v, want user/invalid-input", res.RootFailure)
		}
	})
	t.Run("extracted from error chain", func(t *testing.T) {
		wrapped := fmt.Errorf("preparing image: %w", &classifiedError{msg: "no manifest"})
		res := failingRun(t, "attr-chain", durable.Fail(wrapped))
		if res.RootFailure.Kind != durable.FailureKindUser || res.RootFailure.Reason != "invalid-image" {
			t.Fatalf("RootFailure = %+v, want user/invalid-image from chain", res.RootFailure)
		}
	})
	t.Run("options override the chain", func(t *testing.T) {
		res := failingRun(t, "attr-precedence", durable.Fail(&classifiedError{msg: "x"},
			durable.WithReason("overridden")))
		if res.RootFailure.Reason != "overridden" || res.RootFailure.Kind != durable.FailureKindUser {
			t.Fatalf("RootFailure = %+v, want reason overridden, kind still from chain", res.RootFailure)
		}
	})
}

func TestUnwindFailureAttribution(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "attr-unwind",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					return durable.Fail(errors.New("release rejected"), durable.WithReason("release-rejected"))
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("nope"))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if len(res.UnwindFailures) != 1 || res.UnwindFailures[0].Reason != "release-rejected" ||
		res.UnwindFailures[0].Kind != durable.FailureKindSystem {
		t.Fatalf("UnwindFailures = %+v, want system/release-rejected", res.UnwindFailures)
	}
}

// TestInvalidUTF8ErrorsDoNotWedge pins the sanitization contract found
// by the storage fuzzer: handler errors, failure reasons, and cancel
// causes may contain invalid UTF-8, which protobuf string fields reject
// — recorded raw, the durable transition could never marshal and the
// Run would wedge in a store-retry loop. The engine must sanitize and
// carry on, through both the retry and the permanent-failure paths.
func TestInvalidUTF8ErrorsDoNotWedge(t *testing.T) {
	raw := "raw \xff\xfe bytes"
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "utf8",
		Steps: []durable.StepConfig{
			stateless("flaky/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New(raw) // ordinary error -> LastError
				}
				return nil
			}),
			stateless("explode/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New(raw), durable.WithReason(raw))
			}),
		},
	})
	// The bbolt store is the one that actually marshals to proto.
	store, err := bboltstore.Open(filepath.Join(t.TempDir(), "utf8.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	_, pipes := startEngine(t, store, def)

	run, _, err := pipes[0].Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v (a wedged marshal would hang, not return)", err)
	}
	if res.Outcome != durable.OutcomeFailure || res.RootFailure == nil {
		t.Fatalf("result = %+v", res)
	}
	if !utf8.ValidString(res.RootFailure.Message) || !utf8.ValidString(res.RootFailure.Reason) {
		t.Fatalf("unsanitized failure text: %+v", res.RootFailure)
	}

	// Invalid UTF-8 identifiers are rejected upfront instead.
	if _, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID("res\xff"), nil); err == nil {
		t.Fatal("Schedule accepted an invalid-UTF-8 resource id")
	}
	if _, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID("res\x00x"), nil); err == nil {
		t.Fatal("Schedule accepted a NUL resource id")
	}
}

type reasonedErr struct{ reason string }

func (e reasonedErr) Error() string         { return "reasoned: " + e.reason }
func (e reasonedErr) FailureReason() string { return e.reason }

func TestFailureInfo(t *testing.T) {
	kind, reason, ok := durable.FailureInfo(
		durable.Fail(errors.New("boom"), durable.WithUserKind(), durable.WithReason("invalid-input")))
	if !ok || kind != durable.FailureKindUser || reason != "invalid-input" {
		t.Fatalf("FailureInfo = %v, %q, %v; want user/invalid-input/true", kind, reason, ok)
	}

	// Attribution resolved from the error chain, and permanence
	// detected through wrapping — what a middleware actually sees.
	kind, reason, ok = durable.FailureInfo(
		fmt.Errorf("wrapped: %w", durable.Fail(reasonedErr{reason: "quota"})))
	if !ok || kind != durable.FailureKindSystem || reason != "quota" {
		t.Fatalf("FailureInfo(wrapped) = %v, %q, %v; want system/quota/true", kind, reason, ok)
	}

	for _, err := range []error{nil, errors.New("transient"), durable.AwaitRun("01ARZ3NDEKTSV4RRFFQ69G5FAV")} {
		if _, _, ok := durable.FailureInfo(err); ok {
			t.Fatalf("FailureInfo(%v) claims permanence", err)
		}
	}
}
