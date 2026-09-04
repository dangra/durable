package durable_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dangra/durable"
)

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
