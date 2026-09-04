package durable_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dangra/durable"
)

func TestAwaitTarget(t *testing.T) {
	const id = durable.RunID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if got, ok := durable.AwaitTarget(durable.AwaitRun(id)); !ok || got != id {
		t.Fatalf("AwaitTarget(AwaitRun(%q)) = %q, %v", id, got, ok)
	}
	// Wrapped resolutions still classify: middleware sees whatever the
	// handler stack returned.
	if got, ok := durable.AwaitTarget(fmt.Errorf("wrapped: %w", durable.AwaitRun(id))); !ok || got != id {
		t.Fatalf("AwaitTarget(wrapped) = %q, %v", got, ok)
	}
	for _, err := range []error{nil, errors.New("boom"), durable.Fail(errors.New("boom"))} {
		if got, ok := durable.AwaitTarget(err); ok || got != "" {
			t.Fatalf("AwaitTarget(%v) = %q, %v; want miss", err, got, ok)
		}
	}
}
