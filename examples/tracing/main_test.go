package main

import (
	"testing"

	"github.com/dangra/durable"
)

// TestSpansLinkToOrigin runs the demo and checks the shape the example
// exists to demonstrate: one span per attempt across retries and both
// phases, each linked to the scheduling side's trace context.
func TestSpansLinkToOrigin(t *testing.T) {
	c, origin, res, err := run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != durable.OutcomeFailure {
		t.Fatalf("outcome = %v, want the scripted failure", res.Outcome)
	}
	// reserve attempt 1 (retry), reserve attempt 2, create attempt 1
	// (permanent), reserve unwind attempt 1.
	if len(c.spans) != 4 {
		t.Fatalf("spans = %d, want 4:\n%+v", len(c.spans), c.spans)
	}
	names := map[string]bool{}
	for _, sp := range c.spans {
		if sp.LinkedTo != origin {
			t.Fatalf("span %q linked to %+v, want the origin %+v", sp.Name, sp.LinkedTo, origin)
		}
		if sp.TraceID == origin.traceID {
			t.Fatalf("span %q parented under the origin trace; the shape is links, not a long-lived parent", sp.Name)
		}
		names[sp.Name] = true
	}
	for _, want := range []string{
		"reserve/v1/forward attempt=1",
		"reserve/v1/forward attempt=2",
		"create/v1/forward attempt=1",
		"reserve/v1/unwind attempt=1",
	} {
		if !names[want] {
			t.Fatalf("missing span %q in %v", want, names)
		}
	}
}
