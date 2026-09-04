package main

import (
	"testing"

	"github.com/dangra/durable"
)

// TestAttemptSpansLinkToOrigin runs the demo and asserts the shape:
// one span per attempt across retries and both phases, every one
// carrying a span LINK to the scheduling request's trace and none
// parented under it.
func TestAttemptSpansLinkToOrigin(t *testing.T) {
	recorder, origin, res, err := run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != durable.OutcomeFailure || res.RootFailure == nil || res.RootFailure.Reason != "invalid-address" {
		t.Fatalf("result = %+v, want the scripted shipping failure", res.Result)
	}

	// reserve, charge attempt 1 (retry) + 2, ship (permanent), charge
	// unwind, reserve unwind.
	want := map[string]bool{
		"reserve-stock/v1 forward attempt 1":  true,
		"charge-payment/v1 forward attempt 1": true,
		"charge-payment/v1 forward attempt 2": true,
		"ship/v1 forward attempt 1":           true,
		"charge-payment/v1 unwind attempt 1":  true,
		"reserve-stock/v1 unwind attempt 1":   true,
	}
	attempts := 0
	for _, sp := range recorder.Ended() {
		if sp.Name() == "POST /orders" {
			continue
		}
		attempts++
		if !want[sp.Name()] {
			t.Fatalf("unexpected span %q", sp.Name())
		}
		linked := false
		for _, l := range sp.Links() {
			if l.SpanContext.TraceID() == origin.TraceID() && l.SpanContext.SpanID() == origin.SpanID() {
				linked = true
			}
		}
		if !linked {
			t.Fatalf("span %q not linked to the origin span", sp.Name())
		}
		if sp.SpanContext().TraceID() == origin.TraceID() {
			t.Fatalf("span %q lives in the origin trace; the shape is links, not a long-lived parent", sp.Name())
		}
		if sp.Parent().IsValid() && sp.Parent().TraceID() == origin.TraceID() {
			t.Fatalf("span %q parented under the origin trace", sp.Name())
		}
	}
	if attempts != len(want) {
		t.Fatalf("attempt spans = %d, want %d", attempts, len(want))
	}
}
