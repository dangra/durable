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
	recorder, origin, res, err := run(t.Context())
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
	attempts, childSpans := 0, 0
	attemptTraces := map[string]bool{} // trace IDs owned by attempt spans
	for _, sp := range recorder.Ended() {
		if sp.Name() == "POST /orders" {
			continue
		}
		// The handler-internal span demonstrates relation (4): it must
		// nest under its attempt — same trace, valid parent — and never
		// under the origin.
		if sp.Name() == "charge card" {
			childSpans++
			if !sp.Parent().IsValid() || sp.SpanContext().TraceID() == origin.TraceID() {
				t.Fatalf("handler span not nested under its attempt: parent=%v trace=%v",
					sp.Parent(), sp.SpanContext().TraceID())
			}
			attemptTraces[sp.SpanContext().TraceID().String()] = true
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
	// Two payment attempts, each with a nested gateway-call span.
	if childSpans != 2 {
		t.Fatalf("handler-internal spans = %d, want 2", childSpans)
	}
	for tid := range attemptTraces {
		found := false
		for _, sp := range recorder.Ended() {
			if sp.SpanContext().TraceID().String() == tid && sp.Parent().IsValid() == false && sp.Name() != "POST /orders" {
				found = true
			}
		}
		if !found {
			t.Fatalf("handler span trace %s has no attempt root", tid)
		}
	}
}
