package durable_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dangra/durable"
)

type reasoned struct{ reason string }

func (r reasoned) Error() string         { return "reasoned: " + r.reason }
func (r reasoned) FailureReason() string { return r.reason }

// The engine reads park requests and permanent failures through the same
// exported classifiers middleware does; this pins their contract.
func TestAwaitClassifiers(t *testing.T) {
	err := durable.AwaitAll([]durable.RunID{"a", "b", "a"}, durable.WithAwaitTimeout(time.Minute))
	park, ok := durable.AwaitRequest(err)
	if !ok || park.Mode != durable.AwaitModeAll || len(park.Targets) != 2 || !park.Deadline.IsZero() {
		t.Fatalf("AwaitRequest = %+v, %v", park, ok)
	}
	if d := durable.AwaitTimeout(err); d != time.Minute {
		t.Fatalf("AwaitTimeout = %v", d)
	}
	wrapped := fmt.Errorf("middleware: %w", durable.AwaitRun("c"))
	if _, ok := durable.AwaitRequest(wrapped); !ok {
		t.Fatal("AwaitRequest must see through wrapping")
	}
	if d := durable.AwaitTimeout(wrapped); d != 0 {
		t.Fatalf("AwaitTimeout without WithAwaitTimeout = %v", d)
	}
	if _, ok := durable.AwaitRequest(errors.New("plain")); ok || durable.AwaitTimeout(errors.New("plain")) != 0 {
		t.Fatal("a plain error is not a park")
	}
}

func TestFailureClassifiers(t *testing.T) {
	cause := reasoned{reason: "quota"}
	err := fmt.Errorf("middleware: %w", durable.Fail(cause, durable.WithUserKind()))

	got, ok := durable.FailureCause(err)
	if !ok || got != error(cause) {
		t.Fatalf("FailureCause = %v, %v", got, ok)
	}
	kind, reason, ok := durable.FailureInfo(err)
	if !ok || kind != durable.FailureKindUser || reason != "quota" {
		t.Fatalf("FailureInfo = %v, %q, %v", kind, reason, ok)
	}
	if r := durable.FailureReason(err); r != "quota" {
		t.Fatalf("FailureReason through the chain = %q", r)
	}

	// WithReason overrides the chain for FailureInfo but FailureReason
	// still reports the chain's own reason.
	over := durable.Fail(cause, durable.WithReason("override"))
	if _, reason, _ := durable.FailureInfo(over); reason != "override" {
		t.Fatalf("FailureInfo reason = %q", reason)
	}
	if r := durable.FailureReason(over); r != "quota" {
		t.Fatalf("FailureReason = %q", r)
	}

	plain := reasoned{reason: "transient"}
	if _, ok := durable.FailureCause(plain); ok {
		t.Fatal("an ordinary error has no permanent cause")
	}
	if r := durable.FailureReason(plain); r != "transient" {
		t.Fatalf("FailureReason on an ordinary error = %q", r)
	}
	if durable.FailureReason(nil) != "" {
		t.Fatal("nil has no reason")
	}
}

func TestScheduleOptions(t *testing.T) {
	apply := func(opts ...durable.ScheduleOption) durable.ScheduleOptions {
		var so durable.ScheduleOptions
		for _, o := range opts {
			o(&so)
		}
		return so
	}
	at := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	if so := apply(durable.StartAfter(time.Hour), durable.StartAt(at)); !so.StartAt.Equal(at) || so.StartAfter != 0 {
		t.Fatalf("last start option must win: %+v", so)
	}
	if so := apply(durable.StartAt(at), durable.StartAfter(time.Hour)); !so.StartAt.IsZero() || so.StartAfter != time.Hour {
		t.Fatalf("last start option must win: %+v", so)
	}
	src := map[string]string{"a": "1", "b": "2"}
	so := apply(durable.WithAnnotations(src), durable.WithAnnotations(map[string]string{"b": "3"}))
	src["a"] = "mutated"
	if so.Annotations["a"] != "1" || so.Annotations["b"] != "3" {
		t.Fatalf("annotations must be copied and merged with later keys winning: %v", so.Annotations)
	}
}
