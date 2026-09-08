// Failure attribution: kind and reason through the error chain, on the
// forward and unwind paths, and error-text sanitizing.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/bbolt"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// classifiedError carries its own attribution, the way domain error types
// implement FailureKinder/FailureReasoner once so resolution sites stay
// plain Fail(err).
type classifiedError struct{ msg string }

func (e *classifiedError) Error() string                    { return e.msg }
func (e *classifiedError) FailureKind() durable.FailureKind { return durable.FailureKindUser }
func (e *classifiedError) FailureReason() string            { return "invalid-image" }

func failingRun(t *testing.T, id durable.PipelineID, fail error) engine.Result {
	t.Helper()
	def := pipelinedef.New(pipelinedef.Config{
		ID: id,
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				return fail
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
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
		if res.Failure.Kind != durable.FailureKindSystem || res.Failure.Reason != "" {
			t.Fatalf("Failure = %+v, want system kind, empty reason", res.Failure)
		}
	})
	t.Run("explicit options", func(t *testing.T) {
		res := failingRun(t, "attr-opts", durable.Fail(errors.New("bad region"),
			durable.WithUserKind(), durable.WithReason("invalid-input")))
		if res.Failure.Kind != durable.FailureKindUser || res.Failure.Reason != "invalid-input" {
			t.Fatalf("Failure = %+v, want user/invalid-input", res.Failure)
		}
	})
	t.Run("extracted from error chain", func(t *testing.T) {
		wrapped := fmt.Errorf("preparing image: %w", &classifiedError{msg: "no manifest"})
		res := failingRun(t, "attr-chain", durable.Fail(wrapped))
		if res.Failure.Kind != durable.FailureKindUser || res.Failure.Reason != "invalid-image" {
			t.Fatalf("Failure = %+v, want user/invalid-image from chain", res.Failure)
		}
	})
	t.Run("options override the chain", func(t *testing.T) {
		res := failingRun(t, "attr-precedence", durable.Fail(&classifiedError{msg: "x"},
			durable.WithReason("overridden")))
		if res.Failure.Reason != "overridden" || res.Failure.Kind != durable.FailureKindUser {
			t.Fatalf("Failure = %+v, want reason overridden, kind still from chain", res.Failure)
		}
	})
}

func TestUnwindFailureAttribution(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "attr-unwind",
		Steps: []pipelinedef.Step{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					return durable.Fail(errors.New("release rejected"), durable.WithReason("release-rejected"))
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("nope"))
			}),
		},
	})
	st := mem.New()
	_, pipes := startEngine(t, st, def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	// The unwind failure is a fact on the step's own operation record.
	rec, err := st.GetRun(context.Background(), run.ID())
	if err != nil {
		t.Fatal(err)
	}
	ufs := rec.UnwindFailures()
	if len(ufs) != 1 || ufs[0].Reason != "release-rejected" || ufs[0].Kind != durable.FailureKindSystem {
		t.Fatalf("UnwindFailures = %+v, want system/release-rejected", ufs)
	}
	if op := rec.Step("a/v1").Unwind; op.Status != driver.OpFailed || op.Failure == nil || op.Failure.Reason != "release-rejected" || op.Order == 0 {
		t.Fatalf("a/v1 unwind operation = %+v; want failed, with its failure and an order", op)
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
	def := pipelinedef.New(pipelinedef.Config{
		ID: "utf8",
		Steps: []pipelinedef.Step{
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
	store, err := bbolt.Open(filepath.Join(t.TempDir(), "utf8.db"))
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
	if res.Outcome != durable.OutcomeFailure || res.Failure == nil {
		t.Fatalf("result = %+v", res)
	}
	if !utf8.ValidString(res.Failure.Message) || !utf8.ValidString(res.Failure.Reason) {
		t.Fatalf("unsanitized failure text: %+v", res.Failure)
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

// TestRecordedTextIsBounded pins WithTextLimit: failure messages, reasons,
// and the cursor's last error are cut at the limit with a marker, so a
// handler that wraps a response body into its error cannot grow the Run's
// failure record; the default applies without the option.
func TestRecordedTextIsBounded(t *testing.T) {
	long := strings.Repeat("x", 3*engine.DefaultTextLimit) + "é"
	build := func(release <-chan struct{}) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: "bounded",
			Steps: []pipelinedef.Step{
				{
					ID:     "a/v1",
					Unwind: true,
					Run:    func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
					UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
						return durable.Fail(errors.New("unwind " + long))
					},
				},
				stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
					select {
					case <-release:
						return durable.Fail(errors.New("root "+long), durable.WithReason("reason "+long))
					default:
						return errors.New("retry " + long)
					}
				}),
			},
		})
	}
	for _, tc := range []struct {
		name  string
		opts  []engine.Option
		limit int
	}{
		{"default", nil, engine.DefaultTextLimit},
		{"configured", []engine.Option{engine.WithTextLimit(48)}, 48},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			st := mem.New()
			eng := engine.New(st, append([]engine.Option{fastRetry, engine.WithLogger(discardTestLogger())}, tc.opts...)...)
			pipe, err := eng.Bind(build(release))
			if err != nil {
				t.Fatal(err)
			}
			if err := eng.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer eng.Stop(context.Background())
			bounded := func(what, s string) {
				t.Helper()
				if len(s) > tc.limit || !strings.HasSuffix(s, "…") || !utf8.ValidString(s) {
					t.Fatalf("%s = %d bytes, suffix %q, valid=%v; want ≤ %d bytes ending in the marker", what, len(s), s[max(0, len(s)-4):], utf8.ValidString(s), tc.limit)
				}
			}

			run, _, err := pipe.Schedule(context.Background(), "r", nil)
			if err != nil {
				t.Fatal(err)
			}
			// The retrying step's last error is bounded while in flight.
			deadline := time.Now().Add(5 * time.Second)
			for {
				st, err := run.Status(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if st.LastError != "" {
					bounded("LastError", st.LastError)
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("LastError never surfaced")
				}
				time.Sleep(time.Millisecond)
			}
			close(release)
			res, err := run.Wait(context.Background())
			if err != nil || !res.Failed() {
				t.Fatalf("Wait = %+v, %v", res, err)
			}
			bounded("Failure.Message", res.Failure.Message)
			bounded("Failure.Reason", res.Failure.Reason)
			rec, err := st.GetRun(context.Background(), run.ID())
			if err != nil {
				t.Fatal(err)
			}
			ufs := rec.UnwindFailures()
			if len(ufs) != 1 {
				t.Fatalf("UnwindFailures = %+v", ufs)
			}
			bounded("UnwindFailures[0].Message", ufs[0].Message)
			if !strings.HasPrefix(res.Failure.Message, "root x") || !strings.HasPrefix(ufs[0].Message, "unwind x") {
				t.Fatalf("messages lost their head: %q / %q", res.Failure.Message[:8], ufs[0].Message[:8])
			}
		})
	}
}

// TestFailureReducer pins the failure reducer: it runs once when the
// unwind completes, sees the run's Failure and each failed compensation by
// step, and its output is the failed run's terminal output; a nil result
// marks the run invalid rather than terminal, as for the success reducer.
func TestFailureReducer(t *testing.T) {
	var seen struct {
		failure  *durable.Failure
		aFailed  bool
		bFailed  bool
		hasState bool
	}
	build := func(returnNil bool) *pipelinedef.Definition {
		bRef := refFor("b/v1")
		return pipelinedef.New(pipelinedef.Config{
			ID: "freduce",
			Steps: []pipelinedef.Step{
				{ID: "a/v1", Unwind: true,
					Run:        func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
					UnwindFunc: func(ctx context.Context, inv durable.Invocation) error { return nil }},
				{ID: "b/v1", Unwind: true, HasState: true,
					Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return str("held"), nil },
					UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
						return durable.Fail(errors.New("release rejected"), durable.WithReason("stuck"))
					}},
				stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error {
					return durable.Fail(errors.New("boom"), durable.WithUserKind(), durable.WithReason("quota"))
				}),
			},
			ReduceFailure: func(v durable.ReduceView) proto.Message {
				seen.failure = v.Failure()
				_, seen.aFailed = v.UnwindFailure("a/v1")
				_, seen.bFailed = v.UnwindFailure("b/v1")
				_, seen.hasState = durable.LookupState(v, bRef)
				if returnNil {
					return nil
				}
				return str("leaked:" + string(v.Failure().StepID))
			},
		})
	}

	st := mem.New()
	_, pipes := startEngine(t, st, build(false))
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if seen.failure == nil || seen.failure.StepID != "c/v1" || seen.failure.Reason != "quota" {
		t.Fatalf("reducer saw Failure %+v; want c/v1 quota", seen.failure)
	}
	if seen.aFailed || !seen.bFailed || !seen.hasState {
		t.Fatalf("reducer saw aFailed=%v bFailed=%v state=%v; want only b failed, with its state", seen.aFailed, seen.bFailed, seen.hasState)
	}
	b, err := run.OutputBytes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := &wrapperspb.StringValue{}
	if err := proto.Unmarshal(b, out); err != nil || out.GetValue() != "leaked:c/v1" {
		t.Fatalf("failed run output = %q, %v; want the failure reducer's", out.GetValue(), err)
	}

	// A nil result from the failure reducer invalidates the run instead
	// of committing terminal failure.
	_, pipes = startEngine(t, mem.New(), build(true))
	run, _, err = pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	var invalid *engine.InvalidRunError
	if _, err := run.Wait(context.Background()); !errors.As(err, &invalid) || !strings.Contains(invalid.Reason, "failure reducer") {
		t.Fatalf("Wait = %v; want InvalidRunError naming the failure reducer", err)
	}
}
