package durableotel_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/dangra/durable"
	"github.com/dangra/durable/contrib/durableotel"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

var fastRetry = engine.WithRetryPolicy(engine.RetryPolicy{
	Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2,
})

func quietLogger() engine.Option {
	return engine.WithLogger(slog.New(slog.DiscardHandler))
}

// sagaDef is a two-step pipeline exercising every span shape: a
// retrying step with an unwind, then a permanent failure that triggers
// the unwind. Its attempts: prepare forward 1 (retry), prepare forward
// 2, explode forward 1 (permanent), prepare unwind 1.
func sagaDef() *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{
		ID: "saga",
		Steps: []pipelinedef.Step{
			{
				ID:     "prepare/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient")
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			{
				ID: "explode/v1",
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, durable.Fail(errors.New("boom"),
						durable.WithUserKind(), durable.WithReason("invalid-input"))
				},
			},
		},
	})
}

// runSaga executes sagaDef on a fresh engine wired with the given extra
// options and returns the schedule-side origin span context.
func runSaga(t *testing.T, schedule []durable.ScheduleOption, opts ...engine.Option) {
	t.Helper()
	eng := engine.New(mem.New(),
		append([]engine.Option{fastRetry, quietLogger()}, opts...)...)
	pipe, err := eng.Bind(sagaDef())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop(t.Context())

	run, _, err := pipe.Schedule(t.Context(), "res-1", nil, schedule...)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Outcome != durable.OutcomeFailure {
		t.Fatalf("outcome = %v, want the scripted failure", res.Outcome)
	}
}

func attr(sp sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range sp.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}

func attrSlice(sp sdktrace.ReadOnlySpan, key string) ([]string, bool) {
	for _, kv := range sp.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsStringSlice(), true
		}
	}
	return nil, false
}

func TestMiddlewareSpansLinkToOrigin(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())
	tracer := tp.Tracer("test")

	reqCtx, requestSpan := tracer.Start(t.Context(), "POST /saga")
	origin := requestSpan.SpanContext()
	runSaga(t,
		[]durable.ScheduleOption{durableotel.WithTraceContext(reqCtx)},
		engine.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
	requestSpan.End()

	// Span names are low-cardinality "<step> <phase>"; the attempt
	// number rides as an attribute.
	wantErr := map[string]bool{ // span name + attempt -> expect Error status
		"prepare/v1 forward|1": true,  // retried
		"prepare/v1 forward|2": false, // succeeded
		"explode/v1 forward|1": true,  // permanent failure
		"prepare/v1 unwind|1":  false, // unwind succeeded
	}
	seen := map[string]bool{}
	for _, sp := range recorder.Ended() {
		if sp.Name() == "POST /saga" {
			continue
		}
		attempt, ok := attr(sp, string(durableotel.AttrAttempt))
		if !ok {
			t.Fatalf("span %q has no attempt attribute", sp.Name())
		}
		key := sp.Name() + "|" + attempt
		wantErrStatus, ok := wantErr[key]
		if !ok || seen[key] {
			t.Fatalf("unexpected or duplicate span %q", key)
		}
		seen[key] = true

		if got := sp.Status().Code == codes.Error; got != wantErrStatus {
			t.Fatalf("span %q error status = %v, want %v", key, got, wantErrStatus)
		}
		// Only the permanent failure carries the engine's attribution;
		// a retryable error must not claim a failure kind.
		kind, hasKind := attr(sp, string(durableotel.AttrFailureKind))
		reason, hasReason := attr(sp, string(durableotel.AttrReason))
		if key == "explode/v1 forward|1" {
			if kind != "user" || reason != "invalid-input" {
				t.Fatalf("permanent-failure span attribution = %q/%q, want user/invalid-input", kind, reason)
			}
		} else if hasKind || hasReason {
			t.Fatalf("span %q claims failure attribution %q/%q", key, kind, reason)
		}
		if sp.SpanContext().TraceID() == origin.TraceID() {
			t.Fatalf("span %q lives in the origin trace; the shape is links, not a parent", key)
		}
		if sp.Parent().IsValid() {
			t.Fatalf("span %q has a parent; attempts must be roots", key)
		}
		linked := false
		for _, l := range sp.Links() {
			if l.SpanContext.SpanID() == origin.SpanID() {
				linked = true
			}
		}
		if !linked {
			t.Fatalf("span %q not linked to the origin span", key)
		}
		for _, key := range []string{
			string(durableotel.AttrPipeline), string(durableotel.AttrResource),
			string(durableotel.AttrRunID), string(durableotel.AttrStep),
			string(durableotel.AttrPhase),
		} {
			if _, ok := attr(sp, key); !ok {
				t.Fatalf("span %q missing attribute %q", sp.Name(), key)
			}
		}
	}
	if len(seen) != len(wantErr) {
		t.Fatalf("attempt spans = %d, want %d (%v)", len(seen), len(wantErr), seen)
	}
}

// TestMiddlewareWithoutOrigin schedules without WithTraceContext: spans
// still exist, valid and root, just unlinked.
func TestMiddlewareWithoutOrigin(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	runSaga(t, nil,
		engine.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))

	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("spans = %d, want 4", len(spans))
	}
	for _, sp := range spans {
		if len(sp.Links()) != 0 {
			t.Fatalf("span %q has links without an injected origin", sp.Name())
		}
		if !sp.SpanContext().IsValid() || sp.Parent().IsValid() {
			t.Fatalf("span %q should be a valid root", sp.Name())
		}
	}
}

// TestMiddlewareAwaitIsNotAnError parks a step on a Run that does not
// exist (an immediate wake) and asserts the park span records the
// await target instead of an error: AwaitRun is a resolution, not a
// failure.
func TestMiddlewareAwaitIsNotAnError(t *testing.T) {
	const missing = durable.RunID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	def := pipelinedef.New(pipelinedef.Config{
		ID: "awaiter",
		Steps: []pipelinedef.Step{{
			ID: "wait/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if _, woken := inv.AwaitedRunID(); !woken {
					return nil, durable.AwaitRun(missing)
				}
				return nil, nil
			},
		}},
	})
	eng := engine.New(mem.New(), fastRetry, quietLogger(),
		engine.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
	pipe, err := eng.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop(t.Context())

	run, _, err := pipe.Schedule(t.Context(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := run.Wait(t.Context()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("Wait = %v, %v; want success", res, err)
	}

	parked := 0
	for _, sp := range recorder.Ended() {
		targets, ok := attrSlice(sp, string(durableotel.AttrAwaitTargets))
		if !ok {
			continue
		}
		parked++
		if len(targets) != 1 || targets[0] != string(missing) {
			t.Fatalf("await targets = %q, want [%q]", targets, missing)
		}
		if mode, _ := attr(sp, string(durableotel.AttrAwaitMode)); mode != "all" {
			t.Fatalf("await mode = %q, want all", mode)
		}
		if sp.Status().Code == codes.Error {
			t.Fatal("park span carries Error status; a park is not a failure")
		}
	}
	if parked != 1 {
		t.Fatalf("park spans = %d, want 1", parked)
	}
}

// TestMiddlewarePanicRecordedAndRethrown panics a handler once and
// asserts the panic attempt's span is not silently OK — Error status,
// durable.panicked, an exception event with a stack trace — while the
// engine still sees the panic and retries the attempt to success.
func TestMiddlewarePanicRecordedAndRethrown(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	def := pipelinedef.New(pipelinedef.Config{
		ID: "panicky",
		Steps: []pipelinedef.Step{{
			ID: "boom/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.Attempt() == 1 {
					panic("kaboom")
				}
				return nil, nil
			},
		}},
	})
	eng := engine.New(mem.New(), fastRetry, quietLogger(),
		engine.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
	pipe, err := eng.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop(t.Context())
	run, _, err := pipe.Schedule(t.Context(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := run.Wait(t.Context()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("Wait = %v, %v; want success — the panic must reach the eng as a retryable", res, err)
	}

	panicked := 0
	for _, sp := range recorder.Ended() {
		if v, ok := attr(sp, string(durableotel.AttrPanicked)); !ok || v != "true" {
			continue
		}
		panicked++
		if sp.Status().Code != codes.Error {
			t.Fatal("panic span not marked Error")
		}
		found := false
		for _, ev := range sp.Events() {
			if ev.Name != "exception" {
				continue
			}
			for _, kv := range ev.Attributes {
				if kv.Key == "exception.stacktrace" && kv.Value.AsString() != "" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("panic span has no exception event with a stack trace")
		}
	}
	if panicked != 1 {
		t.Fatalf("panicked spans = %d, want 1", panicked)
	}
}

// TestWithSpanAnnotations pins that selected Run annotations surface as
// span attributes on every attempt, and unselected or absent keys do
// not.
func TestWithSpanAnnotations(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	runSaga(t,
		[]durable.ScheduleOption{durable.WithAnnotations(map[string]string{
			"machine.id": "m-42",
			"tenant":     "acme",
		})},
		engine.WithMiddleware(durableotel.Middleware(
			durableotel.WithTracerProvider(tp),
			durableotel.WithSpanAnnotations("machine.id", "absent-key"))))

	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("spans = %d, want 4", len(spans))
	}
	for _, sp := range spans {
		if v, ok := attr(sp, "machine.id"); !ok || v != "m-42" {
			t.Fatalf("span %q machine.id = %q, %v; want m-42", sp.Name(), v, ok)
		}
		if _, ok := attr(sp, "tenant"); ok {
			t.Fatalf("span %q carries unselected annotation", sp.Name())
		}
		if _, ok := attr(sp, "absent-key"); ok {
			t.Fatalf("span %q carries an absent annotation", sp.Name())
		}
	}
}

// TestWithTraceContextRoundTrip pins that inject and extract agree on
// the default propagator: what WithTraceContext writes, Middleware
// links to.
func TestWithTraceContextRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(t.Context())
	ctx, span := tp.Tracer("test").Start(t.Context(), "origin")
	defer span.End()

	got := map[string]string{}
	engineOpt := durableotel.WithTraceContext(ctx)
	// The ScheduleOption is opaque; observe its effect through a real
	// schedule.
	eng := engine.New(mem.New(), fastRetry, quietLogger(),
		engine.WithMiddleware(func(next durable.Handler) durable.Handler {
			return func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				for k, v := range inv.Annotations() {
					got[k] = v
				}
				return next(ctx, inv)
			}
		}))
	def := pipelinedef.New(pipelinedef.Config{
		ID: "probe",
		Steps: []pipelinedef.Step{{
			ID: "noop/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	pipe, err := eng.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop(t.Context())
	run, _, err := pipe.Schedule(t.Context(), "res-1", nil, engineOpt)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if _, err := run.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	want := "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if got["traceparent"] != want {
		t.Fatalf("traceparent = %q, want %q", got["traceparent"], want)
	}
}
