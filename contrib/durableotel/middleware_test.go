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
	"github.com/dangra/durable/durabletest"
)

var fastRetry = durable.WithRetryPolicy(durable.RetryPolicy{
	Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2,
})

func quietLogger() durable.Option {
	return durable.WithLogger(slog.New(slog.DiscardHandler))
}

// sagaDef is a two-step pipeline exercising every span shape: a
// retrying step with an unwind, then a permanent failure that triggers
// the unwind. Its attempts: prepare forward 1 (retry), prepare forward
// 2, explode forward 1 (permanent), prepare unwind 1.
func sagaDef() *durable.Definition {
	return durable.NewDefinition(durable.DefinitionConfig{
		ID: "saga",
		Steps: []durable.StepConfig{
			{
				ID:     "prepare/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient")
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			{
				ID: "explode/v1",
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, durable.Fail(errors.New("boom"),
						durable.WithUserKind(), durable.WithReason("invalid-input"))
				},
			},
		},
	})
}

// runSaga executes sagaDef on a fresh engine wired with the given extra
// options and returns the schedule-side origin span context.
func runSaga(t *testing.T, schedule []durable.ScheduleOption, opts ...durable.Option) {
	t.Helper()
	engine := durable.NewEngine(durabletest.NewMemStore(),
		append([]durable.Option{fastRetry, quietLogger()}, opts...)...)
	pipe, err := sagaDef().Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(t.Context())

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

func TestMiddlewareSpansLinkToOrigin(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())
	tracer := tp.Tracer("test")

	reqCtx, requestSpan := tracer.Start(t.Context(), "POST /saga")
	origin := requestSpan.SpanContext()
	runSaga(t,
		[]durable.ScheduleOption{durableotel.WithTraceContext(reqCtx)},
		durable.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
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
		durable.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))

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

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "awaiter",
		Steps: []durable.StepConfig{{
			ID: "wait/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if _, woken := inv.AwaitedRunID(); !woken {
					return nil, durable.AwaitRun(missing)
				}
				return nil, nil
			},
		}},
	})
	engine := durable.NewEngine(durabletest.NewMemStore(), fastRetry, quietLogger(),
		durable.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
	pipe, err := def.Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(t.Context())

	run, _, err := pipe.Schedule(t.Context(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := run.Wait(t.Context()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("Wait = %v, %v; want success", res, err)
	}

	parked := 0
	for _, sp := range recorder.Ended() {
		target, ok := attr(sp, string(durableotel.AttrAwaitTarget))
		if !ok {
			continue
		}
		parked++
		if target != string(missing) {
			t.Fatalf("await target = %q, want %q", target, missing)
		}
		if sp.Status().Code == codes.Error {
			t.Fatal("park span carries Error status; a park is not a failure")
		}
	}
	if parked != 1 {
		t.Fatalf("park spans = %d, want 1", parked)
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
	engine := durable.NewEngine(durabletest.NewMemStore(), fastRetry, quietLogger(),
		durable.WithMiddleware(func(next durable.Handler) durable.Handler {
			return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				for k, v := range inv.Annotations() {
					got[k] = v
				}
				return next(ctx, inv)
			}
		}))
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "probe",
		Steps: []durable.StepConfig{{
			ID: "noop/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})
	pipe, err := def.Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(t.Context())
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
