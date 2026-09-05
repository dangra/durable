package durableotel_test

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/dangra/durable"
	"github.com/dangra/durable/contrib/durableotel"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

// baggageCtx is a scheduling-side ctx carrying two baggage members, the
// shape an edge gateway (FLAPS-style) hands down.
func baggageCtx(t *testing.T) context.Context {
	t.Helper()
	tenant, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	machine, err := baggage.NewMember("machine_id", "m-42")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(tenant, machine)
	if err != nil {
		t.Fatal(err)
	}
	return baggage.ContextWithBaggage(t.Context(), bag)
}

// runBaggageProbe schedules a one-step pipeline and returns what the
// handler observed: its ctx baggage and the Run's annotations.
func runBaggageProbe(t *testing.T, tp *sdktrace.TracerProvider, schedule []durable.ScheduleOption, mwOpts ...durableotel.Option) (seen map[string]string, annotations map[string]string) {
	t.Helper()
	var mu sync.Mutex
	seen, annotations = map[string]string{}, map[string]string{}

	def := pipelinedef.New(pipelinedef.Config{
		ID: "baggage-probe",
		Steps: []pipelinedef.Step{{
			ID: "probe/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				mu.Lock()
				defer mu.Unlock()
				for _, m := range baggage.FromContext(ctx).Members() {
					seen[m.Key()] = m.Value()
				}
				for k, v := range inv.Annotations() {
					annotations[k] = v
				}
				return nil, nil
			},
		}},
	})
	eng := engine.New(durabletest.NewMemStore(), fastRetry, quietLogger(),
		engine.WithMiddleware(durableotel.Middleware(
			append([]durableotel.Option{durableotel.WithTracerProvider(tp)}, mwOpts...)...)))
	pipe, err := eng.Bind(def)
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
	if _, err := run.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return seen, annotations
}

// TestBaggageRoundTrip is the FLAPS-shaped story: baggage set upstream
// of Schedule persists in the Run and reappears in every attempt's ctx,
// with selected members surfacing on attempt spans.
func TestBaggageRoundTrip(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	ctx := baggageCtx(t)
	seen, annotations := runBaggageProbe(t, tp,
		[]durable.ScheduleOption{durableotel.WithTraceContext(ctx, durableotel.WithBaggage())},
		durableotel.WithBaggage(), durableotel.WithSpanBaggage("tenant"))

	if seen["tenant"] != "acme" || seen["machine_id"] != "m-42" {
		t.Fatalf("handler baggage = %v, want both members", seen)
	}
	if annotations["baggage"] == "" {
		t.Fatal("baggage annotation not persisted with the Run")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if v, ok := attr(spans[0], "tenant"); !ok || v != "acme" {
		t.Fatalf("span tenant = %q, %v; want acme (selected member)", v, ok)
	}
	if _, ok := attr(spans[0], "machine_id"); ok {
		t.Fatal("span carries unselected baggage member")
	}
}

// TestSpanBaggageAllMembers pins the no-args mode: every member becomes
// a span attribute.
func TestSpanBaggageAllMembers(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	ctx := baggageCtx(t)
	_, _ = runBaggageProbe(t, tp,
		[]durable.ScheduleOption{durableotel.WithTraceContext(ctx, durableotel.WithBaggage())},
		durableotel.WithBaggage(), durableotel.WithSpanBaggage())

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	for k, want := range map[string]string{"tenant": "acme", "machine_id": "m-42"} {
		if v, ok := attr(spans[0], k); !ok || v != want {
			t.Fatalf("span %s = %q, %v; want %q", k, v, ok, want)
		}
	}
}

// TestBaggageIsOptIn pins the default: without WithBaggage, baggage on
// the scheduling ctx neither persists nor reaches attempts.
func TestBaggageIsOptIn(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(t.Context())

	ctx := baggageCtx(t)
	seen, annotations := runBaggageProbe(t, tp,
		[]durable.ScheduleOption{durableotel.WithTraceContext(ctx)})

	if len(seen) != 0 {
		t.Fatalf("handler baggage = %v, want none without WithBaggage", seen)
	}
	if _, ok := annotations["baggage"]; ok {
		t.Fatal("baggage annotation persisted without WithBaggage")
	}
}
