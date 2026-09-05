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
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// TestAnnotatorEngineWide is the declare-once DX story: with Annotator
// installed at engine construction, a bare Schedule call — no per-call
// options — propagates the trace context and baggage already riding the
// caller's ctx.
func TestAnnotatorEngineWide(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	var (
		mu   sync.Mutex
		seen = map[string]string{}
	)
	def := pipelinedef.New(pipelinedef.Config{
		ID: "declared-once",
		Steps: []pipelinedef.Step{{
			ID: "probe/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				mu.Lock()
				defer mu.Unlock()
				for _, m := range baggage.FromContext(ctx).Members() {
					seen[m.Key()] = m.Value()
				}
				return nil, nil
			},
		}},
	})
	eng := engine.New(mem.New(), fastRetry, quietLogger(),
		engine.WithMiddleware(durableotel.Middleware(
			durableotel.WithTracerProvider(tp), durableotel.WithBaggage())),
		engine.WithScheduleAnnotator(durableotel.Annotator(durableotel.WithBaggage())))
	pipe, err := eng.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer eng.Stop(t.Context())

	// The subsystem's ctx: a request span plus upstream baggage — and a
	// bare Schedule with nothing to remember.
	reqCtx, requestSpan := tp.Tracer("test").Start(baggageCtx(t), "POST /thing")
	origin := requestSpan.SpanContext()
	run, _, err := pipe.Schedule(reqCtx, "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	requestSpan.End()
	if _, err := run.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	mu.Lock()
	if seen["tenant"] != "acme" || seen["machine_id"] != "m-42" {
		t.Fatalf("handler baggage = %v, want both members without per-call options", seen)
	}
	mu.Unlock()

	linked := false
	for _, sp := range recorder.Ended() {
		if sp.Name() != "probe/v1 forward" {
			continue
		}
		for _, l := range sp.Links() {
			if l.SpanContext.SpanID() == origin.SpanID() {
				linked = true
			}
		}
	}
	if !linked {
		t.Fatal("attempt span not linked to the origin without per-call options")
	}
}
