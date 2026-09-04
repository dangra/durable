// Command tracing-otel is the real-OpenTelemetry version of the trace
// propagation shape (see examples/tracing for the dependency-free
// walkthrough of the same pattern):
//
//  1. The scheduling side — here, a pretend "POST /orders" request span
//     — INJECTS its W3C trace context into the Run at acceptance:
//     propagation.TraceContext writes traceparent into a MapCarrier
//     that durable.WithAnnotations persists with the Run.
//  2. A durable.WithMiddleware tracing middleware EXTRACTS the context
//     from Invocation.Annotations on every attempt — retries, unwind,
//     restarts, hours later — and starts one span per attempt.
//  3. Each attempt span carries a LINK to the originating span
//     (trace.WithLinks), not a parent relationship: extracting into the
//     attempt's context before tracer.Start would parent the span under
//     a request trace that may have ended hours ago, which is the
//     anti-pattern span links exist to avoid.
//
// This module is separate from the durable module precisely so the
// OpenTelemetry SDK never enters durable's dependency graph: the engine
// core stays tracing-library-free.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/examples/tracing-otel/orderspb"
)

// tracingMiddleware wraps every operation attempt in a span linked to
// the trace that scheduled the Run. This is the complete integration.
func tracingMiddleware(tracer trace.Tracer) durable.Middleware {
	prop := propagation.TraceContext{}
	return func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			// Extract into a THROWAWAY context, not the attempt's: the
			// origin must become a span link below, never a parent.
			carrier := propagation.MapCarrier(inv.Annotations())
			origin := trace.SpanContextFromContext(prop.Extract(context.Background(), carrier))

			opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}
			if origin.IsValid() {
				opts = append(opts, trace.WithLinks(trace.Link{SpanContext: origin}))
			}
			ctx, span := tracer.Start(ctx,
				fmt.Sprintf("%s %s attempt %d", inv.StepID(), inv.Phase(), inv.Attempt()), opts...)
			defer span.End()
			span.SetAttributes(
				attribute.String("durable.pipeline", string(inv.PipelineID())),
				attribute.String("durable.run_id", string(inv.RunID())),
			)
			out, err := next(ctx, inv)
			if err != nil {
				span.RecordError(err)
			}
			return out, err
		}
	}
}

// warehouse is the fake order-fulfillment backend.
type warehouse struct {
	mu       sync.Mutex
	nextID   int
	released []string
	refunded []string
}

func (w *warehouse) id(prefix string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	return fmt.Sprintf("%s-%d", prefix, w.nextID)
}

type reserveStock struct{ w *warehouse }

func (h *reserveStock) Run(ctx context.Context, inv orderspb.ReserveStockInvocation) (*orderspb.ReserveStock, error) {
	return &orderspb.ReserveStock{ReservationId: h.w.id("stock")}, nil
}

func (h *reserveStock) Unwind(ctx context.Context, inv orderspb.ReserveStockInvocation, f durable.Failure) error {
	if res, ok := inv.State(orderspb.ReserveStockStep); ok {
		h.w.mu.Lock()
		h.w.released = append(h.w.released, res.GetReservationId())
		h.w.mu.Unlock()
	}
	return nil
}

type chargePayment struct{ w *warehouse }

func (h *chargePayment) Run(ctx context.Context, inv orderspb.ChargePaymentInvocation) (*orderspb.ChargePayment, error) {
	if inv.Attempt() == 1 {
		return nil, errors.New("payment gateway timeout") // retried
	}
	return &orderspb.ChargePayment{ChargeId: h.w.id("charge")}, nil
}

func (h *chargePayment) Unwind(ctx context.Context, inv orderspb.ChargePaymentInvocation, f durable.Failure) error {
	if c, ok := inv.State(orderspb.ChargePaymentStep); ok {
		h.w.mu.Lock()
		h.w.refunded = append(h.w.refunded, c.GetChargeId())
		h.w.mu.Unlock()
	}
	return nil
}

type ship struct{}

func (ship) Run(ctx context.Context, inv orderspb.ShipInvocation) (*orderspb.Ship, error) {
	return nil, durable.Fail(errors.New("carrier rejected the address"),
		durable.WithUserKind(), durable.WithReason("invalid-address"))
}

func run() (*tracetest.SpanRecorder, trace.SpanContext, orderspb.FulfillOrderResult, error) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("examples/tracing-otel")

	engine := durable.NewEngine(durabletest.NewMemStore(),
		durable.WithRetryPolicy(durable.RetryPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2}),
		durable.WithLogger(slog.New(slog.DiscardHandler)),
		durable.WithMiddleware(tracingMiddleware(tracer)))
	w := &warehouse{}
	pipe, err := orderspb.NewFulfillOrder(
		&reserveStock{w}, &chargePayment{w}, ship{},
		func(o *orderspb.FulfillOrder) *orderspb.FulfillOrderOutput {
			s, _ := o.State(orderspb.ShipStep)
			return &orderspb.FulfillOrderOutput{ShipmentId: s.GetShipmentId()}
		},
	).Bind(engine)
	if err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	if err := engine.Start(context.Background()); err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	defer engine.Stop(context.Background())

	// The scheduling side: an ordinary request span whose context is
	// injected durably into the Run.
	ctx, requestSpan := tracer.Start(context.Background(), "POST /orders")
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	origin := requestSpan.SpanContext()

	run, _, err := pipe.Schedule(ctx, "order-7421", &orderspb.FulfillOrderInput{
		Sku: "SKU-COFFEE-1KG", Quantity: 2, AmountCents: 3400, ShipTo: "invalid",
	}, durable.WithAnnotations(carrier))
	if err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	requestSpan.End() // the request returns long before the Run finishes

	res, err := run.Wait(context.Background())
	return recorder, origin, res, err
}

func main() {
	recorder, origin, res, err := run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("origin trace  %s (the POST /orders request)\n", origin.TraceID())
	fmt.Printf("run outcome   %v\n\n", res.Outcome)
	for _, sp := range recorder.Ended() {
		if sp.Name() == "POST /orders" {
			continue
		}
		link := "UNLINKED"
		for _, l := range sp.Links() {
			if l.SpanContext.TraceID() == origin.TraceID() {
				link = "link -> " + origin.TraceID().String()
			}
		}
		fmt.Printf("span %s  %-36s %s\n", sp.SpanContext().SpanID(), sp.Name(), link)
	}
}
