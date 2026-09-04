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
// the trace that scheduled the Run. This is the complete integration;
// the context relations are the subtle part:
//
//  1. The attempt ctx handed to this middleware descends from the
//     ENGINE's context, not from the ctx that called Schedule — the
//     Run outlives that request, and the engine never carries its
//     values forward. At this point the ctx holds cancellation and
//     preemption semantics but NO ambient trace: the annotations are
//     the only bridge back to the scheduling trace.
//  2. Extract installs the carrier's span context into the returned
//     ctx as the ambient parent — which is precisely what must NOT
//     reach tracer.Start unmodified: OTel would parent every attempt
//     under a request trace that may have ended hours ago, producing
//     one monster trace across retries, parks, and restarts.
//  3. WithNewRoot severs that parent relation — each attempt is its
//     own trace — and WithLinks preserves the correlation instead:
//     links are the recommended shape for asynchronous work.
//  4. The ctx returned by tracer.Start carries the new attempt span
//     into the handler, so spans the handler starts (an instrumented
//     payment-gateway call, say) nest under the attempt correctly.
func tracingMiddleware(tracer trace.Tracer) durable.Middleware {
	prop := propagation.TraceContext{}
	return func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			carrier := propagation.MapCarrier(inv.Annotations())
			ctx = prop.Extract(ctx, carrier) // (2) origin becomes the ambient context...
			origin := trace.SpanContextFromContext(ctx)

			opts := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithNewRoot(), // (3) ...which WithNewRoot refuses as a parent...
			}
			if origin.IsValid() {
				opts = append(opts, trace.WithLinks(trace.Link{SpanContext: origin})) // ...keeping it as a link.
			}
			ctx, span := tracer.Start(ctx,
				fmt.Sprintf("%s %s attempt %d", inv.StepID(), inv.Phase(), inv.Attempt()), opts...)
			defer span.End()
			span.SetAttributes(
				attribute.String("durable.pipeline", string(inv.PipelineID())),
				attribute.String("durable.run_id", string(inv.RunID())),
			)
			out, err := next(ctx, inv) // (4) the handler sees the attempt span
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

type chargePayment struct {
	w      *warehouse
	tracer trace.Tracer
}

func (h *chargePayment) Run(ctx context.Context, inv orderspb.ChargePaymentInvocation) (*orderspb.ChargePayment, error) {
	// The handler's ctx carries the attempt span the middleware
	// started, so downstream instrumentation nests under it — this is
	// what an otelhttp-instrumented gateway call would do implicitly.
	ctx, gatewayCall := h.tracer.Start(ctx, "charge card")
	defer gatewayCall.End()
	_ = ctx
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

func run(ctx context.Context) (*tracetest.SpanRecorder, trace.SpanContext, orderspb.FulfillOrderResult, error) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(ctx)
	tracer := tp.Tracer("examples/tracing-otel")

	engine := durable.NewEngine(durabletest.NewMemStore(),
		durable.WithRetryPolicy(durable.RetryPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2}),
		durable.WithLogger(slog.New(slog.DiscardHandler)),
		durable.WithMiddleware(tracingMiddleware(tracer)))
	w := &warehouse{}
	pipe, err := orderspb.NewFulfillOrder(
		&reserveStock{w}, &chargePayment{w: w, tracer: tracer}, ship{},
		func(o *orderspb.FulfillOrder) *orderspb.FulfillOrderOutput {
			s, _ := o.State(orderspb.ShipStep)
			return &orderspb.FulfillOrderOutput{ShipmentId: s.GetShipmentId()}
		},
	).Bind(engine)
	if err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	if err := engine.Start(ctx); err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	defer engine.Stop(ctx)

	// The scheduling side: an ordinary request span whose context is
	// injected durably into the Run. Schedule's ctx itself never
	// reaches the handlers — the carrier annotation is the only bridge.
	reqCtx, requestSpan := tracer.Start(ctx, "POST /orders")
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(reqCtx, carrier)
	origin := requestSpan.SpanContext()

	run, _, err := pipe.Schedule(reqCtx, "order-7421", &orderspb.FulfillOrderInput{
		Sku: "SKU-COFFEE-1KG", Quantity: 2, AmountCents: 3400, ShipTo: "invalid",
	}, durable.WithAnnotations(carrier))
	if err != nil {
		return nil, trace.SpanContext{}, orderspb.FulfillOrderResult{}, err
	}
	requestSpan.End() // the request returns long before the Run finishes

	res, err := run.Wait(ctx)
	return recorder, origin, res, err
}

func main() {
	recorder, origin, res, err := run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("origin trace  %s (the POST /orders request)\n", origin.TraceID())
	fmt.Printf("run outcome   %v\n\n", res.Outcome)
	for _, sp := range recorder.Ended() {
		if sp.Name() == "POST /orders" {
			continue
		}
		relation := "UNLINKED"
		for _, l := range sp.Links() {
			if l.SpanContext.TraceID() == origin.TraceID() {
				relation = "link -> origin " + origin.TraceID().String()[:8]
			}
		}
		if p := sp.Parent(); p.IsValid() {
			relation = "child of " + p.SpanID().String() // handler span nesting under its attempt
		}
		fmt.Printf("span %s  %-36s %s\n", sp.SpanContext().SpanID(), sp.Name(), relation)
	}
}
