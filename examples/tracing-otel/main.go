// Command tracing-otel demonstrates durable trace propagation with the
// real OpenTelemetry SDK, wired through contrib/durableotel:
//
//  1. The scheduling side — here, a pretend "POST /orders" request span
//     — INJECTS its W3C trace context into the Run at acceptance:
//     durableotel.WithTraceContext persists it with the Run.
//  2. durableotel.Middleware EXTRACTS that context on every attempt —
//     retries, unwind, restarts, hours later — and starts one span per
//     attempt.
//  3. Each attempt span carries a LINK to the originating span, not a
//     parent relationship: parenting would pin every attempt under a
//     request trace that may have ended hours ago, the anti-pattern
//     span links exist to avoid. The full context-relations story is
//     documented on durableotel.Middleware.
//  4. The handler's ctx carries the attempt span, so spans the handler
//     starts itself (chargePayment's "charge card" gateway call) nest
//     under the attempt.
//
// Both this module and durableotel are separate from the durable module
// precisely so the OpenTelemetry SDK never enters durable's dependency
// graph: the engine core stays tracing-library-free. durableotel also
// bridges engine events to OTel metrics (NewObserver, RegisterStats);
// this example keeps to the tracing half.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dangra/durable"
	"github.com/dangra/durable/contrib/durableotel"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/examples/tracing-otel/orderspb"
)

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
		durable.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
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
	// injected durably into the Run by WithTraceContext. Schedule's ctx
	// itself never reaches the handlers — the annotation is the only
	// bridge.
	reqCtx, requestSpan := tracer.Start(ctx, "POST /orders")
	origin := requestSpan.SpanContext()

	run, _, err := pipe.Schedule(reqCtx, "order-7421", &orderspb.FulfillOrderInput{
		Sku: "SKU-COFFEE-1KG", Quantity: 2, AmountCents: 3400, ShipTo: "invalid",
	}, durableotel.WithTraceContext(reqCtx))
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
		attempt := ""
		for _, kv := range sp.Attributes() {
			if kv.Key == durableotel.AttrAttempt {
				attempt = "attempt " + kv.Value.String()
			}
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
		fmt.Printf("span %s  %-28s %-10s %s\n", sp.SpanContext().SpanID(), sp.Name(), attempt, relation)
	}
}
