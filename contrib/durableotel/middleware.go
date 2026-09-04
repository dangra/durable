package durableotel

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/dangra/durable"
)

// WithTraceContext returns a ScheduleOption that injects ctx's trace
// context (and whatever else the configured propagator carries) into the
// Run's annotations, durably tying every future attempt back to the
// trace that scheduled it. The scheduling ctx itself never reaches the
// handlers — the Run outlives that request — so this annotation is the
// only bridge:
//
//	pipe.Schedule(reqCtx, resource, input, durableotel.WithTraceContext(reqCtx))
func WithTraceContext(ctx context.Context, opts ...Option) durable.ScheduleOption {
	cfg := newConfig(opts)
	carrier := propagation.MapCarrier{}
	cfg.propagator.Inject(ctx, carrier)
	return durable.WithAnnotations(carrier)
}

// Middleware returns a durable.Middleware that wraps every operation
// attempt in a span linked to the trace that scheduled the Run (injected
// by WithTraceContext). The context relations are the subtle part:
//
//  1. The attempt ctx handed to the middleware descends from the
//     ENGINE's context, not from the ctx that called Schedule — the Run
//     outlives that request, and the engine never carries its values
//     forward. At this point the ctx holds cancellation and preemption
//     semantics but NO ambient trace: the annotations are the only
//     bridge back to the scheduling trace.
//  2. Extract installs the carrier's span context into the returned ctx
//     as the ambient parent — which is precisely what must NOT reach
//     tracer.Start unmodified: OTel would parent every attempt under a
//     request trace that may have ended hours ago, producing one
//     monster trace across retries, parks, and restarts.
//  3. WithNewRoot severs that parent relation — each attempt is its own
//     trace — and WithLinks preserves the correlation instead: links
//     are the recommended shape for asynchronous work.
//  4. The ctx returned by tracer.Start carries the new attempt span
//     into the handler, so spans the handler starts (an instrumented
//     payment-gateway call, say) nest under the attempt correctly.
//
// Span names are "<step> <phase>" (low cardinality); the attempt number
// and Run identity ride as durable.* attributes, and WithSpanAnnotations
// adds selected Run annotations. A handler error records on the span and
// sets Error status; a permanent failure additionally stamps the
// durable.failure_kind and durable.reason the engine will commit
// (FailureInfo). An AwaitRun resolution is a park, not a failure, and
// becomes a durable.await_target attribute instead. A handler panic is
// recorded on the span — exception event with stack trace, Error
// status, durable.panicked — and re-panicked for the engine, which
// treats it as an ordinary retryable error.
func Middleware(opts ...Option) durable.Middleware {
	cfg := newConfig(opts)
	tracer := cfg.tracerProvider.Tracer(ScopeName)
	return func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			annotations := inv.Annotations()
			ctx = cfg.propagator.Extract(ctx, propagation.MapCarrier(annotations))
			origin := trace.SpanContextFromContext(ctx)

			attrs := []attribute.KeyValue{
				AttrPipeline.String(string(inv.PipelineID())),
				AttrResource.String(string(inv.ResourceID())),
				AttrRunID.String(string(inv.RunID())),
				AttrStep.String(string(inv.StepID())),
				AttrPhase.String(inv.Phase().String()),
				AttrAttempt.Int64(int64(inv.Attempt())),
			}
			for _, k := range cfg.spanAnnotations {
				if v, ok := annotations[k]; ok {
					attrs = append(attrs, attribute.String(k, v))
				}
			}
			startOpts := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithNewRoot(),
				trace.WithAttributes(attrs...),
			}
			if origin.IsValid() {
				startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: origin}))
			}
			ctx, span := tracer.Start(ctx,
				fmt.Sprintf("%s %s", inv.StepID(), inv.Phase()), startOpts...)
			defer span.End()
			defer func() {
				// The engine's own recover sits outside the middleware
				// chain, so without this the span would end silently OK
				// on the worst failure mode. Record, then hand the panic
				// back to the engine untouched.
				if p := recover(); p != nil {
					span.SetAttributes(AttrPanicked.Bool(true))
					span.RecordError(fmt.Errorf("handler panic: %v", p), trace.WithStackTrace(true))
					span.SetStatus(codes.Error, "handler panic")
					panic(p)
				}
			}()

			out, err := next(ctx, inv)
			if err != nil {
				if target, ok := durable.AwaitTarget(err); ok {
					span.SetAttributes(AttrAwaitTarget.String(string(target)))
				} else {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					if kind, reason, ok := durable.FailureInfo(err); ok {
						span.SetAttributes(AttrFailureKind.String(kind.String()))
						if reason != "" {
							span.SetAttributes(AttrReason.String(reason))
						}
					}
				}
			}
			return out, err
		}
	}
}
