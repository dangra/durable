// Package durableotel instruments durable with OpenTelemetry without
// adding the SDK to durable's own dependency graph: it is a separate Go
// module wired entirely through durable's public seams — WithMiddleware,
// WithAnnotations, Observer, and Engine.Stats.
//
// The complete integration is three lines:
//
//	obs, _ := durableotel.NewObserver()
//	engine := durable.NewEngine(store,
//		durable.WithMiddleware(durableotel.Middleware()),
//		durable.WithObserver(obs))
//	// ... and per request:
//	pipe.Schedule(ctx, resource, input, durableotel.WithTraceContext(ctx))
//
// [Middleware] starts one span per operation attempt, linked (not
// parented) to the trace that scheduled the Run; [WithTraceContext]
// injects that scheduling trace into the Run's annotations at
// acceptance; [NewObserver] translates engine lifecycle events into
// OTel metrics; [RegisterStats] publishes Engine.Stats occupancy as
// gauges; [NewLogHandler] stamps trace_id/span_id onto slog records so
// handler log lines join their attempt span. Traces, metrics, and logs
// all label with the same durable.* attribute keys declared in this
// package.
package durableotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope under which this package's
// tracer and meter are registered.
const ScopeName = "github.com/dangra/durable/contrib/durableotel"

// Attribute keys shared by every signal this package emits. Values come
// verbatim from durable identifiers and enums, all bounded-cardinality
// except AttrRunID and AttrResource, which appear only on spans, never
// on metrics.
const (
	AttrPipeline    attribute.Key = "durable.pipeline"
	AttrResource    attribute.Key = "durable.resource"
	AttrRunID       attribute.Key = "durable.run_id"
	AttrStep        attribute.Key = "durable.step"
	AttrPhase       attribute.Key = "durable.phase"
	AttrAttempt     attribute.Key = "durable.attempt"
	AttrResult      attribute.Key = "durable.result"
	AttrOutcome     attribute.Key = "durable.outcome"
	AttrFailureKind attribute.Key = "durable.failure_kind"
	AttrReason      attribute.Key = "durable.reason"
	AttrClass       attribute.Key = "durable.class"
	AttrAwaitTarget attribute.Key = "durable.await_target"
	AttrPanicked    attribute.Key = "durable.panicked"
	AttrStoreOp     attribute.Key = "durable.store.op"
	AttrStoreWrite  attribute.Key = "durable.store.write"
	AttrError       attribute.Key = "durable.error"
)

// Option configures this package's constructors.
type Option func(*config)

type config struct {
	tracerProvider  trace.TracerProvider
	meterProvider   metric.MeterProvider
	propagator      propagation.TextMapPropagator
	spanAnnotations []string
}

func newConfig(opts []Option) config {
	cfg := config{
		tracerProvider: otel.GetTracerProvider(),
		meterProvider:  otel.GetMeterProvider(),
		propagator:     propagation.TraceContext{},
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithTracerProvider sets the TracerProvider used by Middleware. The
// default is the global otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		if tp != nil {
			c.tracerProvider = tp
		}
	}
}

// WithMeterProvider sets the MeterProvider used by NewObserver and
// RegisterStats. The default is the global otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		if mp != nil {
			c.meterProvider = mp
		}
	}
}

// WithPropagator sets the TextMapPropagator shared by WithTraceContext
// (inject) and Middleware (extract). The default is W3C
// propagation.TraceContext alone — deliberately not the global
// propagator, so the pair round-trips with zero global setup. Pass a
// composite propagator to carry more:
//
//	durableotel.WithPropagator(propagation.NewCompositeTextMapPropagator(
//		propagation.TraceContext{}, propagation.Baggage{}))
//
// flows OTel baggage (tenant, order ID) into every attempt alongside
// the trace context. Inject and extract must be configured alike.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) {
		if p != nil {
			c.propagator = p
		}
	}
}

// WithSpanAnnotations selects Run annotation keys Middleware copies
// onto every attempt span, each under its annotation key verbatim as
// the attribute name. It surfaces domain identity persisted at Schedule
// time (a machine ID, a tenant) on every span of the Run:
//
//	pipe.Schedule(ctx, res, input, durable.WithAnnotations(
//		map[string]string{"machine.id": id}))
//	durableotel.Middleware(durableotel.WithSpanAnnotations("machine.id"))
//
// Keys absent from a Run's annotations are skipped.
func WithSpanAnnotations(keys ...string) Option {
	return func(c *config) {
		c.spanAnnotations = append(c.spanAnnotations, keys...)
	}
}
