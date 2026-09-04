// Package durable executes fixed, linear pipelines whose execution state
// survives process crashes and restarts.
//
// A pipeline is an ordered sequence of Steps declared in Protocol Buffers
// and compiled into typed Go APIs by protoc-gen-durable. Step operations
// use at-least-once execution semantics: ordinary errors are retried,
// permanent failure is declared explicitly with Fail, and permanent forward
// failure triggers reverse unwind of previously successful Steps.
//
// The durable representation is immutable execution facts interpreted
// against the currently registered pipeline definition using monotonic
// forward and unwind frontiers. See the spec/ directory for the full
// specification.
//
// # Logging
//
// The Engine logs lifecycle events through the log/slog logger given to
// WithLogger: per-attempt progress at Debug, once-per-run milestones at
// Info, permanent unwind failures at Warn, and anomalies at Error.
// Lifecycle lines carry the canonical keys pipeline, resource, and run;
// operation-scoped lines add step, phase, and attempt; failure causes
// appear under error. Handlers log through Invocation.Logger, which
// pre-attaches the same canonical keys; contrib/durableotel's
// NewLogHandler additionally stamps trace correlation onto records
// logged with the attempt's context.
//
// # Metrics
//
// WithObserver installs typed lifecycle callbacks (attempts, terminal
// outcomes, unwind starts, wake and throttle waits, store operations)
// for counter- and histogram-style metrics; Engine.Stats returns a
// point-in-time occupancy snapshot for poll-style gauges. Adapters for
// specific metrics systems belong outside the engine — see Observer;
// contrib/durableotel (a separate module) ships the OpenTelemetry one.
//
// # Tracing
//
// A Run outlives the process and trace that scheduled it, so trace
// context is propagated durably: inject it (for example a W3C
// traceparent) at Schedule with WithAnnotations, and extract it in a
// WithMiddleware tracing middleware via Invocation.Annotations, emitting
// per-attempt spans linked to the originating trace — span links, not a
// long-lived parent, are the recommended shape for work that may run
// hours later. The engine itself never depends on a tracing library;
// contrib/durableotel — a separate module, keeping the OpenTelemetry
// SDK out of this module's dependency graph — implements the complete
// shape (Middleware to extract, WithTraceContext to inject), and
// examples/tracing-otel demonstrates it over a generated
// order-fulfillment pipeline.
package durable
