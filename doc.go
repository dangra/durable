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
// # The API in five groups
//
// Alphabetical godoc flattens this package; it reads better in its five
// working groups:
//
// Declaring pipelines. Generated code (protoc-gen-durable over
// durable.v1 proto options) is the primary path; NewDefinition with
// DefinitionConfig and StepConfig is the hand-rolled equivalent the
// generated code itself uses. StepRef and StateStepRef are the step
// references generated packages export (StepIdentifier is the interface
// both satisfy), LookupState and StateSource power typed state reads,
// and ReduceView is what a Reducer folds.
//
// Running an engine. NewEngine over a storedriver implementation, the
// With* Options, Start, Stop (graceful with WithDrainTimeout), and
// Stats.
//
// Scheduling and watching runs. Pipeline.Schedule with ScheduleOptions
// (WithAnnotations, StartAt, StartAfter) or an engine-wide
// ScheduleAnnotator; the Run handle (Wait, Cancel, Status); Result,
// Status, RunState, Outcome, and the failure records; the identity
// types; ScheduleConflictError, PipelineMismatchError, InvalidRunError,
// and the Err* sentinels.
//
// Writing handlers. Invocation carries input, prior state, and attempt
// metadata; it is an interface the engine implements, and
// durabletest.NewInvocation fakes it so handlers are unit-testable
// without an engine. The ways out: success, an ordinary error (retried — a
// returned ctx.Err() included), Fail with FailOptions and
// kind/reason attribution, or AwaitRun, AwaitAll, and AwaitAny to park on
// other Runs, bounded by WithAwaitTimeout (the woken attempt reads the
// park back through Awaited).
// Middleware wraps every attempt (AwaitRequest and FailureInfo classify
// results; PreemptedError and ErrEngineStopping name why an attempt ctx
// died); FailFastOnCancel with FailFastExcept opts preemption-safe
// pipelines out of cooperative cancellation.
//
// Observing. Logging below; observe holds the Observer event surface,
// contrib/durableotel the packaged OpenTelemetry adapter.
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
// WithObserver installs observe.Observer typed lifecycle callbacks (attempts, terminal
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
//
// # Package map
//
// This package is the whole user API: declaring pipelines (generated
// code plus NewDefinition for hand-rolled ones), running an engine, and
// writing handlers. The rest of the module serves narrower audiences:
//
//   - kernel is the shared vocabulary — identities, phases, outcomes,
//     parks, failure records — that every other package builds on. This
//     package aliases all of it, so user code never imports kernel.
//   - observe holds the lifecycle event surface — Observer and its
//     typed events — for telemetry-adapter authors; contrib/durableotel
//     is the packaged adapter most applications install instead.
//   - storedriver holds the store SPI — the Store interface and the
//     durable record types — for implementers of persistence backends,
//     in the spirit of database/sql/driver. Users pick an existing
//     implementation and never import it.
//   - bboltstore is the production bbolt-backed Store.
//   - durabletest provides the in-memory Store, the fake Clock, and the
//     fake Invocation for tests.
//   - durablepb and cmd/protoc-gen-durable are the protobuf options and
//     the code generator behind the generated typed API.
//   - contrib/durableotel (separate module) is the OpenTelemetry
//     integration.
//
// docs/tour.md walks the features; spec/ holds the normative rules.
package durable
