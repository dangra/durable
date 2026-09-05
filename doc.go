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
// # The handler contract
//
// This package is what handler code programs against, and nothing else:
// the types a step handler receives, the ways it can resolve, and the
// vocabulary shared with the rest of the module. Running an engine,
// scheduling Runs, and declaring pipelines by hand live in the engine
// package; handler files import only this one.
//
// Alphabetical godoc flattens the package; it reads better in three
// working groups:
//
// Writing handlers. Invocation carries input, prior state, and attempt
// metadata; it is an interface the engine implements, and
// durabletest.NewInvocation fakes it so handlers are unit-testable
// without an engine. The ways out: success, an ordinary error (retried — a
// returned ctx.Err() included), Fail with FailOptions and
// kind/reason attribution, or AwaitRun, AwaitAll, and AwaitAny to park on
// other Runs, bounded by WithAwaitTimeout (the woken attempt reads the
// park back through Awaited). Unwind handlers additionally receive the
// Failure being unwound.
//
// Scheduling children. A step that fans out schedules child Runs through
// a bound pipeline handle and parks on them; everything that call needs
// is here: the ScheduleOptions (WithAnnotations, StartAt, StartAfter),
// ScheduleConflictError with the blocking RunID to park on, and the
// ErrRunNotFound and ErrRunTerminal sentinels. Handler files import only
// this package.
//
// Middleware. Handler and Middleware are the net/http-shaped operation
// layer every attempt passes through (installed with
// engine.WithMiddleware). AwaitRequest, AwaitTimeout, FailureInfo,
// FailureCause, and FailureReason classify a handler's return the way
// the engine will; PreemptedError and ErrEngineStopping name why an
// attempt ctx died; FailFastOnCancel with FailFastExcept opts
// preemption-safe pipelines out of cooperative cancellation.
//
// Vocabulary. StepRef and StateStepRef are the step references generated
// packages export, built by pipelinedef (StepIdentifier is the interface
// both satisfy),
// LookupState and StateSource power typed state reads, and ReduceView is
// what a Reducer folds. The identity, phase, outcome, park, and
// failure-record types are aliases of the kernel package's, so a
// durable.RunID is a kernel.RunID wherever it appears.
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
// This package is the handler contract. The rest of the module serves
// the other audiences:
//
//   - engine runs pipelines: New, the With* options, Start and Stop,
//     Bind, Pipeline.Schedule, the Run handle, Result and Status. It is
//     the wiring side of an application.
//   - pipelinedef is the type-erased pipeline description generated
//     code builds and Engine.Bind validates; hand-rolled definitions use
//     it too. Application code does not import it.
//   - kernel is the shared vocabulary — identities, phases, outcomes,
//     parks, failure records — that every other package builds on. This
//     package aliases all of it, so user code never imports kernel.
//   - observe holds the lifecycle event surface — Observer and its
//     typed events — for telemetry-adapter authors; contrib/durableotel
//     is the packaged adapter most applications install instead.
//   - store opens a store from a URI (store.Open("bbolt:///path")) via
//     drivers that register a scheme; store/bbolt is the persistent
//     driver, store/mem the in-memory one for ephemeral runs and tests.
//     store/driver is the SPI — the Store interface and the durable
//     record types — for implementers of new backends, in the spirit of
//     database/sql/driver; users never import it.
//   - durabletest provides the fake Clock and the fake Invocation for
//     engine-free tests.
//   - durablepb and cmd/protoc-gen-durable are the protobuf options and
//     the code generator behind the generated typed API.
//   - contrib/durableotel (separate module) is the OpenTelemetry
//     integration.
//
// docs/tour.md walks the features; spec/ holds the normative rules.
package durable
