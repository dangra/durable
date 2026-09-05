// Package engine runs durable pipelines: it owns the worker pool, the
// scheduler, recovery, retries, parks, cancellation, retention, and the
// store, and it is where an application wires and observes Runs.
//
// Handler code never imports this package; it programs against the
// durable package, the handler contract. This package is for the wiring
// side of an application: the main function that opens a store, binds
// the generated pipeline definitions, starts the engine, schedules Runs,
// and waits on or inspects them.
//
// # The API in four groups
//
// Running an engine. New over a store (store.Open, or a driver package's Open), the With*
// Options (concurrency, classes, retry and recovery policy, retention,
// clock, logger, observer, middleware, schedule annotator, drain
// timeout), Start, Stop, and Stats.
//
// Binding pipelines. Bind validates a pipelinedef.Definition — the
// type-erased description generated code builds — registers it before
// Start, and returns the Pipeline handle. Generated code wraps both in
// typed Definition and Pipeline types; hand-rolled definitions call Bind
// directly. Bind is the single validator: a malformed definition is an
// error here, never a panic at construction.
//
// Scheduling and watching runs. Pipeline.Schedule, taking the durable
// package's ScheduleOptions (handlers schedule children, so the options
// are handler vocabulary) or seeded by an engine-wide ScheduleAnnotator;
// the Run handle (Wait, Cancel, Status, Annotations, InputBytes,
// OutputBytes); Result, Status, and RunState.
//
// Errors. ErrNotStarted, ErrStarted, ErrRunInProgress,
// PipelineMismatchError, and InvalidRunError. What a handler may meet
// when scheduling or looking up a child — ScheduleConflictError,
// ErrRunNotFound, ErrRunTerminal — and the causes an attempt context
// carries — ErrEngineStopping and PreemptedError — are the durable
// package's.
//
// Exported signatures use the durable package's aliases for the kernel
// vocabulary (durable.RunID, durable.Phase, ...), so wiring code and
// handler code name the same types the same way.
package engine
