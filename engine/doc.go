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
// Running an engine. New over a storedriver implementation, the With*
// Options (concurrency, classes, retry and recovery policy, retention,
// clock, logger, observer, middleware, schedule annotator, drain
// timeout), Start, Stop, and Stats.
//
// Binding pipelines. Generated code exposes a typed Definition whose Bind
// registers it with an engine before Start and returns a typed Pipeline;
// the untyped Definition, DefinitionConfig, StepConfig, and NewDefinition
// beneath it are what generated code builds and what hand-rolled
// definitions use.
//
// Scheduling and watching runs. Pipeline.Schedule with ScheduleOptions
// (WithAnnotations, StartAt, StartAfter) or an engine-wide
// ScheduleAnnotator; the Run handle (Wait, Cancel, Status, Annotations,
// InputBytes, OutputBytes); Result, Status, and RunState.
//
// Errors. ErrNotStarted, ErrStarted, ErrRunInProgress, ErrRunNotFound,
// ErrRunTerminal, ScheduleConflictError, PipelineMismatchError, and
// InvalidRunError. The causes an attempt context carries — the durable
// package's ErrEngineStopping and PreemptedError — are handler
// vocabulary and live there.
//
// Exported signatures use the durable package's aliases for the kernel
// vocabulary (durable.RunID, durable.Phase, ...), so wiring code and
// handler code name the same types the same way.
package engine
