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
// pre-attaches the same canonical keys. Metrics and tracing hooks are
// future work; per-attempt tracing spans can be built today with
// WithMiddleware.
package durable
