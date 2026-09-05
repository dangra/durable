package durable

import (
	"log/slog"

	"google.golang.org/protobuf/proto"
)

// Invocation is what a handler attempt runs against: the identity of the
// Run and operation, the attempt number, the immutable Input, committed
// Step State, and the durable memory of an earlier park. Generated code
// wraps it in a typed per-step Invocation; middleware and hand-rolled
// handlers use it directly.
//
// The engine implements it; application code never does. Handler unit
// tests use durabletest.NewInvocation, a fake that satisfies this
// interface without an engine or a store.
type Invocation interface {
	StateSource

	PipelineID() PipelineID
	ResourceID() ResourceID
	RunID() RunID
	StepID() StepID

	// Attempt is the durable invocation reservation number of the current
	// operation, starting at 1. Reserved attempts may have crashed before
	// application code began, so observed executions may have gaps.
	Attempt() uint64

	Phase() Phase

	// InputMessage returns a defensive caller-owned copy of the Pipeline
	// Input, or nil for an Input-less Pipeline. Generated code wraps it
	// with a typed Input method.
	InputMessage() proto.Message

	// Annotations returns a caller-owned copy of the Run's immutable
	// acceptance-time annotations (trace contexts, tenant tags), nil when
	// none were supplied. A tracing middleware extracts its propagation
	// context here — for example a W3C traceparent injected by the
	// scheduling side via WithAnnotations — and emits per-attempt spans
	// linked to the originating trace.
	Annotations() map[string]string

	// CancelRequested reports whether a cancellation request was pending
	// when this attempt was reserved. A handler retrying a doomed
	// operation can reconcile its partial effects and return Fail to
	// resolve quickly instead of retrying toward a success nobody wants.
	CancelRequested() bool

	// Awaited reports the park an earlier attempt of this operation made,
	// once it resolved: what was parked on, which of those were done at
	// wake time, and whether the deadline fired first. ok is false on a
	// first execution.
	//
	// The memory is durable and belongs to the operation, not to one
	// attempt: it persists through ordinary-error retries and engine
	// restarts, and is cleared when the operation resolves or parks
	// again. It is the memory that prevents a schedule-then-await step
	// from respawning its child on re-execution.
	Awaited() (w Wake, ok bool)

	// AwaitedRunID is Awaited for the single-target park made by AwaitRun:
	// the Run reached terminality, turned out to be missing, or a
	// cancellation request bypassed the park (CancelRequested is then also
	// set). A multi-target park (AwaitAll, AwaitAny) yields ("", false)
	// here and is read through Awaited.
	AwaitedRunID() (RunID, bool)

	// Logger returns a logger scoped to this invocation: the Engine's
	// WithLogger logger with the canonical keys (pipeline, resource, run,
	// step, phase, attempt) pre-attached, so handler and middleware lines
	// correlate with the Engine's own lifecycle logging.
	Logger() *slog.Logger
}

// ReduceView is what a Reducer folds: the immutable Input and the
// committed Step States of one Run. Generated code wraps it in the
// Pipeline marker type's typed Input and State methods.
type ReduceView interface {
	StateSource

	// InputMessage returns a defensive caller-owned copy of the Pipeline
	// Input, or nil for an Input-less Pipeline.
	InputMessage() proto.Message
}
