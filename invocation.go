package durable

import (
	"log/slog"
	"sync"

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

// attemptInvocation is the engine's Invocation.
type attemptInvocation struct {
	pipelineID PipelineID
	resourceID ResourceID
	runID      RunID
	stepID     StepID
	attempt    uint64
	phase      Phase

	input       []byte
	newInput    func() proto.Message
	states      map[StepID][]byte
	annotations map[string]string

	cancelRequested bool
	awaited         *Wake

	baseLogger *slog.Logger

	mu           sync.Mutex
	violation    error
	scopedLogger *slog.Logger
}

var _ Invocation = (*attemptInvocation)(nil)

// Logger is built lazily — handlers that never log pay nothing — and is
// safe for concurrent use.
func (inv *attemptInvocation) Logger() *slog.Logger {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.scopedLogger == nil {
		base := inv.baseLogger
		if base == nil {
			base = slog.Default()
		}
		inv.scopedLogger = base.With(
			"pipeline", string(inv.pipelineID),
			"resource", string(inv.resourceID),
			"run", string(inv.runID),
			"step", string(inv.stepID),
			"phase", inv.phase.String(),
			"attempt", inv.attempt,
		)
	}
	return inv.scopedLogger
}

func (inv *attemptInvocation) AwaitedRunID() (RunID, bool) {
	if inv.awaited == nil || len(inv.awaited.Targets) != 1 {
		return "", false
	}
	return inv.awaited.Targets[0], true
}

func (inv *attemptInvocation) Awaited() (w Wake, ok bool) {
	if inv.awaited == nil {
		return Wake{}, false
	}
	return *inv.awaited.Clone(), true
}

func (inv *attemptInvocation) Annotations() map[string]string {
	if len(inv.annotations) == 0 {
		return nil
	}
	out := make(map[string]string, len(inv.annotations))
	for k, v := range inv.annotations {
		out[k] = v
	}
	return out
}

func (inv *attemptInvocation) CancelRequested() bool  { return inv.cancelRequested }
func (inv *attemptInvocation) PipelineID() PipelineID { return inv.pipelineID }
func (inv *attemptInvocation) ResourceID() ResourceID { return inv.resourceID }
func (inv *attemptInvocation) RunID() RunID           { return inv.runID }
func (inv *attemptInvocation) StepID() StepID         { return inv.stepID }
func (inv *attemptInvocation) Attempt() uint64        { return inv.attempt }
func (inv *attemptInvocation) Phase() Phase           { return inv.phase }

func (inv *attemptInvocation) InputMessage() proto.Message {
	if inv.newInput == nil {
		return nil
	}
	msg := inv.newInput()
	if err := proto.Unmarshal(inv.input, msg); err != nil {
		inv.ReportViolation(&inputDecodeError{err: err})
	}
	return msg
}

func (inv *attemptInvocation) StateBytes(id StepID) ([]byte, bool) {
	b, ok := inv.states[id]
	return b, ok
}

func (inv *attemptInvocation) ReportViolation(err error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.violation == nil {
		inv.violation = err
	}
}

func (inv *attemptInvocation) takeViolation() error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.violation
}

// reduceView is the engine's ReduceView.
type reduceView struct {
	input    []byte
	newInput func() proto.Message
	states   map[StepID][]byte

	mu        sync.Mutex
	violation error
}

var _ ReduceView = (*reduceView)(nil)

func (v *reduceView) InputMessage() proto.Message {
	if v.newInput == nil {
		return nil
	}
	msg := v.newInput()
	if err := proto.Unmarshal(v.input, msg); err != nil {
		v.ReportViolation(&inputDecodeError{err: err})
	}
	return msg
}

func (v *reduceView) StateBytes(id StepID) ([]byte, bool) {
	b, ok := v.states[id]
	return b, ok
}

func (v *reduceView) ReportViolation(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.violation == nil {
		v.violation = err
	}
}

func (v *reduceView) takeViolation() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.violation
}

type inputDecodeError struct {
	err error
}

func (e *inputDecodeError) Error() string {
	return "cannot decode pipeline input: " + e.err.Error()
}

func (e *inputDecodeError) Unwrap() error { return e.err }
