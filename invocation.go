package durable

import (
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable/storedriver"
)

// Invocation is the untyped core behind generated Invocation types. It is
// exported for generated code; application handlers receive the generated
// typed wrappers.
type Invocation struct {
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
	awaited         *storedriver.Wake

	baseLogger *slog.Logger

	mu           sync.Mutex
	violation    error
	scopedLogger *slog.Logger
}

// Logger returns a logger scoped to this invocation: the Engine's
// WithLogger logger with the canonical keys (pipeline, resource, run,
// step, phase, attempt) pre-attached, so handler and middleware lines
// correlate with the Engine's own lifecycle logging. It is built lazily —
// handlers that never log pay nothing — and is safe for concurrent use.
func (inv *Invocation) Logger() *slog.Logger {
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

// AwaitedRunID reports the Run an earlier attempt of this operation parked
// on via AwaitRun, once that park resolved: the Run reached terminality,
// turned out to be missing, or a cancellation request bypassed the park
// (CancelRequested is then also set). It lets a handler distinguish "woken
// after the awaited Run completed" from a first execution — the memory
// that prevents a schedule-then-await step from respawning its child on
// re-execution.
//
// The memory is durable and belongs to the operation, not to one attempt:
// it persists through ordinary-error retries and engine restarts, and is
// cleared when the operation resolves or parks again.
//
// It reports only single-target parks; a multi-target park (AwaitAll,
// AwaitAny) yields ("", false) here and is read through Awaited.
func (inv *Invocation) AwaitedRunID() (RunID, bool) {
	if inv.awaited == nil || len(inv.awaited.Targets) != 1 {
		return "", false
	}
	return inv.awaited.Targets[0], true
}

// Annotations returns a caller-owned copy of the Run's immutable
// acceptance-time annotations (trace contexts, tenant tags), nil when
// none were supplied. A tracing middleware extracts its propagation
// context here — for example a W3C traceparent injected by the
// scheduling side via WithAnnotations — and emits per-attempt spans
// linked to the originating trace.
func (inv *Invocation) Annotations() map[string]string {
	if len(inv.annotations) == 0 {
		return nil
	}
	out := make(map[string]string, len(inv.annotations))
	for k, v := range inv.annotations {
		out[k] = v
	}
	return out
}

// CancelRequested reports whether a cancellation request was pending when
// this attempt was reserved. A handler retrying a doomed operation can
// reconcile its partial effects and return Fail to resolve quickly instead
// of retrying toward a success nobody wants.
func (inv *Invocation) CancelRequested() bool { return inv.cancelRequested }

func (inv *Invocation) PipelineID() PipelineID { return inv.pipelineID }
func (inv *Invocation) ResourceID() ResourceID { return inv.resourceID }
func (inv *Invocation) RunID() RunID           { return inv.runID }
func (inv *Invocation) StepID() StepID         { return inv.stepID }

// Attempt is the durable invocation reservation number of the current
// operation, starting at 1. Reserved attempts may have crashed before
// application code began, so observed executions may have gaps.
func (inv *Invocation) Attempt() uint64 { return inv.attempt }

func (inv *Invocation) Phase() Phase { return inv.phase }

// InputMessage returns a defensive caller-owned copy of the Pipeline Input,
// or nil for an Input-less Pipeline. Generated code wraps it with a typed
// Input method.
func (inv *Invocation) InputMessage() proto.Message {
	if inv.newInput == nil {
		return nil
	}
	msg := inv.newInput()
	if err := proto.Unmarshal(inv.input, msg); err != nil {
		inv.reportViolation(&inputDecodeError{err: err})
	}
	return msg
}

func (inv *Invocation) stateBytes(id StepID) ([]byte, bool) {
	b, ok := inv.states[id]
	return b, ok
}

func (inv *Invocation) reportViolation(err error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.violation == nil {
		inv.violation = err
	}
}

func (inv *Invocation) takeViolation() error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.violation
}

// ReduceView is the untyped core behind the generated read-only Reducer
// input view on Pipeline marker types. It is exported for generated code.
type ReduceView struct {
	input    []byte
	newInput func() proto.Message
	states   map[StepID][]byte

	mu        sync.Mutex
	violation error
}

// InputMessage returns a defensive caller-owned copy of the Pipeline Input,
// or nil for an Input-less Pipeline.
func (v *ReduceView) InputMessage() proto.Message {
	if v.newInput == nil {
		return nil
	}
	msg := v.newInput()
	if err := proto.Unmarshal(v.input, msg); err != nil {
		v.reportViolation(&inputDecodeError{err: err})
	}
	return msg
}

func (v *ReduceView) stateBytes(id StepID) ([]byte, bool) {
	b, ok := v.states[id]
	return b, ok
}

func (v *ReduceView) reportViolation(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.violation == nil {
		v.violation = err
	}
}

func (v *ReduceView) takeViolation() error {
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
