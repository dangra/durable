package durable

import (
	"sync"

	"google.golang.org/protobuf/proto"
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

	input    []byte
	newInput func() proto.Message
	states   map[StepID][]byte

	cancelRequested bool
	awaitedRunID    RunID

	mu        sync.Mutex
	violation error
}

// AwaitedRunID reports the Run the previous attempt of this operation
// parked on via AwaitRun, once that Run reached terminality (or turned out
// to be missing). It lets a handler distinguish "woken after the awaited
// Run completed" from a first execution — the memory that prevents a
// schedule-then-await step from respawning its child on re-execution.
func (inv *Invocation) AwaitedRunID() (RunID, bool) {
	return inv.awaitedRunID, inv.awaitedRunID != ""
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
