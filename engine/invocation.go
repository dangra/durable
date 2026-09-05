package engine

import (
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
)

// attemptInvocation is the engine's Invocation.
type attemptInvocation struct {
	pipelineID durable.PipelineID
	resourceID durable.ResourceID
	runID      durable.RunID
	stepID     durable.StepID
	attempt    uint64
	phase      durable.Phase

	input       []byte
	newInput    func() proto.Message
	states      map[durable.StepID][]byte
	annotations map[string]string

	cancelRequested bool
	awaited         *durable.Wake

	baseLogger *slog.Logger

	mu           sync.Mutex
	violation    error
	scopedLogger *slog.Logger
}

var _ durable.Invocation = (*attemptInvocation)(nil)

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

func (inv *attemptInvocation) AwaitedRunID() (durable.RunID, bool) {
	if inv.awaited == nil || len(inv.awaited.Targets) != 1 {
		return "", false
	}
	return inv.awaited.Targets[0], true
}

func (inv *attemptInvocation) Awaited() (w durable.Wake, ok bool) {
	if inv.awaited == nil {
		return durable.Wake{}, false
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

func (inv *attemptInvocation) CancelRequested() bool          { return inv.cancelRequested }
func (inv *attemptInvocation) PipelineID() durable.PipelineID { return inv.pipelineID }
func (inv *attemptInvocation) ResourceID() durable.ResourceID { return inv.resourceID }
func (inv *attemptInvocation) RunID() durable.RunID           { return inv.runID }
func (inv *attemptInvocation) StepID() durable.StepID         { return inv.stepID }
func (inv *attemptInvocation) Attempt() uint64                { return inv.attempt }
func (inv *attemptInvocation) Phase() durable.Phase           { return inv.phase }

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

func (inv *attemptInvocation) StateBytes(id durable.StepID) ([]byte, bool) {
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
	states   map[durable.StepID][]byte

	mu        sync.Mutex
	violation error
}

var _ durable.ReduceView = (*reduceView)(nil)

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

func (v *reduceView) StateBytes(id durable.StepID) ([]byte, bool) {
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
