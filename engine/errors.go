package engine

import (
	"errors"
	"fmt"

	"github.com/dangra/durable"
	"github.com/dangra/durable/kernel"
)

// ErrNotStarted is returned by Schedule and other execution APIs invoked
// before Engine.Start succeeded.
var ErrNotStarted = errors.New("durable: engine not started")

// ErrStarted is returned by configuration APIs (such as Bind) invoked
// after Engine.Start.
var ErrStarted = errors.New("durable: engine already started")

// ErrRunInProgress is returned by Run.Wait when it is called from inside a
// handler (with the attempt context, or one derived from it) and the
// target Run is not yet terminal. Wait never blocks a worker: a handler
// that needs another Run's outcome parks with durable.AwaitRun instead.
var ErrRunInProgress = errors.New("durable: run still in progress; a handler must park with AwaitRun instead of blocking on Wait")

// ErrRunNotFound is returned when no Run exists for a RunID.
var ErrRunNotFound = kernel.ErrRunNotFound

// ErrRunTerminal is returned by Cancel when the Run already has a committed
// terminal outcome.
var ErrRunTerminal = kernel.ErrRunTerminal

// ScheduleConflictError is returned by Schedule when a nonterminal Run
// already occupies the resource slot: a Run of the same pipeline with
// different Input, or a Run of another pipeline in the same exclusion
// group. PipelineID identifies the blocking Run's pipeline so callers can
// route to its handle.
type ScheduleConflictError struct {
	RunID      durable.RunID
	PipelineID durable.PipelineID
}

func (e *ScheduleConflictError) Error() string {
	return fmt.Sprintf("durable: schedule conflict: active run %s of pipeline %q occupies the slot", e.RunID, e.PipelineID)
}

// PipelineMismatchError is returned when a Run is looked up through a
// Pipeline it does not belong to.
type PipelineMismatchError struct {
	RunID    durable.RunID
	Expected durable.PipelineID
	Actual   durable.PipelineID
}

func (e *PipelineMismatchError) Error() string {
	return fmt.Sprintf("durable: run %s belongs to pipeline %q, not %q", e.RunID, e.Actual, e.Expected)
}

// InvalidRunError reports that the current application deployment cannot
// safely continue a nonterminal Run. Invalidity is an operational condition,
// not a terminal Pipeline Result; a corrected deployment may make the Run
// runnable again.
type InvalidRunError struct {
	RunID      durable.RunID
	PipelineID durable.PipelineID
	Reason     string
}

func (e *InvalidRunError) Error() string {
	return fmt.Sprintf("durable: run %s (pipeline %q) is invalid for the current deployment: %s", e.RunID, e.PipelineID, e.Reason)
}
