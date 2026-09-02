package durable

import (
	"errors"
	"fmt"
)

// ErrEngineNotStarted is returned by Schedule and other execution APIs
// invoked before Engine.Start succeeded.
var ErrEngineNotStarted = errors.New("durable: engine not started")

// ErrEngineStarted is returned by configuration APIs (such as Bind) invoked
// after Engine.Start.
var ErrEngineStarted = errors.New("durable: engine already started")

// ErrRunNotFound is returned when no Run exists for a RunID.
var ErrRunNotFound = errors.New("durable: run not found")

// ErrRunTerminal is returned by Cancel when the Run already has a committed
// terminal outcome.
var ErrRunTerminal = errors.New("durable: run already terminal")

// ScheduleConflictError is returned by Schedule when a nonterminal Run
// already occupies the resource slot: a Run of the same pipeline with
// different Input, or a Run of another pipeline in the same exclusion
// group. PipelineID identifies the blocking Run's pipeline so callers can
// route to its handle.
type ScheduleConflictError struct {
	RunID      RunID
	PipelineID PipelineID
}

func (e *ScheduleConflictError) Error() string {
	return fmt.Sprintf("durable: schedule conflict: active run %s of pipeline %q occupies the slot", e.RunID, e.PipelineID)
}

// PipelineMismatchError is returned when a Run is looked up through a
// Pipeline it does not belong to.
type PipelineMismatchError struct {
	RunID    RunID
	Expected PipelineID
	Actual   PipelineID
}

func (e *PipelineMismatchError) Error() string {
	return fmt.Sprintf("durable: run %s belongs to pipeline %q, not %q", e.RunID, e.Actual, e.Expected)
}

// InvalidRunError reports that the current application deployment cannot
// safely continue a nonterminal Run. Invalidity is an operational condition,
// not a terminal Pipeline Result; a corrected deployment may make the Run
// runnable again.
type InvalidRunError struct {
	RunID      RunID
	PipelineID PipelineID
	Reason     string
}

func (e *InvalidRunError) Error() string {
	return fmt.Sprintf("durable: run %s (pipeline %q) is invalid for the current deployment: %s", e.RunID, e.PipelineID, e.Reason)
}
