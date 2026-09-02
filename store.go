package durable

import (
	"context"
	"time"
)

// OpStatus is the durable resolution status of one logical operation
// (a Step's forward execution or its unwind).
type OpStatus uint8

const (
	// OpNone means the operation never started. It is the zero value.
	OpNone OpStatus = iota
	// OpUnresolved means at least one invocation attempt was durably
	// reserved and the operation has no successful or permanent resolution.
	OpUnresolved
	OpSucceeded
	OpFailed
)

// StepRecord holds the durable execution facts of one Step for one Run.
type StepRecord struct {
	ForwardStatus   OpStatus
	ForwardAttempts uint64
	// State is the committed Step State, present only when ForwardStatus
	// is OpSucceeded and the Step is state-producing.
	State []byte

	UnwindStatus   OpStatus
	UnwindAttempts uint64
}

// CancelRequest is a durable request to cancel a Run. The first request
// wins; later requests are ignored.
type CancelRequest struct {
	Cause string
	At    time.Time
}

// RunRecord is the durable representation of a Run: execution facts, not a
// materialized topology. Stores persist it opaquely.
type RunRecord struct {
	RunID      RunID
	PipelineID PipelineID
	ResourceID ResourceID

	Input []byte

	Phase Phase
	Steps map[StepID]*StepRecord

	RootFailure    *RootFailure
	UnwindFailures []UnwindFailure

	Output []byte
	// Outcome is set only once the Run is terminal.
	Outcome *Outcome

	// NextAttemptAt gates execution eligibility of the next operation
	// attempt — a retry, or the first attempt of a delayed Run. It
	// survives restart.
	NextAttemptAt time.Time

	// LastError, LastReason, and LastErrorAt describe the most recent
	// ordinary-error attempt of the current unresolved operation. They are
	// informational, ride the same write as NextAttemptAt, and are cleared
	// when the operation resolves.
	LastError   string
	LastReason  string
	LastErrorAt time.Time

	// Cancel is the pending cancellation request, if any. It is written
	// only through Store.RequestCancel; once set it is never cleared, and
	// UpdateRun preserves a stored request even when the incoming record
	// lacks one.
	Cancel *CancelRequest

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Terminal reports whether the Run has a committed terminal outcome.
func (r *RunRecord) Terminal() bool { return r.Outcome != nil }

// Step returns the StepRecord for id, creating it if absent.
func (r *RunRecord) Step(id StepID) *StepRecord {
	if r.Steps == nil {
		r.Steps = make(map[StepID]*StepRecord)
	}
	sr, ok := r.Steps[id]
	if !ok {
		sr = &StepRecord{}
		r.Steps[id] = sr
	}
	return sr
}

// Clone returns a deep copy of the record.
func (r *RunRecord) Clone() *RunRecord {
	c := *r
	c.Steps = make(map[StepID]*StepRecord, len(r.Steps))
	for id, sr := range r.Steps {
		sc := *sr
		sc.State = append([]byte(nil), sr.State...)
		c.Steps[id] = &sc
	}
	c.Input = append([]byte(nil), r.Input...)
	c.Output = append([]byte(nil), r.Output...)
	if r.RootFailure != nil {
		rf := *r.RootFailure
		c.RootFailure = &rf
	}
	c.UnwindFailures = append([]UnwindFailure(nil), r.UnwindFailures...)
	if r.Outcome != nil {
		o := *r.Outcome
		c.Outcome = &o
	}
	if r.Cancel != nil {
		cr := *r.Cancel
		c.Cancel = &cr
	}
	return &c
}

// Store is durable persistence for Runs. Exactly one Engine may execute
// against a Store at a time in v1; implementations SHOULD enforce or detect
// exclusive ownership where practical.
//
// Implementations must treat records as opaque values: return defensive
// copies (or decode fresh values) so callers never share mutable state with
// the store.
type Store interface {
	// CreateRun persists rec if no nonterminal Run occupies its
	// (PipelineID, ResourceID) slot, returning (nil, true, nil).
	// If a nonterminal Run occupies the slot, it returns that record with
	// created=false and does not persist rec.
	CreateRun(ctx context.Context, rec *RunRecord) (existing *RunRecord, created bool, err error)

	// GetRun returns the record for id, or ErrRunNotFound.
	GetRun(ctx context.Context, id RunID) (*RunRecord, error)

	// UpdateRun atomically replaces the record for rec.RunID. A stored
	// cancellation request is monotonic: if the stored record carries one
	// and rec does not, the stored request is preserved (the engine worker
	// is the sole record writer, and a concurrent RequestCancel must not
	// be clobbered by its read-modify-write).
	UpdateRun(ctx context.Context, rec *RunRecord) error

	// RequestCancel durably records a cancellation request for the Run.
	// The first request wins: a later request returns accepted=false with
	// the stored request unchanged. A missing Run returns ErrRunNotFound;
	// a terminal Run returns ErrRunTerminal.
	RequestCancel(ctx context.Context, id RunID, req CancelRequest) (accepted bool, err error)

	// ListNonterminal returns all Runs without a terminal outcome.
	ListNonterminal(ctx context.Context) ([]*RunRecord, error)

	// ListRuns returns all Runs (terminal and nonterminal) for a slot,
	// oldest first.
	ListRuns(ctx context.Context, pipeline PipelineID, resource ResourceID) ([]*RunRecord, error)

	Close() error
}
