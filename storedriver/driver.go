// Package storedriver is durable's store SPI — the contract between the
// engine and a persistence backend, in the spirit of database/sql/driver.
// Normal users of durable never import it: they configure an engine with
// an existing implementation (bboltstore, durabletest.NewMemStore) and
// work with the durable package. Implementers of a new backend implement
// Store over the record types here; the differential fuzzer against
// durabletest's MemStore (bboltstore/fuzz_test.go) is the executable
// form of this contract.
package storedriver

import (
	"context"
	"time"

	"github.com/dangra/durable/kernel"
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

// Cursor is the per-Run scheduling state rewritten on every durable
// transition. It is deliberately small: per-attempt write volume is
// bounded by the Cursor, independent of Input and State sizes.
type Cursor struct {
	Phase kernel.Phase

	// The single in-flight operation; StepID is empty when none. Whether
	// it is a forward or unwind operation follows from Phase.
	StepID   kernel.StepID
	Attempts uint64

	// Awaiting parks the in-flight operation; nil when not parked.
	Awaiting *kernel.Await

	// Awaited is the memory of the in-flight operation's last park, once
	// resolved. It persists across the operation's retries and restarts
	// and is cleared when the operation resolves or parks again; nil when
	// no park has resolved.
	Awaited *kernel.Wake

	NextAttemptAt time.Time
	LastError     string
	LastReason    string
	LastErrorAt   time.Time
	UpdatedAt     time.Time
}

// StepWrite upserts the durable facts of one Step.
type StepWrite struct {
	StepID kernel.StepID
	Record StepRecord
}

// Transition is one atomic durable state change of a Run: the Cursor is
// always applied; the remaining fields carry the write-once or append-only
// facts the transition produced, if any.
type Transition struct {
	Cursor Cursor

	Steps []StepWrite

	// RootFailure is set at most once per Run.
	RootFailure *kernel.RootFailure
	// UnwindFailure is appended.
	UnwindFailure *kernel.UnwindFailure

	// Outcome commits terminality; Output accompanies a successful
	// outcome for Output-producing pipelines. Committing an Outcome
	// releases the Run's resource slot.
	Output  []byte
	Outcome *kernel.Outcome
}

// RunRecord is the durable representation of a Run: execution facts, not a
// materialized topology. Stores persist it opaquely.
type RunRecord struct {
	RunID      kernel.RunID
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID

	// Group is the namespaced exclusion scope this Run's slot belongs to
	// ("pipeline/<id>" or "group/<name>"), set by the engine at
	// acceptance. Stores use SlotGroup, which falls back to the
	// per-pipeline scope for records without one.
	Group string

	// Annotations are caller-supplied propagation metadata (trace
	// contexts, tenant tags), set once at acceptance and immutable for
	// the life of the Run. They are never part of duplicate-scheduling
	// identity: the active Run's annotations win on a dedup hit.
	Annotations map[string]string

	Input []byte

	Phase kernel.Phase
	Steps map[kernel.StepID]*StepRecord

	RootFailure    *kernel.RootFailure
	UnwindFailures []kernel.UnwindFailure

	Output []byte
	// Outcome is set only once the Run is terminal.
	Outcome *kernel.Outcome

	// NextAttemptAt gates execution eligibility of the next operation
	// attempt — a retry, or the first attempt of a delayed Run. It
	// survives restart.
	NextAttemptAt time.Time

	// Awaiting mirrors Cursor.Awaiting: the in-flight operation's park.
	Awaiting *kernel.Await

	// Awaited mirrors Cursor.Awaited: the resolved memory of the
	// in-flight operation's last park.
	Awaited *kernel.Wake

	// LastError, LastReason, and LastErrorAt describe the most recent
	// ordinary-error attempt of the current unresolved operation. They are
	// informational, ride the same write as NextAttemptAt, and are cleared
	// when the operation resolves.
	LastError   string
	LastReason  string
	LastErrorAt time.Time

	// Cancel is the pending cancellation request, if any. It is written
	// only through Store.RequestCancel and never cleared.
	Cancel *CancelRequest

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Terminal reports whether the Run has a committed terminal outcome.
func (r *RunRecord) Terminal() bool { return r.Outcome != nil }

// SlotGroup returns the exclusion scope of the Run's resource slot. At
// most one nonterminal Run may occupy (SlotGroup, ResourceID).
func (r *RunRecord) SlotGroup() string {
	if r.Group != "" {
		return r.Group
	}
	return "pipeline/" + string(r.PipelineID)
}

// Step returns the StepRecord for id, creating it if absent.
func (r *RunRecord) Step(id kernel.StepID) *StepRecord {
	if r.Steps == nil {
		r.Steps = make(map[kernel.StepID]*StepRecord)
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
	c.Steps = make(map[kernel.StepID]*StepRecord, len(r.Steps))
	for id, sr := range r.Steps {
		sc := *sr
		sc.State = append([]byte(nil), sr.State...)
		c.Steps[id] = &sc
	}
	c.Input = append([]byte(nil), r.Input...)
	c.Output = append([]byte(nil), r.Output...)
	if r.Annotations != nil {
		c.Annotations = make(map[string]string, len(r.Annotations))
		for k, v := range r.Annotations {
			c.Annotations[k] = v
		}
	}
	if r.RootFailure != nil {
		rf := *r.RootFailure
		c.RootFailure = &rf
	}
	c.UnwindFailures = append([]kernel.UnwindFailure(nil), r.UnwindFailures...)
	if r.Outcome != nil {
		o := *r.Outcome
		c.Outcome = &o
	}
	if r.Cancel != nil {
		cr := *r.Cancel
		c.Cancel = &cr
	}
	c.Awaiting = r.Awaiting.Clone()
	c.Awaited = r.Awaited.Clone()
	return &c
}

// Store is durable persistence for Runs. Exactly one Engine may execute
// against a Store at a time in v1; implementations SHOULD enforce or detect
// exclusive ownership where practical.
//
// Implementations must treat records as opaque values: return defensive
// copies (or decode fresh values) so callers never share mutable state with
// the store.
//
// Identifiers reaching a Store are NUL-free valid UTF-8, and free-text
// fields (messages, reasons, causes) and annotation keys and values are
// valid UTF-8 — Engine.Bind, Schedule, and the engine's recording
// sites enforce it — so implementations may use NUL as a key separator
// and protobuf string fields for text.
type Store interface {
	// CreateRun persists rec if no nonterminal Run occupies its
	// (SlotGroup, ResourceID) slot, returning (nil, true, nil).
	// rec.RunID must be fresh: the engine's ULID generation guarantees
	// it, and behavior on reusing the id of an existing (even terminal)
	// Run is undefined.
	// If a nonterminal Run occupies the slot — possibly belonging to a
	// different pipeline in the same exclusion group — it returns that
	// record with created=false and does not persist rec.
	CreateRun(ctx context.Context, rec *RunRecord) (existing *RunRecord, created bool, err error)

	// GetRun returns the record for id, or kernel.ErrRunNotFound.
	GetRun(ctx context.Context, id kernel.RunID) (*RunRecord, error)

	// ApplyTransition atomically applies one durable state change to the
	// Run: the Cursor is written, step facts are upserted, failures
	// recorded, and a Transition carrying an Outcome commits terminality
	// and releases the resource slot. A missing Run returns
	// kernel.ErrRunNotFound.
	ApplyTransition(ctx context.Context, id kernel.RunID, t Transition) error

	// ReapTerminal deletes up to limit Runs whose terminal outcome was
	// committed before the cutoff, returning how many were deleted. It
	// MUST never touch a nonterminal Run, regardless of age. Each Run's
	// components are removed atomically.
	ReapTerminal(ctx context.Context, before time.Time, limit int) (int, error)

	// RequestCancel durably records a cancellation request for the Run.
	// The first request wins: a later request returns accepted=false with
	// the stored request unchanged. A missing Run returns kernel.ErrRunNotFound;
	// a terminal Run returns kernel.ErrRunTerminal.
	RequestCancel(ctx context.Context, id kernel.RunID, req CancelRequest) (accepted bool, err error)

	// ListNonterminal returns all Runs without a terminal outcome.
	ListNonterminal(ctx context.Context) ([]*RunRecord, error)

	// ListRuns returns all Runs (terminal and nonterminal) for a slot,
	// oldest first.
	ListRuns(ctx context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*RunRecord, error)

	Close() error
}
