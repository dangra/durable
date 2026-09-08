// Package driver is durable's store SPI — the contract between the
// engine and a persistence backend, in the spirit of database/sql/driver.
// Normal users of durable never import it: they open a store through
// package store (or a driver package's Open) and hand it to engine.New.
// Implementers of a new backend implement Store over the record types
// here, which are built from the kernel vocabulary, and register a URI
// scheme with store.Register; the differential fuzzer against store/mem
// (store/bbolt/fuzz_test.go) is the executable form of this contract.
package driver

import (
	"context"
	"sort"
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

// OperationRecord holds the durable facts of one operation — a Step's
// forward execution or its unwind — for one Run. Stores persist each
// operation as its own row, written when the operation resolves, so an
// unwind never rewrites the forward row's State.
type OperationRecord struct {
	Status   OpStatus
	Attempts uint64
	// State is the committed Step State: set on the forward operation of a
	// state-producing Step once it succeeded, nil otherwise.
	State []byte
	// Failure is the permanent failure that resolved the operation, set
	// exactly when Status is OpFailed.
	Failure *kernel.Failure
	// Order is the operation's resolution sequence within the Run, from 1
	// in the order operations resolved; 0 while unresolved. It is what
	// puts unwind failures in execution order without the store knowing
	// the topology.
	Order uint32
}

// StepRecord is the pair of operations of one Step for one Run.
type StepRecord struct {
	Forward OperationRecord
	Unwind  OperationRecord
}

// Op returns the half of the record for phase.
func (sr *StepRecord) Op(phase kernel.Phase) *OperationRecord {
	if phase == kernel.PhaseUnwind {
		return &sr.Unwind
	}
	return &sr.Forward
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

// OpWrite upserts the durable facts of one operation of one Step.
type OpWrite struct {
	StepID kernel.StepID
	Phase  kernel.Phase
	Record OperationRecord
}

// Transition is one atomic durable state change of a Run: the Cursor is
// always applied; the remaining fields carry the write-once facts the
// transition produced, if any.
type Transition struct {
	Cursor Cursor

	// Ops are the operations this transition resolved or reserved, each
	// written whole; a permanent failure rides in the record's Failure.
	Ops []OpWrite

	// Failure is set at most once per Run: the failure that ended the
	// forward phase. A step's own permanent failure is on its operation
	// record; a cancellation has no resolving operation, which is why the
	// run failure is a Run-level fact.
	Failure *kernel.Failure

	// Outcome commits terminality; Output accompanies it when the
	// pipeline reduces one for that outcome (the Output on success, the
	// failure Output on failure). Committing an Outcome releases the
	// Run's resource slot.
	Output  []byte
	Outcome *kernel.Outcome
}

// RunRecord is the durable representation of a Run: execution facts, not a
// materialized topology. Stores persist it opaquely.
//
// A Run has two storage stages. Nonterminal, the record is complete.
// Terminal, it is what CompactTerminal leaves: identity, Annotations,
// Phase, Outcome, Output, Failure, Cancel, CreatedAt, UpdatedAt (the
// terminality commit time), and the failed unwind operations. The
// terminality commit releases Input, every other operation record, and
// the cursor's scheduling fields, which read back as zero from then on.
// Input and Step State have been folded into the Output by the time a
// Run ends, so nothing the engine or a caller can reach through a
// terminal Run needs them, and releasing them at terminality rather than
// at retention keeps the retained history proportional to outputs.
type RunRecord struct {
	RunID      kernel.RunID
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID

	// Annotations are caller-supplied propagation metadata (trace
	// contexts, tenant tags), set once at acceptance and immutable for
	// the life of the Run. They are never part of duplicate-scheduling
	// identity: the active Run's annotations win on a dedup hit.
	Annotations map[string]string

	Input []byte

	Phase kernel.Phase
	Steps map[kernel.StepID]*StepRecord

	Failure *kernel.Failure

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

func (o OperationRecord) clone() OperationRecord {
	o.State = append([]byte(nil), o.State...)
	if o.Failure != nil {
		f := *o.Failure
		o.Failure = &f
	}
	return o
}

// Terminal reports whether the Run has a committed terminal outcome.
func (r *RunRecord) Terminal() bool { return r.Outcome != nil }

// CompactTerminal applies the terminal-stage retention rule to r in
// place: Input, the cursor's scheduling fields (NextAttemptAt,
// LastError, LastReason, LastErrorAt, Awaiting, Awaited), and every
// operation record other than the permanently failed unwinds are
// released. What remains of Steps is exactly the Run's unwind failures —
// UnwindFailures still answers — each on its own entry with the forward
// half zeroed. Stores apply it inside the terminality commit; it is
// exported so every driver compacts by one rule, which the differential
// fuzzer then pins.
func (r *RunRecord) CompactTerminal() {
	r.Input = nil
	r.NextAttemptAt, r.LastErrorAt = time.Time{}, time.Time{}
	r.LastError, r.LastReason = "", ""
	r.Awaiting, r.Awaited = nil, nil
	var kept map[kernel.StepID]*StepRecord
	for id, sr := range r.Steps {
		if sr.Unwind.Status != OpFailed {
			continue
		}
		if kept == nil {
			kept = make(map[kernel.StepID]*StepRecord)
		}
		kept[id] = &StepRecord{Unwind: sr.Unwind.clone()}
	}
	r.Steps = kept
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

// UnwindFailures returns the permanent unwind failures of the Run in
// resolution order, derived from the unwind operations' records.
func (r *RunRecord) UnwindFailures() []kernel.Failure {
	type ordered struct {
		order uint32
		f     kernel.Failure
	}
	var found []ordered
	for _, sr := range r.Steps {
		if sr.Unwind.Status == OpFailed && sr.Unwind.Failure != nil {
			found = append(found, ordered{sr.Unwind.Order, *sr.Unwind.Failure})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].order < found[j].order })
	out := make([]kernel.Failure, len(found))
	for i, o := range found {
		out[i] = o.f
	}
	return out
}

// NextOrder returns the resolution sequence number for the operation
// about to resolve: one past the highest Order recorded so far.
func (r *RunRecord) NextOrder() uint32 {
	var max uint32
	for _, sr := range r.Steps {
		max = maxU32(max, sr.Forward.Order, sr.Unwind.Order)
	}
	return max + 1
}

func maxU32(a uint32, rest ...uint32) uint32 {
	for _, v := range rest {
		if v > a {
			a = v
		}
	}
	return a
}

// Clone returns a deep copy of the record.
func (r *RunRecord) Clone() *RunRecord {
	c := *r
	c.Steps = make(map[kernel.StepID]*StepRecord, len(r.Steps))
	for id, sr := range r.Steps {
		sc := *sr
		sc.Forward = sr.Forward.clone()
		sc.Unwind = sr.Unwind.clone()
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
	if r.Failure != nil {
		rf := *r.Failure
		c.Failure = &rf
	}
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
	// CreateRun persists rec unless a nonterminal Run already occupies
	// the (PipelineID, ResourceID) slot of rec's own pipeline or of any
	// pipeline in excluding, for rec.ResourceID — returning (nil, true,
	// nil) when it persisted, or the occupying record with created=false
	// when it did not. The check and the write are one atomic step.
	// excluding is the engine's view of the pipelines sharing a mutex
	// with rec's at admission time; it is not persisted, which is what
	// lets a mutex change in a later deployment apply to new admissions
	// at once without touching Runs in flight.
	// rec.RunID must be fresh: the engine's ULID generation guarantees
	// it, and behavior on reusing the id of an existing (even terminal)
	// Run is undefined.
	CreateRun(ctx context.Context, rec *RunRecord, excluding []kernel.PipelineID) (existing *RunRecord, created bool, err error)

	// GetRun returns the record for id, or kernel.ErrRunNotFound.
	GetRun(ctx context.Context, id kernel.RunID) (*RunRecord, error)

	// ApplyTransition atomically applies one durable state change to the
	// Run: the Cursor is written, step facts are upserted, failures
	// recorded, and a Transition carrying an Outcome commits terminality
	// and releases the resource slot. The terminality commit also
	// releases the nonterminal stage — Input, Steps, and the cursor's
	// scheduling fields — in the same atomic step; the Transition's own
	// Ops are dropped with it, and only the Cursor's Phase and UpdatedAt
	// survive, as the terminal record's Phase and commit time. A missing
	// Run returns kernel.ErrRunNotFound; a terminal Run accepts no
	// further transitions and returns kernel.ErrRunTerminal.
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

	// ListRuns returns all Runs (terminal and nonterminal) of a pipeline
	// against a resource, oldest first. It is an enumeration of store
	// contents — the differential fuzzer and tests rely on it — and may
	// scan.
	ListRuns(ctx context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*RunRecord, error)

	// GetActiveRunID returns the RunID occupying the (pipeline, resource)
	// slot — the pipeline's one nonterminal Run on the resource — or
	// ok=false when the slot is free. It is an indexed read:
	// implementations answer it from the structure CreateRun consults to
	// enforce the slot, never by scanning.
	GetActiveRunID(ctx context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) (kernel.RunID, bool, error)

	Close() error
}
