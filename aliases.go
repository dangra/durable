package durable

import "github.com/dangra/durable/storedriver"

// The identity, phase, outcome, and failure-record vocabulary is defined
// in storedriver — the store SPI builds its records from it — and
// aliased here so user code needs only this package. The aliases are the
// same types: a durable.RunID is a storedriver.RunID.

// PipelineID identifies a durable Pipeline.
type PipelineID = storedriver.PipelineID

// ResourceID identifies the logical resource a Run operates on.
type ResourceID = storedriver.ResourceID

// RunID identifies one exact execution of one Pipeline against one resource.
type RunID = storedriver.RunID

// StepID identifies durable Step semantics: forward behavior, unwind
// behavior, and Step State schema. Its ID method satisfies
// StepIdentifier, so a bare StepID is accepted wherever a generated step
// reference is.
type StepID = storedriver.StepID

// Phase is the execution phase of a Run.
type Phase = storedriver.Phase

const (
	PhaseForward = storedriver.PhaseForward
	PhaseUnwind  = storedriver.PhaseUnwind
	PhaseDone    = storedriver.PhaseDone
)

// Outcome is the terminal business outcome of a Run.
type Outcome = storedriver.Outcome

const (
	OutcomeSuccess = storedriver.OutcomeSuccess
	OutcomeFailure = storedriver.OutcomeFailure
)

// FailureKind attributes a permanent failure. It is purely informational:
// the engine's scheduling, retry, and unwind behavior never depend on it.
type FailureKind = storedriver.FailureKind

const (
	// FailureKindSystem attributes the failure to infrastructure or
	// environment; it is the zero value and the default.
	FailureKindSystem = storedriver.FailureKindSystem
	// FailureKindUser attributes the failure to the request or intent
	// itself.
	FailureKindUser = storedriver.FailureKindUser
	// FailureKindCanceled marks a RootFailure established by Run
	// cancellation; it is created by the engine.
	FailureKindCanceled = storedriver.FailureKindCanceled
)

// FailureRecord is the durable representation of one permanent operation
// failure: execution location, attempt, phase, timestamp, message, and
// informational kind/reason attribution.
type FailureRecord = storedriver.FailureRecord

// RootFailure is the permanent forward failure that established the Run's
// transition from forward execution to unwind.
type RootFailure = storedriver.RootFailure

// UnwindFailure is a permanent failure of one unwind operation. It does
// not stop the remaining unwind.
type UnwindFailure = storedriver.UnwindFailure
