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

// AwaitMode is how a multi-target park resolves: AwaitModeAll once every
// target is terminal (or missing), AwaitModeAny once the first one is.
type AwaitMode = storedriver.AwaitMode

const (
	AwaitModeAll = storedriver.AwaitModeAll
	AwaitModeAny = storedriver.AwaitModeAny
)

// Await describes a park: its mode, targets, and deadline (zero when
// none). AwaitRequest returns one for a handler's park resolution.
type Await = storedriver.Await

// Wake is the resolved memory of a park, handed to the attempt that runs
// after it: Targets is what the operation parked on, Done the Targets that
// were terminal or missing at wake time, and Expired reports that the
// park's deadline passed first. A cancellation request bypassing the park
// also produces a Wake, with Done reflecting the targets' state at that
// moment and Invocation.CancelRequested set.
type Wake = storedriver.Wake

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
