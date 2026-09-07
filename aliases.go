package durable

import "github.com/dangra/durable/kernel"

// The identity, phase, outcome, park, and failure-record vocabulary is
// defined in kernel, the leaf package every other package in the module
// builds on, and aliased here so user code needs only this package. The
// aliases are the same types: a durable.RunID is a kernel.RunID.

// PipelineID identifies a durable Pipeline.
type PipelineID = kernel.PipelineID

// ResourceID identifies the logical resource a Run operates on.
type ResourceID = kernel.ResourceID

// RunID identifies one exact execution of one Pipeline against one resource.
type RunID = kernel.RunID

// StepID identifies durable Step semantics: forward behavior, unwind
// behavior, and Step State schema. Its ID method satisfies
// StepIdentifier, so a bare StepID is accepted wherever a generated step
// reference is.
type StepID = kernel.StepID

// Phase is the execution phase of a Run.
type Phase = kernel.Phase

const (
	PhaseForward = kernel.PhaseForward
	PhaseUnwind  = kernel.PhaseUnwind
	PhaseDone    = kernel.PhaseDone
)

// Outcome is the terminal business outcome of a Run.
type Outcome = kernel.Outcome

const (
	OutcomeSuccess = kernel.OutcomeSuccess
	OutcomeFailure = kernel.OutcomeFailure
)

// AwaitMode is how a multi-target park resolves: AwaitModeAll once every
// target is terminal (or missing), AwaitModeAny once the first one is.
type AwaitMode = kernel.AwaitMode

const (
	AwaitModeAll = kernel.AwaitModeAll
	AwaitModeAny = kernel.AwaitModeAny
)

// Await describes a park: its mode, targets, and deadline (zero when
// none). AwaitRequest returns one for a handler's park resolution.
type Await = kernel.Await

// Wake is the resolved memory of a park, handed to the attempt that runs
// after it: Targets is what the operation parked on, Done the Targets that
// were terminal or missing at wake time, and Expired reports that the
// park's deadline passed first. A cancellation request bypassing the park
// also produces a Wake, with Done reflecting the targets' state at that
// moment and Invocation.CancelRequested set.
type Wake = kernel.Wake

// FailureKind attributes a permanent failure. It is purely informational:
// the engine's scheduling, retry, and unwind behavior never depend on it.
type FailureKind = kernel.FailureKind

const (
	// FailureKindSystem attributes the failure to infrastructure or
	// environment; it is the zero value and the default.
	FailureKindSystem = kernel.FailureKindSystem
	// FailureKindUser attributes the failure to the request or intent
	// itself.
	FailureKindUser = kernel.FailureKindUser
	// FailureKindCanceled marks a Run's Failure established by
	// cancellation; it is created by the engine.
	FailureKindCanceled = kernel.FailureKindCanceled
)

// Failure is the durable representation of one permanent failure:
// execution location, attempt, phase, timestamp, message, and
// informational kind/reason attribution. The same type is a Run's
// failure (the one that ended its forward phase, read through
// Result.Failure, Status.Failure, and Invocation.Failure), an
// operation's failure, and each entry of Invocation.UnwindFailures.
type Failure = kernel.Failure
