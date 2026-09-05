// Package observe holds the engine's lifecycle event surface: the
// Observer callback set, the typed events it receives, and the
// occupancy snapshot returned by durable's Engine.Stats. Its audience
// is telemetry-adapter authors — contrib/durableotel is the packaged
// OpenTelemetry adapter, and most applications install that instead of
// writing an Observer by hand. Handler authors never import this
// package; an Observer is installed through durable.WithObserver.
package observe

import (
	"time"

	"github.com/dangra/durable/kernel"
)

// Observer receives engine lifecycle events for metrics. Nil fields are
// skipped at the cost of a nil check per event. Use WithObserver
// (repeatable — observers compose) to install one.
//
// Callbacks run synchronously on engine goroutines immediately after the
// fact they report is durably committed: they must be fast and must not
// block. A callback that panics is recovered and logged at Error; it
// never affects the Run. Callbacks may call Engine.Stats.
//
// Events are in-memory facts about this process, not durable records: a
// crash between a commit and its callback loses the event, and a
// redispatched at-least-once attempt can fire again. Treat counters
// built from them as operational signal, not accounting — anything that
// must be exact reads the storedriver.Store.
type Observer struct {
	// RunScheduled fires when Schedule accepts a new Run (created=true).
	RunScheduled func(RunEvent)
	// AttemptDone fires when one operation attempt resolves (success or
	// permanent failure), schedules a retry, or parks via AwaitRun.
	AttemptDone func(AttemptEvent)
	// RunUnwinding fires when a RootFailure is established — permanent
	// forward failure or an accepted cancellation — and unwind begins.
	RunUnwinding func(RunFailureEvent)
	// RunTerminal fires when a Run commits its terminal outcome.
	RunTerminal func(RunTerminalEvent)
	// RunInvalid fires when the current deployment marks a Run invalid.
	RunInvalid func(RunFailureEvent)
	// WaiterWoken fires when a Run parked via AwaitRun resolves its park
	// because the awaited Run reached terminality or was found missing;
	// Duration spans first park to resolution. A cancellation bypass or
	// a spurious poke (the target turning invalid) emits nothing.
	WaiterWoken func(WakeEvent)
	// ClassWait fires when a Run throttled on a concurrency class is
	// granted a token, reporting how long it waited for it. A canceled
	// Run that bypasses the gate without a token emits nothing.
	ClassWait func(ClassWaitEvent)
	// RunsReaped fires after each retention sweep that deleted anything.
	RunsReaped func(count int)
	// StoreOp fires after every storedriver.Store call, reads included. Installing
	// it wraps the storedriver.Store at NewEngine time.
	StoreOp func(StoreOpEvent)
}

// RunEvent identifies a Run at acceptance. StartAt is nonzero when the
// Run was scheduled with a delayed start. Annotations is a copy shared
// by this event's observers; the engine's own state is never exposed.
type RunEvent struct {
	PipelineID  kernel.PipelineID
	ResourceID  kernel.ResourceID
	RunID       kernel.RunID
	StartAt     time.Time
	Annotations map[string]string
}

// AttemptResult classifies how an operation attempt ended.
type AttemptResult uint8

const (
	// AttemptSucceeded resolved the operation successfully.
	AttemptSucceeded AttemptResult = iota + 1
	// AttemptRetrying left the operation unresolved; a retry is
	// scheduled RetryIn from now.
	AttemptRetrying
	// AttemptFailed resolved the operation as permanently failed.
	AttemptFailed
	// AttemptAwaiting parked the operation via AwaitRun.
	AttemptAwaiting
)

func (r AttemptResult) String() string {
	switch r {
	case AttemptSucceeded:
		return "succeeded"
	case AttemptRetrying:
		return "retrying"
	case AttemptFailed:
		return "failed"
	case AttemptAwaiting:
		return "awaiting"
	default:
		return "unknown"
	}
}

// AttemptEvent reports one resolved, retried, or parked operation attempt.
type AttemptEvent struct {
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID
	RunID      kernel.RunID
	StepID     kernel.StepID
	Phase      kernel.Phase
	Attempt    uint64
	// Duration is the handler execution time of this attempt.
	Duration time.Duration
	Result   AttemptResult
	// Err is the handler error for AttemptRetrying and AttemptFailed.
	Err error
	// RetryIn is the scheduled backoff delay for AttemptRetrying.
	RetryIn time.Duration
	// Panicked marks an attempt whose handler panicked; the panic is
	// treated as an ordinary retryable error.
	Panicked bool
}

// RunFailureEvent reports a RootFailure being established (RunUnwinding)
// or a Run turning invalid for the current deployment (RunInvalid, where
// only Reason is populated besides the identity, and kernel.StepID when the
// problem is step-scoped).
type RunFailureEvent struct {
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID
	RunID      kernel.RunID
	StepID     kernel.StepID
	Kind       kernel.FailureKind
	Reason     string
	Message    string
}

// RunTerminalEvent reports a Run's terminal commit. Kind and Reason
// carry RootFailure attribution for OutcomeFailure; Duration is
// acceptance-to-terminal. Annotations is a copy shared by this event's
// observers — it carries the acceptance-time metadata (tenant tags)
// metric adapters label by; the engine's own state is never exposed.
type RunTerminalEvent struct {
	PipelineID  kernel.PipelineID
	ResourceID  kernel.ResourceID
	RunID       kernel.RunID
	Outcome     kernel.Outcome
	Kind        kernel.FailureKind
	Reason      string
	Duration    time.Duration
	Annotations map[string]string
}

// WakeEvent reports a park resolving: Duration is how long
// kernel.RunID was parked on Targets, Done lists the Targets terminal
// or missing at wake time, and Expired reports that the park's deadline
// fired before it resolved per its mode.
type WakeEvent struct {
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID
	RunID      kernel.RunID
	Targets    []kernel.RunID
	Done       []kernel.RunID
	Expired    bool
	Duration   time.Duration
}

// ClassWaitEvent reports a Run proceeding after being throttled on a
// concurrency class for Duration.
type ClassWaitEvent struct {
	PipelineID kernel.PipelineID
	ResourceID kernel.ResourceID
	RunID      kernel.RunID
	Class      string
	Duration   time.Duration
}

// StoreOpEvent reports one storedriver.Store call: Op is the method name, and Write
// marks methods that durably write. Write is declared by the engine's
// store decorator method-by-method, so a storedriver.Store method added later cannot
// reach observers unclassified — the decorator will not compile without
// an implementation stating it.
type StoreOpEvent struct {
	Op       string
	Write    bool
	Duration time.Duration
	Err      error
}

// ClassStats is the point-in-time state of one configured concurrency
// class.
type ClassStats struct {
	Capacity int
	InUse    int
	Waiting  int
}

// EngineStats is a point-in-time snapshot of engine occupancy, intended
// for poll-style gauge collection.
type EngineStats struct {
	// ActiveRuns is the number of Runs with a live worker (executing or
	// in a delayed dispatch).
	ActiveRuns int
	// AwaitingRuns is the number of Runs parked via AwaitRun.
	AwaitingRuns int
	// ThrottledRuns is the number of Runs parked on concurrency classes.
	ThrottledRuns int
	// DelayedRuns is the number of Runs waiting out a retry backoff or
	// delayed start.
	DelayedRuns int
	// InvalidRuns is the number of Runs marked invalid for the current
	// deployment.
	InvalidRuns int
	// Classes holds per-class token occupancy for classes that have
	// been used since Start.
	Classes map[string]ClassStats
}
