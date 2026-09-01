package durable

import "time"

// FailureRecord is the durable representation of one permanent operation
// failure. Arbitrary Go error chains are intentionally flattened: only the
// execution location, attempt, phase, timestamp, and human-readable message
// are preserved.
type FailureRecord struct {
	StepID  StepID
	Phase   Phase
	Attempt uint64
	Message string
	At      time.Time
}

// RootFailure is the permanent forward failure that established the Run's
// transition from forward execution to unwind.
type RootFailure struct {
	FailureRecord
}

// UnwindFailure is a permanent failure of one unwind operation. It does not
// stop the remaining unwind.
type UnwindFailure struct {
	FailureRecord
}

// Failure is passed to Unwind handlers. UnwindFailures contains the
// permanent unwind failures accumulated so far for this Run, in unwind
// execution order. Ordinary retry errors are not included.
type Failure struct {
	Root           RootFailure
	UnwindFailures []UnwindFailure
}
