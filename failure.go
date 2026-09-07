package durable

// Failure is what an unwinding Run is unwinding, read through
// Invocation.Failure. UnwindFailures contains the permanent unwind
// failures accumulated so far for this Run, in unwind execution order.
// Ordinary retry errors are not included.
type Failure struct {
	Root           RootFailure
	UnwindFailures []UnwindFailure
}
