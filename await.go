package durable

import "errors"

// AwaitRun parks the current operation until the referenced Run reaches a
// terminal outcome. Like Fail, it is a resolution returned from a handler —
// not an error condition:
//
//	return durable.AwaitRun(child.ID())
//
// The operation remains unresolved (it still pins the Run), the worker is
// released immediately, and no retry attempts burn while parked. The
// moment the target terminates — or immediately, if it is already terminal
// or does not exist (e.g. reaped by retention) — the operation re-executes
// as a fresh attempt, whose Invocation reports the park target through
// AwaitedRunID so handlers can distinguish "woken after completion" from a
// first execution. That memory belongs to the operation: it survives the
// woken attempt's ordinary-error retries and engine restarts, and clears
// only when the operation resolves or parks again.
//
// Awaiting must not form a cycle; a Run whose park would close a cycle of
// awaits becomes invalid for the current deployment.
//
// Handlers cannot block on other Runs: Run.Wait called with the attempt
// context fails fast with ErrRunInProgress on a nonterminal target, since
// a handler blocking on another Run holds a worker slot and can deadlock
// the pool. AwaitRun is the mechanism for cross-run waiting.
func AwaitRun(id RunID) error {
	return &awaitResolution{target: id}
}

type awaitResolution struct {
	target RunID
}

func (e *awaitResolution) Error() string {
	return "durable: awaiting run " + string(e.target)
}

// AwaitTarget reports whether a handler's returned error is an AwaitRun
// resolution and, if so, which Run it parks on. Middleware wrapping
// handlers use it to classify the return correctly: a park is a normal
// resolution, not a failure to record on spans or error counters.
func AwaitTarget(err error) (RunID, bool) {
	if ar, ok := asAwait(err); ok {
		return ar.target, true
	}
	return "", false
}

// asAwait reports whether err is an AwaitRun resolution.
func asAwait(err error) (*awaitResolution, bool) {
	if ar, ok := errors.AsType[*awaitResolution](err); ok {
		return ar, true
	}
	return nil, false
}
