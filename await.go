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
// first execution.
//
// Awaiting must not form a cycle; a Run whose park would close a cycle of
// awaits becomes invalid for the current deployment.
//
// Handlers MUST NOT block on other Runs (run.Wait inside a handler can
// exhaust the worker pool and deadlock); AwaitRun is the mechanism for
// cross-run waiting.
func AwaitRun(id RunID) error {
	return &awaitResolution{target: id}
}

type awaitResolution struct {
	target RunID
}

func (e *awaitResolution) Error() string {
	return "durable: awaiting run " + string(e.target)
}

// asAwait reports whether err is an AwaitRun resolution.
func asAwait(err error) (*awaitResolution, bool) {
	if ar, ok := errors.AsType[*awaitResolution](err); ok {
		return ar, true
	}
	return nil, false
}
