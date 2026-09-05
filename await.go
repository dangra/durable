package durable

import (
	"errors"
	"strings"
	"time"

	"github.com/dangra/durable/storedriver"
)

// AwaitOption configures a park; see WithAwaitTimeout.
type AwaitOption func(*awaitResolution)

// WithAwaitTimeout bounds a park: if it has not resolved per its mode
// within d of parking, the operation wakes anyway, with Wake.Expired set
// and Wake.Done listing whatever had finished. Expiry is a wake, not a
// failure — the handler decides: Fail, cancel what is pending, or park
// again to extend. The deadline is absolute once parked, so it survives
// restarts. A non-positive d means no deadline.
func WithAwaitTimeout(d time.Duration) AwaitOption {
	return func(ar *awaitResolution) {
		if d > 0 {
			ar.timeout = d
		}
	}
}

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
// as a fresh attempt, whose Invocation reports the park through Awaited
// (and, for a single-target park, AwaitedRunID) so handlers can
// distinguish "woken after completion" from a first execution. That memory
// belongs to the operation: it survives the woken attempt's ordinary-error
// retries and engine restarts, and clears only when the operation resolves
// or parks again.
//
// AwaitAll and AwaitAny park on several Runs at once. Awaiting must not
// form a cycle, whatever the mode; a Run whose park would close a cycle of
// awaits becomes invalid for the current deployment.
//
// Handlers cannot block on other Runs: Run.Wait called with the attempt
// context fails fast with ErrRunInProgress on a nonterminal target, since
// a handler blocking on another Run holds a worker slot and can deadlock
// the pool. Parking is the mechanism for cross-run waiting.
func AwaitRun(id RunID, opts ...AwaitOption) error {
	return newAwait(AwaitModeAll, []RunID{id}, opts)
}

// AwaitAll parks the current operation until every referenced Run is
// terminal (or missing). The woken attempt's Wake lists them all in Done.
// ids must be non-empty; duplicates are collapsed.
func AwaitAll(ids []RunID, opts ...AwaitOption) error {
	return newAwait(AwaitModeAll, ids, opts)
}

// AwaitAny parks the current operation until the first referenced Run is
// terminal (or missing). The woken attempt's Wake lists the Runs that
// were done at wake time — possibly several — in Done, and the handler
// typically re-parks on Wake.Pending to keep waiting for the rest. ids
// must be non-empty; duplicates are collapsed.
func AwaitAny(ids []RunID, opts ...AwaitOption) error {
	return newAwait(AwaitModeAny, ids, opts)
}

func newAwait(mode AwaitMode, ids []RunID, opts []AwaitOption) error {
	ar := &awaitResolution{park: storedriver.Await{Mode: mode, Targets: dedupeRunIDs(ids)}}
	for _, o := range opts {
		o(ar)
	}
	return ar
}

func dedupeRunIDs(ids []RunID) []RunID {
	out := make([]RunID, 0, len(ids))
	seen := make(map[RunID]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

type awaitResolution struct {
	park    storedriver.Await
	timeout time.Duration
}

func (e *awaitResolution) Error() string {
	ids := make([]string, len(e.park.Targets))
	for i, id := range e.park.Targets {
		ids[i] = string(id)
	}
	if len(ids) == 1 {
		return "durable: awaiting run " + ids[0]
	}
	return "durable: awaiting " + e.park.Mode.String() + " of runs [" + strings.Join(ids, " ") + "]"
}

// AwaitRequest reports whether a handler's returned error is a park
// resolution (AwaitRun, AwaitAll, AwaitAny) and, if so, the park it
// requests. Middleware wrapping handlers use it to classify the return
// correctly: a park is a normal resolution, not a failure to record on
// spans or error counters.
func AwaitRequest(err error) (Await, bool) {
	if ar, ok := asAwait(err); ok {
		return *ar.park.Clone(), true
	}
	return Await{}, false
}

// AwaitTarget is AwaitRequest reduced to the park's first target, for
// callers that only need to classify the return.
func AwaitTarget(err error) (RunID, bool) {
	if ar, ok := asAwait(err); ok && len(ar.park.Targets) > 0 {
		return ar.park.Targets[0], true
	}
	return "", false
}

// asAwait reports whether err is a park resolution.
func asAwait(err error) (*awaitResolution, bool) {
	if ar, ok := errors.AsType[*awaitResolution](err); ok {
		return ar, true
	}
	return nil, false
}
