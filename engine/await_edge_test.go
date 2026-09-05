// Parks end to end: the AwaitRun memory under intermixed ordinary and
// permanent errors, chained and unwind-phase parks, cancellation, and
// restart; AwaitAll and AwaitAny fan-out, select loops, races, and cycles;
// WithAwaitTimeout expiry, extension, and persistence.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/storedriver"
	"google.golang.org/protobuf/proto"
)

// awaitedLog records, per attempt of one operation, what AwaitedRunID
// reported. It is the observable the edge-case tests reason about.
type awaitedLog struct {
	mu      sync.Mutex
	entries []awaitedEntry
}

type awaitedEntry struct {
	phase   durable.Phase
	attempt uint64
	awaited durable.RunID
	woken   bool
}

func (l *awaitedLog) record(inv durable.Invocation) awaitedEntry {
	id, ok := inv.AwaitedRunID()
	e := awaitedEntry{phase: inv.Phase(), attempt: inv.Attempt(), awaited: id, woken: ok}
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
	return e
}

func (l *awaitedLog) all() []awaitedEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]awaitedEntry(nil), l.entries...)
}

// The canonical schedule-then-await handler, with a hook that decides what
// the woken attempt returns. It follows the documented contract to the
// letter: "not woken" means "first execution", so it schedules the child.
func scheduleThenAwait(childPipe **engine.Pipeline, log *awaitedLog, onWoken func(e awaitedEntry) error) func(context.Context, durable.Invocation) error {
	return func(ctx context.Context, inv durable.Invocation) error {
		e := log.record(inv)
		if e.woken {
			return onWoken(e)
		}
		run, _, err := (*childPipe).Schedule(ctx, "child-res", nil)
		if conflict, ok := errors.AsType[*engine.ScheduleConflictError](err); ok {
			return durable.AwaitRun(conflict.RunID)
		}
		if err != nil {
			return err
		}
		return durable.AwaitRun(run.ID())
	}
}

// Edge case 1: the woken attempt hits an ordinary (retryable) error. The
// retry is still the continuation of the same operation, whose previous
// park has resolved — so it must still observe AwaitedRunID; otherwise a
// contract-following handler respawns the child on every transient error.
func TestAwaitWokenAttemptTransientErrorKeepsMemory(t *testing.T) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	var wokenAttempts atomic.Int32
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
				if wokenAttempts.Add(1) == 1 {
					return errors.New("transient: downstream flaked") // ordinary error → retry
				}
				return nil
			})),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), trivialChild("edge-child"), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}

	children, err := childPipe.Runs(context.Background(), "child-res")
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	entries := log.all()
	t.Logf("attempts: %+v", entries)
	if len(children) != 1 {
		t.Errorf("children = %d, want exactly 1: the retry after the woken attempt's transient error respawned the child", len(children))
	}
	for _, e := range entries[1:] {
		if !e.woken {
			t.Errorf("attempt %d lost AwaitedRunID after a transient error on the woken attempt", e.attempt)
		}
	}
}

// Edge case 2: the woken attempt fails permanently. The run unwinds; the
// root failure is attributed to the woken attempt; the child is never
// respawned; the step's unwind operation is a different operation and does
// not inherit the forward operation's await memory.
func TestAwaitWokenAttemptPermanentFailureUnwinds(t *testing.T) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	var unwindLog awaitedLog
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent-fail",
		Steps: []engine.StepConfig{
			{
				ID: "p/v1",
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
						return durable.Fail(errors.New("child outcome unacceptable"))
					})(ctx, inv)
				},
				Unwind: true,
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					unwindLog.record(inv)
					return nil
				},
			},
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), trivialChild("edge-child-fail"), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	res, err := pRun.Wait(context.Background())
	if err != nil || res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v; want failure", res, err)
	}
	if res.RootFailure == nil || res.RootFailure.StepID != "p/v1" || res.RootFailure.Attempt != 2 || res.RootFailure.Phase != durable.PhaseForward {
		t.Errorf("RootFailure = %+v; want p/v1 forward attempt 2", res.RootFailure)
	}
	children, _ := childPipe.Runs(context.Background(), "child-res")
	if len(children) != 1 {
		t.Errorf("children = %d, want 1", len(children))
	}
	t.Logf("forward attempts: %+v; unwind attempts: %+v", log.all(), unwindLog.all())
	for _, e := range unwindLog.all() {
		if e.woken {
			t.Errorf("unwind attempt %d inherited the forward operation's AwaitedRunID %s", e.attempt, e.awaited)
		}
	}
}

// Edge case 3: an ordinary error before the park. Attempt 1 retries,
// attempt 2 parks, attempt 3 is the wake: counters are continuous, the
// wake sees the memory, and the pre-park LastError is cleared on success.
func TestAwaitTransientErrorBeforePark(t *testing.T) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	var first atomic.Bool
	inner := scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error { return nil })
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent-pre",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if first.CompareAndSwap(false, true) {
					log.record(inv)
					return errors.New("transient before park")
				}
				return inner(ctx, inv)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), trivialChild("edge-child-pre"), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	entries := log.all()
	t.Logf("attempts: %+v", entries)
	if len(entries) != 3 || entries[0].woken || entries[1].woken || !entries[2].woken || entries[2].attempt != 3 {
		t.Errorf("attempt sequence = %+v; want [retry, park, wake@3]", entries)
	}
	st, _ := pRun.Status(context.Background())
	if st.LastError != "" {
		t.Errorf("LastError = %q after success; want cleared", st.LastError)
	}
	children, _ := childPipe.Runs(context.Background(), "child-res")
	if len(children) != 1 {
		t.Errorf("children = %d, want 1", len(children))
	}
}

// Edge case 4: chained awaits. The wake from child A parks again on child
// B; the second wake must report B, not A.
func TestAwaitChainedParksReportLatestTarget(t *testing.T) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	var ids sync.Map
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent-chain",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				e := log.record(inv)
				if !e.woken {
					run, _, err := childPipe.Schedule(ctx, "a", nil)
					if err != nil {
						return err
					}
					ids.Store("a", run.ID())
					return durable.AwaitRun(run.ID())
				}
				if a, _ := ids.Load("a"); e.awaited == a.(durable.RunID) {
					run, _, err := childPipe.Schedule(ctx, "b", nil)
					if err != nil {
						return err
					}
					ids.Store("b", run.ID())
					return durable.AwaitRun(run.ID())
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), trivialChild("edge-child-chain"), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	entries := log.all()
	t.Logf("attempts: %+v", entries)
	b, _ := ids.Load("b")
	if len(entries) != 3 || entries[2].awaited != b.(durable.RunID) {
		t.Errorf("attempt sequence = %+v; want third wake to report child b %s", entries, b)
	}
}

// Edge case 5: a park from an unwind handler (a cleanup child), whose wake
// hits a transient error. Same contract as forward: the retried unwind
// attempt must still see the memory.
func TestAwaitUnwindParkThenTransientError(t *testing.T) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	var wokenAttempts atomic.Int32
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent-unwind",
		Steps: []engine.StepConfig{
			{
				ID:     "cleanup-owner/v1",
				Run:    func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				Unwind: true,
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					return scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
						if wokenAttempts.Add(1) == 1 {
							return errors.New("transient during unwind")
						}
						return nil
					})(ctx, inv)
				},
			},
			stateless("boom/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("boom"))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), trivialChild("edge-child-unwind"), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	res, err := pRun.Wait(context.Background())
	if err != nil || res.Succeeded() || len(res.UnwindFailures) != 0 {
		t.Fatalf("parent Wait = %+v, %v; want clean unwind after failure", res, err)
	}
	entries := log.all()
	t.Logf("unwind attempts: %+v", entries)
	children, _ := childPipe.Runs(context.Background(), "child-res")
	if len(children) != 1 {
		t.Errorf("children = %d, want 1: the retried unwind attempt respawned the cleanup child", len(children))
	}
	for _, e := range entries[1:] {
		if !e.woken {
			t.Errorf("unwind attempt %d lost AwaitedRunID after a transient error on the woken attempt", e.attempt)
		}
	}
}

// Edge case 6: cancel while parked. The cancel bypasses the park while the
// target is still running; the bypass attempt sees both CancelRequested
// and the park target through AwaitedRunID, so it can act on the child it
// spawned (cancel it, say) instead of mistaking itself for a first
// execution.
func TestAwaitCancelBypassReportsTargetAndCancel(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	target := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-cancel-target",
		Steps: []engine.StepConfig{
			stateless("t/v1", func(ctx context.Context, inv durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	var targetPipe *engine.Pipeline
	var log awaitedLog
	var cancelSeen atomic.Bool
	waiter := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-cancel-waiter",
		Steps: []engine.StepConfig{
			stateless("w/v1", func(ctx context.Context, inv durable.Invocation) error {
				log.record(inv)
				if inv.CancelRequested() {
					cancelSeen.Store(true)
					return nil
				}
				run, _, err := targetPipe.ActiveRun(ctx, "res")
				if err != nil {
					return err
				}
				return durable.AwaitRun(run.ID())
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), target, waiter)
	targetPipe = pipes[0]
	tRun, _, err := targetPipe.Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	wRun, _, err := pipes[1].Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}
	waitForState(t, wRun, engine.RunStateAwaiting)
	if err := wRun.Cancel(context.Background(), "bye"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := wRun.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if st, _ := tRun.Status(context.Background()); st.State == engine.RunStateDone {
		t.Fatal("target finished; the test did not exercise a bypass")
	}
	if !cancelSeen.Load() {
		t.Error("bypass attempt did not see CancelRequested")
	}
	entries := log.all()
	if last := entries[len(entries)-1]; !last.woken || last.awaited != tRun.ID() {
		t.Errorf("bypass attempt = %+v; want AwaitedRunID %s", last, tRun.ID())
	}
}

// Edge case 7: engine restart while the woken attempt is in flight. The
// attempt the next engine runs is still the continuation of a resolved
// park, so it must see the memory rather than respawn the child. Run
// against both stores: the memory has to survive a real round trip.
func TestAwaitRestartDuringWokenAttemptKeepsMemory(t *testing.T) {
	t.Run("memstore", func(t *testing.T) {
		testAwaitRestartDuringWokenAttempt(t, durabletest.NewMemStore())
	})
	t.Run("bbolt", func(t *testing.T) {
		store, err := bboltstore.Open(filepath.Join(t.TempDir(), "await.db"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		testAwaitRestartDuringWokenAttempt(t, store)
	})
}

func testAwaitRestartDuringWokenAttempt(t *testing.T, store storedriver.Store) {
	var childPipe *engine.Pipeline
	var log awaitedLog
	inWoken := make(chan struct{}, 1)
	// Generation 1's woken attempt blocks until the engine stops, returning
	// the ctx error like a well-behaved handler; generation 2's completes.
	var gen atomic.Int32
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "edge-parent-restart",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				return scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
					if gen.Load() == 1 {
						inWoken <- struct{}{}
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				})(ctx, inv)
			}),
		},
	})
	boot := func() (*engine.Engine, *engine.Pipeline) {
		gen.Add(1)
		e := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0))
		cp, err := trivialChild("edge-child-restart").Bind(e)
		if err != nil {
			t.Fatalf("Bind child: %v", err)
		}
		pp, err := parent.Bind(e)
		if err != nil {
			t.Fatalf("Bind parent: %v", err)
		}
		childPipe = cp
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return e, pp
	}

	e1, pp := boot()
	pRun, _, err := pp.Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	<-inWoken
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	e2, pp2 := boot()
	t.Cleanup(func() { _ = e2.Stop(context.Background()) })
	pRun2, err := pp2.Run(context.Background(), pRun.ID())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res, err := pRun2.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait after restart = %+v, %v", res, err)
	}
	entries := log.all()
	children, _ := childPipe.Runs(context.Background(), "child-res")
	if len(children) != 1 {
		t.Errorf("children = %d, want 1: the attempt resumed after restart respawned the child (attempts %+v)", len(children), entries)
	}
	if last := entries[len(entries)-1]; !last.woken {
		t.Errorf("attempt %d resumed after restart lost AwaitedRunID (attempts %+v)", last.attempt, entries)
	}
}

// ---- Multi-target parks: AwaitAll and AwaitAny ----

// AwaitAll wakes once, when the last child is done; the woken attempt's
// ordinary error does not lose the memory, and no child is respawned.
func TestAwaitAllWakesOnceEveryTargetIsDone(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var log awaitedLog
	var wakes []durable.Wake
	var mu sync.Mutex
	var wokenAttempts atomic.Int32
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "all-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				e := log.record(inv)
				if w, ok := inv.Awaited(); ok {
					mu.Lock()
					wakes = append(wakes, w)
					mu.Unlock()
					if id, single := inv.AwaitedRunID(); single {
						return durable.Fail(fmt.Errorf("AwaitedRunID reported %s for a multi-target park", id))
					}
					if wokenAttempts.Add(1) == 1 {
						return errors.New("transient after wake")
					}
					return nil
				}
				if e.woken {
					return durable.Fail(errors.New("log and Awaited disagree"))
				}
				ids, err := scheduleChildren(ctx, childPipe, 3)
				if err != nil {
					return err
				}
				return durable.AwaitAll(ids)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("all-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	if st.AwaitMode != durable.AwaitModeAll || len(st.AwaitingRunIDs) != 3 {
		t.Fatalf("Status = %+v; want an all-of park on 3 children", st)
	}
	ids := st.AwaitingRunIDs

	// Two of three done: still parked, on the same set.
	g.open("child-0")
	g.open("child-2")
	for _, id := range []durable.RunID{ids[0], ids[2]} {
		run, _ := childPipe.Run(context.Background(), id)
		waitForState(t, run, engine.RunStateDone)
	}
	time.Sleep(20 * time.Millisecond)
	if st, _ := pRun.Status(context.Background()); st.State != engine.RunStateAwaiting {
		t.Fatalf("parent woke with a child still running: %v", st.State)
	}
	if got := log.all(); len(got) != 1 {
		t.Fatalf("attempts before the last child finished = %+v; want just the park", got)
	}

	g.open("child-1")
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(wakes) != 2 {
		t.Fatalf("woken attempts = %d (%+v); want 2: wake, retry after transient", len(wakes), wakes)
	}
	for _, w := range wakes {
		if !slices.Equal(w.Targets, ids) || len(w.Done) != 3 || w.Expired || len(w.Pending()) != 0 {
			t.Errorf("Wake = %+v; want all 3 targets done", w)
		}
	}
	children, _ := childPipe.Active(context.Background())
	if len(children) != 0 {
		t.Errorf("active children after completion = %d", len(children))
	}
	for i := 0; i < 3; i++ {
		runs, _ := childPipe.Runs(context.Background(), durable.ResourceID(fmt.Sprintf("child-%d", i)))
		if len(runs) != 1 {
			t.Errorf("child-%d has %d runs; want 1 (no respawn)", i, len(runs))
		}
	}
}

// AwaitAny as a select loop: each wake reports the children done so far,
// the handler re-parks on Pending until none remain, and Status shows the
// shrinking park.
func TestAwaitAnySelectLoopDrainsChildren(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var mu sync.Mutex
	var wakes []durable.Wake
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "any-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					mu.Lock()
					wakes = append(wakes, w)
					mu.Unlock()
					if pending := w.Pending(); len(pending) > 0 {
						return durable.AwaitAny(pending)
					}
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 3)
				if err != nil {
					return err
				}
				return durable.AwaitAny(ids)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("any-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	if st.AwaitMode != durable.AwaitModeAny || len(st.AwaitingRunIDs) != 3 {
		t.Fatalf("Status = %+v; want an any-of park on 3 children", st)
	}
	ids := st.AwaitingRunIDs

	// Finish the middle child first, then the last, then the first: each
	// wake carries exactly the one that finished, and the park shrinks.
	g.open("child-1")
	waitForAwaiting(t, pRun, []durable.RunID{ids[0], ids[2]})
	g.open("child-2")
	waitForAwaiting(t, pRun, []durable.RunID{ids[0]})
	g.open("child-0")
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	mu.Lock()
	defer mu.Unlock()
	wantDone := [][]durable.RunID{{ids[1]}, {ids[2]}, {ids[0]}}
	if len(wakes) != 3 {
		t.Fatalf("wakes = %+v; want 3", wakes)
	}
	for i, w := range wakes {
		if !slices.Equal(w.Done, wantDone[i]) || w.Expired {
			t.Errorf("wake %d = %+v; want Done %v", i, w, wantDone[i])
		}
	}
	if !slices.Equal(wakes[0].Targets, ids) || !slices.Equal(wakes[2].Targets, []durable.RunID{ids[0]}) {
		t.Errorf("wake targets did not track the re-parks: %+v", wakes)
	}
}

// AwaitAny as a race: the first child to finish wins and the handler
// cancels the rest, which resolve as canceled.
func TestAwaitAnyRaceCancelsLosers(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var winner atomic.Value
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "race-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					if len(w.Done) != 1 {
						return durable.Fail(fmt.Errorf("Done = %v; want exactly the winner", w.Done))
					}
					winner.Store(w.Done[0])
					for _, id := range w.Pending() {
						run, err := childPipe.Run(ctx, id)
						if err != nil {
							return err
						}
						if err := run.Cancel(ctx, "lost the race"); err != nil && !errors.Is(err, engine.ErrRunTerminal) {
							return err
						}
					}
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 3)
				if err != nil {
					return err
				}
				return durable.AwaitAny(ids)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("race-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	ids := st.AwaitingRunIDs
	g.open("child-2")
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	if got := winner.Load(); got != ids[2] {
		t.Fatalf("winner = %v; want %s", got, ids[2])
	}
	for _, id := range ids[:2] {
		run, _ := childPipe.Run(context.Background(), id)
		res, err := run.Wait(context.Background())
		if err != nil || !res.Canceled() {
			t.Errorf("loser %s = %+v, %v; want canceled", id, res, err)
		}
	}
}

// A cancel bypassing an all-of park reports which targets had finished.
func TestAwaitAllCancelBypassReportsDone(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var bypass atomic.Pointer[durable.Wake]
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "bypass-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.CancelRequested() {
					if w, ok := inv.Awaited(); ok {
						bypass.Store(&w)
					}
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 2)
				if err != nil {
					return err
				}
				return durable.AwaitAll(ids)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("bypass-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	ids := waitForState(t, pRun, engine.RunStateAwaiting).AwaitingRunIDs
	g.open("child-0")
	first, _ := childPipe.Run(context.Background(), ids[0])
	waitForState(t, first, engine.RunStateDone)
	if err := pRun.Cancel(context.Background(), "abandon"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("parent Wait = %+v, %v; want canceled", res, err)
	}
	w := bypass.Load()
	if w == nil {
		t.Fatal("bypass attempt saw no Wake")
	}
	if !slices.Equal(w.Targets, ids) || !slices.Equal(w.Done, ids[:1]) || !slices.Equal(w.Pending(), ids[1:]) {
		t.Fatalf("bypass Wake = %+v; want Done %v Pending %v", w, ids[:1], ids[1:])
	}
	g.open("child-1")
}

// A cycle through an any-of edge is refused like any other: A awaits any
// of {B, C} and B awaits A, even though C could have let A escape.
func TestAwaitAnyCycleIsInvalid(t *testing.T) {
	var g gates
	defer g.open("c")
	store := durabletest.NewMemStore()
	var pipeA, pipeB, pipeC *engine.Pipeline
	defA := engine.NewDefinition(engine.DefinitionConfig{
		ID: "cycle-any-a",
		Steps: []engine.StepConfig{
			stateless("a/v1", func(ctx context.Context, inv durable.Invocation) error {
				if _, ok := inv.Awaited(); ok {
					return nil
				}
				b, okB, err := pipeB.ActiveRun(ctx, "b")
				if err != nil {
					return err
				}
				c, okC, err := pipeC.ActiveRun(ctx, "c")
				if err != nil {
					return err
				}
				if !okB || !okC {
					return errors.New("peers not scheduled yet")
				}
				return durable.AwaitAny([]durable.RunID{b.ID(), c.ID()})
			}),
		},
	})
	defB := engine.NewDefinition(engine.DefinitionConfig{
		ID: "cycle-any-b",
		Steps: []engine.StepConfig{
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				if _, ok := inv.Awaited(); ok {
					return nil
				}
				a, ok, err := pipeA.ActiveRun(ctx, "a")
				if err != nil {
					return err
				}
				if !ok {
					return errors.New("peer not scheduled yet")
				}
				return durable.AwaitRun(a.ID())
			}),
		},
	})
	_, pipes := startEngine(t, store, defA, defB, gatedChild("cycle-any-c", &g))
	pipeA, pipeB, pipeC = pipes[0], pipes[1], pipes[2]

	if _, _, err := pipeC.Schedule(context.Background(), "c", nil); err != nil {
		t.Fatalf("Schedule C: %v", err)
	}
	runA, _, err := pipeA.Schedule(context.Background(), "a", nil)
	if err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	runB, _, err := pipeB.Schedule(context.Background(), "b", nil)
	if err != nil {
		t.Fatalf("Schedule B: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, r := range []engine.Run{runA, runB} {
			st, err := r.Status(context.Background())
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if st.State == engine.RunStateInvalid {
				if !strings.Contains(st.InvalidReason, "await cycle") {
					t.Fatalf("InvalidReason = %q", st.InvalidReason)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no run was marked invalid for the any-of await cycle")
		}
		time.Sleep(time.Millisecond)
	}
}

// Scheduling N children is safe against a crash halfway: the retry gets
// the same runs back from the idempotent Schedule, so the park is on
// exactly N children. (Only while they are still running — a child that
// finishes before the retry is terminal, and Schedule creates a fresh
// one; the gates stay shut until the park is in place.) Duplicate ids in
// the park collapse.
func TestAwaitAllScheduleIsIdempotentAcrossRetry(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var first atomic.Bool
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "idem-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					if len(w.Targets) != 3 {
						return durable.Fail(fmt.Errorf("targets = %v; want 3", w.Targets))
					}
					return nil
				}
				if first.CompareAndSwap(false, true) {
					if _, err := scheduleChildren(ctx, childPipe, 1); err != nil {
						return err
					}
					return errors.New("crashed after scheduling one child")
				}
				ids, err := scheduleChildren(ctx, childPipe, 3)
				if err != nil {
					return err
				}
				return durable.AwaitAll(append(ids, ids[0])) // duplicate collapses
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("idem-child", &g), parent)
	childPipe = pipes[0]
	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if ids := waitForState(t, pRun, engine.RunStateAwaiting).AwaitingRunIDs; len(ids) != 3 {
		t.Fatalf("parked on %v; want 3 distinct children", ids)
	}
	for i := 0; i < 3; i++ {
		g.open(durable.ResourceID(fmt.Sprintf("child-%d", i)))
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	for i := 0; i < 3; i++ {
		runs, _ := childPipe.Runs(context.Background(), durable.ResourceID(fmt.Sprintf("child-%d", i)))
		if len(runs) != 1 {
			t.Errorf("child-%d has %d runs; want 1", i, len(runs))
		}
	}
}

// An empty park is a handler bug, refused as an invalid run rather than
// parked forever.
func TestAwaitAllWithNoTargetsIsInvalid(t *testing.T) {
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "empty-await",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.AwaitAll(nil)
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	st := waitForState(t, run, engine.RunStateInvalid)
	if !strings.Contains(st.InvalidReason, "no targets") {
		t.Fatalf("InvalidReason = %q", st.InvalidReason)
	}
}

// ---- Deadlines: WithAwaitTimeout ----

// Expiry is a wake: the attempt runs with Expired set and Done listing
// what had finished, and the handler decides — here, a permanent failure
// with a reason.
func TestAwaitTimeoutExpiryIsAWake(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var expiredWake atomic.Pointer[durable.Wake]
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "timeout-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					if !w.Expired {
						return durable.Fail(fmt.Errorf("woke without expiry: %+v", w))
					}
					expiredWake.Store(&w)
					return durable.Fail(errors.New("children did not finish in time"), durable.WithReason("deploy-timeout"))
				}
				ids, err := scheduleChildren(ctx, childPipe, 2)
				if err != nil {
					return err
				}
				return durable.AwaitAll(ids, durable.WithAwaitTimeout(150*time.Millisecond))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("timeout-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	if st.AwaitDeadline.IsZero() {
		t.Fatalf("Status = %+v; want a deadline", st)
	}
	ids := st.AwaitingRunIDs
	g.open("child-0")
	first, _ := childPipe.Run(context.Background(), ids[0])
	waitForState(t, first, engine.RunStateDone)
	defer g.open("child-1")

	res, err := pRun.Wait(context.Background())
	if err != nil || res.Succeeded() || res.RootFailure == nil || res.RootFailure.Reason != "deploy-timeout" {
		t.Fatalf("parent Wait = %+v, %v; want failure with reason deploy-timeout", res, err)
	}
	w := expiredWake.Load()
	if w == nil || !slices.Equal(w.Targets, ids) || !slices.Equal(w.Done, ids[:1]) || !slices.Equal(w.Pending(), ids[1:]) {
		t.Fatalf("expired Wake = %+v; want Done %v Pending %v", w, ids[:1], ids[1:])
	}
}

// A handler can extend: re-park on Pending with a fresh timeout. The
// extended park wakes normally once the child finishes, and a park whose
// target finishes first never reports expiry.
func TestAwaitTimeoutExtendAndCompleteBeforeDeadline(t *testing.T) {
	var g gates
	var childPipe *engine.Pipeline
	var mu sync.Mutex
	var wakes []durable.Wake
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "extend-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					mu.Lock()
					wakes = append(wakes, w)
					mu.Unlock()
					if w.Expired {
						return durable.AwaitAll(w.Pending(), durable.WithAwaitTimeout(time.Minute))
					}
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 1)
				if err != nil {
					return err
				}
				return durable.AwaitAll(ids, durable.WithAwaitTimeout(100*time.Millisecond))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("extend-child", &g), parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	first := waitForState(t, pRun, engine.RunStateAwaiting)
	// Wait for the first park to expire and the second, longer one to be in place.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := pRun.Status(context.Background())
		if st.State == engine.RunStateAwaiting && st.AwaitDeadline.After(first.AwaitDeadline.Add(30*time.Second)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent never re-parked with the extended deadline; status %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	g.open("child-0")
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(wakes) != 2 || !wakes[0].Expired || len(wakes[0].Done) != 0 || wakes[1].Expired || len(wakes[1].Done) != 1 {
		t.Fatalf("wakes = %+v; want [expired with nothing done, completed]", wakes)
	}
}

// The deadline is absolute and persisted: a park that expires while no
// engine is running wakes expired on the next engine. Against bbolt, so
// the timestamp survives a real round trip.
func TestAwaitTimeoutSurvivesRestart(t *testing.T) {
	store, err := bboltstore.Open(filepath.Join(t.TempDir(), "deadline.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var g gates
	defer g.open("child-0")
	var childPipe *engine.Pipeline
	var wake atomic.Pointer[durable.Wake]
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "restart-deadline-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if w, ok := inv.Awaited(); ok {
					wake.Store(&w)
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 1)
				if err != nil {
					return err
				}
				return durable.AwaitRun(ids[0], durable.WithAwaitTimeout(100*time.Millisecond))
			}),
		},
	})
	boot := func() (*engine.Engine, *engine.Pipeline) {
		e := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0))
		cp, err := gatedChild("restart-deadline-child", &g).Bind(e)
		if err != nil {
			t.Fatalf("Bind child: %v", err)
		}
		pp, err := parent.Bind(e)
		if err != nil {
			t.Fatalf("Bind parent: %v", err)
		}
		childPipe = cp
		if err := e.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return e, pp
	}
	e1, pp := boot()
	pRun, _, err := pp.Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	time.Sleep(time.Until(st.AwaitDeadline) + 50*time.Millisecond)

	e2, pp2 := boot()
	t.Cleanup(func() { _ = e2.Stop(context.Background()) })
	pRun2, err := pp2.Run(context.Background(), pRun.ID())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res, err := pRun2.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait after restart = %+v, %v", res, err)
	}
	w := wake.Load()
	if w == nil || !w.Expired || len(w.Done) != 0 || !slices.Equal(w.Targets, st.AwaitingRunIDs) {
		t.Fatalf("Wake after restart = %+v; want expired with the original target pending", w)
	}
}

// A non-positive timeout is no deadline.
func TestAwaitTimeoutNonPositiveIsNoDeadline(t *testing.T) {
	var g gates
	defer g.open("child-0")
	var childPipe *engine.Pipeline
	parent := engine.NewDefinition(engine.DefinitionConfig{
		ID: "nodeadline-parent",
		Steps: []engine.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if _, ok := inv.Awaited(); ok {
					return nil
				}
				ids, err := scheduleChildren(ctx, childPipe, 1)
				if err != nil {
					return err
				}
				return durable.AwaitRun(ids[0], durable.WithAwaitTimeout(0))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), gatedChild("nodeadline-child", &g), parent)
	childPipe = pipes[0]
	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if st := waitForState(t, pRun, engine.RunStateAwaiting); !st.AwaitDeadline.IsZero() {
		t.Fatalf("Status = %+v; want no deadline", st)
	}
}

// A park with no targets cannot be requested (parkAwait refuses it), but
// a store could still hand one back. The gate marks the run invalid
// rather than parking it on nothing.
func TestAwaitEmptyParkFromStoreIsInvalid(t *testing.T) {
	store := durabletest.NewMemStore()
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "corrupt-park",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("must not run: the park is refused first"))
			}),
		},
	})
	now := time.Now()
	rec := &storedriver.RunRecord{
		RunID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", PipelineID: "corrupt-park", ResourceID: "r",
		Phase: durable.PhaseForward, Steps: map[durable.StepID]*storedriver.StepRecord{},
		CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := store.CreateRun(context.Background(), rec); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := store.ApplyTransition(context.Background(), rec.RunID, storedriver.Transition{Cursor: storedriver.Cursor{
		Phase: durable.PhaseForward, StepID: "s/v1", Attempts: 1, UpdatedAt: now,
		Awaiting: &durable.Await{Mode: durable.AwaitModeAny},
	}}); err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	_, pipes := startEngine(t, store, def)
	run, err := pipes[0].Run(context.Background(), rec.RunID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	st := waitForState(t, run, engine.RunStateInvalid)
	if !strings.Contains(st.InvalidReason, "no targets") {
		t.Fatalf("InvalidReason = %q", st.InvalidReason)
	}
}
