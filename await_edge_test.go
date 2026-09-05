// Edge cases of AwaitRun + AwaitedRunID: the park memory under intermixed
// ordinary and permanent errors, chained parks, unwind-phase parks,
// cancellation, and restart.
package durable_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
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

func (l *awaitedLog) record(inv *durable.Invocation) awaitedEntry {
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

func waitForState(t *testing.T, run durable.Run, want durable.RunState) durable.Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := run.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.State == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached %v; last state %v", run.ID(), want, st.State)
		}
		time.Sleep(time.Millisecond)
	}
}

func trivialChild(id durable.PipelineID) *durable.Definition {
	return durable.NewDefinition(durable.DefinitionConfig{
		ID: id,
		Steps: []durable.StepConfig{
			stateless("c/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})
}

// The canonical schedule-then-await handler, with a hook that decides what
// the woken attempt returns. It follows the documented contract to the
// letter: "not woken" means "first execution", so it schedules the child.
func scheduleThenAwait(childPipe **durable.Pipeline, log *awaitedLog, onWoken func(e awaitedEntry) error) func(context.Context, *durable.Invocation) error {
	return func(ctx context.Context, inv *durable.Invocation) error {
		e := log.record(inv)
		if e.woken {
			return onWoken(e)
		}
		run, _, err := (*childPipe).Schedule(ctx, "child-res", nil)
		if conflict, ok := errors.AsType[*durable.ScheduleConflictError](err); ok {
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	var wokenAttempts atomic.Int32
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent",
		Steps: []durable.StepConfig{
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	var unwindLog awaitedLog
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent-fail",
		Steps: []durable.StepConfig{
			{
				ID: "p/v1",
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
						return durable.Fail(errors.New("child outcome unacceptable"))
					})(ctx, inv)
				},
				Unwind: true,
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	var first atomic.Bool
	inner := scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error { return nil })
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent-pre",
		Steps: []durable.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	var ids sync.Map
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent-chain",
		Steps: []durable.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	var wokenAttempts atomic.Int32
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent-unwind",
		Steps: []durable.StepConfig{
			{
				ID:     "cleanup-owner/v1",
				Run:    func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) { return nil, nil },
				Unwind: true,
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return scheduleThenAwait(&childPipe, &log, func(e awaitedEntry) error {
						if wokenAttempts.Add(1) == 1 {
							return errors.New("transient during unwind")
						}
						return nil
					})(ctx, inv)
				},
			},
			stateless("boom/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-cancel-target",
		Steps: []durable.StepConfig{
			stateless("t/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	var targetPipe *durable.Pipeline
	var log awaitedLog
	var cancelSeen atomic.Bool
	waiter := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-cancel-waiter",
		Steps: []durable.StepConfig{
			stateless("w/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	waitForState(t, wRun, durable.RunStateAwaiting)
	if err := wRun.Cancel(context.Background(), "bye"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := wRun.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if st, _ := tRun.Status(context.Background()); st.State == durable.RunStateDone {
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
	var childPipe *durable.Pipeline
	var log awaitedLog
	inWoken := make(chan struct{}, 1)
	// Generation 1's woken attempt blocks until the engine stops, returning
	// the ctx error like a well-behaved handler; generation 2's completes.
	var gen atomic.Int32
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "edge-parent-restart",
		Steps: []durable.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	boot := func() (*durable.Engine, *durable.Pipeline) {
		gen.Add(1)
		e := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
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
