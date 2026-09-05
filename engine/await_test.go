// Parks, the basics: the park resolution and its classification, a park
// on a running, missing, or cycle-closing target, the memory that stops
// a child respawn, cancellation cutting through, and Wait inside a
// handler. The edge cases live in await_edge_test.go.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// A handler that calls Run.Wait with its attempt context must not block a
// worker: a nonterminal target fails fast with ErrRunInProgress, while an
// already-terminal target still yields its Result.
func TestWaitInsideHandlerFailsFast(t *testing.T) {
	release := make(chan struct{})
	child := pipelinedef.New(pipelinedef.Config{
		ID: "child",
		Steps: []pipelinedef.Step{
			stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})

	var (
		childID   durable.RunID
		childPipe *engine.Pipeline
		firstErr  = make(chan error, 1)
		wokenRes  = make(chan engine.Result, 1)
	)
	parent := pipelinedef.New(pipelinedef.Config{
		ID: "parent",
		Steps: []pipelinedef.Step{
			stateless("ship/v1", func(ctx context.Context, inv durable.Invocation) error {
				if awaited, woken := inv.AwaitedRunID(); woken {
					run, err := childPipe.Run(ctx, awaited)
					if err != nil {
						return durable.Fail(err)
					}
					res, err := run.Wait(ctx)
					if err != nil {
						return durable.Fail(fmt.Errorf("wait on terminal child: %w", err))
					}
					wokenRes <- res
					return nil
				}
				run, _, err := childPipe.Schedule(ctx, "svc", nil)
				if err != nil {
					return durable.Fail(err)
				}
				childID = run.ID()
				_, err = run.Wait(ctx)
				firstErr <- err
				close(release)
				return durable.AwaitRun(run.ID())
			}),
		},
	})

	_, pipes := startEngine(t, mem.New(), child, parent)
	childPipe = pipes[0]

	run, _, err := pipes[1].Schedule(context.Background(), "train", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	select {
	case err := <-firstErr:
		if !errors.Is(err, engine.ErrRunInProgress) {
			t.Fatalf("Wait inside handler on running child = %v; want ErrRunInProgress", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler's Wait did not return: it blocked the worker")
	}

	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("parent result = %+v; want success", res)
	}
	select {
	case got := <-wokenRes:
		if !got.Succeeded() {
			t.Fatalf("Wait inside handler on terminal child = %+v; want success", got)
		}
	default:
		t.Fatalf("woken attempt did not observe child %s's result", childID)
	}
}

func TestAwaitRequest(t *testing.T) {
	const a, b = durable.RunID("01ARZ3NDEKTSV4RRFFQ69G5FAV"), durable.RunID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if got, ok := durable.AwaitRequest(durable.AwaitRun(a)); !ok || got.Mode != durable.AwaitModeAll || !slices.Equal(got.Targets, []durable.RunID{a}) || !got.Deadline.IsZero() {
		t.Fatalf("AwaitRequest(AwaitRun(%q)) = %+v, %v", a, got, ok)
	}
	// Wrapped resolutions still classify: middleware sees whatever the
	// handler stack returned. Duplicate targets collapse; the deadline is
	// set by the engine at park time, never by the resolution.
	got, ok := durable.AwaitRequest(fmt.Errorf("wrapped: %w", durable.AwaitAny([]durable.RunID{b, a, b}, durable.WithAwaitTimeout(time.Minute))))
	if !ok || got.Mode != durable.AwaitModeAny || !slices.Equal(got.Targets, []durable.RunID{b, a}) || !got.Deadline.IsZero() {
		t.Fatalf("AwaitRequest(wrapped AwaitAny) = %+v, %v", got, ok)
	}
	for _, err := range []error{nil, errors.New("boom"), durable.Fail(errors.New("boom"))} {
		if got, ok := durable.AwaitRequest(err); ok || len(got.Targets) != 0 {
			t.Fatalf("AwaitRequest(%v) = %+v, %v; want miss", err, got, ok)
		}
	}
}

// The wait-for-existing shape (flyd monitor.go: drain WAIT_FOR_INIT).
func TestAwaitRunParksUntilTargetCompletes(t *testing.T) {
	release := make(chan struct{})
	var waiterAttempts atomic.Uint64

	target := pipelinedef.New(pipelinedef.Config{
		ID: "await-target",
		Steps: []pipelinedef.Step{
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
	var waiterDef *pipelinedef.Definition
	var targetPipe *engine.Pipeline
	waiterDef = pipelinedef.New(pipelinedef.Config{
		ID: "await-waiter",
		Steps: []pipelinedef.Step{
			stateless("w/v1", func(ctx context.Context, inv durable.Invocation) error {
				waiterAttempts.Store(inv.Attempt())
				run, ok, err := targetPipe.ActiveRun(ctx, "res")
				if err != nil {
					return err
				}
				if ok {
					return durable.AwaitRun(run.ID())
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), target, waiterDef)
	targetPipe = pipes[0]

	tRun, _, err := targetPipe.Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	wRun, _, err := pipes[1].Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}

	// The waiter parks: RunStateAwaiting, pointing at the target.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := wRun.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.State == engine.RunStateAwaiting {
			if len(st.AwaitingRunIDs) != 1 || st.AwaitingRunIDs[0] != tRun.ID() {
				t.Fatalf("AwaitingRunIDs = %v, want [%s]", st.AwaitingRunIDs, tRun.ID())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiter never parked; state %v", st.State)
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	if res, err := wRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("waiter Wait = %+v, %v", res, err)
	}
	// Exactly two attempts: the parking one and the wake.
	if got := waiterAttempts.Load(); got != 2 {
		t.Fatalf("waiter attempts = %d, want 2 (park + wake, no polling)", got)
	}
}

// The create-then-wait shape (flyd runServiceReconciler): AwaitedRunID
// prevents respawning the child on re-execution.
func TestAwaitedRunIDPreventsChildRespawn(t *testing.T) {
	var sawAwaited atomic.Bool
	child := pipelinedef.New(pipelinedef.Config{
		ID: "spawn-child",
		Steps: []pipelinedef.Step{
			stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error { return nil }),
		},
	})
	var childPipe *engine.Pipeline
	parent := pipelinedef.New(pipelinedef.Config{
		ID: "spawn-parent",
		Steps: []pipelinedef.Step{
			stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
				if _, ok := inv.AwaitedRunID(); ok {
					sawAwaited.Store(true)
					return nil // child completed; do not respawn
				}
				run, _, err := childPipe.Schedule(ctx, "child-res", nil)
				if conflict, ok := errors.AsType[*durable.ScheduleConflictError](err); ok {
					return durable.AwaitRun(conflict.RunID)
				}
				if err != nil {
					return err
				}
				return durable.AwaitRun(run.ID())
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), child, parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	if !sawAwaited.Load() {
		t.Fatal("wake attempt did not observe AwaitedRunID")
	}
	// Exactly one child was ever created.
	children, err := childPipe.Runs(context.Background(), "child-res")
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("children = %d, want exactly 1 (no respawn loop)", len(children))
	}
}

func TestAwaitRunResolvesImmediatelyForMissingTarget(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "await-missing",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				if id, ok := inv.AwaitedRunID(); ok {
					if id != "no-such-run" {
						return durable.Fail(errors.New("wrong awaited id"))
					}
					return nil
				}
				return durable.AwaitRun("no-such-run")
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want immediate resolution", res, err)
	}
}

func TestAwaitCycleIsInvalid(t *testing.T) {
	store := mem.New()
	var pipeA, pipeB *engine.Pipeline
	mk := func(id durable.PipelineID, other **engine.Pipeline, otherRes durable.ResourceID) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: id,
			Steps: []pipelinedef.Step{
				stateless(durable.StepID(string(id)+"/v1"), func(ctx context.Context, inv durable.Invocation) error {
					if _, ok := inv.AwaitedRunID(); ok {
						return nil
					}
					run, ok, err := (*other).ActiveRun(ctx, otherRes)
					if err != nil {
						return err
					}
					if !ok {
						return errors.New("peer not scheduled yet") // retry
					}
					return durable.AwaitRun(run.ID())
				}),
			},
		})
	}
	defA := mk("cycle-a", &pipeB, "res-b")
	defB := mk("cycle-b", &pipeA, "res-a")
	_, pipes := startEngine(t, store, defA, defB)
	pipeA, pipeB = pipes[0], pipes[1]

	runA, _, err := pipeA.Schedule(context.Background(), "res-a", nil)
	if err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	runB, _, err := pipeB.Schedule(context.Background(), "res-b", nil)
	if err != nil {
		t.Fatalf("Schedule B: %v", err)
	}

	// One of the two must be rejected as an await cycle.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var invalid int
		for _, r := range []engine.Run{runA, runB} {
			st, err := r.Status(context.Background())
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if st.State == engine.RunStateInvalid {
				if !strings.Contains(st.InvalidReason, "await cycle") {
					t.Fatalf("InvalidReason = %q", st.InvalidReason)
				}
				invalid++
			}
		}
		if invalid > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no run was marked invalid for the await cycle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCancelCutsThroughAwait(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	target := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-await-target",
		Steps: []pipelinedef.Step{
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
	waiter := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-await-waiter",
		Steps: []pipelinedef.Step{
			stateless("w/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.CancelRequested() {
					return nil
				}
				run, ok, err := targetPipe.ActiveRun(ctx, "res")
				if err != nil {
					return err
				}
				if ok {
					return durable.AwaitRun(run.ID())
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), target, waiter)
	targetPipe = pipes[0]

	if _, _, err := targetPipe.Schedule(context.Background(), "res", nil); err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	wRun, _, err := pipes[1].Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := wRun.Status(context.Background())
		if st.State == engine.RunStateAwaiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never parked")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel the parked run: it must resolve without the target finishing.
	if err := wRun.Cancel(context.Background(), "no longer needed"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := wRun.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled while target still running", res, err)
	}
}
