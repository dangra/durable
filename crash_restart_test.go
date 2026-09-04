package durable_test

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
)

// The crash-restart model stress exercises durable's core promise: an
// engine stopped at an arbitrary moment and replaced by a fresh engine
// over the same store must still converge every Run to the outcome its
// handler script dictates, with unwind completeness, under at-least-once
// semantics. Handler behavior is a pure function of (resource, step,
// attempt) derived from the scenario seed, so re-executions after a
// crash behave consistently and the expected terminal state is
// computable without running the engine.

// scriptOf derives a step's scripted behavior from the scenario seed.
type stepScript struct {
	failUntil uint64 // transient error while attempt <= failUntil
	permanent bool   // durable.Fail instead of succeeding
	napMicros int64  // pre-result sleep, creating mid-attempt crashes
}

func scriptOf(seed uint64, resource durable.ResourceID, step durable.StepID) stepScript {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d|%s|%s", seed, resource, step)
	v := h.Sum64()
	s := stepScript{
		failUntil: v % 3,
		napMicros: int64(v % 1500),
	}
	// Roughly a quarter of resources fail permanently, always at the
	// last step so earlier steps have succeeded and must unwind.
	if v%4 == 0 && step == crashLastStep {
		s.permanent = true
	}
	return s
}

const (
	crashSteps    = 4
	crashLastStep = durable.StepID("crash-step-3/v1")
	crashRuns     = 14
)

func crashPipeline(seed uint64) *durable.Definition {
	var steps []durable.StepConfig
	for i := 0; i < crashSteps; i++ {
		id := durable.StepID(fmt.Sprintf("crash-step-%d/v1", i))
		sc := durable.StepConfig{
			ID:     id,
			Unwind: i < crashSteps-1,
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				s := scriptOf(seed, inv.ResourceID(), inv.StepID())
				select {
				case <-time.After(time.Duration(s.napMicros) * time.Microsecond):
				case <-ctx.Done():
					return nil, ctx.Err() // crashed mid-attempt: ordinary retry
				}
				if s.permanent {
					return nil, durable.Fail(errors.New("scripted permanent failure"))
				}
				if inv.Attempt() <= s.failUntil {
					return nil, errors.New("scripted transient failure")
				}
				return nil, nil
			},
		}
		if sc.Unwind {
			sc.UnwindFunc = func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
				select {
				case <-time.After(200 * time.Microsecond):
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
		}
		steps = append(steps, sc)
	}
	return durable.NewDefinition(durable.DefinitionConfig{ID: "crash", Steps: steps})
}

// waiterPipeline parks on a scripted target Run via AwaitRun, covering
// await parks and watcher recovery across restarts.
func waiterPipeline(target func(durable.ResourceID) durable.RunID) *durable.Definition {
	return durable.NewDefinition(durable.DefinitionConfig{
		ID: "crash-waiter",
		Steps: []durable.StepConfig{{
			ID: "await/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if _, ok := inv.AwaitedRunID(); ok {
					return nil, nil
				}
				return nil, durable.AwaitRun(target(inv.ResourceID()))
			},
		}},
	})
}

func TestCrashRestartConvergence(t *testing.T) {
	stores := []struct {
		name string
		open func(t *testing.T) durable.Store
	}{
		{"memstore", func(t *testing.T) durable.Store { return durabletest.NewMemStore() }},
		{"bbolt", func(t *testing.T) durable.Store {
			s, err := bboltstore.Open(filepath.Join(t.TempDir(), "crash.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		}},
	}
	seeds := []uint64{1, 7, 42}
	if testing.Short() {
		seeds = seeds[:1]
	}
	for _, st := range stores {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed=%d", st.name, seed), func(t *testing.T) {
				runCrashScenario(t, seed, st.open(t))
			})
		}
	}
}

func runCrashScenario(t *testing.T, seed uint64, store durable.Store) {
	ctx := context.Background()
	def := crashPipeline(seed)
	targetOf := map[durable.ResourceID]durable.RunID{}
	waiter := waiterPipeline(func(res durable.ResourceID) durable.RunID { return targetOf[res] })

	// boot builds a fresh engine over the shared store — a "process".
	boot := func() (*durable.Engine, *durable.Pipeline, *durable.Pipeline) {
		t.Helper()
		e := durable.NewEngine(store, fastRetry,
			durable.WithRecoveryBackoff(0), durable.WithConcurrency(8),
			durable.WithLogger(discardTestLogger()))
		crashPipe, err := def.Bind(e)
		if err != nil {
			t.Fatalf("Bind crash: %v", err)
		}
		waitPipe, err := waiter.Bind(e)
		if err != nil {
			t.Fatalf("Bind waiter: %v", err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return e, crashPipe, waitPipe
	}
	stop := func(e *durable.Engine) {
		t.Helper()
		sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := e.Stop(sctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}

	// Process one: schedule everything, some with delayed starts, then
	// crash almost immediately.
	e, crashPipe, waitPipe := boot()
	var crashIDs, waiterIDs []durable.RunID
	for i := 0; i < crashRuns; i++ {
		res := durable.ResourceID(fmt.Sprintf("res-%d", i))
		var opts []durable.ScheduleOption
		if i%5 == 0 {
			opts = append(opts, durable.StartAfter(5*time.Millisecond))
		}
		run, created, err := crashPipe.Schedule(ctx, res, nil, opts...)
		if err != nil || !created {
			t.Fatalf("Schedule crash %d: created=%v err=%v", i, created, err)
		}
		crashIDs = append(crashIDs, run.ID())
		targetOf[res] = run.ID()
	}
	for i := 0; i < crashRuns; i += 2 { // every second resource gets a waiter
		res := durable.ResourceID(fmt.Sprintf("res-%d", i))
		run, created, err := waitPipe.Schedule(ctx, res, nil)
		if err != nil || !created {
			t.Fatalf("Schedule waiter %d: created=%v err=%v", i, created, err)
		}
		waiterIDs = append(waiterIDs, run.ID())
	}

	// Crash cycles: run briefly, inject a cancellation, stop.
	canceled := map[durable.RunID]bool{}
	for round := 0; round < 4; round++ {
		time.Sleep(time.Duration(4+(seed+uint64(round))%12) * time.Millisecond)
		victim := crashIDs[(int(seed)+round*3)%len(crashIDs)]
		if run, err := crashPipe.Run(ctx, victim); err == nil {
			if err := run.Cancel(ctx, "crash-round cancel"); err == nil {
				canceled[victim] = true
			} else if !errors.Is(err, durable.ErrRunTerminal) {
				t.Fatalf("Cancel(%s): %v", victim, err)
			}
		}
		stop(e)
		e, crashPipe, waitPipe = boot()
	}

	// Final process: drain everything to terminal and check the model.
	defer stop(e)
	drain, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	results := map[durable.RunID]durable.Result{}
	for _, id := range crashIDs {
		run, err := crashPipe.Run(ctx, id)
		if err != nil {
			t.Fatalf("lookup %s: %v", id, err)
		}
		res, err := run.Wait(drain)
		if err != nil {
			t.Fatalf("Wait(%s): %v (non-convergence: run stuck or invalid)", id, err)
		}
		results[id] = res
	}
	for _, id := range waiterIDs {
		run, err := waitPipe.Run(ctx, id)
		if err != nil {
			t.Fatalf("lookup waiter %s: %v", id, err)
		}
		res, err := run.Wait(drain)
		if err != nil {
			t.Fatalf("Wait(waiter %s): %v", id, err)
		}
		if res.Outcome != durable.OutcomeSuccess && !res.Canceled() {
			t.Fatalf("waiter %s outcome = %+v, want success", id, res)
		}
	}

	// Model check per crash run, straight off the store facts.
	for i, id := range crashIDs {
		res := durable.ResourceID(fmt.Sprintf("res-%d", i))
		result := results[id]
		expectFail := scriptOf(seed, res, crashLastStep).permanent

		if result.Canceled() {
			if !canceled[id] {
				t.Fatalf("run %s reports cancellation nobody requested", id)
			}
		} else if expectFail != (result.Outcome == durable.OutcomeFailure) {
			t.Fatalf("run %s (resource %s) outcome = %v, script expects failure=%v", id, res, result.Outcome, expectFail)
		}

		rec, err := store.GetRun(ctx, id)
		if err != nil {
			t.Fatalf("GetRun(%s): %v", id, err)
		}
		for stepIdx := 0; stepIdx < crashSteps; stepIdx++ {
			stepID := durable.StepID(fmt.Sprintf("crash-step-%d/v1", stepIdx))
			sr, ok := rec.Steps[stepID]
			if !ok {
				continue
			}
			script := scriptOf(seed, res, stepID)
			unwindable := stepIdx < crashSteps-1
			switch {
			case rec.Outcome == nil:
				t.Fatalf("run %s nonterminal after Wait returned", id)
			case *rec.Outcome == durable.OutcomeSuccess:
				if sr.ForwardStatus != durable.OpSucceeded {
					t.Fatalf("run %s step %s: success run with unresolved step %+v", id, stepID, sr)
				}
				if sr.UnwindStatus != durable.OpNone {
					t.Fatalf("run %s step %s: success run has unwind facts %+v", id, stepID, sr)
				}
			default: // failure: unwind completeness
				if sr.ForwardStatus == durable.OpSucceeded && unwindable && sr.UnwindStatus != durable.OpSucceeded {
					t.Fatalf("run %s step %s: succeeded unwindable step not unwound: %+v", id, stepID, sr)
				}
			}
			// At-least-once sanity: a resolved forward op consumed at
			// least the scripted transient attempts (crashes may add
			// more, never fewer logical attempts than failures scripted).
			if sr.ForwardStatus == durable.OpSucceeded && sr.ForwardAttempts < script.failUntil+1 {
				t.Fatalf("run %s step %s: succeeded after %d attempts, script demands > %d",
					id, stepID, sr.ForwardAttempts, script.failUntil)
			}
		}

		// Status agrees with Wait.
		run, _ := crashPipe.Run(ctx, id)
		st, err := run.Status(ctx)
		if err != nil {
			t.Fatalf("Status(%s): %v", id, err)
		}
		if st.State != durable.RunStateDone || st.Outcome == nil || *st.Outcome != result.Outcome {
			t.Fatalf("run %s Status %+v disagrees with Wait outcome %v", id, st, result.Outcome)
		}
	}
}
