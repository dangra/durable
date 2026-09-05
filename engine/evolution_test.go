// Evolving a deployment with runs in flight: recovery across engines,
// retired and removed steps, reducer repair, and invalidity.
package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/storedriver"
	"google.golang.org/protobuf/proto"
)

func TestRecoveryResumesAcrossEngines(t *testing.T) {
	store := durabletest.NewMemStore()
	var attempts atomic.Uint64

	makeDef := func(succeed bool) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: "recoverable",
			Steps: []pipelinedef.Step{
				stateless("only/v1", func(ctx context.Context, inv durable.Invocation) error {
					attempts.Store(inv.Attempt())
					if !succeed {
						return errors.New("still deploying")
					}
					return nil
				}),
			},
		})
	}

	// Deployment 1: the step never succeeds.
	e1 := engine.New(store, fastRetry)
	p1, _ := e1.Bind(makeDef(false))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("step never retried")
		}
		time.Sleep(time.Millisecond)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}
	attemptsAtShutdown := attempts.Load()

	// Deployment 2: same store, corrected handler.
	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0))
	p2, _ := e2.Bind(makeDef(true))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start2: %v", err)
	}
	defer e2.Stop(context.Background())

	run2, err := p2.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success after recovery", res, err)
	}
	if attempts.Load() <= attemptsAtShutdown {
		t.Fatalf("attempt numbering restarted: %d <= %d", attempts.Load(), attemptsAtShutdown)
	}
}

func TestRetiredStepIsBypassed(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
	})

	var bRan, cRan atomic.Bool
	def := pipelinedef.New(pipelinedef.Config{
		ID: "evolving",
		Steps: []pipelinedef.Step{
			stateless("a/v1", func(context.Context, durable.Invocation) error { return nil }),
			{
				ID:      "b/v1",
				Retired: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					bRan.Store(true)
					return nil, nil
				},
			},
			stateless("c/v1", func(context.Context, durable.Invocation) error {
				cRan.Store(true)
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, store, def)

	run, err := pipes[0].Run(context.Background(), runID)
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if bRan.Load() {
		t.Fatal("retired never-started step executed")
	}
	if !cRan.Load() {
		t.Fatal("step after retired step did not execute")
	}
}

func TestRetiredUnresolvedStepContinues(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
		"b/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 2},
	})

	var bAttempt atomic.Uint64
	def := pipelinedef.New(pipelinedef.Config{
		ID: "evolving",
		Steps: []pipelinedef.Step{
			stateless("a/v1", func(context.Context, durable.Invocation) error { return nil }),
			{
				ID:      "b/v1",
				Retired: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					bAttempt.Store(inv.Attempt())
					return nil, nil
				},
			},
		},
	})
	_, pipes := startEngine(t, store, def)

	run, _ := pipes[0].Run(context.Background(), runID)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if got := bAttempt.Load(); got != 3 {
		t.Fatalf("retired unresolved step ran with attempt %d, want 3 (continuing prior attempts)", got)
	}
}

func TestUnresolvedStepRemovedIsInvalid(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
		"b/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 1},
	})

	def := pipelinedef.New(pipelinedef.Config{
		ID: "evolving",
		Steps: []pipelinedef.Step{
			stateless("a/v1", func(context.Context, durable.Invocation) error { return nil }),
			// b/v1 removed while unresolved.
			stateless("c/v1", func(context.Context, durable.Invocation) error { return nil }),
		},
	})
	_, pipes := startEngine(t, store, def)

	run, _ := pipes[0].Run(context.Background(), runID)
	_, err := run.Wait(context.Background())
	if _, ok := errors.AsType[*engine.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != engine.RunStateInvalid || st.InvalidReason == "" {
		t.Fatalf("Status = %+v, want RunStateInvalid with reason", st)
	}
}

func TestInvalidReducerRepairedByRedeploy(t *testing.T) {
	store := durabletest.NewMemStore()

	makeDef := func(broken bool) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: "reduced",
			Steps: []pipelinedef.Step{
				stateless("s/v1", func(context.Context, durable.Invocation) error { return nil }),
			},
			Reduce: func(v durable.ReduceView) proto.Message {
				if broken {
					panic("bad reducer")
				}
				return str("ok")
			},
		})
	}

	// Deployment 1: broken reducer invalidates the Run without retry loops.
	e1 := engine.New(store, fastRetry)
	p1, _ := e1.Bind(makeDef(true))
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	_, err = run.Wait(context.Background())
	if _, ok := errors.AsType[*engine.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}

	// Deployment 2: corrected reducer completes the same nonterminal Run.
	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0))
	p2, _ := e2.Bind(makeDef(false))
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start2: %v", err)
	}
	defer e2.Stop(context.Background())

	run2, err := p2.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait after repair = %+v, %v; want success", res, err)
	}
}

func TestNilStateFromStateProducingHandlerIsInvalid(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "nilstate",
		Steps: []pipelinedef.Step{
			{
				ID:       "s/v1",
				HasState: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
			},
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	_, err := run.Wait(context.Background())
	if _, ok := errors.AsType[*engine.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
}
