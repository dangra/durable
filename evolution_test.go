// Evolving a deployment with runs in flight: recovery across engines,
// retired and removed steps, reducer repair, and invalidity.
package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/storedriver"
	"google.golang.org/protobuf/proto"
)

func TestRecoveryResumesAcrossEngines(t *testing.T) {
	store := durabletest.NewMemStore()
	var attempts atomic.Uint64

	makeDef := func(succeed bool) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: "recoverable",
			Steps: []durable.StepConfig{
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
	e1 := durable.NewEngine(store, fastRetry)
	p1, _ := makeDef(false).Bind(e1)
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
	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
	p2, _ := makeDef(true).Bind(e2)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
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

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
			stateless("a/v1", func(context.Context, durable.Invocation) error { return nil }),
			// b/v1 removed while unresolved.
			stateless("c/v1", func(context.Context, durable.Invocation) error { return nil }),
		},
	})
	_, pipes := startEngine(t, store, def)

	run, _ := pipes[0].Run(context.Background(), runID)
	_, err := run.Wait(context.Background())
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != durable.RunStateInvalid || st.InvalidReason == "" {
		t.Fatalf("Status = %+v, want RunStateInvalid with reason", st)
	}
}

func TestInvalidReducerRepairedByRedeploy(t *testing.T) {
	store := durabletest.NewMemStore()

	makeDef := func(broken bool) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: "reduced",
			Steps: []durable.StepConfig{
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
	e1 := durable.NewEngine(store, fastRetry)
	p1, _ := makeDef(true).Bind(e1)
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	_, err = run.Wait(context.Background())
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}

	// Deployment 2: corrected reducer completes the same nonterminal Run.
	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
	p2, _ := makeDef(false).Bind(e2)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "nilstate",
		Steps: []durable.StepConfig{
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
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
}
