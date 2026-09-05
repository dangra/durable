// Shared fixtures for the engine test suite: the fast retry policy,
// engine construction and start, step and state shorthands, gated child
// pipelines, and polling helpers.
package engine_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var fastRetry = engine.WithRetryPolicy(engine.RetryPolicy{
	Initial:    time.Millisecond,
	Max:        5 * time.Millisecond,
	Multiplier: 2,
})

func str(s string) *wrapperspb.StringValue { return wrapperspb.String(s) }

func newString() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }

// refFor builds the typed reference a generator would emit for a
// state-producing step whose state is a StringValue.
func refFor(id durable.StepID) durable.StateStepRef[*wrapperspb.StringValue] {
	return pipelinedef.StateStepRef(id, newString)
}

func stateless(id durable.StepID, run func(context.Context, durable.Invocation) error) pipelinedef.Step {
	return pipelinedef.Step{
		ID: id,
		Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
			return nil, run(ctx, inv)
		},
	}
}

func startEngine(t *testing.T, store driver.Store, defs ...*pipelinedef.Definition) (*engine.Engine, []*engine.Pipeline) {
	t.Helper()
	e := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0))
	var pipes []*engine.Pipeline
	for _, d := range defs {
		p, err := e.Bind(d)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		pipes = append(pipes, p)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e, pipes
}

// seedRun persists a nonterminal record with pre-existing execution facts,
// simulating a Run started under an earlier deployment.
func seedRun(t *testing.T, store driver.Store, pipeline durable.PipelineID, steps map[durable.StepID]*driver.StepRecord) durable.RunID {
	t.Helper()
	rec := &driver.RunRecord{
		RunID:      durable.RunID("seeded-" + t.Name()),
		PipelineID: pipeline,
		ResourceID: "seed-resource",
		Phase:      durable.PhaseForward,
		Steps:      steps,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if _, created, err := store.CreateRun(context.Background(), rec); err != nil || !created {
		t.Fatalf("seeding run: created=%v err=%v", created, err)
	}
	return rec.RunID
}

func waitForState(t *testing.T, run engine.Run, want engine.RunState) engine.Status {
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

func trivialChild(id durable.PipelineID) *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{
		ID: id,
		Steps: []pipelinedef.Step{
			stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error { return nil }),
		},
	})
}

// gates holds one release channel per child resource so a test can finish
// children in a chosen order.
type gates struct {
	mu sync.Mutex
	m  map[durable.ResourceID]chan struct{}
}

func (g *gates) get(r durable.ResourceID) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m == nil {
		g.m = make(map[durable.ResourceID]chan struct{})
	}
	ch, ok := g.m[r]
	if !ok {
		ch = make(chan struct{})
		g.m[r] = ch
	}
	return ch
}

func (g *gates) open(r durable.ResourceID) { close(g.get(r)) }

// gatedChild completes when its resource's gate opens, and resolves
// promptly on cancellation.
func gatedChild(id durable.PipelineID, g *gates) *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{
		ID: id,
		Steps: []pipelinedef.Step{
			stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.CancelRequested() {
					return nil
				}
				select {
				case <-g.get(inv.ResourceID()):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
}

// scheduleChildren schedules n children on deterministic resources, so a
// retry of the scheduling attempt gets the same runs back.
func scheduleChildren(ctx context.Context, pipe *engine.Pipeline, n int) ([]durable.RunID, error) {
	ids := make([]durable.RunID, 0, n)
	for i := 0; i < n; i++ {
		run, _, err := pipe.Schedule(ctx, durable.ResourceID(fmt.Sprintf("child-%d", i)), nil)
		if err != nil {
			return nil, err
		}
		ids = append(ids, run.ID())
	}
	return ids, nil
}

func waitForAwaiting(t *testing.T, run engine.Run, want []durable.RunID) engine.Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := run.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.State == engine.RunStateAwaiting && slices.Equal(st.AwaitingRunIDs, want) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never parked on %v; last %v awaiting %v", run.ID(), want, st.State, st.AwaitingRunIDs)
		}
		time.Sleep(time.Millisecond)
	}
}
