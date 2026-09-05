// Definition binding: step ownership across pipelines and the Start
// freeze.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

func TestStepOwnershipIsExclusive(t *testing.T) {
	e := engine.New(durabletest.NewMemStore())
	mk := func(pipeline durable.PipelineID) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: pipeline,
			Steps: []pipelinedef.Step{
				stateless("shared/v1", func(context.Context, durable.Invocation) error { return nil }),
			},
		})
	}
	if _, err := e.Bind(mk("p1")); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if _, err := e.Bind(mk("p2")); err == nil {
		t.Fatal("second Bind sharing a step succeeded, want error")
	}
}

// TestBindAfterStartRejected pins the registration freeze that the
// engine's lock-free pipelines read rests on: once Start has run,
// binding another definition must fail with engine.ErrStarted — while
// runs are actively executing, so under the race detector this also
// covers the exact interleaving of a rejected concurrent registration
// attempt against unlocked pipeline lookups.
func TestBindAfterStartRejected(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "frozen",
		Steps: []pipelinedef.Step{
			stateless("step/v1", func(ctx context.Context, inv durable.Invocation) error { return nil }),
		},
	})
	e, pipes := startEngine(t, durabletest.NewMemStore(), def)

	latecomer := pipelinedef.New(pipelinedef.Config{
		ID: "latecomer",
		Steps: []pipelinedef.Step{
			stateless("late/v1", func(ctx context.Context, inv durable.Invocation) error { return nil }),
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("res-%d", i)), nil)
			if err != nil {
				t.Errorf("Schedule: %v", err)
				return
			}
			if _, err := run.Wait(context.Background()); err != nil {
				t.Errorf("Wait: %v", err)
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Bind(latecomer); !errors.Is(err, engine.ErrStarted) {
				t.Errorf("Bind after Start = %v, want engine.ErrStarted; the pipelines freeze is broken", err)
			}
		}()
	}
	wg.Wait()
}
