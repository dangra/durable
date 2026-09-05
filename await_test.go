package durable_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
)

// A handler that calls Run.Wait with its attempt context must not block a
// worker: a nonterminal target fails fast with ErrRunInProgress, while an
// already-terminal target still yields its Result.
func TestWaitInsideHandlerFailsFast(t *testing.T) {
	release := make(chan struct{})
	child := durable.NewDefinition(durable.DefinitionConfig{
		ID: "child",
		Steps: []durable.StepConfig{
			stateless("work/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
		childPipe *durable.Pipeline
		firstErr  = make(chan error, 1)
		wokenRes  = make(chan durable.Result, 1)
	)
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "parent",
		Steps: []durable.StepConfig{
			stateless("ship/v1", func(ctx context.Context, inv *durable.Invocation) error {
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

	_, pipes := startEngine(t, durabletest.NewMemStore(), child, parent)
	childPipe = pipes[0]

	run, _, err := pipes[1].Schedule(context.Background(), "train", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	select {
	case err := <-firstErr:
		if !errors.Is(err, durable.ErrRunInProgress) {
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
