package engine_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// Engine.GetRun finds a run by id across pipelines and hands back the
// untyped handle; Status carries the PipelineID for the typed upgrade.
func TestEngineGetRunAcrossPipelines(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	held := func(ctx context.Context, inv durable.Invocation) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defA := pipelinedef.New(pipelinedef.Config{ID: "a", Steps: []pipelinedef.Step{stateless("a/v1", held)}})
	defB := pipelinedef.New(pipelinedef.Config{ID: "b", Steps: []pipelinedef.Step{stateless("b/v1", held)}})
	e, pipes := startEngine(t, mem.New(), defA, defB)

	ra, _, err := pipes[0].Schedule(ctx, "r1", nil)
	if err != nil {
		t.Fatal(err)
	}
	rb, _, err := pipes[1].Schedule(ctx, "r1", nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []struct {
		run  engine.Run
		pipe durable.PipelineID
	}{{ra, "a"}, {rb, "b"}} {
		got, err := e.GetRun(ctx, want.run.ID())
		if err != nil {
			t.Fatalf("GetRun(%s): %v", want.run.ID(), err)
		}
		st, err := got.Status(ctx)
		if err != nil || st.PipelineID != want.pipe || st.RunID != want.run.ID() {
			t.Fatalf("Status = %+v, %v; want pipeline %q", st, err, want.pipe)
		}
	}
	if _, err := e.GetRun(ctx, "01ARZ3NDEKTSV4RRFFQ69G5FAV"); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("missing run: %v", err)
	}

	// The typed upgrade: the pipeline named by Status owns the run, the
	// other one refuses it.
	if _, err := pipes[0].GetRun(ctx, ra.ID()); err != nil {
		t.Fatalf("owning pipeline: %v", err)
	}
	var mismatch *engine.PipelineMismatchError
	if _, err := pipes[1].GetRun(ctx, ra.ID()); !errors.As(err, &mismatch) || mismatch.Actual != "a" {
		t.Fatalf("other pipeline: %v", err)
	}

	// The engine-level handle is a full handle: Wait sees the outcome.
	close(release)
	h, _ := e.GetRun(ctx, rb.ID())
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if res, err := h.Wait(wctx); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
}

// Engine.ListActiveRuns is the union of every pipeline's ListActiveRuns
// and excludes terminal runs.
func TestEngineListActiveRuns(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	held := func(ctx context.Context, inv durable.Invocation) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defA := pipelinedef.New(pipelinedef.Config{ID: "a", Steps: []pipelinedef.Step{stateless("a/v1", held)}})
	defB := pipelinedef.New(pipelinedef.Config{ID: "b", Steps: []pipelinedef.Step{stateless("b/v1", held)}})
	e, pipes := startEngine(t, mem.New(), defA, defB)

	ids := func(runs []engine.Run) []durable.RunID {
		out := make([]durable.RunID, len(runs))
		for i, r := range runs {
			out[i] = r.ID()
		}
		slices.Sort(out)
		return out
	}

	ra1, _, _ := pipes[0].Schedule(ctx, "r1", nil)
	ra2, _, _ := pipes[0].Schedule(ctx, "r2", nil)
	rb1, _, _ := pipes[1].Schedule(ctx, "r1", nil)

	all, err := e.ListActiveRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []durable.RunID{ra1.ID(), ra2.ID(), rb1.ID()}
	slices.Sort(want)
	if got := ids(all); !slices.Equal(got, want) {
		t.Fatalf("engine listing = %v, want %v", got, want)
	}
	onlyA, _ := pipes[0].ListActiveRuns(ctx)
	if len(onlyA) != 2 {
		t.Fatalf("pipeline a listing = %d runs, want 2", len(onlyA))
	}

	close(release)
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, r := range []engine.Run{ra1, ra2, rb1} {
		if _, err := r.Wait(wctx); err != nil {
			t.Fatal(err)
		}
	}
	if after, _ := e.ListActiveRuns(ctx); len(after) != 0 {
		t.Fatalf("terminal runs listed: %v", ids(after))
	}
}
