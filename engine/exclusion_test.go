package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
)

// heldDef is a one-step pipeline whose handler blocks until release is
// closed (or its attempt context dies, leaving the run nonterminal).
func heldDef(id durable.PipelineID, group string, release <-chan struct{}) *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{ID: id, ExclusionGroup: group, Steps: []pipelinedef.Step{
		stateless(durable.StepID(string(id)+"/v1"), func(ctx context.Context, inv durable.Invocation) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	}})
}

func deploy(t *testing.T, st driver.Store, defs ...*pipelinedef.Definition) (*engine.Engine, []*engine.Pipeline) {
	t.Helper()
	e := engine.New(st, fastRetry, engine.WithLogger(discardTestLogger()))
	var pipes []*engine.Pipeline
	for _, d := range defs {
		p, err := e.Bind(d)
		if err != nil {
			t.Fatal(err)
		}
		pipes = append(pipes, p)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return e, pipes
}

// Forming a group in a later deployment never touches runs in flight: two
// runs that were legal under private slots keep executing, the new
// membership blocks new admissions on the resource from the first
// Schedule on, and the overlap drains by itself.
func TestExclusionGroupFormedAcrossDeploymentsConverges(t *testing.T) {
	ctx := context.Background()
	store := mem.New()
	release := make(chan struct{})

	// Yesterday: a and b are private; both run on machine-1 at once.
	e1, p1 := deploy(t, store, heldDef("a", "", release), heldDef("b", "", release))
	ra, _, err := p1[0].Schedule(ctx, "machine-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	rb, _, err := p1[1].Schedule(ctx, "machine-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, ra, engine.RunStateRunning)
	waitForState(t, rb, engine.RunStateRunning)
	if err := e1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// Today: a, b, and a newcomer c share one group.
	e2, p2 := deploy(t, store, heldDef("a", "g", release), heldDef("b", "g", release), heldDef("c", "g", release))
	t.Cleanup(func() { e2.Stop(ctx) })
	ha, _ := e2.GetRun(ctx, ra.ID())
	hb, _ := e2.GetRun(ctx, rb.ID())
	waitForState(t, ha, engine.RunStateRunning)
	waitForState(t, hb, engine.RunStateRunning)

	// The newcomer is blocked by either in-flight run.
	var conflict *durable.ScheduleConflictError
	if _, created, err := p2[2].Schedule(ctx, "machine-1", nil); created || !errors.As(err, &conflict) {
		t.Fatalf("c.Schedule under the new group = created=%v err=%v; want a conflict", created, err)
	}
	if conflict.RunID != ra.ID() && conflict.RunID != rb.ID() {
		t.Fatalf("conflict names %s; want one of the in-flight runs", conflict.RunID)
	}
	// Another resource is untouched by the group.
	if _, created, err := p2[2].Schedule(ctx, "machine-2", nil); err != nil || !created {
		t.Fatalf("c.Schedule on a free resource = created=%v err=%v", created, err)
	}

	// The overlap drains: once both finish, the newcomer is admitted.
	close(release)
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, h := range []engine.Run{ha, hb} {
		if res, err := h.Wait(wctx); err != nil || !res.Succeeded() {
			t.Fatalf("in-flight run under new group: %+v, %v", res, err)
		}
	}
	if _, created, err := p2[2].Schedule(ctx, "machine-1", nil); err != nil || !created {
		t.Fatalf("c.Schedule after the overlap drained = created=%v err=%v", created, err)
	}
}

// Dissolving a group applies to the next admission at once: the sibling
// stops counting, while the run that held the machine keeps running.
func TestExclusionGroupDissolvedAppliesAtOnce(t *testing.T) {
	ctx := context.Background()
	store := mem.New()
	release := make(chan struct{})
	defer close(release)

	e1, p1 := deploy(t, store, heldDef("a", "g", release), heldDef("b", "g", release))
	ra, _, err := p1[0].Schedule(ctx, "machine-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, ra, engine.RunStateRunning)
	var conflict *durable.ScheduleConflictError
	if _, _, err := p1[1].Schedule(ctx, "machine-1", nil); !errors.As(err, &conflict) {
		t.Fatalf("b.Schedule inside the group = %v; want a conflict", err)
	}
	if err := e1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	e2, p2 := deploy(t, store, heldDef("a", "g", release), heldDef("b", "", release))
	t.Cleanup(func() { e2.Stop(ctx) })
	ha, _ := e2.GetRun(ctx, ra.ID())
	waitForState(t, ha, engine.RunStateRunning)
	if _, created, err := p2[1].Schedule(ctx, "machine-1", nil); err != nil || !created {
		t.Fatalf("b.Schedule after leaving the group = created=%v err=%v", created, err)
	}
	// a's own slot is still enforced for a itself.
	if _, created, _ := p2[0].Schedule(ctx, "machine-1", nil); created {
		t.Fatal("a must dedup against its own in-flight run")
	}
}

// Renaming a group is a no-op for exclusion: membership is the same set.
func TestExclusionGroupRenameChangesNothing(t *testing.T) {
	ctx := context.Background()
	store := mem.New()
	release := make(chan struct{})
	defer close(release)

	e1, p1 := deploy(t, store, heldDef("a", "old", release), heldDef("b", "old", release))
	ra, _, _ := p1[0].Schedule(ctx, "machine-1", nil)
	waitForState(t, ra, engine.RunStateRunning)
	if err := e1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	e2, p2 := deploy(t, store, heldDef("a", "new", release), heldDef("b", "new", release))
	t.Cleanup(func() { e2.Stop(ctx) })
	var conflict *durable.ScheduleConflictError
	if _, created, err := p2[1].Schedule(ctx, "machine-1", nil); created || !errors.As(err, &conflict) || conflict.RunID != ra.ID() {
		t.Fatalf("b.Schedule after the rename = created=%v err=%v; want conflict with %s", created, err, ra.ID())
	}
	if active, ok, _ := p2[0].GetActiveRun(ctx, "machine-1"); !ok || active.ID() != ra.ID() {
		t.Fatalf("GetActiveRun after the rename = %v %v", active.ID(), ok)
	}
}
