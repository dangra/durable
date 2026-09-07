package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
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

// Status carries the root failure from the moment the Run starts
// unwinding, so a poller learns why a Run is unwinding without Wait, and
// keeps it on the terminal failure where it equals Result's.
func TestStatusCarriesRootFailureDuringUnwind(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "status-root",
		Steps: []pipelinedef.Step{
			{
				ID:     "a/v1",
				Unwind: true,
				Run:    func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("no capacity"), durable.WithUserKind(), durable.WithReason("capacity"))
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, err := pipes[0].Schedule(ctx, "r1", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Forward: no root failure yet.
	if st, err := run.Status(ctx); err != nil || (st.Phase == durable.PhaseForward && st.RootFailure != nil) {
		t.Fatalf("forward Status = %+v, %v; want no root failure", st, err)
	}
	// Unwinding, held in a/v1's unwind: the cause is observable now.
	deadline := time.Now().Add(5 * time.Second)
	var st engine.Status
	for {
		st, err = run.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.Phase == durable.PhaseUnwind && st.StepID == "a/v1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never observed the held unwind: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	if st.Outcome != nil || st.RootFailure == nil || st.RootFailure.StepID != "b/v1" ||
		st.RootFailure.Kind != durable.FailureKindUser || st.RootFailure.Reason != "capacity" {
		t.Fatalf("unwinding Status = %+v; want root failure b/v1 user/capacity and no outcome", st)
	}

	close(release)
	res, err := run.Wait(ctx)
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	st, err = run.Status(ctx)
	if err != nil || st.Outcome == nil || *st.Outcome != durable.OutcomeFailure || st.RootFailure == nil || *st.RootFailure != *res.RootFailure {
		t.Fatalf("terminal Status = %+v, %v; want Result's root failure", st, err)
	}
	// The copy is the caller's.
	st.RootFailure.Reason = "mutated"
	if again, _ := run.Status(ctx); again.RootFailure.Reason != "capacity" {
		t.Fatal("Status must hand out a copy of the root failure")
	}
}
