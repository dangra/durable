// Cancellation: what a request does to a scheduled, dormant, parked,
// running, reducing, unwinding, and terminal Run, and how it reaches
// the Run's children.
package engine_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// A scheduled Run never attempted anything: its first step is resolved
// as canceled with attempt 0 and the slot frees at once.
func TestCancelScheduledRunFreesSlot(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-scheduled",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				t.Error("step of canceled scheduled run executed")
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	p := pipes[0]

	run, _, err := p.Schedule(context.Background(), "r", nil, durable.StartAfter(time.Hour))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := run.Cancel(context.Background(), "operator retracted"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Failed() || !res.Canceled() {
		t.Fatalf("result = %+v, want canceled failure", res)
	}
	if f := res.Failure; f.Kind != durable.FailureKindCanceled || f.Message != "operator retracted" || f.StepID != "s/v1" || f.Attempt != 0 {
		t.Fatalf("Failure = %+v; want canceled at s/v1 attempt 0", f)
	}
	// The slot is free again immediately.
	if _, created, err := p.Schedule(context.Background(), "r", nil, durable.StartAfter(time.Hour)); err != nil || !created {
		t.Fatalf("post-cancel Schedule = created=%v err=%v", created, err)
	}
}

// A running attempt's ctx dies; returning ctx.Err() resolves the step
// as canceled on that attempt. The step never completed, so it does not
// unwind; the steps before it do, and nothing after it runs.
func TestCancelCutsRunningAttempt(t *testing.T) {
	var (
		mu       sync.Mutex
		unwound  []durable.StepID
		attempts atomic.Int32
		cRan     atomic.Bool
	)
	unwind := func(ctx context.Context, inv durable.Invocation) error {
		mu.Lock()
		unwound = append(unwound, inv.StepID())
		mu.Unlock()
		return nil
	}
	blocked := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-midflight",
		Steps: []pipelinedef.Step{
			{
				ID:         "a/v1",
				Unwind:     true,
				Run:        func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: unwind,
			},
			{
				ID:     "b/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					attempts.Add(1)
					close(blocked)
					<-ctx.Done()
					return nil, ctx.Err()
				},
				UnwindFunc: unwind,
			},
			stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error {
				cRan.Store(true)
				return nil
			}),
		},
	})
	store := mem.New()
	_, pipes := startEngine(t, store, def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-blocked
	if err := run.Cancel(context.Background(), "changed my mind"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() {
		t.Fatalf("result = %+v, want canceled", res)
	}
	if f := res.Failure; f.StepID != "b/v1" || f.Attempt != 1 || f.Message != "changed my mind" {
		t.Fatalf("Failure = %+v; want b/v1 attempt 1 with the cause", f)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("b attempts = %d; want 1: no re-execution under a cancel", n)
	}
	if cRan.Load() {
		t.Error("new forward work selected after cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(unwound) != 1 || unwound[0] != "a/v1" {
		t.Fatalf("unwound = %v, want [a/v1]: b never completed", unwound)
	}
}

// A running attempt that still succeeds after its ctx died commits its
// step, and that step unwinds with the rest.
func TestCancelRunningAttemptSuccessCommits(t *testing.T) {
	var (
		mu      sync.Mutex
		unwound []durable.StepID
	)
	unwind := func(ctx context.Context, inv durable.Invocation) error {
		mu.Lock()
		unwound = append(unwound, inv.StepID())
		mu.Unlock()
		return nil
	}
	blocked := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-success",
		Steps: []pipelinedef.Step{
			{
				ID:         "a/v1",
				Unwind:     true,
				Run:        func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: unwind,
			},
			{
				ID:     "b/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					close(blocked)
					<-ctx.Done()
					return nil, nil // finished anyway
				},
				UnwindFunc: unwind,
			},
			stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error {
				t.Error("new forward work selected after cancel")
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-blocked
	if err := run.Cancel(context.Background(), "changed my mind"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if res.Failure.StepID != "c/v1" || res.Failure.Attempt != 0 {
		t.Fatalf("Failure = %+v; want the never-attempted c/v1", res.Failure)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(unwound) != 2 || unwound[0] != "b/v1" || unwound[1] != "a/v1" {
		t.Fatalf("unwound = %v, want [b/v1 a/v1]", unwound)
	}
}

// A running attempt that returns Fail after its ctx died is still a
// cancellation: the Run's Failure is the cancel's cause and the Fail's
// reason rides along as attribution.
func TestCancelWinsOverFail(t *testing.T) {
	blocked := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-vs-fail",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				close(blocked)
				<-ctx.Done()
				return durable.Fail(errors.New("broken anyway"), durable.WithReason("broken"))
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	<-blocked
	if err := run.Cancel(context.Background(), "stop"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if f := res.Failure; f.StepID != "s/v1" || f.Message != "stop" || f.Reason != "broken" {
		t.Fatalf("Failure = %+v; want the cancel's cause at s/v1 with reason broken", f)
	}
}

// A step waiting out a retry backoff is not re-executed to be canceled:
// the operation resolves with the attempt it had.
func TestCancelDormantRetryDoesNotReexecute(t *testing.T) {
	var attempts atomic.Int32
	failed := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-dormant",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				if attempts.Add(1) == 1 {
					close(failed)
				}
				return errors.New("flaky")
			}),
		},
	})
	store := mem.New()
	e := engine.New(store, engine.WithLogger(discardTestLogger()),
		engine.WithRetryPolicy(engine.RetryPolicy{Initial: time.Hour, Max: time.Hour, Multiplier: 1}))
	pipe, err := e.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())
	run, _, err := pipe.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-failed
	waitForState(t, run, engine.RunStateWaitingRetry)
	if err := run.Cancel(context.Background(), "enough"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if f := res.Failure; f.StepID != "s/v1" || f.Attempt != 1 {
		t.Fatalf("Failure = %+v; want s/v1 attempt 1", f)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("attempts = %d; want 1", n)
	}
	rec, err := store.GetRun(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	// The terminal record keeps only failed unwinds; the cancellation is
	// the Run's Failure.
	if rec.Failure == nil || rec.Failure.Kind != durable.FailureKindCanceled {
		t.Fatalf("stored failure = %+v", rec.Failure)
	}
}

// Once the last forward step succeeded the Run is reducing and a request
// has no effect: the Run succeeds.
func TestCancelDuringReductionHasNoEffect(t *testing.T) {
	var run engine.Run
	var ready atomic.Bool
	def := pipelinedef.New(pipelinedef.Config{
		ID:    "uncancellable-reduce",
		Steps: []pipelinedef.Step{stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error { return nil })},
		Reduce: func(view durable.ReduceView) proto.Message {
			if ready.Load() {
				if err := run.Cancel(context.Background(), "too late"); err != nil {
					t.Errorf("Cancel during reduce: %v", err)
				}
			}
			return wrapperspb.String("reduced")
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	r, _, err := pipes[0].Schedule(context.Background(), "r", nil, durable.StartAfter(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	run = r
	ready.Store(true)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success", res, err)
	}
}

// A request on a Run already unwinding is recorded and changes nothing:
// the unwind attempt's ctx stays alive and the failure stays organic.
func TestCancelDuringUnwindIsNoOp(t *testing.T) {
	unwinding := make(chan struct{})
	release := make(chan struct{})
	var unwindCtxErr atomic.Value
	def := pipelinedef.New(pipelinedef.Config{
		ID: "cancel-unwinding",
		Steps: []pipelinedef.Step{
			{
				ID:     "a/v1",
				Unwind: true,
				Run:    func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					close(unwinding)
					<-release
					unwindCtxErr.Store(errors.Is(ctx.Err(), context.Canceled))
					return nil
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("organic"))
			}),
		},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-unwinding
	if err := run.Cancel(context.Background(), "late"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	close(release)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() || res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want the organic failure", res, err)
	}
	if got := unwindCtxErr.Load(); got != false {
		t.Fatal("the unwind attempt's ctx was canceled by the request")
	}
}

func TestCancelTerminalRun(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID:    "cancel-terminal",
		Steps: []pipelinedef.Step{stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error { return nil })},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if err := run.Cancel(context.Background(), "too late"); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("Cancel = %v, want ErrRunTerminal", err)
	}
}

// A request after the first is a no-op and the first's cause stands.
func TestDuplicateCancelIsANoOp(t *testing.T) {
	blocked := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "dup-cancel",
		Steps: []pipelinedef.Step{stateless("a/v1", func(ctx context.Context, inv durable.Invocation) error {
			close(blocked)
			<-ctx.Done()
			return ctx.Err()
		})},
	})
	_, pipes := startEngine(t, mem.New(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-blocked
	if err := run.Cancel(context.Background(), "first"); err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(context.Background(), "second"); err != nil {
		t.Fatalf("duplicate Cancel = %v; want nil", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
	if res.Failure == nil || res.Failure.Message != "first" {
		t.Fatalf("Failure = %+v; want the first request's cause", res.Failure)
	}
}

// childPipeline is a child that blocks until its ctx dies or release
// closes, for the cascade tests.
func childPipeline(id durable.PipelineID, release <-chan struct{}) *pipelinedef.Definition {
	return pipelinedef.New(pipelinedef.Config{
		ID: id,
		Steps: []pipelinedef.Step{stateless("c/v1", func(ctx context.Context, inv durable.Invocation) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})},
	})
}

// Canceling a parent cancels the child its attempt scheduled, with the
// same cause, and the parked parent is not woken to do it.
func TestCancelCascadesToChild(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var childPipe *engine.Pipeline
	var attempts atomic.Int32
	parent := pipelinedef.New(pipelinedef.Config{
		ID: "cascade-parent",
		Steps: []pipelinedef.Step{stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
			attempts.Add(1)
			child, _, err := childPipe.Schedule(ctx, "child-res", nil)
			if err != nil {
				return err
			}
			return durable.AwaitRun(child.ID())
		})},
	})
	store := mem.New()
	_, pipes := startEngine(t, store, childPipeline("cascade-child", release), parent)
	childPipe = pipes[0]
	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	st := waitForState(t, pRun, engine.RunStateAwaiting)
	child, err := childPipe.GetRun(context.Background(), st.AwaitingRunIDs[0])
	if err != nil {
		t.Fatalf("GetRun child: %v", err)
	}
	if head, err := store.GetRunHead(context.Background(), child.ID()); err != nil || head.Parent != pRun.ID() {
		t.Fatalf("child head = %+v, %v; want parent %s", head, err, pRun.ID())
	}
	if err := pRun.Cancel(context.Background(), "abandon"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("parent Wait = %+v, %v; want canceled", res, err)
	}
	res, err := child.Wait(context.Background())
	if err != nil || !res.Canceled() || res.Failure.Message != "abandon" {
		t.Fatalf("child Wait = %+v, %v; want canceled with the parent's cause", res, err)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("parent attempts = %d; want 1", n)
	}
	if children, err := store.ListChildren(context.Background(), pRun.ID()); err != nil || len(children) != 0 {
		t.Fatalf("ListChildren after terminality = %v, %v; want none", children, err)
	}
}

// A child scheduled by a parent's attempt after the parent's request
// landed is canceled at acceptance.
func TestChildScheduledAfterParentCancelIsCanceled(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var childPipe *engine.Pipeline
	var childID atomic.Value
	blocked := make(chan struct{})
	parent := pipelinedef.New(pipelinedef.Config{
		ID: "late-parent",
		Steps: []pipelinedef.Step{stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error {
			close(blocked)
			<-ctx.Done() // the cancel lands here
			child, _, err := childPipe.Schedule(ctx, "child-res", nil)
			if err != nil {
				return err
			}
			childID.Store(child.ID())
			return ctx.Err()
		})},
	})
	_, pipes := startEngine(t, mem.New(), childPipeline("late-child", release), parent)
	childPipe = pipes[0]
	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	<-blocked
	if err := pRun.Cancel(context.Background(), "abandon"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("parent Wait = %+v, %v; want canceled", res, err)
	}
	child, err := childPipe.GetRun(context.Background(), childID.Load().(durable.RunID))
	if err != nil {
		t.Fatalf("GetRun child: %v", err)
	}
	if res, err := child.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("child Wait = %+v, %v; want canceled", res, err)
	}
}

// A crash between a parent's request and its cascade leaves the child
// uncanceled; the next engine reconciles it against the parent's
// durable request.
func TestCascadeReconciledAtStartup(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	store := mem.New()
	now := time.Now()
	parentRec := &driver.RunRecord{RunID: "01PARENT", PipelineID: "recon-parent", ResourceID: "p", Phase: durable.PhaseForward, CreatedAt: now, UpdatedAt: now}
	childRec := &driver.RunRecord{RunID: "01CHILD", PipelineID: "recon-child", ResourceID: "c", Parent: "01PARENT", Phase: durable.PhaseForward, CreatedAt: now, UpdatedAt: now}
	for _, rec := range []*driver.RunRecord{parentRec, childRec} {
		if _, created, err := store.CreateRun(context.Background(), rec, nil); err != nil || !created {
			t.Fatalf("CreateRun(%s) = %v, %v", rec.RunID, created, err)
		}
	}
	// The request reached the parent; the process died before the cascade.
	if ok, err := store.RequestCancel(context.Background(), "01PARENT", driver.CancelRequest{Cause: "abandon", At: now}); err != nil || !ok {
		t.Fatalf("RequestCancel = %v, %v", ok, err)
	}
	parent := pipelinedef.New(pipelinedef.Config{
		ID:    "recon-parent",
		Steps: []pipelinedef.Step{stateless("p/v1", func(ctx context.Context, inv durable.Invocation) error { return nil })},
	})
	_, pipes := startEngine(t, store, childPipeline("recon-child", release), parent)
	child, err := pipes[0].GetRun(context.Background(), "01CHILD")
	if err != nil {
		t.Fatalf("GetRun child: %v", err)
	}
	if res, err := child.Wait(context.Background()); err != nil || !res.Canceled() || res.Failure.Message != "abandon" {
		t.Fatalf("child Wait = %+v, %v; want canceled with the parent's cause", res, err)
	}
}

// A Run scheduled from an unwind handler is not a child: compensation
// may need Runs that outlive the cancellation.
func TestUnwindScheduledRunHasNoParent(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var childPipe *engine.Pipeline
	var childID atomic.Value
	parent := pipelinedef.New(pipelinedef.Config{
		ID: "unwind-parent",
		Steps: []pipelinedef.Step{
			{
				ID:     "a/v1",
				Unwind: true,
				Run:    func(ctx context.Context, inv durable.Invocation) (proto.Message, error) { return nil, nil },
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					child, _, err := childPipe.Schedule(ctx, "cleanup-res", nil)
					if err != nil {
						return err
					}
					childID.Store(child.ID())
					return nil
				},
			},
			stateless("b/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("organic"))
			}),
		},
	})
	store := mem.New()
	_, pipes := startEngine(t, store, childPipeline("cleanup", release), parent)
	childPipe = pipes[0]
	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Failed() {
		t.Fatalf("parent Wait = %+v, %v; want failed", res, err)
	}
	head, err := store.GetRunHead(context.Background(), childID.Load().(durable.RunID))
	if err != nil {
		t.Fatalf("GetRunHead: %v", err)
	}
	if head.Parent != "" {
		t.Fatalf("cleanup run parent = %q; want none", head.Parent)
	}
}
