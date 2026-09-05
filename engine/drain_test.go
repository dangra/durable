package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// TestDrainLetsAttemptFinish pins the graceful path: with a drain
// timeout, Stop leaves the in-flight attempt's ctx alive, its success
// commits, the run reaches terminality — and Stop returns as soon as
// workers drain, well before the deadline.
func TestDrainLetsAttemptFinish(t *testing.T) {
	var (
		mu     sync.Mutex
		ctxErr error = errors.New("unset")
	)
	running, release := make(chan struct{}), make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "drained",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			close(running)
			<-release
			mu.Lock()
			ctxErr = ctx.Err()
			mu.Unlock()
			return nil
		})},
	})
	store := mem.New()
	e := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()),
		engine.WithDrainTimeout(30*time.Second))
	pipe, err := e.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-running

	stopped := make(chan error, 1)
	start := time.Now()
	go func() { stopped <- e.Stop(context.Background()) }()
	close(release) // the handler finishes on its own terms
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("Stop took %v; drain must return when workers finish, not at the deadline", elapsed)
	}
	mu.Lock()
	if ctxErr != nil {
		mu.Unlock()
		t.Fatalf("handler ctx err = %v, want alive through the drain", ctxErr)
	}
	mu.Unlock()

	// The attempt's success committed and the run completed during the
	// drain — the restarted engine sees it terminal immediately.
	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0),
		engine.WithLogger(discardTestLogger()))
	pipe3, err := e2.Bind(pipelinedef.New(pipelinedef.Config{
		ID: "drained",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			t.Error("handler re-executed; the drained attempt must have committed")
			return nil
		})},
	}))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e2.Stop(context.Background())
	run2, err := pipe3.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("result = %+v, want success committed during drain", res)
	}
}

// TestDrainDeadlinePreempts pins the backstop: a handler that ignores
// the drain blocks until the deadline, then is preempted with
// ErrEngineStopping exactly as without a drain.
func TestDrainDeadlinePreempts(t *testing.T) {
	var (
		mu    sync.Mutex
		cause error
	)
	running := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "stubborn",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			close(running)
			<-ctx.Done()
			mu.Lock()
			cause = context.Cause(ctx)
			mu.Unlock()
			return ctx.Err()
		})},
	})
	e := engine.New(mem.New(), fastRetry,
		engine.WithLogger(discardTestLogger()),
		engine.WithDrainTimeout(50*time.Millisecond))
	pipe, err := e.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	_ = run
	<-running
	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(cause, durable.ErrEngineStopping) {
		t.Fatalf("context.Cause = %v, want ErrEngineStopping at the drain deadline", cause)
	}
}

// TestDrainStartsNoNewAttempts pins the quiesce half: an attempt that
// finishes during the drain does not hand the worker the next step —
// the run stays nonterminal for the next engine.
func TestDrainStartsNoNewAttempts(t *testing.T) {
	var (
		mu   sync.Mutex
		ran  []string
		gate = make(chan struct{})
	)
	running := make(chan struct{})
	step := func(id durable.StepID) pipelinedef.Step {
		return stateless(id, func(ctx context.Context, inv durable.Invocation) error {
			mu.Lock()
			ran = append(ran, string(inv.StepID()))
			mu.Unlock()
			if id == "first/v1" {
				close(running)
				<-gate
			}
			return nil
		})
	}
	store := mem.New()
	e := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()),
		engine.WithDrainTimeout(30*time.Second))
	pipe, err := e.Bind(pipelinedef.New(pipelinedef.Config{
		ID: "twostep", Steps: []pipelinedef.Step{step("first/v1"), step("second/v1")},
	}))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, _, err := pipe.Schedule(context.Background(), "res-1", nil); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-running

	stopped := make(chan error, 1)
	go func() { stopped <- e.Stop(context.Background()) }()
	// Give Stop a moment to enter the drain before releasing the
	// in-flight first step.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 || ran[0] != "first/v1" {
		t.Fatalf("steps executed during drain = %v, want only the in-flight first/v1", ran)
	}
}
