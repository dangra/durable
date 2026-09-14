package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/observe"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// TestPreemptionCarriesCause pins the ctx cause contract: an attempt
// cut by Cancel sees a *PreemptedError carrying the cancel cause via
// context.Cause, and returning ctx.Err() ends the Run canceled on that
// same attempt.
func TestPreemptionCarriesCause(t *testing.T) {
	var (
		mu    sync.Mutex
		cause error
	)
	running := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "coop",
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
		engine.WithLogger(discardTestLogger()))
	pipe, err := e.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-running
	if err := run.Cancel(context.Background(), "incident declared"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() {
		t.Fatalf("result = %+v, want canceled", res)
	}
	mu.Lock()
	defer mu.Unlock()
	pe, ok := errors.AsType[*durable.PreemptedError](cause)
	if !ok || pe.Cause != "incident declared" {
		t.Fatalf("context.Cause = %v, want *PreemptedError{incident declared}", cause)
	}
}

// TestStopCarriesCause pins the shutdown half: an attempt killed by
// Engine.Stop sees ErrEngineStopping, the Run stays nonterminal, and a
// restarted engine completes it.
func TestStopCarriesCause(t *testing.T) {
	var (
		mu    sync.Mutex
		cause error
	)
	running := make(chan struct{})
	var once sync.Once
	def := pipelinedef.New(pipelinedef.Config{
		ID: "stoppable",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			select {
			case <-ctx.Done():
				mu.Lock()
				cause = context.Cause(ctx)
				mu.Unlock()
				return ctx.Err()
			default:
				once.Do(func() { close(running) })
				<-ctx.Done()
				mu.Lock()
				cause = context.Cause(ctx)
				mu.Unlock()
				return ctx.Err()
			}
		})},
	})
	store := mem.New()
	e1 := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()))
	pipe1, err := e1.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	run, _, err := pipe1.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-running
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	mu.Lock()
	if !errors.Is(cause, durable.ErrEngineStopping) {
		mu.Unlock()
		t.Fatalf("context.Cause = %v, want ErrEngineStopping", cause)
	}
	mu.Unlock()

	// The run resumed under a new engine completes: shutdown was not
	// cancellation.
	def2 := pipelinedef.New(pipelinedef.Config{
		ID: "stoppable",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			return nil
		})},
	})
	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0),
		engine.WithLogger(discardTestLogger()))
	pipe2, err := e2.Bind(def2)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e2.Stop(context.Background())
	run2, err := pipe2.GetRun(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("result = %+v, want success after restart", res)
	}
}

// TestStopInterruptionIsNotAFailure pins that an attempt shutdown cuts
// short leaves no failure behind: no last error, no retry backoff, an
// AttemptInterrupted event, and the next engine re-executes it at once.
func TestStopInterruptionIsNotAFailure(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	def := pipelinedef.New(pipelinedef.Config{
		ID: "interrupted",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			once.Do(func() { close(running) })
			<-ctx.Done()
			return fmt.Errorf("dial: %w", ctx.Err())
		})},
	})
	store := mem.New()
	log := &eventLog{}
	e1 := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()),
		engine.WithObserver(log.observer()))
	pipe1, err := e1.Bind(def)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	run, _, err := pipe1.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-running
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	head, err := store.GetRunHead(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("GetRunHead: %v", err)
	}
	if !head.NextAttemptAt.IsZero() {
		t.Errorf("NextAttemptAt = %v, want zero: an interruption schedules no backoff", head.NextAttemptAt)
	}
	if head.LastError != "" || !head.LastErrorAt.IsZero() {
		t.Errorf("last error = %q at %v, want none recorded", head.LastError, head.LastErrorAt)
	}
	log.locked(func() {
		if len(log.attempts) != 1 {
			t.Fatalf("attempt events = %d, want 1", len(log.attempts))
		}
		ev := log.attempts[0]
		if ev.Result != observe.AttemptInterrupted || ev.Attempt != 1 || ev.RetryIn != 0 || !errors.Is(ev.Err, context.Canceled) {
			t.Errorf("attempt event = %+v, want interrupted attempt 1 with no retry and the handler's error", ev)
		}
	})

	// The next engine re-executes the operation as attempt 2 with no
	// memory of an error.
	var attempt uint64
	def2 := pipelinedef.New(pipelinedef.Config{
		ID: "interrupted",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			attempt = inv.Attempt()
			return nil
		})},
	})
	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0),
		engine.WithLogger(discardTestLogger()))
	pipe2, err := e2.Bind(def2)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e2.Stop(context.Background())
	run2, err := pipe2.GetRun(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("outcome = %v, want success", res.Outcome)
	}
	if attempt != 2 {
		t.Errorf("completing attempt = %d, want 2", attempt)
	}
}

// TestStopInterruptedFailStillFails pins the boundary: a killed attempt
// that returns Fail decided, and its failure stands.
func TestStopInterruptedFailStillFails(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	def := pipelinedef.New(pipelinedef.Config{
		ID: "decided",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			once.Do(func() { close(running) })
			<-ctx.Done()
			return durable.Fail(errors.New("gave up"), durable.WithReason("gave-up"))
		})},
	})
	store := mem.New()
	e := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()))
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
	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	head, err := store.GetRunHead(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("GetRunHead: %v", err)
	}
	if head.Failure == nil || head.Failure.Reason != "gave-up" {
		t.Fatalf("failure = %+v, want the handler's permanent failure", head.Failure)
	}
}
