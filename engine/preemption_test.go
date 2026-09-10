package engine_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/observe"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/mem"
)

// TestPreemptionCarriesCause pins the ctx cause contract: an attempt
// preempted by Cancel sees a *PreemptedError carrying the cancel cause
// via context.Cause, while the cooperative path (return ctx.Err(),
// observe CancelRequested on the retry) still terminates the Run as
// canceled.
func TestPreemptionCarriesCause(t *testing.T) {
	var (
		mu    sync.Mutex
		cause error
	)
	running := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "coop",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			if inv.CancelRequested() {
				return nil // cooperative resolution
			}
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

// TestFailFastOnCancel is the opt-in story end to end: with the
// middleware installed, a blocking handler that never checks
// CancelRequested is preempted exactly once, its ctx death is converted
// into a yield, the Run terminates Canceled with the cancel's cause,
// and the unwind of the earlier step still runs.
func TestFailFastOnCancel(t *testing.T) {
	var (
		attempts atomic.Int64
		unwound  atomic.Bool
	)
	running := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "failfast",
		Steps: []pipelinedef.Step{
			{
				ID:     "prepare/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation) error {
					unwound.Store(true)
					return nil
				},
			},
			stateless("block/v1", func(ctx context.Context, inv durable.Invocation) error {
				attempts.Add(1)
				close(running)
				<-ctx.Done()
				return ctx.Err()
			}),
		},
	})
	e := engine.New(mem.New(), fastRetry,
		engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(durable.FailFastOnCancel()))
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
	if err := run.Cancel(context.Background(), "release frozen"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() {
		t.Fatalf("result = %+v, want Canceled()", res)
	}
	if got := res.Failure.Message; got != "release frozen" {
		t.Fatalf("Failure.Message = %q, want the cancel cause", got)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("blocking handler ran %d times, want exactly 1 (yield converts the preempted attempt)", n)
	}
	if !unwound.Load() {
		t.Fatal("earlier step was not unwound")
	}
}

// TestFailFastShortCircuit pins the other entry path: a cancel landing
// between retries is honored before the handler runs again — the
// handler itself never observes CancelRequested.
func TestFailFastShortCircuit(t *testing.T) {
	var sawCancel atomic.Bool
	tried := make(chan struct{}, 64)
	def := pipelinedef.New(pipelinedef.Config{
		ID: "shortcircuit",
		Steps: []pipelinedef.Step{stateless("flaky/v1", func(ctx context.Context, inv durable.Invocation) error {
			if inv.CancelRequested() {
				sawCancel.Store(true)
			}
			select {
			case tried <- struct{}{}:
			default:
			}
			return errors.New("transient")
		})},
	})
	e := engine.New(mem.New(), fastRetry,
		engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(durable.FailFastOnCancel()))
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
	<-tried
	if err := run.Cancel(context.Background(), "stop retrying"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() {
		t.Fatalf("result = %+v, want Canceled()", res)
	}
	if res.Failure.Message != "stop retrying" {
		t.Fatalf("Failure.Message = %q, want the cancel cause", res.Failure.Message)
	}
	if sawCancel.Load() {
		t.Fatal("handler observed CancelRequested; the middleware must short-circuit first")
	}
}

// TestFailFastExcept pins the per-step escape hatch: an excepted step
// stays on the cooperative path — its preempted attempt is retried and
// the handler itself observes CancelRequested — while the Run still
// terminates canceled.
func TestFailFastExcept(t *testing.T) {
	var (
		sawCancel atomic.Bool
		attempts  atomic.Int64
	)
	running := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "excepted",
		Steps: []pipelinedef.Step{stateless("careful/v1", func(ctx context.Context, inv durable.Invocation) error {
			attempts.Add(1)
			if inv.CancelRequested() {
				sawCancel.Store(true)
				return nil // cooperative resolution
			}
			close(running)
			<-ctx.Done()
			return ctx.Err()
		})},
	})
	e := engine.New(mem.New(), fastRetry,
		engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(durable.FailFastOnCancel(durable.FailFastExcept(
			// A generated reference and a bare StepID both satisfy
			// StepIdentifier; the ref form is what generated-code users
			// write (orderspb.ChargePaymentStep).
			pipelinedef.StepRef("careful/v1"),
			durable.StepID("also-careful/v1"),
		))))
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
	if err := run.Cancel(context.Background(), "careful shutdown"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() {
		t.Fatalf("result = %+v, want canceled", res)
	}
	if !sawCancel.Load() {
		t.Fatal("excepted step never observed CancelRequested; it must stay cooperative")
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("excepted step attempts = %d, want the cooperative retry", n)
	}
}

// TestFabricatedPreemptionNotCanceled pins the masquerade guard: a Fail
// wrapping *PreemptedError with no cancel anywhere is attributed as an
// ordinary failure, never as canceled.
func TestFabricatedPreemptionNotCanceled(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "fabricated",
		Steps: []pipelinedef.Step{stateless("liar/v1", func(ctx context.Context, inv durable.Invocation) error {
			return durable.Fail(fmt.Errorf("pretend: %w", &durable.PreemptedError{Cause: "fake"}))
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
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Canceled() {
		t.Fatal("fabricated preemption attributed as canceled")
	}
	if res.Failure.Kind != durable.FailureKindSystem {
		t.Fatalf("kind = %v, want system", res.Failure.Kind)
	}
}

// TestFailFastShutdownUntouched pins that the middleware never converts
// a shutdown: the Run stays nonterminal through Stop and completes
// after restart.
func TestFailFastShutdownUntouched(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	handler := func(done bool) pipelinedef.Step {
		return stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			if done {
				return nil
			}
			once.Do(func() { close(running) })
			<-ctx.Done()
			return ctx.Err()
		})
	}
	store := mem.New()
	e1 := engine.New(store, fastRetry, engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(durable.FailFastOnCancel()))
	pipe1, err := e1.Bind(pipelinedef.New(pipelinedef.Config{
		ID: "shutdown", Steps: []pipelinedef.Step{handler(false)},
	}))
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

	e2 := engine.New(store, fastRetry, engine.WithRecoveryBackoff(0),
		engine.WithLogger(discardTestLogger()),
		engine.WithMiddleware(durable.FailFastOnCancel()))
	pipe2, err := e2.Bind(pipelinedef.New(pipelinedef.Config{
		ID: "shutdown", Steps: []pipelinedef.Step{handler(true)},
	}))
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
		t.Fatalf("result = %+v, want success — shutdown must not become a yield", res)
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

// TestYieldOnCancelRequested pins the cooperative yield: the preempted
// attempt returns ctx.Err(), the re-executed one finds the request and
// yields, and the Run ends Canceled with the request's cause.
func TestYieldOnCancelRequested(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	var attempts atomic.Int32
	def := pipelinedef.New(pipelinedef.Config{
		ID: "yielder",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			attempts.Add(1)
			if inv.CancelRequested() {
				return durable.Yield()
			}
			once.Do(func() { close(running) })
			<-ctx.Done()
			return ctx.Err()
		})},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithLogger(discardTestLogger()))
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
	if err := run.Cancel(context.Background(), "operator"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() || res.Failure.Message != "operator" {
		t.Fatalf("result = %+v, want canceled with cause \"operator\"", res.Failure)
	}
	if n := attempts.Load(); n != 2 {
		t.Errorf("attempts = %d, want 2 (preempted, then yielded)", n)
	}
}

// TestYieldOnPreemptedAttempt pins the immediate yield: the preempted
// attempt itself yields and the Run ends Canceled on attempt 1.
func TestYieldOnPreemptedAttempt(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	def := pipelinedef.New(pipelinedef.Config{
		ID: "yielder",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			once.Do(func() { close(running) })
			<-ctx.Done()
			if durable.Preempted(ctx) {
				return durable.Yield()
			}
			return ctx.Err()
		})},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithLogger(discardTestLogger()))
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
	if err := run.Cancel(context.Background(), "operator"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Canceled() || res.Failure.Message != "operator" || res.Failure.Attempt != 1 {
		t.Fatalf("result = %+v, want canceled with cause \"operator\" on attempt 1", res.Failure)
	}
}

// TestYieldWithoutCancelIsFailure pins that a Yield with nothing to
// yield to is a permanent system failure, never a cancellation.
func TestYieldWithoutCancelIsFailure(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "eager",
		Steps: []pipelinedef.Step{stateless("work/v1", func(ctx context.Context, inv durable.Invocation) error {
			return durable.Yield()
		})},
	})
	e := engine.New(mem.New(), fastRetry, engine.WithLogger(discardTestLogger()))
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
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Canceled() || res.Failure.Kind != durable.FailureKindSystem || res.Failure.Message != "yielded with no cancellation pending" {
		t.Fatalf("result = %+v, want a system failure saying no cancellation was pending", res.Failure)
	}
}
