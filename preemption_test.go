package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "coop",
		Steps: []durable.StepConfig{stateless("work/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(discardTestLogger()))
	pipe, err := def.Bind(e)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "stoppable",
		Steps: []durable.StepConfig{stateless("work/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	store := durabletest.NewMemStore()
	e1 := durable.NewEngine(store, fastRetry, durable.WithLogger(discardTestLogger()))
	pipe1, err := def.Bind(e1)
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
	def2 := durable.NewDefinition(durable.DefinitionConfig{
		ID: "stoppable",
		Steps: []durable.StepConfig{stateless("work/v1", func(ctx context.Context, inv *durable.Invocation) error {
			return nil
		})},
	})
	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0),
		durable.WithLogger(discardTestLogger()))
	pipe2, err := def2.Bind(e2)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e2.Stop(context.Background())
	run2, err := pipe2.Run(context.Background(), run.ID())
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "failfast",
		Steps: []durable.StepConfig{
			{
				ID:     "prepare/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					unwound.Store(true)
					return nil
				},
			},
			stateless("block/v1", func(ctx context.Context, inv *durable.Invocation) error {
				attempts.Add(1)
				close(running)
				<-ctx.Done()
				return ctx.Err()
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(discardTestLogger()),
		durable.WithMiddleware(durable.FailFastOnCancel()))
	pipe, err := def.Bind(e)
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
	if got := res.RootFailure.Message; got != "release frozen" {
		t.Fatalf("RootFailure.Message = %q, want the cancel cause", got)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "shortcircuit",
		Steps: []durable.StepConfig{stateless("flaky/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(discardTestLogger()),
		durable.WithMiddleware(durable.FailFastOnCancel()))
	pipe, err := def.Bind(e)
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
	if res.RootFailure.Message != "stop retrying" {
		t.Fatalf("RootFailure.Message = %q, want the cancel cause", res.RootFailure.Message)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "excepted",
		Steps: []durable.StepConfig{stateless("careful/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(discardTestLogger()),
		durable.WithMiddleware(durable.FailFastOnCancel(durable.FailFastExcept(
			// A generated reference and a bare StepID both satisfy
			// StepIdentifier; the ref form is what generated-code users
			// write (orderspb.ChargePaymentStep).
			durable.NewStepRef("careful/v1"),
			durable.StepID("also-careful/v1"),
		))))
	pipe, err := def.Bind(e)
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "fabricated",
		Steps: []durable.StepConfig{stateless("liar/v1", func(ctx context.Context, inv *durable.Invocation) error {
			return durable.Fail(fmt.Errorf("pretend: %w", &durable.PreemptedError{Cause: "fake"}))
		})},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(discardTestLogger()))
	pipe, err := def.Bind(e)
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
	if res.RootFailure.Kind != durable.FailureKindSystem {
		t.Fatalf("kind = %v, want system", res.RootFailure.Kind)
	}
}

// TestFailFastShutdownUntouched pins that the middleware never converts
// a shutdown: the Run stays nonterminal through Stop and completes
// after restart.
func TestFailFastShutdownUntouched(t *testing.T) {
	running := make(chan struct{})
	var once sync.Once
	handler := func(done bool) durable.StepConfig {
		return stateless("work/v1", func(ctx context.Context, inv *durable.Invocation) error {
			if done {
				return nil
			}
			once.Do(func() { close(running) })
			<-ctx.Done()
			return ctx.Err()
		})
	}
	store := durabletest.NewMemStore()
	e1 := durable.NewEngine(store, fastRetry, durable.WithLogger(discardTestLogger()),
		durable.WithMiddleware(durable.FailFastOnCancel()))
	pipe1, err := durable.NewDefinition(durable.DefinitionConfig{
		ID: "shutdown", Steps: []durable.StepConfig{handler(false)},
	}).Bind(e1)
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

	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0),
		durable.WithLogger(discardTestLogger()),
		durable.WithMiddleware(durable.FailFastOnCancel()))
	pipe2, err := durable.NewDefinition(durable.DefinitionConfig{
		ID: "shutdown", Steps: []durable.StepConfig{handler(true)},
	}).Bind(e2)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e2.Stop(context.Background())
	run2, err := pipe2.Run(context.Background(), run.ID())
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
