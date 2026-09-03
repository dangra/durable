package durable_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
)

func discardTestLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// eventLog collects observer events for assertion; callbacks run on
// engine goroutines.
type eventLog struct {
	mu        sync.Mutex
	scheduled []durable.RunEvent
	attempts  []durable.AttemptEvent
	unwinding []durable.RunFailureEvent
	terminal  []durable.RunTerminalEvent
	invalid   []durable.RunFailureEvent
	wakes     []durable.WakeEvent
	classWait []durable.ClassWaitEvent
	storeOps  []durable.StoreOpEvent
}

func (l *eventLog) observer() durable.Observer {
	return durable.Observer{
		RunScheduled: func(ev durable.RunEvent) { l.mu.Lock(); l.scheduled = append(l.scheduled, ev); l.mu.Unlock() },
		AttemptDone:  func(ev durable.AttemptEvent) { l.mu.Lock(); l.attempts = append(l.attempts, ev); l.mu.Unlock() },
		RunUnwinding: func(ev durable.RunFailureEvent) { l.mu.Lock(); l.unwinding = append(l.unwinding, ev); l.mu.Unlock() },
		RunTerminal:  func(ev durable.RunTerminalEvent) { l.mu.Lock(); l.terminal = append(l.terminal, ev); l.mu.Unlock() },
		RunInvalid:   func(ev durable.RunFailureEvent) { l.mu.Lock(); l.invalid = append(l.invalid, ev); l.mu.Unlock() },
		WaiterWoken:  func(ev durable.WakeEvent) { l.mu.Lock(); l.wakes = append(l.wakes, ev); l.mu.Unlock() },
		ClassWait:    func(ev durable.ClassWaitEvent) { l.mu.Lock(); l.classWait = append(l.classWait, ev); l.mu.Unlock() },
		StoreOp:      func(ev durable.StoreOpEvent) { l.mu.Lock(); l.storeOps = append(l.storeOps, ev); l.mu.Unlock() },
	}
}

// locked runs fn under the log lock, for reading collected events after
// the Run is terminal.
func (l *eventLog) locked(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn()
}

func startObservedEngine(t *testing.T, log *eventLog, defs []*durable.Definition, opts ...durable.Option) (*durable.Engine, []*durable.Pipeline) {
	t.Helper()
	opts = append([]durable.Option{
		fastRetry, durable.WithRecoveryBackoff(0), durable.WithObserver(log.observer()),
	}, opts...)
	e := durable.NewEngine(durabletest.NewMemStore(), opts...)
	var pipes []*durable.Pipeline
	for _, def := range defs {
		pipe, err := def.Bind(e)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		pipes = append(pipes, pipe)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e, pipes
}

// TestObserverLifecycle drives retry, success, permanent failure, and
// unwind, asserting the full event sequence with attribution.
func TestObserverLifecycle(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed",
		Steps: []durable.StepConfig{
			{
				ID:     "flaky/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if inv.Attempt() == 1 {
						return nil, errors.New("transient boom")
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			stateless("explode/v1", func(ctx context.Context, inv *durable.Invocation) error {
				return durable.Fail(errors.New("permanent boom"), durable.WithUserKind(), durable.WithReason("bad-request"))
			}),
		},
	})
	log := &eventLog{}
	_, pipes := startObservedEngine(t, log, []*durable.Definition{def})
	pipe := pipes[0]

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := run.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeFailure {
		t.Fatalf("Wait = %+v, %v", res, err)
	}

	log.locked(func() {
		if len(log.scheduled) != 1 || log.scheduled[0].RunID != run.ID() || log.scheduled[0].PipelineID != "observed" || !log.scheduled[0].StartAt.IsZero() {
			t.Fatalf("scheduled = %+v", log.scheduled)
		}

		type short struct {
			step    durable.StepID
			phase   durable.Phase
			attempt uint64
			result  durable.AttemptResult
		}
		var got []short
		for _, a := range log.attempts {
			got = append(got, short{a.StepID, a.Phase, a.Attempt, a.Result})
			if a.RunID != run.ID() || a.PipelineID != "observed" || a.ResourceID != "res-1" {
				t.Errorf("attempt identity = %+v", a)
			}
		}
		want := []short{
			{"flaky/v1", durable.PhaseForward, 1, durable.AttemptRetrying},
			{"flaky/v1", durable.PhaseForward, 2, durable.AttemptSucceeded},
			{"explode/v1", durable.PhaseForward, 1, durable.AttemptFailed},
			{"flaky/v1", durable.PhaseUnwind, 1, durable.AttemptSucceeded},
		}
		if len(got) != len(want) {
			t.Fatalf("attempts = %+v, want %+v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("attempt[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
		if log.attempts[0].Err == nil || log.attempts[0].RetryIn <= 0 {
			t.Errorf("retrying attempt lacks err/backoff: %+v", log.attempts[0])
		}
		if log.attempts[2].Err == nil {
			t.Errorf("failed attempt lacks err: %+v", log.attempts[2])
		}

		if len(log.unwinding) != 1 {
			t.Fatalf("unwinding = %+v", log.unwinding)
		}
		uw := log.unwinding[0]
		if uw.StepID != "explode/v1" || uw.Kind != durable.FailureKindUser || uw.Reason != "bad-request" {
			t.Errorf("unwinding = %+v", uw)
		}

		if len(log.terminal) != 1 {
			t.Fatalf("terminal = %+v", log.terminal)
		}
		term := log.terminal[0]
		if term.Outcome != durable.OutcomeFailure || term.Kind != durable.FailureKindUser || term.Reason != "bad-request" || term.RunID != run.ID() {
			t.Errorf("terminal = %+v", term)
		}

		var creates, applies int
		for _, op := range log.storeOps {
			if op.Err != nil {
				t.Errorf("store op error: %+v", op)
			}
			switch op.Op {
			case "CreateRun":
				creates++
			case "ApplyTransition":
				applies++
			}
		}
		if creates != 1 || applies == 0 {
			t.Errorf("store ops: creates=%d applies=%d", creates, applies)
		}
	})
}

// TestObserverCancel asserts an accepted cancellation surfaces as
// RunUnwinding with FailureKindCanceled and that a delayed run shows in
// Stats as delayed.
func TestObserverCancel(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-cancel",
		Steps: []durable.StepConfig{
			stateless("never/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})
	log := &eventLog{}
	e, pipes := startObservedEngine(t, log, []*durable.Definition{def})
	pipe := pipes[0]

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil, durable.StartAfter(time.Hour))
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().DelayedRuns == 0 {
		if time.Now().After(deadline) {
			t.Fatal("delayed run never appeared in Stats")
		}
		time.Sleep(time.Millisecond)
	}
	if err := run.Cancel(context.Background(), "superseded"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := run.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}

	log.locked(func() {
		if len(log.scheduled) != 1 || log.scheduled[0].StartAt.IsZero() {
			t.Errorf("scheduled = %+v", log.scheduled)
		}
		if len(log.unwinding) != 1 || log.unwinding[0].Kind != durable.FailureKindCanceled || log.unwinding[0].Message != "superseded" {
			t.Errorf("unwinding = %+v", log.unwinding)
		}
		if len(log.terminal) != 1 || log.terminal[0].Kind != durable.FailureKindCanceled {
			t.Errorf("terminal = %+v", log.terminal)
		}
	})
}

// TestObserverAwaitWake asserts AwaitRun parks emit AttemptAwaiting and
// wakes emit WaiterWoken with the awaited target.
func TestObserverAwaitWake(t *testing.T) {
	release := make(chan struct{})
	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "await-target",
		Steps: []durable.StepConfig{
			stateless("hold/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	var targetID durable.RunID
	waiter := durable.NewDefinition(durable.DefinitionConfig{
		ID: "await-waiter",
		Steps: []durable.StepConfig{
			stateless("wait/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if _, ok := inv.AwaitedRunID(); ok {
					return nil
				}
				return durable.AwaitRun(targetID)
			}),
		},
	})
	log := &eventLog{}
	e, pipes := startObservedEngine(t, log, []*durable.Definition{target, waiter})
	targetPipe, waiterPipe := pipes[0], pipes[1]

	trun, _, err := targetPipe.Schedule(context.Background(), "res-t", nil)
	if err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	targetID = trun.ID()
	wrun, _, err := waiterPipe.Schedule(context.Background(), "res-w", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().AwaitingRuns == 0 {
		if time.Now().After(deadline) {
			t.Fatal("waiter never parked")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if res, err := wrun.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("waiter Wait = %+v, %v", res, err)
	}

	log.locked(func() {
		var awaiting bool
		for _, a := range log.attempts {
			if a.RunID == wrun.ID() && a.Result == durable.AttemptAwaiting {
				awaiting = true
			}
		}
		if !awaiting {
			t.Errorf("no AttemptAwaiting event: %+v", log.attempts)
		}
		if len(log.wakes) != 1 || log.wakes[0].RunID != wrun.ID() || log.wakes[0].Target != trun.ID() {
			t.Errorf("wakes = %+v", log.wakes)
		}
	})
	if e.Stats().AwaitingRuns != 0 {
		t.Errorf("AwaitingRuns = %d after wake", e.Stats().AwaitingRuns)
	}
}

// TestObserverClassWaitAndStats asserts throttled runs emit ClassWait on
// proceeding and appear in Stats while parked.
func TestObserverClassWaitAndStats(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-class",
		Steps: []durable.StepConfig{{
			ID:               "gated/v1",
			ConcurrencyClass: "boot",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				entered <- struct{}{}
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}},
	})
	log := &eventLog{}
	e, pipes := startObservedEngine(t, log, []*durable.Definition{def}, durable.WithConcurrencyClass("boot", 1))
	pipe := pipes[0]

	first, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule first: %v", err)
	}
	<-entered // first holds the only token
	second, _, err := pipe.Schedule(context.Background(), "res-2", nil)
	if err != nil {
		t.Fatalf("Schedule second: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := e.Stats()
		if st.ThrottledRuns == 1 && st.Classes["boot"].Waiting == 1 && st.Classes["boot"].InUse == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("second never throttled in Stats: %+v", e.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-entered // second proceeds after first releases the token
	for _, r := range []durable.Run{first, second} {
		if res, err := r.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
			t.Fatalf("Wait = %+v, %v", res, err)
		}
	}

	log.locked(func() {
		if len(log.classWait) != 1 || log.classWait[0].RunID != second.ID() || log.classWait[0].Class != "boot" {
			t.Errorf("classWait = %+v", log.classWait)
		}
	})
}

// TestObserverPanicIsolated asserts a panicking callback neither affects
// the Run nor later observers.
func TestObserverPanicIsolated(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-panic",
		Steps: []durable.StepConfig{
			stateless("ok/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})
	log := &eventLog{}
	panicky := durable.Observer{
		RunTerminal: func(durable.RunTerminalEvent) { panic("observer bug") },
	}
	e := durable.NewEngine(durabletest.NewMemStore(),
		fastRetry, durable.WithRecoveryBackoff(0), durable.WithLogger(discardTestLogger()),
		durable.WithObserver(panicky), durable.WithObserver(log.observer()))
	pipe, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := run.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	log.locked(func() {
		if len(log.terminal) != 1 {
			t.Errorf("second observer missed terminal event: %+v", log.terminal)
		}
	})
}
