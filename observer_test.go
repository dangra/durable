package durable_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/observe"
)

func discardTestLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// eventLog collects observer events for assertion; callbacks run on
// engine goroutines.
type eventLog struct {
	mu        sync.Mutex
	scheduled []observe.RunEvent
	attempts  []observe.AttemptEvent
	unwinding []observe.RunFailureEvent
	terminal  []observe.RunTerminalEvent
	invalid   []observe.RunFailureEvent
	wakes     []observe.WakeEvent
	classWait []observe.ClassWaitEvent
	storeOps  []observe.StoreOpEvent
}

func (l *eventLog) observer() observe.Observer {
	return observe.Observer{
		RunScheduled: func(ev observe.RunEvent) { l.mu.Lock(); l.scheduled = append(l.scheduled, ev); l.mu.Unlock() },
		AttemptDone:  func(ev observe.AttemptEvent) { l.mu.Lock(); l.attempts = append(l.attempts, ev); l.mu.Unlock() },
		RunUnwinding: func(ev observe.RunFailureEvent) { l.mu.Lock(); l.unwinding = append(l.unwinding, ev); l.mu.Unlock() },
		RunTerminal:  func(ev observe.RunTerminalEvent) { l.mu.Lock(); l.terminal = append(l.terminal, ev); l.mu.Unlock() },
		RunInvalid:   func(ev observe.RunFailureEvent) { l.mu.Lock(); l.invalid = append(l.invalid, ev); l.mu.Unlock() },
		WaiterWoken:  func(ev observe.WakeEvent) { l.mu.Lock(); l.wakes = append(l.wakes, ev); l.mu.Unlock() },
		ClassWait:    func(ev observe.ClassWaitEvent) { l.mu.Lock(); l.classWait = append(l.classWait, ev); l.mu.Unlock() },
		StoreOp:      func(ev observe.StoreOpEvent) { l.mu.Lock(); l.storeOps = append(l.storeOps, ev); l.mu.Unlock() },
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
		durable.WithLogger(discardTestLogger()),
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
			result  observe.AttemptResult
		}
		var got []short
		for _, a := range log.attempts {
			got = append(got, short{a.StepID, a.Phase, a.Attempt, a.Result})
			if a.RunID != run.ID() || a.PipelineID != "observed" || a.ResourceID != "res-1" {
				t.Errorf("attempt identity = %+v", a)
			}
		}
		want := []short{
			{"flaky/v1", durable.PhaseForward, 1, observe.AttemptRetrying},
			{"flaky/v1", durable.PhaseForward, 2, observe.AttemptSucceeded},
			{"explode/v1", durable.PhaseForward, 1, observe.AttemptFailed},
			{"flaky/v1", durable.PhaseUnwind, 1, observe.AttemptSucceeded},
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
			if a.RunID == wrun.ID() && a.Result == observe.AttemptAwaiting {
				awaiting = true
			}
		}
		if !awaiting {
			t.Errorf("no AttemptAwaiting event: %+v", log.attempts)
		}
		if len(log.wakes) != 1 || log.wakes[0].RunID != wrun.ID() || !slices.Equal(log.wakes[0].Targets, []durable.RunID{trun.ID()}) {
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

// TestObserverCancelThrottledRun asserts canceling a throttled Run clears
// its park state — Stats drops to zero, and the stale FIFO slot does not
// eat the wake a later waiter needs.
func TestObserverCancelThrottledRun(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-cancel-throttle",
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

	holder, _, err := pipe.Schedule(context.Background(), "res-hold", nil)
	if err != nil {
		t.Fatalf("Schedule holder: %v", err)
	}
	<-entered // holder owns the only token
	doomed, _, err := pipe.Schedule(context.Background(), "res-doomed", nil)
	if err != nil {
		t.Fatalf("Schedule doomed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().Classes["boot"].Waiting != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("doomed never queued: %+v", e.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if err := doomed.Cancel(context.Background(), "superseded"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := doomed.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("doomed Wait = %+v, %v", res, err)
	}
	if st := e.Stats(); st.ThrottledRuns != 0 || st.Classes["boot"].Waiting != 0 {
		t.Errorf("park state leaked after cancel: %+v", st)
	}

	// The token's next release must reach a real waiter, not a stale slot.
	waiter, _, err := pipe.Schedule(context.Background(), "res-wait", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second) // the first phase consumed the old one
	for e.Stats().Classes["boot"].Waiting != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter never queued: %+v", e.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-entered // the waiter got the token
	for _, r := range []durable.Run{holder, waiter} {
		if res, err := r.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
			t.Fatalf("Wait = %+v, %v", res, err)
		}
	}
}

// TestObserverCancelAwaitingRun asserts canceling a parked waiter clears
// AwaitingRuns immediately and suppresses the stale WaiterWoken event
// when the abandoned target later completes.
func TestObserverCancelAwaitingRun(t *testing.T) {
	release := make(chan struct{})
	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-await-target",
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
		ID: "cancel-await-waiter",
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
	if err := wrun.Cancel(context.Background(), "abandoned"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := wrun.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("waiter Wait = %+v, %v", res, err)
	}
	if st := e.Stats(); st.AwaitingRuns != 0 {
		t.Errorf("AwaitingRuns leaked after cancel: %+v", st)
	}

	close(release)
	if res, err := trun.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
		t.Fatalf("target Wait = %+v, %v", res, err)
	}
	// Give the stale watcher goroutine a beat, then assert it stayed
	// silent: the canceled Run was not woken by the target completing.
	time.Sleep(50 * time.Millisecond)
	log.locked(func() {
		if len(log.wakes) != 0 {
			t.Errorf("stale watcher emitted: %+v", log.wakes)
		}
	})
}

// TestObserverCancelHandsOnWake asserts that a queued waiter canceled
// while others wait behind it neither strands the queue nor emits a
// phantom ClassWait: the wake chain reaches the next waiter, which is
// the only Run to report a class wait.
func TestObserverCancelHandsOnWake(t *testing.T) {
	entered := make(chan durable.ResourceID, 3)
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-hand-on",
		Steps: []durable.StepConfig{{
			ID:               "gated/v1",
			ConcurrencyClass: "boot",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				entered <- inv.ResourceID()
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

	holder, _, err := pipe.Schedule(context.Background(), "res-hold", nil)
	if err != nil {
		t.Fatalf("Schedule holder: %v", err)
	}
	<-entered // holder owns the only token
	doomed, _, err := pipe.Schedule(context.Background(), "res-doomed", nil)
	if err != nil {
		t.Fatalf("Schedule doomed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().Classes["boot"].Waiting != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("doomed never queued: %+v", e.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	behind, _, err := pipe.Schedule(context.Background(), "res-behind", nil)
	if err != nil {
		t.Fatalf("Schedule behind: %v", err)
	}
	for e.Stats().Classes["boot"].Waiting != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("behind never queued: %+v", e.Stats())
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel the queue head: the wake it would have received must be
	// handed on to `behind` when the token frees.
	if err := doomed.Cancel(context.Background(), "superseded"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := doomed.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("doomed Wait = %+v, %v", res, err)
	}
	close(release)
	<-entered // behind got the token
	for _, r := range []durable.Run{holder, behind} {
		if res, err := r.Wait(context.Background()); err != nil || res.Outcome != durable.OutcomeSuccess {
			t.Fatalf("Wait = %+v, %v", res, err)
		}
	}

	log.locked(func() {
		if len(log.classWait) != 1 || log.classWait[0].RunID != behind.ID() {
			t.Errorf("classWait = %+v, want exactly one grant, for the run behind the canceled one", log.classWait)
		}
	})
}

// invalidStateDef builds a pipeline whose single HasState step returns
// (nil, nil) once gate closes — the state violation that marks the Run
// invalid for the current deployment.
func invalidStateDef(id durable.PipelineID, gate chan struct{}) *durable.Definition {
	return durable.NewDefinition(durable.DefinitionConfig{
		ID: id,
		Steps: []durable.StepConfig{{
			ID:       "nil-state/v1",
			HasState: true,
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				if gate != nil {
					select {
					case <-gate:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return nil, nil // HasState violation -> invalid Run
			},
		}},
	})
}

// TestObserverInvalidIdempotent asserts an already-invalid Run
// re-dispatched (here via Cancel) does not re-emit RunInvalid.
func TestObserverInvalidIdempotent(t *testing.T) {
	def := invalidStateDef("observed-invalid", nil)
	log := &eventLog{}
	_, pipes := startObservedEngine(t, log, []*durable.Definition{def})
	pipe := pipes[0]

	run, _, err := pipe.Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	var ie *durable.InvalidRunError
	if _, err := run.Wait(context.Background()); !errors.As(err, &ie) {
		t.Fatalf("Wait err = %v, want InvalidRunError", err)
	}
	// Cancel dispatches the invalid Run again; the re-derived invalidity
	// must not re-fire the event.
	if err := run.Cancel(context.Background(), "cleanup"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	log.locked(func() {
		if len(log.invalid) != 1 {
			t.Errorf("invalid events = %+v, want exactly 1", log.invalid)
		}
	})
}

// TestObserverInvalidTargetNoSpuriousWake asserts that an awaited target
// turning invalid — which fires the same notify channels terminality
// does — neither emits WaiterWoken nor resets the waiter's park.
func TestObserverInvalidTargetNoSpuriousWake(t *testing.T) {
	gate := make(chan struct{})
	target := invalidStateDef("invalid-target", gate)
	var targetID durable.RunID
	waiter := durable.NewDefinition(durable.DefinitionConfig{
		ID: "invalid-target-waiter",
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

	close(gate) // target's handler returns nil state -> target invalid
	for e.Stats().InvalidRuns == 0 {
		if time.Now().After(deadline) {
			t.Fatal("target never turned invalid")
		}
		time.Sleep(time.Millisecond)
	}
	// The invalidity notify pokes the waiter's watcher; the re-run gate
	// must find the target nonterminal, stay silent, and keep the park.
	time.Sleep(50 * time.Millisecond)
	log.locked(func() {
		if len(log.wakes) != 0 {
			t.Errorf("spurious WaiterWoken: %+v", log.wakes)
		}
	})
	if st := e.Stats(); st.AwaitingRuns != 1 {
		t.Errorf("AwaitingRuns = %d, want the park preserved", st.AwaitingRuns)
	}

	if err := wrun.Cancel(context.Background(), "give up"); err != nil {
		t.Fatalf("Cancel waiter: %v", err)
	}
	if res, err := wrun.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("waiter Wait = %+v, %v", res, err)
	}
	log.locked(func() {
		if len(log.wakes) != 0 {
			t.Errorf("WaiterWoken after cancel: %+v", log.wakes)
		}
	})
}

// TestObserverReapedPerBatch asserts RunsReaped fires once per reap
// batch, so runs deleted by earlier batches stay reported even if a
// later batch were to fail.
func TestObserverReapedPerBatch(t *testing.T) {
	const runs = 300 // > one 256-run reap batch
	store := durabletest.NewMemStore()
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed-reap",
		Steps: []durable.StepConfig{
			stateless("noop/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})

	seeder := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0), durable.WithLogger(discardTestLogger()))
	pipe, err := def.Bind(seeder)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := seeder.Start(context.Background()); err != nil {
		t.Fatalf("Start seeder: %v", err)
	}
	for i := 0; i < runs; i++ {
		run, _, err := pipe.Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("res-%d", i)), nil)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if _, err := run.Wait(context.Background()); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}
	if err := seeder.Stop(context.Background()); err != nil {
		t.Fatalf("Stop seeder: %v", err)
	}

	log := &eventLog{}
	var reaps []int
	reaper := durable.NewEngine(store,
		durable.WithRecoveryBackoff(0), durable.WithLogger(discardTestLogger()),
		durable.WithRetention(durable.RetentionPolicy{TerminalAfter: time.Nanosecond, Interval: time.Hour}),
		durable.WithObserver(observe.Observer{RunsReaped: func(n int) {
			log.mu.Lock()
			reaps = append(reaps, n)
			log.mu.Unlock()
		}}))
	if _, err := def.Bind(reaper); err != nil {
		t.Fatalf("Bind reaper: %v", err)
	}
	if err := reaper.Start(context.Background()); err != nil {
		t.Fatalf("Start reaper: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = reaper.Stop(ctx)
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		log.mu.Lock()
		total, batches := 0, len(reaps)
		for _, n := range reaps {
			total += n
		}
		log.mu.Unlock()
		if total == runs {
			if batches < 2 {
				t.Errorf("reap batches = %d (%v), want per-batch events", batches, reaps)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reaped %d/%d within deadline (%v)", total, runs, reaps)
		}
		time.Sleep(time.Millisecond)
	}
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
	panicky := observe.Observer{
		RunTerminal: func(observe.RunTerminalEvent) { panic("observer bug") },
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
