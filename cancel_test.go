// Cancellation: scheduled, in-flight, and terminal runs, and the race
// between an organic failure and a cancel request.
package durable_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"google.golang.org/protobuf/proto"
)

func TestCancelScheduledRunFreesSlot(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-scheduled",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				t.Error("step of canceled scheduled run executed")
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
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
	if res.RootFailure.Kind != durable.FailureKindCanceled || res.RootFailure.Message != "operator retracted" {
		t.Fatalf("RootFailure = %+v", res.RootFailure)
	}
	// The slot is free again immediately.
	if _, created, err := p.Schedule(context.Background(), "r", nil, durable.StartAfter(time.Hour)); err != nil || !created {
		t.Fatalf("post-cancel Schedule = created=%v err=%v", created, err)
	}
}

func TestCancelPreemptsAndUnwinds(t *testing.T) {
	var (
		mu           sync.Mutex
		unwound      []durable.StepID
		preempted    atomic.Bool
		sawRequested atomic.Bool
		cRan         atomic.Bool
	)
	unwind := func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
		mu.Lock()
		unwound = append(unwound, inv.StepID())
		mu.Unlock()
		return nil
	}
	blocked := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-midflight",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: unwind,
			},
			{
				ID:     "b/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if inv.CancelRequested() {
						// The doomed operation resolves cleanly.
						sawRequested.Store(true)
						return nil, nil
					}
					close(blocked)
					<-ctx.Done() // preempted by Cancel
					preempted.Store(true)
					return nil, ctx.Err()
				},
				UnwindFunc: unwind,
			},
			stateless("c/v1", func(ctx context.Context, inv *durable.Invocation) error {
				cRan.Store(true)
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	<-blocked // b's first attempt is in flight
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
	if !preempted.Load() {
		t.Error("in-flight attempt was not preempted")
	}
	if !sawRequested.Load() {
		t.Error("retry attempt did not observe CancelRequested")
	}
	if cRan.Load() {
		t.Error("new forward work selected after cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	// b resolved successfully after the cancel, so it participates in
	// unwind, in reverse order.
	want := []durable.StepID{"b/v1", "a/v1"}
	if len(unwound) != 2 || unwound[0] != want[0] || unwound[1] != want[1] {
		t.Fatalf("unwound = %v, want %v", unwound, want)
	}
}

func TestCancelTerminalRun(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-terminal",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(context.Context, *durable.Invocation) error { return nil }),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if err := run.Cancel(context.Background(), "too late"); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("Cancel = %v, want ErrRunTerminal", err)
	}
}

func TestOrganicFailureBeatsCancel(t *testing.T) {
	blocked := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-organic",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.CancelRequested() {
					return durable.Fail(errors.New("broken anyway"))
				}
				close(blocked)
				<-ctx.Done()
				return ctx.Err()
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	<-blocked
	if err := run.Cancel(context.Background(), "stop"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if res.Canceled() {
		t.Fatalf("result = %+v; organic permanent failure should be the root", res.RootFailure)
	}
	if res.RootFailure.StepID != "s/v1" {
		t.Fatalf("RootFailure = %+v, want step s/v1", res.RootFailure)
	}
}
