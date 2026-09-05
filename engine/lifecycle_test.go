// The forward path of one run: success and state commit, ordinary-error
// retries, panics, permanent failure and unwind, and the last-error
// surface while retrying.
package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestForwardSuccessWithReducer(t *testing.T) {
	selectRef := refFor("select-host/v1")
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "provision",
		Steps: []engine.StepConfig{
			stateless("validate/v1", func(ctx context.Context, inv durable.Invocation) error {
				in, ok := inv.InputMessage().(*wrapperspb.StringValue)
				if !ok || in.GetValue() != "ord" {
					return durable.Fail(errors.New("unexpected input"))
				}
				return nil
			}),
			{
				ID:       "select-host/v1",
				HasState: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return str("host-7"), nil
				},
			},
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
		Reduce: func(v durable.ReduceView) proto.Message {
			host, ok := durable.LookupState(v, selectRef)
			if !ok {
				panic("select-host state unavailable")
			}
			in := v.InputMessage().(*wrapperspb.StringValue)
			return str(in.GetValue() + ":" + host.GetValue())
		},
	})

	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, created, err := pipes[0].Schedule(context.Background(), "machine-1", str("ord"))
	if err != nil || !created {
		t.Fatalf("Schedule = created=%v err=%v", created, err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Succeeded() {
		t.Fatalf("Outcome = %v, want success (root failure: %+v)", res.Outcome, res.RootFailure)
	}
	b, err := run.OutputBytes(context.Background())
	if err != nil {
		t.Fatalf("OutputBytes: %v", err)
	}
	out := &wrapperspb.StringValue{}
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if out.GetValue() != "ord:host-7" {
		t.Fatalf("Output = %q, want %q", out.GetValue(), "ord:host-7")
	}
}

func TestRetryUntilSuccess(t *testing.T) {
	var attempts atomic.Uint64
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "retrying",
		Steps: []engine.StepConfig{
			stateless("flaky/v1", func(ctx context.Context, inv durable.Invocation) error {
				attempts.Store(inv.Attempt())
				if inv.Attempt() < 3 {
					return errors.New("transient")
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success", res, err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("final attempt = %d, want 3", got)
	}
}

func TestHandlerPanicIsRetried(t *testing.T) {
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "panicky",
		Steps: []engine.StepConfig{
			stateless("boom/v1", func(ctx context.Context, inv durable.Invocation) error {
				if inv.Attempt() == 1 {
					panic("kaboom")
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success after panic retry", res, err)
	}
}

func TestPermanentFailureUnwinds(t *testing.T) {
	reserveRef := refFor("reserve/v1")
	var mu sync.Mutex
	var unwoundSteps []durable.StepID
	var failureSeenByA durable.Failure

	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "failing",
		Steps: []engine.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					mu.Lock()
					unwoundSteps = append(unwoundSteps, inv.StepID())
					failureSeenByA = f
					mu.Unlock()
					return nil
				},
			},
			{
				ID:       "reserve/v1",
				Unwind:   true,
				HasState: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return str("res-42"), nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					state, ok := durable.LookupState(inv, reserveRef)
					if !ok || state.GetValue() != "res-42" {
						return durable.Fail(errors.New("own state unavailable during unwind"))
					}
					mu.Lock()
					unwoundSteps = append(unwoundSteps, inv.StepID())
					mu.Unlock()
					return durable.Fail(errors.New("release rejected"))
				},
			},
			stateless("create/v1", func(ctx context.Context, inv durable.Invocation) error {
				return durable.Fail(errors.New("quota exceeded"))
			}),
		},
	})

	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Failed() {
		t.Fatalf("Outcome = %v, want failure", res.Outcome)
	}
	if res.RootFailure == nil || res.RootFailure.StepID != "create/v1" {
		t.Fatalf("RootFailure = %+v, want step create/v1", res.RootFailure)
	}
	if !strings.Contains(res.RootFailure.Message, "quota exceeded") {
		t.Fatalf("RootFailure.Message = %q", res.RootFailure.Message)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []durable.StepID{"reserve/v1", "a/v1"}
	if len(unwoundSteps) != 2 || unwoundSteps[0] != want[0] || unwoundSteps[1] != want[1] {
		t.Fatalf("unwind order = %v, want %v", unwoundSteps, want)
	}
	// A unwinds after reserve permanently failed: it must see that failure.
	if failureSeenByA.Root.StepID != "create/v1" {
		t.Fatalf("failure.Root.StepID = %q", failureSeenByA.Root.StepID)
	}
	if len(failureSeenByA.UnwindFailures) != 1 || failureSeenByA.UnwindFailures[0].StepID != "reserve/v1" {
		t.Fatalf("failure.UnwindFailures = %+v, want reserve/v1", failureSeenByA.UnwindFailures)
	}
	if len(res.UnwindFailures) != 1 || res.UnwindFailures[0].StepID != "reserve/v1" {
		t.Fatalf("Result.UnwindFailures = %+v, want reserve/v1", res.UnwindFailures)
	}
	// Failed Runs have no Pipeline Output.
	if b, _ := run.OutputBytes(context.Background()); b != nil {
		t.Fatalf("OutputBytes = %v, want nil for failed run", b)
	}
}

func TestLastErrorSurfacedDuringRetries(t *testing.T) {
	release := make(chan struct{})
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "lasterr",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				select {
				case <-release:
					return nil
				default:
					return fmt.Errorf("mounting: %w", &classifiedError{msg: "device busy"})
				}
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := run.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.LastError != "" {
			if !strings.Contains(st.LastError, "device busy") {
				t.Fatalf("LastError = %q", st.LastError)
			}
			if st.LastReason != "invalid-image" {
				t.Fatalf("LastReason = %q, want extracted from chain", st.LastReason)
			}
			if st.LastErrorAt.IsZero() {
				t.Fatal("LastErrorAt not set")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("LastError never surfaced")
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.LastError != "" || st.LastReason != "" || !st.LastErrorAt.IsZero() {
		t.Fatalf("last-error fields not cleared on resolution: %+v", st)
	}
}
