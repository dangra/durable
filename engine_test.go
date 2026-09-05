package durable_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/dangra/durable/storedriver"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/oklog/ulid/v2"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/durabletest"
)

var fastRetry = durable.WithRetryPolicy(durable.RetryPolicy{
	Initial:    time.Millisecond,
	Max:        5 * time.Millisecond,
	Multiplier: 2,
})

func str(s string) *wrapperspb.StringValue { return wrapperspb.String(s) }

func newString() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }

// refFor builds the typed reference a generator would emit for a
// state-producing step whose state is a StringValue.
func refFor(id durable.StepID) durable.StateStepRef[*wrapperspb.StringValue] {
	return durable.NewStateStepRef(id, newString)
}

func stateless(id durable.StepID, run func(context.Context, *durable.Invocation) error) durable.StepConfig {
	return durable.StepConfig{
		ID: id,
		Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			return nil, run(ctx, inv)
		},
	}
}

func startEngine(t *testing.T, store storedriver.Store, defs ...*durable.Definition) (*durable.Engine, []*durable.Pipeline) {
	t.Helper()
	e := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
	var pipes []*durable.Pipeline
	for _, d := range defs {
		p, err := d.Bind(e)
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		pipes = append(pipes, p)
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

func TestForwardSuccessWithReducer(t *testing.T) {
	selectRef := refFor("select-host/v1")
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "provision",
		Steps: []durable.StepConfig{
			stateless("validate/v1", func(ctx context.Context, inv *durable.Invocation) error {
				in, ok := inv.InputMessage().(*wrapperspb.StringValue)
				if !ok || in.GetValue() != "ord" {
					return durable.Fail(errors.New("unexpected input"))
				}
				return nil
			}),
			{
				ID:       "select-host/v1",
				HasState: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return str("host-7"), nil
				},
			},
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
		Reduce: func(v *durable.ReduceView) proto.Message {
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "retrying",
		Steps: []durable.StepConfig{
			stateless("flaky/v1", func(ctx context.Context, inv *durable.Invocation) error {
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
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "panicky",
		Steps: []durable.StepConfig{
			stateless("boom/v1", func(ctx context.Context, inv *durable.Invocation) error {
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

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "failing",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
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
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return str("res-42"), nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
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
			stateless("create/v1", func(ctx context.Context, inv *durable.Invocation) error {
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

func TestDuplicateScheduling(t *testing.T) {
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "dedup",
		Steps: []durable.StepConfig{
			stateless("wait/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	p := pipes[0]

	run1, created, err := p.Schedule(context.Background(), "res-1", str("in"))
	if err != nil || !created {
		t.Fatalf("first Schedule = created=%v err=%v", created, err)
	}
	run2, created, err := p.Schedule(context.Background(), "res-1", str("in"))
	if err != nil || created {
		t.Fatalf("equivalent Schedule = created=%v err=%v", created, err)
	}
	if run1.ID() != run2.ID() {
		t.Fatalf("equivalent Schedule returned %s, want %s", run2.ID(), run1.ID())
	}

	_, created, err = p.Schedule(context.Background(), "res-1", str("different"))
	var conflict *durable.ScheduleConflictError
	if !errors.As(err, &conflict) || created {
		t.Fatalf("conflicting Schedule = created=%v err=%v, want ScheduleConflictError", created, err)
	}
	if conflict.RunID != run1.ID() {
		t.Fatalf("conflict.RunID = %s, want %s", conflict.RunID, run1.ID())
	}

	// A different resource is a different slot.
	_, created, err = p.Schedule(context.Background(), "res-2", str("in"))
	if err != nil || !created {
		t.Fatalf("other-slot Schedule = created=%v err=%v", created, err)
	}

	close(release)
	if res, err := run1.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}

	// With the slot free again, scheduling creates a new Run.
	run3, created, err := p.Schedule(context.Background(), "res-1", str("in"))
	if err != nil || !created {
		t.Fatalf("post-terminal Schedule = created=%v err=%v", created, err)
	}
	if run3.ID() == run1.ID() {
		t.Fatal("post-terminal Schedule reused RunID")
	}
}

func TestScheduleValidation(t *testing.T) {
	withInput := durable.NewDefinition(durable.DefinitionConfig{
		ID:       "with-input",
		Steps:    []durable.StepConfig{stateless("s/v1", func(context.Context, *durable.Invocation) error { return nil })},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})

	// Before Start.
	e := durable.NewEngine(durabletest.NewMemStore())
	p, err := withInput.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, _, err := p.Schedule(context.Background(), "r", str("x")); !errors.Is(err, durable.ErrEngineNotStarted) {
		t.Fatalf("Schedule before Start = %v, want ErrEngineNotStarted", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	// Nil input for an input-declaring pipeline.
	if _, _, err := p.Schedule(context.Background(), "r", nil); err == nil {
		t.Fatal("Schedule with nil input succeeded, want error")
	}
}

func TestRecoveryResumesAcrossEngines(t *testing.T) {
	store := durabletest.NewMemStore()
	var attempts atomic.Uint64

	makeDef := func(succeed bool) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: "recoverable",
			Steps: []durable.StepConfig{
				stateless("only/v1", func(ctx context.Context, inv *durable.Invocation) error {
					attempts.Store(inv.Attempt())
					if !succeed {
						return errors.New("still deploying")
					}
					return nil
				}),
			},
		})
	}

	// Deployment 1: the step never succeeds.
	e1 := durable.NewEngine(store, fastRetry)
	p1, _ := makeDef(false).Bind(e1)
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("step never retried")
		}
		time.Sleep(time.Millisecond)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}
	attemptsAtShutdown := attempts.Load()

	// Deployment 2: same store, corrected handler.
	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
	p2, _ := makeDef(true).Bind(e2)
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start2: %v", err)
	}
	defer e2.Stop(context.Background())

	run2, err := p2.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success after recovery", res, err)
	}
	if attempts.Load() <= attemptsAtShutdown {
		t.Fatalf("attempt numbering restarted: %d <= %d", attempts.Load(), attemptsAtShutdown)
	}
}

// seedRun persists a nonterminal record with pre-existing execution facts,
// simulating a Run started under an earlier deployment.
func seedRun(t *testing.T, store storedriver.Store, pipeline durable.PipelineID, steps map[durable.StepID]*storedriver.StepRecord) durable.RunID {
	t.Helper()
	rec := &storedriver.RunRecord{
		RunID:      durable.RunID("seeded-" + t.Name()),
		PipelineID: pipeline,
		ResourceID: "seed-resource",
		Phase:      durable.PhaseForward,
		Steps:      steps,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if _, created, err := store.CreateRun(context.Background(), rec); err != nil || !created {
		t.Fatalf("seeding run: created=%v err=%v", created, err)
	}
	return rec.RunID
}

func TestRetiredStepIsBypassed(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
	})

	var bRan, cRan atomic.Bool
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
			stateless("a/v1", func(context.Context, *durable.Invocation) error { return nil }),
			{
				ID:      "b/v1",
				Retired: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					bRan.Store(true)
					return nil, nil
				},
			},
			stateless("c/v1", func(context.Context, *durable.Invocation) error {
				cRan.Store(true)
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, store, def)

	run, err := pipes[0].Run(context.Background(), runID)
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if bRan.Load() {
		t.Fatal("retired never-started step executed")
	}
	if !cRan.Load() {
		t.Fatal("step after retired step did not execute")
	}
}

func TestRetiredUnresolvedStepContinues(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
		"b/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 2},
	})

	var bAttempt atomic.Uint64
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
			stateless("a/v1", func(context.Context, *durable.Invocation) error { return nil }),
			{
				ID:      "b/v1",
				Retired: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					bAttempt.Store(inv.Attempt())
					return nil, nil
				},
			},
		},
	})
	_, pipes := startEngine(t, store, def)

	run, _ := pipes[0].Run(context.Background(), runID)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if got := bAttempt.Load(); got != 3 {
		t.Fatalf("retired unresolved step ran with attempt %d, want 3 (continuing prior attempts)", got)
	}
}

func TestUnresolvedStepRemovedIsInvalid(t *testing.T) {
	store := durabletest.NewMemStore()
	runID := seedRun(t, store, "evolving", map[durable.StepID]*storedriver.StepRecord{
		"a/v1": {ForwardStatus: storedriver.OpSucceeded},
		"b/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 1},
	})

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "evolving",
		Steps: []durable.StepConfig{
			stateless("a/v1", func(context.Context, *durable.Invocation) error { return nil }),
			// b/v1 removed while unresolved.
			stateless("c/v1", func(context.Context, *durable.Invocation) error { return nil }),
		},
	})
	_, pipes := startEngine(t, store, def)

	run, _ := pipes[0].Run(context.Background(), runID)
	_, err := run.Wait(context.Background())
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != durable.RunStateInvalid || st.InvalidReason == "" {
		t.Fatalf("Status = %+v, want RunStateInvalid with reason", st)
	}
}

func TestInvalidReducerRepairedByRedeploy(t *testing.T) {
	store := durabletest.NewMemStore()

	makeDef := func(broken bool) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: "reduced",
			Steps: []durable.StepConfig{
				stateless("s/v1", func(context.Context, *durable.Invocation) error { return nil }),
			},
			Reduce: func(v *durable.ReduceView) proto.Message {
				if broken {
					panic("bad reducer")
				}
				return str("ok")
			},
		})
	}

	// Deployment 1: broken reducer invalidates the Run without retry loops.
	e1 := durable.NewEngine(store, fastRetry)
	p1, _ := makeDef(true).Bind(e1)
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	_, err = run.Wait(context.Background())
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}

	// Deployment 2: corrected reducer completes the same nonterminal Run.
	e2 := durable.NewEngine(store, fastRetry, durable.WithRecoveryBackoff(0))
	p2, _ := makeDef(false).Bind(e2)
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start2: %v", err)
	}
	defer e2.Stop(context.Background())

	run2, err := p2.Run(context.Background(), run.ID())
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	res, err := run2.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait after repair = %+v, %v; want success", res, err)
	}
}

func TestNilStateFromStateProducingHandlerIsInvalid(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "nilstate",
		Steps: []durable.StepConfig{
			{
				ID:       "s/v1",
				HasState: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
			},
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	_, err := run.Wait(context.Background())
	if _, ok := errors.AsType[*durable.InvalidRunError](err); !ok {
		t.Fatalf("Wait = %v, want InvalidRunError", err)
	}
}

func TestStepOwnershipIsExclusive(t *testing.T) {
	e := durable.NewEngine(durabletest.NewMemStore())
	mk := func(pipeline durable.PipelineID) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: pipeline,
			Steps: []durable.StepConfig{
				stateless("shared/v1", func(context.Context, *durable.Invocation) error { return nil }),
			},
		})
	}
	if _, err := mk("p1").Bind(e); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if _, err := mk("p2").Bind(e); err == nil {
		t.Fatal("second Bind sharing a step succeeded, want error")
	}
}

func TestMiddlewareOrderingAndPhases(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	mw := func(name string) durable.Middleware {
		return func(next durable.Handler) durable.Handler {
			return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				record(fmt.Sprintf("%s:in:%v:%s:%d", name, inv.Phase(), inv.StepID(), inv.Attempt()))
				state, err := next(ctx, inv)
				record(name + ":out")
				return state, err
			}
		}
	}

	store := durabletest.NewMemStore()
	e := durable.NewEngine(store, fastRetry, durable.WithMiddleware(mw("outer"), mw("inner")))
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "mw",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return nil
				},
			},
			stateless("b/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New("transient")
				}
				return durable.Fail(errors.New("permanent"))
			}),
		},
	})
	p, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", res, err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Four operations, each an onion of outer(inner(handler)):
	// a.Run, b.Run attempt 1 (retry), b.Run attempt 2 (Fail), a.Unwind.
	want := []string{
		"outer:in:forward:a/v1:1", "inner:in:forward:a/v1:1", "inner:out", "outer:out",
		"outer:in:forward:b/v1:1", "inner:in:forward:b/v1:1", "inner:out", "outer:out",
		"outer:in:forward:b/v1:2", "inner:in:forward:b/v1:2", "inner:out", "outer:out",
		"outer:in:unwind:a/v1:1", "inner:in:unwind:a/v1:1", "inner:out", "outer:out",
	}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events[%d] = %q, want %q (all: %v)", i, events[i], want[i], events)
		}
	}
}

func TestMiddlewareCanEscalateToFail(t *testing.T) {
	escalate := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			state, err := next(ctx, inv)
			if err != nil {
				return state, durable.Fail(err)
			}
			return state, err
		}
	}
	var attempts atomic.Uint64
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "escalating",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				attempts.Store(inv.Attempt())
				return errors.New("would ordinarily retry")
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry, durable.WithMiddleware(escalate))
	p, _ := def.Bind(e)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want permanent failure via middleware", res, err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry after escalation)", attempts.Load())
	}
}

type ctxKey struct{}

func TestMiddlewareContextReachesHandlers(t *testing.T) {
	inject := func(next durable.Handler) durable.Handler {
		return func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
			return next(context.WithValue(ctx, ctxKey{}, "present"), inv)
		}
	}
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "ctxpipe",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if ctx.Value(ctxKey{}) != "present" {
					return durable.Fail(errors.New("middleware context value missing"))
				}
				return nil
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry, durable.WithMiddleware(inject))
	p, _ := def.Bind(e)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run, _, _ := p.Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success", res, err)
	}
}

// classifiedError carries its own attribution, the way domain error types
// implement FailureKinder/FailureReasoner once so resolution sites stay
// plain Fail(err).
type classifiedError struct{ msg string }

func (e *classifiedError) Error() string                    { return e.msg }
func (e *classifiedError) FailureKind() durable.FailureKind { return durable.FailureKindUser }
func (e *classifiedError) FailureReason() string            { return "invalid-image" }

func failingRun(t *testing.T, id durable.PipelineID, fail error) durable.Result {
	t.Helper()
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: id,
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				return fail
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, err := pipes[0].Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", res, err)
	}
	return res
}

func TestFailureAttribution(t *testing.T) {
	t.Run("defaults to system with no reason", func(t *testing.T) {
		res := failingRun(t, "attr-default", durable.Fail(errors.New("boom")))
		if res.RootFailure.Kind != durable.FailureKindSystem || res.RootFailure.Reason != "" {
			t.Fatalf("RootFailure = %+v, want system kind, empty reason", res.RootFailure)
		}
	})
	t.Run("explicit options", func(t *testing.T) {
		res := failingRun(t, "attr-opts", durable.Fail(errors.New("bad region"),
			durable.WithUserKind(), durable.WithReason("invalid-input")))
		if res.RootFailure.Kind != durable.FailureKindUser || res.RootFailure.Reason != "invalid-input" {
			t.Fatalf("RootFailure = %+v, want user/invalid-input", res.RootFailure)
		}
	})
	t.Run("extracted from error chain", func(t *testing.T) {
		wrapped := fmt.Errorf("preparing image: %w", &classifiedError{msg: "no manifest"})
		res := failingRun(t, "attr-chain", durable.Fail(wrapped))
		if res.RootFailure.Kind != durable.FailureKindUser || res.RootFailure.Reason != "invalid-image" {
			t.Fatalf("RootFailure = %+v, want user/invalid-image from chain", res.RootFailure)
		}
	})
	t.Run("options override the chain", func(t *testing.T) {
		res := failingRun(t, "attr-precedence", durable.Fail(&classifiedError{msg: "x"},
			durable.WithReason("overridden")))
		if res.RootFailure.Reason != "overridden" || res.RootFailure.Kind != durable.FailureKindUser {
			t.Fatalf("RootFailure = %+v, want reason overridden, kind still from chain", res.RootFailure)
		}
	})
}

func TestUnwindFailureAttribution(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "attr-unwind",
		Steps: []durable.StepConfig{
			{
				ID:     "a/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					return durable.Fail(errors.New("release rejected"), durable.WithReason("release-rejected"))
				},
			},
			stateless("b/v1", func(ctx context.Context, inv *durable.Invocation) error {
				return durable.Fail(errors.New("nope"))
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	res, err := run.Wait(context.Background())
	if err != nil || !res.Failed() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if len(res.UnwindFailures) != 1 || res.UnwindFailures[0].Reason != "release-rejected" ||
		res.UnwindFailures[0].Kind != durable.FailureKindSystem {
		t.Fatalf("UnwindFailures = %+v, want system/release-rejected", res.UnwindFailures)
	}
}

func TestLastErrorSurfacedDuringRetries(t *testing.T) {
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "lasterr",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
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

func TestDelayedStart(t *testing.T) {
	const delay = 80 * time.Millisecond
	var ranAt atomic.Int64
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "delayed",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				ranAt.Store(time.Now().UnixNano())
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	p := pipes[0]

	scheduledAt := time.Now()
	run, created, err := p.Schedule(context.Background(), "r", nil, durable.StartAfter(delay))
	if err != nil || !created {
		t.Fatalf("Schedule = created=%v err=%v", created, err)
	}

	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != durable.RunStateScheduled {
		t.Fatalf("State = %v, want scheduled", st.State)
	}
	if st.NextAttemptAt.IsZero() {
		t.Fatal("NextAttemptAt not set for delayed run")
	}

	// The start time is not part of duplicate-scheduling identity:
	// an equivalent Schedule with a different start returns the same run.
	run2, created, err := p.Schedule(context.Background(), "r", nil, durable.StartAt(time.Now().Add(time.Hour)))
	if err != nil || created || run2.ID() != run.ID() {
		t.Fatalf("dedup Schedule = %s created=%v err=%v, want existing %s", run2.ID(), created, err, run.ID())
	}

	res, err := run.Wait(context.Background())
	if err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if elapsed := time.Unix(0, ranAt.Load()).Sub(scheduledAt); elapsed < delay-10*time.Millisecond {
		t.Fatalf("step ran after %v, want >= ~%v", elapsed, delay)
	}
}

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

func TestRunIDsAreULIDs(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "ulids",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(context.Context, *durable.Invocation) error { return nil }),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)

	before := time.Now().Add(-time.Second)
	run1, _, err := pipes[0].Schedule(context.Background(), "r1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	run2, _, err := pipes[0].Schedule(context.Background(), "r2", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	id1, err := ulid.Parse(string(run1.ID()))
	if err != nil {
		t.Fatalf("RunID %q is not a ULID: %v", run1.ID(), err)
	}
	id2, err := ulid.Parse(string(run2.ID()))
	if err != nil {
		t.Fatalf("RunID %q is not a ULID: %v", run2.ID(), err)
	}
	if id1 == id2 {
		t.Fatal("distinct runs share a RunID")
	}
	if at := ulid.Time(id1.Time()); at.Before(before) || at.After(time.Now().Add(time.Second)) {
		t.Fatalf("embedded timestamp %v not near now", at)
	}
}

func TestExclusionGroupSemantics(t *testing.T) {
	release := make(chan struct{})
	blocking := func(id durable.PipelineID, group string) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID:             id,
			ExclusionGroup: group,
			Steps: []durable.StepConfig{
				stateless("s-"+durable.StepID(id)+"/v1", func(ctx context.Context, inv *durable.Invocation) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}),
			},
			NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
		})
	}
	_, pipes := startEngine(t, durabletest.NewMemStore(),
		blocking("grp-a", "lifecycle"),
		blocking("grp-b", "lifecycle"),
		blocking("solo", ""),
	)
	a, b, solo := pipes[0], pipes[1], pipes[2]

	runA, created, err := a.Schedule(context.Background(), "res", str("in"))
	if err != nil || !created {
		t.Fatalf("a.Schedule = created=%v err=%v", created, err)
	}

	// Same pipeline, equivalent input: dedup still applies inside a group.
	runA2, created, err := a.Schedule(context.Background(), "res", str("in"))
	if err != nil || created || runA2.ID() != runA.ID() {
		t.Fatalf("a dedup = %s created=%v err=%v", runA2.ID(), created, err)
	}

	// Group sibling: always a conflict, even with equivalent input.
	_, created, err = b.Schedule(context.Background(), "res", str("in"))
	var conflict *durable.ScheduleConflictError
	if !errors.As(err, &conflict) || created {
		t.Fatalf("b.Schedule = created=%v err=%v, want conflict", created, err)
	}
	if conflict.PipelineID != "grp-a" || conflict.RunID != runA.ID() {
		t.Fatalf("conflict = %+v", conflict)
	}

	// A pipeline outside the group shares the resource freely.
	if _, created, err := solo.Schedule(context.Background(), "res", str("in")); err != nil || !created {
		t.Fatalf("solo.Schedule = created=%v err=%v", created, err)
	}

	close(release)
	if res, err := runA.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if _, created, err := b.Schedule(context.Background(), "res", str("in")); err != nil || !created {
		t.Fatalf("post-terminal b.Schedule = created=%v err=%v", created, err)
	}
}

func TestActiveRun(t *testing.T) {
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "observed",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	p := pipes[0]

	if _, ok, err := p.ActiveRun(context.Background(), "r"); err != nil || ok {
		t.Fatalf("ActiveRun before schedule = ok=%v err=%v", ok, err)
	}
	run, _, err := p.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	got, ok, err := p.ActiveRun(context.Background(), "r")
	if err != nil || !ok || got.ID() != run.ID() {
		t.Fatalf("ActiveRun = %s ok=%v err=%v, want %s", got.ID(), ok, err, run.ID())
	}
	close(release)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	if _, ok, err := p.ActiveRun(context.Background(), "r"); err != nil || ok {
		t.Fatalf("ActiveRun after terminal = ok=%v err=%v", ok, err)
	}
}

// TestSupersedeReconcile exercises the full reconcile-loop toolkit: a newer
// intent hits a conflict, inspects the blocking run's input, finds it
// stale, cancels it (unwinding its work), and reschedules.
func TestSupersedeReconcile(t *testing.T) {
	var mu sync.Mutex
	var unwound []string
	started := make(chan struct{})
	var startedOnce sync.Once
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "versioned",
		Steps: []durable.StepConfig{
			{
				ID:     "apply/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					in := inv.InputMessage().(*wrapperspb.StringValue)
					if in.GetValue() == "v1" {
						// The stale run holds the slot until preempted,
						// then resolves fast so cancellation can proceed.
						if inv.CancelRequested() {
							return nil, nil
						}
						startedOnce.Do(func() { close(started) })
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv *durable.Invocation, f durable.Failure) error {
					in := inv.InputMessage().(*wrapperspb.StringValue)
					mu.Lock()
					unwound = append(unwound, in.GetValue())
					mu.Unlock()
					return nil
				},
			},
		},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	p := pipes[0]

	stale, _, err := p.Schedule(context.Background(), "res", str("v1"))
	if err != nil {
		t.Fatalf("Schedule v1: %v", err)
	}
	<-started // the stale run's operation is in flight

	// The reconcile loop delivers newer intent.
	_, _, err = p.Schedule(context.Background(), "res", str("v2"))
	var conflict *durable.ScheduleConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("Schedule v2 = %v, want conflict", err)
	}

	// Inspect the blocker: is it doing older or newer work?
	blocker, err := p.Run(context.Background(), conflict.RunID)
	if err != nil {
		t.Fatalf("Run lookup: %v", err)
	}
	b, err := blocker.InputBytes(context.Background())
	if err != nil {
		t.Fatalf("InputBytes: %v", err)
	}
	blockerInput := &wrapperspb.StringValue{}
	if err := proto.Unmarshal(b, blockerInput); err != nil {
		t.Fatalf("unmarshal blocker input: %v", err)
	}
	if blockerInput.GetValue() != "v1" {
		t.Fatalf("blocker input = %q, want v1", blockerInput.GetValue())
	}

	// Stale: cancel it, let unwind clean up, then reschedule.
	if err := blocker.Cancel(context.Background(), "superseded by v2"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if res, err := stale.Wait(context.Background()); err != nil || !res.Canceled() {
		t.Fatalf("stale Wait = %+v, %v; want canceled", res, err)
	}
	fresh, created, err := p.Schedule(context.Background(), "res", str("v2"))
	if err != nil || !created {
		t.Fatalf("reschedule v2 = created=%v err=%v", created, err)
	}
	if res, err := fresh.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("v2 Wait = %+v, %v", res, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(unwound) != 1 || unwound[0] != "v1" {
		t.Fatalf("unwound = %v, want the stale v1 work", unwound)
	}
}

func TestRetentionReapsOnlyOldTerminalRuns(t *testing.T) {
	fake := durabletest.NewFakeClock(time.Now())
	store := durabletest.NewMemStore()

	// A nonterminal seeded run belonging to an unregistered pipeline: it
	// will be invalid under this deployment and must survive any sweep.
	invalidID := seedRun(t, store, "ghost-pipeline", map[durable.StepID]*storedriver.StepRecord{
		"g/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 1},
	})

	blocked := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "retained",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.ResourceID() == "stuck" {
					select {
					case <-blocked:
					case <-ctx.Done():
					}
					return ctx.Err()
				}
				return nil
			}),
		},
	})

	e := durable.NewEngine(store, fastRetry,
		durable.WithClock(fake),
		durable.WithRecoveryBackoff(0),
		durable.WithRetention(durable.RetentionPolicy{
			TerminalAfter: time.Hour,
			Interval:      time.Minute,
		}),
	)
	p, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		close(blocked)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	}()

	// One run completes now; one stays nonterminal forever.
	done, _, err := p.Schedule(context.Background(), "done", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if res, err := done.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v", res, err)
	}
	stuck, _, err := p.Schedule(context.Background(), "stuck", nil)
	if err != nil {
		t.Fatalf("Schedule stuck: %v", err)
	}

	// Advance the fake clock past the retention window until the sweep
	// reaps the terminal run.
	deadline := time.Now().Add(5 * time.Second)
	for {
		fake.Advance(2 * time.Hour)
		if _, err := store.GetRun(context.Background(), done.ID()); errors.Is(err, durable.ErrRunNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal run never reaped")
		}
		time.Sleep(time.Millisecond)
	}

	// Nonterminal runs survive regardless of age — the stuck one and the
	// invalid one alike.
	if _, err := store.GetRun(context.Background(), stuck.ID()); err != nil {
		t.Fatalf("stuck run reaped: %v", err)
	}
	if _, err := store.GetRun(context.Background(), invalidID); err != nil {
		t.Fatalf("invalid run reaped: %v", err)
	}
}

// The wait-for-existing shape (flyd monitor.go: drain WAIT_FOR_INIT).
func TestAwaitRunParksUntilTargetCompletes(t *testing.T) {
	release := make(chan struct{})
	var waiterAttempts atomic.Uint64

	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "await-target",
		Steps: []durable.StepConfig{
			stateless("t/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	var waiterDef *durable.Definition
	var targetPipe *durable.Pipeline
	waiterDef = durable.NewDefinition(durable.DefinitionConfig{
		ID: "await-waiter",
		Steps: []durable.StepConfig{
			stateless("w/v1", func(ctx context.Context, inv *durable.Invocation) error {
				waiterAttempts.Store(inv.Attempt())
				run, ok, err := targetPipe.ActiveRun(ctx, "res")
				if err != nil {
					return err
				}
				if ok {
					return durable.AwaitRun(run.ID())
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), target, waiterDef)
	targetPipe = pipes[0]

	tRun, _, err := targetPipe.Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	wRun, _, err := pipes[1].Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}

	// The waiter parks: RunStateAwaiting, pointing at the target.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := wRun.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.State == durable.RunStateAwaiting {
			if st.AwaitingRunID != tRun.ID() {
				t.Fatalf("AwaitingRunID = %s, want %s", st.AwaitingRunID, tRun.ID())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiter never parked; state %v", st.State)
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	if res, err := wRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("waiter Wait = %+v, %v", res, err)
	}
	// Exactly two attempts: the parking one and the wake.
	if got := waiterAttempts.Load(); got != 2 {
		t.Fatalf("waiter attempts = %d, want 2 (park + wake, no polling)", got)
	}
}

// The create-then-wait shape (flyd runServiceReconciler): AwaitedRunID
// prevents respawning the child on re-execution.
func TestAwaitedRunIDPreventsChildRespawn(t *testing.T) {
	var sawAwaited atomic.Bool
	child := durable.NewDefinition(durable.DefinitionConfig{
		ID: "spawn-child",
		Steps: []durable.StepConfig{
			stateless("c/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})
	var childPipe *durable.Pipeline
	parent := durable.NewDefinition(durable.DefinitionConfig{
		ID: "spawn-parent",
		Steps: []durable.StepConfig{
			stateless("p/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if _, ok := inv.AwaitedRunID(); ok {
					sawAwaited.Store(true)
					return nil // child completed; do not respawn
				}
				run, _, err := childPipe.Schedule(ctx, "child-res", nil)
				if conflict, ok := errors.AsType[*durable.ScheduleConflictError](err); ok {
					return durable.AwaitRun(conflict.RunID)
				}
				if err != nil {
					return err
				}
				return durable.AwaitRun(run.ID())
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), child, parent)
	childPipe = pipes[0]

	pRun, _, err := pipes[1].Schedule(context.Background(), "parent-res", nil)
	if err != nil {
		t.Fatalf("Schedule parent: %v", err)
	}
	if res, err := pRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("parent Wait = %+v, %v", res, err)
	}
	if !sawAwaited.Load() {
		t.Fatal("wake attempt did not observe AwaitedRunID")
	}
	// Exactly one child was ever created.
	children, err := childPipe.Runs(context.Background(), "child-res")
	if err != nil {
		t.Fatalf("Runs: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("children = %d, want exactly 1 (no respawn loop)", len(children))
	}
}

func TestAwaitRunResolvesImmediatelyForMissingTarget(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "await-missing",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if id, ok := inv.AwaitedRunID(); ok {
					if id != "no-such-run" {
						return durable.Fail(errors.New("wrong awaited id"))
					}
					return nil
				}
				return durable.AwaitRun("no-such-run")
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	run, _, _ := pipes[0].Schedule(context.Background(), "r", nil)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want immediate resolution", res, err)
	}
}

func TestAwaitCycleIsInvalid(t *testing.T) {
	store := durabletest.NewMemStore()
	var pipeA, pipeB *durable.Pipeline
	mk := func(id durable.PipelineID, other **durable.Pipeline, otherRes durable.ResourceID) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: id,
			Steps: []durable.StepConfig{
				stateless(durable.StepID(string(id)+"/v1"), func(ctx context.Context, inv *durable.Invocation) error {
					if _, ok := inv.AwaitedRunID(); ok {
						return nil
					}
					run, ok, err := (*other).ActiveRun(ctx, otherRes)
					if err != nil {
						return err
					}
					if !ok {
						return errors.New("peer not scheduled yet") // retry
					}
					return durable.AwaitRun(run.ID())
				}),
			},
		})
	}
	defA := mk("cycle-a", &pipeB, "res-b")
	defB := mk("cycle-b", &pipeA, "res-a")
	_, pipes := startEngine(t, store, defA, defB)
	pipeA, pipeB = pipes[0], pipes[1]

	runA, _, err := pipeA.Schedule(context.Background(), "res-a", nil)
	if err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	runB, _, err := pipeB.Schedule(context.Background(), "res-b", nil)
	if err != nil {
		t.Fatalf("Schedule B: %v", err)
	}

	// One of the two must be rejected as an await cycle.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var invalid int
		for _, r := range []durable.Run{runA, runB} {
			st, err := r.Status(context.Background())
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if st.State == durable.RunStateInvalid {
				if !strings.Contains(st.InvalidReason, "await cycle") {
					t.Fatalf("InvalidReason = %q", st.InvalidReason)
				}
				invalid++
			}
		}
		if invalid > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no run was marked invalid for the await cycle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCancelCutsThroughAwait(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	target := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-await-target",
		Steps: []durable.StepConfig{
			stateless("t/v1", func(ctx context.Context, inv *durable.Invocation) error {
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	var targetPipe *durable.Pipeline
	waiter := durable.NewDefinition(durable.DefinitionConfig{
		ID: "cancel-await-waiter",
		Steps: []durable.StepConfig{
			stateless("w/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.CancelRequested() {
					return nil
				}
				run, ok, err := targetPipe.ActiveRun(ctx, "res")
				if err != nil {
					return err
				}
				if ok {
					return durable.AwaitRun(run.ID())
				}
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), target, waiter)
	targetPipe = pipes[0]

	if _, _, err := targetPipe.Schedule(context.Background(), "res", nil); err != nil {
		t.Fatalf("Schedule target: %v", err)
	}
	wRun, _, err := pipes[1].Schedule(context.Background(), "res", nil)
	if err != nil {
		t.Fatalf("Schedule waiter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := wRun.Status(context.Background())
		if st.State == durable.RunStateAwaiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never parked")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel the parked run: it must resolve without the target finishing.
	if err := wRun.Cancel(context.Background(), "no longer needed"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := wRun.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled while target still running", res, err)
	}
}

func TestConcurrencyClassLimitsExecution(t *testing.T) {
	var (
		concurrent atomic.Int64
		peak       atomic.Int64
	)
	release := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID:               "throttled-pipe",
		ConcurrencyClass: "snapshots",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				n := concurrent.Add(1)
				defer concurrent.Add(-1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithConcurrencyClass("snapshots", 1))
	p, err := def.Bind(e)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	run1, _, err := p.Schedule(context.Background(), "r1", nil)
	if err != nil {
		t.Fatalf("Schedule r1: %v", err)
	}
	run2, _, err := p.Schedule(context.Background(), "r2", nil)
	if err != nil {
		t.Fatalf("Schedule r2: %v", err)
	}

	// One executes; the other parks as throttled with the class name.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s1, _ := run1.Status(context.Background())
		s2, _ := run2.Status(context.Background())
		throttled, running := 0, 0
		for _, st := range []durable.Status{s1, s2} {
			switch st.State {
			case durable.RunStateThrottled:
				if st.ThrottledClass != "snapshots" {
					t.Fatalf("ThrottledClass = %q", st.ThrottledClass)
				}
				throttled++
			case durable.RunStateRunning:
				running++
			}
		}
		if throttled == 1 && running == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("states = %v/%v, want one running one throttled", s1.State, s2.State)
		}
		time.Sleep(time.Millisecond)
	}

	// Releasing lets both complete, never exceeding the capacity.
	close(release)
	for _, r := range []durable.Run{run1, run2} {
		if res, err := r.Wait(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("Wait = %+v, %v", res, err)
		}
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrent executions = %d, want 1", got)
	}
}

func TestUnconfiguredClassIsUnlimited(t *testing.T) {
	const parallel = 4
	var (
		concurrent atomic.Int64
		peak       atomic.Int64
	)
	gate := make(chan struct{})
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID:               "unlimited-pipe",
		ConcurrencyClass: "never-configured",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				n := concurrent.Add(1)
				defer concurrent.Add(-1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)

	var runs []durable.Run
	for i := range parallel {
		r, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("r%d", i)), nil)
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		runs = append(runs, r)
	}
	deadline := time.Now().Add(5 * time.Second)
	for concurrent.Load() < parallel {
		if time.Now().After(deadline) {
			t.Fatalf("concurrent = %d, want %d (class should be unlimited)", concurrent.Load(), parallel)
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)
	for _, r := range runs {
		if res, err := r.Wait(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("Wait = %+v, %v", res, err)
		}
	}
}

func TestCancelBypassesThrottle(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	holderEntered := make(chan struct{})
	var enteredOnce sync.Once
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID:               "throttle-cancel",
		ConcurrencyClass: "narrow",
		Steps: []durable.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.CancelRequested() {
					return nil
				}
				if inv.ResourceID() == "holder" {
					enteredOnce.Do(func() { close(holderEntered) })
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}),
		},
	})
	e := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithConcurrencyClass("narrow", 1))
	p, _ := def.Bind(e)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer e.Stop(context.Background())

	if _, _, err := p.Schedule(context.Background(), "holder", nil); err != nil {
		t.Fatalf("Schedule holder: %v", err)
	}
	// Schedule order does not determine dispatch order: wait until the
	// holder actually occupies the class token before adding a contender,
	// or the roles can flip and the poll below waits on the wrong run.
	<-holderEntered
	parked, _, err := p.Schedule(context.Background(), "parked", nil)
	if err != nil {
		t.Fatalf("Schedule parked: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _ := parked.Status(context.Background())
		if st.State == durable.RunStateThrottled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second run never throttled")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancellation cuts through the throttle: the parked run resolves
	// while the token holder still executes.
	if err := parked.Cancel(context.Background(), "not needed"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	res, err := parked.Wait(context.Background())
	if err != nil || !res.Canceled() {
		t.Fatalf("Wait = %+v, %v; want canceled", res, err)
	}
}

// TestBindAfterStartRejected pins the registration freeze that the
// engine's lock-free pipelines read rests on: once Start has run,
// binding another definition must fail with ErrEngineStarted — while
// runs are actively executing, so under the race detector this also
// covers the exact interleaving of a rejected concurrent registration
// attempt against unlocked pipeline lookups.
func TestBindAfterStartRejected(t *testing.T) {
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "frozen",
		Steps: []durable.StepConfig{
			stateless("step/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})
	e, pipes := startEngine(t, durabletest.NewMemStore(), def)

	latecomer := durable.NewDefinition(durable.DefinitionConfig{
		ID: "latecomer",
		Steps: []durable.StepConfig{
			stateless("late/v1", func(ctx context.Context, inv *durable.Invocation) error { return nil }),
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID(fmt.Sprintf("res-%d", i)), nil)
			if err != nil {
				t.Errorf("Schedule: %v", err)
				return
			}
			if _, err := run.Wait(context.Background()); err != nil {
				t.Errorf("Wait: %v", err)
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := latecomer.Bind(e); !errors.Is(err, durable.ErrEngineStarted) {
				t.Errorf("Bind after Start = %v, want ErrEngineStarted; the pipelines freeze is broken", err)
			}
		}()
	}
	wg.Wait()
}

// TestInvalidUTF8ErrorsDoNotWedge pins the sanitization contract found
// by the storage fuzzer: handler errors, failure reasons, and cancel
// causes may contain invalid UTF-8, which protobuf string fields reject
// — recorded raw, the durable transition could never marshal and the
// Run would wedge in a store-retry loop. The engine must sanitize and
// carry on, through both the retry and the permanent-failure paths.
func TestInvalidUTF8ErrorsDoNotWedge(t *testing.T) {
	raw := "raw \xff\xfe bytes"
	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "utf8",
		Steps: []durable.StepConfig{
			stateless("flaky/v1", func(ctx context.Context, inv *durable.Invocation) error {
				if inv.Attempt() == 1 {
					return errors.New(raw) // ordinary error -> LastError
				}
				return nil
			}),
			stateless("explode/v1", func(ctx context.Context, inv *durable.Invocation) error {
				return durable.Fail(errors.New(raw), durable.WithReason(raw))
			}),
		},
	})
	// The bbolt store is the one that actually marshals to proto.
	store, err := bboltstore.Open(filepath.Join(t.TempDir(), "utf8.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	_, pipes := startEngine(t, store, def)

	run, _, err := pipes[0].Schedule(context.Background(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	res, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v (a wedged marshal would hang, not return)", err)
	}
	if res.Outcome != durable.OutcomeFailure || res.RootFailure == nil {
		t.Fatalf("result = %+v", res)
	}
	if !utf8.ValidString(res.RootFailure.Message) || !utf8.ValidString(res.RootFailure.Reason) {
		t.Fatalf("unsanitized failure text: %+v", res.RootFailure)
	}

	// Invalid UTF-8 identifiers are rejected upfront instead.
	if _, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID("res\xff"), nil); err == nil {
		t.Fatal("Schedule accepted an invalid-UTF-8 resource id")
	}
	if _, _, err := pipes[0].Schedule(context.Background(), durable.ResourceID("res\x00x"), nil); err == nil {
		t.Fatal("Schedule accepted a NUL resource id")
	}
}
