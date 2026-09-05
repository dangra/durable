// Contention: duplicate scheduling, exclusion groups, supersede
// reconciliation, and concurrency classes.
package engine_test

import (
	"context"
	"errors"
	"fmt"
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

func TestDuplicateScheduling(t *testing.T) {
	release := make(chan struct{})
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "dedup",
		Steps: []engine.StepConfig{
			stateless("wait/v1", func(ctx context.Context, inv durable.Invocation) error {
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
	var conflict *engine.ScheduleConflictError
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

func TestExclusionGroupSemantics(t *testing.T) {
	release := make(chan struct{})
	blocking := func(id durable.PipelineID, group string) *engine.Definition {
		return engine.NewDefinition(engine.DefinitionConfig{
			ID:             id,
			ExclusionGroup: group,
			Steps: []engine.StepConfig{
				stateless("s-"+durable.StepID(id)+"/v1", func(ctx context.Context, inv durable.Invocation) error {
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
	var conflict *engine.ScheduleConflictError
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
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "observed",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
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
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID: "versioned",
		Steps: []engine.StepConfig{
			{
				ID:     "apply/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
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
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
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
	var conflict *engine.ScheduleConflictError
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

func TestConcurrencyClassLimitsExecution(t *testing.T) {
	var (
		concurrent atomic.Int64
		peak       atomic.Int64
	)
	release := make(chan struct{})
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID:               "throttled-pipe",
		ConcurrencyClass: "snapshots",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
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
	e := engine.New(durabletest.NewMemStore(), fastRetry,
		engine.WithConcurrencyClass("snapshots", 1))
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
		for _, st := range []engine.Status{s1, s2} {
			switch st.State {
			case engine.RunStateThrottled:
				if st.ThrottledClass != "snapshots" {
					t.Fatalf("ThrottledClass = %q", st.ThrottledClass)
				}
				throttled++
			case engine.RunStateRunning:
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
	for _, r := range []engine.Run{run1, run2} {
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
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID:               "unlimited-pipe",
		ConcurrencyClass: "never-configured",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
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

	var runs []engine.Run
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
	def := engine.NewDefinition(engine.DefinitionConfig{
		ID:               "throttle-cancel",
		ConcurrencyClass: "narrow",
		Steps: []engine.StepConfig{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
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
	e := engine.New(durabletest.NewMemStore(), fastRetry,
		engine.WithConcurrencyClass("narrow", 1))
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
		if st.State == engine.RunStateThrottled {
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
