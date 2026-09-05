// Scheduling and time: input validation, run identity, delayed starts,
// and retention.
package engine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/storedriver"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestScheduleValidation(t *testing.T) {
	withInput := pipelinedef.New(pipelinedef.Config{
		ID:       "with-input",
		Steps:    []pipelinedef.Step{stateless("s/v1", func(context.Context, durable.Invocation) error { return nil })},
		NewInput: func() proto.Message { return &wrapperspb.StringValue{} },
	})

	// Before Start.
	e := engine.New(durabletest.NewMemStore())
	p, err := e.Bind(withInput)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, _, err := p.Schedule(context.Background(), "r", str("x")); !errors.Is(err, engine.ErrNotStarted) {
		t.Fatalf("Schedule before Start = %v, want engine.ErrNotStarted", err)
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

func TestDelayedStart(t *testing.T) {
	const delay = 80 * time.Millisecond
	var ranAt atomic.Int64
	def := pipelinedef.New(pipelinedef.Config{
		ID: "delayed",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
				ranAt.Store(time.Now().UnixNano())
				return nil
			}),
		},
	})
	_, pipes := startEngine(t, durabletest.NewMemStore(), def)
	p := pipes[0]

	scheduledAt := time.Now()
	run, created, err := p.Schedule(context.Background(), "r", nil, engine.StartAfter(delay))
	if err != nil || !created {
		t.Fatalf("Schedule = created=%v err=%v", created, err)
	}

	st, err := run.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.State != engine.RunStateScheduled {
		t.Fatalf("State = %v, want scheduled", st.State)
	}
	if st.NextAttemptAt.IsZero() {
		t.Fatal("NextAttemptAt not set for delayed run")
	}

	// The start time is not part of duplicate-scheduling identity:
	// an equivalent Schedule with a different start returns the same run.
	run2, created, err := p.Schedule(context.Background(), "r", nil, engine.StartAt(time.Now().Add(time.Hour)))
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

func TestRunIDsAreULIDs(t *testing.T) {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "ulids",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(context.Context, durable.Invocation) error { return nil }),
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

func TestRetentionReapsOnlyOldTerminalRuns(t *testing.T) {
	fake := durabletest.NewFakeClock(time.Now())
	store := durabletest.NewMemStore()

	// A nonterminal seeded run belonging to an unregistered pipeline: it
	// will be invalid under this deployment and must survive any sweep.
	invalidID := seedRun(t, store, "ghost-pipeline", map[durable.StepID]*storedriver.StepRecord{
		"g/v1": {ForwardStatus: storedriver.OpUnresolved, ForwardAttempts: 1},
	})

	blocked := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "retained",
		Steps: []pipelinedef.Step{
			stateless("s/v1", func(ctx context.Context, inv durable.Invocation) error {
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

	e := engine.New(store, fastRetry,
		engine.WithClock(fake),
		engine.WithRecoveryBackoff(0),
		engine.WithRetention(engine.RetentionPolicy{
			TerminalAfter: time.Hour,
			Interval:      time.Minute,
		}),
	)
	p, err := e.Bind(def)
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
		if _, err := store.GetRun(context.Background(), done.ID()); errors.Is(err, engine.ErrRunNotFound) {
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
