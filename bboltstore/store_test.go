package bboltstore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSlotSemantics(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()

	rec := &durable.RunRecord{
		RunID:      "run-1",
		PipelineID: "p",
		ResourceID: "r",
		Phase:      durable.PhaseForward,
		CreatedAt:  time.Now(),
	}
	if _, created, err := s.CreateRun(ctx, rec); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}

	// The slot is occupied.
	dup := &durable.RunRecord{RunID: "run-2", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	existing, created, err := s.CreateRun(ctx, dup)
	if err != nil || created {
		t.Fatalf("second CreateRun = created=%v err=%v", created, err)
	}
	if existing.RunID != "run-1" {
		t.Fatalf("existing.RunID = %s, want run-1", existing.RunID)
	}

	// Facts round-trip.
	err = s.ApplyTransition(ctx, "run-1", durable.Transition{
		Cursor: durable.Cursor{Phase: durable.PhaseForward},
		Steps: []durable.StepWrite{{StepID: "a", Record: durable.StepRecord{
			ForwardStatus: durable.OpSucceeded, ForwardAttempts: 1, State: []byte{1, 2, 3},
		}}},
	})
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	got, err := s.GetRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if sr := got.Steps["a"]; sr == nil || sr.ForwardStatus != durable.OpSucceeded || len(sr.State) != 3 {
		t.Fatalf("round-tripped step record = %+v", got.Steps["a"])
	}

	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != 1 {
		t.Fatalf("ListNonterminal = %v, %v", runs, err)
	}

	// Terminal completion frees the slot.
	oc := durable.OutcomeSuccess
	err = s.ApplyTransition(ctx, "run-1", durable.Transition{
		Cursor:  durable.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	})
	if err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != 0 {
		t.Fatalf("ListNonterminal after terminal = %v, %v", runs, err)
	}
	if _, created, err := s.CreateRun(ctx, dup); err != nil || !created {
		t.Fatalf("CreateRun after slot freed = created=%v err=%v", created, err)
	}

	if runs, err := s.ListRuns(ctx, "p", "r"); err != nil || len(runs) != 2 {
		t.Fatalf("ListRuns = %d, %v; want 2", len(runs), err)
	}

	if _, err := s.GetRun(ctx, "missing"); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("GetRun(missing) = %v, want ErrRunNotFound", err)
	}
}

func TestEngineSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.db")
	fastRetry := durable.WithRetryPolicy(durable.RetryPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2})

	makeDef := func(succeed *bool) *durable.Definition {
		return durable.NewDefinition(durable.DefinitionConfig{
			ID: "restartable",
			Steps: []durable.StepConfig{{
				ID: "only/v1",
				Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
					if !*succeed {
						return nil, errors.New("not yet")
					}
					return nil, nil
				},
			}},
		})
	}

	// First process: the step keeps failing; stop with the run unresolved.
	s1 := open(t, path)
	e1 := durable.NewEngine(s1, fastRetry)
	no := false
	p1, err := makeDef(&no).Bind(e1)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := e1.Start(context.Background()); err != nil {
		t.Fatalf("Start1: %v", err)
	}
	run, _, err := p1.Schedule(context.Background(), "r", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := run.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.Attempt >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("step never attempted")
		}
		time.Sleep(time.Millisecond)
	}
	if err := e1.Stop(context.Background()); err != nil {
		t.Fatalf("Stop1: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close1: %v", err)
	}

	// Second process: reopen the database, recover, and finish the run.
	s2 := open(t, path)
	e2 := durable.NewEngine(s2, fastRetry, durable.WithRecoveryBackoff(0))
	yes := true
	p2, err := makeDef(&yes).Bind(e2)
	if err != nil {
		t.Fatalf("Bind2: %v", err)
	}
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
		t.Fatalf("Wait after restart = %+v, %v; want success", res, err)
	}
}

func TestRequestCancel(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()

	rec := &durable.RunRecord{RunID: "run-c", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, rec); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}

	if _, err := s.RequestCancel(ctx, "missing", durable.CancelRequest{}); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("RequestCancel(missing) = %v, want ErrRunNotFound", err)
	}

	accepted, err := s.RequestCancel(ctx, "run-c", durable.CancelRequest{Cause: "first", At: time.Now()})
	if err != nil || !accepted {
		t.Fatalf("RequestCancel = accepted=%v err=%v", accepted, err)
	}
	// First cancel wins.
	accepted, err = s.RequestCancel(ctx, "run-c", durable.CancelRequest{Cause: "second"})
	if err != nil || accepted {
		t.Fatalf("second RequestCancel = accepted=%v err=%v", accepted, err)
	}

	// The request survives later transitions untouched.
	err = s.ApplyTransition(ctx, "run-c", durable.Transition{
		Cursor: durable.Cursor{Phase: durable.PhaseForward, StepID: "s/v1", Attempts: 1},
	})
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	got, err := s.GetRun(ctx, "run-c")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.Cancel == nil || got.Cancel.Cause != "first" {
		t.Fatalf("Cancel = %+v, want preserved first request", got.Cancel)
	}

	// Terminal runs reject cancellation.
	oc := durable.OutcomeFailure
	err = s.ApplyTransition(ctx, "run-c", durable.Transition{
		Cursor:  durable.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	})
	if err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if _, err := s.RequestCancel(ctx, "run-c", durable.CancelRequest{}); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("RequestCancel(terminal) = %v, want ErrRunTerminal", err)
	}
}

func TestExclusionGroupSlot(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()

	recA := &durable.RunRecord{RunID: "ga-1", PipelineID: "pa", ResourceID: "r", Group: "group/g", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, recA); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}
	// A different pipeline in the same group hits the occupied slot.
	recB := &durable.RunRecord{RunID: "gb-1", PipelineID: "pb", ResourceID: "r", Group: "group/g", Phase: durable.PhaseForward}
	existing, created, err := s.CreateRun(ctx, recB)
	if err != nil || created || existing.RunID != "ga-1" {
		t.Fatalf("group CreateRun = %+v created=%v err=%v", existing, created, err)
	}
	// A pipeline outside the group is unaffected.
	recC := &durable.RunRecord{RunID: "gc-1", PipelineID: "pc", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, recC); err != nil || !created {
		t.Fatalf("non-group CreateRun = created=%v err=%v", created, err)
	}
	// Terminal completion frees the group slot.
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "ga-1", durable.Transition{
		Cursor:  durable.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	}); err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if _, created, err := s.CreateRun(ctx, recB); err != nil || !created {
		t.Fatalf("post-terminal group CreateRun = created=%v err=%v", created, err)
	}
}

func TestReapTerminal(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()
	base := time.Now().UTC()
	oc := durable.OutcomeSuccess

	mkRun := func(id durable.RunID, resource durable.ResourceID, terminalAt time.Time, terminal bool) {
		rec := &durable.RunRecord{
			RunID: id, PipelineID: "p", ResourceID: resource,
			Phase: durable.PhaseForward, CreatedAt: base,
		}
		if _, created, err := s.CreateRun(ctx, rec); err != nil || !created {
			t.Fatalf("CreateRun %s: created=%v err=%v", id, created, err)
		}
		tr := durable.Transition{
			Cursor: durable.Cursor{Phase: durable.PhaseForward, UpdatedAt: terminalAt},
			Steps: []durable.StepWrite{{StepID: "a", Record: durable.StepRecord{
				ForwardStatus: durable.OpSucceeded, ForwardAttempts: 1, State: []byte{1},
			}}},
		}
		if terminal {
			tr.Cursor.Phase = durable.PhaseDone
			tr.Outcome = &oc
		}
		if err := s.ApplyTransition(ctx, id, tr); err != nil {
			t.Fatalf("ApplyTransition %s: %v", id, err)
		}
	}

	mkRun("old-1", "r1", base.Add(-48*time.Hour), true)
	mkRun("old-2", "r2", base.Add(-48*time.Hour), true)
	mkRun("recent", "r3", base.Add(-time.Minute), true)
	mkRun("alive", "r4", base.Add(-48*time.Hour), false)
	if _, err := s.RequestCancel(ctx, "alive", durable.CancelRequest{Cause: "x", At: base}); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	cutoff := base.Add(-time.Hour)

	// The limit bounds one pass.
	n, err := s.ReapTerminal(ctx, cutoff, 1)
	if err != nil || n != 1 {
		t.Fatalf("ReapTerminal(limit 1) = %d, %v", n, err)
	}
	n, err = s.ReapTerminal(ctx, cutoff, 10)
	if err != nil || n != 1 {
		t.Fatalf("second ReapTerminal = %d, %v; want the remaining old run", n, err)
	}

	// Old terminal runs are fully gone; every component is deleted.
	for _, id := range []durable.RunID{"old-1", "old-2"} {
		if _, err := s.GetRun(ctx, id); !errors.Is(err, durable.ErrRunNotFound) {
			t.Fatalf("GetRun(%s) = %v, want ErrRunNotFound", id, err)
		}
	}
	s.db.View(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{metaBucket, cursorBucket, failuresBucket, terminalBucket, cancelBucket} {
			for _, id := range []string{"old-1", "old-2"} {
				if tx.Bucket(bucket).Get([]byte(id)) != nil {
					t.Errorf("bucket %s still holds %s", bucket, id)
				}
			}
		}
		c := tx.Bucket(stepsBucket).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if bytes.HasPrefix(k, []byte("old-")) {
				t.Errorf("steps bucket still holds %s", k)
			}
		}
		return nil
	})

	// Recent terminal and old nonterminal (with its cancel record) survive.
	if _, err := s.GetRun(ctx, "recent"); err != nil {
		t.Fatalf("recent run reaped: %v", err)
	}
	alive, err := s.GetRun(ctx, "alive")
	if err != nil || alive.Cancel == nil {
		t.Fatalf("alive run = %+v, %v; want intact with cancel", alive, err)
	}
}
