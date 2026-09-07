package bbolt

import (
	"bytes"
	"context"
	"errors"
	"github.com/dangra/durable/store/driver"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
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

	rec := &driver.RunRecord{
		RunID:      "run-1",
		PipelineID: "p",
		ResourceID: "r",
		Phase:      durable.PhaseForward,
		CreatedAt:  time.Now(),
	}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}

	// The slot is occupied.
	dup := &driver.RunRecord{RunID: "run-2", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	existing, created, err := s.CreateRun(ctx, dup, nil)
	if err != nil || created {
		t.Fatalf("second CreateRun = created=%v err=%v", created, err)
	}
	if existing.RunID != "run-1" {
		t.Fatalf("existing.RunID = %s, want run-1", existing.RunID)
	}

	// Facts round-trip.
	err = s.ApplyTransition(ctx, "run-1", driver.Transition{
		Cursor: driver.Cursor{Phase: durable.PhaseForward},
		Ops: []driver.OpWrite{{StepID: "a", Phase: durable.PhaseForward, Record: driver.OperationRecord{
			Status: driver.OpSucceeded, Attempts: 1, State: []byte{1, 2, 3}, Order: 1,
		}}},
	})
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	got, err := s.GetRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if sr := got.Steps["a"]; sr == nil || sr.Forward.Status != driver.OpSucceeded || len(sr.Forward.State) != 3 {
		t.Fatalf("round-tripped step record = %+v", got.Steps["a"])
	}

	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != 1 {
		t.Fatalf("ListNonterminal = %v, %v", runs, err)
	}

	// Terminal completion frees the slot.
	oc := durable.OutcomeSuccess
	err = s.ApplyTransition(ctx, "run-1", driver.Transition{
		Cursor:  driver.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	})
	if err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != 0 {
		t.Fatalf("ListNonterminal after terminal = %v, %v", runs, err)
	}
	if _, created, err := s.CreateRun(ctx, dup, nil); err != nil || !created {
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
	fastRetry := engine.WithRetryPolicy(engine.RetryPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2})

	makeDef := func(succeed *bool) *pipelinedef.Definition {
		return pipelinedef.New(pipelinedef.Config{
			ID: "restartable",
			Steps: []pipelinedef.Step{{
				ID: "only/v1",
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
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
	e1 := engine.New(s1, fastRetry)
	no := false
	p1, err := e1.Bind(makeDef(&no))
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
	e2 := engine.New(s2, fastRetry, engine.WithRecoveryBackoff(0))
	yes := true
	p2, err := e2.Bind(makeDef(&yes))
	if err != nil {
		t.Fatalf("Bind2: %v", err)
	}
	if err := e2.Start(context.Background()); err != nil {
		t.Fatalf("Start2: %v", err)
	}
	defer e2.Stop(context.Background())

	run2, err := p2.GetRun(context.Background(), run.ID())
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

	rec := &driver.RunRecord{RunID: "run-c", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}

	if _, err := s.RequestCancel(ctx, "missing", driver.CancelRequest{}); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("RequestCancel(missing) = %v, want ErrRunNotFound", err)
	}

	accepted, err := s.RequestCancel(ctx, "run-c", driver.CancelRequest{Cause: "first", At: time.Now()})
	if err != nil || !accepted {
		t.Fatalf("RequestCancel = accepted=%v err=%v", accepted, err)
	}
	// First cancel wins.
	accepted, err = s.RequestCancel(ctx, "run-c", driver.CancelRequest{Cause: "second"})
	if err != nil || accepted {
		t.Fatalf("second RequestCancel = accepted=%v err=%v", accepted, err)
	}

	// The request survives later transitions untouched.
	err = s.ApplyTransition(ctx, "run-c", driver.Transition{
		Cursor: driver.Cursor{Phase: durable.PhaseForward, StepID: "s/v1", Attempts: 1},
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
	err = s.ApplyTransition(ctx, "run-c", driver.Transition{
		Cursor:  driver.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	})
	if err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if _, err := s.RequestCancel(ctx, "run-c", driver.CancelRequest{}); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("RequestCancel(terminal) = %v, want ErrRunTerminal", err)
	}
}

func TestMutexSlot(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()

	recA := &driver.RunRecord{RunID: "ga-1", PipelineID: "pa", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, recA, nil); err != nil || !created {
		t.Fatalf("CreateRun = created=%v err=%v", created, err)
	}
	// A different pipeline in the same group hits the occupied slot.
	recB := &driver.RunRecord{RunID: "gb-1", PipelineID: "pb", ResourceID: "r", Phase: durable.PhaseForward}
	group := []durable.PipelineID{"pa", "pb"}
	existing, created, err := s.CreateRun(ctx, recB, group)
	if err != nil || created || existing.RunID != "ga-1" {
		t.Fatalf("group CreateRun = %+v created=%v err=%v", existing, created, err)
	}
	// A pipeline outside the group is unaffected.
	recC := &driver.RunRecord{RunID: "gc-1", PipelineID: "pc", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, recC, nil); err != nil || !created {
		t.Fatalf("non-group CreateRun = created=%v err=%v", created, err)
	}
	// Terminal completion frees the group slot.
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "ga-1", driver.Transition{
		Cursor:  driver.Cursor{Phase: durable.PhaseDone},
		Outcome: &oc,
	}); err != nil {
		t.Fatalf("terminal ApplyTransition: %v", err)
	}
	if _, created, err := s.CreateRun(ctx, recB, group); err != nil || !created {
		t.Fatalf("post-terminal group CreateRun = created=%v err=%v", created, err)
	}
}

func TestReapTerminal(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable.db"))
	ctx := context.Background()
	base := time.Now().UTC()
	oc := durable.OutcomeSuccess

	mkRun := func(id durable.RunID, resource durable.ResourceID, terminalAt time.Time, terminal bool) {
		rec := &driver.RunRecord{
			RunID: id, PipelineID: "p", ResourceID: resource,
			Phase: durable.PhaseForward, CreatedAt: base,
		}
		if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
			t.Fatalf("CreateRun %s: created=%v err=%v", id, created, err)
		}
		tr := driver.Transition{
			Cursor: driver.Cursor{Phase: durable.PhaseForward, UpdatedAt: terminalAt},
			Ops: []driver.OpWrite{{StepID: "a", Phase: durable.PhaseForward, Record: driver.OperationRecord{
				Status: driver.OpSucceeded, Attempts: 1, State: []byte{1}, Order: 1,
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
	if _, err := s.RequestCancel(ctx, "alive", driver.CancelRequest{Cause: "x", At: base}); err != nil {
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

// GetActiveRunID reads the slots bucket: set at CreateRun, released at
// the terminal outcome, reusable afterwards.
func TestGetActiveRunIDFollowsTheSlot(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "slots.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec := func(id durable.RunID) *driver.RunRecord {
		return &driver.RunRecord{RunID: id, PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(1, 0)}
	}
	if _, ok, err := s.GetActiveRunID(ctx, "p", "r"); err != nil || ok {
		t.Fatalf("empty store: ok=%v err=%v", ok, err)
	}
	if _, created, err := s.CreateRun(ctx, rec("a"), nil); err != nil || !created {
		t.Fatal(err)
	}
	if id, ok, _ := s.GetActiveRunID(ctx, "p", "r"); !ok || id != "a" {
		t.Fatalf("after create: %q %v", id, ok)
	}
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "a", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}, Outcome: &oc}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetActiveRunID(ctx, "p", "r"); ok {
		t.Fatal("terminal outcome must release the slot")
	}
	if _, created, err := s.CreateRun(ctx, rec("c"), nil); err != nil || !created {
		t.Fatalf("slot must be reusable: created=%v err=%v", created, err)
	}
}

// TestOperationRowsAreWrittenOnce pins the row layout: one row per
// operation under run id, step id, and phase; an unwind resolution leaves
// the forward row's bytes untouched; a failed operation carries its own
// failure; the failures bucket holds the root only; and reap sweeps all
// of it.
func TestOperationRowsAreWrittenOnce(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "ops.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Unix(1_700_000_000, 0).UTC()
	rec := &driver.RunRecord{
		RunID: "run-o", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward,
		Steps: map[durable.StepID]*driver.StepRecord{}, CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	apply := func(tr driver.Transition) {
		t.Helper()
		tr.Cursor.UpdatedAt = now
		if err := s.ApplyTransition(ctx, "run-o", tr); err != nil {
			t.Fatal(err)
		}
	}
	rowBytes := func(step string, phase durable.Phase) []byte {
		var b []byte
		s.db.View(func(tx *bolt.Tx) error {
			b = bytes.Clone(tx.Bucket(stepsBucket).Get(opKey("run-o", durable.StepID(step), phase)))
			return nil
		})
		return b
	}

	// Forward success with a large state.
	state := bytes.Repeat([]byte{7}, 4096)
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward}, Ops: []driver.OpWrite{{
		StepID: "a/v1", Phase: durable.PhaseForward,
		Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: state, Order: 1},
	}}})
	forwardRow := rowBytes("a/v1", durable.PhaseForward)
	if len(forwardRow) < len(state) {
		t.Fatalf("forward row = %d bytes; want the state in it", len(forwardRow))
	}

	// The root failure ends the forward phase: a failed forward row for
	// b/v1 and one root row.
	root := &durable.RootFailure{FailureRecord: durable.FailureRecord{StepID: "b/v1", Phase: durable.PhaseForward, Attempt: 1, Message: "boom", At: now}}
	failed := root.FailureRecord
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind}, RootFailure: root, Ops: []driver.OpWrite{{
		StepID: "b/v1", Phase: durable.PhaseForward,
		Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 1, Failure: &failed, Order: 2},
	}}})

	// a/v1 unwinds and fails permanently: its own small row, and the
	// forward row is byte-for-byte what it was.
	uf := durable.FailureRecord{StepID: "a/v1", Phase: durable.PhaseUnwind, Attempt: 2, Message: "stuck", At: now}
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind}, Ops: []driver.OpWrite{{
		StepID: "a/v1", Phase: durable.PhaseUnwind,
		Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 2, Failure: &uf, Order: 3},
	}}})
	if !bytes.Equal(rowBytes("a/v1", durable.PhaseForward), forwardRow) {
		t.Fatal("unwind resolution rewrote the forward row")
	}
	if unwindRow := rowBytes("a/v1", durable.PhaseUnwind); len(unwindRow) == 0 || len(unwindRow) > 256 {
		t.Fatalf("unwind row = %d bytes; want a small row of its own", len(unwindRow))
	}

	got, err := s.GetRun(ctx, "run-o")
	if err != nil {
		t.Fatal(err)
	}
	if got.RootFailure == nil || got.RootFailure.StepID != "b/v1" {
		t.Fatalf("RootFailure = %+v", got.RootFailure)
	}
	if op := got.Step("b/v1").Forward; op.Status != driver.OpFailed || op.Failure == nil || op.Failure.Message != "boom" || op.Order != 2 {
		t.Fatalf("b/v1 forward = %+v", op)
	}
	if ufs := got.UnwindFailures(); len(ufs) != 1 || ufs[0].StepID != "a/v1" || ufs[0].Message != "stuck" {
		t.Fatalf("UnwindFailures = %+v", ufs)
	}
	if a := got.Step("a/v1"); a.Forward.Status != driver.OpSucceeded || len(a.Forward.State) != len(state) || a.Unwind.Order != 3 {
		t.Fatalf("a/v1 = %+v", a)
	}
	s.db.View(func(tx *bolt.Tx) error {
		n := 0
		tx.Bucket(failuresBucket).ForEach(func(k, _ []byte) error { n++; return nil })
		if n != 1 {
			t.Fatalf("failures bucket rows = %d; want the root only", n)
		}
		n = 0
		tx.Bucket(stepsBucket).ForEach(func(k, _ []byte) error { n++; return nil })
		if n != 3 {
			t.Fatalf("operation rows = %d; want a forward, a unwind, b forward", n)
		}
		return nil
	})

	oc := durable.OutcomeFailure
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}, Outcome: &oc})
	if n, err := s.ReapTerminal(ctx, now.Add(time.Second), 10); err != nil || n != 1 {
		t.Fatalf("ReapTerminal = %d, %v", n, err)
	}
	s.db.View(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{stepsBucket, failuresBucket} {
			if k, _ := tx.Bucket(b).Cursor().First(); k != nil {
				t.Fatalf("bucket %s still holds %q after reap", b, k)
			}
		}
		return nil
	})
}
