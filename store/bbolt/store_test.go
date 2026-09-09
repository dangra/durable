package bbolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/dangra/durable/store/driver"
	"path/filepath"
	"reflect"
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

	for _, id := range []durable.RunID{"run-1", "run-2"} {
		if _, err := s.GetRun(ctx, id); err != nil {
			t.Fatalf("GetRun(%s) = %v; both runs must be readable", id, err)
		}
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
		for _, id := range []string{"old-1", "old-2"} {
			for _, bucket := range [][]byte{cursorBucket, terminalBucket} {
				if tx.Bucket(bucket).Get([]byte(id)) != nil {
					t.Errorf("bucket %s still holds %s", bucket, id)
				}
			}
			if tx.Bucket(activeBucket).Bucket(activeKey(durable.RunID(id), tagInput)) != nil {
				t.Errorf("input bucket still holds %s", id)
			}
			if n := len(activeRows(tx, durable.RunID(id))); n != 0 {
				t.Errorf("active bucket still holds %d rows of %s", n, id)
			}
		}
		return nil
	})

	// The expiry index is exactly the terminal runs left, oldest first.
	s.db.View(func(tx *bolt.Tx) error {
		var left []string
		tx.Bucket(expiryBucket).ForEach(func(k, _ []byte) error { left = append(left, string(k[8:])); return nil })
		if !reflect.DeepEqual(left, []string{"recent"}) {
			t.Errorf("expiry index = %v; want [recent]", left)
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

// activeRows returns a run's rows in the active bucket in key order,
// values keyed by the bytes after the run prefix.
func activeRows(tx *bolt.Tx, id durable.RunID) []struct{ key, value []byte } {
	var out []struct{ key, value []byte }
	prefix := runPrefix(id)
	c := tx.Bucket(activeBucket).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		out = append(out, struct{ key, value []byte }{bytes.Clone(k[len(prefix):]), bytes.Clone(v)})
	}
	return out
}

// TestOperationRowsAreWrittenOnce pins the row layout: one row per
// operation under run id, resolution order, step id, and phase, so a
// prefix walk returns them in execution order; an unwind resolution
// leaves the forward row's bytes untouched; a failed operation carries
// its own failure; the run's failure is one row; and terminality sweeps
// all of it.
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
			_, v := findOp(tx.Bucket(activeBucket), "run-o", durable.StepID(step), phase)
			b = bytes.Clone(v)
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
	if len(forwardRow) == 0 || len(forwardRow) > 64 {
		t.Fatalf("forward row = %d bytes; want a small row, the state beside it", len(forwardRow))
	}
	s.db.View(func(tx *bolt.Tx) error {
		if got := getBlob(tx.Bucket(activeBucket), stateKey(opKey("run-o", 1, "a/v1", durable.PhaseForward))); !bytes.Equal(got, state) {
			t.Fatalf("state blob = %d bytes, want %d", len(got), len(state))
		}
		return nil
	})

	// The run failure ends the forward phase: a failed forward row for
	// b/v1 and one root row.
	root := &durable.Failure{StepID: "b/v1", Phase: durable.PhaseForward, Attempt: 1, Message: "boom", At: now}
	failed := *root
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind}, Failure: root, Ops: []driver.OpWrite{{
		StepID: "b/v1", Phase: durable.PhaseForward,
		Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 1, Failure: &failed, Order: 2},
	}}})

	// a/v1 unwinds and fails permanently: its own small row, and the
	// forward row is byte-for-byte what it was.
	uf := durable.Failure{StepID: "a/v1", Phase: durable.PhaseUnwind, Attempt: 2, Message: "stuck", At: now}
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
	if got.Failure == nil || got.Failure.StepID != "b/v1" {
		t.Fatalf("Failure = %+v", got.Failure)
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
		// The walk reads meta, failure, then operations in resolution
		// order — a forward (1) with its state beside it, b forward (2),
		// a unwind (3).
		var tags []byte
		var ops []string
		for _, row := range activeRows(tx, "run-o") {
			tags = append(tags, row.key[0])
			if row.key[0] == tagOp && !bytes.HasSuffix(row.key, stateSuffix) {
				order, step, phase, ok := splitOpRest(row.key[1:])
				if !ok {
					t.Fatalf("malformed operation key %q", row.key)
				}
				ops = append(ops, fmt.Sprintf("%d:%s/%s", order, step, phase))
			}
		}
		if string(tags) != "Mfoooo" {
			t.Fatalf("active row tags = %q; want meta, failure, three operations and a's state beside its row", tags)
		}
		if want := []string{"1:a/v1/forward", "2:b/v1/forward", "3:a/v1/unwind"}; !reflect.DeepEqual(ops, want) {
			t.Fatalf("operation rows = %v; want %v", ops, want)
		}
		return nil
	})

	oc := durable.OutcomeFailure
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}, Outcome: &oc})
	if err := s.Drain(); err != nil {
		t.Fatal(err)
	}
	s.db.View(func(tx *bolt.Tx) error {
		if k, _ := tx.Bucket(activeBucket).Cursor().First(); k != nil {
			t.Fatalf("active bucket still holds %q after terminality", k)
		}
		return nil
	})
	if n, err := s.ReapTerminal(ctx, now.Add(time.Second), 10); err != nil || n != 1 {
		t.Fatalf("ReapTerminal = %d, %v", n, err)
	}
}

// TestPendingRowMovesToItsOrder pins the one non-append write of the
// active bucket: an unresolved operation flushed at order zero sorts
// ahead of the resolved history and, when it resolves, its row moves to
// its real order; and the write-once rows refuse a second write.
func TestPendingRowMovesToItsOrder(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "pending.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Unix(1_700_000_000, 0).UTC()
	rec := &driver.RunRecord{RunID: "run-p", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	apply := func(tr driver.Transition) error {
		tr.Cursor.UpdatedAt = now
		return s.ApplyTransition(ctx, "run-p", tr)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward}, Ops: []driver.OpWrite{{
		StepID: "a/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, Order: 1},
	}}}))
	// b/v1 displaced while unresolved: flushed at order zero.
	must(apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward}, Ops: []driver.OpWrite{{
		StepID: "b/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpUnresolved, Attempts: 2},
	}}}))
	orders := func() []uint32 {
		var out []uint32
		s.db.View(func(tx *bolt.Tx) error {
			for _, row := range activeRows(tx, "run-p") {
				if row.key[0] == tagOp {
					order, _, _, _ := splitOpRest(row.key[1:])
					out = append(out, order)
				}
			}
			return nil
		})
		return out
	}
	if got := orders(); !reflect.DeepEqual(got, []uint32{0, 1}) {
		t.Fatalf("orders with a pending row = %v; want [0 1]", got)
	}
	// It resolves: one row, at its order, attempts carried.
	must(apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward}, Ops: []driver.OpWrite{{
		StepID: "b/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 3, Order: 2},
	}}}))
	if got := orders(); !reflect.DeepEqual(got, []uint32{1, 2}) {
		t.Fatalf("orders after resolution = %v; want [1 2]", got)
	}
	got, err := s.GetRun(ctx, "run-p")
	if err != nil || got.Step("b/v1").Forward.Attempts != 3 || got.NextOrder() != 3 {
		t.Fatalf("GetRun = %+v, %v", got, err)
	}

	// Write-once rows refuse a second write.
	root := &durable.Failure{StepID: "c/v1", Phase: durable.PhaseForward, Attempt: 1, Message: "boom", At: now}
	must(apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind}, Failure: root}))
	if err := apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind}, Failure: root}); err == nil {
		t.Fatal("a second run failure must be refused")
	}
	// Reusing a live run id under another slot is outside the contract;
	// the meta row's write-once guard is what catches it.
	dup := *rec
	dup.ResourceID = "other"
	if _, _, err := s.CreateRun(ctx, &dup, nil); err == nil {
		t.Fatal("re-creating a live run id must be refused")
	}
}

// TestTerminalityCompactsRun pins the stage split: the terminality commit
// deletes meta, cursor, and every operation row but keeps the run
// readable — identity, outcome, output, failure, cancel, and the failed
// unwinds — from the terminal record alone, and reap sweeps that.
func TestTerminalityCompactsRun(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "stages.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Unix(1_700_000_000, 0).UTC()
	input := bytes.Repeat([]byte{1}, 8192)
	rec := &driver.RunRecord{
		RunID: "run-t", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward,
		Input: input, Annotations: map[string]string{"tenant": "t1"},
		Steps: map[durable.StepID]*driver.StepRecord{}, CreatedAt: now, UpdatedAt: now,
	}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	apply := func(tr driver.Transition) {
		t.Helper()
		if err := s.ApplyTransition(ctx, "run-t", tr); err != nil {
			t.Fatal(err)
		}
	}
	state := bytes.Repeat([]byte{7}, 4096)
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward, UpdatedAt: now}, Ops: []driver.OpWrite{{
		StepID: "a/v1", Phase: durable.PhaseForward,
		Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: state, Order: 1},
	}}})
	root := &durable.Failure{StepID: "b/v1", Phase: durable.PhaseForward, Attempt: 1, Message: "boom", At: now}
	apply(driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseUnwind, UpdatedAt: now}, Failure: root, Ops: []driver.OpWrite{{
		StepID: "b/v1", Phase: durable.PhaseForward,
		Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 1, Failure: root, Order: 2},
	}}})
	if _, err := s.RequestCancel(ctx, "run-t", driver.CancelRequest{Cause: "late", At: now}); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	uf := durable.Failure{StepID: "a/v1", Phase: durable.PhaseUnwind, Attempt: 2, Message: "stuck", At: now}
	oc := durable.OutcomeFailure
	// While nonterminal, the input sits in its own nested bucket and
	// reads back whole.
	s.db.View(func(tx *bolt.Tx) error {
		ib := tx.Bucket(activeBucket).Bucket(activeKey("run-t", tagInput))
		if ib == nil || !bytes.Equal(ib.Get(blobKey), input) {
			t.Fatal("an input this large must sit in its own nested bucket")
		}
		return nil
	})
	if got, err := s.GetRun(ctx, "run-t"); err != nil || !bytes.Equal(got.Input, input) {
		t.Fatalf("GetRun input = %d bytes, %v", len(got.Input), err)
	}

	committed := now.Add(time.Minute)
	// The last unwind resolution rides the terminality commit.
	apply(driver.Transition{
		Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: committed, LastError: "stale", NextAttemptAt: now},
		Ops: []driver.OpWrite{{StepID: "a/v1", Phase: durable.PhaseUnwind,
			Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 2, Failure: &uf, Order: 3}}},
		Outcome: &oc, Output: []byte{9},
	})

	// The stage lingers, staged, until the drain — and reads already
	// come from the terminal row.
	s.db.View(func(tx *bolt.Tx) error {
		if len(activeRows(tx, "run-t")) == 0 {
			t.Fatal("the stage must be staged, not deleted, by the terminality commit")
		}
		return nil
	})
	if got, err := s.GetRun(ctx, "run-t"); err != nil || got.Input != nil || len(got.Steps) != 1 {
		t.Fatalf("GetRun before drain = %+v, %v; want the terminal record", got, err)
	}
	if err := s.Drain(); err != nil {
		t.Fatal(err)
	}

	// After the drain a terminal run is one row.
	s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(cursorBucket).Get([]byte("run-t")) != nil {
			t.Error("cursor survived terminality")
		}
		if rows := activeRows(tx, "run-t"); len(rows) != 0 {
			t.Errorf("%d active rows survived terminality", len(rows))
		}
		if tb := tx.Bucket(terminalBucket).Get([]byte("run-t")); len(tb) > 1024 {
			t.Errorf("terminal record = %d bytes; the input and state must not be in it", len(tb))
		}
		return nil
	})

	// The run reads back from the terminal stage.
	got, err := s.GetRun(ctx, "run-t")
	if err != nil {
		t.Fatal(err)
	}
	if got.PipelineID != "p" || got.ResourceID != "r" || got.Annotations["tenant"] != "t1" || !got.CreatedAt.Equal(now) {
		t.Fatalf("identity = %+v", got)
	}
	if got.Outcome == nil || *got.Outcome != oc || string(got.Output) != "\x09" || got.Phase != durable.PhaseDone || !got.UpdatedAt.Equal(committed) {
		t.Fatalf("terminal facts = %+v", got)
	}
	if got.Input != nil || got.LastError != "" || !got.NextAttemptAt.IsZero() {
		t.Fatalf("nonterminal facts leaked: input=%d lastError=%q next=%v", len(got.Input), got.LastError, got.NextAttemptAt)
	}
	if got.Failure == nil || got.Failure.Message != "boom" || got.Cancel == nil || got.Cancel.Cause != "late" {
		t.Fatalf("failure/cancel = %+v / %+v", got.Failure, got.Cancel)
	}
	if ufs := got.UnwindFailures(); len(ufs) != 1 || ufs[0].Message != "stuck" {
		t.Fatalf("UnwindFailures = %+v", ufs)
	}
	if len(got.Steps) != 1 || got.Steps["a/v1"].Forward.Status != driver.OpNone {
		t.Fatalf("Steps = %+v; want the failed unwind only", got.Steps)
	}
	if err := s.ApplyTransition(ctx, "run-t", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}}); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("ApplyTransition(terminal) = %v, want ErrRunTerminal", err)
	}
	if _, err := s.RequestCancel(ctx, "run-t", driver.CancelRequest{}); !errors.Is(err, durable.ErrRunTerminal) {
		t.Fatalf("RequestCancel(terminal) = %v, want ErrRunTerminal", err)
	}
	if id, ok, _ := s.GetActiveRunID(ctx, "p", "r"); ok {
		t.Fatalf("slot still held by %s", id)
	}

	if n, err := s.ReapTerminal(ctx, committed.Add(time.Second), 10); err != nil || n != 1 {
		t.Fatalf("ReapTerminal = %d, %v", n, err)
	}
	if _, err := s.GetRun(ctx, "run-t"); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("GetRun after reap = %v", err)
	}
	s.db.View(func(tx *bolt.Tx) error {
		if k, _ := tx.Bucket(terminalBucket).Cursor().First(); k != nil {
			t.Fatalf("terminal bucket still holds %q after reap", k)
		}
		return nil
	})
}

// TestStageDrainSurvivesCrash pins the recovery of the deferred stage
// deletion: a terminal run left staged by a crashed process is drained
// at the next Open, from the staged bucket.
func TestStageDrainSurvivesCrash(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "crash.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	rec := &driver.RunRecord{RunID: "run-x", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward,
		Input: bytes.Repeat([]byte{1}, 8192), CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "run-x", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: now}, Outcome: &oc, Output: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	// Crash: stop the sweep and close the file without draining.
	close(s.stop)
	<-s.done
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := open(t, path)
	s2.db.View(func(tx *bolt.Tx) error {
		if rows := activeRows(tx, "run-x"); len(rows) != 0 {
			t.Fatalf("%d stage rows survived reopen", len(rows))
		}
		if tx.Bucket(cursorBucket).Get([]byte("run-x")) != nil {
			t.Fatal("cursor survived reopen")
		}
		return nil
	})
	got, err := s2.GetRun(ctx, "run-x")
	if err != nil || got.Outcome == nil || got.Input != nil {
		t.Fatalf("GetRun after reopen = %+v, %v", got, err)
	}
}

// TestBlobRule pins the two forms of a variable-length value: at or
// below the store's row limit the input, a state, and the output are the
// row's value; above it each is a nested bucket holding one value. Both
// read back the same, and terminality and reap delete both forms.
func TestBlobRule(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "blobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.blobRowMax <= 0 || s.blobRowMax >= 4096 {
		t.Fatalf("blobRowMax = %d; want bbolt's inline limit for a one-key bucket", s.blobRowMax)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	oc := durable.OutcomeSuccess
	for _, tc := range []struct {
		name   string
		size   int
		bucket bool
	}{
		{"row", s.blobRowMax, false},
		{"bucket", s.blobRowMax + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := durable.RunID("run-" + tc.name)
			val := bytes.Repeat([]byte{9}, tc.size)
			rec := &driver.RunRecord{RunID: id, PipelineID: "p", ResourceID: durable.ResourceID(tc.name), Phase: durable.PhaseForward, Input: val, CreatedAt: now, UpdatedAt: now}
			if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
				t.Fatalf("CreateRun = %v, %v", created, err)
			}
			if err := s.ApplyTransition(ctx, id, driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward, UpdatedAt: now}, Ops: []driver.OpWrite{{
				StepID: "a/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: val, Order: 1},
			}}}); err != nil {
				t.Fatal(err)
			}
			s.db.View(func(tx *bolt.Tx) error {
				active := tx.Bucket(activeBucket)
				if isBucket := active.Bucket(activeKey(id, tagInput)) != nil; isBucket != tc.bucket {
					t.Errorf("input stored as bucket=%v, want %v", isBucket, tc.bucket)
				}
				// A small state is inside its record; a large one is a
				// bucket beside the row.
				sk := stateKey(opKey(id, 1, "a/v1", durable.PhaseForward))
				if isBucket := active.Bucket(sk) != nil; isBucket != tc.bucket {
					t.Errorf("state beside the row as bucket=%v, want %v", isBucket, tc.bucket)
				}
				if _, v := findOp(active, id, "a/v1", durable.PhaseForward); (len(v) > tc.size) != !tc.bucket {
					t.Errorf("operation row = %d bytes for a %d-byte state, bucket=%v", len(v), tc.size, tc.bucket)
				}
				return nil
			})
			got, err := s.GetRun(ctx, id)
			if err != nil || !bytes.Equal(got.Input, val) || !bytes.Equal(got.Step("a/v1").Forward.State, val) {
				t.Fatalf("GetRun = input %d, state %d, %v; want %d each", len(got.Input), len(got.Step("a/v1").Forward.State), err, tc.size)
			}
			if err := s.ApplyTransition(ctx, id, driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: now}, Outcome: &oc, Output: val}); err != nil {
				t.Fatal(err)
			}
			s.db.View(func(tx *bolt.Tx) error {
				terminal := tx.Bucket(terminalBucket)
				if isBucket := terminal.Bucket(outputKey(id)) != nil; isBucket != tc.bucket {
					t.Errorf("output beside the row as bucket=%v, want %v", isBucket, tc.bucket)
				}
				if row := terminal.Get([]byte(id)); (len(row) > tc.size) != !tc.bucket {
					t.Errorf("terminal row = %d bytes for a %d-byte output, bucket=%v", len(row), tc.size, tc.bucket)
				}
				return nil
			})
			if got, err := s.GetRun(ctx, id); err != nil || !bytes.Equal(got.Output, val) || got.Input != nil {
				t.Fatalf("terminal GetRun = output %d, input %d, %v", len(got.Output), len(got.Input), err)
			}
			if err := s.Drain(); err != nil {
				t.Fatal(err)
			}
			if n, err := s.ReapTerminal(ctx, now.Add(time.Second), 10); err != nil || n != 1 {
				t.Fatalf("ReapTerminal = %d, %v", n, err)
			}
			s.db.View(func(tx *bolt.Tx) error {
				if rows := activeRows(tx, id); len(rows) != 0 {
					t.Errorf("%d active rows survived", len(rows))
				}
				if k, _ := tx.Bucket(terminalBucket).Cursor().First(); k != nil {
					t.Errorf("terminal bucket still holds %q", k)
				}
				return nil
			})
		})
	}
}

// TestBlobCacheServesReads pins the blob cache: once a run's input and
// large states are written, reads come from the cache — shown by
// planting other bytes in the cache and reading them back while the file
// still holds the originals — the entry goes at terminality, and a
// reopened store fills its entry from the file on the first read.
func TestBlobCacheServesReads(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.db")
	s := open(t, path)
	now := time.Unix(1_700_000_000, 0).UTC()
	input := bytes.Repeat([]byte{1}, 8192)
	state := bytes.Repeat([]byte{2}, 4*s.pageSizeOrDefault())
	rec := &driver.RunRecord{RunID: "run-c", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, Input: input, CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	opk := opKey("run-c", 1, "a/v1", durable.PhaseForward)
	if err := s.ApplyTransition(ctx, "run-c", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward, UpdatedAt: now}, Ops: []driver.OpWrite{{
		StepID: "a/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: state, Order: 1},
	}}}); err != nil {
		t.Fatal(err)
	}
	_ = opk
	// Reads take the blobs from the cache: plant different bytes there
	// and the read returns them, while the file still holds the
	// originals.
	planted, plantedState := bytes.Repeat([]byte{7}, len(input)), bytes.Repeat([]byte{8}, len(state))
	s.blobs.setInput("run-c", planted)
	s.blobs.setState("run-c", "a/v1", plantedState)
	got, err := s.GetRun(ctx, "run-c")
	if err != nil || !bytes.Equal(got.Input, planted) || !bytes.Equal(got.Step("a/v1").Forward.State, plantedState) {
		t.Fatalf("GetRun = input %x…, state %x…, %v; want the cache's bytes", got.Input[:1], got.Step("a/v1").Forward.State[:1], err)
	}
	s.db.View(func(tx *bolt.Tx) error {
		if in := getBlob(tx.Bucket(activeBucket), activeKey("run-c", tagInput)); !bytes.Equal(in, input) {
			t.Fatal("the file must still hold the original input")
		}
		return nil
	})
	// Terminality drops the entry.
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "run-c", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: now}, Outcome: &oc}); err != nil {
		t.Fatal(err)
	}
	if _, cached := s.blobs.input("run-c"); cached {
		t.Fatal("terminality must drop the run's cache entry")
	}

	// A reopened store fills from the file on the first read.
	rec2 := &driver.RunRecord{RunID: "run-d", PipelineID: "p", ResourceID: "r2", Phase: durable.PhaseForward, Input: input, CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.CreateRun(ctx, rec2, nil); err != nil || !created {
		t.Fatalf("CreateRun = %v, %v", created, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := open(t, path)
	if _, cached := s2.blobs.input("run-d"); cached {
		t.Fatal("a fresh store starts cold")
	}
	if got, err := s2.GetRun(ctx, "run-d"); err != nil || !bytes.Equal(got.Input, input) {
		t.Fatalf("GetRun after reopen = %d bytes, %v", len(got.Input), err)
	}
	if in, cached := s2.blobs.input("run-d"); !cached || !bytes.Equal(in, input) {
		t.Fatal("the first read must fill the cache")
	}
}

func (s *Store) pageSizeOrDefault() int { return s.db.Info().PageSize }
