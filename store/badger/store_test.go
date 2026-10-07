package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	bdg "github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
	"github.com/dangra/durable/store/driver"
)

// testOptions keeps the per-store memory small: tests open many.
var testOptions = []Option{WithMemTableSize(8 << 20)}

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, testOptions...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// keys returns every key under prefix, in order, for layout assertions.
func keys(t *testing.T, s *Store, prefix []byte) []string {
	t.Helper()
	var out []string
	err := s.db.View(func(txn *bdg.Txn) error {
		for _, k := range keysWithPrefix(txn, prefix, 0) {
			out = append(out, string(k))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSlotSemantics(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable"))
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

// A second Open of the same directory fails while the first holds it:
// Badger's directory lock is the exclusive ownership the v1 model
// requires.
func TestExclusiveOwnership(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "durable")
	s := open(t, dir)
	if other, err := Open(dir, testOptions...); err == nil {
		other.Close()
		t.Fatal("a second Open of a held directory must fail")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(dir, testOptions...)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	again.Close()
}

func TestEngineSurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "durable")
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
	s1 := open(t, dir)
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

	// Second process: reopen the directory, recover, and finish the run.
	s2 := open(t, dir)
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
	s := open(t, filepath.Join(t.TempDir(), "durable"))
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
	s := open(t, filepath.Join(t.TempDir(), "durable"))
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

// Two CreateRuns racing for one slot both read it free; Badger refuses
// the second commit as a conflict, and the retry finds the occupant.
func TestConcurrentCreateRunsOneWins(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable"))
	ctx := context.Background()
	const racers = 16
	type result struct {
		created bool
		err     error
	}
	results := make(chan result, racers)
	for i := 0; i < racers; i++ {
		go func(i int) {
			rec := &driver.RunRecord{RunID: durable.RunID("run-" + string(rune('a'+i))), PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
			_, created, err := s.CreateRun(ctx, rec, nil)
			results <- result{created, err}
		}(i)
	}
	winners := 0
	for i := 0; i < racers; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("CreateRun: %v", r.err)
		}
		if r.created {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d racers created a run on one slot; want exactly 1", winners)
	}
	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != 1 {
		t.Fatalf("ListNonterminal = %d runs, %v; want 1", len(runs), err)
	}
}

func TestReapTerminal(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "durable"))
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

	// Old terminal runs are fully gone: no key of theirs in any namespace.
	for _, id := range []durable.RunID{"old-1", "old-2"} {
		if _, err := s.GetRun(ctx, id); !errors.Is(err, durable.ErrRunNotFound) {
			t.Fatalf("GetRun(%s) = %v, want ErrRunNotFound", id, err)
		}
		for _, prefix := range [][]byte{runPrefix(id), cursorKey(id), terminalKey(id)} {
			if left := keys(t, s, prefix); len(left) != 0 {
				t.Errorf("%q still holds %q", prefix, left)
			}
		}
	}
	// The expiry index is exactly the terminal runs left, oldest first.
	var left []string
	for _, k := range keys(t, s, []byte{nsExpiry}) {
		left = append(left, k[9:])
	}
	if !reflect.DeepEqual(left, []string{"recent"}) {
		t.Errorf("expiry index = %v; want [recent]", left)
	}

	// Recent terminal and old nonterminal (with its cancel record) survive.
	if _, err := s.GetRun(ctx, "recent"); err != nil {
		t.Fatalf("recent run reaped: %v", err)
	}
	alive, err := s.GetRun(ctx, "alive")
	if err != nil || alive.Cancel == nil {
		t.Fatalf("alive run = %+v, %v; want intact with cancel", alive, err)
	}
}

// GetActiveRunID reads the slot: set at CreateRun, released at the
// terminal outcome, reusable afterwards.
func TestGetActiveRunIDFollowsTheSlot(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "slots"))
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

// TestOperationRowsAreUpserted pins the row layout: one row per step and
// phase, addressed without the order, so a resolution that changes an
// operation's order rewrites its row in place and leaves no other.
func TestOperationRowsAreUpserted(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "rows"))
	rec := &driver.RunRecord{RunID: "run-o", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatal(err)
	}
	apply := func(status driver.OpStatus, order uint32) {
		t.Helper()
		if err := s.ApplyTransition(ctx, "run-o", driver.Transition{
			Cursor: driver.Cursor{Phase: durable.PhaseForward},
			Ops: []driver.OpWrite{{StepID: "a/v1", Phase: durable.PhaseForward,
				Record: driver.OperationRecord{Status: status, Attempts: 1, Order: order, State: []byte{byte(order)}}}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(driver.OpUnresolved, 0)
	apply(driver.OpSucceeded, 1)
	apply(driver.OpSucceeded, 7) // the contract's upsert
	ops := keys(t, s, opPrefix("run-o"))
	if len(ops) != 1 || ops[0] != string(opKey("run-o", "a/v1", durable.PhaseForward)) {
		t.Fatalf("operation rows = %q; want the one row for a/v1 forward", ops)
	}
	got, err := s.GetRun(ctx, "run-o")
	if err != nil || got.Steps["a/v1"].Forward.Order != 7 || !bytes.Equal(got.Steps["a/v1"].Forward.State, []byte{7}) {
		t.Fatalf("row after upsert = %+v, %v", got.Steps["a/v1"], err)
	}
}

// TestTerminalityCompactsRun pins the stage split: the terminality
// commit deletes meta, input, cursor, every operation row, and the
// slot in its own transaction, and the run stays readable — identity,
// outcome, output, failure, cancel, and the failed unwinds — from the
// terminal record alone, which reap then sweeps.
func TestTerminalityCompactsRun(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "stages"))
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
	if got, err := s.GetRun(ctx, "run-t"); err != nil || !bytes.Equal(got.Input, input) || !bytes.Equal(got.Steps["a/v1"].Forward.State, state) {
		t.Fatalf("GetRun nonterminal = %+v, %v", got, err)
	}
	// Seven active rows: meta, cancel, failure, input, two operations,
	// and the committed state beside a/v1's.
	if rows := keys(t, s, runPrefix("run-t")); len(rows) != 7 {
		t.Fatalf("active rows = %q; want 7", rows)
	}

	committed := now.Add(time.Minute)
	// The last unwind resolution rides the terminality commit.
	apply(driver.Transition{
		Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: committed, LastError: "stale", NextAttemptAt: now},
		Ops: []driver.OpWrite{{StepID: "a/v1", Phase: durable.PhaseUnwind,
			Record: driver.OperationRecord{Status: driver.OpFailed, Attempts: 2, Failure: &uf, Order: 3}}},
		Outcome: &oc, Output: []byte{9},
	})

	// A terminal run is one row and its expiry key; the stage and the
	// slot are gone with the same commit.
	if rows := keys(t, s, runPrefix("run-t")); len(rows) != 0 {
		t.Errorf("%d active rows survived terminality: %q", len(rows), rows)
	}
	if left := keys(t, s, cursorKey("run-t")); len(left) != 0 {
		t.Error("cursor survived terminality")
	}
	if left := keys(t, s, []byte{nsSlots}); len(left) != 0 {
		t.Errorf("slot survived terminality: %q", left)
	}
	if left := keys(t, s, expiryKey(committed, "run-t")); len(left) != 1 {
		t.Errorf("expiry key = %q; want the run's", left)
	}
	s.db.View(func(txn *bdg.Txn) error {
		tb, _ := get(txn, terminalKey("run-t"))
		if len(tb) == 0 || len(tb) > 1024 {
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
	for _, ns := range []byte{nsActive, nsCursor, nsTerminal, nsExpiry, nsSlots} {
		if left := keys(t, s, []byte{ns}); len(left) != 0 {
			t.Fatalf("namespace %q still holds %q after reap", ns, left)
		}
	}
}

// A head is the run without its blobs and operation history, read in
// point reads.
func TestGetRunHead(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "head"))
	now := time.Unix(1_700_000_000, 0).UTC()
	rec := &driver.RunRecord{RunID: "run-1", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward,
		Annotations: map[string]string{"tenant": "t1"},
		Input:       bytes.Repeat([]byte{1}, 8192), CreatedAt: now, UpdatedAt: now}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatal(err)
	}
	// One resolved step with a large state, one in flight, a cancel.
	err := s.ApplyTransition(ctx, "run-1", driver.Transition{
		Cursor: driver.Cursor{Phase: durable.PhaseForward, StepID: "b", Attempts: 3, LastError: "boom", NextAttemptAt: now.Add(time.Minute), UpdatedAt: now.Add(time.Second)},
		Ops: []driver.OpWrite{{StepID: "a", Phase: durable.PhaseForward, Record: driver.OperationRecord{
			Status: driver.OpSucceeded, Attempts: 1, State: bytes.Repeat([]byte{2}, 8192), Order: 1}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestCancel(ctx, "run-1", driver.CancelRequest{Cause: "op", At: now}); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetRunHead(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetRunHead: %v", err)
	}
	if h.Input != nil || len(h.Steps) != 1 || h.Steps["a"] != nil {
		t.Fatalf("head carries blobs or history: %+v", h)
	}
	if op := h.Steps["b"]; op == nil || op.Forward.Status != driver.OpUnresolved || op.Forward.Attempts != 3 {
		t.Fatalf("head in-flight op = %+v", h.Steps["b"])
	}
	if h.PipelineID != "p" || h.Annotations["tenant"] != "t1" || h.LastError != "boom" || h.Cancel == nil || h.Cancel.Cause != "op" || !h.NextAttemptAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("head = %+v", h)
	}
	full, err := s.GetRun(ctx, "run-1")
	if err != nil || len(full.Input) != 8192 || len(full.Steps["a"].Forward.State) != 8192 {
		t.Fatalf("GetRun after head = %+v, %v", full, err)
	}
	// Terminal: the record without its output.
	oc := durable.OutcomeSuccess
	err = s.ApplyTransition(ctx, "run-1", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone, UpdatedAt: now.Add(time.Hour)},
		Outcome: &oc, Output: bytes.Repeat([]byte{3}, 8192)})
	if err != nil {
		t.Fatal(err)
	}
	h, err = s.GetRunHead(ctx, "run-1")
	if err != nil || !h.Terminal() || *h.Outcome != oc || h.Output != nil || h.Cancel == nil {
		t.Fatalf("terminal head = %+v, %v", h, err)
	}
	if full, err := s.GetRun(ctx, "run-1"); err != nil || len(full.Output) != 8192 {
		t.Fatalf("GetRun terminal = %+v, %v", full, err)
	}
	if _, err := s.GetRunHead(ctx, "nope"); !errors.Is(err, durable.ErrRunNotFound) {
		t.Fatalf("missing head = %v", err)
	}
}

// TestOptions pins the options and their URI forms.
func TestOptions(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "64": 64, "16k": 16 << 10, "8MiB": 8 << 20, " 1 GiB ": 1 << 30} {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "x", "-1", "1.5M", "1TiB", "9223372036854775807g", "8589934592g"} {
		if _, err := parseBytes(bad); err == nil {
			t.Errorf("parseBytes(%q) accepted", bad)
		}
	}
	// A large input and state cross the value threshold the memtable
	// sets, and round-trip through the value log.
	s := open(t, filepath.Join(t.TempDir(), "opts"))
	ctx := context.Background()
	big := bytes.Repeat([]byte{5}, 1<<20)
	rec := &driver.RunRecord{RunID: "run-b", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, Input: big}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatal(err)
	}
	if err := s.ApplyTransition(ctx, "run-b", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward},
		Ops: []driver.OpWrite{{StepID: "a", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: big, Order: 1}}}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, "run-b")
	if err != nil || !bytes.Equal(got.Input, big) || !bytes.Equal(got.Steps["a"].Forward.State, big) {
		t.Fatalf("large blobs = input %d, state %d, %v", len(got.Input), len(got.Steps["a"].Forward.State), err)
	}
}

// A cancel request that commits while a terminality commit is in flight
// is not lost: the terminality commit conflicts and its retry folds the
// request into the terminal record. The two transactions are
// interleaved by hand: the terminality transaction reads and writes,
// RequestCancel commits, then the terminality transaction commits.
func TestCancelRacingTerminalityIsKept(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "race"))
	rec := &driver.RunRecord{RunID: "run-r", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatal(err)
	}
	oc := durable.OutcomeSuccess
	terminal := driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}, Outcome: &oc}

	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	if err := s.applyTransition(txn, "run-r", terminal); err != nil {
		t.Fatal(err)
	}
	if accepted, err := s.RequestCancel(ctx, "run-r", driver.CancelRequest{Cause: "late"}); err != nil || !accepted {
		t.Fatalf("RequestCancel = %v, %v", accepted, err)
	}
	if err := txn.Commit(); !errors.Is(err, bdg.ErrConflict) {
		t.Fatalf("terminality commit = %v; want a conflict with the cancel that committed first", err)
	}
	// The retry, as ApplyTransition runs it, keeps the request.
	if err := s.ApplyTransition(ctx, "run-r", terminal); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, "run-r")
	if err != nil || !got.Terminal() || got.Cancel == nil || got.Cancel.Cause != "late" {
		t.Fatalf("terminal record = %+v, %v; want the accepted cancel in it", got, err)
	}
}

// Writes that arrive together share a transaction; one that fails runs
// alone and returns its own error, and the others commit regardless.
func TestBatchIsolatesFailures(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "batch"))
	const n = 200
	var wg sync.WaitGroup
	errs := make([]error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			rec := &driver.RunRecord{RunID: durable.RunID(fmt.Sprintf("run-%03d", i)), PipelineID: "p", ResourceID: durable.ResourceID(fmt.Sprintf("r-%d", i)), Phase: durable.PhaseForward}
			_, created, err := s.CreateRun(ctx, rec, nil)
			if err == nil && !created {
				err = errors.New("not created")
			}
			errs[i] = err
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[n+i] = s.RequestCancel(ctx, durable.RunID(fmt.Sprintf("missing-%03d", i)), driver.CancelRequest{Cause: "x"})
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("CreateRun %d: %v", i, errs[i])
		}
		if !errors.Is(errs[n+i], durable.ErrRunNotFound) {
			t.Fatalf("RequestCancel(missing) %d = %v; want ErrRunNotFound", i, errs[n+i])
		}
	}
	if runs, err := s.ListNonterminal(ctx); err != nil || len(runs) != n {
		t.Fatalf("ListNonterminal = %d runs, %v; want %d", len(runs), err, n)
	}
}

// Reads of a run in flight take its input and states from the blob
// cache, shown by planting other bytes there; a run that leaves the
// cache reads them from the tree and comes back.
func TestBlobCacheServesReads(t *testing.T) {
	ctx := context.Background()
	s := open(t, filepath.Join(t.TempDir(), "blobs"))
	input, state := bytes.Repeat([]byte{1}, 4096), bytes.Repeat([]byte{2}, 1024)
	rec := &driver.RunRecord{RunID: "run-c", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, Input: input}
	if _, created, err := s.CreateRun(ctx, rec, nil); err != nil || !created {
		t.Fatal(err)
	}
	if err := s.ApplyTransition(ctx, "run-c", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseForward},
		Ops: []driver.OpWrite{{StepID: "a/v1", Phase: durable.PhaseForward, Record: driver.OperationRecord{Status: driver.OpSucceeded, Attempts: 1, State: state, Order: 1}}}}); err != nil {
		t.Fatal(err)
	}
	plantedInput, plantedState := []byte("planted input"), []byte("planted state")
	s.blobs.SetInput("run-c", plantedInput)
	s.blobs.SetState("run-c", "a/v1", plantedState)
	got, err := s.GetRun(ctx, "run-c")
	if err != nil || !bytes.Equal(got.Input, plantedInput) || !bytes.Equal(got.Steps["a/v1"].Forward.State, plantedState) {
		t.Fatalf("GetRun = input %q state %q, %v; want the cached bytes", got.Input, got.Steps["a/v1"].Forward.State, err)
	}
	if got.Steps["a/v1"].Forward.Status != driver.OpSucceeded || got.Steps["a/v1"].Forward.Order != 1 {
		t.Fatalf("operation record = %+v", got.Steps["a/v1"].Forward)
	}
	s.blobs.Drop("run-c")
	got, err = s.GetRun(ctx, "run-c")
	if err != nil || !bytes.Equal(got.Input, input) || !bytes.Equal(got.Steps["a/v1"].Forward.State, state) {
		t.Fatalf("GetRun after drop = %d/%d bytes, %v; want the tree's", len(got.Input), len(got.Steps["a/v1"].Forward.State), err)
	}
	if !s.blobs.Has("run-c") {
		t.Fatal("a read from the tree must refill the cache")
	}
	// Terminality drops the run.
	oc := durable.OutcomeSuccess
	if err := s.ApplyTransition(ctx, "run-c", driver.Transition{Cursor: driver.Cursor{Phase: durable.PhaseDone}, Outcome: &oc}); err != nil {
		t.Fatal(err)
	}
	if s.blobs.Has("run-c") {
		t.Fatal("a terminal run must leave the cache")
	}
}
