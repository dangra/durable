package bboltstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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
	rec.Step("a").ForwardStatus = durable.OpSucceeded
	rec.Step("a").State = []byte{1, 2, 3}
	if err := s.UpdateRun(ctx, rec); err != nil {
		t.Fatalf("UpdateRun: %v", err)
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
	rec.Outcome = &oc
	rec.Phase = durable.PhaseDone
	if err := s.UpdateRun(ctx, rec); err != nil {
		t.Fatalf("UpdateRun terminal: %v", err)
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
