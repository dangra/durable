package mem_test

import (
	"context"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
)

// GetActiveRunID follows the slot: set at CreateRun, released when the
// Run commits an Outcome, reusable afterwards.
func TestGetActiveRunIDFollowsTheSlot(t *testing.T) {
	ctx := context.Background()
	s := mem.New()
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
	if _, ok, _ := s.GetActiveRunID(ctx, "other", "r"); ok {
		t.Fatal("another pipeline's slot must be free")
	}
	if _, created, _ := s.CreateRun(ctx, rec("b"), nil); created {
		t.Fatal("slot occupied: second create must dedup")
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
	if id, ok, _ := s.GetActiveRunID(ctx, "p", "r"); !ok || id != "c" {
		t.Fatalf("after reuse: %q %v", id, ok)
	}
}
