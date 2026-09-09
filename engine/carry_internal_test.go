package engine

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/mem"
)

type carryLog struct{ strings.Builder }

func (l *carryLog) Write(p []byte) (int, error) { return l.Builder.Write(p) }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time                       { return c.t }
func (c fixedClock) After(time.Duration) <-chan time.Time { return nil }

// The carry's discipline is checked, not assumed: an iteration that
// continued without a transition abandons the record for a full read,
// as does one that finds the Run marked dirty; otherwise the record
// carries on and nothing is read.
func TestRefreshCarriedGuardsTheDiscipline(t *testing.T) {
	ctx := context.Background()
	logs := &carryLog{}
	st := mem.New()
	e := New(st, WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
	e.baseCtx = ctx
	now := time.Unix(1_700_000_000, 0).UTC()
	rec := &driver.RunRecord{RunID: "r1", PipelineID: "p", ResourceID: "r", Phase: durable.PhaseForward, CreatedAt: now, UpdatedAt: now}
	if _, _, err := st.CreateRun(ctx, rec, nil); err != nil {
		t.Fatal(err)
	}
	rec, err := st.GetRun(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}

	// No transition since the stamp: the continue was illegal.
	if got := e.refreshCarried("r1", rec, rec.UpdatedAt); got != nil {
		t.Fatalf("refreshCarried without a transition = %v; want nil", got)
	}
	if !strings.Contains(logs.String(), "continued without a transition") {
		t.Fatalf("no log:\n%s", logs.String())
	}

	// A transition advances the commit time — under a stalled clock too
	// — and the carry survives.
	stamp := rec.UpdatedAt
	e.clock = fixedClock{now}
	if !e.apply(rec, driver.Transition{Cursor: activeCursor(rec, "a/v1", 1)}) {
		t.Fatal("apply failed")
	}
	if !rec.UpdatedAt.After(stamp) {
		t.Fatalf("UpdatedAt did not advance: %v vs %v", rec.UpdatedAt, stamp)
	}
	if got := e.refreshCarried("r1", rec, stamp); got != rec {
		t.Fatalf("refreshCarried after a transition = %+v; want the record", got)
	}

	// Dirty: abandoned for a re-read, and the mark is consumed.
	e.mu.Lock()
	e.dirty["r1"] = struct{}{}
	e.mu.Unlock()
	if got := e.refreshCarried("r1", rec, stamp); got != nil {
		t.Fatalf("refreshCarried when dirty = %+v; want nil", got)
	}
	if got := e.refreshCarried("r1", rec, stamp); got != rec {
		t.Fatalf("the mark must be consumed; got %v", got)
	}
}
