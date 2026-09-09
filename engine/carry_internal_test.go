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

// The carry's discipline is checked, not assumed: an iteration that
// continued without a transition, or whose head disagrees, abandons the
// record for a full read; one that transitioned carries on, taking the
// cancel request from the head; one holding a cancel reads nothing.
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
	if got, err := e.refreshCarried("r1", rec, rec.UpdatedAt); got != nil || err != nil {
		t.Fatalf("refreshCarried without a transition = %v, %v; want nil", got, err)
	}
	if !strings.Contains(logs.String(), "continued without a transition") {
		t.Fatalf("no log:\n%s", logs.String())
	}

	// A transition advances the commit time — under a stalled clock too
	// — and the carry survives, with the cancel request taken from the
	// store.
	stamp := rec.UpdatedAt
	e.clock = fixedClock{now}
	if !e.apply(rec, driver.Transition{Cursor: activeCursor(rec, "a/v1", 1)}) {
		t.Fatal("apply failed")
	}
	if !rec.UpdatedAt.After(stamp) {
		t.Fatalf("UpdatedAt did not advance: %v vs %v", rec.UpdatedAt, stamp)
	}
	if _, err := st.RequestCancel(ctx, "r1", driver.CancelRequest{Cause: "op", At: now}); err != nil {
		t.Fatal(err)
	}
	got, err := e.refreshCarried("r1", rec, stamp)
	if err != nil || got != rec || got.Cancel == nil || got.Cancel.Cause != "op" {
		t.Fatalf("refreshCarried after a transition = %+v, %v; want the record with the cancel", got, err)
	}

	// Holding a cancel: nothing is read, so even a store that would
	// disagree is never consulted.
	stamp = rec.UpdatedAt
	if !e.apply(rec, driver.Transition{Cursor: idleCursor(rec)}) {
		t.Fatal("apply failed")
	}
	e.store = lyingStore{st}
	if got, err := e.refreshCarried("r1", rec, stamp); err != nil || got != rec {
		t.Fatalf("refreshCarried holding a cancel = %v, %v; want the record untouched", got, err)
	}

	// Without a cancel, a disagreeing head abandons the carry.
	rec.Cancel = nil
	if got, err := e.refreshCarried("r1", rec, stamp); got != nil || err != nil {
		t.Fatalf("refreshCarried with a lying head = %v, %v; want nil", got, err)
	}
	if !strings.Contains(logs.String(), "disagrees with the store") {
		t.Fatalf("no log:\n%s", logs.String())
	}
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time                       { return c.t }
func (c fixedClock) After(time.Duration) <-chan time.Time { return nil }

type lyingStore struct{ driver.Store }

func (s lyingStore) GetRunHead(ctx context.Context, id durable.RunID) (*driver.RunRecord, error) {
	h, err := s.Store.GetRunHead(ctx, id)
	if err == nil {
		h.UpdatedAt = h.UpdatedAt.Add(time.Hour)
	}
	return h, err
}
