package main

import (
	"context"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/snapshots/snapshotspb"
	"github.com/dangra/durable/store/mem"
)

var fastRetry = engine.WithRetryPolicy(engine.RetryPolicy{
	Initial:    time.Millisecond,
	Max:        5 * time.Millisecond,
	Multiplier: 2,
})

func start(t *testing.T, w *world) *snapshotspb.CreateSnapshotPipeline {
	t.Helper()
	eng := engine.New(mem.New(), fastRetry)
	p, err := newCreateSnapshot(w).Bind(eng)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { eng.Stop(context.Background()) })
	return p
}

func TestSnapshotSucceeds(t *testing.T) {
	ctx := context.Background()
	w := newWorld()
	p := start(t, w)

	run, _, err := p.Schedule(ctx, "vol-1", &snapshotspb.CreateSnapshotInput{Bucket: "backups"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	result, err := run.Wait(ctx)
	if err != nil || !result.Succeeded() {
		t.Fatalf("Wait = %+v, %v", result, err)
	}
	out := result.Output()
	if key, ok := w.catalog[out.GetSnapshotId()]; !ok || key != out.GetObjectKey() {
		t.Fatalf("catalog = %v, want %s -> %s", w.catalog, out.GetSnapshotId(), out.GetObjectKey())
	}
	if _, ok := w.objects[out.GetObjectKey()]; !ok {
		t.Fatalf("object %s missing after success", out.GetObjectKey())
	}
	if len(w.frozen) != 0 || w.thawed != 1 {
		t.Fatalf("frozen = %v, thawed = %d; want thawed exactly once on the happy path", w.frozen, w.thawed)
	}
}

// A permanent failure at the last step unwinds the upload and the freeze:
// the object is deleted, the volume is thawed, and the failure is
// attributed the way the handler declared it.
func TestSnapshotUnwindsOnCatalogFull(t *testing.T) {
	ctx := context.Background()
	w := newWorld()
	w.full = true
	p := start(t, w)

	run, _, err := p.Schedule(ctx, "vol-2", &snapshotspb.CreateSnapshotInput{Bucket: "backups"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	result, err := run.Wait(ctx)
	if err != nil || !result.Failed() {
		t.Fatalf("Wait = %+v, %v; want failure", result, err)
	}
	rf := result.RootFailure
	if rf.StepID != "register-snapshot/v1" || rf.Kind != durable.FailureKindUser || rf.Reason != "catalog-full" {
		t.Fatalf("RootFailure = %+v", rf)
	}
	if len(result.UnwindFailures) != 0 {
		t.Fatalf("UnwindFailures = %+v; want a clean unwind", result.UnwindFailures)
	}
	if w.uploaded != 1 || len(w.objects) != 0 {
		t.Fatalf("uploaded = %d, objects = %v; want the one upload deleted by unwind", w.uploaded, w.objects)
	}
	if len(w.frozen) != 0 {
		t.Fatalf("frozen = %v after unwind; want thawed", w.frozen)
	}
	// ThawVolume ran forward, then FreezeVolume's unwind found nothing to
	// thaw: the compensation is idempotent.
	if w.thawed != 1 {
		t.Fatalf("thawed = %d; want 1", w.thawed)
	}
	if len(w.catalog) != 0 {
		t.Fatalf("catalog = %v; want empty", w.catalog)
	}
}
