package main

import (
	"context"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/snapshots/snapshotspb"
	"github.com/dangra/durable/store/mem"
	"google.golang.org/protobuf/proto"
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

// The closures are unit-testable without an engine: the generated
// NewXxxInvocation wraps the durabletest fake, and XxxReducer.Reduce
// folds it as a reducer view.

func TestUploadUnwindDeletesObject(t *testing.T) {
	ctx := context.Background()
	w := newWorld()
	w.objects["backups/vol-9/run.img"] = 1

	inv := durabletest.NewInvocation(durabletest.InvocationConfig{
		ResourceID: "vol-9",
		StepID:     snapshotspb.UploadSnapshotStep.ID(),
		Phase:      durable.PhaseUnwind,
		State: map[durable.StepID]proto.Message{
			snapshotspb.UploadSnapshotStep.ID(): &snapshotspb.UploadSnapshot{ObjectKey: "backups/vol-9/run.img"},
		},
		Failure: &durable.Failure{Root: durable.RootFailure{FailureRecord: durable.FailureRecord{StepID: "register-snapshot/v1"}}},
	})
	if err := uploadSnapshot(w).Unwind(ctx, snapshotspb.NewUploadSnapshotInvocation(inv)); err != nil {
		t.Fatalf("Unwind: %v", err)
	}
	if len(w.objects) != 0 {
		t.Fatalf("objects = %v; want the orphan deleted", w.objects)
	}
	if inv.Violation() != nil {
		t.Fatalf("violation: %v", inv.Violation())
	}

	// Without committed upload state there is nothing to delete.
	bare := durabletest.NewInvocation(durabletest.InvocationConfig{Phase: durable.PhaseUnwind, Failure: &durable.Failure{}})
	w.objects["other"] = 1
	if err := uploadSnapshot(w).Unwind(ctx, snapshotspb.NewUploadSnapshotInvocation(bare)); err != nil || len(w.objects) != 1 {
		t.Fatalf("Unwind without state = %v, objects %v", err, w.objects)
	}
}

func TestRegisterFailsPermanentlyWhenCatalogFull(t *testing.T) {
	w := newWorld()
	w.full = true
	inv := durabletest.NewInvocation(durabletest.InvocationConfig{
		State: map[durable.StepID]proto.Message{
			snapshotspb.UploadSnapshotStep.ID(): &snapshotspb.UploadSnapshot{ObjectKey: "k"},
		},
	})
	_, err := registerSnapshot(w).Run(context.Background(), snapshotspb.NewRegisterSnapshotInvocation(inv))
	kind, reason, permanent := durable.FailureInfo(err)
	if !permanent || kind != durable.FailureKindUser || reason != "catalog-full" {
		t.Fatalf("Run = %v (permanent=%v kind=%v reason=%q); want a user/catalog-full Fail", err, permanent, kind, reason)
	}
}

func TestReduceCreateSnapshot(t *testing.T) {
	view := durabletest.NewInvocation(durabletest.InvocationConfig{
		State: map[durable.StepID]proto.Message{
			snapshotspb.UploadSnapshotStep.ID():   &snapshotspb.UploadSnapshot{ObjectKey: "k", Bytes: 7},
			snapshotspb.RegisterSnapshotStep.ID(): &snapshotspb.RegisterSnapshot{SnapshotId: "snap-1"},
		},
	})
	out := snapshotspb.CreateSnapshotReducer(reduceCreateSnapshot).Reduce(view)
	if out.GetSnapshotId() != "snap-1" || out.GetObjectKey() != "k" {
		t.Fatalf("Reduce = %+v", out)
	}
}
