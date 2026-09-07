// Command snapshots demonstrates a pipeline whose handlers are plain
// functions. Every generated handler interface comes with an adapter in
// the style of http.HandlerFunc: XxxFunc for a forward-only step, XxxFuncs
// for a step with an unwind. The pipeline is assembled from closures over
// one dependency struct, with no handler types and no methods.
//
// The pipeline snapshots a volume: freeze it, upload the frozen image to
// object storage, thaw it, register the snapshot in a catalog. A failure
// unwinds in reverse: the uploaded object is deleted and the volume is
// thawed again, so a failed run leaves the world as it found it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/snapshots/snapshotspb"
	"github.com/dangra/durable/store/mem"
)

// world is the fake infrastructure the handlers close over.
type world struct {
	mu       sync.Mutex
	frozen   map[string]string // volume -> freeze token
	objects  map[string]uint64 // object key -> bytes
	catalog  map[string]string // snapshot id -> object key
	full     bool              // the catalog rejects new entries
	thawed   int
	nextID   int
	uploaded int
}

func newWorld() *world {
	return &world{
		frozen:  map[string]string{},
		objects: map[string]uint64{},
		catalog: map[string]string{},
	}
}

var errCatalogFull = errors.New("snapshot catalog is full")

// freeze is idempotent: a re-executed attempt gets the token the first
// one minted, which is what makes it safe under at-least-once execution.
func (w *world) freeze(volume string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tok, ok := w.frozen[volume]; ok {
		return tok
	}
	w.nextID++
	tok := fmt.Sprintf("frz-%d", w.nextID)
	w.frozen[volume] = tok
	return tok
}

func (w *world) thaw(volume string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.frozen[volume]; ok {
		delete(w.frozen, volume)
		w.thawed++
	}
}

func (w *world) upload(key string) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.uploaded++
	w.objects[key] = 4096 * uint64(len(key))
	return w.objects[key]
}

func (w *world) delete(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.objects, key)
}

func (w *world) register(key string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.full {
		return "", errCatalogFull
	}
	w.nextID++
	id := fmt.Sprintf("snap-%d", w.nextID)
	w.catalog[id] = key
	return id, nil
}

// freezeVolume returns the freeze step as a pair of closures. Returning
// the adapter from a function keeps each step's forward and unwind
// halves next to each other and lets tests build a step on its own.
func freezeVolume(w *world) snapshotspb.FreezeVolumeFuncs {
	return snapshotspb.FreezeVolumeFuncs{
		RunFunc: func(ctx context.Context, inv snapshotspb.FreezeVolumeInvocation) (*snapshotspb.FreezeVolume, error) {
			return &snapshotspb.FreezeVolume{FreezeToken: w.freeze(string(inv.ResourceID()))}, nil
		},
		// Thaw on unwind so a failed run never leaves the volume frozen.
		// After a successful ThawVolume step this is a no-op.
		UnwindFunc: func(ctx context.Context, inv snapshotspb.FreezeVolumeInvocation) error {
			w.thaw(string(inv.ResourceID()))
			return nil
		},
	}
}

func uploadSnapshot(w *world) snapshotspb.UploadSnapshotFuncs {
	return snapshotspb.UploadSnapshotFuncs{
		RunFunc: func(ctx context.Context, inv snapshotspb.UploadSnapshotInvocation) (*snapshotspb.UploadSnapshot, error) {
			if _, ok := inv.State(snapshotspb.FreezeVolumeStep); !ok {
				return nil, durable.Fail(errors.New("volume is not frozen"))
			}
			// The run id in the key makes the upload idempotent per run.
			key := path.Join(inv.Input().GetBucket(), string(inv.ResourceID()), string(inv.RunID())+".img")
			return &snapshotspb.UploadSnapshot{ObjectKey: key, Bytes: w.upload(key)}, nil
		},
		UnwindFunc: func(ctx context.Context, inv snapshotspb.UploadSnapshotInvocation) error {
			up, ok := inv.State(snapshotspb.UploadSnapshotStep)
			if !ok {
				return nil
			}
			// The failure being unwound is on the invocation.
			inv.Logger().Info("deleting orphaned snapshot object",
				"key", up.GetObjectKey(), "root_step", inv.Failure().StepID)
			w.delete(up.GetObjectKey())
			return nil
		},
	}
}

// A forward-only step is a single function.
func thawVolume(w *world) snapshotspb.ThawVolumeFunc {
	return func(ctx context.Context, inv snapshotspb.ThawVolumeInvocation) error {
		w.thaw(string(inv.ResourceID()))
		return nil
	}
}

func registerSnapshot(w *world) snapshotspb.RegisterSnapshotFunc {
	return func(ctx context.Context, inv snapshotspb.RegisterSnapshotInvocation) (*snapshotspb.RegisterSnapshot, error) {
		up, ok := inv.State(snapshotspb.UploadSnapshotStep)
		if !ok {
			return nil, durable.Fail(errors.New("upload state unavailable"))
		}
		id, err := w.register(up.GetObjectKey())
		if errors.Is(err, errCatalogFull) {
			// A permanent decision: start the unwind, attributed to the
			// request rather than to the system.
			return nil, durable.Fail(err, durable.WithUserKind(), durable.WithReason("catalog-full"))
		}
		if err != nil {
			return nil, err // retried
		}
		return &snapshotspb.RegisterSnapshot{SnapshotId: id}, nil
	}
}

func reduceCreateSnapshot(p *snapshotspb.CreateSnapshot) *snapshotspb.CreateSnapshotOutput {
	reg, _ := p.State(snapshotspb.RegisterSnapshotStep)
	up, _ := p.State(snapshotspb.UploadSnapshotStep)
	return &snapshotspb.CreateSnapshotOutput{SnapshotId: reg.GetSnapshotId(), ObjectKey: up.GetObjectKey()}
}

func newCreateSnapshot(w *world) *snapshotspb.CreateSnapshotDefinition {
	return snapshotspb.NewCreateSnapshot(
		freezeVolume(w),
		uploadSnapshot(w),
		thawVolume(w),
		registerSnapshot(w),
		reduceCreateSnapshot,
	)
}

func main() {
	ctx := context.Background()
	w := newWorld()

	eng := engine.New(mem.New())
	snapshots, err := newCreateSnapshot(w).Bind(eng)
	if err != nil {
		log.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer eng.Stop(ctx)

	input := &snapshotspb.CreateSnapshotInput{Bucket: "backups"}

	// The happy path: four steps forward, one snapshot in the catalog.
	run, _, err := snapshots.Schedule(ctx, "vol-1", input)
	if err != nil {
		log.Fatal(err)
	}
	result, err := run.Wait(ctx)
	if err != nil || !result.Succeeded() {
		log.Fatalf("snapshot of vol-1: %+v, %v", result, err)
	}
	out := result.Output()
	fmt.Printf("vol-1: snapshot %s at %s\n", out.GetSnapshotId(), out.GetObjectKey())

	// The catalog fills up. Registration fails permanently, and the unwind
	// deletes the uploaded object and thaws the volume again.
	w.full = true
	run, _, err = snapshots.Schedule(ctx, "vol-2", input)
	if err != nil {
		log.Fatal(err)
	}
	result, err = run.Wait(ctx)
	if err != nil || !result.Failed() {
		log.Fatalf("snapshot of vol-2: %+v, %v", result, err)
	}
	fmt.Printf("vol-2: failed at %s (%s/%s); unwound: objects in storage %d (vol-1's), volumes frozen %d\n",
		result.Failure.StepID, result.Failure.Kind, result.Failure.Reason,
		len(w.objects), len(w.frozen))
}
