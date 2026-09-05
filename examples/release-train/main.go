// Command release-train is the flagship durable demo: one release run
// that lives through everything the library exists for.
//
// The story, in one process simulating two builds of a deploy daemon:
//
//  1. DURABILITY — a release train schedules a web deploy; the daemon
//     "crashes" (engine stops) mid-migration. The bbolt store keeps the
//     committed facts; nothing else is saved.
//  2. AT-LEAST-ONCE — the restarted daemon re-executes the interrupted
//     migration attempt; the idempotent handler finds the schema
//     already applied and skips.
//  3. EVOLUTION — the restarted daemon runs *today's build*, whose
//     deploy pipeline gained a canary-analysis step (proto/ vs
//     legacyproto/). The in-flight web deploy is behind the new step's
//     position, so it executes it — a step added while the run was
//     running.
//  4. COMPOSITION — the release train parent parks on its child deploys
//     via AwaitRun; the parks hold no workers and survive the restart.
//  5. CANCELLATION — an incident freezes the release mid-way through
//     the api deploy. Cancel cascades: the parent's awaiting operation
//     is woken with CancelRequested, cancels its child, and both runs
//     unwind — migrations roll back, the environment is torn down.
//
// The cast: world.go is the fake platform backend, deploy.go and
// legacy.go are today's and yesterday's deploy-service handlers,
// train.go the release-train orchestration, builds.go the two daemon
// generations.
//
// Run it: go run ./examples/release-train
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

// story runs the whole demo against dir and returns the world plus the
// train's and the api deploy's terminal results.
func story(ctx context.Context, dir string) (*world, engine.Result, releasepb.DeployServiceResult, error) {
	w := newWorld()
	fail := func(err error) (*world, engine.Result, releasepb.DeployServiceResult, error) {
		return nil, engine.Result{}, releasepb.DeployServiceResult{}, err
	}

	store, err := bboltstore.Open(filepath.Join(dir, "release.db"))
	if err != nil {
		return fail(err)
	}
	defer store.Close()

	// ---- yesterday: the train leaves the station ----
	engine1, train1, err := yesterdaysBuild(ctx, store, w)
	if err != nil {
		return fail(err)
	}
	trainRun, _, err := train1.Schedule(ctx, "train-2026-09", &releasepb.ReleaseTrainInput{ImageTag: "v42"})
	if err != nil {
		return fail(err)
	}
	<-w.webMigrating // the web deploy is mid-migration...
	w.logf("---- daemon crashes; restarting with today's build (canary step added) ----")
	if err := engine1.Stop(ctx); err != nil { // ...and the daemon dies
		return fail(err)
	}

	// ---- today: same store, evolved definition ----
	engine2, train2, deploy2, err := todaysBuild(ctx, store, w)
	if err != nil {
		return fail(err)
	}
	defer engine2.Stop(ctx)

	<-w.apiCanaryRunning // web landed; the api deploy is mid-canary...
	w.logf("---- incident declared: freezing the release ----")
	trainHandle, err := train2.Run(ctx, trainRun.ID())
	if err != nil {
		return fail(err)
	}
	if err := trainHandle.Cancel(ctx, "incident declared"); err != nil {
		return fail(err)
	}

	trainResult, err := trainHandle.Wait(ctx)
	if err != nil {
		return fail(err)
	}
	w.mu.Lock()
	apiID := w.apiDeployID
	w.mu.Unlock()
	apiRun, err := deploy2.Run(ctx, apiID)
	if err != nil {
		return fail(err)
	}
	apiResult, err := apiRun.Wait(ctx)
	if err != nil {
		return fail(err)
	}
	return w, trainResult, apiResult, nil
}

func main() {
	dir, err := os.MkdirTemp("", "release-train")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, trainResult, apiResult, err := story(context.Background(), dir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	fmt.Println("outcome:")
	fmt.Printf("  train: canceled=%v announced=%v (a frozen train never reaches announce/v1)\n",
		trainResult.Canceled(), w.announced)
	fmt.Printf("  web:   traffic=%s canary=%d (migrations idempotently re-run %d time(s))\n",
		w.traffic["web"], w.canaried["web"], w.skips)
	fmt.Printf("  api:   canceled=%v rolled_back=%v env_torn_down=%v\n",
		apiResult.Canceled(), len(w.rolledBack) > 0, len(w.tornDown) > 0)
}
