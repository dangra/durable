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
//     deploy pipeline gained a canary-analysis step (releaseproto vs
//     legacyproto). The in-flight web deploy is behind the new step's
//     position, so it executes it — a step added while the run was
//     running.
//  4. COMPOSITION — the release train parent parks on its child deploys
//     via AwaitRun; the parks hold no workers and survive the restart.
//  5. CANCELLATION — an incident freezes the release mid-way through
//     the api deploy. Cancel cascades: the parent's awaiting operation
//     is woken with CancelRequested, cancels its child, and both runs
//     unwind — migrations roll back, the environment is torn down.
//
// Run it: go run ./examples/release-train
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/bboltstore"
	"github.com/dangra/durable/examples/release-train/legacypb"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

// world is the fake platform backend. It is shared across both engine
// generations — it stands for the real world (databases, load
// balancers), which is exactly what survives a daemon crash.
type world struct {
	mu sync.Mutex

	envs       map[string]bool   // service -> environment provisioned
	migrated   map[string]string // service -> schema version applied
	rolledBack []string          // services whose migrations were rolled back
	tornDown   []string          // services whose environments were torn down
	traffic    map[string]string // service -> live lb generation
	canaried   map[string]uint32 // service -> canary score
	skips      int               // idempotent migration re-executions

	webDeployID durable.RunID
	apiDeployID durable.RunID

	webMigrating     chan struct{} // closed when web's migration attempt is in flight
	apiCanaryRunning chan struct{} // closed when api's canary attempt is in flight
}

func newWorld() *world {
	return &world{
		envs:     map[string]bool{},
		migrated: map[string]string{},
		traffic:  map[string]string{},
		canaried: map[string]uint32{},

		webMigrating:     make(chan struct{}),
		apiCanaryRunning: make(chan struct{}),
	}
}

func (w *world) logf(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fmt.Printf(format+"\n", args...)
}

// ---- deploy-service handler logic, shared by both builds ----

func (w *world) provision(service string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.envs[service] = true
	fmt.Printf("[%s] environment provisioned\n", service)
	return "env-" + service
}

func (w *world) teardown(service string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.envs, service)
	w.tornDown = append(w.tornDown, service)
	fmt.Printf("[%s] environment torn down (unwind)\n", service)
}

// migrate applies the schema unless it is already applied — the
// idempotency that makes at-least-once re-execution invisible.
func (w *world) migrate(service, version string) (applied bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.migrated[service] == version {
		w.skips++
		fmt.Printf("[%s] migrations already applied — idempotent re-execution\n", service)
		return false
	}
	w.migrated[service] = version
	fmt.Printf("[%s] migrations applied (%s)\n", service, version)
	return true
}

func (w *world) rollback(service string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.migrated, service)
	w.rolledBack = append(w.rolledBack, service)
	fmt.Printf("[%s] migrations rolled back (unwind)\n", service)
}

// ---- today's build: releasepb handlers ----

type provisionEnv struct{ w *world }

func (h *provisionEnv) Run(ctx context.Context, inv releasepb.ProvisionEnvInvocation) (*releasepb.ProvisionEnv, error) {
	return &releasepb.ProvisionEnv{EnvId: h.w.provision(inv.Input().GetService())}, nil
}

func (h *provisionEnv) Unwind(ctx context.Context, inv releasepb.ProvisionEnvInvocation, f durable.Failure) error {
	h.w.teardown(inv.Input().GetService())
	return nil
}

type runMigrations struct{ w *world }

func (h *runMigrations) Run(ctx context.Context, inv releasepb.RunMigrationsInvocation) (*releasepb.RunMigrations, error) {
	h.w.migrate(inv.Input().GetService(), inv.Input().GetImage())
	return &releasepb.RunMigrations{SchemaVersion: inv.Input().GetImage()}, nil
}

func (h *runMigrations) Unwind(ctx context.Context, inv releasepb.RunMigrationsInvocation, f durable.Failure) error {
	h.w.rollback(inv.Input().GetService())
	return nil
}

type canaryAnalysis struct{ w *world }

func (h *canaryAnalysis) Run(ctx context.Context, inv releasepb.CanaryAnalysisInvocation) (*releasepb.CanaryAnalysis, error) {
	service := inv.Input().GetService()
	if inv.CancelRequested() {
		// The cooperative cancellation contract: a started operation is
		// preempted once, then resolves promptly; the engine unwinds.
		h.w.logf("[%s] canary interrupted — yielding to cancellation", service)
		return &releasepb.CanaryAnalysis{}, nil
	}
	if service == "api" {
		// Hold the api canary open so the incident can strike mid-run.
		close(h.w.apiCanaryRunning)
		h.w.logf("[api] canary analysis running...")
		<-ctx.Done() // preempted by the cascading cancel
		return nil, ctx.Err()
	}
	h.w.mu.Lock()
	h.w.canaried[service] = 98
	h.w.mu.Unlock()
	h.w.logf("[%s] canary analysis: score 98 — a step added while this run was in flight", service)
	return &releasepb.CanaryAnalysis{Score: 98}, nil
}

type shiftTraffic struct{ w *world }

func (h *shiftTraffic) Run(ctx context.Context, inv releasepb.ShiftTrafficInvocation) (*releasepb.ShiftTraffic, error) {
	service := inv.Input().GetService()
	h.w.mu.Lock()
	h.w.traffic[service] = inv.Input().GetImage()
	h.w.mu.Unlock()
	h.w.logf("[%s] traffic shifted to %s", service, inv.Input().GetImage())
	return &releasepb.ShiftTraffic{LbGeneration: inv.Input().GetImage()}, nil
}

func reduceDeploy(d *releasepb.DeployService) *releasepb.DeployServiceOutput {
	return &releasepb.DeployServiceOutput{Url: "https://" + d.Input().GetService() + ".example.com"}
}

// ---- yesterday's build: legacypb handlers (no canary step) ----

type legacyProvisionEnv struct{ w *world }

func (h *legacyProvisionEnv) Run(ctx context.Context, inv legacypb.ProvisionEnvInvocation) (*legacypb.ProvisionEnv, error) {
	return &legacypb.ProvisionEnv{EnvId: h.w.provision(inv.Input().GetService())}, nil
}

func (h *legacyProvisionEnv) Unwind(ctx context.Context, inv legacypb.ProvisionEnvInvocation, f durable.Failure) error {
	h.w.teardown(inv.Input().GetService())
	return nil
}

type legacyRunMigrations struct{ w *world }

// Run applies the migration for real, then "crashes" before durable can
// commit the fact: the daemon dies mid-attempt. The restarted build
// re-executes this operation and hits the idempotent skip.
func (h *legacyRunMigrations) Run(ctx context.Context, inv legacypb.RunMigrationsInvocation) (*legacypb.RunMigrations, error) {
	h.w.migrate(inv.Input().GetService(), inv.Input().GetImage())
	close(h.w.webMigrating)
	<-ctx.Done() // the daemon shuts down under us
	h.w.logf("[%s] daemon dying mid-migration; attempt uncommitted", inv.Input().GetService())
	return nil, ctx.Err()
}

func (h *legacyRunMigrations) Unwind(ctx context.Context, inv legacypb.RunMigrationsInvocation, f durable.Failure) error {
	h.w.rollback(inv.Input().GetService())
	return nil
}

type legacyShiftTraffic struct{ w *world }

func (h *legacyShiftTraffic) Run(ctx context.Context, inv legacypb.ShiftTrafficInvocation) (*legacypb.ShiftTraffic, error) {
	// Never reached in this demo: the daemon dies before the web deploy
	// gets here, and the next build routes through canary analysis first.
	h.w.mu.Lock()
	h.w.traffic[inv.Input().GetService()] = inv.Input().GetImage()
	h.w.mu.Unlock()
	return &legacypb.ShiftTraffic{LbGeneration: inv.Input().GetImage()}, nil
}

func reduceLegacyDeploy(d *legacypb.DeployService) *legacypb.DeployServiceOutput {
	return &legacypb.DeployServiceOutput{Url: "https://" + d.Input().GetService() + ".example.com"}
}

// ---- release-train handlers ----

// shipper wires the train's steps to whichever deploy pipeline the
// current build binds; scheduling and canceling child runs goes through
// it so the train handlers stay build-agnostic.
type shipper struct {
	w        *world
	schedule func(ctx context.Context, service, image string) (durable.RunID, error)
	cancel   func(ctx context.Context, id durable.RunID, cause string) error
}

func (s *shipper) plan(ctx context.Context, inv releasepb.PlanReleaseInvocation) error {
	s.w.logf("[train] planning release %s: web, api", inv.Input().GetImageTag())
	return nil
}

func (s *shipper) ship(inv interface {
	Input() *releasepb.ReleaseTrainInput
	AwaitedRunID() (durable.RunID, bool)
	CancelRequested() bool
}, ctx context.Context, service string, store *durable.RunID) error {
	if inv.CancelRequested() {
		s.w.logf("[train] release frozen — canceling %s deploy", service)
		s.w.mu.Lock()
		id := *store
		s.w.mu.Unlock()
		if id != "" {
			if err := s.cancel(ctx, id, "release frozen"); err != nil {
				return err
			}
		}
		return nil // resolve; the engine applies the cancel and unwinds
	}
	if _, woken := inv.AwaitedRunID(); woken {
		s.w.logf("[train] %s shipped", service)
		return nil
	}
	id, err := s.schedule(ctx, service, inv.Input().GetImageTag())
	if err != nil {
		return err
	}
	s.w.mu.Lock()
	*store = id
	s.w.mu.Unlock()
	s.w.logf("[train] %s deploy scheduled; parking until it lands", service)
	return durable.AwaitRun(id)
}

func (s *shipper) shipWeb(ctx context.Context, inv releasepb.ShipWebInvocation) error {
	return s.ship(inv, ctx, "web", &s.w.webDeployID)
}

func (s *shipper) shipApi(ctx context.Context, inv releasepb.ShipApiInvocation) error {
	return s.ship(inv, ctx, "api", &s.w.apiDeployID)
}

// ---- the two daemon generations ----

func quiet() durable.Option { return durable.WithLogger(slog.New(slog.DiscardHandler)) }

func fast() durable.Option {
	return durable.WithRetryPolicy(durable.RetryPolicy{
		Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2})
}

// yesterdaysBuild binds the legacy deploy pipeline (no canary step) and
// the release train, and returns the started engine plus the train
// pipeline handle.
func yesterdaysBuild(ctx context.Context, store durable.Store, w *world) (*durable.Engine, *releasepb.ReleaseTrainPipeline, error) {
	engine := durable.NewEngine(store, quiet(), fast(), durable.WithRecoveryBackoff(0))
	deploy, err := legacypb.NewDeployService(
		&legacyProvisionEnv{w}, &legacyRunMigrations{w}, &legacyShiftTraffic{w}, reduceLegacyDeploy,
	).Bind(engine)
	if err != nil {
		return nil, nil, err
	}
	s := &shipper{
		w: w,
		schedule: func(ctx context.Context, service, image string) (durable.RunID, error) {
			run, _, err := deploy.Schedule(ctx, durable.ResourceID(service),
				&legacypb.DeployServiceInput{Service: service, Image: image})
			return run.ID(), err
		},
		cancel: func(ctx context.Context, id durable.RunID, cause string) error {
			run, err := deploy.Run(ctx, id)
			if err != nil {
				return err
			}
			return run.Cancel(ctx, cause)
		},
	}
	train, err := releasepb.NewReleaseTrain(
		releasepb.PlanReleaseFunc(s.plan), releasepb.ShipWebFunc(s.shipWeb), releasepb.ShipApiFunc(s.shipApi),
	).Bind(engine)
	if err != nil {
		return nil, nil, err
	}
	return engine, train, engine.Start(ctx)
}

// todaysBuild binds the current deploy pipeline — same pipeline id,
// canary-analysis step added — and the same release train.
func todaysBuild(ctx context.Context, store durable.Store, w *world) (*durable.Engine, *releasepb.ReleaseTrainPipeline, *releasepb.DeployServicePipeline, error) {
	engine := durable.NewEngine(store, quiet(), fast(), durable.WithRecoveryBackoff(0))
	deploy, err := releasepb.NewDeployService(
		&provisionEnv{w}, &runMigrations{w}, &canaryAnalysis{w}, &shiftTraffic{w}, reduceDeploy,
	).Bind(engine)
	if err != nil {
		return nil, nil, nil, err
	}
	s := &shipper{
		w: w,
		schedule: func(ctx context.Context, service, image string) (durable.RunID, error) {
			run, _, err := deploy.Schedule(ctx, durable.ResourceID(service),
				&releasepb.DeployServiceInput{Service: service, Image: image})
			return run.ID(), err
		},
		cancel: func(ctx context.Context, id durable.RunID, cause string) error {
			run, err := deploy.Run(ctx, id)
			if err != nil {
				return err
			}
			return run.Cancel(ctx, cause)
		},
	}
	train, err := releasepb.NewReleaseTrain(
		releasepb.PlanReleaseFunc(s.plan), releasepb.ShipWebFunc(s.shipWeb), releasepb.ShipApiFunc(s.shipApi),
	).Bind(engine)
	if err != nil {
		return nil, nil, nil, err
	}
	return engine, train, deploy, engine.Start(ctx)
}

// story runs the whole demo against dir and returns the world plus the
// train's and the api deploy's terminal results.
func story(ctx context.Context, dir string) (*world, durable.Result, releasepb.DeployServiceResult, error) {
	w := newWorld()
	fail := func(err error) (*world, durable.Result, releasepb.DeployServiceResult, error) {
		return nil, durable.Result{}, releasepb.DeployServiceResult{}, err
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
	fmt.Printf("  train: canceled=%v\n", trainResult.Canceled())
	fmt.Printf("  web:   traffic=%s canary=%d (migrations idempotently re-run %d time(s))\n",
		w.traffic["web"], w.canaried["web"], w.skips)
	fmt.Printf("  api:   canceled=%v rolled_back=%v env_torn_down=%v\n",
		apiResult.Canceled(), len(w.rolledBack) > 0, len(w.tornDown) > 0)
}
