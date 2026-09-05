// The two generations of the deploy daemon: same store, same release
// train, but yesterday's build binds the legacy deploy pipeline and
// today's binds the evolved one.
package main

import (
	"context"
	"github.com/dangra/durable/store/driver"
	"log/slog"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/release-train/legacypb"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

func quiet() engine.Option { return engine.WithLogger(slog.New(slog.DiscardHandler)) }

func fast() engine.Option {
	return engine.WithRetryPolicy(engine.RetryPolicy{
		Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2})
}

// yesterdaysBuild binds the legacy deploy pipeline (no canary step) and
// the release train, and returns the started engine plus the train
// pipeline handle.
func yesterdaysBuild(ctx context.Context, store driver.Store, w *world) (*engine.Engine, *releasepb.ReleaseTrainPipeline, error) {
	eng := engine.New(store, quiet(), fast(), engine.WithRecoveryBackoff(0))
	deploy, err := legacypb.NewDeployService(
		&legacyProvisionEnv{w}, &legacyRunMigrations{w}, &legacyShiftTraffic{w}, reduceLegacyDeploy,
	).Bind(eng)
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
		releasepb.AnnounceFunc(s.announce),
	).Bind(eng)
	if err != nil {
		return nil, nil, err
	}
	return eng, train, eng.Start(ctx)
}

// todaysBuild binds the current deploy pipeline — same pipeline id,
// canary-analysis step added — and the same release train.
func todaysBuild(ctx context.Context, store driver.Store, w *world) (*engine.Engine, *releasepb.ReleaseTrainPipeline, *releasepb.DeployServicePipeline, error) {
	eng := engine.New(store, quiet(), fast(), engine.WithRecoveryBackoff(0))
	deploy, err := releasepb.NewDeployService(
		&provisionEnv{w}, &runMigrations{w}, &canaryAnalysis{w}, &shiftTraffic{w}, reduceDeploy,
	).Bind(eng)
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
		releasepb.AnnounceFunc(s.announce),
	).Bind(eng)
	if err != nil {
		return nil, nil, nil, err
	}
	return eng, train, deploy, eng.Start(ctx)
}
