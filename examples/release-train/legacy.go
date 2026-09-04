// Yesterday's build of the deploy-service pipeline: legacypb handlers,
// no canary step yet.
package main

import (
	"context"

	"github.com/dangra/durable"
	"github.com/dangra/durable/examples/release-train/legacypb"
)

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
