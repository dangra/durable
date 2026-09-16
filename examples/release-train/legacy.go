// Yesterday's build of the deploy-service pipeline: legacypb handlers,
// no canary step yet.
package main

import (
	"context"

	"github.com/dangra/durable/examples/release-train/legacypb"
)

// legacyDeploy implements legacypb.DeployServiceHandlers.
type legacyDeploy struct{ w *world }

func (h *legacyDeploy) ProvisionEnv(ctx context.Context, inv legacypb.DeployServiceInvocation) (*legacypb.DeployService_ProvisionEnv, error) {
	return &legacypb.DeployService_ProvisionEnv{EnvId: h.w.provision(inv.Input().GetService())}, nil
}

func (h *legacyDeploy) UnwindProvisionEnv(ctx context.Context, inv legacypb.DeployServiceInvocation) error {
	h.w.teardown(inv.Input().GetService())
	return nil
}

// RunMigrations applies the migration for real, then "crashes" before
// durable can commit the fact: the daemon dies mid-attempt. The
// restarted build re-executes this operation and hits the idempotent
// skip.
func (h *legacyDeploy) RunMigrations(ctx context.Context, inv legacypb.DeployServiceInvocation) (*legacypb.DeployService_RunMigrations, error) {
	h.w.migrate(inv.Input().GetService(), inv.Input().GetImage())
	close(h.w.webMigrating)
	<-ctx.Done() // the daemon shuts down under us
	h.w.logf("[%s] daemon dying mid-migration; attempt uncommitted", inv.Input().GetService())
	return nil, ctx.Err()
}

func (h *legacyDeploy) UnwindRunMigrations(ctx context.Context, inv legacypb.DeployServiceInvocation) error {
	h.w.rollback(inv.Input().GetService())
	return nil
}

func (h *legacyDeploy) ShiftTraffic(ctx context.Context, inv legacypb.DeployServiceInvocation) (*legacypb.DeployService_ShiftTraffic, error) {
	// Never reached in this demo: the daemon dies before the web deploy
	// gets here, and the next build routes through canary analysis first.
	h.w.mu.Lock()
	h.w.traffic[inv.Input().GetService()] = inv.Input().GetImage()
	h.w.mu.Unlock()
	return &legacypb.DeployService_ShiftTraffic{LbGeneration: inv.Input().GetImage()}, nil
}

func (h *legacyDeploy) Reduce(d *legacypb.DeployService) *legacypb.DeployServiceOutput {
	return &legacypb.DeployServiceOutput{Url: "https://" + d.Input().GetService() + ".example.com"}
}
