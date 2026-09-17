// Today's build of the deploy-service pipeline: releasepb handlers,
// canary-analysis step included.
package main

import (
	"context"

	"github.com/dangra/durable/examples/release-train/releasepb"
)

// deploy implements releasepb.DeployServiceHandlers: one method per
// step, an Unwind method for each step that rolls back, and the reducer.
type deploy struct{ w *world }

func (h *deploy) ProvisionEnv(ctx context.Context, inv releasepb.DeployServiceInvocation) (*releasepb.DeployService_ProvisionEnv, error) {
	return &releasepb.DeployService_ProvisionEnv{EnvId: h.w.provision(inv.Input().GetService())}, nil
}

func (h *deploy) UnwindProvisionEnv(ctx context.Context, inv releasepb.DeployServiceInvocation) error {
	h.w.teardown(inv.Input().GetService())
	return nil
}

func (h *deploy) RunMigrations(ctx context.Context, inv releasepb.DeployServiceInvocation) (*releasepb.DeployService_RunMigrations, error) {
	h.w.migrate(inv.Input().GetService(), inv.Input().GetImage())
	return &releasepb.DeployService_RunMigrations{SchemaVersion: inv.Input().GetImage()}, nil
}

func (h *deploy) UnwindRunMigrations(ctx context.Context, inv releasepb.DeployServiceInvocation) error {
	h.w.rollback(inv.Input().GetService())
	return nil
}

func (h *deploy) CanaryAnalysis(ctx context.Context, inv releasepb.DeployServiceInvocation) (*releasepb.DeployService_CanaryAnalysis, error) {
	service := inv.Input().GetService()
	if service == "api" {
		// Hold the api canary open so the incident can strike mid-run.
		close(h.w.apiCanaryRunning)
		h.w.logf("[api] canary analysis running...")
		<-ctx.Done() // the cascading cancel kills the ctx
		h.w.logf("[api] canary interrupted by cancellation")
		return nil, ctx.Err() // under a cancel: the step resolves as canceled
	}
	h.w.mu.Lock()
	h.w.canaried[service] = 98
	h.w.mu.Unlock()
	h.w.logf("[%s] canary analysis: score 98 — a step added while this run was in flight", service)
	return &releasepb.DeployService_CanaryAnalysis{Score: 98}, nil
}

func (h *deploy) ShiftTraffic(ctx context.Context, inv releasepb.DeployServiceInvocation) (*releasepb.DeployService_ShiftTraffic, error) {
	service := inv.Input().GetService()
	h.w.mu.Lock()
	h.w.traffic[service] = inv.Input().GetImage()
	h.w.mu.Unlock()
	h.w.logf("[%s] traffic shifted to %s", service, inv.Input().GetImage())
	return &releasepb.DeployService_ShiftTraffic{LbGeneration: inv.Input().GetImage()}, nil
}

func (h *deploy) ReduceOutput(d *releasepb.DeployService) *releasepb.DeployServiceOutput {
	return &releasepb.DeployServiceOutput{Url: "https://" + d.Input().GetService() + ".example.com"}
}
