// Today's build of the deploy-service pipeline: releasepb handlers,
// canary-analysis step included.
package main

import (
	"context"

	"github.com/dangra/durable"
	"github.com/dangra/durable/examples/release-train/releasepb"
)

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
