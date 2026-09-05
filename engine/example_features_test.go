package engine_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/pipelinedef"
)

// ExampleFail declares a permanent failure with attribution: a SaaS
// tenant-onboarding pipeline whose billing step rejects the request
// itself (user kind — no retry would help), unwinding the tenant
// record that was already created. Kind and reason surface on the
// Result for alert routing.
func ExampleFail() {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "onboard-tenant",
		Steps: []pipelinedef.Step{
			{
				ID:     "create-tenant/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					fmt.Printf("removing tenant record (unwinding: %s)\n", f.Root.Reason)
					return nil
				},
			},
			{
				ID: "configure-billing/v1",
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					return nil, durable.Fail(errors.New("VAT id rejected by billing provider"),
						durable.WithUserKind(), durable.WithReason("invalid-vat-id"))
				},
			},
		},
	})

	eng := engine.New(durabletest.NewMemStore())
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	run, _, err := pipeline.Schedule(ctx, "tenant-acme", nil)
	if err != nil {
		panic(err)
	}
	result, err := run.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("outcome=%s kind=%s reason=%s\n",
		result.Outcome, result.RootFailure.Kind, result.RootFailure.Reason)
	// Output:
	// removing tenant record (unwinding: invalid-vat-id)
	// outcome=failure kind=user reason=invalid-vat-id
}

// ExampleRun_Cancel aborts a deployment mid-flight. Cancellation reuses
// unwind: the environment that step one provisioned is torn down by its
// unwind handler, and the Run terminates as a failure attributed to
// cancellation. A started operation is never abandoned — its in-flight
// attempt is preempted once through its ctx, and the re-executed
// attempt observes CancelRequested and resolves; the Run stays
// cancelable until terminal success commits, so the engine then takes
// over and unwinds.
func ExampleRun_Cancel() {
	verifying := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "deploy-service",
		Steps: []pipelinedef.Step{
			{
				ID:     "provision-env/v1",
				Unwind: true,
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					fmt.Println("provisioned staging env")
					return nil, nil
				},
				UnwindFunc: func(ctx context.Context, inv durable.Invocation, f durable.Failure) error {
					fmt.Println("tearing down staging env")
					return nil
				},
			},
			{
				ID: "verify/v1",
				Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
					if inv.CancelRequested() {
						fmt.Println("verify: cancel requested; yielding")
						return nil, nil // resolve, the engine unwinds from here
					}
					close(verifying)
					<-ctx.Done() // preempted by the cancel request
					return nil, ctx.Err()
				},
			},
		},
	})

	eng := engine.New(durabletest.NewMemStore(),
		engine.WithRetryPolicy(engine.RetryPolicy{Initial: time.Millisecond, Max: time.Millisecond, Multiplier: 1}))
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	run, _, err := pipeline.Schedule(ctx, "service-web", nil)
	if err != nil {
		panic(err)
	}
	<-verifying
	if err := run.Cancel(ctx, "bad canary metrics"); err != nil {
		panic(err)
	}
	result, err := run.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("canceled:", result.Canceled())
	// Output:
	// provisioned staging env
	// verify: cancel requested; yielding
	// tearing down staging env
	// canceled: true
}

// ExampleAwaitRun composes runs: a release-train parent schedules a
// child deploy and parks on it — no worker is held while waiting, and
// the park survives restarts. The woken attempt re-executes fresh,
// distinguishing "woken after completion" through AwaitedRunID.
func ExampleAwaitRun() {
	eng := engine.New(durabletest.NewMemStore())

	deploy := pipelinedef.New(pipelinedef.Config{
		ID: "deploy-service",
		Steps: []pipelinedef.Step{{
			ID: "rollout/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				fmt.Println("deploying", inv.ResourceID())
				return nil, nil
			},
		}},
	})
	deployPipe, err := eng.Bind(deploy)
	if err != nil {
		panic(err)
	}

	release := pipelinedef.New(pipelinedef.Config{
		ID: "release-train",
		Steps: []pipelinedef.Step{{
			ID: "ship-services/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if _, woken := inv.AwaitedRunID(); woken {
					fmt.Println("child deploy finished; release complete")
					return nil, nil
				}
				child, _, err := deployPipe.Schedule(ctx, "service-web", nil)
				if err != nil {
					return nil, err
				}
				return nil, durable.AwaitRun(child.ID())
			},
		}},
	})
	releasePipe, err := eng.Bind(release)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	run, _, err := releasePipe.Schedule(ctx, "train-42", nil)
	if err != nil {
		panic(err)
	}
	result, err := run.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("succeeded:", result.Succeeded())
	// Output:
	// deploying service-web
	// child deploy finished; release complete
	// succeeded: true
}

// ExamplePipeline_Schedule shows duplicate-scheduling semantics: while
// a Run is active on a resource, scheduling the same work again returns
// the existing Run with created=false — Schedule is a safe idempotent
// entry point for at-least-once callers like message consumers.
func ExamplePipeline_Schedule() {
	release := make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID: "deploy-service",
		Steps: []pipelinedef.Step{{
			ID: "rollout/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				<-release
				return nil, nil
			},
		}},
	})

	eng := engine.New(durabletest.NewMemStore())
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	first, created, err := pipeline.Schedule(ctx, "service-web", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("first schedule created:", created)

	dup, created, err := pipeline.Schedule(ctx, "service-web", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("second schedule created:", created, "— same run:", dup.ID() == first.ID())

	close(release)
	if _, err := first.Wait(ctx); err != nil {
		panic(err)
	}
	// Output:
	// first schedule created: true
	// second schedule created: false — same run: true
}

// ExampleWithConcurrencyClass bounds concurrent work: with one
// "cluster" token, the second deploy waits for the first to finish —
// tokens are held only while a handler executes, never across retries
// or parks.
func ExampleWithConcurrencyClass() {
	webRunning, finishWeb := make(chan struct{}), make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID:               "deploy-service",
		ConcurrencyClass: "cluster",
		Steps: []pipelinedef.Step{{
			ID: "rollout/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				fmt.Println("start", inv.ResourceID())
				if inv.ResourceID() == "service-web" {
					close(webRunning)
					<-finishWeb
				}
				fmt.Println("done ", inv.ResourceID())
				return nil, nil
			},
		}},
	})

	eng := engine.New(durabletest.NewMemStore(),
		engine.WithConcurrencyClass("cluster", 1))
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	web, _, err := pipeline.Schedule(ctx, "service-web", nil)
	if err != nil {
		panic(err)
	}
	<-webRunning // web holds the only cluster token
	api, _, err := pipeline.Schedule(ctx, "service-api", nil)
	if err != nil {
		panic(err)
	}
	close(finishWeb)
	if _, err := web.Wait(ctx); err != nil {
		panic(err)
	}
	if _, err := api.Wait(ctx); err != nil {
		panic(err)
	}
	// Output:
	// start service-web
	// done  service-web
	// start service-api
	// done  service-api
}

// ExampleStartAfter delays a Run's first attempt — a certificate
// renewal scheduled ahead of expiry. The Run occupies its resource slot
// immediately and the delay survives restarts.
func ExampleStartAfter() {
	def := pipelinedef.New(pipelinedef.Config{
		ID: "renew-certificate",
		Steps: []pipelinedef.Step{{
			ID: "renew/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				return nil, nil
			},
		}},
	})

	eng := engine.New(durabletest.NewMemStore())
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	run, _, err := pipeline.Schedule(ctx, "example.com", nil,
		durable.StartAfter(time.Second))
	if err != nil {
		panic(err)
	}
	status, err := run.Status(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("state:", status.State)

	result, err := run.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("succeeded:", result.Succeeded())
	// Output:
	// state: scheduled
	// succeeded: true
}

// ExampleEngine_Stats snapshots engine occupancy for poll-style gauge
// collection — here, one deploy holding the only cluster token while a
// second waits on it.
func ExampleEngine_Stats() {
	webRunning, finishWeb := make(chan struct{}), make(chan struct{})
	def := pipelinedef.New(pipelinedef.Config{
		ID:               "deploy-service",
		ConcurrencyClass: "cluster",
		Steps: []pipelinedef.Step{{
			ID: "rollout/v1",
			Run: func(ctx context.Context, inv durable.Invocation) (proto.Message, error) {
				if inv.ResourceID() == "service-web" {
					close(webRunning)
					<-finishWeb
				}
				return nil, nil
			},
		}},
	})

	eng := engine.New(durabletest.NewMemStore(),
		engine.WithConcurrencyClass("cluster", 1))
	pipeline, err := eng.Bind(def)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := eng.Start(ctx); err != nil {
		panic(err)
	}
	defer eng.Stop(ctx)

	web, _, err := pipeline.Schedule(ctx, "service-web", nil)
	if err != nil {
		panic(err)
	}
	<-webRunning
	api, _, err := pipeline.Schedule(ctx, "service-api", nil)
	if err != nil {
		panic(err)
	}
	// Poll until the second deploy has parked on the class token.
	stats := eng.Stats()
	for stats.ThrottledRuns == 0 {
		time.Sleep(time.Millisecond)
		stats = eng.Stats()
	}
	cluster := stats.Classes["cluster"]
	fmt.Printf("throttled=%d cluster: capacity=%d in_use=%d waiting=%d\n",
		stats.ThrottledRuns, cluster.Capacity, cluster.InUse, cluster.Waiting)

	close(finishWeb)
	if _, err := web.Wait(ctx); err != nil {
		panic(err)
	}
	if _, err := api.Wait(ctx); err != nil {
		panic(err)
	}
	// Output:
	// throttled=1 cluster: capacity=1 in_use=1 waiting=1
}
