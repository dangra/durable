package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/examples/machines/machinespb"
)

func startProvision(t *testing.T, c *cloud) *machinespb.ProvisionMachinePipeline {
	t.Helper()
	engine := durable.NewEngine(durabletest.NewMemStore(), durable.WithRetryPolicy(durable.RetryPolicy{
		Initial:    time.Millisecond,
		Max:        5 * time.Millisecond,
		Multiplier: 2,
	}))
	provision, err := newProvisionMachine(c).Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Stop(ctx)
	})
	return provision
}

func TestProvisionMachineSucceeds(t *testing.T) {
	c := newCloud()
	provision := startProvision(t, c)

	run, created, err := provision.Schedule(context.Background(), "machine-1", &machinespb.ProvisionMachineInput{
		Region:   "ord",
		MemoryMb: 8192,
		Cpus:     4,
	})
	if err != nil || !created {
		t.Fatalf("Schedule = created=%v err=%v", created, err)
	}

	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !result.Succeeded() {
		t.Fatalf("Outcome = %v (root: %+v)", result.Outcome, result.RootFailure)
	}
	out := result.Output()
	if out.GetMachineId() == "" || out.GetHostId() != "host-ord-1" {
		t.Fatalf("Output = %+v", out)
	}

	// The reservation is consumed, not released: no unwind happened.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reservations) != 1 {
		t.Fatalf("reservations = %v, want the successful one kept", c.reservations)
	}
}

func TestProvisionMachineFailureUnwindsReservation(t *testing.T) {
	c := newCloud()
	c.saturated["dfw"] = true
	provision := startProvision(t, c)

	run, _, err := provision.Schedule(context.Background(), "machine-2", &machinespb.ProvisionMachineInput{
		Region:   "dfw",
		MemoryMb: 4096,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !result.Failed() {
		t.Fatalf("Outcome = %v, want failure", result.Outcome)
	}
	if result.RootFailure == nil || result.RootFailure.StepID != "create-machine/v1" {
		t.Fatalf("RootFailure = %+v", result.RootFailure)
	}
	if result.Output() != nil {
		t.Fatalf("Output = %+v, want nil for failed run", result.Output())
	}
	if len(result.UnwindFailures) != 0 {
		t.Fatalf("UnwindFailures = %+v, want none", result.UnwindFailures)
	}

	// reserve-capacity unwound: the reservation was released.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reservations) != 0 {
		t.Fatalf("reservations = %v, want released by unwind", c.reservations)
	}
}

func TestProvisionMachineInputValidationFailsFast(t *testing.T) {
	provision := startProvision(t, newCloud())

	run, _, err := provision.Schedule(context.Background(), "machine-3", &machinespb.ProvisionMachineInput{
		Region: "", // invalid
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !result.Failed() || result.RootFailure.StepID != "validate/v1" {
		t.Fatalf("result = %+v, want validate/v1 root failure", result)
	}
	if result.RootFailure.Kind != durable.FailureKindUser || result.RootFailure.Reason != "invalid-input" {
		t.Fatalf("RootFailure = %+v, want user/invalid-input attribution", result.RootFailure)
	}
}

// TestFuncAdapters builds the same pipeline using the generated
// http.HandlerFunc-style adapters instead of struct handlers.
func TestFuncAdapters(t *testing.T) {
	c := newCloud()
	reserve := &reserveCapacity{cloud: c}
	def := machinespb.NewProvisionMachine(
		machinespb.ValidateFunc(validate{}.Run),
		&selectHost{cloud: c},
		machinespb.ReserveCapacityFuncs{
			RunFunc:    reserve.Run,
			UnwindFunc: reserve.Unwind,
		},
		&createMachine{cloud: c},
		reduceProvisionMachine,
	)

	engine := durable.NewEngine(durabletest.NewMemStore())
	provision, err := def.Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(context.Background())

	run, _, err := provision.Schedule(context.Background(), "machine-4", &machinespb.ProvisionMachineInput{
		Region:   "ord",
		MemoryMb: 2048,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	result, err := run.Wait(context.Background())
	if err != nil || !result.Succeeded() {
		t.Fatalf("Wait = %+v, %v; want success", result, err)
	}
}

// TestExclusionGroup shows the machine-lifecycle group: while a provision
// run is in flight for a machine, decommission is rejected with a conflict
// naming the blocker — and vice versa once the slot frees.
func TestExclusionGroup(t *testing.T) {
	c := newCloud()
	c.createGate = make(chan struct{})

	engine := durable.NewEngine(durabletest.NewMemStore())
	provision, err := newProvisionMachine(c).Bind(engine)
	if err != nil {
		t.Fatalf("Bind provision: %v", err)
	}
	decommission, err := newDecommissionMachine(c).Bind(engine)
	if err != nil {
		t.Fatalf("Bind decommission: %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop(context.Background())

	run, _, err := provision.Schedule(context.Background(), "machine-9", &machinespb.ProvisionMachineInput{
		Region: "ord", MemoryMb: 1024,
	})
	if err != nil {
		t.Fatalf("Schedule provision: %v", err)
	}

	// The group slot is held: decommission is rejected, naming the blocker.
	_, created, err := decommission.Schedule(context.Background(), "machine-9")
	var conflict *durable.ScheduleConflictError
	if !errors.As(err, &conflict) || created {
		t.Fatalf("decommission Schedule = created=%v err=%v, want ScheduleConflictError", created, err)
	}
	if conflict.PipelineID != "provision-machine" || conflict.RunID != run.ID() {
		t.Fatalf("conflict = %+v, want blocker provision-machine/%s", conflict, run.ID())
	}

	// The blocker is inspectable, typed, through its pipeline's handle.
	blocker, err := provision.Run(context.Background(), conflict.RunID)
	if err != nil {
		t.Fatalf("blocker lookup: %v", err)
	}
	in, err := blocker.Input(context.Background())
	if err != nil {
		t.Fatalf("blocker Input: %v", err)
	}
	if in.GetRegion() != "ord" {
		t.Fatalf("blocker input = %+v, want the in-flight provision request", in)
	}
	// And discoverable without scheduling intent.
	active, ok, err := provision.ActiveRun(context.Background(), "machine-9")
	if err != nil || !ok || active.ID() != run.ID() {
		t.Fatalf("ActiveRun = %s ok=%v err=%v, want %s", active.ID(), ok, err, run.ID())
	}

	// A different machine is unaffected.
	if _, created, err := decommission.Schedule(context.Background(), "machine-10"); err != nil || !created {
		t.Fatalf("other-machine decommission = created=%v err=%v", created, err)
	}

	// Once provisioning finishes, the group slot frees.
	close(c.createGate)
	if res, err := run.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("provision Wait = %+v, %v", res, err)
	}
	dRun, created, err := decommission.Schedule(context.Background(), "machine-9")
	if err != nil || !created {
		t.Fatalf("post-completion decommission = created=%v err=%v", created, err)
	}
	if res, err := dRun.Wait(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("decommission Wait = %+v, %v", res, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.released) == 0 || c.released[len(c.released)-1] != "machine-9" {
		t.Fatalf("released = %v, want machine-9", c.released)
	}
}
