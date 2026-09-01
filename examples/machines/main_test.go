package main

import (
	"context"
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
}
