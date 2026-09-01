// Command machines demonstrates the provision-machine pipeline from the
// durable specification end to end against an in-memory store.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dangra/durable"
	"github.com/dangra/durable/durabletest"
	"github.com/dangra/durable/examples/machines/machinespb"
)

func main() {
	ctx := context.Background()

	engine := durable.NewEngine(durabletest.NewMemStore())
	provision, err := newProvisionMachine(newCloud()).Bind(engine)
	if err != nil {
		log.Fatal(err)
	}
	if err := engine.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer engine.Stop(ctx)

	run, created, err := provision.Schedule(ctx, "machine-123", &machinespb.ProvisionMachineInput{
		Region:   "ord",
		MemoryMb: 8192,
		Cpus:     4,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("run %s (created=%v)\n", run.ID(), created)

	result, err := run.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if result.Failed() {
		log.Fatalf("provisioning failed at %s: %s", result.RootFailure.StepID, result.RootFailure.Message)
	}
	out := result.Output()
	fmt.Printf("provisioned %s on %s\n", out.GetMachineId(), out.GetHostId())
}
