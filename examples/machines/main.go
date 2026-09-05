// Command machines demonstrates the provision-machine pipeline from the
// durable specification end to end against an in-memory store.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dangra/durable/engine"
	"github.com/dangra/durable/examples/machines/machinespb"
	"github.com/dangra/durable/store/mem"
)

func main() {
	ctx := context.Background()

	eng := engine.New(mem.New(),
		engine.WithConcurrencyClass("host-capacity-api", 2))
	provision, err := newProvisionMachine(newCloud()).Bind(eng)
	if err != nil {
		log.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer eng.Stop(ctx)

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
