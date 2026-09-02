package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dangra/durable"
	"github.com/dangra/durable/examples/machines/machinespb"
)

// cloud is a fake infrastructure provider backing the example handlers.
type cloud struct {
	mu           sync.Mutex
	nextID       int
	reservations map[string]bool // reservation id -> held
	saturated    map[string]bool // regions where machine creation fails
}

func newCloud() *cloud {
	return &cloud{
		reservations: make(map[string]bool),
		saturated:    make(map[string]bool),
	}
}

func (c *cloud) id(prefix string) string {
	c.nextID++
	return fmt.Sprintf("%s-%d", prefix, c.nextID)
}

type validate struct{}

func (validate) Run(ctx context.Context, inv machinespb.ValidateInvocation) error {
	in := inv.Input()
	if in.GetRegion() == "" {
		return durable.Fail(errors.New("region is required"),
			durable.WithUserKind(), durable.WithReason("invalid-input"))
	}
	if in.GetMemoryMb() == 0 {
		return durable.Fail(errors.New("memory_mb is required"),
			durable.WithUserKind(), durable.WithReason("invalid-input"))
	}
	return nil
}

type selectHost struct{ cloud *cloud }

func (h *selectHost) Run(ctx context.Context, inv machinespb.SelectHostInvocation) (*machinespb.SelectHost, error) {
	return &machinespb.SelectHost{
		HostId: "host-" + inv.Input().GetRegion() + "-1",
	}, nil
}

type reserveCapacity struct{ cloud *cloud }

func (h *reserveCapacity) Run(ctx context.Context, inv machinespb.ReserveCapacityInvocation) (*machinespb.ReserveCapacity, error) {
	host, ok := inv.State(machinespb.SelectHostStep)
	if !ok {
		return nil, durable.Fail(errors.New("select-host state unavailable"))
	}
	h.cloud.mu.Lock()
	defer h.cloud.mu.Unlock()
	id := h.cloud.id("res")
	h.cloud.reservations[id] = true
	_ = host
	return &machinespb.ReserveCapacity{ReservationId: id}, nil
}

func (h *reserveCapacity) Unwind(ctx context.Context, inv machinespb.ReserveCapacityInvocation, failure durable.Failure) error {
	reservation, ok := inv.State(machinespb.ReserveCapacityStep)
	if !ok {
		return nil
	}
	h.cloud.mu.Lock()
	defer h.cloud.mu.Unlock()
	delete(h.cloud.reservations, reservation.GetReservationId())
	return nil
}

type createMachine struct{ cloud *cloud }

func (h *createMachine) Run(ctx context.Context, inv machinespb.CreateMachineInvocation) (*machinespb.CreateMachine, error) {
	reservation, ok := inv.State(machinespb.ReserveCapacityStep)
	if !ok {
		return nil, durable.Fail(errors.New("reservation state unavailable"))
	}
	h.cloud.mu.Lock()
	defer h.cloud.mu.Unlock()
	if h.cloud.saturated[inv.Input().GetRegion()] {
		return nil, durable.Fail(fmt.Errorf("region %s has no capacity", inv.Input().GetRegion()),
			durable.WithReason("insufficient-capacity"))
	}
	if !h.cloud.reservations[reservation.GetReservationId()] {
		return nil, errors.New("reservation not yet visible") // transient, retried
	}
	return &machinespb.CreateMachine{MachineId: h.cloud.id("machine")}, nil
}

func reduceProvisionMachine(p *machinespb.ProvisionMachine) *machinespb.ProvisionMachineOutput {
	machine, ok := p.State(machinespb.CreateMachineStep)
	if !ok {
		panic("successful pipeline missing create-machine state")
	}
	host, _ := p.State(machinespb.SelectHostStep)
	return &machinespb.ProvisionMachineOutput{
		MachineId: machine.GetMachineId(),
		HostId:    host.GetHostId(),
	}
}

func newProvisionMachine(c *cloud) *machinespb.ProvisionMachineDefinition {
	return machinespb.NewProvisionMachine(
		validate{},
		&selectHost{cloud: c},
		&reserveCapacity{cloud: c},
		&createMachine{cloud: c},
		reduceProvisionMachine,
	)
}
