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
	released     []string        // machines released by decommission

	// createGate, when non-nil, holds machine creation until closed
	// (used by tests to keep a provision run in flight).
	createGate chan struct{}
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

// handlers implements machinespb.ProvisionMachineHandlers: one method
// per step, an Unwind method for the step that compensates, and the
// reducer. Every step shares the fake cloud through it.
type handlers struct{ cloud *cloud }

func (h *handlers) Validate(ctx context.Context, inv machinespb.ProvisionMachineInvocation) error {
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

func (h *handlers) SelectHost(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.SelectHost, error) {
	return &machinespb.SelectHost{
		HostId: "host-" + inv.Input().GetRegion() + "-1",
	}, nil
}

func (h *handlers) ReserveCapacity(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.ReserveCapacity, error) {
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

func (h *handlers) UnwindReserveCapacity(ctx context.Context, inv machinespb.ProvisionMachineInvocation) error {
	reservation, ok := inv.State(machinespb.ReserveCapacityStep)
	if !ok {
		return nil
	}
	h.cloud.mu.Lock()
	defer h.cloud.mu.Unlock()
	delete(h.cloud.reservations, reservation.GetReservationId())
	return nil
}

func (h *handlers) CreateMachine(ctx context.Context, inv machinespb.ProvisionMachineInvocation) (*machinespb.CreateMachine, error) {
	reservation, ok := inv.State(machinespb.ReserveCapacityStep)
	if !ok {
		return nil, durable.Fail(errors.New("reservation state unavailable"))
	}
	h.cloud.mu.Lock()
	gate := h.cloud.createGate
	h.cloud.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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

func (h *handlers) Reduce(p *machinespb.ProvisionMachine) *machinespb.ProvisionMachineOutput {
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
	return machinespb.NewProvisionMachine(&handlers{cloud: c})
}

// decommission implements machinespb.DecommissionMachineHandlers.
type decommission struct{ cloud *cloud }

func (h *decommission) ReleaseMachine(ctx context.Context, inv machinespb.DecommissionMachineInvocation) error {
	h.cloud.mu.Lock()
	defer h.cloud.mu.Unlock()
	h.cloud.released = append(h.cloud.released, string(inv.ResourceID()))
	return nil
}

func newDecommissionMachine(c *cloud) *machinespb.DecommissionMachineDefinition {
	return machinespb.NewDecommissionMachine(&decommission{cloud: c})
}
