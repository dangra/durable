package durabletest

import (
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/dangra/durable"
)

// InvocationConfig describes the attempt a fake Invocation presents to a
// handler under test. Zero values are sensible: Phase defaults to
// PhaseForward and Attempt to 1; identities stay empty unless set.
type InvocationConfig struct {
	PipelineID durable.PipelineID
	ResourceID durable.ResourceID
	RunID      durable.RunID
	StepID     durable.StepID
	Attempt    uint64
	Phase      durable.Phase

	// Input is the Pipeline Input; nil for an Input-less Pipeline.
	Input proto.Message

	// State holds the committed Step States by StepID, as the handler
	// would read them through its typed State method. Key it with the
	// generated step reference's ID().
	State map[durable.StepID]proto.Message

	Annotations     map[string]string
	CancelRequested bool

	// Awaited is the resolved memory of an earlier park; nil for a first
	// execution.
	Awaited *durable.Wake

	// Logger backs Invocation.Logger; nil discards.
	Logger *slog.Logger
}

// Invocation is a fake durable.Invocation (and durable.ReduceView) for
// handler and reducer unit tests: it needs no engine and no store, and it
// records the contract violations a real engine would invalidate the Run
// for, readable through Violation.
type Invocation struct {
	cfg    InvocationConfig
	states map[durable.StepID][]byte

	mu        sync.Mutex
	violation error
}

var (
	_ durable.Invocation = (*Invocation)(nil)
	_ durable.ReduceView = (*Invocation)(nil)
)

// NewInvocation builds a fake Invocation from cfg. It panics if a State
// message cannot be marshaled, which indicates a broken test fixture.
func NewInvocation(cfg InvocationConfig) *Invocation {
	if cfg.Phase == 0 {
		cfg.Phase = durable.PhaseForward
	}
	if cfg.Attempt == 0 {
		cfg.Attempt = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	states := make(map[durable.StepID][]byte, len(cfg.State))
	for id, msg := range cfg.State {
		b, err := proto.Marshal(msg)
		if err != nil {
			panic("durabletest: cannot marshal state for step " + string(id) + ": " + err.Error())
		}
		states[id] = b
	}
	return &Invocation{cfg: cfg, states: states}
}

func (inv *Invocation) PipelineID() durable.PipelineID { return inv.cfg.PipelineID }
func (inv *Invocation) ResourceID() durable.ResourceID { return inv.cfg.ResourceID }
func (inv *Invocation) RunID() durable.RunID           { return inv.cfg.RunID }
func (inv *Invocation) StepID() durable.StepID         { return inv.cfg.StepID }
func (inv *Invocation) Attempt() uint64                { return inv.cfg.Attempt }
func (inv *Invocation) Phase() durable.Phase           { return inv.cfg.Phase }
func (inv *Invocation) CancelRequested() bool          { return inv.cfg.CancelRequested }

// InputMessage returns a copy of the configured Input, or nil.
func (inv *Invocation) InputMessage() proto.Message {
	if inv.cfg.Input == nil {
		return nil
	}
	return proto.Clone(inv.cfg.Input)
}

// Annotations returns a copy of the configured annotations, nil when
// empty.
func (inv *Invocation) Annotations() map[string]string {
	if len(inv.cfg.Annotations) == 0 {
		return nil
	}
	out := make(map[string]string, len(inv.cfg.Annotations))
	for k, v := range inv.cfg.Annotations {
		out[k] = v
	}
	return out
}

// Awaited returns a copy of the configured park memory.
func (inv *Invocation) Awaited() (durable.Wake, bool) {
	if inv.cfg.Awaited == nil {
		return durable.Wake{}, false
	}
	return *inv.cfg.Awaited.Clone(), true
}

// AwaitedRunID reports the configured park memory when it has exactly one
// target, matching the engine's rule.
func (inv *Invocation) AwaitedRunID() (durable.RunID, bool) {
	if inv.cfg.Awaited == nil || len(inv.cfg.Awaited.Targets) != 1 {
		return "", false
	}
	return inv.cfg.Awaited.Targets[0], true
}

// Logger returns the configured logger with the canonical keys attached,
// as the engine's would carry.
func (inv *Invocation) Logger() *slog.Logger {
	return inv.cfg.Logger.With(
		"pipeline", string(inv.cfg.PipelineID),
		"resource", string(inv.cfg.ResourceID),
		"run", string(inv.cfg.RunID),
		"step", string(inv.cfg.StepID),
		"phase", inv.cfg.Phase.String(),
		"attempt", inv.cfg.Attempt,
	)
}

// StateBytes implements durable.StateSource over the configured State.
func (inv *Invocation) StateBytes(id durable.StepID) ([]byte, bool) {
	b, ok := inv.states[id]
	return b, ok
}

// ReportViolation implements durable.StateSource; the first report wins.
func (inv *Invocation) ReportViolation(err error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.violation == nil {
		inv.violation = err
	}
}

// Violation returns the first contract violation reported during the
// test — a State or Input that could not be decoded — or nil. A real
// engine would invalidate the Run for it once the attempt returned.
func (inv *Invocation) Violation() error {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.violation
}
