// Package durabletest provides test doubles for the durable runtime: an
// in-memory Store, a deterministic fake Clock, and a fake Invocation
// (NewInvocation) for unit-testing handlers without an engine.
package durabletest

import (
	"context"
	"github.com/dangra/durable/storedriver"
	"sync"
	"time"

	"github.com/dangra/durable/kernel"
)

// MemStore is an in-memory storedriver.Store for tests. It is safe for
// concurrent use and returns defensive copies of all records.
type MemStore struct {
	mu   sync.Mutex
	runs map[kernel.RunID]*storedriver.RunRecord
}

// NewMemStore constructs an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{runs: make(map[kernel.RunID]*storedriver.RunRecord)}
}

func (s *MemStore) CreateRun(_ context.Context, rec *storedriver.RunRecord) (*storedriver.RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.runs {
		if r.SlotGroup() == rec.SlotGroup() && r.ResourceID == rec.ResourceID && !r.Terminal() {
			return r.Clone(), false, nil
		}
	}
	s.runs[rec.RunID] = rec.Clone()
	return nil, true, nil
}

func (s *MemStore) GetRun(_ context.Context, id kernel.RunID) (*storedriver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, kernel.ErrRunNotFound
	}
	return r.Clone(), nil
}

func (s *MemStore) ApplyTransition(_ context.Context, id kernel.RunID, t storedriver.Transition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.runs[id]
	if !ok {
		return kernel.ErrRunNotFound
	}

	// Step fact rows.
	for _, sw := range t.Steps {
		sr := sw.Record
		sr.State = append([]byte(nil), sw.Record.State...)
		*rec.Step(sw.StepID) = sr
	}

	// Cursor: scheduling state plus the single in-flight operation. The
	// engine's delta contract covers every previously unresolved
	// operation with either the cursor or an explicit step write, so the
	// overlay below is a pure upsert.
	c := t.Cursor
	rec.Phase = c.Phase
	rec.NextAttemptAt = c.NextAttemptAt
	rec.LastError, rec.LastReason, rec.LastErrorAt = c.LastError, c.LastReason, c.LastErrorAt
	rec.Awaiting = c.Awaiting.Clone()
	rec.Awaited = c.Awaited.Clone()
	rec.UpdatedAt = c.UpdatedAt
	if c.StepID != "" {
		sr := rec.Step(c.StepID)
		if c.Phase == kernel.PhaseUnwind && sr.ForwardStatus == storedriver.OpSucceeded {
			sr.UnwindStatus = storedriver.OpUnresolved
			sr.UnwindAttempts = c.Attempts
		} else {
			sr.ForwardStatus = storedriver.OpUnresolved
			sr.ForwardAttempts = c.Attempts
		}
	}

	if t.RootFailure != nil {
		rf := *t.RootFailure
		rec.RootFailure = &rf
	}
	if t.UnwindFailure != nil {
		rec.UnwindFailures = append(rec.UnwindFailures, *t.UnwindFailure)
	}
	if t.Outcome != nil {
		oc := *t.Outcome
		rec.Outcome = &oc
		rec.Output = append([]byte(nil), t.Output...)
	}
	return nil
}

func (s *MemStore) ReapTerminal(_ context.Context, before time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for id, rec := range s.runs {
		if deleted >= limit {
			break
		}
		if rec.Terminal() && rec.UpdatedAt.Before(before) {
			delete(s.runs, id)
			deleted++
		}
	}
	return deleted, nil
}

func (s *MemStore) RequestCancel(_ context.Context, id kernel.RunID, req storedriver.CancelRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.runs[id]
	switch {
	case !ok:
		return false, kernel.ErrRunNotFound
	case rec.Terminal():
		return false, kernel.ErrRunTerminal
	case rec.Cancel != nil:
		return false, nil
	}
	rec.Cancel = &req
	return true, nil
}

func (s *MemStore) ListNonterminal(_ context.Context) ([]*storedriver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*storedriver.RunRecord
	for _, r := range s.runs {
		if !r.Terminal() {
			out = append(out, r.Clone())
		}
	}
	return out, nil
}

func (s *MemStore) ListRuns(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*storedriver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*storedriver.RunRecord
	for _, r := range s.runs {
		if r.PipelineID == pipeline && r.ResourceID == resource {
			out = append(out, r.Clone())
		}
	}
	sortRecords(out)
	return out, nil
}

func (s *MemStore) Close() error { return nil }

func sortRecords(recs []*storedriver.RunRecord) {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].CreatedAt.Before(recs[j-1].CreatedAt); j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}
