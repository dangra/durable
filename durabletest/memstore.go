// Package durabletest provides test doubles for the durable runtime: an
// in-memory Store and a deterministic fake Clock.
package durabletest

import (
	"context"
	"sync"
	"time"

	"github.com/dangra/durable"
)

// MemStore is an in-memory durable.Store for tests. It is safe for
// concurrent use and returns defensive copies of all records.
type MemStore struct {
	mu   sync.Mutex
	runs map[durable.RunID]*durable.RunRecord
}

// NewMemStore constructs an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{runs: make(map[durable.RunID]*durable.RunRecord)}
}

func (s *MemStore) CreateRun(_ context.Context, rec *durable.RunRecord) (*durable.RunRecord, bool, error) {
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

func (s *MemStore) GetRun(_ context.Context, id durable.RunID) (*durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, durable.ErrRunNotFound
	}
	return r.Clone(), nil
}

func (s *MemStore) ApplyTransition(_ context.Context, id durable.RunID, t durable.Transition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.runs[id]
	if !ok {
		return durable.ErrRunNotFound
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
	rec.UpdatedAt = c.UpdatedAt
	if c.StepID != "" {
		sr := rec.Step(c.StepID)
		if c.Phase == durable.PhaseUnwind && sr.ForwardStatus == durable.OpSucceeded {
			sr.UnwindStatus = durable.OpUnresolved
			sr.UnwindAttempts = c.Attempts
		} else {
			sr.ForwardStatus = durable.OpUnresolved
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

func (s *MemStore) RequestCancel(_ context.Context, id durable.RunID, req durable.CancelRequest) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.runs[id]
	switch {
	case !ok:
		return false, durable.ErrRunNotFound
	case rec.Terminal():
		return false, durable.ErrRunTerminal
	case rec.Cancel != nil:
		return false, nil
	}
	rec.Cancel = &req
	return true, nil
}

func (s *MemStore) ListNonterminal(_ context.Context) ([]*durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*durable.RunRecord
	for _, r := range s.runs {
		if !r.Terminal() {
			out = append(out, r.Clone())
		}
	}
	return out, nil
}

func (s *MemStore) ListRuns(_ context.Context, pipeline durable.PipelineID, resource durable.ResourceID) ([]*durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*durable.RunRecord
	for _, r := range s.runs {
		if r.PipelineID == pipeline && r.ResourceID == resource {
			out = append(out, r.Clone())
		}
	}
	sortRecords(out)
	return out, nil
}

func (s *MemStore) Close() error { return nil }

func sortRecords(recs []*durable.RunRecord) {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].CreatedAt.Before(recs[j-1].CreatedAt); j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}
