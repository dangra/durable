// Package mem is the in-memory durable store: a real driver.Store whose
// runs live only as long as the process. It suits CLI tools and other
// programs whose runs are ephemeral but still want pipeline semantics —
// exclusion slots, retries, unwind, cancellation, parks — and it is the
// executable reference for the store contract, which the persistent
// drivers are checked against by differential fuzzing. It registers the
// "mem" scheme with package store.
package mem

import (
	"context"
	"github.com/dangra/durable/store/driver"
	"sync"
	"time"

	"github.com/dangra/durable/kernel"
)

// Store is the in-memory driver.Store. It is safe for
// concurrent use and returns defensive copies of all records.
type Store struct {
	mu   sync.Mutex
	runs map[kernel.RunID]*driver.RunRecord
}

// New constructs an empty Store.
func New() *Store {
	return &Store{runs: make(map[kernel.RunID]*driver.RunRecord)}
}

func (s *Store) CreateRun(_ context.Context, rec *driver.RunRecord) (*driver.RunRecord, bool, error) {
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

func (s *Store) GetRun(_ context.Context, id kernel.RunID) (*driver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, kernel.ErrRunNotFound
	}
	return r.Clone(), nil
}

func (s *Store) ApplyTransition(_ context.Context, id kernel.RunID, t driver.Transition) error {
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
		if c.Phase == kernel.PhaseUnwind && sr.ForwardStatus == driver.OpSucceeded {
			sr.UnwindStatus = driver.OpUnresolved
			sr.UnwindAttempts = c.Attempts
		} else {
			sr.ForwardStatus = driver.OpUnresolved
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

func (s *Store) ReapTerminal(_ context.Context, before time.Time, limit int) (int, error) {
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

func (s *Store) RequestCancel(_ context.Context, id kernel.RunID, req driver.CancelRequest) (bool, error) {
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

func (s *Store) ListNonterminal(_ context.Context) ([]*driver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*driver.RunRecord
	for _, r := range s.runs {
		if !r.Terminal() {
			out = append(out, r.Clone())
		}
	}
	return out, nil
}

func (s *Store) ListRuns(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*driver.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*driver.RunRecord
	for _, r := range s.runs {
		if r.PipelineID == pipeline && r.ResourceID == resource {
			out = append(out, r.Clone())
		}
	}
	sortRecords(out)
	return out, nil
}

func (s *Store) Close() error { return nil }

func sortRecords(recs []*driver.RunRecord) {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].CreatedAt.Before(recs[j-1].CreatedAt); j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}
