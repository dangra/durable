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
	// slots indexes the nonterminal Run per (pipeline, resource): the
	// structure CreateRun enforces exclusion with and GetActiveRunID
	// answers from. Released when a Run commits its Outcome.
	slots map[string]kernel.RunID
}

// New constructs an empty Store.
func New() *Store {
	return &Store{runs: make(map[kernel.RunID]*driver.RunRecord), slots: make(map[string]kernel.RunID)}
}

func slotKey(pipeline kernel.PipelineID, resource kernel.ResourceID) string {
	return string(pipeline) + "\x00" + string(resource)
}

func (s *Store) CreateRun(_ context.Context, rec *driver.RunRecord, excluding []kernel.PipelineID) (*driver.RunRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active, ok := s.slots[slotKey(rec.PipelineID, rec.ResourceID)]; ok {
		return s.runs[active].Clone(), false, nil
	}
	for _, p := range excluding {
		if active, ok := s.slots[slotKey(p, rec.ResourceID)]; ok {
			return s.runs[active].Clone(), false, nil
		}
	}
	c := rec.Clone()
	if c.Terminal() {
		// A record seeded terminal is stored in its terminal stage.
		c.CompactTerminal()
	}
	s.runs[rec.RunID] = c
	s.slots[slotKey(rec.PipelineID, rec.ResourceID)] = rec.RunID
	return nil, true, nil
}

func (s *Store) GetActiveRunID(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) (kernel.RunID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.slots[slotKey(pipeline, resource)]
	return id, ok, nil
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
	switch {
	case !ok:
		return kernel.ErrRunNotFound
	case rec.Terminal():
		return kernel.ErrRunTerminal
	}

	// Operation rows: each write replaces one half of a step's record.
	for _, ow := range t.Ops {
		op := ow.Record
		op.State = append([]byte(nil), ow.Record.State...)
		if ow.Record.Failure != nil {
			f := *ow.Record.Failure
			op.Failure = &f
		}
		*rec.Step(ow.StepID).Op(ow.Phase) = op
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
		if c.Phase == kernel.PhaseUnwind && sr.Forward.Status == driver.OpSucceeded {
			sr.Unwind.Status = driver.OpUnresolved
			sr.Unwind.Attempts = c.Attempts
		} else {
			sr.Forward.Status = driver.OpUnresolved
			sr.Forward.Attempts = c.Attempts
		}
	}

	if t.Failure != nil {
		rf := *t.Failure
		rec.Failure = &rf
	}
	if t.Outcome != nil {
		oc := *t.Outcome
		rec.Outcome = &oc
		rec.Output = append([]byte(nil), t.Output...)
		// Terminality releases the resource slot and the nonterminal
		// stage.
		if key := slotKey(rec.PipelineID, rec.ResourceID); s.slots[key] == id {
			delete(s.slots, key)
		}
		rec.CompactTerminal()
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

// Runs returns a copy of every record, terminal and nonterminal, in
// CreatedAt order. It is an enumeration for tests and tools — the store
// contract has no listing, since a persistent store could only answer
// one by scanning — and this store, holding a map, answers it cheaply.
func (s *Store) Runs() []*driver.RunRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*driver.RunRecord, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, r.Clone())
	}
	sortRecords(out)
	return out
}

func (s *Store) Close() error { return nil }

func sortRecords(recs []*driver.RunRecord) {
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].CreatedAt.Before(recs[j-1].CreatedAt); j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}
