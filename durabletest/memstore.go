// Package durabletest provides test doubles for the durable runtime: an
// in-memory Store and a deterministic fake Clock.
package durabletest

import (
	"context"
	"sync"

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
		if r.PipelineID == rec.PipelineID && r.ResourceID == rec.ResourceID && !r.Terminal() {
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

func (s *MemStore) UpdateRun(_ context.Context, rec *durable.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[rec.RunID]; !ok {
		return durable.ErrRunNotFound
	}
	s.runs[rec.RunID] = rec.Clone()
	return nil
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
