// Package bboltstore implements durable.Store on a local bbolt database.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation encodes RunRecords as JSON in a "runs" bucket and keeps an
// active-slot index in a "slots" bucket to enforce at most one nonterminal
// Run per (PipelineID, ResourceID).
package bboltstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dangra/durable"
)

var (
	runsBucket  = []byte("runs")
	slotsBucket = []byte("slots")
)

// Store is a durable.Store backed by a bbolt database file.
type Store struct {
	db *bolt.DB
}

// Open opens (creating if needed) the database at path. It fails if another
// process holds the file lock, enforcing exclusive ownership.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: opening %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{runsBucket, slotsBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("bboltstore: initializing buckets: %w", err)
	}
	return &Store{db: db}, nil
}

func slotKey(pipeline durable.PipelineID, resource durable.ResourceID) []byte {
	return []byte(string(pipeline) + "\x00" + string(resource))
}

func decode(b []byte) (*durable.RunRecord, error) {
	rec := &durable.RunRecord{}
	if err := json.Unmarshal(b, rec); err != nil {
		return nil, fmt.Errorf("bboltstore: decoding run record: %w", err)
	}
	return rec, nil
}

func (s *Store) CreateRun(_ context.Context, rec *durable.RunRecord) (*durable.RunRecord, bool, error) {
	var existing *durable.RunRecord
	created := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		slots := tx.Bucket(slotsBucket)
		runs := tx.Bucket(runsBucket)
		key := slotKey(rec.PipelineID, rec.ResourceID)
		if activeID := slots.Get(key); activeID != nil {
			b := runs.Get(activeID)
			if b == nil {
				return fmt.Errorf("bboltstore: slot index references missing run %s", activeID)
			}
			var err error
			existing, err = decode(b)
			return err
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("bboltstore: encoding run record: %w", err)
		}
		if err := runs.Put([]byte(rec.RunID), b); err != nil {
			return err
		}
		if err := slots.Put(key, []byte(rec.RunID)); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return existing, created, nil
}

func (s *Store) GetRun(_ context.Context, id durable.RunID) (*durable.RunRecord, error) {
	var rec *durable.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(runsBucket).Get([]byte(id))
		if b == nil {
			return durable.ErrRunNotFound
		}
		var err error
		rec, err = decode(b)
		return err
	})
	return rec, err
}

func (s *Store) UpdateRun(_ context.Context, rec *durable.RunRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket(runsBucket)
		if runs.Get([]byte(rec.RunID)) == nil {
			return durable.ErrRunNotFound
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("bboltstore: encoding run record: %w", err)
		}
		if err := runs.Put([]byte(rec.RunID), b); err != nil {
			return err
		}
		if rec.Terminal() {
			slots := tx.Bucket(slotsBucket)
			key := slotKey(rec.PipelineID, rec.ResourceID)
			if active := slots.Get(key); active != nil && string(active) == string(rec.RunID) {
				return slots.Delete(key)
			}
		}
		return nil
	})
}

func (s *Store) ListNonterminal(_ context.Context) ([]*durable.RunRecord, error) {
	var out []*durable.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(runsBucket).ForEach(func(_, v []byte) error {
			rec, err := decode(v)
			if err != nil {
				return err
			}
			if !rec.Terminal() {
				out = append(out, rec)
			}
			return nil
		})
	})
	return out, err
}

func (s *Store) ListRuns(_ context.Context, pipeline durable.PipelineID, resource durable.ResourceID) ([]*durable.RunRecord, error) {
	var out []*durable.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(runsBucket).ForEach(func(_, v []byte) error {
			rec, err := decode(v)
			if err != nil {
				return err
			}
			if rec.PipelineID == pipeline && rec.ResourceID == resource {
				out = append(out, rec)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) Close() error { return s.db.Close() }
