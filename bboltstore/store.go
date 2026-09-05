// Package bboltstore implements storedriver.Store on a local bbolt database.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation stores each run as components with distinct write
// cadences (the internal durable.storage.v1 protobuf schema): write-once
// meta (identity + input), step-fact rows written at operation resolution,
// rarely-written failures/terminal/cancel records, and the small cursor
// rewritten per attempt. Per-attempt write volume is therefore independent
// of input and state sizes. An active-slot index enforces at most one
// nonterminal run per (exclusion scope, ResourceID).
package bboltstore

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dangra/durable/storedriver"
	"sort"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dangra/durable"
	"github.com/dangra/durable/internal/storagepb"
)

var (
	metaBucket     = []byte("meta")
	cursorBucket   = []byte("cursor")
	stepsBucket    = []byte("steps")
	failuresBucket = []byte("failures")
	terminalBucket = []byte("terminal")
	cancelBucket   = []byte("cancel")
	slotsBucket    = []byte("slots")
)

// Store is a storedriver.Store backed by a bbolt database file.
type Store struct {
	db *bolt.DB
	// pending counts in-flight ApplyTransition calls for adaptive group
	// commit: a lone caller commits immediately, concurrent callers
	// coalesce into shared transactions.
	pending atomic.Int64
}

// Open opens (creating if needed) the database at path. It fails if another
// process holds the file lock, enforcing exclusive ownership.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout: time.Second,
		// Hashmap freelists stay fast as churn grows (array freelists
		// degrade); a large initial mmap avoids remap stalls, where a
		// write growing the file blocks behind long-running readers on
		// the remap lock. Reserves virtual address space only.
		FreelistType:    bolt.FreelistMapType,
		InitialMmapSize: 1 << 30,
	})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: opening %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{metaBucket, cursorBucket, stepsBucket, failuresBucket, terminalBucket, cancelBucket, slotsBucket} {
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
	db.MaxBatchDelay = 2 * time.Millisecond
	return &Store{db: db}, nil
}

// slotKey joins the slot pair with NUL, which the Store contract
// guarantees appears in no identifier — otherwise distinct
// (group, resource) pairs could alias one key.
func slotKey(rec *storedriver.RunRecord) []byte {
	return []byte(rec.SlotGroup() + "\x00" + string(rec.ResourceID))
}

func stepKey(id durable.RunID, step durable.StepID) []byte {
	return []byte(string(id) + "\x00" + string(step))
}

// groupCommit picks the adaptive commit strategy for one write call: a
// lone caller commits (and fsyncs) immediately via Update, while
// concurrent callers coalesce into shared batch transactions. done must
// be deferred by the caller; it ends the call's participation in the
// concurrency count.
func (s *Store) groupCommit() (commit func(func(*bolt.Tx) error) error, done func()) {
	commit = s.db.Update
	if s.pending.Add(1) > 1 {
		commit = s.db.Batch
	}
	return commit, func() { s.pending.Add(-1) }
}

func (s *Store) CreateRun(_ context.Context, rec *storedriver.RunRecord) (*storedriver.RunRecord, bool, error) {
	var existing *storedriver.RunRecord
	created := false
	commit, done := s.groupCommit()
	defer done()
	err := commit(func(tx *bolt.Tx) error {
		key := slotKey(rec)
		if activeID := tx.Bucket(slotsBucket).Get(key); activeID != nil {
			var err error
			existing, err = getRun(tx, durable.RunID(activeID))
			return err
		}
		if err := putRun(tx, rec); err != nil {
			return err
		}
		if err := tx.Bucket(slotsBucket).Put(key, []byte(rec.RunID)); err != nil {
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

// putRun persists every present component of rec; used at creation (and
// for seeded records carrying pre-existing facts).
func putRun(tx *bolt.Tx, rec *storedriver.RunRecord) error {
	meta, err := storagepb.MarshalRunMeta(rec)
	if err != nil {
		return err
	}
	if err := tx.Bucket(metaBucket).Put([]byte(rec.RunID), meta); err != nil {
		return err
	}
	cursor, err := storagepb.MarshalCursor(storedriver.Cursor{
		Phase:         rec.Phase,
		NextAttemptAt: rec.NextAttemptAt,
		LastError:     rec.LastError,
		LastReason:    rec.LastReason,
		LastErrorAt:   rec.LastErrorAt,
		UpdatedAt:     rec.UpdatedAt,
		AwaitingRunID: rec.AwaitingRunID,
	})
	if err != nil {
		return err
	}
	if err := tx.Bucket(cursorBucket).Put([]byte(rec.RunID), cursor); err != nil {
		return err
	}
	for sid, sr := range rec.Steps {
		b, err := storagepb.MarshalStepRecord(sr)
		if err != nil {
			return err
		}
		if err := tx.Bucket(stepsBucket).Put(stepKey(rec.RunID, sid), b); err != nil {
			return err
		}
	}
	if rec.RootFailure != nil || len(rec.UnwindFailures) > 0 {
		b, err := storagepb.MarshalFailures(rec.RootFailure, rec.UnwindFailures)
		if err != nil {
			return err
		}
		if err := tx.Bucket(failuresBucket).Put([]byte(rec.RunID), b); err != nil {
			return err
		}
	}
	if rec.Outcome != nil {
		b, err := storagepb.MarshalTerminal(*rec.Outcome, rec.Output)
		if err != nil {
			return err
		}
		if err := tx.Bucket(terminalBucket).Put([]byte(rec.RunID), b); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ApplyTransition(_ context.Context, id durable.RunID, t storedriver.Transition) error {
	commit, done := s.groupCommit()
	defer done()
	return commit(func(tx *bolt.Tx) error {
		metaBytes := tx.Bucket(metaBucket).Get([]byte(id))
		if metaBytes == nil {
			return durable.ErrRunNotFound
		}
		cursor, err := storagepb.MarshalCursor(t.Cursor)
		if err != nil {
			return err
		}
		if err := tx.Bucket(cursorBucket).Put([]byte(id), cursor); err != nil {
			return err
		}
		for _, sw := range t.Steps {
			sr := sw.Record
			b, err := storagepb.MarshalStepRecord(&sr)
			if err != nil {
				return err
			}
			if err := tx.Bucket(stepsBucket).Put(stepKey(id, sw.StepID), b); err != nil {
				return err
			}
		}
		if t.RootFailure != nil || t.UnwindFailure != nil {
			root, unwind, err := readFailures(tx, id)
			if err != nil {
				return err
			}
			if t.RootFailure != nil {
				rf := *t.RootFailure
				root = &rf
			}
			if t.UnwindFailure != nil {
				unwind = append(unwind, *t.UnwindFailure)
			}
			b, err := storagepb.MarshalFailures(root, unwind)
			if err != nil {
				return err
			}
			if err := tx.Bucket(failuresBucket).Put([]byte(id), b); err != nil {
				return err
			}
		}
		if t.Outcome != nil {
			b, err := storagepb.MarshalTerminal(*t.Outcome, t.Output)
			if err != nil {
				return err
			}
			if err := tx.Bucket(terminalBucket).Put([]byte(id), b); err != nil {
				return err
			}
			// Terminality releases the resource slot.
			rec := &storedriver.RunRecord{}
			if err := storagepb.UnmarshalRunMetaInto(metaBytes, rec); err != nil {
				return err
			}
			key := slotKey(rec)
			if active := tx.Bucket(slotsBucket).Get(key); active != nil && string(active) == string(id) {
				return tx.Bucket(slotsBucket).Delete(key)
			}
		}
		return nil
	})
}

func readFailures(tx *bolt.Tx, id durable.RunID) (*durable.RootFailure, []durable.UnwindFailure, error) {
	b := tx.Bucket(failuresBucket).Get([]byte(id))
	if b == nil {
		return nil, nil, nil
	}
	return storagepb.UnmarshalFailures(b)
}

// getRun assembles the read model from the run's components: meta, step
// rows, failures, terminal — with the cursor's in-flight operation
// overlaid as an unresolved step entry.
func getRun(tx *bolt.Tx, id durable.RunID) (*storedriver.RunRecord, error) {
	metaBytes := tx.Bucket(metaBucket).Get([]byte(id))
	if metaBytes == nil {
		return nil, durable.ErrRunNotFound
	}
	rec := &storedriver.RunRecord{}
	if err := storagepb.UnmarshalRunMetaInto(metaBytes, rec); err != nil {
		return nil, err
	}

	prefix := stepKey(id, "")
	c := tx.Bucket(stepsBucket).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		sr, err := storagepb.UnmarshalStepRecord(v)
		if err != nil {
			return nil, err
		}
		*rec.Step(durable.StepID(k[len(prefix):])) = *sr
	}

	cb := tx.Bucket(cursorBucket).Get([]byte(id))
	if cb == nil {
		return nil, fmt.Errorf("bboltstore: run %s has no cursor", id)
	}
	cur, err := storagepb.UnmarshalCursor(cb)
	if err != nil {
		return nil, err
	}
	rec.Phase = cur.Phase
	rec.NextAttemptAt = cur.NextAttemptAt
	rec.LastError, rec.LastReason, rec.LastErrorAt = cur.LastError, cur.LastReason, cur.LastErrorAt
	rec.AwaitingRunID = cur.AwaitingRunID
	rec.UpdatedAt = cur.UpdatedAt
	if cur.StepID != "" {
		sr := rec.Step(cur.StepID)
		if cur.Phase == durable.PhaseUnwind && sr.ForwardStatus == storedriver.OpSucceeded {
			sr.UnwindStatus = storedriver.OpUnresolved
			sr.UnwindAttempts = cur.Attempts
		} else {
			sr.ForwardStatus = storedriver.OpUnresolved
			sr.ForwardAttempts = cur.Attempts
		}
	}

	root, unwind, err := readFailures(tx, id)
	if err != nil {
		return nil, err
	}
	rec.RootFailure = root
	rec.UnwindFailures = unwind

	if tb := tx.Bucket(terminalBucket).Get([]byte(id)); tb != nil {
		oc, out, err := storagepb.UnmarshalTerminal(tb)
		if err != nil {
			return nil, err
		}
		rec.Outcome = &oc
		rec.Output = out
	}
	if xb := tx.Bucket(cancelBucket).Get([]byte(id)); xb != nil {
		cr, err := storagepb.UnmarshalCancel(xb)
		if err != nil {
			return nil, err
		}
		rec.Cancel = cr
	}
	return rec, nil
}

func (s *Store) GetRun(_ context.Context, id durable.RunID) (*storedriver.RunRecord, error) {
	var rec *storedriver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		rec, err = getRun(tx, id)
		return err
	})
	return rec, err
}

func (s *Store) ReapTerminal(_ context.Context, before time.Time, limit int) (int, error) {
	deleted := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		// The terminal bucket's keys are exactly the terminal run ids;
		// terminality time is the cursor's UpdatedAt, stamped by the
		// terminal transition.
		var victims [][]byte
		c := tx.Bucket(terminalBucket).Cursor()
		for k, _ := c.First(); k != nil && len(victims) < limit; k, _ = c.Next() {
			cb := tx.Bucket(cursorBucket).Get(k)
			if cb == nil {
				continue
			}
			cur, err := storagepb.UnmarshalCursor(cb)
			if err != nil {
				return err
			}
			if cur.UpdatedAt.Before(before) {
				victims = append(victims, bytes.Clone(k))
			}
		}
		for _, id := range victims {
			prefix := stepKey(durable.RunID(id), "")
			sc := tx.Bucket(stepsBucket).Cursor()
			var stepKeys [][]byte
			for k, _ := sc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = sc.Next() {
				stepKeys = append(stepKeys, bytes.Clone(k))
			}
			for _, k := range stepKeys {
				if err := tx.Bucket(stepsBucket).Delete(k); err != nil {
					return err
				}
			}
			for _, bucket := range [][]byte{failuresBucket, cancelBucket, cursorBucket, terminalBucket, metaBucket} {
				if err := tx.Bucket(bucket).Delete(id); err != nil {
					return err
				}
			}
			deleted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *Store) RequestCancel(_ context.Context, id durable.RunID, req storedriver.CancelRequest) (bool, error) {
	accepted := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		switch {
		case tx.Bucket(metaBucket).Get([]byte(id)) == nil:
			return durable.ErrRunNotFound
		case tx.Bucket(terminalBucket).Get([]byte(id)) != nil:
			return durable.ErrRunTerminal
		case tx.Bucket(cancelBucket).Get([]byte(id)) != nil:
			return nil // first cancel wins
		}
		b, err := storagepb.MarshalCancel(&req)
		if err != nil {
			return err
		}
		if err := tx.Bucket(cancelBucket).Put([]byte(id), b); err != nil {
			return err
		}
		accepted = true
		return nil
	})
	return accepted, err
}

func (s *Store) ListNonterminal(_ context.Context) ([]*storedriver.RunRecord, error) {
	var out []*storedriver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).ForEach(func(k, _ []byte) error {
			if tx.Bucket(terminalBucket).Get(k) != nil {
				return nil
			}
			rec, err := getRun(tx, durable.RunID(k))
			if err != nil {
				return err
			}
			out = append(out, rec)
			return nil
		})
	})
	return out, err
}

func (s *Store) ListRuns(_ context.Context, pipeline durable.PipelineID, resource durable.ResourceID) ([]*storedriver.RunRecord, error) {
	var out []*storedriver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).ForEach(func(k, v []byte) error {
			probe := &storedriver.RunRecord{}
			if err := storagepb.UnmarshalRunMetaInto(v, probe); err != nil {
				return err
			}
			if probe.PipelineID != pipeline || probe.ResourceID != resource {
				return nil
			}
			rec, err := getRun(tx, durable.RunID(k))
			if err != nil {
				return err
			}
			out = append(out, rec)
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

// StoreStats are cumulative write-side counters for performance
// measurement, expressed without exposing the underlying bbolt types.
type StoreStats struct {
	// TxPageAllocBytes is the total bytes of pages allocated by write
	// transactions — the write-amplification measure.
	TxPageAllocBytes int64
	// TxWrites is the number of write operations performed.
	TxWrites int64
}

// Stats returns cumulative counters since Open.
func (s *Store) Stats() StoreStats {
	st := s.db.Stats()
	return StoreStats{
		TxPageAllocBytes: st.TxStats.GetPageAlloc(),
		TxWrites:         st.TxStats.GetWrite(),
	}
}
