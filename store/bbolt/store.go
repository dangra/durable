// Package bbolt implements driver.Store on a local bbolt database and
// registers the "bbolt" scheme with package store. Importing it alongside
// go.etcd.io/bbolt needs an alias for one of the two.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation stores each run as components with distinct write
// cadences (the internal durable.storage.v1 protobuf schema): write-once
// meta (identity + input), step-fact rows written at operation resolution,
// a root failure row and one append-only row per permanent unwind
// failure, write-once terminal and cancel records, and the small cursor
// rewritten per attempt. Per-attempt write volume is therefore independent
// of input and state sizes, and no row is ever read back to be
// rewritten. An active-slot index keyed by (PipelineID, ResourceID) holds
// every nonterminal run: CreateRun admits against it, GetActiveRunID
// reads it, and ListNonterminal walks it, so recovery cost follows the
// runs in flight rather than the retained history.
package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/dangra/durable/store/driver"
	"sort"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/internal/storagepb"
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

// Store is a driver.Store backed by a bbolt database file.
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
		return nil, fmt.Errorf("bbolt: opening %s: %w", path, err)
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
		return nil, fmt.Errorf("bbolt: initializing buckets: %w", err)
	}
	db.MaxBatchDelay = 2 * time.Millisecond
	return &Store{db: db}, nil
}

// slotKey joins the slot pair with NUL, which the Store contract
// guarantees appears in no identifier — otherwise distinct
// (group, resource) pairs could alias one key.
func slotKey(rec *driver.RunRecord) []byte {
	return slotKeyFor(rec.PipelineID, rec.ResourceID)
}

func slotKeyFor(pipeline kernel.PipelineID, resource kernel.ResourceID) []byte {
	return []byte(string(pipeline) + "\x00" + string(resource))
}

func stepKey(id kernel.RunID, step kernel.StepID) []byte {
	return []byte(string(id) + "\x00" + string(step))
}

// unwindFailureKey addresses the n-th permanent unwind failure of a run
// (0-based, in unwind execution order). The fixed-width big-endian
// ordinal keeps byte order equal to execution order, so a prefix scan
// reads them back in sequence. The run's root failure lives under the
// bare run id in the same bucket; the NUL keeps the two apart.
func unwindFailureKey(id kernel.RunID, n uint64) []byte {
	k := make([]byte, 0, len(id)+1+8)
	k = append(k, id...)
	k = append(k, 0)
	return binary.BigEndian.AppendUint64(k, n)
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

func (s *Store) CreateRun(_ context.Context, rec *driver.RunRecord, excluding []kernel.PipelineID) (*driver.RunRecord, bool, error) {
	var existing *driver.RunRecord
	created := false
	commit, done := s.groupCommit()
	defer done()
	err := commit(func(tx *bolt.Tx) error {
		slots := tx.Bucket(slotsBucket)
		// rec's own slot first, then every group member's: the first
		// occupant found is the blocker reported to the caller.
		if activeID := slots.Get(slotKey(rec)); activeID != nil {
			var err error
			existing, err = getRun(tx, kernel.RunID(activeID))
			return err
		}
		for _, p := range excluding {
			if p == rec.PipelineID {
				continue
			}
			if activeID := slots.Get(slotKeyFor(p, rec.ResourceID)); activeID != nil {
				var err error
				existing, err = getRun(tx, kernel.RunID(activeID))
				return err
			}
		}
		if err := putRun(tx, rec); err != nil {
			return err
		}
		if err := slots.Put(slotKey(rec), []byte(rec.RunID)); err != nil {
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
func putRun(tx *bolt.Tx, rec *driver.RunRecord) error {
	meta, err := storagepb.MarshalRunMeta(rec)
	if err != nil {
		return err
	}
	if err := tx.Bucket(metaBucket).Put([]byte(rec.RunID), meta); err != nil {
		return err
	}
	cursor, err := storagepb.MarshalCursor(driver.Cursor{
		Phase:         rec.Phase,
		NextAttemptAt: rec.NextAttemptAt,
		LastError:     rec.LastError,
		LastReason:    rec.LastReason,
		LastErrorAt:   rec.LastErrorAt,
		UpdatedAt:     rec.UpdatedAt,
		Awaiting:      rec.Awaiting,
		Awaited:       rec.Awaited,
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
	if rec.RootFailure != nil {
		if err := putRootFailure(tx, rec.RunID, rec.RootFailure); err != nil {
			return err
		}
	}
	for i, uf := range rec.UnwindFailures {
		if err := putUnwindFailure(tx, rec.RunID, uint64(i), uf); err != nil {
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

func (s *Store) ApplyTransition(_ context.Context, id kernel.RunID, t driver.Transition) error {
	commit, done := s.groupCommit()
	defer done()
	return commit(func(tx *bolt.Tx) error {
		metaBytes := tx.Bucket(metaBucket).Get([]byte(id))
		if metaBytes == nil {
			return kernel.ErrRunNotFound
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
		if t.RootFailure != nil {
			if err := putRootFailure(tx, id, t.RootFailure); err != nil {
				return err
			}
		}
		if t.UnwindFailure != nil {
			// Append-only: the next ordinal is the count of rows already
			// there, found by a key walk with no decode. Nothing is
			// re-read or rewritten.
			if err := putUnwindFailure(tx, id, countUnwindFailures(tx, id), *t.UnwindFailure); err != nil {
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
			rec := &driver.RunRecord{}
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

func putRootFailure(tx *bolt.Tx, id kernel.RunID, rf *kernel.RootFailure) error {
	b, err := storagepb.MarshalFailureRecord(rf.FailureRecord)
	if err != nil {
		return err
	}
	return tx.Bucket(failuresBucket).Put([]byte(id), b)
}

func putUnwindFailure(tx *bolt.Tx, id kernel.RunID, n uint64, uf kernel.UnwindFailure) error {
	b, err := storagepb.MarshalFailureRecord(uf.FailureRecord)
	if err != nil {
		return err
	}
	return tx.Bucket(failuresBucket).Put(unwindFailureKey(id, n), b)
}

// countUnwindFailures walks the run's unwind failure keys without
// decoding a value.
func countUnwindFailures(tx *bolt.Tx, id kernel.RunID) uint64 {
	prefix := unwindFailureKey(id, 0)[:len(id)+1]
	c := tx.Bucket(failuresBucket).Cursor()
	var n uint64
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		n++
	}
	return n
}

// readFailures assembles the root failure from its own row and the
// unwind failures from their ordinal rows, in execution order.
func readFailures(tx *bolt.Tx, id kernel.RunID) (*kernel.RootFailure, []kernel.UnwindFailure, error) {
	fb := tx.Bucket(failuresBucket)
	var root *kernel.RootFailure
	if b := fb.Get([]byte(id)); b != nil {
		f, err := storagepb.UnmarshalFailureRecord(b)
		if err != nil {
			return nil, nil, err
		}
		root = &kernel.RootFailure{FailureRecord: f}
	}
	var unwind []kernel.UnwindFailure
	prefix := unwindFailureKey(id, 0)[:len(id)+1]
	c := fb.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		f, err := storagepb.UnmarshalFailureRecord(v)
		if err != nil {
			return nil, nil, err
		}
		unwind = append(unwind, kernel.UnwindFailure{FailureRecord: f})
	}
	return root, unwind, nil
}

// getRun assembles the read model from the run's components: meta, step
// rows, failures, terminal — with the cursor's in-flight operation
// overlaid as an unresolved step entry.
func getRun(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	metaBytes := tx.Bucket(metaBucket).Get([]byte(id))
	if metaBytes == nil {
		return nil, kernel.ErrRunNotFound
	}
	rec := &driver.RunRecord{}
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
		*rec.Step(kernel.StepID(k[len(prefix):])) = *sr
	}

	cb := tx.Bucket(cursorBucket).Get([]byte(id))
	if cb == nil {
		return nil, fmt.Errorf("bbolt: run %s has no cursor", id)
	}
	cur, err := storagepb.UnmarshalCursor(cb)
	if err != nil {
		return nil, err
	}
	rec.Phase = cur.Phase
	rec.NextAttemptAt = cur.NextAttemptAt
	rec.LastError, rec.LastReason, rec.LastErrorAt = cur.LastError, cur.LastReason, cur.LastErrorAt
	rec.Awaiting = cur.Awaiting
	rec.Awaited = cur.Awaited
	rec.UpdatedAt = cur.UpdatedAt
	if cur.StepID != "" {
		sr := rec.Step(cur.StepID)
		if cur.Phase == kernel.PhaseUnwind && sr.ForwardStatus == driver.OpSucceeded {
			sr.UnwindStatus = driver.OpUnresolved
			sr.UnwindAttempts = cur.Attempts
		} else {
			sr.ForwardStatus = driver.OpUnresolved
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

func (s *Store) GetRun(_ context.Context, id kernel.RunID) (*driver.RunRecord, error) {
	var rec *driver.RunRecord
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
			// Step rows and unwind failure rows hang off the run id with
			// a NUL; both buckets are swept by prefix.
			prefix := stepKey(kernel.RunID(id), "")
			for _, bucket := range [][]byte{stepsBucket, failuresBucket} {
				if err := deletePrefix(tx.Bucket(bucket), prefix); err != nil {
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

// deletePrefix removes every key under prefix, collecting first: bbolt
// forbids mutating a bucket while iterating it.
func deletePrefix(b *bolt.Bucket, prefix []byte) error {
	var keys [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RequestCancel(_ context.Context, id kernel.RunID, req driver.CancelRequest) (bool, error) {
	accepted := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		switch {
		case tx.Bucket(metaBucket).Get([]byte(id)) == nil:
			return kernel.ErrRunNotFound
		case tx.Bucket(terminalBucket).Get([]byte(id)) != nil:
			return kernel.ErrRunTerminal
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

// ListNonterminal walks the slots bucket: a run holds its slot from the
// CreateRun transaction until the terminal transition releases it, so the
// slot values are exactly the nonterminal run ids. The walk is
// proportional to the runs in flight, not to the retained history in
// meta. A slot naming a run with no meta row is corruption, reported
// rather than skipped.
func (s *Store) ListNonterminal(_ context.Context) ([]*driver.RunRecord, error) {
	var out []*driver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(slotsBucket).ForEach(func(_, v []byte) error {
			rec, err := getRun(tx, kernel.RunID(v))
			if err != nil {
				return fmt.Errorf("bbolt: slot references run %s: %w", v, err)
			}
			out = append(out, rec)
			return nil
		})
	})
	return out, err
}

func (s *Store) ListRuns(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*driver.RunRecord, error) {
	var out []*driver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).ForEach(func(k, v []byte) error {
			probe := &driver.RunRecord{}
			if err := storagepb.UnmarshalRunMetaInto(v, probe); err != nil {
				return err
			}
			if probe.PipelineID != pipeline || probe.ResourceID != resource {
				return nil
			}
			rec, err := getRun(tx, kernel.RunID(k))
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

// GetActiveRunID answers from the slots bucket: one point read, the same
// index CreateRun enforces the slot with.
func (s *Store) GetActiveRunID(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) (kernel.RunID, bool, error) {
	var id kernel.RunID
	var ok bool
	err := s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(slotsBucket).Get(slotKeyFor(pipeline, resource)); v != nil {
			id, ok = kernel.RunID(v), true
		}
		return nil
	})
	return id, ok, err
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
