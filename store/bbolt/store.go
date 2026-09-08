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
// meta (identity + input), one operation row per step phase written at
// that operation's resolution and carrying its own failure, the run's
// write-once run failure and cancel records, and the small cursor
// rewritten per attempt. Per-attempt write volume is therefore
// independent of input and state sizes, an unwind never rewrites the
// forward row's state, and no row is read back to be rewritten on the
// attempt path.
//
// A run has two storage stages. Nonterminal, it is meta, cursor, and
// operation rows. The terminality commit reads the run once and
// replaces those three with one terminal record — identity, outcome,
// output, commit time, and the permanently failed unwinds — in the same
// transaction, so the input and step states, folded into the output by
// then, are released when the run ends rather than when retention
// reaps it. Failure and cancel records span both stages. Exactly one of
// meta and terminal exists for a run; GetRun dispatches on which.
//
// An active-slot index keyed by (PipelineID, ResourceID) holds every
// nonterminal run: CreateRun admits against it, GetActiveRunID reads it,
// and ListNonterminal walks it, so recovery cost follows the runs in
// flight rather than the retained history.
package bbolt

import (
	"bytes"
	"context"
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

// opKey addresses one operation row: run id, step id, and a phase byte,
// NUL-separated. runPrefix(id) covers every operation of a run; the
// forward row sorts before the unwind row of the same step.
func opKey(id kernel.RunID, step kernel.StepID, phase kernel.Phase) []byte {
	return []byte(string(id) + "\x00" + string(step) + "\x00" + string(phaseByte(phase)))
}

func runPrefix(id kernel.RunID) []byte { return []byte(string(id) + "\x00") }

func phaseByte(phase kernel.Phase) byte {
	if phase == kernel.PhaseUnwind {
		return 'u'
	}
	return 'f'
}

// splitOpKey recovers the step id and phase from a key under runPrefix.
func splitOpKey(id kernel.RunID, k []byte) (kernel.StepID, kernel.Phase, bool) {
	rest := k[len(id)+1:]
	if len(rest) < 2 || rest[len(rest)-2] != 0 {
		return "", 0, false
	}
	phase := kernel.PhaseForward
	if rest[len(rest)-1] == 'u' {
		phase = kernel.PhaseUnwind
	}
	return kernel.StepID(rest[:len(rest)-2]), phase, true
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
// for seeded records carrying pre-existing facts). A record seeded
// terminal is written in its terminal stage.
func putRun(tx *bolt.Tx, rec *driver.RunRecord) error {
	if rec.Failure != nil {
		if err := putRootFailure(tx, rec.RunID, rec.Failure); err != nil {
			return err
		}
	}
	if rec.Outcome != nil {
		c := rec.Clone()
		c.CompactTerminal()
		return putTerminal(tx, c)
	}
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
		for _, phase := range []kernel.Phase{kernel.PhaseForward, kernel.PhaseUnwind} {
			if op := sr.Op(phase); op.Status != driver.OpNone {
				if err := putOp(tx, rec.RunID, sid, phase, op); err != nil {
					return err
				}
			}
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
			// A terminal run has no meta; it accepts no further
			// transitions, and the engine never sends one.
			if tx.Bucket(terminalBucket).Get([]byte(id)) != nil {
				return kernel.ErrRunTerminal
			}
			return kernel.ErrRunNotFound
		}
		if t.Failure != nil {
			if err := putRootFailure(tx, id, t.Failure); err != nil {
				return err
			}
		}
		if t.Outcome != nil {
			// The terminality commit: the nonterminal stage — meta,
			// cursor, operation rows — gives way to one terminal record,
			// and the slot is released. The record is assembled the way
			// the reference store's would be after this transition (rows,
			// then the transition's ops, then its cursor's in-flight
			// overlay) and compacted by the shared rule, so the failed
			// unwinds it keeps are the same ones the model keeps.
			rec, err := getNonterminal(tx, id, metaBytes)
			if err != nil {
				return err
			}
			for _, ow := range t.Ops {
				*rec.Step(ow.StepID).Op(ow.Phase) = ow.Record
			}
			if t.Cursor.StepID != "" {
				sr := rec.Step(t.Cursor.StepID)
				if t.Cursor.Phase == kernel.PhaseUnwind && sr.Forward.Status == driver.OpSucceeded {
					sr.Unwind.Status = driver.OpUnresolved
				} else {
					sr.Forward.Status = driver.OpUnresolved
				}
			}
			rec.Phase = t.Cursor.Phase
			rec.UpdatedAt = t.Cursor.UpdatedAt
			oc := *t.Outcome
			rec.Outcome = &oc
			rec.Output = t.Output
			rec.CompactTerminal()
			if err := putTerminal(tx, rec); err != nil {
				return err
			}
			if err := deleteNonterminalStage(tx, id); err != nil {
				return err
			}
			key := slotKey(rec)
			if active := tx.Bucket(slotsBucket).Get(key); active != nil && string(active) == string(id) {
				return tx.Bucket(slotsBucket).Delete(key)
			}
			return nil
		}
		cursor, err := storagepb.MarshalCursor(t.Cursor)
		if err != nil {
			return err
		}
		if err := tx.Bucket(cursorBucket).Put([]byte(id), cursor); err != nil {
			return err
		}
		for _, ow := range t.Ops {
			op := ow.Record
			if err := putOp(tx, id, ow.StepID, ow.Phase, &op); err != nil {
				return err
			}
		}
		return nil
	})
}

func putTerminal(tx *bolt.Tx, rec *driver.RunRecord) error {
	b, err := storagepb.MarshalTerminal(rec)
	if err != nil {
		return err
	}
	return tx.Bucket(terminalBucket).Put([]byte(rec.RunID), b)
}

// deleteNonterminalStage removes a run's meta, cursor, and operation
// rows. Operation rows hang off the run id; they are collected before
// deletion since bbolt forbids mutating a bucket while iterating it.
func deleteNonterminalStage(tx *bolt.Tx, id kernel.RunID) error {
	prefix := runPrefix(id)
	sc := tx.Bucket(stepsBucket).Cursor()
	var opKeys [][]byte
	for k, _ := sc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = sc.Next() {
		opKeys = append(opKeys, bytes.Clone(k))
	}
	for _, k := range opKeys {
		if err := tx.Bucket(stepsBucket).Delete(k); err != nil {
			return err
		}
	}
	for _, bucket := range [][]byte{cursorBucket, metaBucket} {
		if err := tx.Bucket(bucket).Delete([]byte(id)); err != nil {
			return err
		}
	}
	return nil
}

func putOp(tx *bolt.Tx, id kernel.RunID, step kernel.StepID, phase kernel.Phase, op *driver.OperationRecord) error {
	b, err := storagepb.MarshalOperationRecord(op)
	if err != nil {
		return err
	}
	return tx.Bucket(stepsBucket).Put(opKey(id, step, phase), b)
}

func putRootFailure(tx *bolt.Tx, id kernel.RunID, rf *kernel.Failure) error {
	b, err := storagepb.MarshalFailureRecord(*rf)
	if err != nil {
		return err
	}
	return tx.Bucket(failuresBucket).Put([]byte(id), b)
}

func readRootFailure(tx *bolt.Tx, id kernel.RunID) (*kernel.Failure, error) {
	b := tx.Bucket(failuresBucket).Get([]byte(id))
	if b == nil {
		return nil, nil
	}
	f, err := storagepb.UnmarshalFailureRecord(b)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// getRun assembles the read model from the run's components, dispatching
// on its stage. Nonterminal: meta, step rows, and the cursor, with the
// cursor's in-flight operation overlaid as an unresolved step entry.
// Terminal: the terminal record alone. Failure and cancel are read in
// both stages.
func getRun(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	var rec *driver.RunRecord
	var err error
	if metaBytes := tx.Bucket(metaBucket).Get([]byte(id)); metaBytes != nil {
		rec, err = getNonterminal(tx, id, metaBytes)
	} else if tb := tx.Bucket(terminalBucket).Get([]byte(id)); tb != nil {
		rec = &driver.RunRecord{}
		err = storagepb.UnmarshalTerminalInto(tb, rec)
	} else {
		return nil, kernel.ErrRunNotFound
	}
	if err != nil {
		return nil, err
	}

	if rec.Failure, err = readRootFailure(tx, id); err != nil {
		return nil, err
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

func getNonterminal(tx *bolt.Tx, id kernel.RunID, metaBytes []byte) (*driver.RunRecord, error) {
	rec := &driver.RunRecord{}
	if err := storagepb.UnmarshalRunMetaInto(metaBytes, rec); err != nil {
		return nil, err
	}

	prefix := runPrefix(id)
	c := tx.Bucket(stepsBucket).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		step, phase, ok := splitOpKey(id, k)
		if !ok {
			return nil, fmt.Errorf("bbolt: malformed operation key %q", k)
		}
		op, err := storagepb.UnmarshalOperationRecord(v)
		if err != nil {
			return nil, err
		}
		*rec.Step(step).Op(phase) = op
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
		if cur.Phase == kernel.PhaseUnwind && sr.Forward.Status == driver.OpSucceeded {
			sr.Unwind.Status = driver.OpUnresolved
			sr.Unwind.Attempts = cur.Attempts
		} else {
			sr.Forward.Status = driver.OpUnresolved
			sr.Forward.Attempts = cur.Attempts
		}
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
		// The terminal bucket's keys are exactly the terminal run ids,
		// and each record carries its commit time.
		var victims [][]byte
		c := tx.Bucket(terminalBucket).Cursor()
		for k, v := c.First(); k != nil && len(victims) < limit; k, v = c.Next() {
			rec := &driver.RunRecord{}
			if err := storagepb.UnmarshalTerminalInto(v, rec); err != nil {
				return err
			}
			if rec.UpdatedAt.Before(before) {
				victims = append(victims, bytes.Clone(k))
			}
		}
		for _, id := range victims {
			// The nonterminal stage is already gone for a run that
			// committed through this driver; sweeping it anyway costs one
			// cursor seek and keeps a database whose terminal runs predate
			// the stage split from leaving orphans behind.
			if err := deleteNonterminalStage(tx, kernel.RunID(id)); err != nil {
				return err
			}
			for _, bucket := range [][]byte{failuresBucket, cancelBucket, terminalBucket} {
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

func (s *Store) RequestCancel(_ context.Context, id kernel.RunID, req driver.CancelRequest) (bool, error) {
	accepted := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		switch {
		case tx.Bucket(terminalBucket).Get([]byte(id)) != nil:
			return kernel.ErrRunTerminal
		case tx.Bucket(metaBucket).Get([]byte(id)) == nil:
			return kernel.ErrRunNotFound
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

// ListRuns scans both stages: meta for the nonterminal runs and the
// terminal bucket for the rest.
func (s *Store) ListRuns(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*driver.RunRecord, error) {
	var out []*driver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		seen := map[string]bool{}
		collect := func(k []byte, probe *driver.RunRecord) error {
			if probe.PipelineID != pipeline || probe.ResourceID != resource || seen[string(k)] {
				return nil
			}
			rec, err := getRun(tx, kernel.RunID(k))
			if err != nil {
				return err
			}
			seen[string(k)] = true
			out = append(out, rec)
			return nil
		}
		if err := tx.Bucket(metaBucket).ForEach(func(k, v []byte) error {
			probe := &driver.RunRecord{}
			if err := storagepb.UnmarshalRunMetaInto(v, probe); err != nil {
				return err
			}
			return collect(k, probe)
		}); err != nil {
			return err
		}
		return tx.Bucket(terminalBucket).ForEach(func(k, v []byte) error {
			probe := &driver.RunRecord{}
			if err := storagepb.UnmarshalTerminalInto(v, probe); err != nil {
				return err
			}
			return collect(k, probe)
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
