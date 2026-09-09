// Package bbolt implements driver.Store on a local bbolt database and
// registers the "bbolt" scheme with package store. Importing it alongside
// go.etcd.io/bbolt needs an alias for one of the two.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation stores a run in five buckets, chosen by write cadence
// and value size:
//
//	input     run id                    -> input bytes         written once
//	active    run id · tag [· ...]      -> small facts          append-only
//	cursor    run id                    -> Cursor               rewritten per attempt
//	terminal  run id                    -> Terminal             written once
//	slots     pipeline · resource       -> run id               index
//
// (· is the NUL separator, which the Store contract keeps out of every
// identifier.) A nonterminal run is its input, its active rows, and its
// cursor; the terminality commit replaces all of them with one terminal
// record — identity, outcome, output, commit time, failure, cancel
// request, and the permanently failed unwinds — in the same transaction,
// so the input and step states, folded into the output by then, are
// released when the run ends rather than when retention reaps it.
// Exactly one of a run's meta row and terminal row exists; GetRun
// dispatches on which.
//
// The active bucket holds every write-once fact of a nonterminal run
// under the run id, so one prefix walk assembles it and one prefix walk
// deletes it. The tag byte after the run id says what a row is, and tags
// sort in the order a reader wants them:
//
//	M   RunMeta (identity, annotations, created_at)
//	c   CancelRequest
//	f   the run's FailureRecord
//	o   an operation row: o · order (4 bytes, big-endian) · step · phase
//
// Operation rows lead with their resolution order, so the walk returns a
// run's operations in the order they resolved; an unresolved operation
// flushed to its row (a topology change displaced it) carries order zero
// and sorts ahead of the resolved history until it resolves, when its
// row moves to its real order. Meta and failure rows are written once and
// the store refuses a second write; operation rows are one per step and
// phase, replaced only as the contract's upsert allows.
//
// Per-attempt write volume is the cursor's, independent of input and
// state sizes: the input sits alone in its bucket because bbolt rewrites
// a whole leaf node on any write to it, so a large value must not share
// a node with rows that change. An active-slot index keyed by
// (PipelineID, ResourceID) holds every nonterminal run: CreateRun admits
// against it, GetActiveRunID reads it, and ListNonterminal walks it, so
// recovery cost follows the runs in flight rather than the retained
// history.
package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/internal/storagepb"
)

var (
	inputBucket    = []byte("input")
	activeBucket   = []byte("active")
	cursorBucket   = []byte("cursor")
	terminalBucket = []byte("terminal")
	slotsBucket    = []byte("slots")
)

// Row tags of the active bucket, in sort order.
const (
	tagMeta    byte = 'M'
	tagCancel  byte = 'c'
	tagFailure byte = 'f'
	tagOp      byte = 'o'
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
		for _, name := range [][]byte{inputBucket, activeBucket, cursorBucket, terminalBucket, slotsBucket} {
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

// runPrefix covers every active row of a run.
func runPrefix(id kernel.RunID) []byte { return []byte(string(id) + "\x00") }

// activeKey addresses a run's single-row fact: meta, cancel, or failure.
func activeKey(id kernel.RunID, tag byte) []byte {
	return append(runPrefix(id), tag)
}

// opPrefix covers every operation row of a run.
func opPrefix(id kernel.RunID) []byte {
	return append(activeKey(id, tagOp), 0)
}

// opKey addresses one operation row: run id, the op tag, the resolution
// order big-endian so rows sort by it, the step id, and a phase byte.
func opKey(id kernel.RunID, order uint32, step kernel.StepID, phase kernel.Phase) []byte {
	k := opPrefix(id)
	k = binary.BigEndian.AppendUint32(k, order)
	k = append(k, 0)
	k = append(k, step...)
	k = append(k, 0, phaseByte(phase))
	return k
}

func phaseByte(phase kernel.Phase) byte {
	if phase == kernel.PhaseUnwind {
		return 'u'
	}
	return 'f'
}

// splitActiveKey recovers the run id and tag of an active key, and the
// bytes after the tag.
func splitActiveKey(k []byte) (id kernel.RunID, tag byte, rest []byte, ok bool) {
	i := bytes.IndexByte(k, 0)
	if i < 0 || i+1 >= len(k) {
		return "", 0, nil, false
	}
	return kernel.RunID(k[:i]), k[i+1], k[i+2:], true
}

// splitOpRest recovers order, step id, and phase from the bytes after an
// operation row's tag.
func splitOpRest(rest []byte) (order uint32, step kernel.StepID, phase kernel.Phase, ok bool) {
	// · order(4) · step · phase
	if len(rest) < 1+4+1+2 || rest[0] != 0 || rest[5] != 0 || rest[len(rest)-2] != 0 {
		return 0, "", 0, false
	}
	order = binary.BigEndian.Uint32(rest[1:5])
	tail := rest[6:]
	phase = kernel.PhaseForward
	if tail[len(tail)-1] == 'u' {
		phase = kernel.PhaseUnwind
	}
	return order, kernel.StepID(tail[:len(tail)-2]), phase, true
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

// putOnce writes a write-once row, refusing to overwrite one.
func putOnce(b *bolt.Bucket, what string, id kernel.RunID, k, v []byte) error {
	if b.Get(k) != nil {
		return fmt.Errorf("bbolt: %s of run %s already written", what, id)
	}
	return b.Put(k, v)
}

// putRun persists every present component of rec; used at creation (and
// for seeded records carrying pre-existing facts). A record seeded
// terminal is written in its terminal stage.
func putRun(tx *bolt.Tx, rec *driver.RunRecord) error {
	if rec.Outcome != nil {
		c := rec.Clone()
		c.CompactTerminal()
		return putTerminal(tx, c)
	}
	active := tx.Bucket(activeBucket)
	meta, err := storagepb.MarshalRunMeta(rec)
	if err != nil {
		return err
	}
	if err := putOnce(active, "meta", rec.RunID, activeKey(rec.RunID, tagMeta), meta); err != nil {
		return err
	}
	if len(rec.Input) > 0 {
		if err := tx.Bucket(inputBucket).Put([]byte(rec.RunID), rec.Input); err != nil {
			return err
		}
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
	if rec.Failure != nil {
		if err := putRootFailure(tx, rec.RunID, rec.Failure); err != nil {
			return err
		}
	}
	if rec.Cancel != nil {
		b, err := storagepb.MarshalCancel(rec.Cancel)
		if err != nil {
			return err
		}
		if err := putOnce(active, "cancel request", rec.RunID, activeKey(rec.RunID, tagCancel), b); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ApplyTransition(_ context.Context, id kernel.RunID, t driver.Transition) error {
	commit, done := s.groupCommit()
	defer done()
	return commit(func(tx *bolt.Tx) error {
		metaBytes := tx.Bucket(activeBucket).Get(activeKey(id, tagMeta))
		if metaBytes == nil {
			// A terminal run has no meta; it accepts no further
			// transitions, and the engine never sends one.
			if tx.Bucket(terminalBucket).Get([]byte(id)) != nil {
				return kernel.ErrRunTerminal
			}
			return kernel.ErrRunNotFound
		}
		if t.Outcome != nil {
			// The terminality commit: the nonterminal stage — input,
			// active rows, cursor — gives way to one terminal record, and
			// the slot is released. The record is assembled the way the
			// reference store's would be after this transition (rows,
			// then the transition's ops, then its cursor's in-flight
			// overlay) and compacted by the shared rule, so the failed
			// unwinds it keeps are the same ones the model keeps.
			rec, err := getNonterminal(tx, id, metaBytes)
			if err != nil {
				return err
			}
			if t.Failure != nil {
				f := *t.Failure
				rec.Failure = &f
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
		if t.Failure != nil {
			if err := putRootFailure(tx, id, t.Failure); err != nil {
				return err
			}
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

// deleteNonterminalStage removes a run's input, active rows, and cursor.
// Active rows hang off the run id; they are collected before deletion
// since bbolt forbids mutating a bucket while iterating it.
func deleteNonterminalStage(tx *bolt.Tx, id kernel.RunID) error {
	active := tx.Bucket(activeBucket)
	prefix := runPrefix(id)
	c := active.Cursor()
	var keys [][]byte
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		if err := active.Delete(k); err != nil {
			return err
		}
	}
	for _, bucket := range [][]byte{inputBucket, cursorBucket} {
		if err := tx.Bucket(bucket).Delete([]byte(id)); err != nil {
			return err
		}
	}
	return nil
}

// findOp locates the run's row for step and phase, whatever its order.
// It returns nil when there is none.
func findOp(active *bolt.Bucket, id kernel.RunID, step kernel.StepID, phase kernel.Phase) (key, value []byte) {
	prefix := opPrefix(id)
	c := active.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		_, st, ph, ok := splitOpRest(k[len(prefix)-1:])
		if ok && st == step && ph == phase {
			return k, v
		}
	}
	return nil, nil
}

// putOp writes the run's one row for the operation. A row already there
// under another order — an unresolved flush resolving to its real order,
// or the contract's upsert — is removed so the step and phase keep one
// row.
func putOp(tx *bolt.Tx, id kernel.RunID, step kernel.StepID, phase kernel.Phase, op *driver.OperationRecord) error {
	b, err := storagepb.MarshalOperationRecord(op)
	if err != nil {
		return err
	}
	active := tx.Bucket(activeBucket)
	key := opKey(id, op.Order, step, phase)
	if old, _ := findOp(active, id, step, phase); old != nil && !bytes.Equal(old, key) {
		if err := active.Delete(bytes.Clone(old)); err != nil {
			return err
		}
	}
	return active.Put(key, b)
}

func putRootFailure(tx *bolt.Tx, id kernel.RunID, rf *kernel.Failure) error {
	b, err := storagepb.MarshalFailureRecord(*rf)
	if err != nil {
		return err
	}
	return putOnce(tx.Bucket(activeBucket), "failure", id, activeKey(id, tagFailure), b)
}

// getRun assembles the read model from the run's components, dispatching
// on its stage. Nonterminal: input, a prefix walk of the active rows, and
// the cursor with its in-flight operation overlaid as an unresolved step
// entry. Terminal: the terminal record alone.
func getRun(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	if metaBytes := tx.Bucket(activeBucket).Get(activeKey(id, tagMeta)); metaBytes != nil {
		return getNonterminal(tx, id, metaBytes)
	}
	tb := tx.Bucket(terminalBucket).Get([]byte(id))
	if tb == nil {
		return nil, kernel.ErrRunNotFound
	}
	rec := &driver.RunRecord{}
	if err := storagepb.UnmarshalTerminalInto(tb, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func getNonterminal(tx *bolt.Tx, id kernel.RunID, metaBytes []byte) (*driver.RunRecord, error) {
	rec := &driver.RunRecord{}
	if err := storagepb.UnmarshalRunMetaInto(metaBytes, rec); err != nil {
		return nil, err
	}
	if in := tx.Bucket(inputBucket).Get([]byte(id)); in != nil {
		rec.Input = bytes.Clone(in)
	}

	prefix := runPrefix(id)
	c := tx.Bucket(activeBucket).Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		_, tag, rest, ok := splitActiveKey(k)
		if !ok {
			return nil, fmt.Errorf("bbolt: malformed active key %q", k)
		}
		switch tag {
		case tagMeta:
		case tagCancel:
			cr, err := storagepb.UnmarshalCancel(v)
			if err != nil {
				return nil, err
			}
			rec.Cancel = cr
		case tagFailure:
			f, err := storagepb.UnmarshalFailureRecord(v)
			if err != nil {
				return nil, err
			}
			rec.Failure = &f
		case tagOp:
			_, step, phase, ok := splitOpRest(rest)
			if !ok {
				return nil, fmt.Errorf("bbolt: malformed operation key %q", k)
			}
			op, err := storagepb.UnmarshalOperationRecord(v)
			if err != nil {
				return nil, err
			}
			*rec.Step(step).Op(phase) = op
		default:
			return nil, fmt.Errorf("bbolt: unknown active row tag %q in key %q", tag, k)
		}
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
			// A terminal run is one row; the nonterminal stage went at
			// terminality.
			if err := tx.Bucket(terminalBucket).Delete(id); err != nil {
				return err
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
		active := tx.Bucket(activeBucket)
		key := activeKey(id, tagCancel)
		switch {
		case tx.Bucket(terminalBucket).Get([]byte(id)) != nil:
			return kernel.ErrRunTerminal
		case active.Get(activeKey(id, tagMeta)) == nil:
			return kernel.ErrRunNotFound
		case active.Get(key) != nil:
			return nil // first cancel wins
		}
		b, err := storagepb.MarshalCancel(&req)
		if err != nil {
			return err
		}
		if err := active.Put(key, b); err != nil {
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

// ListRuns scans both stages: the meta rows of the active bucket for the
// nonterminal runs and the terminal bucket for the rest.
func (s *Store) ListRuns(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) ([]*driver.RunRecord, error) {
	var out []*driver.RunRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		collect := func(id kernel.RunID, probe *driver.RunRecord) error {
			if probe.PipelineID != pipeline || probe.ResourceID != resource {
				return nil
			}
			rec, err := getRun(tx, id)
			if err != nil {
				return err
			}
			out = append(out, rec)
			return nil
		}
		if err := tx.Bucket(activeBucket).ForEach(func(k, v []byte) error {
			id, tag, _, ok := splitActiveKey(k)
			if !ok || tag != tagMeta {
				return nil
			}
			probe := &driver.RunRecord{}
			if err := storagepb.UnmarshalRunMetaInto(v, probe); err != nil {
				return err
			}
			return collect(id, probe)
		}); err != nil {
			return err
		}
		return tx.Bucket(terminalBucket).ForEach(func(k, v []byte) error {
			probe := &driver.RunRecord{}
			if err := storagepb.UnmarshalTerminalInto(v, probe); err != nil {
				return err
			}
			return collect(kernel.RunID(k), probe)
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
