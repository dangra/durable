// Package bbolt implements driver.Store on a local bbolt database and
// registers the "bbolt" scheme with package store. Importing it alongside
// go.etcd.io/bbolt needs an alias for one of the two.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation stores a run in five buckets, chosen by write cadence.
// With · for the NUL separator, which the Store contract keeps out of
// every identifier, and R for a run id:
//
//	bucket: active                     append-only facts of nonterminal runs
//	  R·M                              -> RunMeta          write-once, refused twice
//	  R·c                              -> CancelRequest    write-once, first cancel wins
//	  R·f                              -> FailureRecord    write-once, refused twice
//	  R·i                              -> nested bucket { i -> input bytes }
//	  R·o·<order:4 BE>·<step>·<phase>  -> OperationRecord  one row per step and phase
//	                                      phase byte: f forward, u unwind
//
//	bucket: cursor                     the one mutable row
//	  R                                -> Cursor           rewritten on every attempt
//
//	bucket: terminal                   the whole of a terminal run
//	  R                                -> Terminal         identity, annotations, phase,
//	                                                       outcome, output, committed_at,
//	                                                       failure, cancel, failed_unwinds
//
//	bucket: expiry                     retention order of terminal runs
//	  <committed_at:8 BE>·R            -> (empty)          written with the terminal row
//
//	bucket: slots                      admission index
//	  <pipeline>·<resource>            -> R                held from CreateRun to terminality
//
// A run moves through it like this. CreateRun writes R·M, the input
// bucket when there is one, the cursor row, and the slot, in one
// transaction. Each attempt rewrites only the cursor. Each resolution
// appends one R·o row; an unresolved operation displaced by a topology
// change is flushed at order zero and moves to its real order when it
// resolves, the one delete before terminality. Cancel and failure land
// as R·c and R·f. The terminality commit writes the terminal row —
// from then on the run reads from that row alone, so the input and
// step states, folded into the output by then, are released when the
// run ends rather than when retention reaps it — deletes the slot, and
// queues R. The drain later deletes everything under R· in active, the
// input bucket included, plus the cursor row, in batches (see Store),
// because deleting adjacent runs together lets bbolt free whole leaves
// instead of rewriting one per run. Reap walks the expiry index from its
// oldest key and stops at the first run that has not expired, deleting
// each victim's terminal row and index key: proportional to the victims,
// decoding nothing. The terminal row is authoritative wherever both
// stages exist, which is what makes the deferred drain safe.
//
// What each choice bought. Tags sort M, c, f, i, o, so one prefix walk
// reads a run in the order a reader wants it, with operations in
// resolution order and no sorting. The input is a nested bucket so its
// bytes never share a leaf node with rows that change or with other
// inputs: bbolt rewrites a whole leaf node on any write to it, and a
// bucket over a quarter page gets pages of its own, so creating and
// deleting an input touches the run's own pages plus one small entry in
// the active leaf. Operation rows are plain rows because a nested bucket
// costs a page per write for anything appended to (every write into a
// child bucket rewrites the child's page and the parent's entry for it;
// measured, and rejected, for a bucket per run and for a bucket per
// run's operations). The cursor is its own bucket because it is the
// only row rewritten, so per-attempt write volume is the cursor's,
// independent of input and state sizes. Terminal is one row because
// everything a caller can still reach lives in it. The slots index
// holds every nonterminal run: CreateRun admits against it,
// GetActiveRunID reads it, and ListNonterminal walks it, so recovery
// cost follows the runs in flight rather than the retained history.
package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/internal/storagepb"
)

var (
	activeBucket   = []byte("active")
	cursorBucket   = []byte("cursor")
	terminalBucket = []byte("terminal")
	expiryBucket   = []byte("expiry")
	slotsBucket    = []byte("slots")
)

// Row tags of the active bucket, in sort order.
const (
	tagMeta    byte = 'M'
	tagCancel  byte = 'c'
	tagFailure byte = 'f'
	tagInput   byte = 'i'
	tagOp      byte = 'o'
)

// Store is a driver.Store backed by a bbolt database file.
type Store struct {
	db *bolt.DB
	// pending counts in-flight ApplyTransition calls for adaptive group
	// commit: a lone caller commits immediately, concurrent callers
	// coalesce into shared transactions.
	pending atomic.Int64

	// stage queues the terminal runs whose nonterminal stage is still on
	// disk. Deleting one run's rows per transaction costs a copied
	// root-to-leaf path and a rewritten leaf each time; deleting a batch
	// of adjacent runs empties whole leaves, which bbolt frees without
	// writing. So terminality only queues, and the stage is deleted in
	// batches: inside the next write transaction (see stageDrain), by
	// the sweep goroutine on stageDrainInterval when writes are idle, at
	// Close, and — for a queue lost to a crash — by reconcileStages at
	// Open. Reads never see the lingering rows: every path checks the
	// terminal row first.
	stageMu sync.Mutex
	stage   []kernel.RunID
	queued  atomic.Int64 // len(stage), readable without the lock
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

const (
	// stageDrainInterval is how long a terminal run's stage may linger
	// while the store is idle.
	stageDrainInterval = time.Second
	// stageDrainPerWrite is the batch a write transaction carries. A
	// write only takes a batch once that many runs are queued: deleting
	// a few runs per transaction would rewrite a leaf and a path each
	// time, which is the per-run cost batching exists to avoid. Smaller
	// backlogs wait for the sweep.
	stageDrainPerWrite = 64
	// stageDrainPerSweep bounds one sweep transaction so a large backlog
	// does not hold the write lock for long.
	stageDrainPerSweep = 256
)

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
		// bbolt otherwise writes its freelist on every commit, and that
		// page grows with the number of free pages, so a store that
		// releases a run's input and states when the run ends would pay
		// for every page it freed on every later write. With the sync
		// off the freelist is rebuilt at Open by scanning the file — a
		// cost proportional to the database, which the stage split keeps
		// proportional to the runs in flight and the retained terminal
		// rows.
		NoFreelistSync: true,
	})
	if err != nil {
		return nil, fmt.Errorf("bbolt: opening %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{activeBucket, cursorBucket, terminalBucket, expiryBucket, slotsBucket} {
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
	s := &Store{db: db, stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.reconcileStages(); err != nil {
		db.Close()
		return nil, fmt.Errorf("bbolt: reconciling terminal runs: %w", err)
	}
	go s.sweep()
	return s, nil
}

// reconcileStages queues the stage of every run that has both a meta
// row and a terminal row — terminal runs whose queued deletion a crash
// lost — and drains them. It walks the active bucket, which holds the
// runs in flight and those orphans, not the retained history.
func (s *Store) reconcileStages() error {
	var orphans []kernel.RunID
	err := s.db.View(func(tx *bolt.Tx) error {
		terminal := tx.Bucket(terminalBucket)
		return tx.Bucket(activeBucket).ForEach(func(k, _ []byte) error {
			if id, tag, _, ok := splitActiveKey(k); ok && tag == tagMeta && terminal.Get(id) != nil {
				orphans = append(orphans, kernel.RunID(id))
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	s.enqueueStage(orphans...)
	return s.Drain()
}

func (s *Store) enqueueStage(ids ...kernel.RunID) {
	if len(ids) == 0 {
		return
	}
	s.stageMu.Lock()
	s.stage = append(s.stage, ids...)
	s.queued.Store(int64(len(s.stage)))
	s.stageMu.Unlock()
}

// claimStage takes up to n queued runs. It never returns nil, so a
// caller can tell "claimed nothing" from "not yet claimed".
func (s *Store) claimStage(n int) []kernel.RunID {
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	n = min(n, len(s.stage))
	claimed := append([]kernel.RunID{}, s.stage[:n]...)
	s.stage = s.stage[n:]
	s.queued.Store(int64(len(s.stage)))
	return claimed
}

// stageDrain rides one write transaction: it claims a batch of queued
// runs the first time the closure runs and deletes their stages, and
// returns them to the queue if the transaction ultimately fails. The
// claim is made once because Batch may run a closure twice — a
// rolled-back batch re-runs each closure alone — and a second claim
// would lose the first batch.
type stageDrain struct {
	s       *Store
	n       int
	claimed []kernel.RunID
}

func (d *stageDrain) run(tx *bolt.Tx) error {
	if d.claimed == nil {
		d.claimed = d.s.claimStage(d.n)
	}
	for _, id := range d.claimed {
		if err := deleteNonterminalStage(tx, id); err != nil {
			return err
		}
	}
	return nil
}

func (d *stageDrain) finish(err error) {
	if err != nil {
		d.s.enqueueStage(d.claimed...)
	}
}

// writeDrain is the drain a caller's write transaction carries: a full
// batch when one is queued, nothing otherwise — the idle path allocates
// nothing, since the two functions returned capture no state.
func (s *Store) writeDrain() (run func(*bolt.Tx) error, finish func(error)) {
	if s.queued.Load() < stageDrainPerWrite {
		return noDrain, noFinish
	}
	d := &stageDrain{s: s, n: stageDrainPerWrite}
	return d.run, d.finish
}

func noDrain(*bolt.Tx) error { return nil }
func noFinish(error)         {}

// Drain deletes the nonterminal stage of every queued terminal run now,
// in bounded transactions. The store drains on its own — inside write
// transactions, on a timer, and at Close — so callers need it only to
// observe the on-disk state deterministically.
func (s *Store) Drain() error {
	for {
		d := &stageDrain{s: s, n: stageDrainPerSweep}
		err := s.db.Update(d.run)
		d.finish(err)
		if err != nil {
			return err
		}
		if len(d.claimed) < stageDrainPerSweep {
			return nil
		}
	}
}

// sweep drains the queue on a timer so an idle store does not hold a
// terminal run's input and states for long.
func (s *Store) sweep() {
	defer close(s.done)
	t := time.NewTicker(stageDrainInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			if s.queued.Load() > 0 {
				_ = s.Drain() // a failed sweep requeues; the next tick retries
			}
		}
	}
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

// splitActiveKey recovers the run id bytes and tag of an active key, and
// the bytes after the tag. It allocates nothing; callers that need the
// id as a string convert it.
func splitActiveKey(k []byte) (id []byte, tag byte, rest []byte, ok bool) {
	i := bytes.IndexByte(k, 0)
	if i < 0 || i+1 >= len(k) {
		return nil, 0, nil, false
	}
	return k[:i], k[i+1], k[i+2:], true
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
	drain, finish := s.writeDrain()
	err := commit(func(tx *bolt.Tx) error {
		if err := drain(tx); err != nil {
			return err
		}
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
	finish(err)
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
		if err := putInput(active, rec.RunID, rec.Input); err != nil {
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
	drain, finish := s.writeDrain()
	err := commit(func(tx *bolt.Tx) error {
		if err := drain(tx); err != nil {
			return err
		}
		// A terminal run accepts no further transitions, and the engine
		// never sends one. Its meta may linger until the stage drains,
		// so the terminal row is checked first.
		if tx.Bucket(terminalBucket).Get([]byte(id)) != nil {
			return kernel.ErrRunTerminal
		}
		if tx.Bucket(activeBucket).Get(activeKey(id, tagMeta)) == nil {
			return kernel.ErrRunNotFound
		}
		if t.Outcome != nil {
			// The terminality commit: one terminal record is written and
			// the slot is released; the nonterminal stage — active rows,
			// cursor — is queued for batched deletion once the commit
			// succeeds. The record is assembled the way the reference
			// store's would be after this transition (rows, then the
			// transition's ops, then its cursor's in-flight overlay) and
			// compacted by the shared rule, so the failed unwinds it
			// keeps are the same ones the model keeps.
			rec, err := getNonterminal(tx, id)
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
	finish(err)
	if err == nil && t.Outcome != nil {
		s.enqueueStage(id)
	}
	return err
}

// putTerminal writes the terminal row and its expiry index key, which
// orders terminal runs by commit time for reap.
func putTerminal(tx *bolt.Tx, rec *driver.RunRecord) error {
	b, err := storagepb.MarshalTerminal(rec)
	if err != nil {
		return err
	}
	if err := tx.Bucket(terminalBucket).Put([]byte(rec.RunID), b); err != nil {
		return err
	}
	return tx.Bucket(expiryBucket).Put(expiryKey(rec.UpdatedAt, rec.RunID), []byte{})
}

// expiryPrefix encodes a commit time as 8 big-endian bytes so keys sort
// by it; the zero time encodes as zero and sorts first.
func expiryPrefix(t time.Time) []byte {
	var n uint64
	if !t.IsZero() {
		n = uint64(t.UnixNano())
	}
	return binary.BigEndian.AppendUint64(nil, n)
}

func expiryKey(t time.Time, id kernel.RunID) []byte {
	return append(expiryPrefix(t), id...)
}

// deleteNonterminalStage removes a run's active rows (the input's nested
// bucket among them) and cursor. Active rows hang off the run id; they
// are collected before deletion since bbolt forbids mutating a bucket
// while iterating it. Deleting nothing is fine: a run may be drained
// twice, or reaped before its drain.
func deleteNonterminalStage(tx *bolt.Tx, id kernel.RunID) error {
	active := tx.Bucket(activeBucket)
	prefix := runPrefix(id)
	c := active.Cursor()
	var keys [][]byte
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		var err error
		if k[len(prefix)] == tagInput {
			err = active.DeleteBucket(k)
		} else {
			err = active.Delete(k)
		}
		if err != nil {
			return err
		}
	}
	return tx.Bucket(cursorBucket).Delete([]byte(id))
}

// inputKey is the one key of a run's input bucket.
var inputKey = []byte{'i'}

// putInput stores the input as the sole value of a nested bucket under
// the run's input tag. A bucket larger than a quarter page gets pages of
// its own, so creating and deleting it writes the run's own pages plus
// one small entry in the active leaf — never the neighbours' inputs,
// which sharing a leaf node would rewrite on every insert and delete —
// and, living under the run prefix, it costs the terminality commit no
// extra tree.
func putInput(active *bolt.Bucket, id kernel.RunID, input []byte) error {
	ib, err := active.CreateBucket(activeKey(id, tagInput))
	if err != nil {
		if errors.Is(err, berrors.ErrBucketExists) {
			return fmt.Errorf("bbolt: input of run %s already written", id)
		}
		return err
	}
	return ib.Put(inputKey, input)
}

// findOp locates the run's row for step and phase, whatever its order.
// It returns nil when there is none.
func findOp(active *bolt.Bucket, id kernel.RunID, step kernel.StepID, phase kernel.Phase) (key, value []byte) {
	prefix := opPrefix(id)
	// Everything after the order: · step · phase. Compared as bytes so
	// the scan allocates nothing.
	suffix := append(append([]byte{0}, step...), 0, phaseByte(phase))
	c := active.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if len(k) == len(prefix)+4+len(suffix) && bytes.Equal(k[len(prefix)+4:], suffix) {
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
// on its stage. Terminal: the terminal record alone. Nonterminal: a
// prefix walk of the active rows (the input's bucket among them) and the
// cursor with its in-flight operation overlaid as an unresolved step
// entry.
func getRun(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	// The terminal row is authoritative: the stage may linger until it
	// drains.
	if tb := tx.Bucket(terminalBucket).Get([]byte(id)); tb != nil {
		rec := &driver.RunRecord{}
		if err := storagepb.UnmarshalTerminalInto(tb, rec); err != nil {
			return nil, err
		}
		return rec, nil
	}
	return getNonterminal(tx, id)
}

// getNonterminal reads a nonterminal run from one prefix walk of its
// active rows — the meta row sorts first, so its absence is
// ErrRunNotFound — and its cursor.
func getNonterminal(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	rec := &driver.RunRecord{}
	prefix := runPrefix(id)
	active := tx.Bucket(activeBucket)
	c := active.Cursor()
	k, v := c.Seek(prefix)
	if k == nil || !bytes.HasPrefix(k, prefix) || k[len(prefix)] != tagMeta {
		return nil, kernel.ErrRunNotFound
	}
	if err := storagepb.UnmarshalRunMetaInto(v, rec); err != nil {
		return nil, err
	}
	for k, v = c.Next(); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		_, tag, rest, ok := splitActiveKey(k)
		if !ok {
			return nil, fmt.Errorf("bbolt: malformed active key %q", k)
		}
		switch tag {
		case tagInput:
			if ib := active.Bucket(k); ib != nil {
				rec.Input = bytes.Clone(ib.Get(inputKey))
			}
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
		// The expiry index orders terminal runs by commit time: the walk
		// starts at the oldest and stops at the first run that has not
		// expired, so reap costs its victims and decodes nothing.
		cutoff := expiryPrefix(before)
		expiry := tx.Bucket(expiryBucket)
		var victims [][]byte
		c := expiry.Cursor()
		for k, _ := c.First(); k != nil && len(victims) < limit && bytes.Compare(k[:8], cutoff) < 0; k, _ = c.Next() {
			victims = append(victims, bytes.Clone(k))
		}
		for _, k := range victims {
			id := k[8:]
			// A terminal run is one row, but a retention window shorter
			// than the stage drain could reap a run whose stage is still
			// queued; deleting an absent stage costs one seek.
			if err := deleteNonterminalStage(tx, kernel.RunID(id)); err != nil {
				return err
			}
			if err := tx.Bucket(terminalBucket).Delete(id); err != nil {
				return err
			}
			if err := expiry.Delete(k); err != nil {
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

// Close stops the sweep, drains the queued stages, and closes the
// database.
func (s *Store) Close() error {
	var err error
	s.once.Do(func() {
		close(s.stop)
		<-s.done
		err = s.Drain()
		if cerr := s.db.Close(); err == nil {
			err = cerr
		}
	})
	return err
}

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
