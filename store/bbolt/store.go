// Package bbolt implements driver.Store on a local bbolt database and
// registers the "bbolt" scheme with package store. Importing it alongside
// go.etcd.io/bbolt needs an alias for one of the two.
//
// bbolt's file lock provides the exclusive single-engine ownership the
// durable v1 model requires: a second process opening the same database
// blocks (or times out) rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec; this
// implementation stores a run in six buckets, chosen by write cadence.
// With · for the NUL separator, which the Store contract keeps out of
// every identifier, and R for a run id:
//
//	bucket: active                     append-only facts of nonterminal runs
//	  R·M                              -> RunMeta          write-once, refused twice
//	  R·c                              -> CancelRequest    write-once, first cancel wins
//	  R·f                              -> FailureRecord    write-once, refused twice
//	  R·i                              -> input            blob: row or nested bucket
//	  R·o·<order:4 BE>·<step>·<phase>  -> OperationRecord  one row per step and phase
//	                                      phase byte: f forward, u unwind
//	  R·o·<order:4 BE>·<step>·f·s      -> state            a large one, beside its row;
//	                                                       a small one is in the record
//
//	bucket: cursor                     the one mutable row
//	  R                                -> Cursor           rewritten on every attempt
//
//	bucket: terminal                   the whole of a terminal run
//	  R                                -> Terminal         identity, annotations, phase,
//	                                                       outcome, committed_at,
//	                                                       failure, cancel, failed_unwinds
//	  R·O                              -> output           a large one, beside the row;
//	                                                       a small one is in the record
//
//	bucket: expiry                     retention order of terminal runs
//	  <committed_at:8 BE>·R            -> (empty)          written with the terminal row
//
//	bucket: staged                     terminal runs whose stage is still on disk
//	  R                                -> (empty)          written with the terminal row,
//	                                                       deleted with the stage
//
//	bucket: slots                      admission index
//	  <pipeline>·<resource>            -> R                held from CreateRun to terminality
//
// A run moves through it like this. CreateRun writes R·M, the input
// blob when there is one, the cursor row, and the slot, in one
// transaction. Each attempt rewrites only the cursor. Each resolution
// appends one R·o row, a large state beside it; an unresolved operation
// displaced by a topology change is flushed at order zero and moves to
// its real order when it resolves, the one delete before terminality. Cancel and failure land
// as R·c and R·f. The terminality commit writes the terminal row —
// from then on the run reads from that row alone, so the input and
// step states, folded into the output by then, are released when the
// run ends rather than when retention reaps it — deletes the slot, and
// stages R. The drain later deletes everything under R· in active, the
// input bucket included, plus the cursor row and the staged key, in
// batches (see Store), because deleting adjacent runs together lets
// bbolt free whole leaves instead of rewriting one per run. Reap walks
// the expiry index from its oldest key and stops at the first run that
// has not expired, deleting each victim's terminal row and index key:
// proportional to the victims, decoding nothing. The terminal row is authoritative wherever both
// stages exist, which is what makes the deferred drain safe.
//
// What each choice bought. Tags sort M, c, f, i, o, so one prefix walk
// reads a run in the order a reader wants it, with operations in
// resolution order and no sorting. The variable-length values — input,
// state, output — follow one rule: a small one stays where it is (the
// input as the row's value, a state or output inside its record), a
// large one is a nested bucket of its own beside the row. bbolt
// rewrites a whole leaf node on any write to it, so a large value must
// never share a node with rows that change or with other large values
// that come and go; a bucket over a quarter page gets pages of its own,
// so writing and deleting the blob touches its own pages plus one small
// entry in the parent leaf. A value of half a page or less stays put:
// it shares leaves with its neighbours as cheaply as before, a bucket
// would cost a page to write and an open to read, and a row of its own
// beside the record sat on the leaf split boundary and got rewritten by
// the next resolution (measured). Half a page is bbolt's node split
// threshold, computed from the page size at Open. Operation rows are plain rows because a nested
// bucket costs a page per write for anything appended to (every write
// into a child bucket rewrites the child's page and the parent's entry
// for it; measured, and rejected, for a bucket per run and for a bucket
// per run's operations). The cursor is its own bucket because it is the
// only row rewritten, so per-attempt write volume is the cursor's,
// independent of input and state sizes. Terminal is one row because
// everything a caller can still reach lives in it. The slots index
// holds every nonterminal run: CreateRun admits against it,
// GetActiveRunID reads it, and ListNonterminal walks it, so recovery
// cost follows the runs in flight rather than the retained history.
//
// The blobs a nonterminal run carries — its input and the states kept
// beside their rows — never change once written, so the store keeps
// copies of them in memory for the runs in flight (see blobCache) and a
// read of a run costs one copy per blob instead of a seek, a bucket
// open, and a clone from the file. Entries are filled by the writes
// that store the blobs, dropped at terminality, and refilled from the
// file on the first read after a restart; past its limit the least
// recently used runs leave and read from the file on their next use. A
// second instance holds the large outputs of terminal runs for the read
// that follows Wait, dropped at reap. Both are bounded in bytes by
// Open's options (WithBlobCache, WithOutputCache) or the URI's query
// (see Scheme).
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
	stagedBucket   = []byte("staged")
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

// stateSuffix follows an operation key to address the state blob written
// beside it, so the row and its state share a leaf.
var stateSuffix = []byte{0, 's'}

// blobKey is the one key of a blob's nested bucket.
var blobKey = []byte{'b'}

// Store is a driver.Store backed by a bbolt database file.
type Store struct {
	db *bolt.DB
	// pending counts in-flight ApplyTransition calls for adaptive group
	// commit: a lone caller commits immediately, concurrent callers
	// coalesce into shared transactions.
	pending atomic.Int64

	// staged counts the terminal runs whose nonterminal stage is still
	// on disk — the size of the staged bucket, kept in memory as a hint
	// for the write path. Deleting one run's rows per transaction costs
	// a copied root-to-leaf path and a rewritten leaf each time; deleting
	// a batch of adjacent runs empties whole leaves, which bbolt frees
	// without writing. So terminality only stages, and the stage is
	// deleted in batches: inside the next write transaction once a full
	// batch is staged (see drainStaged), by the sweep goroutine on
	// stageDrainInterval when writes are idle, at Close, and at Open,
	// which drains whatever a crash left staged. The bucket is the
	// truth; the counter only decides whether a write bothers to look.
	staged atomic.Int64

	// blobRowMax is the largest value kept in place — the input as a row,
	// a state or output inside its record; anything larger becomes a
	// nested bucket of its own beside the row. It is half a page, bbolt's
	// node split threshold: a value past it forces its leaf node to split
	// and rides alone through every later rewrite.
	blobRowMax int
	// blobs caches the immutable blobs of the runs in flight and outputs
	// the large outputs of terminal runs; see blobCache.
	blobs   *blobCache
	outputs *blobCache
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

// Option configures a Store at Open.
type Option func(*config)

type config struct {
	blobCache, outputCache int
}

// WithBlobCache bounds, in bytes, the in-memory cache of the blobs of
// the runs in flight — each run's input and the states kept beside their
// rows — which serves reads of a nonterminal run without touching the
// file. Past the limit the least recently used runs leave the cache and
// read from the file on their next use. Zero disables the cache. The
// default is DefaultBlobCache.
func WithBlobCache(limit int) Option { return func(c *config) { c.blobCache = limit } }

// WithOutputCache bounds, in bytes, the in-memory cache of the large
// outputs of terminal runs, which serves the read that follows Wait
// without touching the file. Terminal runs live until reap, so this
// cache evicts least recently used entries past the limit. Zero disables
// it. The default is DefaultOutputCache.
func WithOutputCache(limit int) Option { return func(c *config) { c.outputCache = limit } }

// Open opens (creating if needed) the database at path. It fails if another
// process holds the file lock, enforcing exclusive ownership.
func Open(path string, opts ...Option) (*Store, error) {
	cfg := config{blobCache: DefaultBlobCache, outputCache: DefaultOutputCache}
	for _, opt := range opts {
		opt(&cfg)
	}
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
		for _, name := range [][]byte{activeBucket, cursorBucket, terminalBucket, expiryBucket, stagedBucket, slotsBucket} {
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
	s := &Store{db: db, stop: make(chan struct{}), done: make(chan struct{}), blobRowMax: blobRowMaxFor(db.Info().PageSize), blobs: newBlobCache(cfg.blobCache), outputs: newBlobCache(cfg.outputCache)}
	// Whatever the previous process left staged is drained now; the
	// counter starts at zero and the sweep sees the bucket empty after.
	if err := s.Drain(); err != nil {
		db.Close()
		return nil, fmt.Errorf("bbolt: draining staged runs: %w", err)
	}
	go s.sweep()
	return s, nil
}

// drainStaged deletes the stages of up to n staged runs inside tx,
// walking the staged bucket from its first key, and reports how many it
// took. It reads the bucket afresh on every call, so a closure Batch
// runs twice does no harm, and a run staged twice or reaped before its
// drain costs a seek.
func (s *Store) drainStaged(tx *bolt.Tx, n int) (int, error) {
	staged := tx.Bucket(stagedBucket)
	var ids [][]byte
	c := staged.Cursor()
	for k, _ := c.First(); k != nil && len(ids) < n; k, _ = c.Next() {
		ids = append(ids, bytes.Clone(k))
	}
	for _, id := range ids {
		if err := deleteNonterminalStage(tx, kernel.RunID(id)); err != nil {
			return 0, err
		}
		if err := staged.Delete(id); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

// writeDrain is the drain a caller's write transaction carries: a full
// batch when at least that many runs are staged, nothing otherwise —
// the idle path allocates nothing.
func (s *Store) writeDrain(tx *bolt.Tx) error {
	if s.staged.Load() < stageDrainPerWrite {
		return nil
	}
	n, err := s.drainStaged(tx, stageDrainPerWrite)
	if err == nil {
		s.staged.Add(int64(-n))
	}
	return err
}

// Drain deletes the nonterminal stage of every staged terminal run now,
// in bounded transactions. The store drains on its own — inside write
// transactions, on a timer, at Open, and at Close — so callers need it
// only to observe the on-disk state deterministically.
func (s *Store) Drain() error {
	for {
		var n int
		err := s.db.Update(func(tx *bolt.Tx) error {
			var err error
			n, err = s.drainStaged(tx, stageDrainPerSweep)
			return err
		})
		if err != nil {
			return err
		}
		s.staged.Add(int64(-n))
		if n < stageDrainPerSweep {
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
			if s.staged.Load() > 0 {
				_ = s.Drain() // a failed sweep leaves the bucket; the next tick retries
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
	err := commit(func(tx *bolt.Tx) error {
		if err := s.writeDrain(tx); err != nil {
			return err
		}
		slots := tx.Bucket(slotsBucket)
		// rec's own slot first, then every group member's: the first
		// occupant found is the blocker reported to the caller.
		if activeID := slots.Get(slotKey(rec)); activeID != nil {
			var err error
			existing, err = s.getRun(tx, kernel.RunID(activeID))
			return err
		}
		for _, p := range excluding {
			if p == rec.PipelineID {
				continue
			}
			if activeID := slots.Get(slotKeyFor(p, rec.ResourceID)); activeID != nil {
				var err error
				existing, err = s.getRun(tx, kernel.RunID(activeID))
				return err
			}
		}
		if err := s.putRun(tx, rec); err != nil {
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
	if created {
		s.blobs.setInput(rec.RunID, rec.Input)
		for sid, sr := range rec.Steps {
			if len(sr.Forward.State) > s.blobRowMax {
				s.blobs.setState(rec.RunID, sid, sr.Forward.State)
			}
		}
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
func (s *Store) putRun(tx *bolt.Tx, rec *driver.RunRecord) error {
	if rec.Outcome != nil {
		c := rec.Clone()
		c.CompactTerminal()
		return s.putTerminal(tx, c)
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
		if err := s.putBlob(active, activeKey(rec.RunID, tagInput), rec.Input); err != nil {
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
				if err := s.putOp(tx, rec.RunID, sid, phase, op); err != nil {
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
	err := commit(func(tx *bolt.Tx) error {
		if err := s.writeDrain(tx); err != nil {
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
			// The terminality commit: one terminal record is written, the
			// run is staged, and the slot is released; the nonterminal
			// stage — active rows, cursor — is deleted later in a batch. The record is assembled the way the reference
			// store's would be after this transition (rows, then the
			// transition's ops, then its cursor's in-flight overlay) and
			// compacted by the shared rule, so the failed unwinds it
			// keeps are the same ones the model keeps.
			rec, err := s.getNonterminal(tx, id)
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
			if err := s.putTerminal(tx, rec); err != nil {
				return err
			}
			if err := tx.Bucket(stagedBucket).Put([]byte(id), []byte{}); err != nil {
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
			if err := s.putOp(tx, id, ow.StepID, ow.Phase, &op); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if t.Outcome != nil {
		s.staged.Add(1)
		s.blobs.drop(id)
		if len(t.Output) > s.blobRowMax {
			s.outputs.setOutput(id, t.Output)
		}
		return nil
	}
	// A forward row written with a state beside it caches that state;
	// one written without drops whatever the step had.
	for _, ow := range t.Ops {
		if ow.Phase != kernel.PhaseForward {
			continue
		}
		if len(ow.Record.State) > s.blobRowMax {
			s.blobs.setState(id, ow.StepID, ow.Record.State)
		} else {
			s.blobs.setState(id, ow.StepID, nil)
		}
	}
	return nil
}

// putTerminal writes the terminal row, its output blob when there is
// one, and its expiry index key, which orders terminal runs by commit
// time for reap.
func (s *Store) putTerminal(tx *bolt.Tx, rec *driver.RunRecord) error {
	beside := len(rec.Output) > s.blobRowMax
	b, err := storagepb.MarshalTerminal(rec, beside)
	if err != nil {
		return err
	}
	terminal := tx.Bucket(terminalBucket)
	if err := terminal.Put([]byte(rec.RunID), b); err != nil {
		return err
	}
	if beside {
		if err := s.putBlob(terminal, outputKey(rec.RunID), rec.Output); err != nil {
			return err
		}
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

// blobRowMaxFor is the largest value kept in place: half a page, the
// size at which bbolt splits a leaf node (its default fill is 50%).
// Measured on the perf suite: at bbolt's inline-bucket limit instead
// (a quarter page) the suite's 1 KB states all became buckets, a page
// each and a bucket open per read, for 20% more bytes in Recovery and
// 20 to 80% more allocations everywhere; as rows of their own beside
// the record they cost 7 to 13% more bytes than in it.
func blobRowMaxFor(pageSize int) int {
	return pageSize / 2
}

// stateKey addresses the state blob beside the operation row at opKey.
func stateKey(opKey []byte) []byte {
	return append(bytes.Clone(opKey), stateSuffix...)
}

// outputKey addresses a terminal run's output blob, beside its row.
func outputKey(id kernel.RunID) []byte {
	return append(append([]byte(id), 0), 'O')
}

// putBlob stores v under key in b: as the row's value when it is at
// most blobRowMax, otherwise as a nested bucket holding the one value,
// which gets pages of its own. Nothing may be under key already; a
// caller replacing a blob deletes it first.
func (s *Store) putBlob(b *bolt.Bucket, key, v []byte) error {
	if len(v) <= s.blobRowMax {
		return b.Put(key, v)
	}
	nb, err := b.CreateBucket(key)
	if err != nil {
		return err
	}
	return nb.Put(blobKey, v)
}

// getBlob returns a copy of the blob under key, nil when there is none:
// one seek, and a bucket open only when the key is there as a bucket.
func getBlob(b *bolt.Bucket, key []byte) []byte {
	k, v := b.Cursor().Seek(key)
	if !bytes.Equal(k, key) {
		return nil
	}
	return blobAt(b, key, v)
}

// blobAt returns a copy of the blob a cursor stopped on: v when the key
// is a row, the bucket's value when it is not.
func blobAt(b *bolt.Bucket, key, v []byte) []byte {
	if v != nil {
		return bytes.Clone(v)
	}
	return blobFromBucket(b, key)
}

func blobFromBucket(b *bolt.Bucket, key []byte) []byte {
	if nb := b.Bucket(key); nb != nil {
		return bytes.Clone(nb.Get(blobKey))
	}
	return nil
}

// deleteBlob removes whatever is under key, row or bucket; nothing is
// fine.
func deleteBlob(b *bolt.Bucket, key []byte) error {
	if err := b.Delete(key); errors.Is(err, berrors.ErrIncompatibleValue) {
		return b.DeleteBucket(key)
	} else if err != nil {
		return err
	}
	return nil
}

// deleteNonterminalStage removes a run's active rows (blob buckets among
// them) and cursor. Active rows hang off the run id; they are collected
// before deletion since bbolt forbids mutating a bucket while iterating
// it. Deleting nothing is fine: a run may be reaped before its drain.
func deleteNonterminalStage(tx *bolt.Tx, id kernel.RunID) error {
	active := tx.Bucket(activeBucket)
	prefix := runPrefix(id)
	c := active.Cursor()
	var keys [][]byte
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		if err := deleteBlob(active, k); err != nil {
			return err
		}
	}
	return tx.Bucket(cursorBucket).Delete([]byte(id))
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

// putOp writes the run's one row for the operation and, for a forward
// operation carrying committed state, the state blob beside it. A row
// already there under another order — an unresolved flush resolving to
// its real order, or the contract's upsert — is removed so the step and
// phase keep one row.
func (s *Store) putOp(tx *bolt.Tx, id kernel.RunID, step kernel.StepID, phase kernel.Phase, op *driver.OperationRecord) error {
	beside := phase == kernel.PhaseForward && len(op.State) > s.blobRowMax
	row := *op
	if beside {
		row.State = nil
	}
	b, err := storagepb.MarshalOperationRecord(&row)
	if err != nil {
		return err
	}
	active := tx.Bucket(activeBucket)
	key := opKey(id, op.Order, step, phase)
	old, _ := findOp(active, id, step, phase)
	if old != nil {
		old = bytes.Clone(old)
	}
	if old != nil && !bytes.Equal(old, key) {
		if err := active.Delete(old); err != nil {
			return err
		}
	}
	if err := active.Put(key, b); err != nil {
		return err
	}
	if phase != kernel.PhaseForward {
		return nil
	}
	// A state beside a row can only be there if a row was: replacing a
	// row replaces it, and a fresh row has none to delete.
	if old != nil {
		if err := deleteBlob(active, stateKey(old)); err != nil {
			return err
		}
	}
	if beside {
		return s.putBlob(active, stateKey(key), op.State)
	}
	return nil
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
func (s *Store) getRun(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	// The terminal row is authoritative: the stage may linger until it
	// drains.
	terminal := tx.Bucket(terminalBucket)
	if tb := terminal.Get([]byte(id)); tb != nil {
		rec := &driver.RunRecord{}
		beside, err := storagepb.UnmarshalTerminalInto(tb, rec)
		if err != nil {
			return nil, err
		}
		if beside {
			if out := s.outputs.output(id); out != nil {
				rec.Output = out
			} else {
				rec.Output = getBlob(terminal, outputKey(id))
				s.outputs.fill(id, &runBlobs{output: rec.Output})
			}
		}
		return rec, nil
	}
	return s.getNonterminal(tx, id)
}

// getNonterminal reads a nonterminal run from one prefix walk of its
// active rows — the meta row sorts first, so its absence is
// ErrRunNotFound — and its cursor.
func (s *Store) getNonterminal(tx *bolt.Tx, id kernel.RunID) (*driver.RunRecord, error) {
	rec := &driver.RunRecord{}
	// Blobs come from the cache when the run is in it; a run that is not
	// (a restart, or the cache was full) reads them from the file and,
	// on a cold miss, fills its entry.
	cachedInput, cached := s.blobs.input(id)
	var fill *runBlobs
	if !cached {
		fill = &runBlobs{}
	}
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
	// A state key follows its row: remembering the row's key and step
	// spares parsing the state key, and the string it would allocate.
	var lastOpKey []byte
	var lastStep kernel.StepID
	for k, v = c.Next(); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		_, tag, rest, ok := splitActiveKey(k)
		if !ok {
			return nil, fmt.Errorf("bbolt: malformed active key %q", k)
		}
		switch tag {
		case tagInput:
			if cachedInput != nil {
				rec.Input = cachedInput
			} else {
				rec.Input = blobAt(active, k, v)
				if fill != nil {
					fill.input = rec.Input
				}
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
			if bytes.HasSuffix(rest, stateSuffix) {
				var step kernel.StepID
				if rowKey := k[:len(k)-len(stateSuffix)]; bytes.Equal(rowKey, lastOpKey) {
					step = lastStep
				} else {
					var phase kernel.Phase
					var ok bool
					_, step, phase, ok = splitOpRest(rest[:len(rest)-len(stateSuffix)])
					if !ok || phase != kernel.PhaseForward {
						return nil, fmt.Errorf("bbolt: malformed state key %q", k)
					}
				}
				if st := s.blobs.state(id, step); st != nil {
					rec.Step(step).Forward.State = st
				} else {
					rec.Step(step).Forward.State = blobAt(active, k, v)
					if fill != nil {
						if fill.states == nil {
							fill.states = make(map[kernel.StepID][]byte)
						}
						fill.states[step] = rec.Step(step).Forward.State
					}
				}
				continue
			}
			_, step, phase, ok := splitOpRest(rest)
			if !ok {
				return nil, fmt.Errorf("bbolt: malformed operation key %q", k)
			}
			op, err := storagepb.UnmarshalOperationRecord(v)
			if err != nil {
				return nil, err
			}
			*rec.Step(step).Op(phase) = op
			lastOpKey, lastStep = k, step
		default:
			return nil, fmt.Errorf("bbolt: unknown active row tag %q in key %q", tag, k)
		}
	}

	if fill != nil {
		s.blobs.fill(id, fill)
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
		rec, err = s.getRun(tx, id)
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
			if err := deleteBlob(tx.Bucket(terminalBucket), outputKey(kernel.RunID(id))); err != nil {
				return err
			}
			if err := expiry.Delete(k); err != nil {
				return err
			}
			if err := tx.Bucket(stagedBucket).Delete(id); err != nil {
				return err
			}
			s.outputs.drop(kernel.RunID(id))
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
			rec, err := s.getRun(tx, kernel.RunID(v))
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
