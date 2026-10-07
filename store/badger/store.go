// Package badger implements driver.Store on a local Badger database and
// registers the "badger" scheme with package store. Importing it
// alongside github.com/dgraph-io/badger/v4 needs an alias for one of
// the two.
//
// It is its own Go module, github.com/dangra/durable/store/badger,
// released in lockstep with the core, so Badger's dependency graph
// enters only the binaries that import the driver.
//
// Badger's directory lock provides the exclusive single-engine
// ownership the durable v1 model requires: a second process opening
// the same directory fails rather than executing concurrently.
//
// The storage representation is implementation-defined by the spec.
// Badger is one ordered key space, so a namespace byte leads every
// key. With · for the NUL separator, which the Store contract keeps out
// of every identifier, and R for a run id:
//
//	a·R·M                    -> RunMeta          write-once, refused twice
//	a·R·c                    -> CancelRequest    write-once, first cancel wins
//	a·R·f                    -> FailureRecord    write-once, refused twice
//	a·R·i                    -> input            the blob is the value
//	a·R·o·<step>·<phase>     -> flag, OperationRecord
//	                                             one row per step and phase;
//	                                             phase byte f forward, u unwind;
//	                                             the flag byte says whether a
//	                                             state row sits beside it
//	a·R·s·<step>             -> state            a forward operation's
//	                                             committed state
//	c·R                      -> Cursor           the one mutable row,
//	                                             rewritten on every attempt
//	t·R                      -> Terminal         the whole of a terminal run,
//	                                             output in it
//	e·<committed_at:8 BE>·R  -> (empty)          retention order of terminal runs
//	s·<pipeline>·<resource>  -> R                admission index, held from
//	                                             CreateRun to terminality
//
// A run moves through it like this. CreateRun writes the meta, the
// input when there is one, the cursor, and the slot in one transaction.
// Each attempt rewrites only the cursor. Each resolution upserts the
// operation's one row and, for a forward operation with a state, its
// state row. Cancel and failure land as their rows. The terminality
// commit writes the terminal row and its expiry key and deletes the
// run's active rows, its cursor, and its slot, all in one transaction:
// from then on the run is one row, and the input and states, folded
// into the output by then, are released when the run ends rather than
// when retention reaps it. Reap walks the expiry index from its oldest
// key, stops at the first run that has not expired, and deletes each
// victim's terminal row and index key: proportional to the victims,
// decoding nothing.
//
// Reads. A run's head — what status, waiting, lookups, and recovery
// need — is the terminal row, or for a nonterminal run five point reads
// (the missed terminal row, then meta, cancel, failure, cursor),
// decoded in place. A whole run adds a walk of its operation rows alone
// and point reads of its input and states, which the blob cache serves
// for the runs in flight (see WithBlobCache): Badger's iterator copies
// every value it passes, so a walk over the whole run would copy the
// input on every read, and the engine reads a run whole each time its
// worker picks it up.
//
// Writes. Every write goes through one batching goroutine that commits
// whatever writes are queued in one transaction (see update): Badger in
// sync-writes mode syncs per committed transaction and shares nothing
// between concurrent ones, so this is the group commit bbolt's Batch
// gives the bbolt driver. With one writer, Badger's conflict detection
// has nothing left to catch. Prefix walks inside a write transaction
// read a read-only snapshot instead, because Badger sorts every pending
// write of the transaction — the whole batch — each time an iterator is
// opened in one; of a run's rows, only a cancel request can be pending
// in the same batch as its terminality commit, and that commit reads
// and deletes it by key.
//
// What changes against the bbolt driver. An LSM tree appends: a write
// costs its bytes once in the log and again per compaction, never a
// rewritten page, so nothing here keeps a large value off its leaf
// (Badger moves values past its threshold to the value log by itself),
// defers a run's deletion to a batch, or orders operation rows by
// resolution in the key — the record carries its order, and a reader
// folds rows into a map. The costs are a read, where a point lookup
// walks the memtables and then the levels behind bloom filters and the
// block cache, and space: deleted rows stay on disk until compaction
// rewrites the tables holding them, which level 0 does only once
// several tables pile up there. The store compacts level 0 at Close, so
// a store that has shed its history gives the space back at shutdown.
package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	bdg "github.com/dgraph-io/badger/v4"

	"github.com/dangra/durable/kernel"
	"github.com/dangra/durable/store/driver"
	"github.com/dangra/durable/store/internal/blobcache"
	"github.com/dangra/durable/store/internal/storagepb"
)

// Namespace bytes, the first byte of every key.
const (
	nsActive   byte = 'a'
	nsCursor   byte = 'c'
	nsTerminal byte = 't'
	nsExpiry   byte = 'e'
	nsSlots    byte = 's'
)

// Row tags of a run's active rows, in sort order.
const (
	tagMeta    byte = 'M'
	tagCancel  byte = 'c'
	tagFailure byte = 'f'
	tagInput   byte = 'i'
	tagOp      byte = 'o'
	tagState   byte = 's'
)

// Store is a driver.Store backed by a Badger database directory.
type Store struct {
	db *bdg.DB
	// writes feeds the batching writer (see update).
	writes     chan *writeReq
	writerDone chan struct{}
	// blobs caches the immutable inputs and states of the runs in
	// flight; see package blobcache.
	blobs *blobcache.Cache
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

// gcInterval is how often the value log is offered for garbage
// collection while the store is open. Values stay in the LSM tree up
// to the value threshold (see Open), so the value log holds only the
// largest inputs, states, and outputs and the pass is usually a no-op.
const gcInterval = time.Minute

// DefaultMemTableSize is the default size, in bytes, of Badger's
// in-memory write buffer (see WithMemTableSize).
const DefaultMemTableSize = 64 << 20

// Option configures a Store at Open.
type Option func(*config)

type config struct {
	syncWrites bool
	memTable   int64
	blobCache  int
	logger     *slog.Logger
}

// DefaultBlobCache is the default limit, in bytes, of the cache of the
// blobs of the runs in flight (see WithBlobCache).
const DefaultBlobCache = 64 << 20

// WithBlobCache bounds, in bytes, the in-memory cache of the inputs and
// committed states of the runs in flight, which serves reads of a
// nonterminal run without copying them out of the tree. A run is read
// whole each time its worker picks it up — after every retry backoff,
// every wake — so the cache is what keeps a retrying run with a large
// input from paying for it on every attempt. Past the limit the least
// recently used runs leave the cache. Zero disables it. The default is
// DefaultBlobCache.
func WithBlobCache(limit int) Option { return func(c *config) { c.blobCache = limit } }

// WithSyncWrites controls whether every commit is synced to disk before
// it returns, the way the bbolt driver commits. On by default: a
// durable fact is one the machine can lose power on. Concurrent writes
// share a commit and its syncs (see update), so the cost is per batch,
// not per write. Off, a commit returns once the write is in the OS, and
// the last moments before a power loss can roll back — a process crash
// loses nothing either way.
func WithSyncWrites(on bool) Option { return func(c *config) { c.syncWrites = on } }

// WithMemTableSize sets, in bytes, Badger's in-memory write buffer,
// allocated up front at Open and again as each fills. It also bounds a
// transaction: Badger refuses one past 15% of it, so a run whose
// terminality commit carries more than that in rows is an error. The
// default is DefaultMemTableSize; tests opening many stores use a
// smaller one.
func WithMemTableSize(n int64) Option { return func(c *config) { c.memTable = n } }

// WithLogger routes Badger's own diagnostics — compaction, value-log
// garbage collection, recovery — to l at the matching levels. Without
// it they are discarded.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// Open opens (creating if needed) the database in dir. It fails if
// another process holds the directory lock, enforcing exclusive
// ownership.
func Open(dir string, opts ...Option) (*Store, error) {
	cfg := config{syncWrites: true, memTable: DefaultMemTableSize, blobCache: DefaultBlobCache}
	for _, opt := range opts {
		opt(&cfg)
	}
	// The value threshold sends values past it to the value log; it
	// must stay under Badger's transaction limit (15% of the memtable),
	// so it follows the memtable down.
	threshold := min(int64(1<<20), cfg.memTable/16)
	bo := bdg.DefaultOptions(dir).
		WithSyncWrites(cfg.syncWrites).
		WithMemTableSize(cfg.memTable).
		WithValueThreshold(threshold).
		// A run's stage is deleted at terminality and its history at
		// reap, but the space comes back only when compaction rewrites
		// the tables holding them, and level 0 is compacted only once
		// several tables pile up there, so a quiet store keeps what it
		// deleted. Compacting level 0 at Close returns it at shutdown,
		// at the cost of a slower Close.
		WithCompactL0OnClose(true).
		WithLogger(nil)
	if cfg.logger != nil {
		bo = bo.WithLogger(slogger{cfg.logger})
	}
	db, err := bdg.Open(bo)
	if err != nil {
		return nil, fmt.Errorf("badger: opening %s: %w", dir, err)
	}
	s := &Store{db: db, writes: make(chan *writeReq, maxBatch), writerDone: make(chan struct{}),
		blobs: blobcache.New(cfg.blobCache), stop: make(chan struct{}), done: make(chan struct{})}
	go s.writer()
	go s.gc()
	return s, nil
}

// gc offers the value log for garbage collection on a timer; Badger
// rewrites a log file only when enough of it is dead.
func (s *Store) gc() {
	defer close(s.done)
	t := time.NewTicker(gcInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			for s.db.RunValueLogGC(0.5) == nil {
			}
		}
	}
}

// slogger adapts Badger's logger interface to log/slog.
type slogger struct{ l *slog.Logger }

func (s slogger) Errorf(f string, a ...any) { s.l.Error(strings.TrimRight(fmt.Sprintf(f, a...), "\n")) }
func (s slogger) Warningf(f string, a ...any) {
	s.l.Warn(strings.TrimRight(fmt.Sprintf(f, a...), "\n"))
}
func (s slogger) Infof(f string, a ...any)  { s.l.Info(strings.TrimRight(fmt.Sprintf(f, a...), "\n")) }
func (s slogger) Debugf(f string, a ...any) { s.l.Debug(strings.TrimRight(fmt.Sprintf(f, a...), "\n")) }

// Keys.

func runPrefix(id kernel.RunID) []byte {
	k := make([]byte, 0, len(id)+2)
	return append(append(append(k, nsActive), id...), 0)
}

func activeKey(id kernel.RunID, tag byte) []byte { return append(runPrefix(id), tag) }

func opPrefix(id kernel.RunID) []byte { return append(activeKey(id, tagOp), 0) }

func opKey(id kernel.RunID, step kernel.StepID, phase kernel.Phase) []byte {
	k := opPrefix(id)
	k = append(k, step...)
	return append(k, 0, phaseByte(phase))
}

func phaseByte(phase kernel.Phase) byte {
	if phase == kernel.PhaseUnwind {
		return 'u'
	}
	return 'f'
}

// splitOpRest recovers step and phase from the bytes after an operation
// row's tag: · step · phase.
func splitOpRest(rest []byte) (step kernel.StepID, phase kernel.Phase, ok bool) {
	if len(rest) < 1+1+2 || rest[0] != 0 || rest[len(rest)-2] != 0 {
		return "", 0, false
	}
	phase = kernel.PhaseForward
	if rest[len(rest)-1] == 'u' {
		phase = kernel.PhaseUnwind
	}
	return kernel.StepID(rest[1 : len(rest)-2]), phase, true
}

// An operation row's value is one flag byte, telling whether a state row
// sits beside it, then the encoded record. The flag is what lets a read
// fetch states by point reads, from the blob cache when it has them,
// instead of walking past rows: Badger's iterator copies every value it
// passes.
const (
	opNoState  byte = 0
	opHasState byte = 1
)

// stateKey addresses a forward operation's committed state, a row of
// its own beside the operation's so a read can take it from the blob
// cache instead of copying it: a·R·s·<step>.
func stateKey(id kernel.RunID, step kernel.StepID) []byte {
	return append(append(activeKey(id, tagState), 0), step...)
}

func cursorKey(id kernel.RunID) []byte   { return append([]byte{nsCursor}, id...) }
func terminalKey(id kernel.RunID) []byte { return append([]byte{nsTerminal}, id...) }

// expiryPrefix encodes a commit time as 8 big-endian bytes so keys sort
// by it; the zero time encodes as zero and sorts first.
func expiryPrefix(t time.Time) []byte {
	var n uint64
	if !t.IsZero() {
		n = uint64(t.UnixNano())
	}
	return binary.BigEndian.AppendUint64([]byte{nsExpiry}, n)
}

func expiryKey(t time.Time, id kernel.RunID) []byte { return append(expiryPrefix(t), id...) }

// slotKey joins the slot pair with NUL, which the Store contract
// guarantees appears in no identifier — otherwise distinct
// (pipeline, resource) pairs could alias one key.
func slotKey(pipeline kernel.PipelineID, resource kernel.ResourceID) []byte {
	k := make([]byte, 0, len(pipeline)+len(resource)+2)
	return append(append(append(append(k, nsSlots), pipeline...), 0), resource...)
}

// Reads.

// get returns a copy of the value under key, nil when there is none.
func get(txn *bdg.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if errors.Is(err, bdg.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

// view hands the value under key to fn without copying it, reporting
// whether the key is there. fn must not retain the slice; decoding it
// into a fresh value is the use.
func view(txn *bdg.Txn, key []byte, fn func([]byte) error) (bool, error) {
	item, err := txn.Get(key)
	if errors.Is(err, bdg.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, item.Value(fn)
}

// exists reports whether key is present, without reading its value.
func exists(txn *bdg.Txn, key []byte) (bool, error) {
	_, err := txn.Get(key)
	if errors.Is(err, bdg.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// keysWithPrefix collects the keys under prefix, copied, so a caller
// can mutate after the iterator is closed.
func keysWithPrefix(txn *bdg.Txn, prefix []byte, limit int) [][]byte {
	it := txn.NewIterator(bdg.IteratorOptions{Prefix: prefix})
	defer it.Close()
	var keys [][]byte
	for it.Seek(prefix); it.ValidForPrefix(prefix) && (limit <= 0 || len(keys) < limit); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	return keys
}

// getRun assembles the read model from the run's components,
// dispatching on its stage: the terminal row alone, or a prefix walk of
// the active rows and the cursor with its in-flight operation overlaid
// as an unresolved step entry.
func (s *Store) getRun(txn *bdg.Txn, id kernel.RunID, fill bool) (*driver.RunRecord, error) {
	tb, err := get(txn, terminalKey(id))
	if err != nil {
		return nil, err
	}
	if tb != nil {
		rec := &driver.RunRecord{}
		if _, err := storagepb.UnmarshalTerminalInto(tb, rec); err != nil {
			return nil, err
		}
		return rec, nil
	}
	return s.getNonterminal(txn, id, fill)
}

// getNonterminal reads a nonterminal run: its head rows by point reads
// — no meta row is ErrRunNotFound — its operation rows by a walk of
// their own prefix, and its input and states by point reads, from the
// blob cache when the run is in it. The walk passes only operation
// rows: Badger's iterator copies every value it passes, so walking past
// the input would copy it even on a cache hit. A read that finds blobs
// in the tree fills the cache only when fill is set, which callers
// inside a write transaction leave off, since what they read may not
// commit.
func (s *Store) getNonterminal(txn *bdg.Txn, id kernel.RunID, fill bool) (*driver.RunRecord, error) {
	meta, err := get(txn, activeKey(id, tagMeta))
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, kernel.ErrRunNotFound
	}
	rec := &driver.RunRecord{}
	if err := storagepb.UnmarshalRunMetaInto(meta, rec); err != nil {
		return nil, err
	}
	if err := readHeadRows(txn, rec); err != nil {
		return nil, err
	}
	cachedInput, cached := s.blobs.Input(id)
	var miss *blobcache.Blobs
	if !cached && fill {
		miss = &blobcache.Blobs{}
	}
	if cachedInput != nil {
		rec.Input = cachedInput
	} else if !cached {
		if rec.Input, err = get(txn, activeKey(id, tagInput)); err != nil {
			return nil, err
		}
		if miss != nil {
			miss.Input = rec.Input
		}
	}
	var withState []kernel.StepID
	prefix := opPrefix(id)
	it := txn.NewIterator(bdg.IteratorOptions{Prefix: prefix})
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		step, phase, ok := splitOpRest(k[len(prefix)-1:])
		if !ok {
			it.Close()
			return nil, fmt.Errorf("badger: malformed operation key %q", k)
		}
		var op driver.OperationRecord
		err := item.Value(func(v []byte) error {
			if len(v) == 0 {
				return fmt.Errorf("badger: empty operation row %q", k)
			}
			if v[0] == opHasState {
				withState = append(withState, step)
			}
			var err error
			op, err = storagepb.UnmarshalOperationRecord(v[1:])
			return err
		})
		if err != nil {
			it.Close()
			return nil, err
		}
		*rec.Step(step).Op(phase) = op
	}
	it.Close()
	for _, step := range withState {
		st := s.blobs.State(id, step)
		if st == nil {
			if st, err = get(txn, stateKey(id, step)); err != nil {
				return nil, err
			}
			if miss != nil && st != nil {
				if miss.States == nil {
					miss.States = make(map[kernel.StepID][]byte)
				}
				miss.States[step] = st
			}
		}
		rec.Step(step).Forward.State = st
	}
	if miss != nil {
		s.blobs.Fill(id, miss)
	}
	if err := overlayCursor(txn, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// readHeadRows reads the run's cancel and failure rows onto rec.
func readHeadRows(txn *bdg.Txn, rec *driver.RunRecord) error {
	if _, err := view(txn, activeKey(rec.RunID, tagCancel), func(v []byte) (err error) {
		rec.Cancel, err = storagepb.UnmarshalCancel(v)
		return err
	}); err != nil {
		return err
	}
	_, err := view(txn, activeKey(rec.RunID, tagFailure), func(v []byte) error {
		f, err := storagepb.UnmarshalFailureRecord(v)
		rec.Failure = &f
		return err
	})
	return err
}

// overlayCursor reads the run's cursor onto rec: its scheduling fields,
// and its in-flight operation as an unresolved step entry — the forward
// operation in PhaseForward, the unwind one in PhaseUnwind (the Cursor
// contract).
func overlayCursor(txn *bdg.Txn, rec *driver.RunRecord) error {
	var cur driver.Cursor
	found, err := view(txn, cursorKey(rec.RunID), func(v []byte) (err error) {
		cur, err = storagepb.UnmarshalCursor(v)
		return err
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("badger: run %s has no cursor", rec.RunID)
	}
	rec.Phase = cur.Phase
	rec.NextAttemptAt = cur.NextAttemptAt
	rec.LastError, rec.LastReason, rec.LastErrorAt = cur.LastError, cur.LastReason, cur.LastErrorAt
	rec.Awaiting = cur.Awaiting
	rec.Awaited = cur.Awaited
	rec.UpdatedAt = cur.UpdatedAt
	rec.StartedAt = cur.StartedAt
	if cur.StepID != "" {
		op := rec.Step(cur.StepID).Op(cur.Phase)
		op.Status = driver.OpUnresolved
		op.Attempts = cur.Attempts
	}
	return nil
}

// getHead reads a run's head in point reads: the terminal row, or the
// meta, cancel, and failure rows and the cursor. No operation row is
// decoded and no blob is touched.
func (s *Store) getHead(txn *bdg.Txn, id kernel.RunID) (*driver.RunRecord, error) {
	// Rows are decoded in place (see view): protobuf decoding copies
	// what it keeps, so the copies a get would make buy nothing here.
	rec := &driver.RunRecord{}
	terminal, err := view(txn, terminalKey(id), func(v []byte) error {
		_, err := storagepb.UnmarshalTerminalInto(v, rec)
		return err
	})
	if err != nil {
		return nil, err
	}
	if terminal {
		rec.Output = nil
		return rec, nil
	}
	found, err := view(txn, activeKey(id, tagMeta), func(v []byte) error {
		return storagepb.UnmarshalRunMetaInto(v, rec)
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, kernel.ErrRunNotFound
	}
	if err := readHeadRows(txn, rec); err != nil {
		return nil, err
	}
	if err := overlayCursor(txn, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// Writes.

// putOnce writes a write-once row, refusing to overwrite one.
func putOnce(txn *bdg.Txn, what string, id kernel.RunID, k, v []byte) error {
	if ok, err := exists(txn, k); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("badger: %s of run %s already written", what, id)
	}
	return txn.Set(k, v)
}

// putRun persists every present component of rec; used at creation (and
// for seeded records carrying pre-existing facts). A record seeded
// terminal is written in its terminal stage.
func putRun(txn *bdg.Txn, rec *driver.RunRecord) error {
	if rec.Outcome != nil {
		c := rec.Clone()
		c.CompactTerminal()
		return putTerminal(txn, c)
	}
	meta, err := storagepb.MarshalRunMeta(rec)
	if err != nil {
		return err
	}
	if err := putOnce(txn, "meta", rec.RunID, activeKey(rec.RunID, tagMeta), meta); err != nil {
		return err
	}
	if len(rec.Input) > 0 {
		if err := txn.Set(activeKey(rec.RunID, tagInput), rec.Input); err != nil {
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
		StartedAt:     rec.StartedAt,
	})
	if err != nil {
		return err
	}
	if err := txn.Set(cursorKey(rec.RunID), cursor); err != nil {
		return err
	}
	for sid, sr := range rec.Steps {
		for _, phase := range []kernel.Phase{kernel.PhaseForward, kernel.PhaseUnwind} {
			if op := sr.Op(phase); op.Status != driver.OpNone {
				if err := putOp(txn, rec.RunID, sid, phase, op); err != nil {
					return err
				}
			}
		}
	}
	if rec.Failure != nil {
		if err := putRootFailure(txn, rec.RunID, rec.Failure); err != nil {
			return err
		}
	}
	if rec.Cancel != nil {
		b, err := storagepb.MarshalCancel(rec.Cancel)
		if err != nil {
			return err
		}
		if err := putOnce(txn, "cancel request", rec.RunID, activeKey(rec.RunID, tagCancel), b); err != nil {
			return err
		}
	}
	return nil
}

// putOp upserts the run's one row for the operation and, for a forward
// operation, its state row: written when the record carries a state,
// deleted when it does not and one is there.
func putOp(txn *bdg.Txn, id kernel.RunID, step kernel.StepID, phase kernel.Phase, op *driver.OperationRecord) error {
	row := *op
	row.State = nil
	b, err := storagepb.MarshalOperationRecord(&row)
	if err != nil {
		return err
	}
	flag := opNoState
	if phase == kernel.PhaseForward && len(op.State) > 0 {
		flag = opHasState
	}
	if err := txn.Set(opKey(id, step, phase), append([]byte{flag}, b...)); err != nil {
		return err
	}
	if phase != kernel.PhaseForward {
		return nil
	}
	sk := stateKey(id, step)
	if len(op.State) > 0 {
		return txn.Set(sk, op.State)
	}
	if ok, err := exists(txn, sk); err != nil || !ok {
		return err
	}
	return txn.Delete(sk)
}

func putRootFailure(txn *bdg.Txn, id kernel.RunID, rf *kernel.Failure) error {
	b, err := storagepb.MarshalFailureRecord(*rf)
	if err != nil {
		return err
	}
	return putOnce(txn, "failure", id, activeKey(id, tagFailure), b)
}

// putTerminal writes the terminal row, output in it, and its expiry
// index key, which orders terminal runs by commit time for reap.
func putTerminal(txn *bdg.Txn, rec *driver.RunRecord) error {
	b, err := storagepb.MarshalTerminal(rec, false)
	if err != nil {
		return err
	}
	if err := txn.Set(terminalKey(rec.RunID), b); err != nil {
		return err
	}
	return txn.Set(expiryKey(rec.UpdatedAt, rec.RunID), nil)
}

// deleteNonterminalStage removes a run's active rows and cursor. The
// rows a run can have are known — meta, cancel, failure, input, its
// operation rows and their states — so only the operation rows are
// walked, in snap, a read-only snapshot of what is committed, and the
// rest are deleted by key: walking past the input would copy it.
// Deleting a key that is not there writes a tombstone and is fine. Of a
// run's rows, only the cancel request can have been added by another
// write in the same batch, and it is deleted by key.
func deleteNonterminalStage(txn, snap *bdg.Txn, id kernel.RunID) error {
	keys := [][]byte{activeKey(id, tagMeta), activeKey(id, tagCancel), activeKey(id, tagFailure), activeKey(id, tagInput), cursorKey(id)}
	prefix := opPrefix(id)
	for _, k := range keysWithPrefix(snap, prefix, 0) {
		keys = append(keys, k)
		if step, phase, ok := splitOpRest(k[len(prefix)-1:]); ok && phase == kernel.PhaseForward {
			keys = append(keys, stateKey(id, step))
		}
	}
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// The Store interface.

func (s *Store) CreateRun(_ context.Context, rec *driver.RunRecord, excluding []kernel.PipelineID) (*driver.RunRecord, bool, error) {
	var existing *driver.RunRecord
	created := false
	err := s.update(func(txn *bdg.Txn) error {
		existing, created = nil, false
		// rec's own slot first, then every group member's: the first
		// occupant found is the blocker reported to the caller.
		activeID, err := get(txn, slotKey(rec.PipelineID, rec.ResourceID))
		if err != nil {
			return err
		}
		for _, p := range excluding {
			if activeID != nil {
				break
			}
			if p == rec.PipelineID {
				continue
			}
			if activeID, err = get(txn, slotKey(p, rec.ResourceID)); err != nil {
				return err
			}
		}
		if activeID != nil {
			// The occupant is read from a snapshot, as at terminality;
			// one created earlier in this same batch is only in txn.
			snap := s.db.NewTransaction(false)
			defer snap.Discard()
			existing, err = s.getRun(snap, kernel.RunID(activeID), false)
			if errors.Is(err, kernel.ErrRunNotFound) {
				existing, err = s.getRun(txn, kernel.RunID(activeID), false)
			}
			return err
		}
		if err := putRun(txn, rec); err != nil {
			return err
		}
		if err := txn.Set(slotKey(rec.PipelineID, rec.ResourceID), []byte(rec.RunID)); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if created {
		s.blobs.SetInput(rec.RunID, rec.Input)
		for sid, sr := range rec.Steps {
			if len(sr.Forward.State) > 0 {
				s.blobs.SetState(rec.RunID, sid, sr.Forward.State)
			}
		}
	}
	return existing, created, nil
}

func (s *Store) ApplyTransition(_ context.Context, id kernel.RunID, t driver.Transition) error {
	if err := s.update(func(txn *bdg.Txn) error { return s.applyTransition(txn, id, t) }); err != nil {
		return err
	}
	if t.Outcome != nil {
		s.blobs.Drop(id)
		return nil
	}
	// A forward row written with a state caches it; one written without
	// drops whatever the step had.
	for _, ow := range t.Ops {
		if ow.Phase == kernel.PhaseForward {
			s.blobs.SetState(id, ow.StepID, ow.Record.State)
		}
	}
	return nil
}

// applyTransition is ApplyTransition's body in a caller's transaction.
func (s *Store) applyTransition(txn *bdg.Txn, id kernel.RunID, t driver.Transition) error {
	{
		// A terminal run accepts no further transitions, and the engine
		// never sends one.
		if ok, err := exists(txn, terminalKey(id)); err != nil {
			return err
		} else if ok {
			return kernel.ErrRunTerminal
		}
		if ok, err := exists(txn, activeKey(id, tagMeta)); err != nil {
			return err
		} else if !ok {
			return kernel.ErrRunNotFound
		}
		if t.Outcome != nil {
			// The terminality commit: the record is assembled the way the
			// reference store's would be after this transition (rows, then
			// the transition's ops, then its cursor's in-flight overlay)
			// and compacted by the shared rule, so the failed unwinds it
			// keeps are the same ones the model keeps; then one terminal
			// row is written and the nonterminal stage and the slot are
			// deleted with it. The cancel row is point-read first so a
			// concurrent RequestCancel conflicts with this commit (see
			// the package doc): the prefix walk alone would not see it.
			cancelRow, err := get(txn, activeKey(id, tagCancel))
			if err != nil {
				return err
			}
			// The stage is walked in a read-only snapshot: an iterator
			// in a write transaction sorts every pending write of the
			// batch, so walking there costs the batch, not the run (see
			// readSnapshot). Of the run's rows, only a cancel request
			// can be pending in this batch, and it was point-read above.
			snap := s.db.NewTransaction(false)
			defer snap.Discard()
			rec, err := s.getNonterminal(snap, id, false)
			if err != nil {
				return err
			}
			if rec.Cancel == nil && cancelRow != nil {
				if rec.Cancel, err = storagepb.UnmarshalCancel(cancelRow); err != nil {
					return err
				}
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
			rec.StartedAt = t.Cursor.StartedAt
			oc := *t.Outcome
			rec.Outcome = &oc
			rec.Output = t.Output
			rec.CompactTerminal()
			if err := putTerminal(txn, rec); err != nil {
				return err
			}
			if err := deleteNonterminalStage(txn, snap, id); err != nil {
				return err
			}
			key := slotKey(rec.PipelineID, rec.ResourceID)
			if active, err := get(txn, key); err != nil {
				return err
			} else if string(active) == string(id) {
				return txn.Delete(key)
			}
			return nil
		}
		if t.Failure != nil {
			if err := putRootFailure(txn, id, t.Failure); err != nil {
				return err
			}
		}
		cursor, err := storagepb.MarshalCursor(t.Cursor)
		if err != nil {
			return err
		}
		if err := txn.Set(cursorKey(id), cursor); err != nil {
			return err
		}
		for _, ow := range t.Ops {
			op := ow.Record
			if err := putOp(txn, id, ow.StepID, ow.Phase, &op); err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *Store) GetRun(_ context.Context, id kernel.RunID) (*driver.RunRecord, error) {
	var rec *driver.RunRecord
	err := s.db.View(func(txn *bdg.Txn) error {
		var err error
		rec, err = s.getRun(txn, id, true)
		return err
	})
	return rec, err
}

func (s *Store) GetRunHead(_ context.Context, id kernel.RunID) (*driver.RunRecord, error) {
	var rec *driver.RunRecord
	err := s.db.View(func(txn *bdg.Txn) error {
		var err error
		rec, err = s.getHead(txn, id)
		return err
	})
	return rec, err
}

func (s *Store) ReapTerminal(_ context.Context, before time.Time, limit int) (int, error) {
	deleted := 0
	err := s.update(func(txn *bdg.Txn) error {
		deleted = 0
		// The expiry index orders terminal runs by commit time: the walk
		// starts at the oldest and stops at the first run that has not
		// expired, so reap costs its victims and decodes nothing.
		cutoff := expiryPrefix(before)
		prefix := []byte{nsExpiry}
		it := txn.NewIterator(bdg.IteratorOptions{Prefix: prefix})
		var victims [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(victims) < limit; it.Next() {
			k := it.Item().Key()
			if bytes.Compare(k[:len(cutoff)], cutoff) >= 0 {
				break
			}
			victims = append(victims, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, k := range victims {
			id := kernel.RunID(k[len(cutoff):])
			if err := txn.Delete(terminalKey(id)); err != nil {
				return err
			}
			if err := txn.Delete(k); err != nil {
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
	err := s.update(func(txn *bdg.Txn) error {
		accepted = false
		if ok, err := exists(txn, terminalKey(id)); err != nil {
			return err
		} else if ok {
			return kernel.ErrRunTerminal
		}
		if ok, err := exists(txn, activeKey(id, tagMeta)); err != nil {
			return err
		} else if !ok {
			return kernel.ErrRunNotFound
		}
		key := activeKey(id, tagCancel)
		if ok, err := exists(txn, key); err != nil {
			return err
		} else if ok {
			return nil // first cancel wins
		}
		b, err := storagepb.MarshalCancel(&req)
		if err != nil {
			return err
		}
		if err := txn.Set(key, b); err != nil {
			return err
		}
		accepted = true
		return nil
	})
	return accepted, err
}

// ListNonterminal walks the slots: a run holds its slot from the
// CreateRun transaction until the terminality commit releases it, so
// the slot values are exactly the nonterminal run ids, and the walk is
// proportional to the runs in flight rather than to the retained
// history. Each run is read as its head. A slot naming a run with no
// meta row is corruption, reported rather than skipped.
func (s *Store) ListNonterminal(_ context.Context) ([]*driver.RunRecord, error) {
	var out []*driver.RunRecord
	err := s.db.View(func(txn *bdg.Txn) error {
		prefix := []byte{nsSlots}
		it := txn.NewIterator(bdg.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			rec, err := s.getHead(txn, kernel.RunID(v))
			if err != nil {
				return fmt.Errorf("badger: slot references run %s: %w", v, err)
			}
			out = append(out, rec)
		}
		return nil
	})
	return out, err
}

// GetActiveRunID answers from the slots: one point read, the same index
// CreateRun enforces the slot with.
func (s *Store) GetActiveRunID(_ context.Context, pipeline kernel.PipelineID, resource kernel.ResourceID) (kernel.RunID, bool, error) {
	var id kernel.RunID
	var ok bool
	err := s.db.View(func(txn *bdg.Txn) error {
		v, err := get(txn, slotKey(pipeline, resource))
		if err != nil {
			return err
		}
		if v != nil {
			id, ok = kernel.RunID(v), true
		}
		return nil
	})
	return id, ok, err
}

// Close stops the value-log collector and closes the database, which
// flushes what is in memory to disk.
func (s *Store) Close() error {
	var err error
	s.once.Do(func() {
		close(s.stop)
		<-s.done
		<-s.writerDone
		err = s.db.Close()
	})
	return err
}
