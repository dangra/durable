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
//	a·R·o·<step>·<phase>     -> OperationRecord  one row per step and phase,
//	                                             state in it; phase byte
//	                                             f forward, u unwind
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
// operation's one row. Cancel and failure land as their rows. The
// terminality commit writes the terminal row and its expiry key and
// deletes the run's active rows, its cursor, and its slot, all in one
// transaction: from then on the run is one row, and the input and
// states, folded into the output by then, are released when the run
// ends rather than when retention reaps it. Reap walks the expiry index
// from its oldest key, stops at the first run that has not expired,
// and deletes each victim's terminal row and index key: proportional
// to the victims, decoding nothing. A run's head — what status,
// waiting, lookups, and recovery need — is the terminal row, or for a
// nonterminal run five point reads (the missed terminal row, then
// meta, cancel, failure, cursor): no operation row is decoded and no
// blob is touched.
//
// What the engine changes against the bbolt driver. An LSM tree
// appends: a write costs its bytes once in the log and again per
// compaction, never a rewritten page, so nothing here keeps a large
// value out of its row (Badger moves values past its threshold to the
// value log by itself), defers a run's deletion to a batch, or orders
// operation rows by resolution in the key — the record carries its
// order, and a reader folds rows into a map. Transactions are
// serializable with conflict detection at commit: two CreateRuns racing
// for one slot both read it free, and the second to commit is refused
// and retried, where it finds the occupant. Badger coalesces concurrent
// commits into one write batch, which is the group commit the bbolt
// driver does by hand. The cost is a read: a point lookup walks the
// memtables and then the levels, with bloom filters and the block cache
// in front of the disk, where bbolt follows a few pages of a B+ tree;
// and compaction and value-log garbage collection run in the
// background, where bbolt has no such work.
//
// Conflict detection sees what a transaction read: every point read,
// a missing key included, but of a prefix walk only the seek key and
// the rows it returned. A row another transaction inserts under a
// walked prefix is invisible to it. The one row that can appear that
// way under a run's stage while a transition runs is the cancel request
// (RequestCancel is the only writer besides the run's own worker), so
// the terminality commit point-reads it: a cancel committing first
// forces the commit to retry and fold it in, instead of the commit
// deleting an accepted request.
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
)

// Store is a driver.Store backed by a Badger database directory.
type Store struct {
	db   *bdg.DB
	stop chan struct{}
	done chan struct{}
	once sync.Once
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
	logger     *slog.Logger
}

// WithSyncWrites controls whether every commit is fsynced before it
// returns, the way the bbolt driver commits. On by default: a durable
// fact is one the machine can lose power on. Off, a commit returns
// once the write is in the OS, and the last moments before a power
// loss can roll back — a process crash loses nothing either way.
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
	cfg := config{syncWrites: true, memTable: DefaultMemTableSize}
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
		WithLogger(nil)
	if cfg.logger != nil {
		bo = bo.WithLogger(slogger{cfg.logger})
	}
	db, err := bdg.Open(bo)
	if err != nil {
		return nil, fmt.Errorf("badger: opening %s: %w", dir, err)
	}
	s := &Store{db: db, stop: make(chan struct{}), done: make(chan struct{})}
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

// update runs fn in a read-write transaction, retrying it when the
// commit conflicts with another transaction's. fn must be re-runnable:
// everything it decides it decides from what it reads.
func (s *Store) update(fn func(txn *bdg.Txn) error) error {
	for {
		err := s.db.Update(fn)
		if !errors.Is(err, bdg.ErrConflict) {
			return err
		}
	}
}

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
func (s *Store) getRun(txn *bdg.Txn, id kernel.RunID) (*driver.RunRecord, error) {
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
	return s.getNonterminal(txn, id)
}

// getNonterminal reads a nonterminal run from one prefix walk of its
// active rows — the meta row sorts first, so its absence is
// ErrRunNotFound — and its cursor.
func (s *Store) getNonterminal(txn *bdg.Txn, id kernel.RunID) (*driver.RunRecord, error) {
	rec := &driver.RunRecord{}
	prefix := runPrefix(id)
	it := txn.NewIterator(bdg.IteratorOptions{Prefix: prefix})
	defer it.Close()
	it.Seek(prefix)
	if !it.ValidForPrefix(prefix) || it.Item().Key()[len(prefix)] != tagMeta {
		return nil, kernel.ErrRunNotFound
	}
	for ; it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		tag, rest := k[len(prefix)], k[len(prefix)+1:]
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		switch tag {
		case tagMeta:
			if err := storagepb.UnmarshalRunMetaInto(v, rec); err != nil {
				return nil, err
			}
		case tagInput:
			rec.Input = v
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
			step, phase, ok := splitOpRest(rest)
			if !ok {
				return nil, fmt.Errorf("badger: malformed operation key %q", k)
			}
			op, err := storagepb.UnmarshalOperationRecord(v)
			if err != nil {
				return nil, err
			}
			*rec.Step(step).Op(phase) = op
		default:
			return nil, fmt.Errorf("badger: unknown active row tag %q in key %q", tag, k)
		}
	}
	if err := overlayCursor(txn, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// overlayCursor reads the run's cursor onto rec: its scheduling fields,
// and its in-flight operation as an unresolved step entry — the forward
// operation in PhaseForward, the unwind one in PhaseUnwind (the Cursor
// contract).
func overlayCursor(txn *bdg.Txn, rec *driver.RunRecord) error {
	cb, err := get(txn, cursorKey(rec.RunID))
	if err != nil {
		return err
	}
	if cb == nil {
		return fmt.Errorf("badger: run %s has no cursor", rec.RunID)
	}
	cur, err := storagepb.UnmarshalCursor(cb)
	if err != nil {
		return err
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
	tb, err := get(txn, terminalKey(id))
	if err != nil {
		return nil, err
	}
	if tb != nil {
		rec := &driver.RunRecord{}
		if _, err := storagepb.UnmarshalTerminalInto(tb, rec); err != nil {
			return nil, err
		}
		rec.Output = nil
		return rec, nil
	}
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
	if v, err := get(txn, activeKey(id, tagCancel)); err != nil {
		return nil, err
	} else if v != nil {
		cr, err := storagepb.UnmarshalCancel(v)
		if err != nil {
			return nil, err
		}
		rec.Cancel = cr
	}
	if v, err := get(txn, activeKey(id, tagFailure)); err != nil {
		return nil, err
	} else if v != nil {
		f, err := storagepb.UnmarshalFailureRecord(v)
		if err != nil {
			return nil, err
		}
		rec.Failure = &f
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

// putOp upserts the run's one row for the operation, state included.
func putOp(txn *bdg.Txn, id kernel.RunID, step kernel.StepID, phase kernel.Phase, op *driver.OperationRecord) error {
	b, err := storagepb.MarshalOperationRecord(op)
	if err != nil {
		return err
	}
	return txn.Set(opKey(id, step, phase), b)
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
// keys are collected before deletion; deleting nothing is fine.
func deleteNonterminalStage(txn *bdg.Txn, id kernel.RunID) error {
	for _, k := range keysWithPrefix(txn, runPrefix(id), 0) {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return txn.Delete(cursorKey(id))
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
			existing, err = s.getRun(txn, kernel.RunID(activeID))
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
	return existing, created, nil
}

func (s *Store) ApplyTransition(_ context.Context, id kernel.RunID, t driver.Transition) error {
	return s.update(func(txn *bdg.Txn) error { return s.applyTransition(txn, id, t) })
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
			if _, err := exists(txn, activeKey(id, tagCancel)); err != nil {
				return err
			}
			rec, err := s.getNonterminal(txn, id)
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
			rec.StartedAt = t.Cursor.StartedAt
			oc := *t.Outcome
			rec.Outcome = &oc
			rec.Output = t.Output
			rec.CompactTerminal()
			if err := putTerminal(txn, rec); err != nil {
				return err
			}
			if err := deleteNonterminalStage(txn, id); err != nil {
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
		rec, err = s.getRun(txn, id)
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
		err = s.db.Close()
	})
	return err
}
