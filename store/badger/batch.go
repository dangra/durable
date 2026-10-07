package badger

import (
	"errors"

	bdg "github.com/dgraph-io/badger/v4"
)

// The write batcher is this driver's group commit. Badger in sync-writes
// mode syncs its write-ahead log once per committed transaction and the
// value log once per write batch, and shares neither sync between
// concurrent transactions; measured, a burst of a thousand runs paid
// some ten thousand syncs. So every write goes through one goroutine
// that runs whatever writes are queued in one Badger transaction,
// committed once: concurrent callers share its syncs the way bbolt's
// Batch shares an fsync. A lone writer commits at once; writes that
// arrive while a commit is in flight form the next batch, so there is
// no batching delay to tune.
//
// A write is a re-runnable function of a transaction. When one fails,
// the batch is discarded, the failing write runs alone so its error is
// its own, and the rest are retried as a batch. Inside a batch every
// write sees the ones before it, so the batch is a serial history; and
// with one writer, Badger's conflict detection has nothing to catch,
// though a conflict is still retried.

// maxBatch bounds the writes one transaction carries, well inside
// Badger's transaction size limit for the engine's write sizes; a batch
// that still outgrows it falls back to running its writes alone.
const maxBatch = 128

type writeReq struct {
	fn   func(*bdg.Txn) error
	done chan error
}

// update runs fn in a read-write transaction shared with whatever other
// writes are queued, returning once that transaction is committed (and
// synced, with sync writes on) or fn's own error.
func (s *Store) update(fn func(txn *bdg.Txn) error) error {
	req := &writeReq{fn: fn, done: make(chan error, 1)}
	select {
	case s.writes <- req:
	case <-s.stop:
		return errClosed
	}
	select {
	case err := <-req.done:
		return err
	case <-s.writerDone:
		// The writer finishes a batch before it looks at stop, so a
		// write it took is answered before it exits; one still queued
		// is not run.
		select {
		case err := <-req.done:
			return err
		default:
			return errClosed
		}
	}
}

var errClosed = errors.New("badger: store is closed")

// writer is the batching goroutine: it takes one queued write, gathers
// whatever else is queued behind it, and commits them together.
func (s *Store) writer() {
	defer close(s.writerDone)
	batch := make([]*writeReq, 0, maxBatch)
	for {
		var first *writeReq
		select {
		case first = <-s.writes:
		case <-s.stop:
			return
		}
		batch = append(batch[:0], first)
	gather:
		for len(batch) < maxBatch {
			select {
			case r := <-s.writes:
				batch = append(batch, r)
			default:
				break gather
			}
		}
		s.commitBatch(batch)
	}
}

// commitBatch commits batch in as few transactions as it can.
func (s *Store) commitBatch(batch []*writeReq) {
	for len(batch) > 0 {
		if len(batch) == 1 {
			batch[0].done <- s.solo(batch[0].fn)
			return
		}
		txn := s.db.NewTransaction(true)
		failed := -1
		for i, r := range batch {
			if err := r.fn(txn); err != nil {
				failed = i
				break
			}
		}
		if failed >= 0 {
			// The failing write runs alone: its error must be its own,
			// not the product of the writes before it in the batch.
			txn.Discard()
			r := batch[failed]
			r.done <- s.solo(r.fn)
			batch = append(batch[:failed:failed], batch[failed+1:]...)
			continue
		}
		err := txn.Commit()
		switch {
		case err == nil:
			for _, r := range batch {
				r.done <- nil
			}
			return
		case errors.Is(err, bdg.ErrConflict):
			continue
		default:
			// A batch Badger refuses whole (too big, say) still lets its
			// writes through one at a time.
			for _, r := range batch {
				r.done <- s.solo(r.fn)
			}
			return
		}
	}
}

// solo runs one write in its own transaction, retrying a conflict.
func (s *Store) solo(fn func(*bdg.Txn) error) error {
	for {
		txn := s.db.NewTransaction(true)
		if err := fn(txn); err != nil {
			txn.Discard()
			return err
		}
		err := txn.Commit()
		if !errors.Is(err, bdg.ErrConflict) {
			return err
		}
	}
}
