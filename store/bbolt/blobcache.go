package bbolt

import (
	"sync"

	"github.com/dangra/durable/kernel"
)

// DefaultBlobCache is the blob cache's default limit in bytes (see
// WithBlobCache).
const DefaultBlobCache = 64 << 20

// blobCache holds the immutable blobs of the runs in flight — each run's
// input and the committed states kept beside their rows — so a read of
// a nonterminal run never seeks bbolt for them, never opens a nested
// bucket, and pays one copy per value instead of the cursor, bucket,
// and clone allocations of reading it from the file.
//
// The store is the only writer of its database (one engine per store),
// so it keeps the cache coherent by construction: an entry is filled
// from the bytes in hand when the write that stores them commits, a
// step's state is dropped when its row is replaced, and the run's entry
// is dropped when its terminality commit succeeds, after which reads
// come from the terminal record and carry no blobs. A restart starts
// cold; the first read of each run in flight fills its entry from the
// file. The slices are shared, in and out: the store contract declares
// a Run's Input and Step States immutable, so the cache keeps the
// slice a write passes in and hands the same slice to every read.
type blobCache struct {
	mu    sync.Mutex
	runs  map[kernel.RunID]*runBlobs
	size  int
	limit int
}

// runBlobs is one run's cached blobs. Slices are never modified in
// place, only replaced, so a slice header read under the lock stays
// valid to copy from after it.
type runBlobs struct {
	input  []byte
	states map[kernel.StepID][]byte
	size   int
}

func newBlobCache(limit int) *blobCache {
	return &blobCache{runs: make(map[kernel.RunID]*runBlobs), limit: limit}
}

// setInput caches the run's input, retaining the slice.
func (c *blobCache) setInput(id kernel.RunID, input []byte) {
	if len(input) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rb := c.entry(id, len(input))
	if rb == nil {
		return
	}
	c.size -= len(rb.input)
	rb.size -= len(rb.input)
	rb.input = input
	c.size += len(input)
	rb.size += len(input)
}

// setState caches a step's committed state, retaining the slice; an
// empty state drops the entry (the row was replaced without one).
func (c *blobCache) setState(id kernel.RunID, step kernel.StepID, state []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rb := c.entry(id, len(state))
	if rb == nil {
		return
	}
	if old, ok := rb.states[step]; ok {
		c.size -= len(old)
		rb.size -= len(old)
		delete(rb.states, step)
	}
	if len(state) == 0 {
		return
	}
	if rb.states == nil {
		rb.states = make(map[kernel.StepID][]byte)
	}
	rb.states[step] = state
	c.size += len(state)
	rb.size += len(state)
}

// entry returns the run's entry, creating it when adding need bytes
// stays within the limit; nil when the cache is full and the run is not
// yet cached, in which case the run simply is not cached.
func (c *blobCache) entry(id kernel.RunID, need int) *runBlobs {
	rb, ok := c.runs[id]
	if !ok {
		if c.size+need > c.limit {
			return nil
		}
		rb = &runBlobs{}
		c.runs[id] = rb
	}
	return rb
}

// drop forgets the run.
func (c *blobCache) drop(id kernel.RunID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rb, ok := c.runs[id]; ok {
		c.size -= rb.size
		delete(c.runs, id)
	}
}

// input returns the run's cached input, shared, and whether the run is
// cached at all.
func (c *blobCache) input(id kernel.RunID) (input []byte, cached bool) {
	c.mu.Lock()
	rb, ok := c.runs[id]
	var in []byte
	if ok {
		in = rb.input
	}
	c.mu.Unlock()
	if !ok || in == nil {
		return nil, ok
	}
	return in, true
}

// state returns a step's cached state, shared; nil when none.
func (c *blobCache) state(id kernel.RunID, step kernel.StepID) []byte {
	c.mu.Lock()
	var st []byte
	if rb, ok := c.runs[id]; ok {
		st = rb.states[step]
	}
	c.mu.Unlock()
	return st
}

// fill caches a run read from the file on a cold miss — the slices the
// read returned, which the read cloned out of the file's pages — when
// the run is not cached yet and the limit allows.
func (c *blobCache) fill(id kernel.RunID, rb *runBlobs) {
	need := len(rb.input)
	for _, st := range rb.states {
		need += len(st)
	}
	if need == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.runs[id]; ok || c.size+need > c.limit {
		return
	}
	entry := &runBlobs{input: rb.input, size: need}
	if len(rb.states) > 0 {
		entry.states = make(map[kernel.StepID][]byte, len(rb.states))
		for step, st := range rb.states {
			entry.states[step] = st
		}
	}
	c.runs[id] = entry
	c.size += need
}
