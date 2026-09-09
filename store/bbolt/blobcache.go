package bbolt

import (
	"container/list"
	"sync"

	"github.com/dangra/durable/kernel"
)

// DefaultBlobCache is the default limit, in bytes, of the cache of the
// blobs of the runs in flight (see WithBlobCache).
const DefaultBlobCache = 64 << 20

// DefaultOutputCache is the default limit, in bytes, of the cache of the
// outputs of terminal runs (see WithOutputCache).
const DefaultOutputCache = 16 << 20

// blobCache is an in-memory, least-recently-used cache of the immutable
// blobs of runs, keyed by run: a run's input and the states kept beside
// their rows, or its output. The store keeps two, with their own budgets
// and their own drop events, because their populations differ in what
// bounds them:
//
//   - the blobs of the runs in flight, filled by the writes that store
//     them and dropped when the run's terminality commit succeeds. A
//     read of a nonterminal run then never seeks bbolt for them, never
//     opens a nested bucket, and pays no copy. Runs live for hours or
//     days when they retry, park on other runs, or block in a step, and
//     such runs are read rarely, so the cache evicts the least recently
//     used past its limit rather than pinning every run in flight; an
//     evicted run reads from the file on its next wake and re-enters.
//   - the outputs of terminal runs, filled when the terminality commit
//     succeeds and dropped when reap deletes the run, for the read that
//     follows Wait. Terminal runs live until retention reaps them, so
//     eviction is what bounds this one.
//
// The store is its database's only writer (one engine per store), so it
// keeps a cache coherent by construction; a restart starts cold and the
// first read of a run fills its entry from the file. Slices are shared,
// in and out: the store contract declares these blobs immutable.
type blobCache struct {
	mu    sync.Mutex
	order *list.List // front is most recently used
	byID  map[kernel.RunID]*list.Element
	size  int
	limit int
}

// runBlobs is one run's cached blobs. Slices are never modified in
// place, only replaced.
type runBlobs struct {
	id     kernel.RunID
	input  []byte
	states map[kernel.StepID][]byte
	output []byte
	size   int
}

func newBlobCache(limit int) *blobCache {
	return &blobCache{order: list.New(), byID: make(map[kernel.RunID]*list.Element), limit: limit}
}

// touch returns the run's entry, most recently used, creating it when
// absent; nil when the cache admits nothing (a zero limit).
func (c *blobCache) touch(id kernel.RunID) *runBlobs {
	if c.limit <= 0 {
		return nil
	}
	if el, ok := c.byID[id]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*runBlobs)
	}
	rb := &runBlobs{id: id}
	c.byID[id] = c.order.PushFront(rb)
	return rb
}

// evict drops least recently used entries until the cache fits its
// limit, never the entry passed in, which is the one being written.
func (c *blobCache) evict(keep *runBlobs) {
	for c.size > c.limit {
		last := c.order.Back()
		if last == nil {
			return
		}
		rb := last.Value.(*runBlobs)
		if rb == keep {
			// The one being written is alone and too big: drop it too.
			c.remove(last)
			return
		}
		c.remove(last)
	}
}

func (c *blobCache) remove(el *list.Element) {
	rb := el.Value.(*runBlobs)
	c.size -= rb.size
	c.order.Remove(el)
	delete(c.byID, rb.id)
}

// setInput caches the run's input, retaining the slice.
func (c *blobCache) setInput(id kernel.RunID, input []byte) {
	if len(input) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rb := c.touch(id)
	if rb == nil {
		return
	}
	c.size -= len(rb.input)
	rb.size -= len(rb.input)
	rb.input = input
	c.size += len(input)
	rb.size += len(input)
	c.evict(rb)
}

// setState caches a step's committed state, retaining the slice; an
// empty state drops the entry (the row was replaced without one).
func (c *blobCache) setState(id kernel.RunID, step kernel.StepID, state []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rb := c.touch(id)
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
	c.evict(rb)
}

// setOutput caches the run's output, retaining the slice.
func (c *blobCache) setOutput(id kernel.RunID, output []byte) {
	if len(output) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rb := c.touch(id)
	if rb == nil {
		return
	}
	c.size -= len(rb.output)
	rb.size -= len(rb.output)
	rb.output = output
	c.size += len(output)
	rb.size += len(output)
	c.evict(rb)
}

// fill caches a run read from the file on a miss — the slices the read
// cloned out of the file's pages — when the run is not cached.
func (c *blobCache) fill(id kernel.RunID, rb *runBlobs) {
	need := len(rb.input) + len(rb.output)
	for _, st := range rb.states {
		need += len(st)
	}
	if need == 0 || c.limit <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byID[id]; ok {
		return
	}
	entry := &runBlobs{id: id, input: rb.input, states: rb.states, output: rb.output, size: need}
	c.byID[id] = c.order.PushFront(entry)
	c.size += need
	c.evict(entry)
}

// drop forgets the run.
func (c *blobCache) drop(id kernel.RunID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[id]; ok {
		c.remove(el)
	}
}

// lookup returns the run's entry, marking it most recently used, and
// whether the run is cached.
func (c *blobCache) lookup(id kernel.RunID) (*runBlobs, bool) {
	el, ok := c.byID[id]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*runBlobs), true
}

// input returns the run's cached input, shared, and whether the run is
// cached at all.
func (c *blobCache) input(id kernel.RunID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rb, ok := c.lookup(id)
	if !ok {
		return nil, false
	}
	return rb.input, true
}

// state returns a step's cached state, shared; nil when none.
func (c *blobCache) state(id kernel.RunID, step kernel.StepID) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rb, ok := c.lookup(id); ok {
		return rb.states[step]
	}
	return nil
}

// output returns the run's cached output, shared; nil when none.
func (c *blobCache) output(id kernel.RunID) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rb, ok := c.lookup(id); ok {
		return rb.output
	}
	return nil
}

// has reports whether the run is cached without marking it used; for
// tests, which must not reorder what they observe.
func (c *blobCache) has(id kernel.RunID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.byID[id]
	return ok
}
