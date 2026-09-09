package bbolt

import (
	"container/list"
	"sync"

	"github.com/dangra/durable/kernel"
)

// outputCache holds the large outputs of terminal runs — the ones kept
// beside the terminal row — so the read that typically follows Wait
// finds the bytes in memory instead of opening a bucket and cloning
// them from the file. Unlike the blobs of runs in flight, terminal
// outputs live until retention reaps them, so this cache is bounded by
// least-recently-used eviction rather than by the population: an entry
// enters when the terminality commit succeeds (the bytes are in hand)
// or when a read misses, moves to the front on every read, and leaves
// when the limit pushes it out or reap deletes its run. The slices are
// shared, as the store contract allows for outputs.
type outputCache struct {
	mu    sync.Mutex
	order *list.List // front is most recently used
	byID  map[kernel.RunID]*list.Element
	size  int
	limit int
}

type outputEntry struct {
	id  kernel.RunID
	out []byte
}

func newOutputCache(limit int) *outputCache {
	return &outputCache{order: list.New(), byID: make(map[kernel.RunID]*list.Element), limit: limit}
}

// put caches the run's output, evicting the least recently used entries
// past the limit; an output larger than the whole limit is not cached.
func (c *outputCache) put(id kernel.RunID, out []byte) {
	if len(out) == 0 || len(out) > c.limit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[id]; ok {
		c.size -= len(el.Value.(*outputEntry).out)
		c.order.Remove(el)
		delete(c.byID, id)
	}
	for c.size+len(out) > c.limit {
		last := c.order.Back()
		if last == nil {
			break
		}
		e := last.Value.(*outputEntry)
		c.size -= len(e.out)
		c.order.Remove(last)
		delete(c.byID, e.id)
	}
	c.byID[id] = c.order.PushFront(&outputEntry{id: id, out: out})
	c.size += len(out)
}

// get returns the run's cached output, shared, nil when none, marking it
// most recently used.
func (c *outputCache) get(id kernel.RunID) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byID[id]
	if !ok {
		return nil
	}
	c.order.MoveToFront(el)
	return el.Value.(*outputEntry).out
}

// drop forgets the run.
func (c *outputCache) drop(id kernel.RunID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[id]; ok {
		c.size -= len(el.Value.(*outputEntry).out)
		c.order.Remove(el)
		delete(c.byID, id)
	}
}
