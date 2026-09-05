package frozen

import (
	"sync"
	"testing"
)

func TestPutGetBeforeAndAfterFreeze(t *testing.T) {
	var m Map[string, int]
	if _, ok := m.Get("a"); ok {
		t.Fatal("Get on an empty map found something")
	}
	if m.Len() != 0 || m.Frozen() {
		t.Fatalf("empty map: Len=%d Frozen=%v", m.Len(), m.Frozen())
	}
	m.Put("a", 1)
	m.Put("b", 2)
	m.Put("a", 3) // overwrite
	if v, ok := m.Get("a"); !ok || v != 3 {
		t.Fatalf("Get(a) before Freeze = %d, %v; want 3", v, ok)
	}
	if m.Len() != 2 {
		t.Fatalf("Len = %d; want 2", m.Len())
	}
	m.Freeze()
	m.Freeze() // idempotent
	if !m.Frozen() {
		t.Fatal("not frozen after Freeze")
	}
	if v, ok := m.Get("a"); !ok || v != 3 {
		t.Fatalf("Get(a) after Freeze = %d, %v; want 3", v, ok)
	}
	if _, ok := m.Get("zzz"); ok {
		t.Fatal("Get of a missing key after Freeze found something")
	}
	if m.Len() != 2 {
		t.Fatalf("Len after Freeze = %d; want 2", m.Len())
	}
}

func TestPutAfterFreezePanics(t *testing.T) {
	var m Map[string, int]
	m.Put("a", 1)
	m.Freeze()
	defer func() {
		if r := recover(); r != "frozen: Put after Freeze" {
			t.Fatalf("recovered %v; want the frozen panic", r)
		}
		if v, _ := m.Get("a"); v != 1 {
			t.Fatalf("the rejected Put changed the map: a=%d", v)
		}
	}()
	m.Put("a", 2)
}

func TestRangeBeforeAndAfterFreeze(t *testing.T) {
	var m Map[int, string]
	m.Range(func(int, string) bool { t.Fatal("Range on an empty map called fn"); return true })
	for i := range 5 {
		m.Put(i, "v")
	}
	count := func() int {
		n := 0
		m.Range(func(int, string) bool { n++; return true })
		return n
	}
	if count() != 5 {
		t.Fatalf("Range before Freeze visited %d; want 5", count())
	}
	stopped := 0
	m.Range(func(int, string) bool { stopped++; return false })
	if stopped != 1 {
		t.Fatalf("Range did not stop on false: visited %d", stopped)
	}
	// Before Freeze, fn may use the map: reads see the snapshot's
	// entries, and a Put lands without being visited by this Range.
	visited := 0
	m.Range(func(k int, _ string) bool {
		visited++
		if _, ok := m.Get(k); !ok {
			t.Errorf("Get(%d) inside Range found nothing", k)
		}
		_ = m.Len()
		m.Put(100+k, "late")
		return true
	})
	if visited != 5 || m.Len() != 10 {
		t.Fatalf("Range with a mutating fn visited %d, Len = %d; want 5 and 10", visited, m.Len())
	}
	m.Freeze()
	if count() != 10 {
		t.Fatalf("Range after Freeze visited %d; want 10", count())
	}
}

// Frozen reads are lock-free and race-free against each other; the race
// detector is the assertion.
func TestConcurrentReadsAfterFreeze(t *testing.T) {
	var m Map[int, int]
	for i := range 64 {
		m.Put(i, i*i)
	}
	m.Freeze()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 64 {
				if v, ok := m.Get(i); !ok || v != i*i {
					t.Errorf("goroutine %d: Get(%d) = %d, %v", g, i, v, ok)
				}
			}
			if m.Len() != 64 {
				t.Errorf("goroutine %d: Len = %d", g, m.Len())
			}
			m.Range(func(k, v int) bool { return v == k*k })
		}(g)
	}
	wg.Wait()
}

// Before Freeze, a writer and readers interleave under the lock.
func TestConcurrentWritesAndReadsBeforeFreeze(t *testing.T) {
	var m Map[int, int]
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 256 {
			m.Put(i, i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 256 {
			if v, ok := m.Get(i); ok && v != i {
				t.Errorf("Get(%d) = %d", i, v)
			}
			_ = m.Len()
			m.Range(func(int, int) bool { return true })
		}
	}()
	wg.Wait()
	m.Freeze()
	if m.Len() != 256 {
		t.Fatalf("Len = %d; want 256", m.Len())
	}
}
