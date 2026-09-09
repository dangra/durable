package dirtyset

import (
	"sync"
	"testing"
)

func TestMarkAndTake(t *testing.T) {
	var s Set[string]
	if s.Take("a") {
		t.Fatal("an unmarked key must not be taken")
	}
	s.Mark("a")
	s.Mark("a") // coalesces
	s.Mark("b")
	if !s.Take("a") {
		t.Fatal("a marked key must be taken")
	}
	if s.Take("a") {
		t.Fatal("Take must clear the mark")
	}
	if !s.Take("b") || s.Take("b") {
		t.Fatal("keys are independent, and each mark is taken once")
	}
}

// Every mark is seen by exactly one Take, whatever the interleaving.
func TestConcurrentMarksAreTakenOnce(t *testing.T) {
	var s Set[int]
	const keys, marksPerKey = 8, 1000
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for i := 0; i < marksPerKey; i++ {
				s.Mark(k)
			}
		}(k)
	}
	taken := make([]int, keys)
	var tw sync.WaitGroup
	stop := make(chan struct{})
	for k := 0; k < keys; k++ {
		tw.Add(1)
		go func(k int) {
			defer tw.Done()
			for {
				if s.Take(k) {
					taken[k]++
				}
				select {
				case <-stop:
					if s.Take(k) {
						taken[k]++
					}
					return
				default:
				}
			}
		}(k)
	}
	wg.Wait()
	close(stop)
	tw.Wait()
	for k, n := range taken {
		if n < 1 || n > marksPerKey {
			t.Fatalf("key %d taken %d times; want between 1 and %d", k, n, marksPerKey)
		}
		if s.Take(k) {
			t.Fatalf("key %d still marked after the takers drained it", k)
		}
	}
}
