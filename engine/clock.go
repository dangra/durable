package engine

import "time"

// Clock abstracts time for deterministic tests. It governs retry
// timestamps, retry wakeups, recovery backoff, and Failure timestamps.
type Clock interface {
	Now() time.Time
	// After behaves like time.After relative to this Clock.
	After(d time.Duration) <-chan time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
