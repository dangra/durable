package durable

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// PipelineID identifies a durable Pipeline.
type PipelineID string

// ResourceID identifies the logical resource a Run operates on.
type ResourceID string

// RunID identifies one exact execution of one Pipeline against one resource.
type RunID string

// StepID identifies durable Step semantics: forward behavior, unwind
// behavior, and Step State schema.
type StepID string

// runIDEntropy makes concurrent Schedule calls collision-free and orders
// RunIDs created within the same millisecond.
var runIDEntropy = &ulid.LockedMonotonicReader{MonotonicReader: ulid.Monotonic(rand.Reader, 0)}

// newRunID generates a ULID RunID: time-prefixed and lexicographically
// creation-ordered. This is an implementation convenience for debugging,
// key layout, and tooling — RunIDs remain opaque strings, no API compares
// them, and CreatedAt stays authoritative for ordering.
func newRunID(now time.Time) RunID {
	if now.Before(time.Unix(0, 0)) {
		now = time.Unix(0, 0)
	}
	id, err := ulid.New(ulid.Timestamp(now), runIDEntropy)
	if err != nil {
		panic(fmt.Sprintf("durable: generating run id: %v", err))
	}
	return RunID(id.String())
}
