package durable

import (
	"strings"
	"unicode/utf8"

	"crypto/rand"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// Identifiers must be valid UTF-8 and must not contain NUL bytes: the
// storage layer's key encoding reserves NUL as a field separator (a NUL
// inside an identifier could alias two distinct slots onto one key), and
// the durable representation serializes identifiers into protobuf string
// fields, which reject invalid UTF-8. NewDefinition panics on a bad
// identifier (a code-generation bug); Schedule rejects a bad ResourceID
// with an error.

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

// invalidID reports whether an identifier violates the storage
// constraints: a NUL byte (reserved as a key separator) or invalid UTF-8
// (rejected by protobuf string fields).
func invalidID(s string) bool {
	return strings.IndexByte(s, 0) >= 0 || !utf8.ValidString(s)
}

// sanitizeText makes free-form text (error messages, reasons, causes)
// safe for the durable representation's protobuf string fields: invalid
// UTF-8 would otherwise fail the marshal inside a durable transition and
// wedge the Run in a store-retry loop over a write that can never
// succeed.
func sanitizeText(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, string(utf8.RuneError))
}
