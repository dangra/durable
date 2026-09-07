package engine

import (
	"github.com/dangra/durable"
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
// fields, which reject invalid UTF-8. Engine.Bind rejects a bad
// identifier (a code-generation bug); Schedule rejects a bad ResourceID
// with an error.

// runIDEntropy makes concurrent Schedule calls collision-free and orders
// RunIDs created within the same millisecond.
var runIDEntropy = &ulid.LockedMonotonicReader{MonotonicReader: ulid.Monotonic(rand.Reader, 0)}

// newRunID generates a ULID RunID: time-prefixed and lexicographically
// creation-ordered. This is an implementation convenience for debugging,
// key layout, and tooling — RunIDs remain opaque strings, no API compares
// them, and CreatedAt stays authoritative for ordering.
func newRunID(now time.Time) durable.RunID {
	if now.Before(time.Unix(0, 0)) {
		now = time.Unix(0, 0)
	}
	id, err := ulid.New(ulid.Timestamp(now), runIDEntropy)
	if err != nil {
		panic(fmt.Sprintf("durable: generating run id: %v", err))
	}
	return durable.RunID(id.String())
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

// truncationMark closes text cut by boundText.
const truncationMark = "…"

// boundText sanitizes s for storage and cuts it to the engine's text
// limit at a rune boundary, ending the cut text with truncationMark so a
// reader knows there was more. Text within the limit is unchanged.
func (e *Engine) boundText(s string) string {
	s = sanitizeText(s)
	limit := e.textLimit
	if limit <= 0 {
		limit = DefaultTextLimit
	}
	if len(s) <= limit {
		return s
	}
	cut := limit - len(truncationMark)
	if cut < 0 {
		cut = 0
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncationMark
}
