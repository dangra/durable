// Package store opens a durable store from a URI, the way database/sql
// opens a database from a driver name and a DSN. It knows no driver
// itself: each driver package registers its scheme in an init function,
// so wiring code blank-imports the drivers it wants and nothing else is
// linked into the binary.
//
//	import (
//	    "github.com/dangra/durable/store"
//	    _ "github.com/dangra/durable/store/bbolt"
//	)
//
//	st, err := store.Open("bbolt:///var/lib/app/durable.db")
//	eng := engine.New(st)
//
// The in-tree drivers are store/bbolt (persistent, single process) and
// store/mem (process-local; runs do not survive the process, which is
// what a CLI with ephemeral runs wants). The caller that opened a store
// closes it, after Engine.Stop.
//
// Implementers of a new backend implement driver.Store and call Register
// from an init function; the URI conventions are theirs to define and
// document, with the scheme naming the driver and any options carried in
// the query string.
package store

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"

	"github.com/dangra/durable/store/driver"
)

// ErrUnknownScheme is returned by Open when no driver registered the
// URI's scheme — usually a missing blank import of the driver package.
var ErrUnknownScheme = errors.New("durable/store: unknown scheme")

// Opener opens a store from a parsed URI. The scheme has already been
// matched; the driver interprets the rest.
type Opener func(u *url.URL) (driver.Store, error)

var (
	mu      sync.RWMutex
	drivers = map[string]Opener{}
)

// Register makes a driver available to Open under scheme. It is intended
// to be called from a driver package's init function; registering the
// same scheme twice panics, as does a nil opener.
func Register(scheme string, open Opener) {
	mu.Lock()
	defer mu.Unlock()
	if open == nil {
		panic("durable/store: Register with nil opener")
	}
	if _, dup := drivers[scheme]; dup {
		panic("durable/store: Register called twice for scheme " + scheme)
	}
	drivers[scheme] = open
}

// Schemes returns the registered schemes, sorted.
func Schemes() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(drivers))
	for s := range drivers {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Open parses uri and hands it to the driver registered for its scheme.
// A URI without a scheme is an error; Open never guesses a driver.
func Open(uri string) (driver.Store, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("durable/store: parsing %q: %w", uri, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("durable/store: %q has no scheme (registered: %v)", uri, Schemes())
	}
	mu.RLock()
	open, ok := drivers[u.Scheme]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownScheme, u.Scheme, Schemes())
	}
	return open(u)
}
