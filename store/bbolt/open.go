package bbolt

import (
	"fmt"
	"net/url"

	"github.com/dangra/durable/store"
	"github.com/dangra/durable/store/driver"
)

// Scheme is the URI scheme this driver registers with package store.
//
//	bbolt:///var/lib/app/durable.db   absolute path
//	bbolt:/var/lib/app/durable.db     the same, single slash
//	bbolt:durable.db                   path relative to the working directory
//
// No options are defined yet; a query string is rejected so a typo
// cannot be silently ignored.
const Scheme = "bbolt"

func init() {
	store.Register(Scheme, func(u *url.URL) (driver.Store, error) {
		path := u.Path
		if u.Opaque != "" {
			path = u.Opaque
		}
		if u.Host != "" {
			return nil, fmt.Errorf("bbolt: %q: a host is not meaningful; use bbolt:///absolute/path or bbolt:relative/path", u)
		}
		if path == "" {
			return nil, fmt.Errorf("bbolt: %q: missing database path", u)
		}
		if u.RawQuery != "" {
			return nil, fmt.Errorf("bbolt: %q: no options are defined; drop the query string", u)
		}
		return Open(path)
	})
}
