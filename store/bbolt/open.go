package bbolt

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/dangra/durable/store"
	"github.com/dangra/durable/store/driver"
)

// Scheme is the URI scheme this driver registers with package store.
//
//	bbolt:///var/lib/app/durable.db   absolute path
//	bbolt:/var/lib/app/durable.db     the same, single slash
//	bbolt:durable.db                   path relative to the working directory
//
// The query carries the options, as byte counts with an optional binary
// unit (K, M, G, or KiB, MiB, GiB); an unknown key is rejected so a typo
// cannot be silently ignored:
//
//	bbolt:///var/lib/app/durable.db?blob_cache=128MiB&output_cache=0
//
//	blob_cache    WithBlobCache
//	output_cache  WithOutputCache
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
		var opts []Option
		for key, values := range u.Query() {
			if len(values) != 1 {
				return nil, fmt.Errorf("bbolt: %q: option %q given %d times", u, key, len(values))
			}
			n, err := parseBytes(values[0])
			if err != nil {
				return nil, fmt.Errorf("bbolt: %q: option %q: %w", u, key, err)
			}
			switch key {
			case "blob_cache":
				opts = append(opts, WithBlobCache(n))
			case "output_cache":
				opts = append(opts, WithOutputCache(n))
			default:
				return nil, fmt.Errorf("bbolt: %q: unknown option %q", u, key)
			}
		}
		return Open(path, opts...)
	})
}

// parseBytes reads a byte count with an optional binary unit: K, M, G,
// or KiB, MiB, GiB, in any case.
func parseBytes(v string) (int, error) {
	unit := 1
	s := strings.ToLower(strings.TrimSpace(v))
	for _, u := range []struct {
		suffix string
		mult   int
	}{{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}} {
		if strings.HasSuffix(s, u.suffix) {
			s, unit = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a byte count", v)
	}
	return n * unit, nil
}
