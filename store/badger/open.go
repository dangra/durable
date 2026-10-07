package badger

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/dangra/durable/store"
	"github.com/dangra/durable/store/driver"
)

// Scheme is the URI scheme this driver registers with package store.
// The path is a directory, created if missing:
//
//	badger:///var/lib/app/durable   absolute path
//	badger:/var/lib/app/durable     the same, single slash
//	badger:durable                  path relative to the working directory
//
// The query carries the options; an unknown key is rejected so a typo
// cannot be silently ignored:
//
//	badger:///var/lib/app/durable?sync_writes=false&memtable=16MiB
//
//	sync_writes  WithSyncWrites: true or false
//	memtable     WithMemTableSize: a byte count with an optional binary
//	             unit (K, M, G, or KiB, MiB, GiB)
const Scheme = "badger"

func init() {
	store.Register(Scheme, func(u *url.URL) (driver.Store, error) {
		path := u.Path
		if u.Opaque != "" {
			path = u.Opaque
		}
		if u.Host != "" {
			return nil, fmt.Errorf("badger: %q: a host is not meaningful; use badger:///absolute/path or badger:relative/path", u)
		}
		if path == "" {
			return nil, fmt.Errorf("badger: %q: missing database directory", u)
		}
		var opts []Option
		for key, values := range u.Query() {
			if len(values) != 1 {
				return nil, fmt.Errorf("badger: %q: option %q given %d times", u, key, len(values))
			}
			switch key {
			case "sync_writes":
				on, err := strconv.ParseBool(values[0])
				if err != nil {
					return nil, fmt.Errorf("badger: %q: option %q: %q is not a boolean", u, key, values[0])
				}
				opts = append(opts, WithSyncWrites(on))
			case "memtable":
				n, err := parseBytes(values[0])
				if err != nil {
					return nil, fmt.Errorf("badger: %q: option %q: %w", u, key, err)
				}
				opts = append(opts, WithMemTableSize(n))
			default:
				return nil, fmt.Errorf("badger: %q: unknown option %q", u, key)
			}
		}
		return Open(path, opts...)
	})
}

// parseBytes reads a byte count with an optional binary unit: K, M, G,
// or KiB, MiB, GiB, in any case.
func parseBytes(v string) (int64, error) {
	unit := int64(1)
	s := strings.ToLower(strings.TrimSpace(v))
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}} {
		if strings.HasSuffix(s, u.suffix) {
			s, unit = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a byte count", v)
	}
	return n * unit, nil
}
