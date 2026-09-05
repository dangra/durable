package mem

import (
	"fmt"
	"net/url"

	"github.com/dangra/durable/store"
	"github.com/dangra/durable/store/driver"
)

// Scheme is the URI scheme this driver registers with package store.
// The URI carries nothing else: "mem:" and "mem://" both open a fresh,
// empty store.
const Scheme = "mem"

func init() {
	store.Register(Scheme, func(u *url.URL) (driver.Store, error) {
		if u.Host != "" || u.Path != "" || u.Opaque != "" || u.RawQuery != "" {
			return nil, fmt.Errorf("mem: %q: the URI takes no path or options (want plain mem:)", u)
		}
		return New(), nil
	})
}
