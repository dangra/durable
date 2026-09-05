package store

import (
	"errors"
	"net/url"
	"slices"
	"testing"

	"github.com/dangra/durable/store/driver"
)

func TestOpenDispatchesOnScheme(t *testing.T) {
	var got *url.URL
	Register("fake", func(u *url.URL) (driver.Store, error) { got = u; return nil, nil })
	t.Cleanup(func() {
		mu.Lock()
		delete(drivers, "fake")
		mu.Unlock()
	})

	if _, err := Open("fake:///some/path?opt=1"); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got == nil || got.Path != "/some/path" || got.Query().Get("opt") != "1" {
		t.Fatalf("driver got %v", got)
	}
	if !slices.Contains(Schemes(), "fake") {
		t.Fatalf("Schemes = %v", Schemes())
	}
}

func TestOpenRejectsUnknownAndSchemeless(t *testing.T) {
	if _, err := Open("nope:///x"); !errors.Is(err, ErrUnknownScheme) {
		t.Fatalf("unknown scheme: %v", err)
	}
	if _, err := Open("/just/a/path"); err == nil {
		t.Fatal("a bare path must not open anything")
	}
	if _, err := Open("::bad"); err == nil {
		t.Fatal("an unparsable uri must fail")
	}
}

func TestRegisterGuards(t *testing.T) {
	Register("once", func(*url.URL) (driver.Store, error) { return nil, nil })
	t.Cleanup(func() {
		mu.Lock()
		delete(drivers, "once")
		mu.Unlock()
	})
	mustPanic(t, func() { Register("once", func(*url.URL) (driver.Store, error) { return nil, nil }) })
	mustPanic(t, func() { Register("nilopener", nil) })
}

func mustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	f()
}
