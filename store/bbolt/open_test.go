package bbolt_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dangra/durable/store"
	_ "github.com/dangra/durable/store/bbolt"
)

func TestOpenViaURI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.db")
	st, err := store.Open("bbolt://" + path) // path is absolute: bbolt:///tmp/.../durable.db
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, bad := range []string{"bbolt://", "bbolt://host/x.db", "bbolt:///" + path + "?opt=1"} {
		if _, err := store.Open(bad); err == nil || strings.HasPrefix(err.Error(), "durable/store") {
			t.Fatalf("%q: want a bbolt error, got %v", bad, err)
		}
	}
}
