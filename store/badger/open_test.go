package badger_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/dangra/durable/store"
	_ "github.com/dangra/durable/store/badger"
)

func TestOpenViaURI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable")
	st, err := store.Open("badger://" + path + "?memtable=8MiB&sync_writes=false") // path is absolute: badger:///tmp/.../durable
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, bad := range []string{"badger://", "badger://host/x", "badger:///" + path + "?opt=1", "badger:///" + path + "?sync_writes=maybe", "badger:///" + path + "?memtable=lots"} {
		if _, err := store.Open(bad); err == nil || strings.HasPrefix(err.Error(), "durable/store") {
			t.Fatalf("%q: want a badger error, got %v", bad, err)
		}
	}
}
