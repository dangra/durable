package badger_test

import (
	"path/filepath"
	"testing"

	"github.com/dangra/durable/store/badger"
	"github.com/dangra/durable/store/internal/storetest"
)

// FuzzStoreContract drives identical operation sequences against the
// badger store and mem.Store — the executable specification of the
// Store contract — and fails on any observable divergence.
func FuzzStoreContract(f *testing.F) {
	storetest.Seed(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		bs, err := badger.Open(filepath.Join(t.TempDir(), "fuzz"), badger.WithMemTableSize(8<<20))
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		storetest.Fuzz(t, data, bs)
	})
}
