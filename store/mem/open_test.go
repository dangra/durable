package mem_test

import (
	"context"
	"testing"

	"github.com/dangra/durable/store"
	"github.com/dangra/durable/store/mem"
)

func TestOpenViaURI(t *testing.T) {
	for _, uri := range []string{"mem:", "mem://"} {
		st, err := store.Open(uri)
		if err != nil {
			t.Fatalf("Open(%q): %v", uri, err)
		}
		if _, ok := st.(*mem.Store); !ok {
			t.Fatalf("Open(%q) = %T", uri, st)
		}
		runs, err := st.ListNonterminal(context.Background())
		if err != nil || len(runs) != 0 {
			t.Fatalf("fresh store: %v, %v", runs, err)
		}
	}
	for _, bad := range []string{"mem:///path", "mem://host", "mem:?x=1"} {
		if _, err := store.Open(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}
