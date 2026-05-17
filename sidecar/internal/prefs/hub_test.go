package prefs_test

import (
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// prefs.Hub is a type alias for the generic hub — fan-out, drop-on-full,
// and idempotent-unsubscribe are covered in internal/hub. This only
// proves the alias + NewHub wire through end-to-end with the real
// Settings type.
func TestHubAliasDeliversSettings(t *testing.T) {
	h := prefs.NewHub()
	ch, unsub := h.Subscribe()
	defer unsub()

	h.Publish(prefs.Settings{Placeholder: "x"})

	select {
	case got := <-ch:
		if got.Placeholder != "x" {
			t.Errorf("got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}
