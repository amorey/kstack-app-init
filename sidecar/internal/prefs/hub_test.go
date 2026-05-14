package prefs_test

import (
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// Each subscriber receives all subsequent Publish() values. A buffered
// channel means a slow consumer is dropped rather than blocking the
// publisher — but in this single-flush test, neither consumer is slow.
func TestHubFanOut(t *testing.T) {
	h := prefs.NewHub()

	a, unsubA := h.Subscribe()
	defer unsubA()
	b, unsubB := h.Subscribe()
	defer unsubB()

	h.Publish(prefs.Settings{Placeholder: "x"})

	for name, ch := range map[string]<-chan prefs.Settings{"a": a, "b": b} {
		select {
		case got := <-ch:
			if got.Placeholder != "x" {
				t.Errorf("%s: got %+v", name, got)
			}
		case <-time.After(time.Second):
			t.Errorf("%s: timed out", name)
		}
	}
}

// After unsubscribe the channel is closed; further publishes don't
// resurrect it.
func TestHubUnsubscribeClosesChannel(t *testing.T) {
	h := prefs.NewHub()
	ch, unsub := h.Subscribe()
	unsub()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel still open after unsubscribe")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after unsubscribe")
	}
	h.Publish(prefs.Settings{Placeholder: "y"}) // must not panic
}
