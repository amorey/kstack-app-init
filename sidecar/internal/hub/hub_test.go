package hub_test

import (
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/hub"
)

// Every subscriber receives each subsequent Publish value.
func TestFanOut(t *testing.T) {
	h := hub.New[string]()

	a, unsubA := h.Subscribe()
	defer unsubA()
	b, unsubB := h.Subscribe()
	defer unsubB()

	h.Publish("x")

	for name, ch := range map[string]<-chan string{"a": a, "b": b} {
		select {
		case got := <-ch:
			if got != "x" {
				t.Errorf("%s: got %q", name, got)
			}
		case <-time.After(time.Second):
			t.Errorf("%s: timed out", name)
		}
	}
}

// After unsubscribe the channel is closed and further publishes neither
// resurrect it nor panic. Unsubscribe is idempotent.
func TestUnsubscribeClosesChannelIdempotently(t *testing.T) {
	h := hub.New[int]()
	ch, unsub := h.Subscribe()
	unsub()
	unsub() // must be a no-op

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel still open after unsubscribe")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after unsubscribe")
	}
	h.Publish(7) // must not panic
}

// Publish must never block even when a subscriber never drains its
// buffer — excess messages drop for that subscriber instead of stalling
// the publisher or the other subscribers.
func TestPublishNeverBlocksWhenSubscriberDoesNotDrain(t *testing.T) {
	h := hub.New[int]()
	a, unsubA := h.Subscribe()
	defer unsubA()
	_, unsubB := h.Subscribe() // never read ⇒ its buffer fills and drops
	defer unsubB()

	done := make(chan struct{})
	go func() {
		for i := range 1000 { // far more than the internal buffer
			h.Publish(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on an undrained subscriber")
	}

	// `a` was also undrained during the loop, so its buffer holds the
	// first `buffer` publishes in order and dropped the rest — the first
	// read is deterministically 0.
	select {
	case got := <-a:
		if got != 0 {
			t.Fatalf("first buffered value = %d, want 0", got)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber received nothing")
	}
}
