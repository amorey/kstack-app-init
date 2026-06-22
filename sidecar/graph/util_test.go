package graph

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// recvOrTimeout pulls the next value from ch with a deadline so a stalled
// stream fails the test instead of hanging.
func recvOrTimeout[T any](t *testing.T, ch <-chan T) (T, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting on stream")
		var zero T
		return zero, false
	}
}

// mapStream applies mapFn to every source value in order, then closes the
// output and runs unsub when the source channel closes.
func TestMapStreamMapsValuesAndClosesWithSource(t *testing.T) {
	sub := make(chan int)
	unsubbed := make(chan struct{})

	out := mapStream(context.Background(), sub, func() { close(unsubbed) }, strconv.Itoa)

	go func() {
		sub <- 1
		sub <- 2
		close(sub)
	}()

	for _, want := range []string{"1", "2"} {
		got, ok := recvOrTimeout(t, out)
		if !ok || got != want {
			t.Fatalf("got (%q, %v), want (%q, true)", got, ok, want)
		}
	}

	if _, ok := recvOrTimeout(t, out); ok {
		t.Fatal("output channel still open after source closed")
	}
	if _, ok := recvOrTimeout(t, unsubbed); ok {
		t.Fatal("unsub channel delivered a value; want close")
	}
}

// Cancelling ctx tears the stream down: the output channel closes and unsub
// runs, even though the source channel stays open.
func TestMapStreamStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := make(chan int)
	unsubbed := make(chan struct{})

	out := mapStream(ctx, sub, func() { close(unsubbed) }, strconv.Itoa)

	cancel()

	if _, ok := recvOrTimeout(t, out); ok {
		t.Fatal("output channel still open after ctx cancel")
	}
	if _, ok := recvOrTimeout(t, unsubbed); ok {
		t.Fatal("unsub channel delivered a value; want close")
	}
}

// Cancelling ctx while the pump is blocked sending a mapped value (no reader
// on the output channel) still tears the stream down — the send must not wedge
// the goroutine past cancellation.
func TestMapStreamStopsOnContextCancelDuringSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := make(chan int, 1)
	unsubbed := make(chan struct{})

	sub <- 1 // pump picks this up and blocks sending "1" to the unread output
	out := mapStream(ctx, sub, func() { close(unsubbed) }, strconv.Itoa)

	cancel()

	// Drain whatever arrives; the stream must end (channel close) either way.
	for {
		_, ok := recvOrTimeout(t, out)
		if !ok {
			break
		}
	}
	if _, ok := recvOrTimeout(t, unsubbed); ok {
		t.Fatal("unsub channel delivered a value; want close")
	}
}
