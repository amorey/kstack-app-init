package graph

import (
	"context"
	"strconv"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

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
		if got := testutil.Recv(t, out, "a mapped value"); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}

	testutil.RecvClosed(t, out, "the output channel after the source closed")
	testutil.RecvClosed(t, unsubbed, "the unsub channel")
}

// Cancelling ctx tears the stream down: the output channel closes and unsub
// runs, even though the source channel stays open.
func TestMapStreamStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sub := make(chan int)
	unsubbed := make(chan struct{})

	out := mapStream(ctx, sub, func() { close(unsubbed) }, strconv.Itoa)

	cancel()

	testutil.RecvClosed(t, out, "the output channel after ctx cancel")
	testutil.RecvClosed(t, unsubbed, "the unsub channel")
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
	testutil.WaitClosed(t, out, "the output channel")
	testutil.RecvClosed(t, unsubbed, "the unsub channel")
}
