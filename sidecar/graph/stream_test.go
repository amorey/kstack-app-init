package graph

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func recvWithin[T any](t *testing.T, ch <-chan T) (T, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a value")
		var zero T
		return zero, false
	}
}

// Snapshot is emitted first, then each source event is mapped onto out in
// order; closing the source closes out and runs unsub exactly once.
func TestStreamWithSnapshotEmitsSnapshotThenMappedEvents(t *testing.T) {
	src := make(chan int)
	var unsubbed atomic.Int32
	out := streamWithSnapshot(
		context.Background(),
		src,
		func() { unsubbed.Add(1) },
		strconv.Itoa,
		func() (string, bool) { return "snap", true },
	)

	if v, _ := recvWithin(t, out); v != "snap" {
		t.Fatalf("first = %q, want snap", v)
	}
	src <- 7
	if v, _ := recvWithin(t, out); v != "7" {
		t.Fatalf("mapped = %q, want 7", v)
	}
	close(src)
	if _, ok := recvWithin(t, out); ok {
		t.Fatal("out not closed after source closed")
	}
	if got := unsubbed.Load(); got != 1 {
		t.Fatalf("unsub called %d times, want 1", got)
	}
}

// snapshot returning ok=false skips the initial emit — the first value
// out is the first source event.
func TestStreamWithSnapshotSkipsSnapshotWhenNotOK(t *testing.T) {
	src := make(chan int)
	out := streamWithSnapshot(
		context.Background(),
		src,
		func() {},
		strconv.Itoa,
		func() (string, bool) { return "", false },
	)

	src <- 42
	if v, _ := recvWithin(t, out); v != "42" {
		t.Fatalf("first = %q, want 42 (snapshot skipped)", v)
	}
}

// Cancelling ctx tears the stream down: out closes and unsub runs even
// with no consumer draining and the source still open.
func TestStreamWithSnapshotStopsOnContextCancel(t *testing.T) {
	src := make(chan int)
	var unsubbed atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	out := streamWithSnapshot(
		ctx,
		src,
		func() { unsubbed.Add(1) },
		strconv.Itoa,
		func() (string, bool) { return "", false },
	)

	cancel()
	if _, ok := recvWithin(t, out); ok {
		t.Fatal("out not closed after ctx cancel")
	}
	if got := unsubbed.Load(); got != 1 {
		t.Fatalf("unsub called %d times, want 1", got)
	}
}

// drainUntilClosed receives (and discards) until the channel closes,
// failing on timeout. Tolerates the select race between delivering a
// pending value and observing ctx.Done — the property under test is that
// teardown happens, not which branch wins the cancel race.
func drainUntilClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	for {
		if _, ok := recvWithin(t, ch); !ok {
			return
		}
	}
}

// Cancelling ctx while the goroutine is blocked sending the snapshot (no
// consumer draining out) still tears down and runs unsub once — the
// snapshot-send select's ctx.Done arm.
func TestStreamWithSnapshotCancelWhileBlockedOnSnapshotSend(t *testing.T) {
	src := make(chan int)
	var unsubbed atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	out := streamWithSnapshot(
		ctx,
		src,
		func() { unsubbed.Add(1) },
		strconv.Itoa,
		func() (string, bool) { return "snap", true },
	)

	cancel() // goroutine is parked on `out <- "snap"` with no receiver
	drainUntilClosed(t, out)
	if got := unsubbed.Load(); got != 1 {
		t.Fatalf("unsub called %d times, want 1", got)
	}
}

// Same, but blocked sending a mapped event (snapshot skipped) — the
// inner mapFn-send select's ctx.Done arm.
func TestStreamWithSnapshotCancelWhileBlockedOnEventSend(t *testing.T) {
	src := make(chan int, 1)
	var unsubbed atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	out := streamWithSnapshot(
		ctx,
		src,
		func() { unsubbed.Add(1) },
		strconv.Itoa,
		func() (string, bool) { return "", false },
	)

	src <- 1 // goroutine maps it then parks on `out <- "1"`
	cancel()
	drainUntilClosed(t, out)
	if got := unsubbed.Load(); got != 1 {
		t.Fatalf("unsub called %d times, want 1", got)
	}
}
