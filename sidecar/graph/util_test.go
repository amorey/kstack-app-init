package graph

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
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

// The event-timestamp resolvers map Go's zero time (a source Event with no timestamp)
// to null on the wire rather than serializing 0001-01-01, while a real timestamp passes
// through as a pointer. The domain keeps value time.Time (comparable for the delta-watch
// diff), so this resolver mapping is where absence becomes null.
func TestClusterDataEventTimestampResolversMapZeroToNil(t *testing.T) {
	ctx := context.Background()
	r := &clusterDataEventResolver{&Resolver{}}
	ts := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

	// A real timestamp resolves to a non-nil pointer to that time.
	present := &cluster.ClusterDataEvent{FirstSeen: ts, LastSeen: ts}
	first, err := r.FirstSeen(ctx, present)
	if err != nil || first == nil || !first.Equal(ts) {
		t.Fatalf("FirstSeen(present) = %v, %v; want %v", first, err, ts)
	}
	last, err := r.LastSeen(ctx, present)
	if err != nil || last == nil || !last.Equal(ts) {
		t.Fatalf("LastSeen(present) = %v, %v; want %v", last, err, ts)
	}

	// The zero time (no source timestamp) resolves to nil → null on the wire.
	absent := &cluster.ClusterDataEvent{}
	if got, err := r.FirstSeen(ctx, absent); err != nil || got != nil {
		t.Fatalf("FirstSeen(absent) = %v, %v; want nil", got, err)
	}
	if got, err := r.LastSeen(ctx, absent); err != nil || got != nil {
		t.Fatalf("LastSeen(absent) = %v, %v; want nil", got, err)
	}
}
