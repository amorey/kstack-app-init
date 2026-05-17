package mutationqueue_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/mutationqueue"
)

func strptr(s string) *string { return &s }

func newQ(t *testing.T) *mutationqueue.Queue {
	t.Helper()
	return mutationqueue.New(filepath.Join(t.TempDir(), "mutations.json"))
}

// A fresh queue has nothing pending and Drain is a no-op that never calls
// push.
func TestEmptyQueueDrainsNoop(t *testing.T) {
	q := newQ(t)
	called := false
	if err := q.Drain(context.Background(), func(context.Context, cloud.UpdateInput) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if called {
		t.Fatal("push called on empty queue")
	}
	if p, _ := q.Pending(); p {
		t.Fatal("empty queue reports pending")
	}
}

// Enqueue persists; a successful Drain pushes that input and clears.
func TestEnqueueThenDrainPushesAndClears(t *testing.T) {
	q := newQ(t)
	if err := q.Enqueue(cloud.UpdateInput{Placeholder: strptr("v1")}); err != nil {
		t.Fatal(err)
	}
	if p, _ := q.Pending(); !p {
		t.Fatal("want pending after Enqueue")
	}

	var got string
	if err := q.Drain(context.Background(), func(_ context.Context, in cloud.UpdateInput) error {
		got = *in.Placeholder
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got != "v1" {
		t.Fatalf("pushed %q", got)
	}
	if p, _ := q.Pending(); p {
		t.Fatal("still pending after successful Drain")
	}
}

// Coalesce: a second Enqueue replaces the first; only the latest is
// pushed.
func TestEnqueueCoalescesToLatest(t *testing.T) {
	q := newQ(t)
	_ = q.Enqueue(cloud.UpdateInput{Placeholder: strptr("old")})
	_ = q.Enqueue(cloud.UpdateInput{Placeholder: strptr("new")})

	var pushes []string
	_ = q.Drain(context.Background(), func(_ context.Context, in cloud.UpdateInput) error {
		pushes = append(pushes, *in.Placeholder)
		return nil
	})
	if len(pushes) != 1 || pushes[0] != "new" {
		t.Fatalf("want single push of latest, got %v", pushes)
	}
}

// A failed push leaves the entry queued for the next attempt.
func TestFailedPushKeepsPending(t *testing.T) {
	q := newQ(t)
	_ = q.Enqueue(cloud.UpdateInput{Placeholder: strptr("v")})

	err := q.Drain(context.Background(), func(context.Context, cloud.UpdateInput) error {
		return errors.New("offline")
	})
	if err == nil {
		t.Fatal("want push error surfaced")
	}
	if p, _ := q.Pending(); !p {
		t.Fatal("entry must remain pending after a failed push")
	}
}

// A newer Enqueue that lands while a push is in flight must not be lost:
// Drain only clears if the seq it pushed is still the latest.
func TestEnqueueDuringDrainNotLost(t *testing.T) {
	q := newQ(t)
	_ = q.Enqueue(cloud.UpdateInput{Placeholder: strptr("first")})

	err := q.Drain(context.Background(), func(context.Context, cloud.UpdateInput) error {
		// Simulate a concurrent write arriving mid-push.
		return q.Enqueue(cloud.UpdateInput{Placeholder: strptr("second")})
	})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// "first" pushed OK, but "second" arrived before clear — must survive.
	if p, _ := q.Pending(); !p {
		t.Fatal("newer enqueue lost: nothing pending after drain")
	}
	var got string
	_ = q.Drain(context.Background(), func(_ context.Context, in cloud.UpdateInput) error {
		got = *in.Placeholder
		return nil
	})
	if got != "second" {
		t.Fatalf("want 'second' still pending, got %q", got)
	}
}

// Overlapping Drains (the engine can fire OnConnected on rapid
// Live→drop→Live) must push at most once — single-flight, no double
// network write of the same coalesced mutation.
func TestConcurrentDrainPushesOnce(t *testing.T) {
	q := newQ(t)
	if err := q.Enqueue(cloud.UpdateInput{Placeholder: strptr("v")}); err != nil {
		t.Fatal(err)
	}

	var pushes atomic.Int64
	start := make(chan struct{})
	push := func(context.Context, cloud.UpdateInput) error {
		<-start // hold so both Drains are in flight together
		pushes.Add(1)
		return nil
	}

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Drain(context.Background(), push)
		}()
	}
	time.Sleep(20 * time.Millisecond) // let both enter Drain; one wins
	close(start)
	wg.Wait()

	if pushes.Load() != 1 {
		t.Fatalf("want exactly one push under concurrent Drain, got %d", pushes.Load())
	}
	if p, _ := q.Pending(); p {
		t.Fatal("queue not cleared after the winning drain")
	}
}
