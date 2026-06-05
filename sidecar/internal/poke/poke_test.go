package poke

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// mustRecv reads exactly one Signal from ch within 2s or fails the test.
func mustRecv(t *testing.T, ch <-chan Signal) Signal {
	t.Helper()
	select {
	case s, ok := <-ch:
		require.True(t, ok, "channel closed unexpectedly")
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Signal")
		return Signal{}
	}
}

// mustNotRecv asserts no Signal arrives within 50ms.
func mustNotRecv(t *testing.T, ch <-chan Signal) {
	t.Helper()
	select {
	case s, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		t.Fatalf("expected no Signal, got %+v", s)
	case <-time.After(50 * time.Millisecond):
	}
}

// manualTicker returns a ticker backed by the given channel (injected by tests).
func manualTicker(ch chan time.Time) func(time.Duration) (<-chan time.Time, func()) {
	return func(time.Duration) (<-chan time.Time, func()) {
		return ch, func() {}
	}
}

// A1: a tick arriving within the gap threshold emits no Signal.
func TestWallClock_SmallGap_NoPoke(t *testing.T) {
	tickCh := make(chan time.Time, 1)
	// Tick=10s, GapFactor=2.0 → threshold=20s; Now always returns epoch → gap=0 → no poke.
	b := newWithOptions(
		withTick(10*time.Second),
		withGapFactor(2.0),
		withNow(func() time.Time { return epoch }),
		withTicker(manualTicker(tickCh)),
	)

	ch, cancel := b.Subscribe()
	defer cancel()

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	go b.Run(ctx)

	tickCh <- time.Time{}
	mustNotRecv(t, ch)
}

// A2: a tick arriving past the gap threshold emits exactly one SourceWallClock Signal.
func TestWallClock_LargeGap_Poke(t *testing.T) {
	tickCh := make(chan time.Time, 1)
	// Tick=10s, GapFactor=2.0 → threshold=20s; gap=30s → poke.
	var callN atomic.Int64
	b := newWithOptions(
		withTick(10*time.Second),
		withGapFactor(2.0),
		withNow(func() time.Time {
			if callN.Add(1) == 1 {
				return epoch // first call: seed lastSeen
			}
			return epoch.Add(30 * time.Second) // subsequent: 30s gap
		}),
		withTicker(manualTicker(tickCh)),
	)

	ch, cancel := b.Subscribe()
	defer cancel()

	ctx := t.Context()
	go b.Run(ctx)

	tickCh <- time.Time{}
	s := mustRecv(t, ch)
	assert.Equal(t, SourceWallClock, s.Source)
	mustNotRecv(t, ch) // exactly one poke
}

// A3: Poke fans a Signal out to every active subscriber.
func TestPoke_FanOut(t *testing.T) {
	b := newWithOptions(withNow(func() time.Time { return epoch }))

	ch1, cancel1 := b.Subscribe()
	defer cancel1()
	ch2, cancel2 := b.Subscribe()
	defer cancel2()

	b.Poke(SourceHost)

	s1 := mustRecv(t, ch1)
	s2 := mustRecv(t, ch2)
	assert.Equal(t, SourceHost, s1.Source)
	assert.Equal(t, SourceHost, s2.Source)
	assert.Equal(t, epoch, s1.At)
	assert.Equal(t, epoch, s2.At)
}

// A4: cancelling Run's context closes all subscriber channels.
func TestRun_CtxCancel_ClosesSubscribers(t *testing.T) {
	b := newWithOptions(withNow(func() time.Time { return epoch }))

	ch, cancel := b.Subscribe()
	defer cancel()

	ctx, cancelCtx := context.WithCancel(context.Background())
	go b.Run(ctx)

	cancelCtx()

	select {
	case _, ok := <-ch:
		assert.False(t, ok, "expected subscriber channel to be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscriber channel to close")
	}
}
