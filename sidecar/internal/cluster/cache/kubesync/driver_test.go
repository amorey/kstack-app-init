// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubesync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// These tests call the driver's own methods, with no worker and no monitor around them, so
// each mechanism is pinned where it lives: the graded liveness stamps, the cookie's epoch
// fence, the error budget's cadence switch, and the backstop deadline. What the worker
// REPORTS off the back of them is tested in kubesync_test.go, and the resync pass itself
// in resync_test.go.

// newTestDriver builds a driver with the periodic re-list disabled, so a test that isn't
// about the backstop can't be woken by it. Pass withResyncPeriod to opt back in.
func newTestDriver(src Source, st Store, seedRV string, opts ...driverOption) *driver {
	return newDriver(src, st, seedRV, "seed", append([]driverOption{withResyncPeriod(0)}, opts...)...)
}

// The three proofs are graded, and the grading only means something if a bare connect
// cannot do a strong proof's job. This walks one worker's whole life through the stamps.
func TestConnectOpensAnEpisodeWithoutRefreshingFreshness(t *testing.T) {
	base := time.Now()
	var now time.Time
	d := newTestDriver(&fakeSource{}, newFakeStore(), "", withNow(func() time.Time { return now }))

	// A watch opens: the no-proof episode starts, but nothing has streamed, so neither
	// freshness stamp moves.
	now = base
	d.markConnect()
	live := d.liveness()
	assert.True(t, live.connectedWithoutProof())
	assert.Equal(t, base, live.firstConnect)
	assert.Zero(t, live.proof)
	assert.Zero(t, live.write)

	// It drops and re-establishes. The episode keeps its ORIGINAL start — this is what
	// ages an open-then-error loop into staleness instead of letting it reset the clock
	// every cycle.
	now = base.Add(time.Minute)
	d.markConnect()
	assert.Equal(t, base, d.liveness().firstConnect)

	// A bookmark lands: a strong proof, so the episode closes and freshness moves. It is
	// not a write, so "last update received" stays untouched.
	now = base.Add(2 * time.Minute)
	d.markProof()
	live = d.liveness()
	assert.False(t, live.connectedWithoutProof())
	assert.Equal(t, base.Add(2*time.Minute), live.proof)
	assert.Zero(t, live.write)

	// A later connect opens a fresh episode, measured from itself.
	now = base.Add(3 * time.Minute)
	d.markConnect()
	assert.Equal(t, base.Add(3*time.Minute), d.liveness().firstConnect)

	// A delta lands: the strongest proof, so it stamps both and closes the episode.
	now = base.Add(4 * time.Minute)
	d.markWrite()
	live = d.liveness()
	assert.False(t, live.connectedWithoutProof())
	assert.Equal(t, base.Add(4*time.Minute), live.write)
	assert.Equal(t, base.Add(4*time.Minute), live.proof)
}

// The server places a bookmark at or after every event preceding it, so advancing the
// cookie past a delta the store hasn't landed would let a restart skip that delta
// permanently. The advance waits for applied to catch up to seen.
func TestBookmarkWaitsForEveryPrecedingDeltaToLand(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	d := newTestDriver(&fakeSource{}, st, "")

	var seen, applied atomic.Int64
	seen.Add(1) // the tap forwarded a delta the apply loop hasn't finished with

	d.onBookmark(ctx, d.watchEpoch, &seen, &applied, "50")
	assert.Empty(t, st.resumeRV(), "cookie must not pass an un-applied delta")

	// The delta lands, so the next bookmark is free to advance.
	applied.Add(1)
	d.onBookmark(ctx, d.watchEpoch, &seen, &applied, "51")
	assert.Equal(t, "51", st.resumeRV())
}

// The bookmark tap runs in a forwarding goroutine nothing joins, so a buffered bookmark
// can surface after its phase ended and Run has begun a full LIST that deliberately
// cleared the cookie. Persisting it there would resurrect a position the rows on disk no
// longer back, and a later failed pass would resume from it.
func TestStragglerBookmarkCannotResurrectTheCookie(t *testing.T) {
	ctx := context.Background()
	st := newFakeStore()
	d := newTestDriver(&fakeSource{}, st, "")
	var seen, applied atomic.Int64

	// While the phase is current, its bookmarks advance the cookie.
	epoch := d.watchEpoch
	d.onBookmark(ctx, epoch, &seen, &applied, "42")
	require.Equal(t, "42", st.resumeRV())

	// The phase returns (watchPhase bumps the epoch) and a full LIST clears the cookie.
	d.watchEpoch++
	require.NoError(t, st.PersistRV(ctx, ""))

	// A bookmark still in flight from the retired phase is fenced out.
	d.onBookmark(ctx, epoch, &seen, &applied, "99")
	assert.Empty(t, st.resumeRV())
}

// A bookmark with no resourceVersion names no position, so there is nothing to persist.
func TestBookmarkWithoutAResourceVersionIsIgnored(t *testing.T) {
	st := newFakeStore()
	d := newTestDriver(&fakeSource{}, st, "")
	var seen, applied atomic.Int64

	d.onBookmark(context.Background(), d.watchEpoch, &seen, &applied, "")
	assert.Empty(t, st.resumeRV())
}

// Crossing the error budget marks the worker stuck once, naming the failure that spent it,
// and re-arms the catch-up report so a later recovery is announced rather than swallowed.
func TestStuckFiresOnceOnTheThresholdCrossing(t *testing.T) {
	d := newTestDriver(&fakeSource{}, newFakeStore(), "")
	var fired []Cause
	d.onStuck = func(c Cause) { fired = append(fired, c) }
	d.sawWatch = true // as if a catch-up had already been reported

	failures := 0
	for range d.stuckThreshold + 3 {
		failures = d.recordFailure(failures, CauseListFailed)
	}

	assert.Equal(t, []Cause{CauseListFailed}, fired, "onStuck fires on the crossing only, not on every later failure")
	assert.True(t, d.isStuck())
	assert.Equal(t, CauseListFailed, d.stuckCause())
	assert.False(t, d.sawWatch, "a recovery after a stuck episode must report its catch-up again")
}

// Once stuck, retries drop to the slow cadence so a permanently broken sync stops
// hammering the API server on the exponential schedule.
func TestRetryDelaySwitchesToTheStuckCadence(t *testing.T) {
	d := newTestDriver(&fakeSource{}, newFakeStore(), "")

	assert.Equal(t, time.Second, d.retryDelay(time.Second))

	failures := 0
	for range d.stuckThreshold {
		failures = d.recordFailure(failures, CauseWatchFailed)
	}
	require.True(t, d.isStuck())

	assert.Equal(t, d.stuckRetryInterval, d.retryDelay(time.Second))
}

// An overdue deadline must arm the re-list timer at zero. A negative duration would fire
// immediately in some timer implementations and never in others; zero fires at once by
// contract.
func TestUntilResyncFloorsAtZeroWhenOverdue(t *testing.T) {
	now := time.Now()
	d := newTestDriver(&fakeSource{}, newFakeStore(), "",
		withNow(func() time.Time { return now }),
		withResyncPeriod(time.Hour),
	)

	assert.Positive(t, d.untilResync())
	assert.False(t, d.resyncDue())

	d.resyncAt = now.Add(-time.Minute)
	assert.Zero(t, d.untilResync())
	assert.True(t, d.resyncDue())
}

// With the periodic re-list disabled there is no deadline at all, so the backstop never
// comes due and never ends a healthy watch.
func TestResyncNeverFallsDueWhenDisabled(t *testing.T) {
	d := newTestDriver(&fakeSource{}, newFakeStore(), "")
	assert.False(t, d.resyncDue())
}

// A stock kube-apiserver reports an unusable position as ResourceExpired, but a
// nonconforming intermediary may answer with a bare 410 Gone. Both mean the same thing,
// and the watch and LIST paths have to agree on it — a disagreement would strand one path
// retrying a position the other had already given up on.
func TestExpiryIsDetectedInBothSpellings(t *testing.T) {
	expired := apierrors.NewResourceExpired("rv too old")
	gone := apierrors.NewGone("rv too old")
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "x", errors.New("nope"))

	assert.True(t, isExpired(&expired.ErrStatus))
	assert.True(t, isExpired(&gone.ErrStatus))
	assert.False(t, isExpired(&forbidden.ErrStatus))

	assert.True(t, listExpired(expired))
	assert.True(t, listExpired(gone))
	assert.False(t, listExpired(forbidden))
	assert.False(t, listExpired(errors.New("connection refused")))
}

// The jitter that spreads re-lists across caches must be stable across restarts, or every
// restart would reshuffle the schedule it exists to keep apart.
func TestJitterFractionIsStableAndBounded(t *testing.T) {
	for _, seed := range []string{"", "cache-1", "cache-2"} {
		f := jitterFraction(seed)
		assert.GreaterOrEqual(t, f, 0.0)
		assert.Less(t, f, 1.0)
		assert.Equal(t, f, jitterFraction(seed), "same seed must map to the same fraction")
	}
	assert.NotEqual(t, jitterFraction("cache-1"), jitterFraction("cache-2"))
}
