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

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// testKindAPIVersion is the api group-version these records claim. The fold reports it
// beside each offending plural, so a consumer can key on the exact kind.
const testKindAPIVersion = "apps/v1"

// syncedRec builds one kind's record carrying a Synced condition.
func syncedRec(resource, reason string, status domain.ConditionStatus) gvrSyncRec {
	return gvrSyncRec{
		apiVersion: testKindAPIVersion,
		resource:   resource,
		conditions: []domain.Condition{domain.LiveCondition(domain.ConditionSynced, status, reason, "")},
	}
}

// resourcesOf reduces a verdict's kind refs to their plurals, which is what most of these
// cases are about; the refs' api groups have their own test below.
func resourcesOf(refs []domain.SyncedKindRef) []string {
	if refs == nil {
		return nil
	}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Resource)
	}
	return out
}

// unconfirmedRec is what beehive serves for a kind whose Synced condition a PREVIOUS
// process wrote: the reason and stamps survive, but the status is downgraded to Unknown
// and the record is flagged so nobody asserts it.
func unconfirmedRec(resource, reason string) gvrSyncRec {
	c := domain.LiveCondition(domain.ConditionSynced, domain.ConditionUnknown, reason, "")
	c.Unconfirmed = true
	return gvrSyncRec{apiVersion: testKindAPIVersion, resource: resource, conditions: []domain.Condition{c}}
}

// The rollup is the reading every cluster row shows, so "healthy" has to mean every kind
// was observed to be healthy — not merely that no kind was observed to be broken. The
// distinction is invisible in steady state and total right after a restart, when beehive
// serves every liveness condition downgraded until its worker re-confirms it: counting
// those kinds toward the total and otherwise ignoring them reported a cache fully synced
// before a single worker had run.
func TestSyncHealthVerdict(t *testing.T) {
	tests := []struct {
		name    string
		recs    []gvrSyncRec
		status  domain.ConditionStatus
		reason  string
		offends []string
	}{
		{
			name:   "no kinds at all — discovery hasn't landed",
			status: domain.ConditionUnknown, reason: domain.ReasonSyncing,
		},
		{
			name:   "every kind confirmed watching",
			recs:   []gvrSyncRec{syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue), syncedRec("deployments", domain.ReasonWatching, domain.ConditionTrue)},
			status: domain.ConditionTrue, reason: domain.ReasonWatching,
		},
		{
			name:   "every kind unconfirmed — the post-restart case",
			recs:   []gvrSyncRec{unconfirmedRec("pods", domain.ReasonWatching), unconfirmedRec("deployments", domain.ReasonWatching)},
			status: domain.ConditionUnknown, reason: domain.ReasonSyncing,
		},
		{
			name:   "one kind still unconfirmed among healthy ones",
			recs:   []gvrSyncRec{syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue), unconfirmedRec("deployments", domain.ReasonWatching)},
			status: domain.ConditionUnknown, reason: domain.ReasonSyncing,
		},
		{
			name:   "a kind with no condition yet is not health either",
			recs:   []gvrSyncRec{syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue), {resource: "deployments"}},
			status: domain.ConditionUnknown, reason: domain.ReasonSyncing,
		},
		{
			name: "an observed failure outranks the unobserved rest",
			recs: []gvrSyncRec{
				unconfirmedRec("pods", domain.ReasonWatching),
				syncedRec("widgets", domain.ReasonSyncFailed, domain.ConditionFalse),
			},
			status: domain.ConditionFalse, reason: domain.ReasonSyncFailed, offends: []string{"widgets"},
		},
		{
			name:   "every kind paused",
			recs:   []gvrSyncRec{syncedRec("pods", domain.ReasonPaused, domain.ConditionFalse), syncedRec("deployments", domain.ReasonPaused, domain.ConditionFalse)},
			status: domain.ConditionFalse, reason: domain.ReasonPaused, offends: []string{"deployments", "pods"},
		},
		{
			// A pause is relayed down a cache's hundred-plus children one at a time, so
			// this is what every frame in between looks like. Calling it Watching published
			// health beside a non-zero unhealthyKinds and no names to explain it.
			name: "some kinds paused, the rest watching",
			recs: []gvrSyncRec{
				syncedRec("pods", domain.ReasonPaused, domain.ConditionFalse),
				syncedRec("deployments", domain.ReasonWatching, domain.ConditionTrue),
			},
			status: domain.ConditionFalse, reason: domain.ReasonPaused, offends: []string{"pods"},
		},
		{
			// A real fault still outranks a pause: the ladder is checked first.
			name: "a failure among the paused",
			recs: []gvrSyncRec{
				syncedRec("pods", domain.ReasonPaused, domain.ConditionFalse),
				syncedRec("widgets", domain.ReasonSyncFailed, domain.ConditionFalse),
			},
			status: domain.ConditionFalse, reason: domain.ReasonSyncFailed, offends: []string{"widgets"},
		},
		{
			name:   "paused beside unconfirmed is not a paused cache",
			recs:   []gvrSyncRec{syncedRec("pods", domain.ReasonPaused, domain.ConditionFalse), unconfirmedRec("deployments", domain.ReasonWatching)},
			status: domain.ConditionUnknown, reason: domain.ReasonSyncing,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var acc syncHealthAcc
			for _, r := range tc.recs {
				acc.add(r, domain.ClusterCacheGVRSyncStats{})
			}

			got := acc.verdict(domain.ClusterCacheID(7))

			assert.Equal(t, tc.status, got.Status)
			assert.Equal(t, tc.reason, got.Reason)
			assert.Equal(t, len(tc.recs), got.TotalKinds, "every kind counts toward the total")
			assert.Equal(t, tc.offends, resourcesOf(got.UnhealthyKindRefs))
		})
	}
}

// UnhealthyKinds answers the schema's "how many of them are not currently Watching", so it
// cannot be the offender list's length: that list resets to a single name whenever a worse
// rank appears, so one SyncFailed beside twenty Stale reported one.
func TestSyncHealthVerdictCountsEveryKindNotWatching(t *testing.T) {
	var acc syncHealthAcc
	acc.add(syncedRec("widgets", domain.ReasonSyncFailed, domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})
	for i := range 20 {
		acc.add(syncedRec(fmt.Sprintf("stale-%d", i), domain.ReasonStale, domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})
	}
	acc.add(syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue), domain.ClusterCacheGVRSyncStats{})

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, domain.ReasonSyncFailed, got.Reason, "the worst reason still dominates")
	assert.Equal(t, []string{"widgets"}, resourcesOf(got.UnhealthyKindRefs), "which still names only the worst")
	assert.Equal(t, 21, got.UnhealthyKinds, "but the COUNT is every kind not watching")
	assert.Equal(t, 22, got.TotalKinds)
}

// A paused kind is not watching either, so it counts — "0 of N syncing" is the honest
// reading of a fully paused cache.
func TestSyncHealthVerdictCountsPausedAsNotWatching(t *testing.T) {
	var acc syncHealthAcc
	acc.add(syncedRec("pods", domain.ReasonPaused, domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})
	acc.add(syncedRec("deployments", domain.ReasonPaused, domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, domain.ReasonPaused, got.Reason)
	assert.Equal(t, 2, got.UnhealthyKinds)
	assert.Equal(t, []string{"deployments", "pods"}, resourcesOf(got.UnhealthyKindRefs),
		"the count and the names must describe the same set")
}

// An unconfirmed kind is not currently Watching either — nobody in this process has
// observed it Watching, which is the whole reason the verdict falls to Unknown/Syncing. So
// it counts. Right after a restart every kind is in that state, and reporting 0 unhealthy
// beside an Unknown verdict was a frame whose two halves disagreed: the schema says the
// count is "how many of them are not currently Watching", and none of them were.
func TestSyncHealthVerdictCountsUnconfirmedAsNotWatching(t *testing.T) {
	var acc syncHealthAcc
	acc.add(unconfirmedRec("pods", domain.ReasonWatching), domain.ClusterCacheGVRSyncStats{})
	acc.add(unconfirmedRec("deployments", domain.ReasonWatching), domain.ClusterCacheGVRSyncStats{})
	acc.add(gvrSyncRec{resource: "widgets"}, domain.ClusterCacheGVRSyncStats{}) // never reported at all

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, domain.ConditionUnknown, got.Status)
	assert.Equal(t, domain.ReasonSyncing, got.Reason)
	assert.Equal(t, 3, got.TotalKinds)
	assert.Equal(t, 3, got.UnhealthyKinds, "nothing has been observed watching")
	assert.Empty(t, got.UnhealthyKindRefs, "unobserved is not the same as at fault")
}

// The schema tells consumers to treat an unknown Synced reason as degraded, and this fold
// is a consumer. Ranking it zero made it invisible — counted toward the total, matching no
// branch, and so folding into a healthy Watching verdict.
func TestSyncHealthVerdictTreatsUnknownReasonsAsDegraded(t *testing.T) {
	var acc syncHealthAcc
	acc.add(syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue), domain.ClusterCacheGVRSyncStats{})
	acc.add(syncedRec("widgets", "SomeFutureReason", domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, domain.ConditionFalse, got.Status, "an unrecognized reason must not read as healthy")
	assert.Equal(t, "SomeFutureReason", got.Reason, "and it is reported verbatim")
	assert.Equal(t, []string{"widgets"}, resourcesOf(got.UnhealthyKindRefs))
	assert.Equal(t, 1, got.UnhealthyKinds)
}

// The offenders are named by kind, not by plural alone. A CRD may reuse a built-in's
// plural under its own api group, and a consumer that keys on the first offender — the
// panel opens that kind's transition timeline — would otherwise pick whichever record
// happened to match the bare plural, showing a healthy kind's history under the failing
// kind's heading.
func TestSyncHealthVerdictNamesOffendersByExactKind(t *testing.T) {
	var acc syncHealthAcc
	acc.add(gvrSyncRec{
		apiVersion: "gateway.networking.k8s.io/v1", resource: "gateways",
		conditions: []domain.Condition{domain.LiveCondition(domain.ConditionSynced, domain.ConditionTrue, domain.ReasonWatching, "")},
	}, domain.ClusterCacheGVRSyncStats{})
	acc.add(gvrSyncRec{
		apiVersion: "example.com/v1", resource: "gateways",
		conditions: []domain.Condition{domain.LiveCondition(domain.ConditionSynced, domain.ConditionFalse, domain.ReasonSyncFailed, "")},
	}, domain.ClusterCacheGVRSyncStats{})

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, []domain.SyncedKindRef{{APIVersion: "example.com/v1", Resource: "gateways"}},
		got.UnhealthyKindRefs, "only the CRD is failing, and the ref says which one it is")
}

// An unknown reason ranks at the bottom, so a reason this process DOES understand still
// dominates the verdict.
func TestSyncHealthVerdictUnknownReasonDoesNotMaskAKnownFailure(t *testing.T) {
	var acc syncHealthAcc
	acc.add(syncedRec("widgets", "SomeFutureReason", domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})
	acc.add(syncedRec("pods", domain.ReasonSyncFailed, domain.ConditionFalse), domain.ClusterCacheGVRSyncStats{})

	got := acc.verdict(domain.ClusterCacheID(7))

	assert.Equal(t, domain.ReasonSyncFailed, got.Reason)
	assert.Equal(t, []string{"pods"}, resourcesOf(got.UnhealthyKindRefs))
}

// A cache whose discovery anchor is gone is gone: its verdict must leave the published
// snapshot rather than be recomputed into a permanent "no kinds yet" Unknown.
//
// Keeping it would tell every later subscriber about caches that no longer exist, and a
// delete/recreate cycle — a physical-cluster migration turns caches over — would grow the
// fold and every snapshot it publishes without bound.
func TestSyncHealthFoldDropsCachesWithNoAnchor(t *testing.T) {
	hub := watch.New(syncHealthSnapshot{})
	defer hub.Close()

	const cacheID = domain.ClusterCacheID(7)
	const discoveryID = domain.ClusterCacheGVRDiscoveryID(70)
	f := &syncHealthFold{
		syncs:       map[beehive.ObjectID]gvrSyncRec{},
		cacheOf:     map[domain.ClusterCacheGVRDiscoveryID]domain.ClusterCacheID{discoveryID: cacheID},
		byDiscovery: map[domain.ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}{},
		discoveriesOf: map[domain.ClusterCacheID]map[domain.ClusterCacheGVRDiscoveryID]struct{}{
			cacheID: {discoveryID: {}},
		},
		published: syncHealthSnapshot{},
		dirty:     map[domain.ClusterCacheID]struct{}{},
		stats: func() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats {
			return map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats{}
		},
		out: hub.Sender(),
	}

	// A cache with one healthy kind publishes a verdict.
	f.syncs[1] = syncedRec("pods", domain.ReasonWatching, domain.ConditionTrue)
	f.byDiscovery[discoveryID] = map[beehive.ObjectID]struct{}{1: {}}
	f.mark(cacheID)
	f.flush()
	require.Contains(t, f.published, cacheID)

	// Its subtree is collected: the sync child goes, then the anchor.
	delete(f.syncs, 1)
	f.unlinkSync(1, discoveryID)
	f.unlinkDiscovery(discoveryID, cacheID)
	delete(f.cacheOf, discoveryID)
	f.flush()

	assert.NotContains(t, f.published, cacheID,
		"a cache with no discovery anchor must leave the snapshot, not linger as Unknown")
	assert.Empty(t, f.published)
}

// markAll re-marks everything the fold knows about (the 10s tick, which re-reads stamps
// nothing else announces). It must not resurrect a cache that has already been dropped.
func TestSyncHealthFoldTickDoesNotResurrectDroppedCaches(t *testing.T) {
	hub := watch.New(syncHealthSnapshot{})
	defer hub.Close()

	f := &syncHealthFold{
		syncs:         map[beehive.ObjectID]gvrSyncRec{},
		cacheOf:       map[domain.ClusterCacheGVRDiscoveryID]domain.ClusterCacheID{},
		byDiscovery:   map[domain.ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}{},
		discoveriesOf: map[domain.ClusterCacheID]map[domain.ClusterCacheGVRDiscoveryID]struct{}{},
		published:     syncHealthSnapshot{},
		dirty:         map[domain.ClusterCacheID]struct{}{},
		stats: func() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats {
			return map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats{}
		},
		out: hub.Sender(),
	}

	f.markAll()
	f.flush()

	assert.Empty(t, f.published, "a fold that knows no caches must publish none")
}

// syncSrcClient hands the fold a source channel the test controls, and records the context
// the fold registered its watch under — the two things needed to observe what happens when
// the fold ends on its own rather than by our stop func.
type syncSrcClient struct {
	beehive.Client[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]
	ctx chan context.Context
	src chan beehive.ObjectChange[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]
}

func (c *syncSrcClient) WatchList(ctx context.Context, _ ...beehive.WatchOption) (
	*beehive.ObjectListStream[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
	error,
) {
	c.ctx <- ctx
	return &beehive.ObjectListStream[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]{
		Changes: c.src,
	}, nil
}

// A fold can end without anybody calling its stop func: beehive terminates a watch
// (ErrWatchTooOld during a cold-sync write storm), or a source channel closes. Both
// subscribers are registered against the fold's own context, and forgetSyncHealthFold then
// clears the cached stop func — so unless the fold cancels itself on the way out, nothing
// ever will, and each recurrence strands another pair of fleet-wide watches for the life of
// the process.
func TestSyncHealthFoldCancelsItsWatchesWhenItEndsOnItsOwn(t *testing.T) {
	s, _, _ := newServiceTest(t)

	fake := &syncSrcClient{
		Client: s.gvrSyncClient,
		ctx:    make(chan context.Context, 1),
		src:    make(chan beehive.ObjectChange[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]),
	}
	s.gvrSyncClient = fake

	hub, foldErr, stop, done, err := s.startSyncHealthFold()
	require.NoError(t, err)
	t.Cleanup(stop)
	s.syncHealthMu.Lock()
	s.syncHealth, s.syncHealthErr, s.syncHealthStop, s.syncHealthDone = hub, foldErr, stop, done
	s.syncHealthMu.Unlock()

	watchCtx := testutil.Recv(t, fake.ctx, "the fold to open its watches")
	require.NoError(t, watchCtx.Err(), "the watches are live while the fold runs")

	// End the fold the way beehive would, without going through the stop func.
	close(fake.src)

	testutil.Wait(t, watchCtx.Done(), "a fold that ended on its own to cancel the watches it opened")
}

// The fold is stopped before beehive at shutdown. A subscriber arriving in that window must
// NOT restart it: the lazy start would read "no hub" as "not started yet" and build a fresh
// fold on a background context whose canceller had just been discarded — holding beehive
// watches while beehive is being torn down, with nothing left to stop it.
func TestSyncHealthReceiverDoesNotRestartAfterShutdown(t *testing.T) {
	s := &Service{}

	s.stopSyncHealthFold(context.Background())

	rx, _, err := s.syncHealthReceiver()
	require.Error(t, err, "a post-shutdown subscribe must fail rather than resurrect the fold")
	assert.Nil(t, rx)
	assert.Nil(t, s.syncHealth, "and must leave no hub behind")
}

// The fold caches its hub so every subscriber shares one. If the fold ends on its own — a
// watch beehive terminated, not our Close — the cached hub is closed, and gochan hands a
// pre-closed receiver to everyone who asks afterwards. Leaving it cached therefore ends
// sync status for every window in the process, silently, until a restart. The per-subscriber
// watches already recover by re-subscribing; this must too.
func TestSyncHealthFoldIsRestartableAfterItEndsOnItsOwn(t *testing.T) {
	s := &Service{}
	hub := watch.New(syncHealthSnapshot{})
	s.syncHealth, s.syncHealthStop = hub, func() {}

	// What the fold goroutine's defer does when its watch ends.
	hub.Close()
	s.forgetSyncHealthFold(hub)

	assert.Nil(t, s.syncHealth, "a fold that ended must not stay cached")
	assert.Nil(t, s.syncHealthStop)
	assert.False(t, s.syncHealthClosed, "and the process is not shutting down")
}

// A stop that already replaced the cached hub must not be undone by the old fold's defer
// arriving late.
func TestSyncHealthFoldForgetOnlyClearsItsOwnHub(t *testing.T) {
	s := &Service{}
	old := watch.New(syncHealthSnapshot{})
	current := watch.New(syncHealthSnapshot{})
	defer current.Close()
	s.syncHealth = current

	old.Close()
	s.forgetSyncHealthFold(old)

	assert.Same(t, current, s.syncHealth, "a newer fold's hub must survive an older one ending")
}
