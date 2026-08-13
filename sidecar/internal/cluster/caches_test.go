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

// White-box (package cluster): the service test seeds beehive objects directly and
// exercises the data/mutation/watch surface in isolation from the (network-touching)
// real controllers, using the shared helpers in testutil_test.go.
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// WatchCaches streams each ClusterCache standalone: the snapshot carries an Added
// change per cache with its parent ClusterID resolved from the owner edge, its
// ServerUID, and its conditions. The kind has no status (it measures nothing itself), and
// active-ness is a client-side join, so neither is asserted here.
// Get is the query entrypoint into a cache record (the GraphQL clusterCache field),
// so it must resolve the owner edge the same way the watch does — a record without
// its ClusterID cannot be joined to its cluster. An unknown id is (nil, nil): the
// schema types the field nullable, and a stale id from a closed window is not an error.
func TestCachesGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	got, err := s.Caches().Get(ctx, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, domain.ClusterCacheID(cacheID), got.ID)
	assert.Equal(t, id, got.ClusterID) // resolved from the owner edge
	assert.Equal(t, uid, got.ServerUID)

	missing, err := s.Caches().Get(ctx, domain.ClusterCacheID(cacheID+9999))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

func TestServiceWatchCachesEmitsCaches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, cacheCtl := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// Give the ClusterCache its coarse Synced condition, as its controller would.
	cacheObj, err := s.cacheClient.Get(ctx, cacheID)
	require.NoError(t, err)
	require.NoError(t, cacheCtl.SetConditions(ctx, cacheObj.ID, []domain.Condition{
		domain.LiveCondition(domain.ConditionSynced, domain.ConditionFalse, domain.ReasonSyncing, ""),
	}))

	ch, err := s.Caches().Watch(ctx)
	require.NoError(t, err)

	// WatchList replays current state on subscribe (conflated per object), so drain
	// Added changes until the condition lands.
	deadline := time.After(2 * time.Second)
	for {
		ev := recvBy(t, ch, deadline)
		assert.Equal(t, domain.ChangeAdded, ev.Type)
		require.NotNil(t, ev.Cache)
		assert.Equal(t, domain.ClusterCacheID(cacheID), ev.Cache.ID)
		assert.Equal(t, id, ev.Cache.ClusterID) // resolved from the owner edge
		assert.Equal(t, uid, ev.Cache.ServerUID)
		if cond := domain.FindCondition(ev.Cache.Conditions, domain.ConditionSynced); cond != nil {
			assert.Equal(t, domain.ReasonSyncing, cond.Reason)
			return
		}
	}
}

// WatchCacheSyncHealth folds a cache's per-kind records into one verdict — the reading an
// always-mounted consumer needs, since the per-kind stream is a hundred-plus records per
// cache and no single child's verdict is the cache's.
//
// The fold must be dominated by the worst kind: ninety-nine healthy kinds and one whose
// watch is wedged is not a healthy cache, and reading any one child would call it either
// way at random.

func TestServiceClearCacheDeletesCacheAndReturnsCluster(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// cacheCtrl is nil in this harness, so ClearCache deletes the on-disk cache (a no-op
	// here) and returns the record without restarting an engine. The engine-restart path
	// is covered in cache_controller_test.go.
	c, err := s.Caches().Clear(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, id, c.ID)

	stats, err := s.Caches().GetStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

// Clearing a cache is drain → delete → restart, and past the drain it must finish. A
// client that abandons the mutation midway — a closed window, a navigation — would
// otherwise leave the cache drained, its files deleted, and no workers rebuilt, with
// nothing else that ever rebuilds them.

// Clearing a cache is drain → delete → restart, and past the drain it must finish. A
// client that abandons the mutation midway — a closed window, a navigation — would
// otherwise leave the cache drained, its files deleted, and no workers rebuilt, with
// nothing else that ever rebuilds them.
func TestServiceClearCacheFinishesWhenTheRequestIsAbandoned(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")

	// Open the cache so there is a file to delete, and confirm it is really there.
	ref, found, err := s.cacheRef(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	_, err = s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	_, exists := s.cacheManager.CacheBytes(ref)
	require.True(t, exists)

	// Abandon the request at the last read before the destructive part — which is where a
	// real client disappearing hurts, and the only place a cancellation can be delivered
	// deterministically. Failing the reads themselves would prove nothing: nothing has
	// happened yet at that point.
	aborted, cancel := context.WithCancel(ctx)
	s.cacheClient = &cancelOnLookupCacheClient{Client: s.cacheClient, cancel: cancel}

	c, err := s.Caches().Clear(aborted, id)
	require.NoError(t, err, "an abandoned request must not abort the clear")
	require.NotNil(t, c)

	stats, err := s.Caches().GetStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the cache files must have been deleted")
}

// cancelOnLookupCacheClient abandons the request the moment ClearCache resolves the cache
// it is about to delete — the last read before the drain.

// cancelOnLookupCacheClient abandons the request the moment ClearCache resolves the cache
// it is about to delete — the last read before the drain.
type cancelOnLookupCacheClient struct {
	beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus]
	cancel context.CancelFunc
}

func (c *cancelOnLookupCacheClient) GetByName(
	ctx context.Context, name string, loads ...beehive.LoadOption,
) (*beehive.Object[domain.ClusterCacheSpec, domain.ClusterCacheStatus], error) {
	obj, err := c.Client.GetByName(ctx, name, loads...)
	c.cancel()
	return obj, err
}

// CacheStats rolls the kind catalog's per-kind counts up into ObjectCount/KindCount
// (the whole-cache totals the sync-status summary shows): only kinds with at least
// one cached object count, and events are excluded (not a catalog kind).

// CacheStats rolls the kind catalog's per-kind counts up into ObjectCount/KindCount
// (the whole-cache totals the sync-status summary shows): only kinds with at least
// one cached object count, and events are excluded (not a catalog kind).
func TestServiceCacheStatsRollup(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	// Populate the active cache's on-disk DB: an advertised kind catalog (Pod,
	// Deployment, plus an advertised-but-empty Node), the objects the triggers
	// count (2 Pods, 1 Deployment), and one event that must not be counted.
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	catalog := func(apiVersion, kind, resource, scope string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, 0, NULL)`,
			apiVersion, kind, resource, scope)
		require.NoError(t, err)
	}
	catalog("v1", "Pod", "pods", "Namespaced")
	catalog("apps/v1", "Deployment", "deployments", "Namespaced")
	catalog("v1", "Node", "nodes", "Cluster")

	at := time.Now().UnixMilli()
	insert := func(objUID, apiVersion, kind string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
			   created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'default', ?, '1', ?, ?, ?)`,
			objUID, apiVersion, kind, objUID, at, at, emptyRawJSON(t))
		require.NoError(t, err)
	}
	insert("p1", "v1", "Pod")
	insert("p2", "v1", "Pod")
	insert("d1", "apps/v1", "Deployment")
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
		 VALUES('e1', 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)`, at, at, at)
	require.NoError(t, err)

	stats, err := s.Caches().GetStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.True(t, stats.Exists)
	assert.Equal(t, 2, stats.KindCount, "Pod and Deployment; the empty Node kind and events excluded")
	assert.Equal(t, 3, stats.ObjectCount, "2 Pods + 1 Deployment; the event is excluded")
}

// The Events kind now carries a real count in kind_counts (maintained by triggers
// on the events table), and /apis discovery advertises it in kind_catalog — so it
// appears in Kinds with a non-zero count. CacheStats must still exclude it
// from the whole-cache object totals: events aren't objects, and ObjectCount is
// documented to leave them out. (TestServiceCacheStatsRollup above is insulated
// only because it inserts no Event catalog row; this one adds it to exercise the
// exclusion.)

// The Events kind now carries a real count in kind_counts (maintained by triggers
// on the events table), and /apis discovery advertises it in kind_catalog — so it
// appears in Kinds with a non-zero count. CacheStats must still exclude it
// from the whole-cache object totals: events aren't objects, and ObjectCount is
// documented to leave them out. (TestServiceCacheStatsRollup above is insulated
// only because it inserts no Event catalog row; this one adds it to exercise the
// exclusion.)
func TestServiceCacheStatsExcludesEventKind(t *testing.T) {
	ctx := context.Background()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	const uid = "kube-system-uid"
	cacheID := seedActiveCache(t, s, coreCC, id, uid)

	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)
	catalog := func(apiVersion, kind, resource, scope string) {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, 0, NULL)`,
			apiVersion, kind, resource, scope)
		require.NoError(t, err)
	}
	catalog("apps/v1", "Deployment", "deployments", "Namespaced")
	catalog("v1", "Event", "events", "Namespaced")

	at := time.Now().UnixMilli()
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES ('d1', 'apps/v1', 'Deployment', 'default', 'd1', '1', ?, ?, ?)`, at, at, emptyRawJSON(t))
	require.NoError(t, err)
	for _, uid := range []string{"e1", "e2"} {
		_, err = cdb.Writer().ExecContext(ctx,
			`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
			 VALUES(?, 'Normal', 'Test', 'hello', ?, ?, 1, x'7b7d', ?)`, uid, at, at, at)
		require.NoError(t, err)
	}

	stats, err := s.Caches().GetStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	assert.True(t, stats.Exists)
	assert.Equal(t, 1, stats.KindCount, "only Deployment; the Event kind is excluded")
	assert.Equal(t, 1, stats.ObjectCount, "only the Deployment; the 2 events are excluded")
}

// recvKindChange reads one delta off the ClusterDataKindsWatch stream, failing on
// timeout or an unexpected close.

// ClusterCacheEvents / ClusterCacheEventsWatch are the ClusterCache-kind counterparts:
// same generic reader/watch, but against the cache client and keyed by the
// ClusterCache's own ObjectID (the sync-event timeline under category "sync"). Asserting
// via the interface value locks the public API shape.
func TestClusterCacheEventsPublicSurface(t *testing.T) {
	ctx := t.Context()
	s, coreCC, cacheCC := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")
	cacheOID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")

	const cat = "sync"
	require.NoError(t, cacheCC.AddEvent(ctx, cacheOID, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "Watching",
	}))

	// ClusterCacheEvents: point read, filtered to the category, keyed by cache id.
	category := cat
	evs, err := svc.Caches().ListEvents(ctx, domain.ClusterCacheID(cacheOID), &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "Watching", evs[0].Reason)
	assert.Equal(t, beehive.EventNormal, evs[0].Type)

	// ClusterCacheEventsWatch: snapshot replays the existing run, then a live run.
	ch, err := svc.Caches().WatchEvents(ctx, domain.ClusterCacheID(cacheOID), &category)
	require.NoError(t, err)

	e := recv(t, ch)
	assert.Equal(t, "Watching", e.Reason)

	require.NoError(t, cacheCC.AddEvent(ctx, cacheOID, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "SyncFailed", Message: "boom",
	}))
	e = recv(t, ch)
	assert.Equal(t, "SyncFailed", e.Reason)
	assert.Equal(t, "boom", e.Message)
}

// recvStats takes the next value off a stats gauge, failing on timeout.

// recvStats takes the next value off a stats gauge, failing on timeout.
func recvStats(t *testing.T, ch <-chan domain.ClusterCacheStats) domain.ClusterCacheStats {
	t.Helper()
	return testutil.Recv(t, ch, "a stats frame")
}

// TestClusterCacheStatsWatchTracksWrites is the regression for a frozen cache summary.
// CacheStats is a live measurement, but it used to be readable only as a resolver field
// on ClusterCache — and that object stops changing once its sync settles, so the webview
// rendered whatever the cache held at subscribe time (an early, tiny snapshot of a sync
// still in progress) forever. A gauge has to have its own stream.

// TestClusterCacheStatsWatchTracksWrites is the regression for a frozen cache summary.
// CacheStats is a live measurement, but it used to be readable only as a resolver field
// on ClusterCache — and that object stops changing once its sync settles, so the webview
// rendered whatever the cache held at subscribe time (an early, tiny snapshot of a sync
// still in progress) forever. A gauge has to have its own stream.
func TestClusterCacheStatsWatchTracksWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	ch, err := s.Caches().WatchStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	first := recvStats(t, ch)
	assert.True(t, first.Exists)
	assert.Zero(t, first.ObjectCount, "an empty cache reports nothing cached")

	insertObjectCatalog(t, ctx, cdb, "apps/v1", "Deployment", "deployments", "Namespaced")
	insertObject(t, ctx, cdb, "d1", "apps/v1", "Deployment", "default", "web", "1", 100)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")

	got := recvStats(t, ch)
	assert.Equal(t, 1, got.ObjectCount, "a write must move the gauge")
	assert.Equal(t, 1, got.KindCount)
}

// A cache file with nobody holding it still EXISTS, and the gauge is the only thing that
// says so — the webview drives its "Clear cache" action off this stream. Reporting a
// closed cache as nonexistent disabled that action on precisely the rows that need it:
// a cluster whose kube-context left the kubeconfig is never eligible, so no worker ever
// opens its cache, and its whole reason for still being listed is to be reclaimed.

// A cache file with nobody holding it still EXISTS, and the gauge is the only thing that
// says so — the webview drives its "Clear cache" action off this stream. Reporting a
// closed cache as nonexistent disabled that action on precisely the rows that need it:
// a cluster whose kube-context left the kubeconfig is never eligible, so no worker ever
// opens its cache, and its whole reason for still being listed is to be reclaimed.
func TestClusterCacheStatsWatchReportsAnUnopenedCacheOnDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")

	// Write the file, then close it: the cache exists on disk with no handle bound, which
	// is the steady state of an orphaned or paused cluster.
	ref := domain.NewCacheRef(beehive.ObjectID(id), cacheID)
	_, err := s.cacheManager.Open(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, s.cacheManager.Close(ctx, ref.CacheID))

	ch, err := s.Caches().WatchStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	got := recvStats(t, ch)
	assert.True(t, got.Exists, "a cache file on disk exists whether or not anything is syncing it")
	assert.Positive(t, got.Bytes, "and its size is a file stat, readable with no handle")
	assert.Zero(t, got.ObjectCount, "the counts need an open handle, so they stay zero")
}

// A cache whose files are gone reports nothing — the same closed path as above, but with
// no file behind it. This is what a Clear cache leaves behind between the delete and the
// reopen, and what a removed cluster leaves for good.

// A cache whose files are gone reports nothing — the same closed path as above, but with
// no file behind it. This is what a Clear cache leaves behind between the delete and the
// reopen, and what a removed cluster leaves for good.
func TestClusterCacheStatsWatchReportsADeletedCacheAsGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")

	ch, err := s.Caches().WatchStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)

	got := recvStats(t, ch)
	assert.False(t, got.Exists, "no file, no cache")
	assert.Zero(t, got.Bytes)
}

// TestClusterCacheStatsWatchSkipsUnchangedReads pins the dedupe that makes a per-write
// gauge affordable: a busy cluster pings constantly, and a measurement that reads the
// same twice must send nothing.

// TestClusterCacheStatsWatchSkipsUnchangedReads pins the dedupe that makes a per-write
// gauge affordable: a busy cluster pings constantly, and a measurement that reads the
// same twice must send nothing.
func TestClusterCacheStatsWatchSkipsUnchangedReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	cacheID := seedActiveCache(t, s, coreCC, id, "kube-system-uid")
	cdb, err := s.cacheManager.Open(ctx, domain.NewCacheRef(beehive.ObjectID(id), cacheID))
	require.NoError(t, err)

	ch, err := s.Caches().WatchStats(ctx, id, domain.ClusterCacheID(cacheID))
	require.NoError(t, err)
	recvStats(t, ch) // the opening read

	// A ping that changed nothing.
	cdb.ObjectsNotifyResource("v1", "pods")
	select {
	case v := <-ch:
		t.Fatalf("an unchanged read must not emit, got %+v", v)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestWatchGVRSyncsIsScopedToOneCache pins the scoping that makes this stream usable. A
// cache has one sync record per served kind — a hundred or more — so unlike the other
// object watches this one is opened per cache, and must not leak another cache's kinds
// into it.
