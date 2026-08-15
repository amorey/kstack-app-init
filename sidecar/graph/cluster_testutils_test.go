package graph_test

// Shared fixtures for the cluster/cache/data resolver tests. The resolvers
// delegate to a clustersvc.ClusterService, so the tests wire a fakeClusterService
// built from fixtures — that keeps the focus on the GraphQL wire mapping
// (nil→null, conditions/cache shapes) and off whatever backs the service.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/types"
)

// clusterFixture bundles all data for one test cluster record. id is the beehive
// ObjectID; on the wire it is its decimal string ("1", "2", …).
type clusterFixture struct {
	id         types.ClusterID
	spec       types.ClusterSpec
	connStatus types.ClusterStatus
	// syncStats/syncConds belong to the fixture's per-kind sync child. Like the discovery
	// gauges below, the stamps are NOT on the record — they are sampled from the
	// controller through GVRSyncStats — so the fake serves them from here.
	syncStats *types.ClusterCacheGVRSyncStats
	// discStats/discConds belong to the fixture's GVR-discovery child. The gauges are NOT
	// on the record — they are sampled from the controller through GVRDiscoveryStats — so
	// the fake serves them from here to exercise that resolver path.
	discStats *types.ClusterCacheGVRDiscoveryStats
	// Conditions are beehive object rows now, not part of either status block.
	connConds  []types.Condition
	cacheConds []types.Condition
	syncConds  []types.Condition
	discConds  []types.Condition
}

// fakeClusterService implements clustersvc.ClusterService over an in-memory map
// built from fixtures: it joins each fixture's connection + cache status into a
// types.Cluster (exactly as the real service's buildCluster does), so the
// resolver/wire assertions see the same shapes.
type fakeClusterService struct {
	mu          sync.Mutex
	order       []types.ClusterID
	clusters    map[types.ClusterID]*types.Cluster
	caches      []types.ClusterCache             // one active cache per fixture, streamed via Caches().Watch
	discoveries []types.ClusterCacheGVRDiscovery // one GVR-discovery child per cache, streamed via Discovery().Watch
	gvrSyncs    []types.ClusterCacheGVRSync      // per-kind sync records, streamed cache-scoped via Syncs().Watch
	gvrStats    map[types.ClusterCacheGVRSyncID]*types.ClusterCacheGVRSyncStats
	cacheStats  map[types.ClusterCacheID]types.ClusterCacheStats
	discStats   map[types.ClusterCacheGVRDiscoveryID]*types.ClusterCacheGVRDiscoveryStats
	syncEvents  map[types.ClusterCacheGVRSyncID][]types.Event
	events      map[types.ClusterID][]types.Event             // connection-event history, keyed by ClusterID
	cacheEvents map[types.ClusterCacheID][]types.Event        // sync-event history, keyed by ClusterCacheID
	kinds       map[types.ClusterID][]types.ClusterDataKind   // discovered kind catalog, keyed by ClusterID
	dataEvents  map[types.ClusterID][]types.ClusterDataEvent  // cached Kubernetes Events, keyed by ClusterID
	dataObjects map[types.ClusterID][]types.ClusterDataObject // cached objects for one kind, keyed by ClusterID
	watchFail   error                                         // when set, every watch ends with it after its snapshot
}

// The fake mirrors production's shape: one shared state struct, five accessor
// views that carry the family method sets. Each family is asserted separately —
// satisfying ClusterService only proves the accessors exist.
type (
	fakeClusters  struct{ s *fakeClusterService }
	fakeCaches    struct{ s *fakeClusterService }
	fakeDiscovery struct{ s *fakeClusterService }
	fakeSyncs     struct{ s *fakeClusterService }
	fakeData      struct{ s *fakeClusterService }
)

func (f *fakeClusterService) Clusters() clustersvc.Clusters   { return fakeClusters{f} }
func (f *fakeClusterService) Caches() clustersvc.Caches       { return fakeCaches{f} }
func (f *fakeClusterService) Discovery() clustersvc.Discovery { return fakeDiscovery{f} }
func (f *fakeClusterService) Syncs() clustersvc.Syncs         { return fakeSyncs{f} }
func (f *fakeClusterService) Data() clustersvc.Data           { return fakeData{f} }

var (
	_ clustersvc.ClusterService = (*fakeClusterService)(nil)
	_ clustersvc.Clusters       = fakeClusters{}
	_ clustersvc.Caches         = fakeCaches{}
	_ clustersvc.Discovery      = fakeDiscovery{}
	_ clustersvc.Syncs          = fakeSyncs{}
	_ clustersvc.Data           = fakeData{}
)

// Fixture ids are distinct per kind. Beehive draws every kind from one
// AUTOINCREMENT sequence, so a cache never shares its cluster's id — and a fixture
// that reused one would let a resolver read the wrong id and still pass.
func fixtureCacheID(id types.ClusterID) types.ClusterCacheID {
	return types.ClusterCacheID(id + 100)
}

func fixtureDiscoveryID(id types.ClusterID) types.ClusterCacheGVRDiscoveryID {
	return types.ClusterCacheGVRDiscoveryID(id + 200)
}

func fixtureSyncID(id types.ClusterID) types.ClusterCacheGVRSyncID {
	return types.ClusterCacheGVRSyncID(id + 300)
}

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{
		clusters:   map[types.ClusterID]*types.Cluster{},
		events:     map[types.ClusterID][]types.Event{},
		discStats:  map[types.ClusterCacheGVRDiscoveryID]*types.ClusterCacheGVRDiscoveryStats{},
		gvrStats:   map[types.ClusterCacheGVRSyncID]*types.ClusterCacheGVRSyncStats{},
		cacheStats: map[types.ClusterCacheID]types.ClusterCacheStats{},
	}
	for _, fx := range fixtures {
		id := fx.id
		f.order = append(f.order, id)
		f.clusters[id] = &types.Cluster{
			ID:         id,
			Spec:       fx.spec,
			Status:     fx.connStatus,
			Conditions: fx.connConds,
		}
		// Caches stream standalone via WatchCaches and are joined client-side.
		// Give each fixture one cache whose ServerUID matches the cluster's
		// identity (the client's active-cache rule).
		f.caches = append(f.caches, types.ClusterCache{
			ID:         fixtureCacheID(id),
			ClusterID:  id,
			ServerUID:  "uid-" + strconv.FormatInt(int64(id), 10),
			Conditions: fx.cacheConds,
		})
		// Each cache gets one per-kind sync child, keyed off the same id — the fixture's
		// per-component sync state.
		f.gvrStats[fixtureSyncID(id)] = fx.syncStats
		// …and its GVR-discovery child, the anchor the per-kind syncs hang off.
		f.discoveries = append(f.discoveries, types.ClusterCacheGVRDiscovery{
			ID:         fixtureDiscoveryID(id),
			CacheID:    fixtureCacheID(id),
			Spec:       types.ClusterCacheGVRDiscoverySpec{Enabled: fx.spec.SyncEnabled},
			Conditions: fx.discConds,
		})
		f.discStats[fixtureDiscoveryID(id)] = fx.discStats
		// …and one per-kind sync record under that anchor, so the cache-scoped watch has
		// something to scope. Deliberately one per cache so a leak across caches is
		// visible as an extra frame.
		f.gvrSyncs = append(f.gvrSyncs, types.ClusterCacheGVRSync{
			ID:          fixtureSyncID(id),
			DiscoveryID: fixtureDiscoveryID(id),
			Spec: types.ClusterCacheGVRSyncSpec{
				Enabled: fx.spec.SyncEnabled, APIVersion: "apps/v1",
				Kind: "Deployment", Resource: "deployments", Namespaced: true,
			},
			Conditions: fx.syncConds,
		})
		f.cacheStats[fixtureCacheID(id)] = types.ClusterCacheStats{
			Exists: true, Bytes: 4096, ObjectCount: 1386, KindCount: 62,
		}
	}
	return f
}

func (f *fakeClusterService) snapshot() []*types.Cluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*types.Cluster, 0, len(f.order))
	for _, id := range f.order {
		if c, ok := f.clusters[id]; ok {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out
}

func (f *fakeClusterService) cacheSnapshot() []types.ClusterCache {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.ClusterCache(nil), f.caches...)
}

func (f fakeClusters) List(context.Context) ([]*types.Cluster, error) {
	return f.s.snapshot(), nil
}

func (f fakeClusters) Get(_ context.Context, id types.ClusterID) (*types.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, nil
	}
	cp := *c
	return &cp, nil
}

// snapshotStream models every object watch the service exposes: replay the current set
// as Added changes, then hold the stream open until ctx ends (a real WatchList never
// completes on its own, and several tests assert exactly that). Each watch differs only
// in how it wraps an item into its Change struct.
//
// A fixture with watchFail set ends every watch with it instead, standing in for a
// source that died mid-stream.
func snapshotStream[T, C any](ctx context.Context, f *fakeClusterService, items []T, wrap func(*T) C) *clustersvc.Stream[C] {
	snap := make([]C, 0, len(items))
	for i := range items {
		item := items[i]
		snap = append(snap, wrap(&item))
	}
	return clustersvc.NewStream(func(out chan<- C) error {
		for _, c := range snap {
			select {
			case out <- c:
			case <-ctx.Done():
				return nil
			}
		}
		if err := f.failure(); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
}

// failure is the injected terminal reason, nil unless a test set one.
func (f *fakeClusterService) failure() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchFail
}

// copySlice returns a copy of src taken under the fake's lock, so a watch's replay can't
// race a concurrent mutation of the fixture.
func copySlice[T any](f *fakeClusterService, src *[]T) []T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]T(nil), *src...)
}

func (f fakeClusters) Watch(ctx context.Context) (*clustersvc.Stream[types.ClusterWatchFrame], error) {
	return snapshotStream(ctx, f.s, f.s.snapshot(), func(c **types.Cluster) types.ClusterWatchFrame {
		return types.ClusterWatchFrame{Type: types.DeltaFrameAdded, Cluster: *c}
	}), nil
}

func (f fakeCaches) Watch(ctx context.Context) (*clustersvc.Stream[types.ClusterCacheWatchFrame], error) {
	return snapshotStream(ctx, f.s, f.s.cacheSnapshot(), func(c *types.ClusterCache) types.ClusterCacheWatchFrame {
		return types.ClusterCacheWatchFrame{Type: types.DeltaFrameAdded, Cache: c}
	}), nil
}

func (f fakeDiscovery) Watch(ctx context.Context) (*clustersvc.Stream[types.ClusterCacheGVRDiscoveryWatchFrame], error) {
	return snapshotStream(ctx, f.s, copySlice(f.s, &f.s.discoveries), func(d *types.ClusterCacheGVRDiscovery) types.ClusterCacheGVRDiscoveryWatchFrame {
		return types.ClusterCacheGVRDiscoveryWatchFrame{Type: types.DeltaFrameAdded, Discovery: d}
	}), nil
}

// WatchSyncHealth folds the fixture's per-kind records per cache, the same way the
// real service does — enough to prove the wire shape and the join key.
func (f fakeCaches) WatchSyncHealth(ctx context.Context) (*clustersvc.Stream[types.ClusterCacheSyncHealth], error) {
	f.s.mu.Lock()
	cacheOf := map[types.ClusterCacheGVRDiscoveryID]types.ClusterCacheID{}
	for i := range f.s.discoveries {
		cacheOf[f.s.discoveries[i].ID] = f.s.discoveries[i].CacheID
	}
	byCache := map[types.ClusterCacheID]*types.ClusterCacheSyncHealth{}
	for i := range f.s.gvrSyncs {
		cacheID := cacheOf[f.s.gvrSyncs[i].DiscoveryID]
		h := byCache[cacheID]
		if h == nil {
			h = &types.ClusterCacheSyncHealth{CacheID: cacheID, Status: types.ConditionTrue, Reason: "Watching"}
			byCache[cacheID] = h
		}
		h.TotalKinds++
		for _, c := range f.s.gvrSyncs[i].Conditions {
			if c.Type == string(types.ConditionSynced) && c.Reason != "Watching" {
				h.Status, h.Reason = c.Status, c.Reason
				h.UnhealthyKindRefs = append(h.UnhealthyKindRefs, types.SyncedKindRef{
					APIVersion: f.s.gvrSyncs[i].Spec.APIVersion,
					Resource:   f.s.gvrSyncs[i].Spec.Resource,
				})
				h.UnhealthyKinds++
			}
		}
	}
	f.s.mu.Unlock()
	verdicts := make([]types.ClusterCacheSyncHealth, 0, len(byCache))
	for _, h := range byCache {
		verdicts = append(verdicts, *h)
	}
	return snapshotStream(ctx, f.s, verdicts, func(h *types.ClusterCacheSyncHealth) types.ClusterCacheSyncHealth {
		return *h
	}), nil
}

// Watch serves only the records whose discovery anchor matches the requested
// cache's, standing in for the real service's owner-edge filter.
func (f fakeSyncs) Watch(ctx context.Context, cacheID types.ClusterCacheID) (*clustersvc.Stream[types.ClusterCacheGVRSyncWatchFrame], error) {
	f.s.mu.Lock()
	var want types.ClusterCacheGVRDiscoveryID
	for i := range f.s.discoveries {
		if f.s.discoveries[i].CacheID == cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var scoped []types.ClusterCacheGVRSync
	for i := range f.s.gvrSyncs {
		if f.s.gvrSyncs[i].DiscoveryID == want {
			scoped = append(scoped, f.s.gvrSyncs[i])
		}
	}
	f.s.mu.Unlock()
	return snapshotStream(ctx, f.s, scoped, func(gs *types.ClusterCacheGVRSync) types.ClusterCacheGVRSyncWatchFrame {
		return types.ClusterCacheGVRSyncWatchFrame{Type: types.DeltaFrameAdded, Sync: gs}
	}), nil
}

// WatchStats emits the fixture's single measurement and then holds the
// stream open, as a gauge with nothing new to report does.
func (f fakeCaches) WatchStats(ctx context.Context, _ types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCacheStats, error) {
	f.s.mu.Lock()
	st := f.s.cacheStats[cacheID]
	f.s.mu.Unlock()
	out := make(chan types.ClusterCacheStats, 1)
	out <- st
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// GVRSyncStatsSnapshot is the whole-map read the rollup folds.
func (f fakeSyncs) SnapshotStats() map[types.ClusterCacheGVRSyncID]types.ClusterCacheGVRSyncStats {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	out := map[types.ClusterCacheGVRSyncID]types.ClusterCacheGVRSyncStats{}
	for id, st := range f.s.gvrStats {
		if st != nil {
			out[id] = *st
		}
	}
	return out
}

// GVRSyncStats stands in for the per-kind controller's in-memory stamps.
func (f fakeSyncs) GetStats(_ context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSyncStats, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.gvrStats[id], nil
}

// GVRDiscoveryStats stands in for the controller's in-memory gauges: a record the
// fixture gave no stats to reads as nil, the "no pass in this process yet" case.
func (f fakeDiscovery) GetStats(_ context.Context, id types.ClusterCacheGVRDiscoveryID) (*types.ClusterCacheGVRDiscoveryStats, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.discStats[id], nil
}

func (f fakeCaches) GetStats(context.Context, types.ClusterID, types.ClusterCacheID) (*types.ClusterCacheStats, error) {
	return &types.ClusterCacheStats{}, nil
}

func (f fakeData) ListKinds(_ context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) ([]types.ClusterDataKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.kinds[clusterID], nil
}

func (f fakeData) WatchKinds(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) (<-chan types.ClusterDataKindWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterDataKind(nil), f.s.kinds[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterDataKindWatchFrame, len(snap))
	for _, k := range snap {
		ch <- types.ClusterDataKindWatchFrame{Type: types.DeltaFrameAdded, Kind: &k}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeData) WatchEvents(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) (<-chan types.ClusterDataEventWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterDataEvent(nil), f.s.dataEvents[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterDataEventWatchFrame, len(snap))
	for _, e := range snap {
		ch <- types.ClusterDataEventWatchFrame{Type: types.DeltaFrameAdded, Event: &e}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeData) WatchObjects(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID, _, _ string) (<-chan types.ClusterDataObjectWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterDataObject(nil), f.s.dataObjects[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterDataObjectWatchFrame, len(snap))
	for _, o := range snap {
		ch <- types.ClusterDataObjectWatchFrame{Type: types.DeltaFrameAdded, Object: &o}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) ListEvents(_ context.Context, id types.ClusterID, _ *string, _ *int) ([]types.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.events[id], nil
}

// WatchObjectEvents serves every record's timeline, as the real service does — one
// reader, whatever kind the id names.
func (f *fakeClusterService) WatchObjectEvents(ctx context.Context, _ types.ObjectID, _ *string) (*clustersvc.Stream[types.EventWatchFrame], error) {
	return snapshotStream(ctx, f, nil, func(e *types.Event) types.EventWatchFrame {
		return types.EventWatchFrame{Type: types.EventFrameRun, Event: e}
	}), nil
}

func (f fakeCaches) Get(_ context.Context, id types.ClusterCacheID) (*types.ClusterCache, error) {
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (f fakeCaches) List(_ context.Context, clusterID *types.ClusterID) ([]*types.ClusterCache, error) {
	var out []*types.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if clusterID == nil || c.ClusterID == *clusterID {
			out = append(out, &c)
		}
	}
	return out, nil
}

func (f fakeCaches) ListEvents(_ context.Context, id types.ClusterCacheID, _ *string, _ *int) ([]types.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.cacheEvents[id], nil
}

func (f fakeSyncs) Get(_ context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSync, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.gvrSyncs {
		if f.s.gvrSyncs[i].ID == id {
			gs := f.s.gvrSyncs[i]
			return &gs, nil
		}
	}
	return nil, nil
}

// List mirrors Watch's scoping: the records whose anchor belongs to this cache, or
// every record when unscoped.
func (f fakeSyncs) List(_ context.Context, cacheID *types.ClusterCacheID) ([]*types.ClusterCacheGVRSync, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var want types.ClusterCacheGVRDiscoveryID
	for i := range f.s.discoveries {
		if cacheID != nil && f.s.discoveries[i].CacheID == *cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var out []*types.ClusterCacheGVRSync
	for i := range f.s.gvrSyncs {
		if cacheID == nil || f.s.gvrSyncs[i].DiscoveryID == want {
			gs := f.s.gvrSyncs[i]
			out = append(out, &gs)
		}
	}
	return out, nil
}

func (f fakeSyncs) ListEvents(_ context.Context, id types.ClusterCacheGVRSyncID, _ *string, _ *int) ([]types.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.syncEvents[id], nil
}

func (f fakeClusters) WatchSchedule(ctx context.Context, _ types.ClusterID) (<-chan types.Schedule, error) {
	ch := make(chan types.Schedule)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) SetEnabled(_ context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, types.ErrNotFound
	}
	c.Spec.Enabled = enabled
	cp := *c
	return &cp, nil
}

func (f fakeClusters) SetSyncEnabled(_ context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, types.ErrNotFound
	}
	c.Spec.SyncEnabled = enabled
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) RetryConnection(_ context.Context, id types.ClusterID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusters[id]; !ok {
		return types.ErrNotFound
	}
	return nil
}

func (f fakeCaches) Clear(_ context.Context, id types.ClusterID) (*types.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, types.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f fakeClusters) Delete(_ context.Context, id types.ClusterID) error {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if _, ok := f.s.clusters[id]; !ok {
		return types.ErrNotFound
	}
	delete(f.s.clusters, id)
	return nil
}

func (f *fakeClusterService) GetConnection(types.ClusterID) *rest.Config { return nil }

// clusterFixtures returns two records: one fully-probed/present (1) and
// one never-probed/orphaned (2), so nullable fields exercise both arms.
func clusterFixtures() []clusterFixture {
	prodName := "Production"
	uid1 := "uid-1"
	ver := "v1.29.3"
	admin := "system:admin"
	discoveredAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	return []clusterFixture{
		{
			id: 1,
			spec: types.ClusterSpec{
				Name:        &prodName,
				SyncEnabled: true,
				Enabled:     true,
				Source:      types.ClusterSpecSource{Kubeconfig: &types.ClusterSpecSourceKubeconfig{Context: "prod"}},
			},
			connStatus: types.ClusterStatus{
				Source: types.ClusterStatusSource{Kubeconfig: &types.ClusterStatusSourceKubeconfig{
					Cluster: "prod-cluster", User: "prod-user",
					IsPresent: true, IsDefault: true,
				}},
				Server:    types.ClusterServer{UID: &uid1, Version: &ver},
				Principal: types.ClusterPrincipal{Username: &admin},
			},
			discStats: &types.ClusterCacheGVRDiscoveryStats{LastDiscoveryAt: discoveredAt, ResourceCount: 42},
			discConds: []types.Condition{
				{Type: "Discovered", Status: types.ConditionTrue, Reason: "Discovered", Liveness: true},
			},
		},
		{
			id: 2,
			spec: types.ClusterSpec{
				Source: types.ClusterSpecSource{Kubeconfig: &types.ClusterSpecSourceKubeconfig{Context: "staging"}},
			},
			connStatus: types.ClusterStatus{
				Source: types.ClusterStatusSource{Kubeconfig: &types.ClusterStatusSourceKubeconfig{
					Cluster: "staging-cluster", User: "staging-user",
				}},
			},
		},
	}
}

// newTestServer returns an httptest.Server backed by a real resolver wired to a
// fakeClusterService built from the fixtures. Cleanup is registered via
// t.Cleanup so callers need not defer Close.
func newTestServer(t *testing.T, fixtures []clusterFixture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: newFakeClusterService(fixtures),
		Auth:       newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)
	return srv
}

// clustersQueryData creates a test server from the default fixtures, POSTs
// query, decodes data, and fails the test on any GraphQL errors.
func clustersQueryData(t *testing.T, query string) map[string]any {
	t.Helper()
	srv := newTestServer(t, clusterFixtures())

	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data   map[string]any
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode response %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	return resp.Data
}

// firstCacheFrame opens clusterCachesWatch and returns the `cache` payload of the
// first Added frame for cluster "1", decoded to a generic map — the cache-side
// analogue of clustersQueryData, used by the wire-shape tests; cache status is
// exposed through clusterCachesWatch, not the Cluster query.
func firstCacheFrame(t *testing.T, srvURL string) map[string]any {
	t.Helper()
	resp := openSSESubscription(t, srvURL, "",
		`subscription { clusterCachesWatch { type cache { id clusterID serverUid `+
			`conditions { type status reason } `+
			`stats { exists bytes objectCount kindCount } } } }`)
	t.Cleanup(func() { resp.Body.Close() })
	events := sseEvents(t, resp)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("cache stream closed before a frame arrived")
			}
			if ev.event != "next" {
				continue
			}
			var frame struct {
				Data struct {
					ClusterCachesWatch struct {
						Type  string         `json:"type"`
						Cache map[string]any `json:"cache"`
					} `json:"clusterCachesWatch"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode cache frame %s: %v", ev.data, err)
			}
			if frame.Data.ClusterCachesWatch.Cache["clusterID"] == "1" {
				return frame.Data.ClusterCachesWatch.Cache
			}
		case <-deadline:
			t.Fatal("timed out waiting for cluster 1 cache frame")
		}
	}
}
