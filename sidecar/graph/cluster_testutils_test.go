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
	// Conditions are beehive object rows, not part of either status block. syncConds
	// belong to the fixture's per-kind sync child, discConds to its resource catalog.
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
	mu              sync.Mutex
	order           []types.ClusterID
	clusters        map[types.ClusterID]*types.Cluster
	caches          []types.ClusterCache          // one active cache per fixture, streamed via Caches().Watch
	discoveries     []types.ClusterCachedCatalog  // one resource catalog per cache, streamed via CachedCatalogs().Watch
	cachedResources []types.ClusterCachedResource // per-kind sync records, streamed cache-scoped via CachedResources().Watch
	cacheStats      map[types.ClusterCacheID]types.ClusterCacheStats
	syncEvents      map[types.ClusterCachedResourceID][]types.Event
	events          map[types.ClusterID][]types.Event                   // connection-event history, keyed by ClusterID
	cacheEvents     map[types.ClusterCacheID][]types.Event              // sync-event history, keyed by ClusterCacheID
	kinds           map[types.ClusterID][]types.ClusterCachedDataKind   // discovered kind catalog, keyed by ClusterID
	dataEvents      map[types.ClusterID][]types.ClusterCachedDataEvent  // cached Kubernetes Events, keyed by ClusterID
	dataObjects     map[types.ClusterID][]types.ClusterCachedDataObject // cached objects for one kind, keyed by ClusterID
	watchFail       error                                               // when set, every watch ends with it after its snapshot
}

// The fake mirrors production's shape: one shared state struct, five accessor
// views that carry the family method sets. Each family is asserted separately —
// satisfying clustersvc.Service only proves the accessors exist.
type (
	fakeClusters        struct{ s *fakeClusterService }
	fakeCaches          struct{ s *fakeClusterService }
	fakeCachedCatalogs  struct{ s *fakeClusterService }
	fakeCachedResources struct{ s *fakeClusterService }
	fakeCachedData      struct{ s *fakeClusterService }
)

func (f *fakeClusterService) Clusters() clustersvc.Clusters { return fakeClusters{f} }
func (f *fakeClusterService) Caches() clustersvc.Caches     { return fakeCaches{f} }
func (f *fakeClusterService) CachedCatalogs() clustersvc.CachedCatalogs {
	return fakeCachedCatalogs{f}
}
func (f *fakeClusterService) CachedResources() clustersvc.CachedResources {
	return fakeCachedResources{f}
}
func (f *fakeClusterService) CachedData() clustersvc.CachedData { return fakeCachedData{f} }

// The resolvers never drive the lifecycle — the composition root does — so the fake
// satisfies it and nothing more.
func (f *fakeClusterService) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

func (f *fakeClusterService) Close() error { return nil }

var (
	_ clustersvc.Service         = (*fakeClusterService)(nil)
	_ clustersvc.Clusters        = fakeClusters{}
	_ clustersvc.Caches          = fakeCaches{}
	_ clustersvc.CachedCatalogs  = fakeCachedCatalogs{}
	_ clustersvc.CachedResources = fakeCachedResources{}
	_ clustersvc.CachedData      = fakeCachedData{}
)

// Fixture ids are distinct per kind. Beehive draws every kind from one
// AUTOINCREMENT sequence, so a cache never shares its cluster's id — and a fixture
// that reused one would let a resolver read the wrong id and still pass.
func fixtureCacheID(id types.ClusterID) types.ClusterCacheID {
	return types.ClusterCacheID(id + 100)
}

func fixtureCatalogID(id types.ClusterID) types.ClusterCachedCatalogID {
	return types.ClusterCachedCatalogID(id + 200)
}

func fixtureResourceID(id types.ClusterID) types.ClusterCachedResourceID {
	return types.ClusterCachedResourceID(id + 300)
}

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{
		clusters:   map[types.ClusterID]*types.Cluster{},
		events:     map[types.ClusterID][]types.Event{},
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
		// …and its resource catalog, the anchor the per-kind syncs hang off.
		f.discoveries = append(f.discoveries, types.ClusterCachedCatalog{
			ID:         fixtureCatalogID(id),
			CacheID:    fixtureCacheID(id),
			Spec:       types.ClusterCachedCatalogSpec{Enabled: fx.spec.SyncEnabled},
			Conditions: fx.discConds,
		})
		// …and one per-kind sync record under that anchor, so the cache-scoped watch has
		// something to scope. Deliberately one per cache so a leak across caches is
		// visible as an extra frame.
		f.cachedResources = append(f.cachedResources, types.ClusterCachedResource{
			ID:        fixtureResourceID(id),
			CatalogID: fixtureCatalogID(id),
			Spec: types.ClusterCachedResourceSpec{
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

// deltaStream models every delta watch the service exposes: replay the current set as
// Added frames, close the snapshot with bookmark, then hold the stream open until ctx
// ends (a real watch never completes on its own, and several tests assert exactly
// that). Each watch differs only in how it wraps an item into its frame.
//
// The bookmark rides the stream even when the snapshot is empty — that is the whole
// point of it, and how a consumer tells "nothing here" from "still listing".
//
// A fixture with watchFail set ends every watch with it instead, standing in for a
// source that died mid-stream.
func deltaStream[T, C any](ctx context.Context, f *fakeClusterService, items []T, wrap func(*T) C, bookmark C) *clustersvc.Stream[C] {
	snap := make([]C, 0, len(items)+1)
	for i := range items {
		item := items[i]
		snap = append(snap, wrap(&item))
	}
	snap = append(snap, bookmark)
	return streamOf(ctx, f, snap)
}

// gaugeStream is deltaStream's counterpart for a latest-value gauge: current on
// subscribe, so no bookmark closes anything. WatchHealth is the only one.
func gaugeStream[T, C any](ctx context.Context, f *fakeClusterService, items []T, wrap func(*T) C) *clustersvc.Stream[C] {
	snap := make([]C, 0, len(items))
	for i := range items {
		item := items[i]
		snap = append(snap, wrap(&item))
	}
	return streamOf(ctx, f, snap)
}

func streamOf[C any](ctx context.Context, f *fakeClusterService, frames []C) *clustersvc.Stream[C] {
	return clustersvc.NewStream(func(out chan<- C) error {
		for _, c := range frames {
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

func (f fakeClusters) WatchList(ctx context.Context) (*clustersvc.Stream[types.ClusterWatchFrame], error) {
	return deltaStream(ctx, f.s, f.s.snapshot(), func(c **types.Cluster) types.ClusterWatchFrame {
		return types.ClusterWatchFrame{Type: types.DeltaFrameAdded, Cluster: *c}
	}, types.ClusterWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeClusters) Watch(ctx context.Context, id types.ClusterID) (*clustersvc.Stream[types.ClusterWatchFrame], error) {
	var rows []*types.Cluster
	for _, c := range f.s.snapshot() {
		if c.ID == id {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c **types.Cluster) types.ClusterWatchFrame {
		return types.ClusterWatchFrame{Type: types.DeltaFrameAdded, Cluster: *c}
	}, types.ClusterWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCaches) WatchList(ctx context.Context) (*clustersvc.Stream[types.ClusterCacheWatchFrame], error) {
	return deltaStream(ctx, f.s, f.s.cacheSnapshot(), func(c *types.ClusterCache) types.ClusterCacheWatchFrame {
		return types.ClusterCacheWatchFrame{Type: types.DeltaFrameAdded, Cache: c}
	}, types.ClusterCacheWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCaches) WatchByCluster(ctx context.Context, clusterID types.ClusterID) (*clustersvc.Stream[types.ClusterCacheWatchFrame], error) {
	var rows []types.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.ClusterID == clusterID {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c *types.ClusterCache) types.ClusterCacheWatchFrame {
		return types.ClusterCacheWatchFrame{Type: types.DeltaFrameAdded, Cache: c}
	}, types.ClusterCacheWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCaches) Watch(ctx context.Context, id types.ClusterCacheID) (*clustersvc.Stream[types.ClusterCacheWatchFrame], error) {
	var rows []types.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c *types.ClusterCache) types.ClusterCacheWatchFrame {
		return types.ClusterCacheWatchFrame{Type: types.DeltaFrameAdded, Cache: c}
	}, types.ClusterCacheWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCachedCatalogs) WatchList(ctx context.Context) (*clustersvc.Stream[types.ClusterCachedCatalogWatchFrame], error) {
	return deltaStream(ctx, f.s, copySlice(f.s, &f.s.discoveries), func(d *types.ClusterCachedCatalog) types.ClusterCachedCatalogWatchFrame {
		return types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameAdded, Catalog: d}
	}, types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCachedCatalogs) WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*clustersvc.Stream[types.ClusterCachedCatalogWatchFrame], error) {
	var rows []types.ClusterCachedCatalog
	for _, d := range copySlice(f.s, &f.s.discoveries) {
		if d.CacheID == cacheID {
			rows = append(rows, d)
		}
	}
	return deltaStream(ctx, f.s, rows, func(d *types.ClusterCachedCatalog) types.ClusterCachedCatalogWatchFrame {
		return types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameAdded, Catalog: d}
	}, types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCachedCatalogs) Watch(ctx context.Context, id types.ClusterCachedCatalogID) (*clustersvc.Stream[types.ClusterCachedCatalogWatchFrame], error) {
	var rows []types.ClusterCachedCatalog
	for _, d := range copySlice(f.s, &f.s.discoveries) {
		if d.ID == id {
			rows = append(rows, d)
		}
	}
	return deltaStream(ctx, f.s, rows, func(d *types.ClusterCachedCatalog) types.ClusterCachedCatalogWatchFrame {
		return types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameAdded, Catalog: d}
	}, types.ClusterCachedCatalogWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

// WatchHealth folds the fixture's per-kind records per cache, the same way the
// real service does — enough to prove the wire shape and the join key.
func (f fakeCaches) WatchHealth(ctx context.Context) (*clustersvc.Stream[types.ClusterCacheHealth], error) {
	f.s.mu.Lock()
	cacheOf := map[types.ClusterCachedCatalogID]types.ClusterCacheID{}
	for i := range f.s.discoveries {
		cacheOf[f.s.discoveries[i].ID] = f.s.discoveries[i].CacheID
	}
	byCache := map[types.ClusterCacheID]*types.ClusterCacheHealth{}
	for i := range f.s.cachedResources {
		cacheID := cacheOf[f.s.cachedResources[i].CatalogID]
		h := byCache[cacheID]
		if h == nil {
			h = &types.ClusterCacheHealth{CacheID: cacheID, Status: types.ConditionTrue, Reason: "Watching"}
			byCache[cacheID] = h
		}
		h.TotalKinds++
		for _, c := range f.s.cachedResources[i].Conditions {
			if c.Type == string(types.ConditionSynced) && c.Reason != "Watching" {
				h.Status, h.Reason = c.Status, c.Reason
				h.UnhealthyKindRefs = append(h.UnhealthyKindRefs, types.SyncedKindRef{
					APIVersion: f.s.cachedResources[i].Spec.APIVersion,
					Resource:   f.s.cachedResources[i].Spec.Resource,
				})
				h.UnhealthyKinds++
			}
		}
	}
	f.s.mu.Unlock()
	verdicts := make([]types.ClusterCacheHealth, 0, len(byCache))
	for _, h := range byCache {
		verdicts = append(verdicts, *h)
	}
	return gaugeStream(ctx, f.s, verdicts, func(h *types.ClusterCacheHealth) types.ClusterCacheHealth {
		return *h
	}), nil
}

// WatchByCache serves only the records whose resource catalog matches the requested
// cache's, standing in for the real service's owner-edge filter.
func (f fakeCachedResources) WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*clustersvc.Stream[types.ClusterCachedResourceWatchFrame], error) {
	f.s.mu.Lock()
	var want types.ClusterCachedCatalogID
	for i := range f.s.discoveries {
		if f.s.discoveries[i].CacheID == cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var scoped []types.ClusterCachedResource
	for i := range f.s.cachedResources {
		if f.s.cachedResources[i].CatalogID == want {
			scoped = append(scoped, f.s.cachedResources[i])
		}
	}
	f.s.mu.Unlock()
	return deltaStream(ctx, f.s, scoped, func(gs *types.ClusterCachedResource) types.ClusterCachedResourceWatchFrame {
		return types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameAdded, Resource: gs}
	}, types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCachedResources) WatchList(ctx context.Context) (*clustersvc.Stream[types.ClusterCachedResourceWatchFrame], error) {
	return deltaStream(ctx, f.s, copySlice(f.s, &f.s.cachedResources), func(gs *types.ClusterCachedResource) types.ClusterCachedResourceWatchFrame {
		return types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameAdded, Resource: gs}
	}, types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameBookmark}), nil
}

func (f fakeCachedResources) Watch(ctx context.Context, id types.ClusterCachedResourceID) (*clustersvc.Stream[types.ClusterCachedResourceWatchFrame], error) {
	f.s.mu.Lock()
	var rows []types.ClusterCachedResource
	for i := range f.s.cachedResources {
		if f.s.cachedResources[i].ID == id {
			rows = append(rows, f.s.cachedResources[i])
		}
	}
	f.s.mu.Unlock()
	return deltaStream(ctx, f.s, rows, func(gs *types.ClusterCachedResource) types.ClusterCachedResourceWatchFrame {
		return types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameAdded, Resource: gs}
	}, types.ClusterCachedResourceWatchFrame{Type: types.DeltaFrameBookmark}), nil
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

func (f fakeCachedData) ListKinds(_ context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) ([]types.ClusterCachedDataKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.kinds[clusterID], nil
}

func (f fakeCachedData) WatchKinds(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) (<-chan types.ClusterCachedDataKindWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterCachedDataKind(nil), f.s.kinds[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterCachedDataKindWatchFrame, len(snap))
	for _, k := range snap {
		ch <- types.ClusterCachedDataKindWatchFrame{Type: types.DeltaFrameAdded, Kind: &k}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeCachedData) WatchEvents(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID) (<-chan types.ClusterCachedDataEventWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterCachedDataEvent(nil), f.s.dataEvents[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterCachedDataEventWatchFrame, len(snap))
	for _, e := range snap {
		ch <- types.ClusterCachedDataEventWatchFrame{Type: types.DeltaFrameAdded, Event: &e}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeCachedData) WatchObjects(ctx context.Context, clusterID types.ClusterID, _ types.ClusterCacheID, _, _ string) (<-chan types.ClusterCachedDataObjectWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]types.ClusterCachedDataObject(nil), f.s.dataObjects[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan types.ClusterCachedDataObjectWatchFrame, len(snap))
	for _, o := range snap {
		ch <- types.ClusterCachedDataObjectWatchFrame{Type: types.DeltaFrameAdded, Object: &o}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// ListEvents and WatchEvents serve every record's timeline from one reader, as the
// real service does. The fixtures' ids are disjoint across kinds (see fixtureCacheID
// and friends), so the id alone picks the log out.
func (f *fakeClusterService) ListEvents(_ context.Context, id types.ObjectID, _ *string, _ *int) ([]types.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if evs, ok := f.events[id]; ok {
		return evs, nil
	}
	if evs, ok := f.cacheEvents[id]; ok {
		return evs, nil
	}
	return f.syncEvents[id], nil
}

func (f *fakeClusterService) WatchEvents(ctx context.Context, _ types.ObjectID, _ *string) (*clustersvc.Stream[types.EventWatchFrame], error) {
	return deltaStream(ctx, f, nil, func(e *types.Event) types.EventWatchFrame {
		return types.EventWatchFrame{Type: types.EventFrameRun, Event: e}
	}, types.EventWatchFrame{Type: types.EventFrameBookmark}), nil
}

func (f fakeCaches) Get(_ context.Context, id types.ClusterCacheID) (*types.ClusterCache, error) {
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (f fakeCaches) List(context.Context) ([]*types.ClusterCache, error) {
	var out []*types.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		out = append(out, &c)
	}
	return out, nil
}

func (f fakeCaches) ListByCluster(_ context.Context, clusterID types.ClusterID) ([]*types.ClusterCache, error) {
	var out []*types.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.ClusterID == clusterID {
			out = append(out, &c)
		}
	}
	return out, nil
}

func (f fakeCachedCatalogs) Get(_ context.Context, id types.ClusterCachedCatalogID) (*types.ClusterCachedCatalog, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.discoveries {
		if f.s.discoveries[i].ID == id {
			d := f.s.discoveries[i]
			return &d, nil
		}
	}
	return nil, nil
}

func (f fakeCachedCatalogs) List(context.Context) ([]*types.ClusterCachedCatalog, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var out []*types.ClusterCachedCatalog
	for i := range f.s.discoveries {
		d := f.s.discoveries[i]
		out = append(out, &d)
	}
	return out, nil
}

func (f fakeCachedCatalogs) ListByCache(_ context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedCatalog, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var out []*types.ClusterCachedCatalog
	for i := range f.s.discoveries {
		if f.s.discoveries[i].CacheID == cacheID {
			d := f.s.discoveries[i]
			out = append(out, &d)
		}
	}
	return out, nil
}

func (f fakeCachedResources) Get(_ context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.cachedResources {
		if f.s.cachedResources[i].ID == id {
			gs := f.s.cachedResources[i]
			return &gs, nil
		}
	}
	return nil, nil
}

// List mirrors Watch's scoping: the records whose anchor belongs to this cache, or
// every record when unscoped.
func (f fakeCachedResources) List(context.Context) ([]*types.ClusterCachedResource, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var out []*types.ClusterCachedResource
	for i := range f.s.cachedResources {
		gs := f.s.cachedResources[i]
		out = append(out, &gs)
	}
	return out, nil
}

func (f fakeCachedResources) ListByCache(_ context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedResource, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var want types.ClusterCachedCatalogID
	for i := range f.s.discoveries {
		if f.s.discoveries[i].CacheID == cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var out []*types.ClusterCachedResource
	for i := range f.s.cachedResources {
		if f.s.cachedResources[i].CatalogID == want {
			gs := f.s.cachedResources[i]
			out = append(out, &gs)
		}
	}
	return out, nil
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

func (f fakeCachedResources) Clear(_ context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.cachedResources {
		if f.s.cachedResources[i].ID == id {
			cr := f.s.cachedResources[i]
			return &cr, nil
		}
	}
	return nil, types.ErrNotFound
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
			`conditions { type status reason } } } }`)
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
