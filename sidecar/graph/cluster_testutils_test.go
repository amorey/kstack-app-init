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

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc"
)

// ownerOf reads a record's owner ref out of a decoded JSON response.
func ownerOf(record map[string]any) map[string]any {
	owner, _ := record["owner"].(map[string]any)
	return owner
}

// clusterFixture bundles all data for one test cluster record. id is the beehive
// ObjectID; on the wire it is its decimal string ("1", "2", …).
type clusterFixture struct {
	id         clustersvc.ClusterID
	spec       clustersvc.ClusterSpec
	connStatus clustersvc.ClusterStatus
	// Conditions are beehive object rows, not part of either status block. syncConds
	// belong to the fixture's per-kind sync child.
	connConds  []clustersvc.Condition
	cacheConds []clustersvc.Condition
	syncConds  []clustersvc.Condition
}

// fakeClusterService implements clustersvc.ClusterService over an in-memory map
// built from fixtures: it joins each fixture's connection + cache status into a
// clustersvc.Cluster (exactly as the real service's buildCluster does), so the
// resolver/wire assertions see the same shapes.
type fakeClusterService struct {
	mu          sync.Mutex
	order       []clustersvc.ClusterID
	clusters    map[clustersvc.ClusterID]*clustersvc.Cluster
	caches      []clustersvc.ClusterCache      // one active cache per fixture, streamed via Caches().Watch
	cachedKinds []clustersvc.ClusterCachedKind // per-kind sync records, streamed cache-scoped via CachedKinds().Watch
	cacheStats  map[clustersvc.ClusterCacheID]clustersvc.ClusterCacheStats
	syncEvents  map[clustersvc.ClusterCachedKindID][]clustersvc.Event
	events      map[clustersvc.ClusterID][]clustersvc.Event                   // connection-event history, keyed by ClusterID
	cacheEvents map[clustersvc.ClusterCacheID][]clustersvc.Event              // sync-event history, keyed by ClusterCacheID
	kinds       map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataKind   // discovered kind catalog, keyed by ClusterID
	dataEvents  map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataEvent  // cached Kubernetes Events, keyed by ClusterID
	dataObjects map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataObject // cached objects for one kind, keyed by ClusterID
	watchFail   error                                                         // when set, every watch ends with it after its snapshot
}

// The fake mirrors production's shape: one shared state struct, four accessor
// views that carry the family method sets. Each family is asserted separately —
// satisfying clustersvc.Service only proves the accessors exist.
type (
	fakeClusters    struct{ s *fakeClusterService }
	fakeCaches      struct{ s *fakeClusterService }
	fakeCachedKinds struct{ s *fakeClusterService }
	fakeCachedData  struct{ s *fakeClusterService }
)

func (f *fakeClusterService) Clusters() clustersvc.Clusters { return fakeClusters{f} }
func (f *fakeClusterService) Caches() clustersvc.Caches     { return fakeCaches{f} }
func (f *fakeClusterService) CachedKinds() clustersvc.CachedKinds {
	return fakeCachedKinds{f}
}
func (f *fakeClusterService) CachedData() clustersvc.CachedData { return fakeCachedData{f} }

// The resolvers never drive the lifecycle — the composition root does — so the fake
// satisfies it and nothing more.
func (f *fakeClusterService) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

func (f *fakeClusterService) Close() error { return nil }

var (
	_ clustersvc.Service     = (*fakeClusterService)(nil)
	_ clustersvc.Clusters    = fakeClusters{}
	_ clustersvc.Caches      = fakeCaches{}
	_ clustersvc.CachedKinds = fakeCachedKinds{}
	_ clustersvc.CachedData  = fakeCachedData{}
)

// Fixture ids are distinct per kind. Beehive draws every kind from one
// AUTOINCREMENT sequence, so a cache never shares its cluster's id — and a fixture
// that reused one would let a resolver read the wrong id and still pass.
func fixtureCacheID(id clustersvc.ClusterID) clustersvc.ClusterCacheID {
	return clustersvc.ClusterCacheID(id + 100)
}

func fixtureKindID(id clustersvc.ClusterID) clustersvc.ClusterCachedKindID {
	return clustersvc.ClusterCachedKindID(id + 300)
}

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{
		clusters:   map[clustersvc.ClusterID]*clustersvc.Cluster{},
		events:     map[clustersvc.ClusterID][]clustersvc.Event{},
		cacheStats: map[clustersvc.ClusterCacheID]clustersvc.ClusterCacheStats{},
	}
	for _, fx := range fixtures {
		id := fx.id
		f.order = append(f.order, id)
		f.clusters[id] = &clustersvc.Cluster{
			RecordMeta: clustersvc.RecordMeta{ID: id, Conditions: fx.connConds},
			Spec:       fx.spec,
			Status:     fx.connStatus,
		}
		// Caches stream standalone via WatchCaches and are joined client-side.
		// Give each fixture one cache whose ServerUID matches the cluster's
		// identity (the client's active-cache rule).
		f.caches = append(f.caches, clustersvc.ClusterCache{
			RecordMeta: clustersvc.RecordMeta{ID: fixtureCacheID(id), Conditions: fx.cacheConds},
			Owner:      clustersvc.ObjectRef{ID: id, Kind: "Cluster"},
			Spec:       clustersvc.ClusterCacheSpec{ServerUID: "uid-" + strconv.FormatInt(int64(id), 10)},
		})
		// Each cache gets one per-kind sync record, so the cache-scoped watch has
		// something to scope. Deliberately one per cache so a leak across caches is
		// visible as an extra frame.
		f.cachedKinds = append(f.cachedKinds, clustersvc.ClusterCachedKind{
			RecordMeta: clustersvc.RecordMeta{ID: fixtureKindID(id), Conditions: fx.syncConds},
			Owner:      clustersvc.ObjectRef{ID: fixtureCacheID(id), Kind: "ClusterCache"},
			Spec: clustersvc.ClusterCachedKindSpec{
				APIVersion: "apps/v1", Kind: "Deployment",
				Resource: "deployments", Namespaced: true,
			},
		})
		f.cacheStats[fixtureCacheID(id)] = clustersvc.ClusterCacheStats{
			Exists: true, Bytes: 4096, ObjectCount: 1386, KindCount: 62,
		}
	}
	return f
}

func (f *fakeClusterService) snapshot() []*clustersvc.Cluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*clustersvc.Cluster, 0, len(f.order))
	for _, id := range f.order {
		if c, ok := f.clusters[id]; ok {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out
}

func (f *fakeClusterService) cacheSnapshot() []clustersvc.ClusterCache {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clustersvc.ClusterCache(nil), f.caches...)
}

func (f fakeClusters) List(context.Context) ([]*clustersvc.Cluster, error) {
	return f.s.snapshot(), nil
}

func (f fakeClusters) Get(_ context.Context, id clustersvc.ClusterID) (*clustersvc.Cluster, error) {
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
	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- C) error {
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

func (f fakeClusters) WatchList(ctx context.Context) (*clustersvc.Stream[clustersvc.ClusterWatchFrame], error) {
	return deltaStream(ctx, f.s, f.s.snapshot(), func(c **clustersvc.Cluster) clustersvc.ClusterWatchFrame {
		return clustersvc.ClusterWatchFrame{Type: clustersvc.DeltaFrameAdded, Cluster: *c}
	}, clustersvc.ClusterWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeClusters) Watch(ctx context.Context, id clustersvc.ClusterID) (*clustersvc.Stream[clustersvc.ClusterWatchFrame], error) {
	var rows []*clustersvc.Cluster
	for _, c := range f.s.snapshot() {
		if c.ID == id {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c **clustersvc.Cluster) clustersvc.ClusterWatchFrame {
		return clustersvc.ClusterWatchFrame{Type: clustersvc.DeltaFrameAdded, Cluster: *c}
	}, clustersvc.ClusterWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeCaches) WatchList(ctx context.Context) (*clustersvc.Stream[clustersvc.ClusterCacheWatchFrame], error) {
	return deltaStream(ctx, f.s, f.s.cacheSnapshot(), func(c *clustersvc.ClusterCache) clustersvc.ClusterCacheWatchFrame {
		return clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameAdded, Cache: c}
	}, clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeCaches) WatchByCluster(ctx context.Context, clusterID clustersvc.ClusterID) (*clustersvc.Stream[clustersvc.ClusterCacheWatchFrame], error) {
	var rows []clustersvc.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.Owner.ID == clusterID {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c *clustersvc.ClusterCache) clustersvc.ClusterCacheWatchFrame {
		return clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameAdded, Cache: c}
	}, clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeCaches) Watch(ctx context.Context, id clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCacheWatchFrame], error) {
	var rows []clustersvc.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			rows = append(rows, c)
		}
	}
	return deltaStream(ctx, f.s, rows, func(c *clustersvc.ClusterCache) clustersvc.ClusterCacheWatchFrame {
		return clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameAdded, Cache: c}
	}, clustersvc.ClusterCacheWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

// WatchHealth folds the fixture's per-kind records per cache, the same way the
// real service does — enough to prove the wire shape and the join key.
func (f fakeCaches) WatchHealth(ctx context.Context) (*clustersvc.Stream[clustersvc.ClusterCacheHealth], error) {
	f.s.mu.Lock()
	byCache := map[clustersvc.ClusterCacheID]*clustersvc.ClusterCacheHealth{}
	for i := range f.s.cachedKinds {
		cacheID := f.s.cachedKinds[i].Owner.ID
		h := byCache[cacheID]
		if h == nil {
			h = &clustersvc.ClusterCacheHealth{CacheID: cacheID, Status: clustersvc.ConditionTrue, Reason: "Watching"}
			byCache[cacheID] = h
		}
		h.TotalKinds++
		for _, c := range f.s.cachedKinds[i].Conditions {
			if c.Type == string(clustersvc.ConditionSynced) && c.Reason != "Watching" {
				h.Status, h.Reason = c.Status, c.Reason
				h.UnhealthyKindRefs = append(h.UnhealthyKindRefs, clustersvc.SyncedKindRef{
					APIVersion: f.s.cachedKinds[i].Spec.APIVersion,
					Resource:   f.s.cachedKinds[i].Spec.Resource,
				})
				h.UnhealthyKinds++
			}
		}
	}
	f.s.mu.Unlock()
	verdicts := make([]clustersvc.ClusterCacheHealth, 0, len(byCache))
	for _, h := range byCache {
		verdicts = append(verdicts, *h)
	}
	return gaugeStream(ctx, f.s, verdicts, func(h *clustersvc.ClusterCacheHealth) clustersvc.ClusterCacheHealth {
		return *h
	}), nil
}

// WatchByCache serves only the records the requested cache owns, standing in for the
// real service's owner-edge filter.
func (f fakeCachedKinds) WatchByCache(ctx context.Context, cacheID clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCachedKindWatchFrame], error) {
	f.s.mu.Lock()
	var scoped []clustersvc.ClusterCachedKind
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].Owner.ID == cacheID {
			scoped = append(scoped, f.s.cachedKinds[i])
		}
	}
	f.s.mu.Unlock()
	return deltaStream(ctx, f.s, scoped, func(gs *clustersvc.ClusterCachedKind) clustersvc.ClusterCachedKindWatchFrame {
		return clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameAdded, Kind: gs}
	}, clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeCachedKinds) WatchList(ctx context.Context) (*clustersvc.Stream[clustersvc.ClusterCachedKindWatchFrame], error) {
	return deltaStream(ctx, f.s, copySlice(f.s, &f.s.cachedKinds), func(gs *clustersvc.ClusterCachedKind) clustersvc.ClusterCachedKindWatchFrame {
		return clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameAdded, Kind: gs}
	}, clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

func (f fakeCachedKinds) Watch(ctx context.Context, id clustersvc.ClusterCachedKindID) (*clustersvc.Stream[clustersvc.ClusterCachedKindWatchFrame], error) {
	f.s.mu.Lock()
	var rows []clustersvc.ClusterCachedKind
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].ID == id {
			rows = append(rows, f.s.cachedKinds[i])
		}
	}
	f.s.mu.Unlock()
	return deltaStream(ctx, f.s, rows, func(gs *clustersvc.ClusterCachedKind) clustersvc.ClusterCachedKindWatchFrame {
		return clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameAdded, Kind: gs}
	}, clustersvc.ClusterCachedKindWatchFrame{Type: clustersvc.DeltaFrameBookmark}), nil
}

// WatchSyncStatus expands the fixture's per-kind records for one cache, the counterpart of
// the fold WatchHealth does — enough to prove the wire shape.
func (f fakeCaches) WatchSyncStatus(ctx context.Context, _ clustersvc.ClusterID, cacheID clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCacheSyncStatus], error) {
	f.s.mu.Lock()
	status := clustersvc.ClusterCacheSyncStatus{
		CacheID:   cacheID,
		Discovery: clustersvc.ClusterCacheDiscoveryStatus{Reason: "Discovered"},
	}
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].Owner.ID != cacheID {
			continue
		}
		spec := f.s.cachedKinds[i].Spec
		row := clustersvc.ClusterCacheKindSyncStatus{
			APIVersion: spec.APIVersion, Kind: spec.Kind, Resource: spec.Resource, Reason: "Watching",
		}
		for _, c := range f.s.cachedKinds[i].Conditions {
			if c.Type == string(clustersvc.ConditionSynced) {
				row.Reason, row.Message = c.Reason, c.Message
			}
		}
		status.Kinds = append(status.Kinds, row)
	}
	f.s.mu.Unlock()

	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- clustersvc.ClusterCacheSyncStatus) error {
		select {
		case out <- status:
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	}), nil
}

// WatchStats emits the fixture's single measurement and then holds the
// stream open, as a gauge with nothing new to report does.
func (f fakeCaches) WatchStats(ctx context.Context, _ clustersvc.ClusterID, cacheID clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCacheStats], error) {
	f.s.mu.Lock()
	st := f.s.cacheStats[cacheID]
	f.s.mu.Unlock()
	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- clustersvc.ClusterCacheStats) error {
		select {
		case out <- st:
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	}), nil
}

func (f fakeCachedData) ListKinds(_ context.Context, clusterID clustersvc.ClusterID, _ clustersvc.ClusterCacheID) ([]clustersvc.ClusterCachedDataKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.kinds[clusterID], nil
}

func (f fakeCachedData) WatchKinds(ctx context.Context, clusterID clustersvc.ClusterID, _ clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCachedDataKindWatchFrame], error) {
	f.s.mu.Lock()
	snap := append([]clustersvc.ClusterCachedDataKind(nil), f.s.kinds[clusterID]...)
	f.s.mu.Unlock()
	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- clustersvc.ClusterCachedDataKindWatchFrame) error {
		for _, k := range snap {
			select {
			case out <- clustersvc.ClusterCachedDataKindWatchFrame{Type: clustersvc.DeltaFrameAdded, Kind: &k}:
			case <-ctx.Done():
				return nil
			}
		}
		<-ctx.Done()
		return nil
	}), nil
}

func (f fakeCachedData) WatchEvents(ctx context.Context, clusterID clustersvc.ClusterID, _ clustersvc.ClusterCacheID) (*clustersvc.Stream[clustersvc.ClusterCachedDataEventWatchFrame], error) {
	f.s.mu.Lock()
	snap := append([]clustersvc.ClusterCachedDataEvent(nil), f.s.dataEvents[clusterID]...)
	f.s.mu.Unlock()
	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- clustersvc.ClusterCachedDataEventWatchFrame) error {
		for _, e := range snap {
			select {
			case out <- clustersvc.ClusterCachedDataEventWatchFrame{Type: clustersvc.DeltaFrameAdded, Event: &e}:
			case <-ctx.Done():
				return nil
			}
		}
		<-ctx.Done()
		return nil
	}), nil
}

func (f fakeCachedData) WatchObjects(ctx context.Context, clusterID clustersvc.ClusterID, _ clustersvc.ClusterCacheID, _, _ string) (*clustersvc.Stream[clustersvc.ClusterCachedDataObjectWatchFrame], error) {
	f.s.mu.Lock()
	snap := append([]clustersvc.ClusterCachedDataObject(nil), f.s.dataObjects[clusterID]...)
	f.s.mu.Unlock()
	return clustersvc.NewStream(ctx, func(ctx context.Context, out chan<- clustersvc.ClusterCachedDataObjectWatchFrame) error {
		for _, o := range snap {
			select {
			case out <- clustersvc.ClusterCachedDataObjectWatchFrame{Type: clustersvc.DeltaFrameAdded, Object: &o}:
			case <-ctx.Done():
				return nil
			}
		}
		<-ctx.Done()
		return nil
	}), nil
}

// ListEvents and WatchEvents serve every record's timeline from one reader, as the
// real service does. The fixtures' ids are disjoint across kinds (see fixtureCacheID
// and friends), so the id alone picks the log out.
func (f *fakeClusterService) ListEvents(_ context.Context, id clustersvc.ObjectID, _ *string, _ *int) ([]clustersvc.Event, error) {
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

func (f *fakeClusterService) WatchEvents(ctx context.Context, _ clustersvc.ObjectID, _ *string) (*clustersvc.Stream[clustersvc.EventWatchFrame], error) {
	return deltaStream(ctx, f, nil, func(e *clustersvc.Event) clustersvc.EventWatchFrame {
		return clustersvc.EventWatchFrame{Type: clustersvc.EventFrameRun, Event: e}
	}, clustersvc.EventWatchFrame{Type: clustersvc.EventFrameBookmark}), nil
}

func (f fakeCaches) Get(_ context.Context, id clustersvc.ClusterCacheID) (*clustersvc.ClusterCache, error) {
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (f fakeCaches) List(context.Context) ([]*clustersvc.ClusterCache, error) {
	var out []*clustersvc.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		out = append(out, &c)
	}
	return out, nil
}

func (f fakeCaches) ListByCluster(_ context.Context, clusterID clustersvc.ClusterID) ([]*clustersvc.ClusterCache, error) {
	var out []*clustersvc.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if c.Owner.ID == clusterID {
			out = append(out, &c)
		}
	}
	return out, nil
}

func (f fakeCachedKinds) Get(_ context.Context, id clustersvc.ClusterCachedKindID) (*clustersvc.ClusterCachedKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].ID == id {
			gs := f.s.cachedKinds[i]
			return &gs, nil
		}
	}
	return nil, nil
}

// List is every record, unscoped.
func (f fakeCachedKinds) List(context.Context) ([]*clustersvc.ClusterCachedKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var out []*clustersvc.ClusterCachedKind
	for i := range f.s.cachedKinds {
		gs := f.s.cachedKinds[i]
		out = append(out, &gs)
	}
	return out, nil
}

func (f fakeCachedKinds) ListByCache(_ context.Context, cacheID clustersvc.ClusterCacheID) ([]*clustersvc.ClusterCachedKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var out []*clustersvc.ClusterCachedKind
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].Owner.ID == cacheID {
			gs := f.s.cachedKinds[i]
			out = append(out, &gs)
		}
	}
	return out, nil
}

func (f fakeClusters) WatchSchedule(ctx context.Context, _ clustersvc.ClusterID) (<-chan clustersvc.Schedule, error) {
	ch := make(chan clustersvc.Schedule)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) SetEnabled(_ context.Context, id clustersvc.ClusterID, enabled bool) (*clustersvc.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, clustersvc.ErrNotFound
	}
	c.Spec.Enabled = enabled
	cp := *c
	return &cp, nil
}

func (f fakeClusters) SetSyncEnabled(_ context.Context, id clustersvc.ClusterID, enabled bool) (*clustersvc.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, clustersvc.ErrNotFound
	}
	c.Spec.SyncEnabled = enabled
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) RetryConnection(_ context.Context, id clustersvc.ClusterID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusters[id]; !ok {
		return clustersvc.ErrNotFound
	}
	return nil
}

func (f fakeCachedKinds) SetSyncEnabled(_ context.Context, id clustersvc.ClusterCachedKindID, syncEnabled bool) (*clustersvc.ClusterCachedKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].ID == id {
			f.s.cachedKinds[i].Spec.Paused = !syncEnabled
			cr := f.s.cachedKinds[i]
			return &cr, nil
		}
	}
	return nil, clustersvc.ErrNotFound
}

func (f fakeCachedKinds) Clear(_ context.Context, id clustersvc.ClusterCachedKindID) (*clustersvc.ClusterCachedKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	for i := range f.s.cachedKinds {
		if f.s.cachedKinds[i].ID == id {
			cr := f.s.cachedKinds[i]
			return &cr, nil
		}
	}
	return nil, clustersvc.ErrNotFound
}

func (f fakeCaches) Clear(_ context.Context, id clustersvc.ClusterCacheID) (*clustersvc.ClusterCache, error) {
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, clustersvc.ErrNotFound
}

func (f fakeClusters) Delete(_ context.Context, id clustersvc.ClusterID) error {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if _, ok := f.s.clusters[id]; !ok {
		return clustersvc.ErrNotFound
	}
	delete(f.s.clusters, id)
	return nil
}

func (f *fakeClusterService) AcquireConnection(context.Context, clustersvc.ClusterID) (clustersvc.Lease, error) {
	return nil, nil
}

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
			spec: clustersvc.ClusterSpec{
				Name:        &prodName,
				SyncEnabled: true,
				Enabled:     true,
				Source:      clustersvc.ClusterSpecSource{Kubeconfig: &clustersvc.ClusterSpecSourceKubeconfig{Context: "prod"}},
			},
			connStatus: clustersvc.ClusterStatus{
				Source: clustersvc.ClusterStatusSource{Kubeconfig: &clustersvc.ClusterStatusSourceKubeconfig{
					Cluster: "prod-cluster", User: "prod-user",
					IsPresent: true, IsDefault: true,
				}},
				Server:    clustersvc.ClusterServer{UID: &uid1, Version: &ver},
				Principal: clustersvc.ClusterPrincipal{Username: &admin},
			},
		},
		{
			id: 2,
			spec: clustersvc.ClusterSpec{
				Source: clustersvc.ClusterSpecSource{Kubeconfig: &clustersvc.ClusterSpecSourceKubeconfig{Context: "staging"}},
			},
			connStatus: clustersvc.ClusterStatus{
				Source: clustersvc.ClusterStatusSource{Kubeconfig: &clustersvc.ClusterStatusSourceKubeconfig{
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
		`subscription { clusterCachesWatch { type cache { id owner { id kind } spec { serverUid } `+
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
			if ownerOf(frame.Data.ClusterCachesWatch.Cache)["id"] == "1" {
				return frame.Data.ClusterCachesWatch.Cache
			}
		case <-deadline:
			t.Fatal("timed out waiting for cluster 1 cache frame")
		}
	}
}
