package graph_test

// Shared fixtures for the cluster/cache/data resolver tests. The resolvers
// delegate to a cluster.ClusterService, so the tests wire a fakeClusterService
// built from fixtures — that keeps the focus on the GraphQL wire mapping
// (nil→null, conditions/cache shapes) and off beehive, which the service-level
// tests in internal/cluster already cover.

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
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// clusterFixture bundles all data for one test cluster record. id is the beehive
// ObjectID; on the wire it is its decimal string ("1", "2", …).
type clusterFixture struct {
	id         domain.ClusterID
	spec       domain.ClusterSpec
	connStatus domain.ClusterStatus
	// syncStats/syncConds belong to the fixture's per-kind sync child. Like the discovery
	// gauges below, the stamps are NOT on the record — they are sampled from the
	// controller through GVRSyncStats — so the fake serves them from here.
	syncStats *domain.ClusterCacheGVRSyncStats
	// discStats/discConds belong to the fixture's GVR-discovery child. The gauges are NOT
	// on the record — they are sampled from the controller through GVRDiscoveryStats — so
	// the fake serves them from here to exercise that resolver path.
	discStats *domain.ClusterCacheGVRDiscoveryStats
	// Conditions are beehive object rows now, not part of either status block.
	connConds  []domain.Condition
	cacheConds []domain.Condition
	syncConds  []domain.Condition
	discConds  []domain.Condition
}

// fakeClusterService implements cluster.ClusterService over an in-memory map
// built from fixtures: it joins each fixture's connection + cache status into a
// domain Cluster (exactly as the real service's buildCluster does), so the
// resolver/wire assertions see the same shapes.
type fakeClusterService struct {
	mu          sync.Mutex
	order       []domain.ClusterID
	clusters    map[domain.ClusterID]*domain.Cluster
	caches      []domain.ClusterCache             // one active cache per fixture, streamed via Caches().Watch
	discoveries []domain.ClusterCacheGVRDiscovery // one GVR-discovery child per cache, streamed via Discovery().Watch
	gvrSyncs    []domain.ClusterCacheGVRSync      // per-kind sync records, streamed cache-scoped via Syncs().Watch
	gvrStats    map[domain.ClusterCacheGVRSyncID]*domain.ClusterCacheGVRSyncStats
	cacheStats  map[domain.ClusterCacheID]domain.ClusterCacheStats
	discStats   map[domain.ClusterCacheGVRDiscoveryID]*domain.ClusterCacheGVRDiscoveryStats
	syncEvents  map[domain.ClusterCacheGVRSyncID][]domain.Event
	events      map[domain.ClusterID][]domain.Event             // connection-event history, keyed by ClusterID
	cacheEvents map[domain.ClusterCacheID][]domain.Event        // sync-event history, keyed by ClusterCacheID
	kinds       map[domain.ClusterID][]domain.ClusterDataKind   // discovered kind catalog, keyed by ClusterID
	dataEvents  map[domain.ClusterID][]domain.ClusterDataEvent  // cached Kubernetes Events, keyed by ClusterID
	dataObjects map[domain.ClusterID][]domain.ClusterDataObject // cached objects for one kind, keyed by ClusterID
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

func (f *fakeClusterService) Clusters() cluster.Clusters   { return fakeClusters{f} }
func (f *fakeClusterService) Caches() cluster.Caches       { return fakeCaches{f} }
func (f *fakeClusterService) Discovery() cluster.Discovery { return fakeDiscovery{f} }
func (f *fakeClusterService) Syncs() cluster.Syncs         { return fakeSyncs{f} }
func (f *fakeClusterService) Data() cluster.Data           { return fakeData{f} }

var (
	_ cluster.ClusterService = (*fakeClusterService)(nil)
	_ cluster.Clusters       = fakeClusters{}
	_ cluster.Caches         = fakeCaches{}
	_ cluster.Discovery      = fakeDiscovery{}
	_ cluster.Syncs          = fakeSyncs{}
	_ cluster.Data           = fakeData{}
)

// Fixture ids are distinct per kind. Beehive draws every kind from one
// AUTOINCREMENT sequence, so a cache never shares its cluster's id — and a fixture
// that reused one would let a resolver read the wrong id and still pass.
func fixtureCacheID(id domain.ClusterID) domain.ClusterCacheID {
	return domain.ClusterCacheID(id + 100)
}

func fixtureDiscoveryID(id domain.ClusterID) domain.ClusterCacheGVRDiscoveryID {
	return domain.ClusterCacheGVRDiscoveryID(id + 200)
}

func fixtureSyncID(id domain.ClusterID) domain.ClusterCacheGVRSyncID {
	return domain.ClusterCacheGVRSyncID(id + 300)
}

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{
		clusters:   map[domain.ClusterID]*domain.Cluster{},
		events:     map[domain.ClusterID][]domain.Event{},
		discStats:  map[domain.ClusterCacheGVRDiscoveryID]*domain.ClusterCacheGVRDiscoveryStats{},
		gvrStats:   map[domain.ClusterCacheGVRSyncID]*domain.ClusterCacheGVRSyncStats{},
		cacheStats: map[domain.ClusterCacheID]domain.ClusterCacheStats{},
	}
	for _, fx := range fixtures {
		id := fx.id
		f.order = append(f.order, id)
		f.clusters[id] = &domain.Cluster{
			ID:         id,
			Spec:       fx.spec,
			Status:     fx.connStatus,
			Conditions: fx.connConds,
		}
		// Caches stream standalone via WatchCaches and are joined client-side.
		// Give each fixture one cache whose ServerUID matches the cluster's
		// identity (the client's active-cache rule).
		f.caches = append(f.caches, domain.ClusterCache{
			ID:         fixtureCacheID(id),
			ClusterID:  id,
			ServerUID:  "uid-" + strconv.FormatInt(int64(id), 10),
			Conditions: fx.cacheConds,
		})
		// Each cache gets one per-kind sync child, keyed off the same id — the fixture's
		// per-component sync state.
		f.gvrStats[fixtureSyncID(id)] = fx.syncStats
		// …and its GVR-discovery child, the anchor the per-kind syncs hang off.
		f.discoveries = append(f.discoveries, domain.ClusterCacheGVRDiscovery{
			ID:         fixtureDiscoveryID(id),
			CacheID:    fixtureCacheID(id),
			Spec:       domain.ClusterCacheGVRDiscoverySpec{Enabled: fx.spec.SyncEnabled},
			Conditions: fx.discConds,
		})
		f.discStats[fixtureDiscoveryID(id)] = fx.discStats
		// …and one per-kind sync record under that anchor, so the cache-scoped watch has
		// something to scope. Deliberately one per cache so a leak across caches is
		// visible as an extra frame.
		f.gvrSyncs = append(f.gvrSyncs, domain.ClusterCacheGVRSync{
			ID:          fixtureSyncID(id),
			DiscoveryID: fixtureDiscoveryID(id),
			Spec: domain.ClusterCacheGVRSyncSpec{
				Enabled: fx.spec.SyncEnabled, APIVersion: "apps/v1",
				Kind: "Deployment", Resource: "deployments", Namespaced: true,
			},
			Conditions: fx.syncConds,
		})
		f.cacheStats[fixtureCacheID(id)] = domain.ClusterCacheStats{
			Exists: true, Bytes: 4096, ObjectCount: 1386, KindCount: 62,
		}
	}
	return f
}

func (f *fakeClusterService) snapshot() []*domain.Cluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*domain.Cluster, 0, len(f.order))
	for _, id := range f.order {
		if c, ok := f.clusters[id]; ok {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out
}

func (f *fakeClusterService) cacheSnapshot() []domain.ClusterCache {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.ClusterCache(nil), f.caches...)
}

func (f fakeClusters) List(context.Context) ([]*domain.Cluster, error) {
	return f.s.snapshot(), nil
}

func (f fakeClusters) Get(_ context.Context, id domain.ClusterID) (*domain.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, nil
	}
	cp := *c
	return &cp, nil
}

// snapshotChan models every object watch the service exposes: replay the current set as
// Added changes, then hold the stream open until ctx ends (a real WatchList never
// completes on its own, and several tests assert exactly that). Each watch differs only
// in how it wraps an item into its Change struct.
func snapshotChan[T, C any](ctx context.Context, items []T, wrap func(*T) C) <-chan C {
	ch := make(chan C, len(items))
	for i := range items {
		item := items[i]
		ch <- wrap(&item)
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}

// copySlice returns a copy of src taken under the fake's lock, so a watch's replay can't
// race a concurrent mutation of the fixture.
func copySlice[T any](f *fakeClusterService, src *[]T) []T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]T(nil), *src...)
}

func (f fakeClusters) Watch(ctx context.Context) (<-chan domain.ClusterWatchFrame, error) {
	return snapshotChan(ctx, f.s.snapshot(), func(c **domain.Cluster) domain.ClusterWatchFrame {
		return domain.ClusterWatchFrame{Type: domain.FrameAdded, Cluster: *c}
	}), nil
}

func (f fakeCaches) Watch(ctx context.Context) (<-chan domain.ClusterCacheWatchFrame, error) {
	return snapshotChan(ctx, f.s.cacheSnapshot(), func(c *domain.ClusterCache) domain.ClusterCacheWatchFrame {
		return domain.ClusterCacheWatchFrame{Type: domain.FrameAdded, Cache: c}
	}), nil
}

func (f fakeDiscovery) Watch(ctx context.Context) (<-chan domain.ClusterCacheGVRDiscoveryWatchFrame, error) {
	return snapshotChan(ctx, copySlice(f.s, &f.s.discoveries), func(d *domain.ClusterCacheGVRDiscovery) domain.ClusterCacheGVRDiscoveryWatchFrame {
		return domain.ClusterCacheGVRDiscoveryWatchFrame{Type: domain.FrameAdded, Discovery: d}
	}), nil
}

// WatchSyncHealth folds the fixture's per-kind records per cache, the same way the
// real service does — enough to prove the wire shape and the join key.
func (f fakeCaches) WatchSyncHealth(ctx context.Context) (<-chan domain.ClusterCacheSyncHealth, error) {
	f.s.mu.Lock()
	cacheOf := map[domain.ClusterCacheGVRDiscoveryID]domain.ClusterCacheID{}
	for i := range f.s.discoveries {
		cacheOf[f.s.discoveries[i].ID] = f.s.discoveries[i].CacheID
	}
	byCache := map[domain.ClusterCacheID]*domain.ClusterCacheSyncHealth{}
	for i := range f.s.gvrSyncs {
		cacheID := cacheOf[f.s.gvrSyncs[i].DiscoveryID]
		h := byCache[cacheID]
		if h == nil {
			h = &domain.ClusterCacheSyncHealth{CacheID: cacheID, Status: domain.ConditionTrue, Reason: "Watching"}
			byCache[cacheID] = h
		}
		h.TotalKinds++
		for _, c := range f.s.gvrSyncs[i].Conditions {
			if c.Type == string(domain.ConditionSynced) && c.Reason != "Watching" {
				h.Status, h.Reason = c.Status, c.Reason
				h.UnhealthyKindRefs = append(h.UnhealthyKindRefs, domain.SyncedKindRef{
					APIVersion: f.s.gvrSyncs[i].Spec.APIVersion,
					Resource:   f.s.gvrSyncs[i].Spec.Resource,
				})
				h.UnhealthyKinds++
			}
		}
	}
	f.s.mu.Unlock()
	out := make(chan domain.ClusterCacheSyncHealth, len(byCache)+1)
	for _, h := range byCache {
		out <- *h
	}
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// Watch serves only the records whose discovery anchor matches the requested
// cache's, standing in for the real service's owner-edge filter.
func (f fakeSyncs) Watch(ctx context.Context, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheGVRSyncWatchFrame, error) {
	f.s.mu.Lock()
	var want domain.ClusterCacheGVRDiscoveryID
	for i := range f.s.discoveries {
		if f.s.discoveries[i].CacheID == cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var scoped []domain.ClusterCacheGVRSync
	for i := range f.s.gvrSyncs {
		if f.s.gvrSyncs[i].DiscoveryID == want {
			scoped = append(scoped, f.s.gvrSyncs[i])
		}
	}
	f.s.mu.Unlock()
	return snapshotChan(ctx, scoped, func(gs *domain.ClusterCacheGVRSync) domain.ClusterCacheGVRSyncWatchFrame {
		return domain.ClusterCacheGVRSyncWatchFrame{Type: domain.FrameAdded, Sync: gs}
	}), nil
}

// WatchStats emits the fixture's single measurement and then holds the
// stream open, as a gauge with nothing new to report does.
func (f fakeCaches) WatchStats(ctx context.Context, _ domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheStats, error) {
	f.s.mu.Lock()
	st := f.s.cacheStats[cacheID]
	f.s.mu.Unlock()
	out := make(chan domain.ClusterCacheStats, 1)
	out <- st
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// GVRSyncStatsSnapshot is the whole-map read the rollup folds.
func (f fakeSyncs) SnapshotStats() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	out := map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats{}
	for id, st := range f.s.gvrStats {
		if st != nil {
			out[id] = *st
		}
	}
	return out
}

// GVRSyncStats stands in for the per-kind controller's in-memory stamps.
func (f fakeSyncs) GetStats(_ context.Context, id domain.ClusterCacheGVRSyncID) (*domain.ClusterCacheGVRSyncStats, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.gvrStats[id], nil
}

// GVRDiscoveryStats stands in for the controller's in-memory gauges: a record the
// fixture gave no stats to reads as nil, the "no pass in this process yet" case.
func (f fakeDiscovery) GetStats(_ context.Context, id domain.ClusterCacheGVRDiscoveryID) (*domain.ClusterCacheGVRDiscoveryStats, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.discStats[id], nil
}

func (f fakeCaches) GetStats(context.Context, domain.ClusterID, domain.ClusterCacheID) (*domain.ClusterCacheStats, error) {
	return &domain.ClusterCacheStats{}, nil
}

func (f fakeData) ListKinds(_ context.Context, clusterID domain.ClusterID, _ domain.ClusterCacheID) ([]domain.ClusterDataKind, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.kinds[clusterID], nil
}

func (f fakeData) WatchKinds(ctx context.Context, clusterID domain.ClusterID, _ domain.ClusterCacheID) (<-chan domain.ClusterDataKindWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]domain.ClusterDataKind(nil), f.s.kinds[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan domain.ClusterDataKindWatchFrame, len(snap))
	for _, k := range snap {
		ch <- domain.ClusterDataKindWatchFrame{Type: domain.FrameAdded, Kind: &k}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeData) WatchEvents(ctx context.Context, clusterID domain.ClusterID, _ domain.ClusterCacheID) (<-chan domain.ClusterDataEventWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]domain.ClusterDataEvent(nil), f.s.dataEvents[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan domain.ClusterDataEventWatchFrame, len(snap))
	for _, e := range snap {
		ch <- domain.ClusterDataEventWatchFrame{Type: domain.FrameAdded, Event: &e}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeData) WatchObjects(ctx context.Context, clusterID domain.ClusterID, _ domain.ClusterCacheID, _, _ string) (<-chan domain.ClusterDataObjectWatchFrame, error) {
	f.s.mu.Lock()
	snap := append([]domain.ClusterDataObject(nil), f.s.dataObjects[clusterID]...)
	f.s.mu.Unlock()
	ch := make(chan domain.ClusterDataObjectWatchFrame, len(snap))
	for _, o := range snap {
		ch <- domain.ClusterDataObjectWatchFrame{Type: domain.FrameAdded, Object: &o}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) ListEvents(_ context.Context, id domain.ClusterID, _ *string, _ *int) ([]domain.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.events[id], nil
}

func (f fakeClusters) WatchEvents(ctx context.Context, _ domain.ClusterID, _ *string) (<-chan domain.Event, error) {
	ch := make(chan domain.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeCaches) Get(_ context.Context, id domain.ClusterCacheID) (*domain.ClusterCache, error) {
	for _, c := range f.s.cacheSnapshot() {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (f fakeCaches) List(_ context.Context, clusterID *domain.ClusterID) ([]*domain.ClusterCache, error) {
	var out []*domain.ClusterCache
	for _, c := range f.s.cacheSnapshot() {
		if clusterID == nil || c.ClusterID == *clusterID {
			out = append(out, &c)
		}
	}
	return out, nil
}

func (f fakeCaches) ListEvents(_ context.Context, id domain.ClusterCacheID, _ *string, _ *int) ([]domain.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.cacheEvents[id], nil
}

func (f fakeCaches) WatchEvents(ctx context.Context, _ domain.ClusterCacheID, _ *string) (<-chan domain.Event, error) {
	ch := make(chan domain.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeSyncs) Get(_ context.Context, id domain.ClusterCacheGVRSyncID) (*domain.ClusterCacheGVRSync, error) {
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
func (f fakeSyncs) List(_ context.Context, cacheID *domain.ClusterCacheID) ([]*domain.ClusterCacheGVRSync, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	var want domain.ClusterCacheGVRDiscoveryID
	for i := range f.s.discoveries {
		if cacheID != nil && f.s.discoveries[i].CacheID == *cacheID {
			want = f.s.discoveries[i].ID
		}
	}
	var out []*domain.ClusterCacheGVRSync
	for i := range f.s.gvrSyncs {
		if cacheID == nil || f.s.gvrSyncs[i].DiscoveryID == want {
			gs := f.s.gvrSyncs[i]
			out = append(out, &gs)
		}
	}
	return out, nil
}

func (f fakeSyncs) ListEvents(_ context.Context, id domain.ClusterCacheGVRSyncID, _ *string, _ *int) ([]domain.Event, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return f.s.syncEvents[id], nil
}

func (f fakeSyncs) WatchEvents(ctx context.Context, _ domain.ClusterCacheGVRSyncID, _ *string) (<-chan domain.Event, error) {
	ch := make(chan domain.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) WatchSchedule(ctx context.Context, _ domain.ClusterID) (<-chan domain.Schedule, error) {
	ch := make(chan domain.Schedule)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f fakeClusters) SetEnabled(_ context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	c.Spec.Enabled = enabled
	cp := *c
	return &cp, nil
}

func (f fakeClusters) SetSyncEnabled(_ context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	c.Spec.SyncEnabled = enabled
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) RetryConnection(_ context.Context, id domain.ClusterID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusters[id]; !ok {
		return domain.ErrNotFound
	}
	return nil
}

func (f fakeCaches) Clear(_ context.Context, id domain.ClusterID) (*domain.Cluster, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	c, ok := f.s.clusters[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f fakeClusters) Delete(_ context.Context, id domain.ClusterID) error {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if _, ok := f.s.clusters[id]; !ok {
		return domain.ErrNotFound
	}
	delete(f.s.clusters, id)
	return nil
}

func (f *fakeClusterService) GetConnection(domain.ClusterID) *rest.Config { return nil }

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
			spec: domain.ClusterSpec{
				Name:        &prodName,
				SyncEnabled: true,
				Enabled:     true,
				Source:      domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: "prod"}},
			},
			connStatus: domain.ClusterStatus{
				Source: domain.ClusterStatusSource{Kubeconfig: &domain.ClusterStatusSourceKubeconfig{
					Cluster: "prod-cluster", User: "prod-user",
					IsPresent: true, IsDefault: true,
				}},
				Server:    domain.ClusterServer{UID: &uid1, Version: &ver},
				Principal: domain.ClusterPrincipal{Username: &admin},
			},
			discStats: &domain.ClusterCacheGVRDiscoveryStats{LastDiscoveryAt: discoveredAt, ResourceCount: 42},
			discConds: []domain.Condition{
				{Type: "Discovered", Status: domain.ConditionTrue, Reason: "Discovered", Liveness: true},
			},
		},
		{
			id: 2,
			spec: domain.ClusterSpec{
				Source: domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: "staging"}},
			},
			connStatus: domain.ClusterStatus{
				Source: domain.ClusterStatusSource{Kubeconfig: &domain.ClusterStatusSourceKubeconfig{
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
