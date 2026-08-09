package graph_test

// Behavioral tests for the cluster resolvers, exercised over a real gqlgen HTTP
// server. The resolvers delegate to a cluster.ClusterService, so the tests
// wire a fakeClusterService built from fixtures — that keeps the focus on the
// GraphQL wire mapping (nil→null, conditions/cache shapes) and off beehive,
// which the service-level tests in internal/cluster already cover.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
)

// clusterFixture bundles all data for one test cluster record. id is the beehive
// ObjectID; on the wire it is its decimal string ("1", "2", …).
type clusterFixture struct {
	id         cluster.ClusterID
	spec       cluster.ClusterSpec
	connStatus cluster.ClusterStatus
	// syncStats/syncConds belong to the fixture's per-kind sync child. Like the discovery
	// gauges below, the stamps are NOT on the record — they are sampled from the
	// controller through GVRSyncStats — so the fake serves them from here.
	syncStats *cluster.ClusterCacheGVRSyncStats
	// discStats/discConds belong to the fixture's GVR-discovery child. The gauges are NOT
	// on the record — they are sampled from the controller through GVRDiscoveryStats — so
	// the fake serves them from here to exercise that resolver path.
	discStats *cluster.ClusterCacheGVRDiscoveryStats
	// Conditions are beehive object rows now, not part of either status block.
	connConds  []cluster.Condition
	cacheConds []cluster.Condition
	syncConds  []cluster.Condition
	discConds  []cluster.Condition
}

// fakeClusterService implements cluster.ClusterService over an in-memory map
// built from fixtures: it joins each fixture's connection + cache status into a
// domain Cluster (exactly as the real service's buildCluster does), so the
// resolver/wire assertions see the same shapes.
type fakeClusterService struct {
	mu          sync.Mutex
	order       []cluster.ClusterID
	clusters    map[cluster.ClusterID]*cluster.Cluster
	caches      []cluster.ClusterCache             // one active cache per fixture, streamed via WatchCaches
	discoveries []cluster.ClusterCacheGVRDiscovery // one GVR-discovery child per cache, streamed via WatchGVRDiscoveries
	gvrSyncs    []cluster.ClusterCacheGVRSync      // per-kind sync records, streamed cache-scoped via WatchGVRSyncs
	gvrStats    map[cluster.ClusterCacheGVRSyncID]*cluster.ClusterCacheGVRSyncStats
	cacheStats  map[cluster.ClusterCacheID]cluster.ClusterCacheStats
	discStats   map[cluster.ClusterCacheGVRDiscoveryID]*cluster.ClusterCacheGVRDiscoveryStats
	syncEvents  map[cluster.ClusterCacheGVRSyncID][]cluster.Event
	events      map[cluster.ClusterID][]cluster.Event             // connection-event history, keyed by ClusterID
	cacheEvents map[cluster.ClusterCacheID][]cluster.Event        // sync-event history, keyed by ClusterCacheID
	kinds       map[cluster.ClusterID][]cluster.ClusterDataKind   // discovered kind catalog, keyed by ClusterID
	dataEvents  map[cluster.ClusterID][]cluster.ClusterDataEvent  // cached Kubernetes Events, keyed by ClusterID
	dataObjects map[cluster.ClusterID][]cluster.ClusterDataObject // cached objects for one kind, keyed by ClusterID
}

var _ cluster.ClusterService = (*fakeClusterService)(nil)

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{
		clusters:   map[cluster.ClusterID]*cluster.Cluster{},
		events:     map[cluster.ClusterID][]cluster.Event{},
		discStats:  map[cluster.ClusterCacheGVRDiscoveryID]*cluster.ClusterCacheGVRDiscoveryStats{},
		gvrStats:   map[cluster.ClusterCacheGVRSyncID]*cluster.ClusterCacheGVRSyncStats{},
		cacheStats: map[cluster.ClusterCacheID]cluster.ClusterCacheStats{},
	}
	for _, fx := range fixtures {
		id := fx.id
		f.order = append(f.order, id)
		f.clusters[id] = &cluster.Cluster{
			ID:         id,
			Spec:       fx.spec,
			Status:     fx.connStatus,
			Conditions: fx.connConds,
		}
		// Caches stream standalone via WatchCaches and are joined client-side.
		// Give each fixture one cache whose ServerUID matches the cluster's
		// identity (the client's active-cache rule).
		f.caches = append(f.caches, cluster.ClusterCache{
			ID:         cluster.ClusterCacheID(id),
			ClusterID:  id,
			ServerUID:  "uid-" + strconv.FormatInt(int64(id), 10),
			Conditions: fx.cacheConds,
		})
		// Each cache gets one per-kind sync child, keyed off the same id — the fixture's
		// per-component sync state.
		f.gvrStats[cluster.ClusterCacheGVRSyncID(id)] = fx.syncStats
		// …and its GVR-discovery child, the anchor the per-kind syncs hang off.
		f.discoveries = append(f.discoveries, cluster.ClusterCacheGVRDiscovery{
			ID:         cluster.ClusterCacheGVRDiscoveryID(id),
			CacheID:    cluster.ClusterCacheID(id),
			Spec:       cluster.ClusterCacheGVRDiscoverySpec{Enabled: fx.spec.SyncEnabled},
			Conditions: fx.discConds,
		})
		f.discStats[cluster.ClusterCacheGVRDiscoveryID(id)] = fx.discStats
		// …and one per-kind sync record under that anchor, so the cache-scoped watch has
		// something to scope. Deliberately one per cache so a leak across caches is
		// visible as an extra frame.
		f.gvrSyncs = append(f.gvrSyncs, cluster.ClusterCacheGVRSync{
			ID:          cluster.ClusterCacheGVRSyncID(id),
			DiscoveryID: cluster.ClusterCacheGVRDiscoveryID(id),
			Spec: cluster.ClusterCacheGVRSyncSpec{
				Enabled: fx.spec.SyncEnabled, APIVersion: "apps/v1",
				Kind: "Deployment", Resource: "deployments", Namespaced: true,
			},
			Conditions: fx.syncConds,
		})
		f.cacheStats[cluster.ClusterCacheID(id)] = cluster.ClusterCacheStats{
			Exists: true, Bytes: 4096, ObjectCount: 1386, KindCount: 62,
		}
	}
	return f
}

func (f *fakeClusterService) snapshot() []*cluster.Cluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*cluster.Cluster, 0, len(f.order))
	for _, id := range f.order {
		if c, ok := f.clusters[id]; ok {
			cp := *c
			out = append(out, &cp)
		}
	}
	return out
}

func (f *fakeClusterService) cacheSnapshot() []cluster.ClusterCache {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cluster.ClusterCache(nil), f.caches...)
}

func (f *fakeClusterService) List(context.Context) ([]*cluster.Cluster, error) {
	return f.snapshot(), nil
}

func (f *fakeClusterService) Get(_ context.Context, id cluster.ClusterID) (*cluster.Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.clusters[id]
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

func (f *fakeClusterService) Watch(ctx context.Context) (<-chan cluster.ClusterChange, error) {
	return snapshotChan(ctx, f.snapshot(), func(c **cluster.Cluster) cluster.ClusterChange {
		return cluster.ClusterChange{Type: cluster.ChangeAdded, Cluster: *c}
	}), nil
}

func (f *fakeClusterService) WatchCaches(ctx context.Context) (<-chan cluster.ClusterCacheChange, error) {
	return snapshotChan(ctx, f.cacheSnapshot(), func(c *cluster.ClusterCache) cluster.ClusterCacheChange {
		return cluster.ClusterCacheChange{Type: cluster.ChangeAdded, Cache: c}
	}), nil
}

func (f *fakeClusterService) WatchGVRDiscoveries(ctx context.Context) (<-chan cluster.ClusterCacheGVRDiscoveryChange, error) {
	return snapshotChan(ctx, copySlice(f, &f.discoveries), func(d *cluster.ClusterCacheGVRDiscovery) cluster.ClusterCacheGVRDiscoveryChange {
		return cluster.ClusterCacheGVRDiscoveryChange{Type: cluster.ChangeAdded, Discovery: d}
	}), nil
}

// WatchCacheSyncHealth folds the fixture's per-kind records per cache, the same way the
// real service does — enough to prove the wire shape and the join key.
func (f *fakeClusterService) WatchCacheSyncHealth(ctx context.Context) (<-chan cluster.ClusterCacheSyncHealth, error) {
	f.mu.Lock()
	cacheOf := map[cluster.ClusterCacheGVRDiscoveryID]cluster.ClusterCacheID{}
	for i := range f.discoveries {
		cacheOf[f.discoveries[i].ID] = f.discoveries[i].CacheID
	}
	byCache := map[cluster.ClusterCacheID]*cluster.ClusterCacheSyncHealth{}
	for i := range f.gvrSyncs {
		cacheID := cacheOf[f.gvrSyncs[i].DiscoveryID]
		h := byCache[cacheID]
		if h == nil {
			h = &cluster.ClusterCacheSyncHealth{CacheID: cacheID, Status: cluster.ConditionTrue, Reason: "Watching"}
			byCache[cacheID] = h
		}
		h.TotalKinds++
		for _, c := range f.gvrSyncs[i].Conditions {
			if c.Type == string(cluster.ConditionSynced) && c.Reason != "Watching" {
				h.Status, h.Reason = c.Status, c.Reason
				h.UnhealthyKindRefs = append(h.UnhealthyKindRefs, cluster.SyncedKindRef{
					APIVersion: f.gvrSyncs[i].Spec.APIVersion,
					Resource:   f.gvrSyncs[i].Spec.Resource,
				})
				h.UnhealthyKinds++
			}
		}
	}
	f.mu.Unlock()
	out := make(chan cluster.ClusterCacheSyncHealth, len(byCache)+1)
	for _, h := range byCache {
		out <- *h
	}
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// WatchGVRSyncs serves only the records whose discovery anchor matches the requested
// cache's, standing in for the real service's owner-edge filter.
func (f *fakeClusterService) WatchGVRSyncs(ctx context.Context, cacheID cluster.ClusterCacheID) (<-chan cluster.ClusterCacheGVRSyncChange, error) {
	f.mu.Lock()
	var want cluster.ClusterCacheGVRDiscoveryID
	for i := range f.discoveries {
		if f.discoveries[i].CacheID == cacheID {
			want = f.discoveries[i].ID
		}
	}
	var scoped []cluster.ClusterCacheGVRSync
	for i := range f.gvrSyncs {
		if f.gvrSyncs[i].DiscoveryID == want {
			scoped = append(scoped, f.gvrSyncs[i])
		}
	}
	f.mu.Unlock()
	return snapshotChan(ctx, scoped, func(gs *cluster.ClusterCacheGVRSync) cluster.ClusterCacheGVRSyncChange {
		return cluster.ClusterCacheGVRSyncChange{Type: cluster.ChangeAdded, Sync: gs}
	}), nil
}

// ClusterCacheStatsWatch emits the fixture's single measurement and then holds the
// stream open, as a gauge with nothing new to report does.
func (f *fakeClusterService) ClusterCacheStatsWatch(ctx context.Context, _ cluster.ClusterID, cacheID cluster.ClusterCacheID) (<-chan cluster.ClusterCacheStats, error) {
	f.mu.Lock()
	st := f.cacheStats[cacheID]
	f.mu.Unlock()
	out := make(chan cluster.ClusterCacheStats, 1)
	out <- st
	go func() {
		<-ctx.Done()
		close(out)
	}()
	return out, nil
}

// GVRSyncStatsSnapshot is the whole-map read the rollup folds.
func (f *fakeClusterService) GVRSyncStatsSnapshot() map[cluster.ClusterCacheGVRSyncID]cluster.ClusterCacheGVRSyncStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[cluster.ClusterCacheGVRSyncID]cluster.ClusterCacheGVRSyncStats{}
	for id, st := range f.gvrStats {
		if st != nil {
			out[id] = *st
		}
	}
	return out
}

// GVRSyncStats stands in for the per-kind controller's in-memory stamps.
func (f *fakeClusterService) GVRSyncStats(_ context.Context, id cluster.ClusterCacheGVRSyncID) (*cluster.ClusterCacheGVRSyncStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gvrStats[id], nil
}

// GVRDiscoveryStats stands in for the controller's in-memory gauges: a record the
// fixture gave no stats to reads as nil, the "no pass in this process yet" case.
func (f *fakeClusterService) GVRDiscoveryStats(_ context.Context, id cluster.ClusterCacheGVRDiscoveryID) (*cluster.ClusterCacheGVRDiscoveryStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.discStats[id], nil
}

func (f *fakeClusterService) CacheStats(context.Context, cluster.ClusterID, cluster.ClusterCacheID) (*cluster.ClusterCacheStats, error) {
	return &cluster.ClusterCacheStats{}, nil
}

func (f *fakeClusterService) ClusterDataKinds(_ context.Context, clusterID cluster.ClusterID, _ cluster.ClusterCacheID) ([]cluster.ClusterDataKind, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kinds[clusterID], nil
}

func (f *fakeClusterService) ClusterDataKindsWatch(ctx context.Context, clusterID cluster.ClusterID, _ cluster.ClusterCacheID) (<-chan cluster.ClusterDataKindChange, error) {
	f.mu.Lock()
	snap := append([]cluster.ClusterDataKind(nil), f.kinds[clusterID]...)
	f.mu.Unlock()
	ch := make(chan cluster.ClusterDataKindChange, len(snap))
	for _, k := range snap {
		ch <- cluster.ClusterDataKindChange{Type: cluster.ChangeAdded, Kind: k}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterDataEventsWatch(ctx context.Context, clusterID cluster.ClusterID, _ cluster.ClusterCacheID) (<-chan cluster.ClusterDataEventChange, error) {
	f.mu.Lock()
	snap := append([]cluster.ClusterDataEvent(nil), f.dataEvents[clusterID]...)
	f.mu.Unlock()
	ch := make(chan cluster.ClusterDataEventChange, len(snap))
	for _, e := range snap {
		ch <- cluster.ClusterDataEventChange{Type: cluster.ChangeAdded, Event: e}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterDataObjectsWatch(ctx context.Context, clusterID cluster.ClusterID, _ cluster.ClusterCacheID, _, _ string) (<-chan cluster.ClusterDataObjectChange, error) {
	f.mu.Lock()
	snap := append([]cluster.ClusterDataObject(nil), f.dataObjects[clusterID]...)
	f.mu.Unlock()
	ch := make(chan cluster.ClusterDataObjectChange, len(snap))
	for _, o := range snap {
		ch <- cluster.ClusterDataObjectChange{Type: cluster.ChangeAdded, Object: o}
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterEvents(_ context.Context, id cluster.ClusterID, _ *string, _ *int) ([]cluster.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.events[id], nil
}

func (f *fakeClusterService) ClusterEventsWatch(ctx context.Context, _ cluster.ClusterID, _ *string) (<-chan cluster.Event, error) {
	ch := make(chan cluster.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterCacheEvents(_ context.Context, id cluster.ClusterCacheID, _ *string, _ *int) ([]cluster.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cacheEvents[id], nil
}

func (f *fakeClusterService) ClusterCacheEventsWatch(ctx context.Context, _ cluster.ClusterCacheID, _ *string) (<-chan cluster.Event, error) {
	ch := make(chan cluster.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterCacheGVRSyncEvents(_ context.Context, id cluster.ClusterCacheGVRSyncID, _ *string, _ *int) ([]cluster.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncEvents[id], nil
}

func (f *fakeClusterService) ClusterCacheGVRSyncEventsWatch(ctx context.Context, _ cluster.ClusterCacheGVRSyncID, _ *string) (<-chan cluster.Event, error) {
	ch := make(chan cluster.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) ClusterScheduleWatch(ctx context.Context, _ cluster.ClusterID) (<-chan cluster.Schedule, error) {
	ch := make(chan cluster.Schedule)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) SetEnabled(_ context.Context, id cluster.ClusterID, enabled bool) (*cluster.Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.clusters[id]
	if !ok {
		return nil, cluster.ErrNotFound
	}
	c.Spec.Enabled = enabled
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) SetSyncEnabled(_ context.Context, id cluster.ClusterID, enabled bool) (*cluster.Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.clusters[id]
	if !ok {
		return nil, cluster.ErrNotFound
	}
	c.Spec.SyncEnabled = enabled
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) RetryConnection(_ context.Context, id cluster.ClusterID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusters[id]; !ok {
		return cluster.ErrNotFound
	}
	return nil
}

func (f *fakeClusterService) ClearCache(_ context.Context, id cluster.ClusterID) (*cluster.Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.clusters[id]
	if !ok {
		return nil, cluster.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f *fakeClusterService) Delete(_ context.Context, id cluster.ClusterID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusters[id]; !ok {
		return cluster.ErrNotFound
	}
	delete(f.clusters, id)
	return nil
}

func (f *fakeClusterService) GetConnection(cluster.ClusterID) *rest.Config { return nil }

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
			spec: cluster.ClusterSpec{
				Name:        &prodName,
				SyncEnabled: true,
				Enabled:     true,
				Source:      cluster.ClusterSpecSource{Kubeconfig: &cluster.ClusterSpecSourceKubeconfig{Context: "prod"}},
			},
			connStatus: cluster.ClusterStatus{
				Source: cluster.ClusterStatusSource{Kubeconfig: &cluster.ClusterStatusSourceKubeconfig{
					Cluster: "prod-cluster", User: "prod-user",
					IsPresent: true, IsDefault: true,
				}},
				Server:    cluster.ClusterServer{UID: &uid1, Version: &ver},
				Principal: cluster.ClusterPrincipal{Username: &admin},
			},
			discStats: &cluster.ClusterCacheGVRDiscoveryStats{LastDiscoveryAt: discoveredAt, ResourceCount: 42},
			discConds: []cluster.Condition{
				{Type: "Discovered", Status: cluster.ConditionTrue, Reason: "Discovered", Liveness: true},
			},
		},
		{
			id: 2,
			spec: cluster.ClusterSpec{
				Source: cluster.ClusterSpecSource{Kubeconfig: &cluster.ClusterSpecSourceKubeconfig{Context: "staging"}},
			},
			connStatus: cluster.ClusterStatus{
				Source: cluster.ClusterStatusSource{Kubeconfig: &cluster.ClusterStatusSourceKubeconfig{
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

// The clusters query maps the beehive records including the nil → null
// convention for unset/never-probed pointer fields.
func TestClustersQuery(t *testing.T) {
	data := clustersQueryData(t, `{ clusters {
		id
		generation
		spec { name syncEnabled enabled source { kubeconfig { context } } }
		status {
			source { kubeconfig { cluster user isPresent isDefault } }
			server { uid version }
			principal { username }
			lastConnectedAt
		}
	} }`)

	clusters, ok := data["clusters"].([]any)
	if !ok || len(clusters) != 2 {
		t.Fatalf("want 2 clusters, got: %v", data["clusters"])
	}

	probed := clusters[0].(map[string]any)
	spec := probed["spec"].(map[string]any)
	if spec["name"] != "Production" || spec["syncEnabled"] != true || spec["enabled"] != true {
		t.Errorf("probed cluster spec: %v", spec)
	}
	if kcSrc := spec["source"].(map[string]any)["kubeconfig"].(map[string]any); kcSrc["context"] != "prod" {
		t.Errorf("probed cluster source: %v", kcSrc)
	}
	status := probed["status"].(map[string]any)
	if ci := status["server"].(map[string]any); ci["uid"] != "uid-1" || ci["version"] != "v1.29.3" {
		t.Errorf("probed server: %v", ci)
	}
	if p := status["principal"].(map[string]any); p["username"] != "system:admin" {
		t.Errorf("probed principal: %v", p)
	}
	if kc := status["source"].(map[string]any)["kubeconfig"].(map[string]any); kc["cluster"] != "prod-cluster" || kc["isPresent"] != true || kc["isDefault"] != true {
		t.Errorf("probed kubeconfig: %v", kc)
	}

	unprobed := clusters[1].(map[string]any)
	if name := unprobed["spec"].(map[string]any)["name"]; name != nil {
		t.Errorf("unset name should be null, got: %v", name)
	}
	unprobedStatus := unprobed["status"].(map[string]any)
	if ci := unprobedStatus["server"].(map[string]any); ci["uid"] != nil || ci["version"] != nil {
		t.Errorf("never-probed server should be null, got: %v", ci)
	}
	if p := unprobedStatus["principal"].(map[string]any); p["username"] != nil {
		t.Errorf("never-probed username should be null, got: %v", p)
	}
	if at := unprobedStatus["lastConnectedAt"]; at != nil {
		t.Errorf("never-connected lastConnectedAt should be null, got: %v", at)
	}
}

// The cluster query returns the record for a tracked id.
// clusterEvents maps the service's domain Events onto the wire: the run id rides
// the ObjectID scalar (decimal string), the type binds to the EventType enum
// (Normal/Warning), and the value slice is adapted to gqlgen's pointer slice.
func TestClusterEventsResolver(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	now := time.Now().UTC()
	svc.events[id] = []cluster.Event{{
		ID: id, Category: "connection", Type: beehive.EventWarning,
		Reason: "ProbeFailed", Message: "boom", Count: 3, FirstAt: now, LastAt: now,
	}}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	query := `{ clusterEvents(id: "` + strconv.FormatInt(int64(id), 10) + `", category: "connection") {
		id category type reason message count firstAt lastAt
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			ClusterEvents []map[string]any `json:"clusterEvents"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	if len(resp.Data.ClusterEvents) != 1 {
		t.Fatalf("want 1 event, got %d: %s", len(resp.Data.ClusterEvents), raw)
	}
	ev := resp.Data.ClusterEvents[0]
	if ev["id"] != strconv.FormatInt(int64(id), 10) {
		t.Errorf("id: want decimal-string %d, got %v", id, ev["id"])
	}
	if ev["type"] != "Warning" {
		t.Errorf("type: want Warning enum, got %v", ev["type"])
	}
	if ev["reason"] != "ProbeFailed" || ev["category"] != "connection" {
		t.Errorf("reason/category: %v", ev)
	}
	if ev["count"] != float64(3) {
		t.Errorf("count: want 3, got %v", ev["count"])
	}
}

// clusterDataKinds maps the service's domain ClusterDataKinds onto the wire 1:1 (bound
// via gqlgen.yml), so the resolver just adapts the value slice to a pointer slice.
func TestClusterDataKindsResolver(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.kinds = map[cluster.ClusterID][]cluster.ClusterDataKind{
		id: {
			{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: "Namespaced", IsCRD: false},
			{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: "Namespaced", IsCRD: true},
		},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	query := `{ clusterDataKinds(id: "` + strconv.FormatInt(int64(id), 10) + `", cacheID: "` + strconv.FormatInt(int64(id), 10) + `") {
		apiVersion kind resource scope isCRD
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			ClusterDataKinds []map[string]any `json:"clusterDataKinds"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	if len(resp.Data.ClusterDataKinds) != 2 {
		t.Fatalf("want 2 kinds, got %d: %s", len(resp.Data.ClusterDataKinds), raw)
	}
	k := resp.Data.ClusterDataKinds[0]
	if k["apiVersion"] != "apps/v1" || k["kind"] != "Deployment" || k["resource"] != "deployments" {
		t.Errorf("first kind: %v", k)
	}
	if k["scope"] != "Namespaced" || k["isCRD"] != false {
		t.Errorf("first kind scope/isCRD: %v", k)
	}
	if resp.Data.ClusterDataKinds[1]["isCRD"] != true {
		t.Errorf("second kind should be a CRD: %v", resp.Data.ClusterDataKinds[1])
	}

	// Unknown cluster → empty list, not an error.
	q2 := `{ clusterDataKinds(id: "99999", cacheID: "99999") { kind } }`
	b2, _ := json.Marshal(map[string]string{"query": q2})
	raw2 := postGQL(t, srv.URL, string(b2))
	var resp2 struct {
		Data struct {
			ClusterDataKinds []map[string]any `json:"clusterDataKinds"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw2, &resp2); err != nil {
		t.Fatalf("decode %s: %v", raw2, err)
	}
	if len(resp2.Errors) > 0 || len(resp2.Data.ClusterDataKinds) != 0 {
		t.Fatalf("unknown cluster should yield empty kinds, got %s", raw2)
	}
}

// clusterDataObjectsWatch wires the subscription resolver to the service: the fake with
// no seeded objects opens an empty-until-ctx stream, so the SSE dial succeeds and the
// stream stays open with no frames rather than erroring.
func TestClusterDataObjectsWatchOpensWithoutError(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	idStr := strconv.FormatInt(int64(id), 10)
	q := `subscription { clusterDataObjectsWatch(id: "` + idStr + `", cacheID: "` + idStr +
		`", apiVersion: "apps/v1", resource: "deployments") { type object { uid name } } }`
	resp := openSSESubscription(t, srv.URL, "", q)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	// The stub emits no frames; assert only that the dial produced no error frame within a
	// short window (a `next` carrying `errors`, or an SSE `error`/`complete` on open).
	select {
	case ev, ok := <-events:
		if ok && (ev.event == "error" || strings.Contains(ev.data, `"errors"`)) {
			t.Fatalf("objects watch should open cleanly, got %s: %s", ev.event, ev.data)
		}
	case <-time.After(200 * time.Millisecond):
		// No frame is the expected empty-until-ctx posture.
	}
}

// The resolver-gated `object` field carries the full native body as the JSON scalar,
// marshaled verbatim through gqlgen — a consumer selecting it gets the object JSON back
// as a nested value (not a string), with the identity fields alongside.
func TestClusterDataObjectsWatchServesNativeBody(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.dataObjects = map[cluster.ClusterID][]cluster.ClusterDataObject{
		id: {{
			UID: "d1", APIVersion: "apps/v1", Kind: "Deployment", Namespace: "default", Name: "web",
			RawJSON: cluster.RawJSON(`{"kind":"Deployment","spec":{"replicas":3}}`),
		}},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	idStr := strconv.FormatInt(int64(id), 10)
	q := `subscription { clusterDataObjectsWatch(id: "` + idStr + `", cacheID: "` + idStr +
		`", apiVersion: "apps/v1", resource: "deployments") { type object { uid name rawJSON } } }`
	resp := openSSESubscription(t, srv.URL, "", q)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before the snapshot frame")
			}
			if ev.event != "next" {
				continue
			}
			var frame struct {
				Data struct {
					ClusterDataObjectsWatch struct {
						Type   string `json:"type"`
						Object struct {
							UID     string         `json:"uid"`
							Name    string         `json:"name"`
							RawJSON map[string]any `json:"rawJSON"`
						} `json:"object"`
					} `json:"clusterDataObjectsWatch"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode frame %s: %v", ev.data, err)
			}
			got := frame.Data.ClusterDataObjectsWatch
			if got.Object.UID != "d1" || got.Object.Name != "web" {
				t.Fatalf("identity fields: got %+v", got.Object)
			}
			// The body decoded as a nested JSON object, not a string.
			if got.Object.RawJSON["kind"] != "Deployment" {
				t.Fatalf("native body not served as JSON: %s", ev.data)
			}
			if spec, _ := got.Object.RawJSON["spec"].(map[string]any); spec == nil || spec["replicas"] != float64(3) {
				t.Fatalf("nested body fields missing: %s", ev.data)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for the object snapshot frame")
		}
	}
}

// clusterDataKindsWatch streams the kind catalog as a delta watch: the resolver
// adapts the service's ClusterDataKindChange stream to the wire 1:1, so the snapshot
// arrives as Added changes carrying the kind's fields (incl. the live count) and the
// stream stays open for live updates.
func TestClusterDataKindsWatchEmitsSnapshotAndStaysOpen(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.kinds = map[cluster.ClusterID][]cluster.ClusterDataKind{
		id: {
			{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: "Namespaced", IsCRD: false, Count: 3},
			{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: "Namespaced", IsCRD: true, Count: 0},
		},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	q := `subscription { clusterDataKindsWatch(id: "` + strconv.FormatInt(int64(id), 10) +
		`", cacheID: "` + strconv.FormatInt(int64(id), 10) + `") { type kind { apiVersion kind resource count } } }`
	resp := openSSESubscription(t, srv.URL, "", q)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	seen := map[string]int{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before snapshot completed")
			}
			if ev.event != "next" {
				continue
			}
			if !strings.Contains(ev.data, `"type":"Added"`) {
				t.Fatalf("snapshot change should be Added, got: %s", ev.data)
			}
			var frame struct {
				Data struct {
					ClusterDataKindsWatch struct {
						Type string `json:"type"`
						Kind struct {
							Kind  string `json:"kind"`
							Count int    `json:"count"`
						} `json:"kind"`
					} `json:"clusterDataKindsWatch"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode frame %s: %v", ev.data, err)
			}
			seen[frame.Data.ClusterDataKindsWatch.Kind.Kind] = frame.Data.ClusterDataKindsWatch.Kind.Count
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot; saw %v", seen)
		}
	}
	if seen["Deployment"] != 3 {
		t.Errorf("Deployment count: want 3, got %d", seen["Deployment"])
	}
	if _, ok := seen["Widget"]; !ok {
		t.Errorf("Widget kind missing from snapshot: %v", seen)
	}

	// The stream stays open after the snapshot (no completion).
	select {
	case _, ok := <-events:
		if !ok {
			t.Fatal("stream closed; want it held open")
		}
	case <-time.After(250 * time.Millisecond):
		// stayed open ✓
	}
}

// The two cache-side event timelines are the same generic reader keyed by a different
// object: `clusterCacheEvents` reads the ClusterCache's own timeline (what the cache
// layer records, e.g. SyncStopped), `clusterCacheGVRSyncEvents` one synced kind's
// record's (where each worker report lands). One table because the wire mapping under
// test — domain Event → the generic Event shape, enum included — is identical; only the
// entrypoint and the object it keys on differ.
func TestCacheEventTimelineResolvers(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name     string
		field    string
		seed     func(*fakeClusterService, cluster.ObjectID, cluster.Event)
		event    cluster.Event
		wantEnum string
	}{{
		name:  "cache timeline",
		field: "clusterCacheEvents",
		seed: func(f *fakeClusterService, id cluster.ObjectID, ev cluster.Event) {
			f.cacheEvents = map[cluster.ClusterCacheID][]cluster.Event{id: {ev}}
		},
		event: cluster.Event{
			Category: "sync", Type: beehive.EventWarning, Reason: "SyncFailed",
			Message: "boom", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Warning",
	}, {
		name:  "per-kind sync timeline",
		field: "clusterCacheGVRSyncEvents",
		seed: func(f *fakeClusterService, id cluster.ObjectID, ev cluster.Event) {
			f.syncEvents = map[cluster.ClusterCacheGVRSyncID][]cluster.Event{id: {ev}}
		},
		event: cluster.Event{
			Category: "sync", Type: beehive.EventNormal, Reason: "SyncComplete",
			Message: "cached 12 events", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Normal",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fix := clusterFixtures()
			svc := newFakeClusterService(fix)
			id := cluster.ObjectID(fix[0].id)
			ev := tt.event
			ev.ID = id
			tt.seed(svc, id, ev)
			srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
				ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
			}))
			t.Cleanup(srv.Close)

			query := `{ ` + tt.field + `(id: "` + strconv.FormatInt(int64(id), 10) + `", category: "sync") {
				id category type reason message count firstAt lastAt
			} }`
			body, _ := json.Marshal(map[string]string{"query": query})
			raw := postGQL(t, srv.URL, string(body))

			var resp struct {
				Data   map[string][]map[string]any
				Errors []struct{ Message string }
			}
			if err := json.Unmarshal(raw, &resp); err != nil {
				t.Fatalf("decode %s: %v", raw, err)
			}
			if len(resp.Errors) > 0 {
				t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
			}
			got := resp.Data[tt.field]
			if len(got) != 1 {
				t.Fatalf("want 1 event, got %d: %s", len(got), raw)
			}
			if got[0]["type"] != tt.wantEnum {
				t.Errorf("type: want %s enum, got %v", tt.wantEnum, got[0]["type"])
			}
			if got[0]["reason"] != tt.event.Reason || got[0]["category"] != "sync" {
				t.Errorf("reason/category: %v", got[0])
			}
			if got[0]["count"] != float64(2) {
				t.Errorf("count: want 2, got %v", got[0]["count"])
			}
		})
	}
}

func TestClusterQueryByID(t *testing.T) {
	data := clustersQueryData(t, `{ cluster(id: 2) {
		id
		spec { source { kubeconfig { context } } }
		status { source { kubeconfig { isPresent } } }
	} }`)

	cl, ok := data["cluster"].(map[string]any)
	if !ok || cl["id"] != "2" {
		t.Fatalf("want cluster 2, got: %v", data["cluster"])
	}
	spec := cl["spec"].(map[string]any)
	if kcSrc := spec["source"].(map[string]any)["kubeconfig"].(map[string]any); kcSrc["context"] != "staging" {
		t.Errorf("spec source: %v", spec)
	}
	if kc := cl["status"].(map[string]any)["source"].(map[string]any)["kubeconfig"].(map[string]any); kc["isPresent"] != false {
		t.Errorf("kubeconfig: %v", kc)
	}
}

// An untracked id resolves to null, not a GraphQL error.
func TestClusterQueryNotFound(t *testing.T) {
	data := clustersQueryData(t, `{ cluster(id: "999") { id } }`)
	if data["cluster"] != nil {
		t.Fatalf("want null cluster, got: %v", data["cluster"])
	}
}

// clustersWatch is a delta watch: the snapshot arrives as one Added change per
// cluster (not a single list frame), then the stream holds open (no completion)
// until the subscriber goes away.
func TestClustersWatchEmitsSnapshotAndStaysOpen(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		"subscription { clustersWatch { type cluster { id spec { name } } } }")
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	// Collect frames until both fixtures have arrived; each is an Added change.
	seen := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before snapshot completed")
			}
			if ev.event != "next" {
				continue
			}
			if !strings.Contains(ev.data, `"type":"Added"`) {
				t.Fatalf("snapshot change should be Added, got: %s", ev.data)
			}
			if strings.Contains(ev.data, `"id":"1"`) {
				seen["1"] = true
			}
			if strings.Contains(ev.data, `"id":"2"`) {
				seen["2"] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for snapshot; saw %v", seen)
		}
	}

	// The stream stays open after the snapshot (no completion).
	select {
	case _, ok := <-events:
		if !ok {
			t.Fatal("stream closed; want it held open")
		}
	case <-time.After(250 * time.Millisecond):
		// stayed open ✓
	}
}

// The GVR-discovery stream serves the record's identity + Discovered condition, keyed to
// its cache by cacheID — the join the client makes — plus `stats`, which is resolved on
// read from the controller rather than carried on the record. Asserted on the wire
// because a resolver that isn't wired returns null rather than failing.
func TestClusterCacheGVRDiscoveriesWatchServesRecord(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCacheGVRDiscoveriesWatch { type discovery { id cacheID `+
			`stats { lastDiscoveryAt resourceCount } conditions { type status reason } } } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	deadline := time.After(2 * time.Second)
	for {
		var frame struct {
			Data struct {
				Watch struct {
					Type      string         `json:"type"`
					Discovery map[string]any `json:"discovery"`
				} `json:"clusterCacheGVRDiscoveriesWatch"`
			} `json:"data"`
		}
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before a frame arrived")
			}
			if ev.event != "next" {
				continue
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode discovery frame %s: %v", ev.data, err)
			}
			d := frame.Data.Watch.Discovery
			if d["cacheID"] != "1" {
				continue // fixture 2's record; it carries no discovery status
			}
			if frame.Data.Watch.Type != "Added" {
				t.Fatalf("snapshot change should be Added, got %q", frame.Data.Watch.Type)
			}
			stats, _ := d["stats"].(map[string]any)
			if got := stats["resourceCount"]; got != float64(42) {
				t.Errorf("resourceCount = %v, want 42", got)
			}
			if got := stats["lastDiscoveryAt"]; got != "2026-02-03T04:05:06Z" {
				t.Errorf("lastDiscoveryAt = %v", got)
			}
			conds, _ := d["conditions"].([]any)
			if len(conds) != 1 {
				t.Fatalf("conditions = %v, want the Discovered row", conds)
			}
			cond, _ := conds[0].(map[string]any)
			if cond["type"] != "Discovered" || cond["reason"] != "Discovered" || cond["status"] != "True" {
				t.Errorf("condition = %v, want a True/Discovered row", cond)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for cluster 1's discovery frame")
		}
	}
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

// clusterEnabledSet writes through and returns the updated record; the change
// is visible in subsequent reads.
func TestClusterEnabledSetMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	raw := string(postGQL(t, srv.URL,
		`{"query":"mutation { clusterEnabledSet(id: \"1\", enabled: false) { id spec { enabled } } }"}`))
	if !strings.Contains(raw, `"enabled":false`) || strings.Contains(raw, `"errors"`) {
		t.Fatalf("mutation result: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"{ cluster(id: \"1\") { spec { enabled } } }"}`))
	if !strings.Contains(raw, `"enabled":false`) {
		t.Fatalf("change not visible to reads: %s", raw)
	}
}

// clusterSyncEnabledSet writes through the beehive store and returns the
// updated record; the change is visible in subsequent reads.
func TestClusterSyncEnabledSetMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	raw := string(postGQL(t, srv.URL,
		`{"query":"mutation { clusterSyncEnabledSet(id: \"1\", syncEnabled: false) { id spec { syncEnabled } } }"}`))
	if !strings.Contains(raw, `"syncEnabled":false`) || strings.Contains(raw, `"errors"`) {
		t.Fatalf("mutation result: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"{ cluster(id: \"1\") { spec { syncEnabled } } }"}`))
	if !strings.Contains(raw, `"syncEnabled":false`) {
		t.Fatalf("change not visible to reads: %s", raw)
	}
}

// clusterDelete marks the cluster for deletion; the record is no longer
// visible via the cluster query. An unknown id is a GraphQL error.
func TestClusterDeleteMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { clusterDelete(id: \"2\") }"}`))
	if !strings.Contains(raw, `"clusterDelete":true`) {
		t.Fatalf("delete result: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"{ cluster(id: \"2\") { id } }"}`))
	if !strings.Contains(raw, `"cluster":null`) {
		t.Fatalf("deleted cluster still readable: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"mutation { clusterDelete(id: \"999\") }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want error for unknown id, got: %s", raw)
	}
}

// The status condition lists and the cache object resolve without panicking
// or erroring on bare fixtures: the cluster carries no conditions (empty arrays
// on the wire, never null) and the cache — streamed via clusterCachesWatch —
// has no on-disk files (exists=false, bytes=0, objectCount=0, kindCount=0).
func TestClusterEphemeralFields(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	// The cluster's own conditions are an empty list (never null).
	body, _ := json.Marshal(map[string]string{"query": `{ cluster(id: 1) { conditions { type status reason } } }`})
	raw := postGQL(t, srv.URL, string(body))
	var resp struct {
		Data struct {
			Cluster struct {
				Conditions []any `json:"conditions"`
			} `json:"cluster"`
		} `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	if resp.Data.Cluster.Conditions == nil || len(resp.Data.Cluster.Conditions) != 0 {
		t.Errorf("conditions should be an empty list, got: %v", resp.Data.Cluster.Conditions)
	}

	// The cache resolves its conditions + on-disk stats on a bare fixture. It has no status
	// block: the kind measures nothing itself.
	cache := firstCacheFrame(t, srv.URL)
	if cache["serverUid"] != "uid-1" || cache["clusterID"] != "1" {
		t.Errorf("cache identity: %v", cache)
	}
	if conds, ok := cache["conditions"].([]any); !ok || len(conds) != 0 {
		t.Errorf("sync conditions should be an empty list, got: %v", cache["conditions"])
	}
	stats := cache["stats"].(map[string]any)
	if stats["exists"] != false || stats["bytes"] != float64(0) {
		t.Errorf("cache stats placeholder: %v", stats)
	}
	if stats["objectCount"] != float64(0) || stats["kindCount"] != float64(0) {
		t.Errorf("never-cached counts should be 0, got: %v", stats)
	}
}

// Live conditions (cluster + cache) reach the wire with the correct GraphQL shapes —
// type/status/reason/message/liveness/timestamps. Conditions sit beside status, not
// inside it, since beehive stores them as their own object rows.
func TestConditionsAndSyncStatusOnWire(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	fixtures := clusterFixtures()
	fixtures[0].connConds = []cluster.Condition{{
		Type: string(cluster.ConditionConnected), Status: cluster.ConditionFalse,
		Reason: "ProbeFailed", Message: "connection refused",
		Liveness: true, TransitionedAt: at, UpdatedAt: at,
	}}
	fixtures[0].cacheConds = []cluster.Condition{{
		Type: string(cluster.ConditionSynced), Status: cluster.ConditionTrue,
		Reason: "Watching", Liveness: true, TransitionedAt: at, UpdatedAt: at,
	}}

	srv := newTestServer(t, fixtures)

	// The cluster's own conditions ride the cluster query.
	body, _ := json.Marshal(map[string]string{"query": `{ cluster(id: 1) {
		conditions { type status reason message liveness transitionedAt updatedAt }
	} }`})
	raw := postGQL(t, srv.URL, string(body))

	type wireCondition struct {
		Type           string  `json:"type"`
		Status         string  `json:"status"`
		Reason         string  `json:"reason"`
		Message        string  `json:"message"`
		Liveness       bool    `json:"liveness"`
		TransitionedAt *string `json:"transitionedAt"`
		UpdatedAt      *string `json:"updatedAt"`
	}
	var resp struct {
		Data struct {
			Cluster struct {
				Conditions []wireCondition `json:"conditions"`
			} `json:"cluster"`
		} `json:"data"`
		Errors []struct{ Message string } `json:"errors"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode response %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}

	conds := resp.Data.Cluster.Conditions
	if len(conds) != 1 {
		t.Fatalf("conditions: %+v", conds)
	}
	if conds[0].Type != "Connected" || conds[0].Status != "False" ||
		conds[0].Reason != "ProbeFailed" || conds[0].Message != "connection refused" ||
		!conds[0].Liveness || conds[0].TransitionedAt == nil || conds[0].UpdatedAt == nil {
		t.Errorf("Connected condition on the wire: %+v", conds[0])
	}

	// The cache's coarse Synced condition + stats ride clusterCachesWatch. Freshness does
	// not: the sync children report it, out of band from the object graph.
	cache := firstCacheFrame(t, srv.URL)
	syncConds, _ := cache["conditions"].([]any)
	if len(syncConds) != 1 {
		t.Fatalf("Synced condition on the wire: %+v", cache["conditions"])
	}
	c0 := syncConds[0].(map[string]any)
	if c0["type"] != "Synced" || c0["status"] != "True" || c0["reason"] != "Watching" {
		t.Errorf("Synced condition on the wire: %+v", c0)
	}

	// Cache stats with no on-disk files: exists=false.
	if stats := cache["stats"].(map[string]any); stats["exists"] != false {
		t.Errorf("cache without files should report exists=false")
	}
}

// clusterCacheClear deletes the on-disk cache and returns the (still-tracked)
// record; an unknown id surfaces the not-found error.
func TestClusterCacheClearMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	body, _ := json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: 1) { id } }`})
	raw := postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), `"id":"1"`) {
		t.Errorf("expected the cleared cluster back, got %s", raw)
	}

	body, _ = json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: "999") { id } }`})
	raw = postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), "errors") {
		t.Errorf("expected a GraphQL error for an unknown id, got %s", raw)
	}
}

// TestClusterCacheGVRSyncsWatchIsCacheScoped pins the scoping on the wire: the stream is
// opened for one cache and must carry only that cache's kinds. The fixture gives each
// cache one record, so a leak shows up as a second frame.
func TestClusterCacheGVRSyncsWatchIsCacheScoped(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCacheGVRSyncsWatch(cacheID: "1") { type sync { id discoveryID `+
			`spec { enabled apiVersion kind resource namespaced } conditions { type status reason } } } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	seen := 0
	deadline := time.After(time.Second)
	for {
		var frame struct {
			Data struct {
				Watch struct {
					Type string         `json:"type"`
					Sync map[string]any `json:"sync"`
				} `json:"clusterCacheGVRSyncsWatch"`
			} `json:"data"`
		}
		select {
		case ev, ok := <-events:
			if !ok {
				if seen == 0 {
					t.Fatal("stream closed before a frame arrived")
				}
				return
			}
			if ev.event != "next" {
				continue
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode gvr sync frame %s: %v", ev.data, err)
			}
			seen++
			sync := frame.Data.Watch.Sync
			if sync["discoveryID"] != "1" {
				t.Fatalf("another cache's record leaked into the stream: %v", sync)
			}
			spec, _ := sync["spec"].(map[string]any)
			if spec["resource"] != "deployments" {
				t.Errorf("resource = %v, want deployments", spec["resource"])
			}
		case <-deadline:
			if seen != 1 {
				t.Fatalf("expected exactly this cache's one record, saw %d", seen)
			}
			return
		}
	}
}

// TestClusterCacheStatsWatchServesGauge pins that the cache summary is reachable as a
// stream. It used to be a field on ClusterCache, which froze once that record settled.
func TestClusterCacheStatsWatchServesGauge(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCacheStatsWatch(id: "1", cacheID: "1") { exists bytes objectCount kindCount } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	deadline := time.After(2 * time.Second)
	for {
		var frame struct {
			Data struct {
				Stats map[string]any `json:"clusterCacheStatsWatch"`
			} `json:"data"`
		}
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before a frame arrived")
			}
			if ev.event != "next" {
				continue
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode stats frame %s: %v", ev.data, err)
			}
			if got := frame.Data.Stats["objectCount"]; got != float64(1386) {
				t.Errorf("objectCount = %v, want 1386", got)
			}
			if got := frame.Data.Stats["kindCount"]; got != float64(62) {
				t.Errorf("kindCount = %v, want 62", got)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for a stats frame")
		}
	}
}
