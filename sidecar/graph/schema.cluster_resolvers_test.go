package graph_test

// Behavioral tests for the cluster resolvers, exercised over a real gqlgen HTTP
// server. The resolvers now delegate to a cluster.ClusterService, so the tests
// wire a fakeClusterService built from fixtures — that keeps the focus on the
// GraphQL wire mapping (nil→null, conditions/cache shapes) and off beehive,
// which the service-level tests in internal/cluster already cover.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
)

// clusterFixture bundles all data for one test cluster record.
type clusterFixture struct {
	id          string
	spec        cluster.ClusterSpec
	connStatus  cluster.ClusterConnectionStatus
	cacheStatus cluster.ClusterCacheStatus
}

// fakeClusterService implements cluster.ClusterService over an in-memory map
// built from fixtures: it joins each fixture's connection + cache status into a
// domain Cluster (exactly as the real service's buildCluster does), so the
// resolver/wire assertions see the same shapes.
type fakeClusterService struct {
	mu       sync.Mutex
	order    []cluster.ClusterID
	clusters map[cluster.ClusterID]*cluster.Cluster
}

var _ cluster.ClusterService = (*fakeClusterService)(nil)

func newFakeClusterService(fixtures []clusterFixture) *fakeClusterService {
	f := &fakeClusterService{clusters: map[cluster.ClusterID]*cluster.Cluster{}}
	for _, fx := range fixtures {
		id := cluster.ClusterID(fx.id)
		f.order = append(f.order, id)
		f.clusters[id] = &cluster.Cluster{
			ID:   id,
			Spec: fx.spec,
			Status: cluster.ClusterStatus{
				Source:          fx.connStatus.Source,
				Server:          fx.connStatus.Server,
				Principal:       fx.connStatus.Principal,
				LastConnectedAt: fx.connStatus.LastConnectedAt,
				Conditions:      fx.connStatus.Conditions,
				SyncStatus:      fx.cacheStatus,
			},
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

func (f *fakeClusterService) Watch(ctx context.Context) (<-chan []*cluster.Cluster, error) {
	ch := make(chan []*cluster.Cluster, 1)
	ch <- f.snapshot()
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClusterService) CacheStats(context.Context, cluster.ClusterID) (*cluster.CacheStats, error) {
	return &cluster.CacheStats{}, nil
}

func (f *fakeClusterService) SetSyncEnabled(_ context.Context, id cluster.ClusterID, enabled bool) (*cluster.Cluster, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.clusters[id]
	if !ok {
		return nil, cluster.ErrNotFound
	}
	c.Spec.IsSyncEnabled = enabled
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

// clusterFixtures returns two records: one fully-probed/present (cl-1) and
// one never-probed/orphaned (cl-2), so nullable fields exercise both arms.
func clusterFixtures() []clusterFixture {
	prodName := "Production"
	uid1 := "uid-1"
	ver := "v1.29.3"
	admin := "system:admin"
	return []clusterFixture{
		{
			id: "cl-1",
			spec: cluster.ClusterSpec{
				Name:          &prodName,
				IsSyncEnabled: true,
				IsActive:      true,
				Source:        cluster.ClusterSource{Kubeconfig: &cluster.ClusterSourceKubeconfig{Context: "prod"}},
			},
			connStatus: cluster.ClusterConnectionStatus{
				Source: cluster.ClusterSourceStatus{Kubeconfig: &cluster.KubeconfigStatus{
					Cluster: "prod-cluster", User: "prod-user",
					IsPresent: true, IsDefault: true,
				}},
				Server:    cluster.ClusterServer{UID: &uid1, Version: &ver},
				Principal: cluster.ClusterPrincipal{Username: &admin},
			},
		},
		{
			id: "cl-2",
			spec: cluster.ClusterSpec{
				Source: cluster.ClusterSource{Kubeconfig: &cluster.ClusterSourceKubeconfig{Context: "staging"}},
			},
			connStatus: cluster.ClusterConnectionStatus{
				Source: cluster.ClusterSourceStatus{Kubeconfig: &cluster.KubeconfigStatus{
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
		spec { name isSyncEnabled isActive source { kubeconfig { context } } }
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
	if spec["name"] != "Production" || spec["isSyncEnabled"] != true {
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
func TestClusterQueryByID(t *testing.T) {
	data := clustersQueryData(t, `{ cluster(id: "cl-2") {
		id
		spec { source { kubeconfig { context } } }
		status { source { kubeconfig { isPresent } } }
	} }`)

	cl, ok := data["cluster"].(map[string]any)
	if !ok || cl["id"] != "cl-2" {
		t.Fatalf("want cluster cl-2, got: %v", data["cluster"])
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
	data := clustersQueryData(t, `{ cluster(id: "nope") { id } }`)
	if data["cluster"] != nil {
		t.Fatalf("want null cluster, got: %v", data["cluster"])
	}
}

// clustersWatch emits the current cluster list once, then holds the stream
// open (no completion) until the subscriber goes away. Extra re-emits from
// the beehive WatchList snapshot are expected and consumed here.
func TestClustersWatchEmitsSnapshotAndStaysOpen(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "", "subscription { clustersWatch { id spec { name } } }")
	defer resp.Body.Close()
	events := sseEvents(resp)

	ev := nextSSE(t, events)
	if ev.event != "next" {
		t.Fatalf("want first event=next, got %q", ev.event)
	}
	if !strings.Contains(ev.data, `"id":"cl-1"`) || !strings.Contains(ev.data, `"id":"cl-2"`) {
		t.Fatalf("snapshot frame should carry both clusters, got: %s", ev.data)
	}

	// Drain any extra events from the WatchList snapshot (beehive sends Added
	// events for existing objects when a watch is opened). The stream must not
	// close during this window.
	timeout := time.After(250 * time.Millisecond)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				t.Fatal("stream closed; want it held open")
			}
			// extra emit from WatchList snapshot — acceptable, keep draining
		case <-timeout:
			return // stream stayed open ✓
		}
	}
}

// clusterSyncEnabledSet writes through the beehive store and returns the
// updated record; the change is visible in subsequent reads.
func TestClusterSyncEnabledSetMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	raw := string(postGQL(t, srv.URL,
		`{"query":"mutation { clusterSyncEnabledSet(id: \"cl-1\", syncEnabled: false) { id spec { isSyncEnabled } } }"}`))
	if !strings.Contains(raw, `"isSyncEnabled":false`) || strings.Contains(raw, `"errors"`) {
		t.Fatalf("mutation result: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"{ cluster(id: \"cl-1\") { spec { isSyncEnabled } } }"}`))
	if !strings.Contains(raw, `"isSyncEnabled":false`) {
		t.Fatalf("change not visible to reads: %s", raw)
	}
}

// clusterDelete marks the cluster for deletion; the record is no longer
// visible via the cluster query. An unknown id is a GraphQL error.
func TestClusterDeleteMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { clusterDelete(id: \"cl-2\") }"}`))
	if !strings.Contains(raw, `"clusterDelete":true`) {
		t.Fatalf("delete result: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"{ cluster(id: \"cl-2\") { id } }"}`))
	if !strings.Contains(raw, `"cluster":null`) {
		t.Fatalf("deleted cluster still readable: %s", raw)
	}

	raw = string(postGQL(t, srv.URL, `{"query":"mutation { clusterDelete(id: \"nope\") }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want error for unknown id, got: %s", raw)
	}
}

// The status condition lists and the cache object resolve without panicking
// or erroring on bare fixtures: the fixtures carry no conditions (empty
// arrays on the wire, never null) and the cache manager has no on-disk files
// (exists=false, bytes=0, resources=[]).
func TestClusterEphemeralFields(t *testing.T) {
	data := clustersQueryData(t, `{ cluster(id: "cl-1") { status {
		conditions { type status reason }
		syncStatus { conditions { type } lastSyncedAt }
		cache { exists bytes resources { resource } }
	} } }`)

	cl := data["cluster"].(map[string]any)["status"].(map[string]any)
	if conds, ok := cl["conditions"].([]any); !ok || len(conds) != 0 {
		t.Errorf("conditions should be an empty list, got: %v", cl["conditions"])
	}
	syncStatus := cl["syncStatus"].(map[string]any)
	if conds, ok := syncStatus["conditions"].([]any); !ok || len(conds) != 0 {
		t.Errorf("sync conditions should be an empty list, got: %v", syncStatus["conditions"])
	}
	if at := syncStatus["lastSyncedAt"]; at != nil {
		t.Errorf("never-synced lastSyncedAt should be null, got: %v", at)
	}
	cache := cl["cache"].(map[string]any)
	if cache["exists"] != false || cache["bytes"] != float64(0) {
		t.Errorf("cache placeholder: %v", cache)
	}
	if res, ok := cache["resources"].([]any); !ok || len(res) != 0 {
		t.Errorf("cache resources should be empty list, got: %v", cache["resources"])
	}
}

// Live conditions (cluster + sync blocks) reach the wire with the correct
// GraphQL shapes — type/status/reason/message/observedGeneration/timestamps.
func TestClusterConditionsAndSyncStatusOnWire(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	fixtures := clusterFixtures()
	fixtures[0].connStatus.Conditions = []cluster.ClusterCondition{{
		Type: cluster.ClusterConditionConnected, Status: cluster.ConditionFalse,
		Reason: "ProbeFailed", Message: "connection refused",
		ObservedGeneration: 2, LastTransitionTime: at,
	}}
	fixtures[0].cacheStatus = cluster.ClusterCacheStatus{
		Conditions: []cluster.ClusterCondition{{
			Type: cluster.ClusterConditionSynced, Status: cluster.ConditionTrue,
			Reason: "Watching", LastTransitionTime: at,
		}},
		LastSyncedAt: &at,
	}

	srv := newTestServer(t, fixtures)

	body, _ := json.Marshal(map[string]string{"query": `{ cluster(id: "cl-1") { status {
		conditions { type status reason message observedGeneration lastTransitionTime }
		syncStatus { conditions { type status reason } lastSyncedAt }
		cache { exists bytes }
	} } }`})
	raw := postGQL(t, srv.URL, string(body))

	type wireCondition struct {
		Type               string  `json:"type"`
		Status             string  `json:"status"`
		Reason             string  `json:"reason"`
		Message            string  `json:"message"`
		ObservedGeneration int     `json:"observedGeneration"`
		LastTransitionTime *string `json:"lastTransitionTime"`
	}
	var resp struct {
		Data struct {
			Cluster struct {
				Status struct {
					Conditions []wireCondition `json:"conditions"`
					SyncStatus struct {
						Conditions   []wireCondition `json:"conditions"`
						LastSyncedAt *string         `json:"lastSyncedAt"`
					} `json:"syncStatus"`
					Cache struct {
						Exists bool    `json:"exists"`
						Bytes  float64 `json:"bytes"`
					} `json:"cache"`
				} `json:"status"`
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

	conds := resp.Data.Cluster.Status.Conditions
	if len(conds) != 1 {
		t.Fatalf("conditions: %+v", conds)
	}
	if conds[0].Type != "Connected" || conds[0].Status != "False" ||
		conds[0].Reason != "ProbeFailed" || conds[0].Message != "connection refused" ||
		conds[0].ObservedGeneration != 2 || conds[0].LastTransitionTime == nil {
		t.Errorf("Connected condition on the wire: %+v", conds[0])
	}

	syncStatus := resp.Data.Cluster.Status.SyncStatus
	if len(syncStatus.Conditions) != 1 || syncStatus.Conditions[0].Type != "Synced" ||
		syncStatus.Conditions[0].Status != "True" || syncStatus.Conditions[0].Reason != "Watching" {
		t.Errorf("Synced condition on the wire: %+v", syncStatus.Conditions)
	}
	if syncStatus.LastSyncedAt == nil {
		t.Error("lastSyncedAt should be set")
	}

	// Cache stats with no on-disk files: exists=false.
	if resp.Data.Cluster.Status.Cache.Exists {
		t.Errorf("cache without files should report exists=false")
	}
}

// clusterCacheClear deletes the on-disk cache and returns the (still-tracked)
// record; an unknown id surfaces the not-found error.
func TestClusterCacheClearMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	body, _ := json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: "cl-1") { id } }`})
	raw := postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), `"id":"cl-1"`) {
		t.Errorf("expected the cleared cluster back, got %s", raw)
	}

	body, _ = json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: "nope") { id } }`})
	raw = postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), "errors") {
		t.Errorf("expected a GraphQL error for an unknown id, got %s", raw)
	}
}
