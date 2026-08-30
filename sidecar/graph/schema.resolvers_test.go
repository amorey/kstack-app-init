package graph_test

// Behavioral tests for the GraphQL resolvers, exercised over a real gqlgen HTTP
// server. Fixtures and the fakeClusterService live in cluster_testutils_test.go.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// --- Cluster ---

// The clusters query maps the beehive records including the nil → null
// convention for unset/never-probed pointer fields.
func TestClustersQuery(t *testing.T) {
	data := clustersQueryData(t, `{ clusters {
		id
		generation
		spec { name syncEnabled enabled source { kubeconfig { context } } }
		status {
			source { kubeconfig { cluster user isPresent isDefault } }
			server { uid version endpoint }
			principal { username groups }
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
	if ep := unprobedStatus["server"].(map[string]any)["endpoint"]; ep != nil {
		t.Errorf("never-probed endpoint should be null, got: %v", ep)
	}
}

// Cluster.events maps the service's Events onto the wire: the run id rides
// the ObjectID scalar (decimal string), the type binds to the EventType enum
// (Normal/Warning), and the value slice is adapted to gqlgen's pointer slice.
func TestClusterEventsResolver(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	now := time.Now().UTC()
	svc.events[id] = []clustersvc.Event{{
		ID: id, Category: "connection", Type: beehive.EventWarning,
		Reason: "ProbeFailed", Message: "boom", Count: 3, FirstAt: now, LastAt: now,
	}}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	query := `{ cluster(id: "` + strconv.FormatInt(int64(id), 10) + `") {
		events(category: "connection") { id category type reason message count firstAt lastAt }
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			Cluster struct {
				Events []map[string]any `json:"events"`
			} `json:"cluster"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	if len(resp.Data.Cluster.Events) != 1 {
		t.Fatalf("want 1 event, got %d: %s", len(resp.Data.Cluster.Events), raw)
	}
	ev := resp.Data.Cluster.Events[0]
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

// The cluster query returns the record for a tracked id.
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

	// The cache resolves its conditions on a bare fixture. It has no status block: the
	// kind measures nothing itself.
	cache := firstCacheFrame(t, srv.URL)
	spec, _ := cache["spec"].(map[string]any)
	if spec["serverUid"] != "uid-1" || ownerOf(cache)["id"] != "1" {
		t.Errorf("cache identity: %v", cache)
	}
	if conds, ok := cache["conditions"].([]any); !ok || len(conds) != 0 {
		t.Errorf("sync conditions should be an empty list, got: %v", cache["conditions"])
	}
}

// Live conditions (cluster + cache) reach the wire with the correct GraphQL shapes —
// type/status/reason/message/liveness/timestamps. Conditions sit beside status, not
// inside it, since beehive stores them as their own object rows.
func TestConditionsAndSyncStatusOnWire(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	fixtures := clusterFixtures()
	fixtures[0].connConds = []clustersvc.Condition{{
		Type: string(clustersvc.ConditionConnected), Status: clustersvc.ConditionFalse,
		Reason: "ProbeFailed", Message: "connection refused",
		Liveness: true, TransitionedAt: at, UpdatedAt: at,
	}}
	fixtures[0].cacheConds = []clustersvc.Condition{{
		Type: string(clustersvc.ConditionSynced), Status: clustersvc.ConditionTrue,
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

	// The cache's coarse Synced condition rides clusterCachesWatch. Freshness does not:
	// the sync children report it, out of band from the object graph.
	cache := firstCacheFrame(t, srv.URL)
	syncConds, _ := cache["conditions"].([]any)
	if len(syncConds) != 1 {
		t.Fatalf("Synced condition on the wire: %+v", cache["conditions"])
	}
	c0 := syncConds[0].(map[string]any)
	if c0["type"] != "Synced" || c0["status"] != "True" || c0["reason"] != "Watching" {
		t.Errorf("Synced condition on the wire: %+v", c0)
	}
}

// --- ClusterCache ---

// The plural root fields serve the same records as the nested ones, scoped by an
// OPTIONAL parent id: omit it for the whole fleet, pass it for one parent. The fixture
// gives each cluster one cache and each cache one sync record, so the two forms are
// distinguishable by count.
func TestPluralRootFieldsScopeOptionally(t *testing.T) {
	fix := clusterFixtures()
	srv := newTestServer(t, fix)
	clusterID := strconv.FormatInt(int64(fix[0].id), 10)
	cacheID := strconv.FormatInt(int64(fixtureCacheID(fix[0].id)), 10)

	tests := []struct {
		name      string
		query     string
		field     string
		wantCount int
	}{
		{"caches unscoped", `{ clusterCaches { id } }`, "clusterCaches", len(fix)},
		{"caches scoped", `{ clusterCaches(clusterID: "` + clusterID + `") { id } }`, "clusterCaches", 1},
		{"syncs unscoped", `{ clusterCachedKinds { id } }`, "clusterCachedKinds", len(fix)},
		{"syncs scoped", `{ clusterCachedKinds(cacheID: "` + cacheID + `") { id } }`, "clusterCachedKinds", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]string{"query": tt.query})
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
			if got := len(resp.Data[tt.field]); got != tt.wantCount {
				t.Fatalf("want %d records, got %d: %s", tt.wantCount, got, raw)
			}
		})
	}
}

// ClusterCache.syncs completes the navigable path Cluster → caches → syncs.
func TestClusterCacheSyncsResolver(t *testing.T) {
	fix := clusterFixtures()
	srv := newTestServer(t, fix)
	cacheID := fixtureCacheID(fix[0].id)

	query := `{ clusterCache(id: "` + strconv.FormatInt(int64(cacheID), 10) + `") {
		cachedKinds { id owner { id kind } spec { apiVersion resource } }
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			ClusterCache struct {
				CachedKinds []map[string]any `json:"cachedKinds"`
			} `json:"clusterCache"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	got := resp.Data.ClusterCache.CachedKinds
	if len(got) != 1 {
		t.Fatalf("want this cache's one record, got %d: %s", len(got), raw)
	}
	if got[0]["id"] != strconv.FormatInt(int64(fixtureKindID(fix[0].id)), 10) {
		t.Errorf("id: want the sync record's own id, got %v", got[0]["id"])
	}
	if ownerOf(got[0])["id"] != strconv.FormatInt(int64(cacheID), 10) {
		t.Errorf("owner: want the cache's id, got %v", ownerOf(got[0]))
	}
}

// Cluster.caches is the navigable path down the owner chain — the only way into a
// cache by query without already holding its id. Asserted on the wire because a
// resolver that isn't wired returns an empty list rather than failing.
func TestClusterCachesResolver(t *testing.T) {
	fix := clusterFixtures()
	srv := newTestServer(t, fix)
	id := fix[0].id

	query := `{ cluster(id: "` + strconv.FormatInt(int64(id), 10) + `") {
		caches { id owner { id kind } spec { serverUid } }
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			Cluster struct {
				Caches []map[string]any `json:"caches"`
			} `json:"cluster"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	got := resp.Data.Cluster.Caches
	if len(got) != 1 {
		t.Fatalf("want this cluster's one cache, got %d: %s", len(got), raw)
	}
	// Its own id, not its cluster's — the two differ in the fixture on purpose.
	if got[0]["id"] != strconv.FormatInt(int64(fixtureCacheID(id)), 10) {
		t.Errorf("id: want the cache's own id, got %v", got[0]["id"])
	}
	if ownerOf(got[0])["id"] != strconv.FormatInt(int64(id), 10) {
		t.Errorf("owner: want the parent's id, got %v", ownerOf(got[0]))
	}
}

// The two cache-side event timelines are the same generic reader hung off a different
// record: `ClusterCache.events` reads the cache's own timeline (what the cache layer
// records, e.g. SyncStopped), `ClusterCachedKind.events` one synced kind's (where
// each worker report lands). One table because the wire mapping under test —
// clustersvc.Event → the generic Event shape, enum included — is identical; only the record it
// hangs off differs. Reaching either also exercises its root lookup, which is the only
// way into these records by query.
func TestCacheEventTimelineResolvers(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name string
		// field is the root lookup; lookupID derives the record's own id from its
		// cluster's, since the two are deliberately different in the fixture.
		field    string
		lookupID func(clustersvc.ClusterID) clustersvc.ObjectID
		seed     func(*fakeClusterService, clustersvc.ClusterID, clustersvc.Event)
		event    clustersvc.Event
		wantEnum string
	}{{
		name:     "cache timeline",
		field:    "clusterCache",
		lookupID: func(id clustersvc.ClusterID) clustersvc.ObjectID { return clustersvc.ObjectID(fixtureCacheID(id)) },
		seed: func(f *fakeClusterService, id clustersvc.ClusterID, ev clustersvc.Event) {
			f.cacheEvents = map[clustersvc.ClusterCacheID][]clustersvc.Event{fixtureCacheID(id): {ev}}
		},
		event: clustersvc.Event{
			Category: "sync", Type: beehive.EventWarning, Reason: "SyncFailed",
			Message: "boom", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Warning",
	}, {
		name:     "per-kind sync timeline",
		field:    "clusterCachedKind",
		lookupID: func(id clustersvc.ClusterID) clustersvc.ObjectID { return clustersvc.ObjectID(fixtureKindID(id)) },
		seed: func(f *fakeClusterService, id clustersvc.ClusterID, ev clustersvc.Event) {
			f.syncEvents = map[clustersvc.ClusterCachedKindID][]clustersvc.Event{fixtureKindID(id): {ev}}
		},
		event: clustersvc.Event{
			Category: "sync", Type: beehive.EventNormal, Reason: "SyncComplete",
			Message: "cached 12 events", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Normal",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fix := clusterFixtures()
			svc := newFakeClusterService(fix)
			id := tt.lookupID(fix[0].id)
			ev := tt.event
			ev.ID = id
			tt.seed(svc, fix[0].id, ev)
			srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
				ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
			}))
			t.Cleanup(srv.Close)

			query := `{ ` + tt.field + `(id: "` + strconv.FormatInt(int64(id), 10) + `") {
				events(category: "sync") { id category type reason message count firstAt lastAt }
			} }`
			body, _ := json.Marshal(map[string]string{"query": query})
			raw := postGQL(t, srv.URL, string(body))

			var resp struct {
				Data map[string]struct {
					Events []map[string]any `json:"events"`
				}
				Errors []struct{ Message string }
			}
			if err := json.Unmarshal(raw, &resp); err != nil {
				t.Fatalf("decode %s: %v", raw, err)
			}
			if len(resp.Errors) > 0 {
				t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
			}
			got := resp.Data[tt.field].Events
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

// clusterCacheClear empties one cache's file and returns the (still-tracked) record.
// The id it takes is the cache's own — a cluster id names no single cache once a UID
// migration has left two — so the fixture's cluster id must not resolve. An unknown id
// surfaces the not-found error.
func TestClusterCacheClearMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	body, _ := json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: 101) { id } }`})
	raw := postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), `"id":"101"`) {
		t.Errorf("expected the cleared cache back, got %s", raw)
	}

	body, _ = json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: 1) { id } }`})
	raw = postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), "errors") {
		t.Errorf("expected a GraphQL error for a cluster id, got %s", raw)
	}

	body, _ = json.Marshal(map[string]string{"query": `mutation { clusterCacheClear(id: "999") { id } }`})
	raw = postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), "errors") {
		t.Errorf("expected a GraphQL error for an unknown id, got %s", raw)
	}
}

// The wire keeps the positive form, matching the cluster's own two toggles, while the
// stored field is its inverse so a record written before the field decodes as syncing. One
// negation at the projection — so the round trip is what pins them together.
func TestClusterCachedKindSyncEnabledSetMutation(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())
	id := strconv.FormatInt(int64(fixtureKindID(1)), 10)

	body, _ := json.Marshal(map[string]string{
		"query": `mutation { clusterCachedKindSyncEnabledSet(id: ` + id + `, syncEnabled: false) { id spec { syncEnabled } } }`,
	})
	raw := postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), `"syncEnabled":false`) {
		t.Errorf("expected the paused record back, got %s", raw)
	}

	body, _ = json.Marshal(map[string]string{
		"query": `mutation { clusterCachedKindSyncEnabledSet(id: "999", syncEnabled: false) { id } }`,
	})
	raw = postGQL(t, srv.URL, string(body))
	if !strings.Contains(string(raw), "errors") {
		t.Errorf("expected a GraphQL error for an unknown id, got %s", raw)
	}
}

// TestClusterCachedKindsWatchIsCacheScoped pins the scoping on the wire: the stream is
// opened for one cache and must carry only that cache's kinds. The fixture gives each
// cache one record, so a leak shows up as a second frame.
func TestClusterCachedKindsWatchIsCacheScoped(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCachedKindsWatch(cacheID: "`+strconv.FormatInt(int64(fixtureCacheID(1)), 10)+`") { type kind { id owner { id kind } `+
			`spec { apiVersion kind resource namespaced } conditions { type status reason } } } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	seen := 0
	deadline := time.After(time.Second)
	for {
		var frame struct {
			Data struct {
				Watch struct {
					Type string         `json:"type"`
					Kind map[string]any `json:"kind"`
				} `json:"clusterCachedKindsWatch"`
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
				t.Fatalf("decode cached-kind frame %s: %v", ev.data, err)
			}
			// Detect the snapshot boundary by type, never by a missing entity: an
			// errored non-null field nulls its parent, so a null entity also rides
			// ordinary frames.
			if frame.Data.Watch.Type == string(clustersvc.DeltaFrameBookmark) {
				continue
			}
			seen++
			cachedKind := frame.Data.Watch.Kind
			if ownerOf(cachedKind)["id"] != strconv.FormatInt(int64(fixtureCacheID(1)), 10) {
				t.Fatalf("another cache's record leaked into the stream: %v", cachedKind)
			}
			spec, _ := cachedKind["spec"].(map[string]any)
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

// TestDeltaWatchClosesSnapshotWithBookmark pins the boundary a consumer waits on before
// rendering an empty state: exactly one Bookmark, after the Added frames, carrying no
// entity. Without it a still-listing collection is indistinguishable from an empty one.
func TestDeltaWatchClosesSnapshotWithBookmark(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCachesWatch { type cache { id } } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	added, bookmarks := 0, 0
	deadline := time.After(time.Second)
	for {
		var frame struct {
			Data struct {
				Watch struct {
					Type  string         `json:"type"`
					Cache map[string]any `json:"cache"`
				} `json:"clusterCachesWatch"`
			} `json:"data"`
		}
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("stream closed before the bookmark arrived")
			}
			if ev.event != "next" {
				continue
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode cache frame %s: %v", ev.data, err)
			}
			switch frame.Data.Watch.Type {
			case string(clustersvc.DeltaFrameBookmark):
				bookmarks++
				if frame.Data.Watch.Cache != nil {
					t.Errorf("the bookmark carries no entity, got: %v", frame.Data.Watch.Cache)
				}
				if bookmarks == 1 && added != len(clusterFixtures()) {
					t.Errorf("bookmark closed the snapshot after %d of %d records", added, len(clusterFixtures()))
				}
			case string(clustersvc.DeltaFrameAdded):
				if bookmarks > 0 {
					t.Error("an Added frame arrived after the snapshot closed")
				}
				added++
			}
		case <-deadline:
			if bookmarks != 1 {
				t.Fatalf("expected exactly one bookmark, saw %d", bookmarks)
			}
			return
		}
	}
}

// TestClusterCacheStatsWatchServesGauge pins that the cache summary is reachable as a
// stream — the only shape that keeps reporting once the cache record itself settles.
func TestClusterCacheStatsWatchServesGauge(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCacheStatsWatch(id: "1", cacheID: "`+strconv.FormatInt(int64(fixtureCacheID(1)), 10)+`") { exists bytes objectCount kindCount } }`)
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

// --- Cluster data ---
// The discovered kind catalog and the cached objects read out of a ClusterCache.

// ClusterCache.kinds maps the service's ClusterCachedDataKinds onto the wire 1:1
// (bound via gqlgen.yml), so the resolver just adapts the value slice to a pointer
// slice. Both ids it reads with come off the record, so the pair cannot disagree.
func TestClusterCachedDataKindsResolver(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.kinds = map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataKind{
		id: {
			{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: "Namespaced", IsCRD: false},
			{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: "Namespaced", IsCRD: true},
		},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	query := `{ clusterCache(id: "` + strconv.FormatInt(int64(fixtureCacheID(id)), 10) + `") {
		kinds { apiVersion kind resource scope isCRD }
	} }`
	body, _ := json.Marshal(map[string]string{"query": query})
	raw := postGQL(t, srv.URL, string(body))

	var resp struct {
		Data struct {
			ClusterCache struct {
				Kinds []map[string]any `json:"kinds"`
			} `json:"clusterCache"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if len(resp.Errors) > 0 {
		t.Fatalf("unexpected GraphQL errors: %+v", resp.Errors)
	}
	if len(resp.Data.ClusterCache.Kinds) != 2 {
		t.Fatalf("want 2 kinds, got %d: %s", len(resp.Data.ClusterCache.Kinds), raw)
	}
	k := resp.Data.ClusterCache.Kinds[0]
	if k["apiVersion"] != "apps/v1" || k["kind"] != "Deployment" || k["resource"] != "deployments" {
		t.Errorf("first kind: %v", k)
	}
	if k["scope"] != "Namespaced" || k["isCRD"] != false {
		t.Errorf("first kind scope/isCRD: %v", k)
	}
	if resp.Data.ClusterCache.Kinds[1]["isCRD"] != true {
		t.Errorf("second kind should be a CRD: %v", resp.Data.ClusterCache.Kinds[1])
	}

	// An unknown id resolves the record to null rather than erroring, so the catalog
	// is never reached — where the root field used to answer with an empty list.
	q2 := `{ clusterCache(id: "99999") { kinds { kind } } }`
	b2, _ := json.Marshal(map[string]string{"query": q2})
	raw2 := postGQL(t, srv.URL, string(b2))
	var resp2 struct {
		Data struct {
			ClusterCache *struct {
				Kinds []map[string]any `json:"kinds"`
			} `json:"clusterCache"`
		}
		Errors []struct{ Message string }
	}
	if err := json.Unmarshal(raw2, &resp2); err != nil {
		t.Fatalf("decode %s: %v", raw2, err)
	}
	if len(resp2.Errors) > 0 || resp2.Data.ClusterCache != nil {
		t.Fatalf("unknown cache should resolve to null, got %s", raw2)
	}
}

// clusterCachedDataObjectsWatch wires the subscription resolver to the service: the fake with
// no seeded objects opens an empty-until-ctx stream, so the SSE dial succeeds and the
// stream stays open with no frames rather than erroring.
func TestClusterCachedDataObjectsWatchOpensWithoutError(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	idStr := strconv.FormatInt(int64(id), 10)
	q := `subscription { clusterCachedDataObjectsWatch(id: "` + idStr + `", cacheID: "` + idStr +
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
func TestClusterCachedDataObjectsWatchServesNativeBody(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.dataObjects = map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataObject{
		id: {{
			UID: "d1", APIVersion: "apps/v1", Kind: "Deployment", Namespace: "default", Name: "web",
			RawJSON: clustersvc.RawJSON(`{"kind":"Deployment","spec":{"replicas":3}}`),
		}},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	idStr := strconv.FormatInt(int64(id), 10)
	q := `subscription { clusterCachedDataObjectsWatch(id: "` + idStr + `", cacheID: "` + idStr +
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
					ClusterCachedDataObjectsWatch struct {
						Type   string `json:"type"`
						Object struct {
							UID     string         `json:"uid"`
							Name    string         `json:"name"`
							RawJSON map[string]any `json:"rawJSON"`
						} `json:"object"`
					} `json:"clusterCachedDataObjectsWatch"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode frame %s: %v", ev.data, err)
			}
			got := frame.Data.ClusterCachedDataObjectsWatch
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

// clusterCachedDataKindsWatch streams the kind catalog as a delta watch: the resolver
// adapts the service's ClusterCachedDataKindWatchFrame stream to the wire 1:1, so the snapshot
// arrives as Added changes carrying the kind's fields (incl. the live count) and the
// stream stays open for live updates.
func TestClusterCachedDataKindsWatchEmitsSnapshotAndStaysOpen(t *testing.T) {
	fix := clusterFixtures()
	svc := newFakeClusterService(fix)
	id := fix[0].id
	svc.kinds = map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataKind{
		id: {
			{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: "Namespaced", IsCRD: false, Count: 3},
			{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: "Namespaced", IsCRD: true, Count: 0},
		},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{
		ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{}),
	}))
	t.Cleanup(srv.Close)

	q := `subscription { clusterCachedDataKindsWatch(id: "` + strconv.FormatInt(int64(id), 10) +
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
					ClusterCachedDataKindsWatch struct {
						Type string `json:"type"`
						Kind struct {
							Kind  string `json:"kind"`
							Count int    `json:"count"`
						} `json:"kind"`
					} `json:"clusterCachedDataKindsWatch"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(ev.data), &frame); err != nil {
				t.Fatalf("decode frame %s: %v", ev.data, err)
			}
			seen[frame.Data.ClusterCachedDataKindsWatch.Kind.Kind] = frame.Data.ClusterCachedDataKindsWatch.Kind.Count
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

// --- Cloud account ---

// postGQL POSTs a GraphQL query/mutation body to url's /graphql endpoint and
// returns the raw response.
func postGQL(t *testing.T, url, body string) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return raw
}

// The fakes (memCredStore/fakeOAuthFlow/fakeLoopback) live in testutils_test.go;
// the postGQL / SSE helpers are defined above + in server_test.go.

// The authState query reflects a signed-out auth service (no stale identity).
func TestAuthStateQuerySignedOut(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: newFakeAuth(auth.Identity{})}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"{ authState { authenticated identity { sub } } }"}`))
	if !strings.Contains(raw, `"identity":null`) {
		t.Fatalf("want signed-out auth state (null identity), got: %s", raw)
	}
	if !strings.Contains(raw, `"authenticated":false`) {
		t.Fatalf("want authenticated false, got: %s", raw)
	}
}

// signedInAuth returns a fake auth.Service already signed in as the given
// identity. The resolver depends on the auth.Service interface, so the tests fake
// it (see fakeAuth) rather than constructing the real service.
func signedInAuth(t *testing.T, id auth.Identity) auth.Service {
	t.Helper()
	return signedInFakeAuth(id)
}

// configuredAuth returns a signed-out fake auth.Service that signs in as id when
// Login runs (the resolver's login flow).
func configuredAuth(t *testing.T, id auth.Identity) auth.Service {
	t.Helper()
	return newFakeAuth(id)
}

// The authState query reflects the current signed-in identity.
func TestAuthStateQuerySignedIn(t *testing.T) {
	svc := signedInAuth(t, auth.Identity{UserID: "u1", Email: "a@x.com", Name: "Ada"})

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"{ authState { authenticated identity { sub email name } } }"}`))
	if !strings.Contains(raw, `"email":"a@x.com"`) {
		t.Fatalf("want signed-in identity, got: %s", raw)
	}
	if !strings.Contains(raw, `"authenticated":true`) {
		t.Fatalf("want authenticated true, got: %s", raw)
	}
}

// authStateWatch emits the current snapshot first, then a fresh snapshot on change.
func TestAuthStateWatchSnapshotThenDelta(t *testing.T) {
	svc := configuredAuth(t, auth.Identity{Email: "a@x.com"})
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	resp := openSSESubscription(t, srv.URL, "",
		"subscription { authStateWatch { authenticated identity { email } } }")
	defer resp.Body.Close() // ends the subscription; must run before srv.Close()
	events := sseEvents(t, resp)

	if ev := nextSSE(t, events); ev.event != "next" || !strings.Contains(ev.data, `"authenticated":false`) {
		t.Fatalf("first frame: event=%q data=%s", ev.event, ev.data)
	}

	if err := svc.StartLogin(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}

	if ev := nextSSE(t, events); ev.event != "next" || !strings.Contains(ev.data, `"email":"a@x.com"`) {
		t.Fatalf("delta frame: event=%q data=%s", ev.event, ev.data)
	}
}

// logout delegates to the account: returns true and flips the session signed-out.
func TestLogoutMutation(t *testing.T) {
	svc := signedInFakeAuth(auth.Identity{})
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLogout }"}`))
	if !strings.Contains(raw, `"authLogout":true`) {
		t.Fatalf("want authLogout true, got: %s", raw)
	}
	if cur, _ := svc.Current(context.Background()); cur.Authenticated {
		t.Fatal("session still signed in after logout")
	}
}

// login is non-blocking: it returns true immediately and the resulting signed-in
// session arrives asynchronously (observed here via the auth-state watch), proving
// the mutation kicked off the flow without blocking on the browser round-trip.
func TestLoginMutationKicksOffFlow(t *testing.T) {
	svc := newFakeAuth(auth.Identity{Email: "a@x.com"})

	// The auth-state stream is latest-value (current-on-subscribe): the first State
	// is the signed-out baseline, and the signed-in flow surfaces as a later State
	// with Authenticated true. The loop skips the baseline and waits for sign-in.
	states, cancel := svc.Subscribe()
	defer cancel()

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLoginStart }"}`))
	if !strings.Contains(raw, `"authLoginStart":true`) {
		t.Fatalf("want login true, got: %s", raw)
	}

	for {
		select {
		case st := <-states:
			if st.Authenticated {
				if st.Identity == nil || st.Identity.Email != "a@x.com" {
					t.Fatalf("signed-in identity = %+v", st.Identity)
				}
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("login never produced a signed-in session")
		}
	}
}

// A synchronous setup failure (loopback bind / browser launch) surfaces as a
// GraphQL error rather than a silent login:true — the whole point of running the
// flow's setup phase synchronously.
func TestLoginMutationSurfacesSetupError(t *testing.T) {
	svc := newFakeAuth(auth.Identity{Email: "a@x.com"})
	svc.loginErr = errors.New("loopback bind failed")

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLoginStart }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want GraphQL error for a setup failure, got: %s", raw)
	}
	if strings.Contains(raw, `"authLoginStart":true`) {
		t.Fatalf("login must not report true when setup failed, got: %s", raw)
	}
}

// TestClusterCacheSyncStatusWatchServesEveryKind pins the one field on the wire that carries
// a per-kind verdict: nothing else can say which of a cache's hundred kinds is failing.
func TestClusterCacheSyncStatusWatchServesEveryKind(t *testing.T) {
	srv := newTestServer(t, clusterFixtures())

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCacheSyncStatusWatch(id: "1", cacheID: "`+strconv.FormatInt(int64(fixtureCacheID(1)), 10)+`") { cacheID discovery { reason } kinds { apiVersion resource reason restarts objectCount } } }`)
	defer resp.Body.Close()
	events := sseEvents(t, resp)

	deadline := time.After(2 * time.Second)
	for {
		var frame struct {
			Data struct {
				Status struct {
					CacheID   string           `json:"cacheID"`
					Discovery map[string]any   `json:"discovery"`
					Kinds     []map[string]any `json:"kinds"`
				} `json:"clusterCacheSyncStatusWatch"`
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
				t.Fatalf("decode sync status frame %s: %v", ev.data, err)
			}
			if got := frame.Data.Status.Discovery["reason"]; got != "Discovered" {
				t.Errorf("discovery reason = %v, want Discovered", got)
			}
			if len(frame.Data.Status.Kinds) == 0 {
				t.Fatal("a cache with mirrored kinds served none")
			}
			for _, kind := range frame.Data.Status.Kinds {
				if kind["apiVersion"] == "" || kind["resource"] == "" {
					t.Errorf("a kind row identifies nothing: %v", kind)
				}
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for a sync status frame")
		}
	}
}

// A timestamp the source object never carried serializes as null, not as 0001-01-01. The records
// keep a value time.Time — the delta-watch diff compares frames with ==, so they must stay
// comparable — and this is the wire's whole answer for "absent".
func TestAnAbsentTimestampSerializesAsNull(t *testing.T) {
	seen := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	svc := newFakeClusterService(clusterFixtures())
	svc.dataEvents = map[clustersvc.ClusterID][]clustersvc.ClusterCachedDataEvent{
		1: {{UID: "e-1", LastSeen: seen}},
	}
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{ClusterSvc: svc, Auth: newFakeAuth(auth.Identity{})}))
	t.Cleanup(srv.Close)

	resp := openSSESubscription(t, srv.URL, "",
		`subscription { clusterCachedDataEventsWatch(id: "1", cacheID: "1") { event { firstSeen lastSeen } } }`)
	defer resp.Body.Close()

	frame := testutil.Recv(t, sseEvents(t, resp), "the first frame")

	assert.Contains(t, frame.data, `"firstSeen":null`)
	assert.Contains(t, frame.data, `"lastSeen":"2026-08-30T12:00:00Z"`)
}
