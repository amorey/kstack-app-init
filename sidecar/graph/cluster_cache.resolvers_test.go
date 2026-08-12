package graph_test

// Behavioral tests for the ClusterCache resolvers — the cache record, its
// GVR-discovery/per-kind sync children, and the cache-side event timelines.
// Fixtures and the fakeClusterService live in cluster_testutils_test.go.

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

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
		seed     func(*fakeClusterService, domain.ObjectID, domain.Event)
		event    domain.Event
		wantEnum string
	}{{
		name:  "cache timeline",
		field: "clusterCacheEvents",
		seed: func(f *fakeClusterService, id domain.ObjectID, ev domain.Event) {
			f.cacheEvents = map[domain.ClusterCacheID][]domain.Event{id: {ev}}
		},
		event: domain.Event{
			Category: "sync", Type: beehive.EventWarning, Reason: "SyncFailed",
			Message: "boom", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Warning",
	}, {
		name:  "per-kind sync timeline",
		field: "clusterCacheGVRSyncEvents",
		seed: func(f *fakeClusterService, id domain.ObjectID, ev domain.Event) {
			f.syncEvents = map[domain.ClusterCacheGVRSyncID][]domain.Event{id: {ev}}
		},
		event: domain.Event{
			Category: "sync", Type: beehive.EventNormal, Reason: "SyncComplete",
			Message: "cached 12 events", Count: 2, FirstAt: now, LastAt: now,
		},
		wantEnum: "Normal",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fix := clusterFixtures()
			svc := newFakeClusterService(fix)
			id := domain.ObjectID(fix[0].id)
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
// stream — the only shape that keeps reporting once the cache record itself settles.
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
