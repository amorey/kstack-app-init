package graph_test

// Behavioral tests for the resolvers in schema.resolvers.go, exercised over a
// real gqlgen HTTP server (POST for queries/mutations, SSE for subscriptions).
// The SSE plumbing helpers (openSSESubscription/sseEvents/nextSSE) live in
// server_test.go alongside the transport canaries — they're shared within this
// package.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/hub"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
)

// postGQL POSTs a GraphQL query/mutation body to url's /graphql endpoint and
// returns the raw response. A non-empty token is sent as the Authorization
// bearer header — the single auth path shared with SSE subscriptions.
func postGQL(t *testing.T, url, token, body string) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return raw
}

// --- settings -------------------------------------------------------------

// Post-cutover wiring: the Resolver reads the engine's syncstore and shares
// the engine's Hub. The read path never touches the cloud (the engine is
// the only cloud talker); only `updateSettings` still write-throughs.
func newStack(t *testing.T, cloudHandler http.Handler) (sidecarURL string, store *syncstore.Store[prefs.Settings], hub *prefs.Hub) {
	t.Helper()
	cloudSrv := httptest.NewServer(cloudHandler)
	t.Cleanup(cloudSrv.Close)

	store = syncstore.NewStore[prefs.Settings](filepath.Join(t.TempDir(), "settings.json"))
	hub = prefs.NewHub()
	r := &graph.Resolver{
		Cloud: cloud.New(cloudSrv.URL),
		Store: store,
		Hub:   hub,
	}
	sidecar := httptest.NewServer(graph.NewServer(r))
	t.Cleanup(sidecar.Close)
	return sidecar.URL, store, hub
}

// `settings` is served from the syncstore (engine-maintained) and must not
// contact the cloud — the engine owns the only upstream connection.
func TestSettingsServedFromStoreNoCloud(t *testing.T) {
	cloudHit := false
	url, store, _ := newStack(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		cloudHit = true
	}))
	if err := store.Save(syncstore.Envelope[prefs.Settings]{
		Data:    prefs.Settings{Placeholder: "from-store"},
		Version: "1",
	}); err != nil {
		t.Fatal(err)
	}

	raw := postGQL(t, url, "tok", `{"query":"{ settings { placeholder } }"}`)
	if !strings.Contains(string(raw), `"placeholder":"from-store"`) {
		t.Fatalf("response: %s", raw)
	}
	if cloudHit {
		t.Fatal("read path contacted the cloud")
	}
}

// An empty store (engine never synced — e.g. logged out) is not an error;
// the zero value flows through.
func TestSettingsEmptyStoreReturnsZero(t *testing.T) {
	url, _, _ := newStack(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("cloud must not be contacted")
	}))
	raw := postGQL(t, url, "tok", `{"query":"{ settings { placeholder } }"}`)
	if !strings.Contains(string(raw), `"placeholder":""`) {
		t.Fatalf("response: %s", raw)
	}
}

// `updateSettings` still write-throughs to the cloud and publishes to the
// shared Hub so active subscribers see it immediately (the engine persists
// it when the cloud echoes it back on its stream).
func TestUpdateSettingsWriteThroughAndPublishes(t *testing.T) {
	var sawInput map[string]any
	url, _, hub := newStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if vars, ok := body["variables"].(map[string]any); ok {
			sawInput, _ = vars["input"].(map[string]any)
		}
		_, _ = w.Write([]byte(`{"data":{"updateSettings":{"placeholder":"v2"}}}`))
	}))

	sub, unsub := hub.Subscribe()
	defer unsub()

	mutation := `{"query":"mutation($input: UpdateSettingsInput!) { updateSettings(input: $input) { placeholder } }",` +
		`"variables":{"input":{"placeholder":"v2"}}}`
	raw := postGQL(t, url, "tok", mutation)
	if !strings.Contains(string(raw), `"placeholder":"v2"`) {
		t.Fatalf("response: %s", raw)
	}
	if sawInput["placeholder"] != "v2" {
		t.Fatalf("input not forwarded: %+v", sawInput)
	}
	select {
	case s := <-sub:
		if s.Placeholder != "v2" {
			t.Fatalf("hub got %+v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutation did not publish to the Hub")
	}
}

// `settingsWatch` subscribes the shared Hub only (no per-resolver cloud
// SSE): a new subscriber gets the current store snapshot first, then live
// Hub publishes.
func TestSettingsWatchSnapshotThenHubDeltas(t *testing.T) {
	url, store, hub := newStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A cloud SSE attempt here is a regression (per-resolver SSE
		// must be gone).
		if r.Header.Get("Accept") == "text/event-stream" {
			t.Error("settingsWatch opened a cloud SSE")
		}
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	if err := store.Save(syncstore.Envelope[prefs.Settings]{
		Data: prefs.Settings{Placeholder: "snap"}, Version: "1",
	}); err != nil {
		t.Fatal(err)
	}

	resp := openSSESubscription(t, url, "tok", "subscription { settingsWatch { placeholder } }")
	defer resp.Body.Close() // ends the subscription; must run before srv.Close()
	events := sseEvents(resp)

	readPlaceholder := func(want string) {
		ev := nextSSE(t, events)
		if ev.event != "next" {
			t.Fatalf("want event=next, got %q (data=%s)", ev.event, ev.data)
		}
		var msg struct {
			Data struct {
				SettingsWatch struct {
					Placeholder string `json:"placeholder"`
				} `json:"settingsWatch"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(ev.data), &msg); err != nil {
			t.Fatalf("decode %s: %v", ev.data, err)
		}
		if msg.Data.SettingsWatch.Placeholder != want {
			t.Fatalf("want %q, got %s", want, ev.data)
		}
	}

	readPlaceholder("snap") // current snapshot first
	hub.Publish(prefs.Settings{Placeholder: "live"})
	readPlaceholder("live") // then live Hub deltas
}

// updateSettings: a cloud-failed write surfaces the error to the client AND
// persists the mutation; a subsequent successful Drain replays it and clears
// the queue.
func TestUpdateSettingsQueuesOnCloudFailure(t *testing.T) {
	cloudCalls := 0
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls++
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer cloudSrv.Close()

	q := mutationqueue.New(filepath.Join(t.TempDir(), "mutations.json"))
	r := &graph.Resolver{
		Cloud: cloud.New(cloudSrv.URL),
		Store: syncstore.NewStore[prefs.Settings](filepath.Join(t.TempDir(), "settings.json")),
		Hub:   prefs.NewHub(),
		Queue: q,
	}
	sidecar := httptest.NewServer(graph.NewServer(r))
	defer sidecar.Close()

	mutation := `{"query":"mutation($input: UpdateSettingsInput!) { updateSettings(input: $input) { placeholder } }",` +
		`"variables":{"input":{"placeholder":"offline-edit"}}}`
	raw := postGQL(t, sidecar.URL, "tok", mutation)

	// Client is told it failed (not silently 'ok').
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("want a GraphQL error surfaced, got %s", raw)
	}
	if p, _ := q.Pending(); !p {
		t.Fatal("mutation not queued after cloud failure")
	}

	// A successful drain replays the queued mutation and clears it.
	var pushed string
	if err := q.Drain(context.Background(), func(_ context.Context, in cloud.UpdateInput) error {
		pushed = *in.Placeholder
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if pushed != "offline-edit" {
		t.Fatalf("drained input = %q", pushed)
	}
	if p, _ := q.Pending(); p {
		t.Fatal("queue not cleared after successful drain")
	}
	if cloudCalls == 0 {
		t.Fatal("cloud was never attempted")
	}
}

// --- syncStatus -----------------------------------------------------------

// fakeStatus is a hand StatusSource: drives Status() / WatchStatus()
// without standing up a real engine + fake upstream. The fan-out is the
// real hub.Hub (same as the engine uses), so the test double can't drift
// from production subscribe/unsub semantics.
type fakeStatus struct {
	mu  sync.Mutex
	cur syncpkg.Status
	hub *hub.Hub[syncpkg.Status]
}

func newFakeStatus(s syncpkg.Status) *fakeStatus {
	return &fakeStatus{cur: s, hub: hub.New[syncpkg.Status]()}
}

func (f *fakeStatus) Status() syncpkg.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *fakeStatus) WatchStatus() (<-chan syncpkg.Status, func()) {
	return f.hub.Subscribe()
}

func (f *fakeStatus) push(s syncpkg.Status) {
	f.mu.Lock()
	f.cur = s
	f.mu.Unlock()
	f.hub.Publish(s)
}

func statusStack(t *testing.T, fs graph.StatusSource) string {
	t.Helper()
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Sync: fs}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// syncStatus query maps the engine Status onto the GraphQL type: state
// stringified, timestamps as Unix-millis ints, RetryAt only while backing
// off.
func TestSyncStatusQueryMapsEngineStatus(t *testing.T) {
	synced := time.UnixMilli(1_700_000_000_000)
	retry := time.UnixMilli(1_700_000_030_000)
	url := statusStack(t, newFakeStatus(syncpkg.Status{
		State:        syncpkg.StateBackoff,
		LastError:    "cloud unreachable",
		LastSyncedAt: synced,
		RetryAt:      retry,
	}))

	raw := postGQL(t, url, "", `{"query":"{ syncStatus { state lastError lastSyncedAt retryAt } }"}`)
	var resp struct {
		Data struct {
			SyncStatus struct {
				State        string `json:"state"`
				LastError    string `json:"lastError"`
				LastSyncedAt int64  `json:"lastSyncedAt"`
				RetryAt      int64  `json:"retryAt"`
			} `json:"syncStatus"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	got := resp.Data.SyncStatus
	if got.State != "BACKOFF" || got.LastError != "cloud unreachable" {
		t.Fatalf("got %+v", got)
	}
	if got.LastSyncedAt != synced.UnixMilli() || got.RetryAt != retry.UnixMilli() {
		t.Fatalf("timestamps: got %+v", got)
	}
}

// RetryAt is zeroed unless the engine is actually backing off, even if a
// stale RetryAt lingers on the struct.
func TestSyncStatusRetryAtZeroWhenNotBackoff(t *testing.T) {
	url := statusStack(t, newFakeStatus(syncpkg.Status{
		State:   syncpkg.StateLive,
		RetryAt: time.UnixMilli(1_700_000_030_000),
	}))
	raw := postGQL(t, url, "", `{"query":"{ syncStatus { state retryAt lastSyncedAt } }"}`)
	if !strings.Contains(string(raw), `"retryAt":0`) || !strings.Contains(string(raw), `"state":"LIVE"`) {
		t.Fatalf("want live + retryAt 0, got %s", raw)
	}
}

// syncStatusWatch emits the current status immediately, then every
// transition.
func TestSyncStatusWatchSnapshotThenTransitions(t *testing.T) {
	fs := newFakeStatus(syncpkg.Status{State: syncpkg.StateConnecting})
	url := statusStack(t, fs)

	resp := openSSESubscription(t, url, "", "subscription { syncStatusWatch { state } }")
	defer resp.Body.Close() // ends the subscription; must run before srv.Close()
	events := sseEvents(resp)

	readState := func(want string) {
		ev := nextSSE(t, events)
		if ev.event != "next" {
			t.Fatalf("want event=next, got %q (data=%s)", ev.event, ev.data)
		}
		var msg struct {
			Data struct {
				SyncStatusWatch struct {
					State string `json:"state"`
				} `json:"syncStatusWatch"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(ev.data), &msg); err != nil {
			t.Fatalf("decode %s: %v", ev.data, err)
		}
		if msg.Data.SyncStatusWatch.State != want {
			t.Fatalf("want %q, got %s", want, ev.data)
		}
	}

	readState("CONNECTING") // current snapshot first
	fs.push(syncpkg.Status{State: syncpkg.StateLive})
	readState("LIVE") // then transitions
}

// clusterSyncStatusWatch is a stub today: it emits an empty snapshot
// (no clusters wired to the sync engine yet), then holds the stream open
// until the client unsubscribes. The follow-up PR backs it with the real
// per-cluster source. This pins the contract + transport so the frontend
// can build against it now.
func TestClusterSyncStatusWatchEmitsEmptySnapshot(t *testing.T) {
	h := graph.NewServer(&graph.Resolver{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	resp := openSSESubscription(t, ts.URL, "",
		"subscription { clusterSyncStatusWatch { context state lastError lastSyncedAt downloadRateBps } }")
	defer resp.Body.Close() // ends the subscription; must run before ts.Close()
	events := sseEvents(resp)

	ev := nextSSE(t, events)
	if ev.event != "next" {
		t.Fatalf("want event=next, got %q (data=%s)", ev.event, ev.data)
	}
	var msg struct {
		Data struct {
			ClusterSyncStatusWatch []struct {
				Context string `json:"context"`
			} `json:"clusterSyncStatusWatch"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(ev.data), &msg); err != nil {
		t.Fatalf("decode %s: %v", ev.data, err)
	}
	if len(msg.Data.ClusterSyncStatusWatch) != 0 {
		t.Fatalf("want empty cluster list, got %s", ev.data)
	}
}

// --- kubeConfig / setCurrentContext ---------------------------------------

// writeKubeConfig writes a kubeconfig with the named contexts and the given
// current-context, returning the path.
func writeKubeConfig(t *testing.T, current string, contexts ...string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	for _, name := range contexts {
		cfg.Clusters["cluster-"+name] = &clientcmdapi.Cluster{}
		cfg.AuthInfos["user-"+name] = &clientcmdapi.AuthInfo{}
		cfg.Contexts[name] = &clientcmdapi.Context{Cluster: "cluster-" + name, AuthInfo: "user-" + name}
	}
	cfg.CurrentContext = current
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, clientcmd.WriteToFile(*cfg, path))
	return path
}

// newKubeConfigStack stands up a sidecar GraphQL server backed by a real
// KubeConfigWatcher over the given kubeconfig path.
func newKubeConfigStack(t *testing.T, kubeconfigPath string) (url string, watcher *k8shelpers.KubeConfigWatcher) {
	t.Helper()
	w, err := k8shelpers.NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	t.Cleanup(w.Close)

	r := &graph.Resolver{KubeConfigWatcher: w}
	srv := httptest.NewServer(graph.NewServer(r))
	t.Cleanup(srv.Close)
	return srv.URL, w
}

func TestSetCurrentContext_Mutation_Success(t *testing.T) {
	path := writeKubeConfig(t, "context-A", "context-A", "context-B")
	url, w := newKubeConfigStack(t, path)

	raw := postGQL(t, url, "", `{"query":"mutation { setCurrentContext(name: \"context-B\") }"}`)
	if !strings.Contains(string(raw), `"setCurrentContext":true`) {
		t.Fatalf("response: %s", raw)
	}

	assert.Equal(t, "context-B", w.Get().CurrentContext)

	onDisk, err := clientcmd.LoadFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, "context-B", onDisk.CurrentContext)
}

func TestSetCurrentContext_Mutation_UnknownContext(t *testing.T) {
	path := writeKubeConfig(t, "context-A", "context-A", "context-B")
	url, w := newKubeConfigStack(t, path)

	raw := postGQL(t, url, "", `{"query":"mutation { setCurrentContext(name: \"nope\") }"}`)
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("expected GraphQL error, got: %s", raw)
	}
	assert.Equal(t, "context-A", w.Get().CurrentContext)
}

// A handler with no watcher (Config{}) must return a clean GraphQL error
// rather than panicking — mirrors the KubeConfigWatch nil-guard.
func TestSetCurrentContext_Mutation_NilWatcher(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
	t.Cleanup(srv.Close)

	raw := postGQL(t, srv.URL, "", `{"query":"mutation { setCurrentContext(name: \"x\") }"}`)
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("expected GraphQL error, got: %s", raw)
	}
}
