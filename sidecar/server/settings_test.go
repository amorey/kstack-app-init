package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
	"github.com/kubetail-org/kstack-app/sidecar/server"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

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
	sidecar := httptest.NewServer(server.NewHandlerWithResolver(r))
	t.Cleanup(sidecar.Close)
	return sidecar.URL, store, hub
}

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

	conn := dialGraphQLWS(t, url, "tok")
	mustWrite(t, conn, `{"id":"1","type":"subscribe","payload":{"query":"subscription { settingsWatch { placeholder } }"}}`)

	readPlaceholder := func(want string) {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read %s: %v", want, err)
		}
		var msg struct {
			Type    string `json:"type"`
			Payload struct {
				Data struct {
					SettingsWatch struct {
						Placeholder string `json:"placeholder"`
					} `json:"settingsWatch"`
				} `json:"data"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if msg.Type != "next" || msg.Payload.Data.SettingsWatch.Placeholder != want {
			t.Fatalf("want %q, got %s", want, raw)
		}
	}

	readPlaceholder("snap") // current snapshot first
	hub.Publish(prefs.Settings{Placeholder: "live"})
	readPlaceholder("live") // then live Hub deltas

	mustWrite(t, conn, `{"id":"1","type":"complete"}`)
}
