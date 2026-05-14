package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/server"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// Spins up a fake cloud (handler chosen by caller) plus a sidecar handler
// wired to it. Returns the running sidecar URL plus the prefs.Store so
// tests can inspect the local cache.
func newStack(t *testing.T, cloudHandler http.Handler) (sidecarURL string, store *prefs.Store) {
	t.Helper()
	cloudSrv := httptest.NewServer(cloudHandler)
	t.Cleanup(cloudSrv.Close)

	store = prefs.NewStore(filepath.Join(t.TempDir(), "preferences.json"))
	r := &graph.Resolver{
		Cloud: cloud.New(cloudSrv.URL),
		Store: store,
		Hub:   prefs.NewHub(),
	}
	sidecar := httptest.NewServer(server.NewHandlerWithResolver(r))
	t.Cleanup(sidecar.Close)
	return sidecar.URL, store
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

// `settings` query: sidecar forwards to cloud (with the bearer header
// from the inbound request), returns the cloud value, and writes it to
// the local store.
func TestSettingsQueryWriteThrough(t *testing.T) {
	var sawAuth string
	url, store := newStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"data":{"settings":{"placeholder":"from-cloud"}}}`))
	}))

	raw := postGQL(t, url, "abc", `{"query":"{ settings { placeholder } }"}`)
	if !strings.Contains(string(raw), `"placeholder":"from-cloud"`) {
		t.Fatalf("response: %s", raw)
	}
	if sawAuth != "Bearer abc" {
		t.Fatalf("bearer not forwarded to cloud: %q", sawAuth)
	}
	got, _ := store.Load()
	if got.Placeholder != "from-cloud" {
		t.Fatalf("store not updated: %+v", got)
	}
}

// When the cloud errors, `settings` falls back to whatever is in the
// local store — the offline-read promise.
func TestSettingsQueryFallsBackToCache(t *testing.T) {
	url, store := newStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	if err := store.Save(prefs.Settings{Placeholder: "cached"}); err != nil {
		t.Fatal(err)
	}

	raw := postGQL(t, url, "tok", `{"query":"{ settings { placeholder } }"}`)
	if !strings.Contains(string(raw), `"placeholder":"cached"`) {
		t.Fatalf("response: %s", raw)
	}
}

// `updateSettings` posts to the cloud, stores the cloud's response,
// and publishes to any active settingsWatch subscriber.
func TestUpdateSettingsWriteThrough(t *testing.T) {
	var sawInput map[string]any
	url, store := newStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if vars, ok := body["variables"].(map[string]any); ok {
			sawInput, _ = vars["input"].(map[string]any)
		}
		_, _ = w.Write([]byte(`{"data":{"updateSettings":{"placeholder":"v2"}}}`))
	}))

	mutation := `{"query":"mutation($input: UpdateSettingsInput!) { updateSettings(input: $input) { placeholder } }",` +
		`"variables":{"input":{"placeholder":"v2"}}}`
	raw := postGQL(t, url, "tok", mutation)
	if !strings.Contains(string(raw), `"placeholder":"v2"`) {
		t.Fatalf("response: %s", raw)
	}
	if sawInput["placeholder"] != "v2" {
		t.Fatalf("input not forwarded: %+v", sawInput)
	}
	got, _ := store.Load()
	if got.Placeholder != "v2" {
		t.Fatalf("store not updated: %+v", got)
	}
}

// `settingsWatch` over WebSocket: each cloud SSE event lands on the
// client's WS stream in order; cancelling the WS tears down the cloud SSE.
func TestSettingsWatchSubscription(t *testing.T) {
	emit := make(chan string, 4)
	cloudClosed := make(chan struct{})
	cloudHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			// Non-subscription request — answer cleanly to keep tests focused.
			_, _ = w.Write([]byte(`{"data":{}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		defer close(cloudClosed)
		for {
			select {
			case <-r.Context().Done():
				return
			case v := <-emit:
				fmt.Fprintf(w, "event: next\ndata: {\"data\":{\"settingsWatch\":{\"placeholder\":%q}}}\n\n", v)
				flusher.Flush()
			}
		}
	})
	url, _ := newStack(t, cloudHandler)

	wsURL := "ws" + strings.TrimPrefix(url, "http") + "/graphql"
	dialer := websocket.Dialer{
		Subprotocols:     []string{"graphql-transport-ws"},
		HandshakeTimeout: 5 * time.Second,
	}
	header := http.Header{"Authorization": []string{"Bearer tok"}}
	conn, _, err := dialer.DialContext(context.Background(), wsURL, header)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	mustWrite(t, conn, `{"type":"connection_init","payload":{"Authorization":"Bearer tok"}}`)
	if got := mustReadType(t, conn); got != "connection_ack" {
		t.Fatalf("want connection_ack, got %q", got)
	}
	mustWrite(t, conn, `{"id":"1","type":"subscribe","payload":{"query":"subscription { settingsWatch { placeholder } }"}}`)

	// Push two events from the fake cloud.
	emit <- "alpha"
	emit <- "beta"

	for _, want := range []string{"alpha", "beta"} {
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
			t.Fatalf("event %s: got %s", want, raw)
		}
	}

	// Client unsubscribe — cloud SSE should tear down.
	mustWrite(t, conn, `{"id":"1","type":"complete"}`)
	select {
	case <-cloudClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("cloud SSE not closed after client unsubscribe")
	}
}
