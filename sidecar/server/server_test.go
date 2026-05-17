package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
	"github.com/kubetail-org/kstack-app/sidecar/server"
)

// TestPingQuery is the canary: a fresh server should answer `{ ping }` with "pong".
// If this passes, the gqlgen wiring (schema -> resolver -> handler) is intact.
func TestPingQuery(t *testing.T) {
	h, _ := server.NewHandler(server.Config{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	body := strings.NewReader(`{"query":"{ ping }"}`)
	resp, err := http.Post(ts.URL+"/graphql", "application/json", body)
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Data struct {
			Ping string `json:"ping"`
		} `json:"data"`
	}
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if out.Data.Ping != "pong" {
		t.Fatalf("want ping=pong, got %q (raw=%s)", out.Data.Ping, raw)
	}
}

// TestTickSubscription opens a graphql-transport-ws connection, subscribes to
// `tick`, and asserts the first two values are 1 and 2. Validates that the
// Websocket transport is wired and the Subscription resolver streams.
func TestTickSubscription(t *testing.T) {
	h, _ := server.NewHandler(server.Config{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/graphql"
	dialer := websocket.Dialer{
		Subprotocols:     []string{"graphql-transport-ws"},
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.DialContext(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	mustWrite(t, conn, `{"type":"connection_init"}`)
	if got := mustReadType(t, conn); got != "connection_ack" {
		t.Fatalf("want connection_ack, got %q", got)
	}

	mustWrite(t, conn, `{"id":"1","type":"subscribe","payload":{"query":"subscription { tick }"}}`)

	for want := 1; want <= 2; want++ {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read tick %d: %v", want, err)
		}
		var msg struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Payload struct {
				Data struct {
					Tick int `json:"tick"`
				} `json:"data"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if msg.Type != "next" || msg.ID != "1" || msg.Payload.Data.Tick != want {
			t.Fatalf("tick %d: got %s", want, raw)
		}
	}

	mustWrite(t, conn, `{"id":"1","type":"complete"}`)
}

// TestResolverErrorIsLogged exercises the error presenter installed in
// NewHandler: a query against an unknown field should produce one JSON
// log line at ERROR level via the configured slog default.
func TestResolverErrorIsLogged(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	var buf bytes.Buffer
	slog.SetDefault(logging.Init(&buf, slog.LevelInfo))

	h, _ := server.NewHandler(server.Config{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	body := strings.NewReader(`{"query":"{ noSuchField }"}`)
	resp, err := http.Post(ts.URL+"/graphql", "application/json", body)
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	resp.Body.Close()

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var found bool
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if entry["level"] == "ERROR" && strings.Contains(entry["msg"].(string), "graphql") {
			if entry["error"] == nil {
				t.Errorf("missing error key: %v", entry)
			}
			found = true
		}
	}
	if !found {
		t.Errorf("expected an ERROR graphql log line, got: %s", buf.String())
	}
}

// TestGracefulShutdownClosesWebsocket verifies that when the http.Server
// begins shutdown, every active subscription receives a graceful WS Close
// frame (CloseNormalClosure / 1000) instead of a TCP reset. This is what
// keeps the Rust host's graphql-ws-client from logging a
// `ResetWithoutClosingHandshake` warning on every app exit.
func TestGracefulShutdownClosesWebsocket(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.NotFoundHandler())
	h, _ := server.NewHandler(server.Config{})
	wrapped, wait := server.AttachGracefulShutdown(ts.Config, h)
	ts.Config.Handler = wrapped
	ts.Start()
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/graphql"
	dialer := websocket.Dialer{
		Subprotocols:     []string{"graphql-transport-ws"},
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.DialContext(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	mustWrite(t, conn, `{"type":"connection_init"}`)
	if got := mustReadType(t, conn); got != "connection_ack" {
		t.Fatalf("want connection_ack, got %q", got)
	}
	mustWrite(t, conn, `{"id":"1","type":"subscribe","payload":{"query":"subscription { tick }"}}`)

	// Wait for one tick so we know the subscription is established.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read first tick: %v", err)
	}

	// Trigger graceful shutdown in the background.
	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr <- ts.Config.Shutdown(ctx)
	}()

	// The next read should eventually surface a graceful Close frame.
	// Subsequent ticks may still arrive before the close lands; loop
	// until we see the close (or fail).
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		_, _, err := conn.ReadMessage()
		if err == nil {
			continue
		}
		var ce *websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("expected CloseError, got %v (%T)", err, err)
		}
		if ce.Code != websocket.CloseNormalClosure {
			t.Errorf("expected NormalClosure (1000), got %d (%s)", ce.Code, ce.Text)
		}
		break
	}

	// Wait must complete promptly — proves hijacked WS handlers actually
	// returned (so srv exit doesn't drop a half-written close frame).
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hijacked WS handler didn't finish after Close frame")
	}

	if err := <-shutdownErr; err != nil {
		t.Errorf("shutdown returned: %v", err)
	}
}

func mustWrite(t *testing.T, c *websocket.Conn, msg string) {
	t.Helper()
	if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatalf("write %s: %v", msg, err)
	}
}

func mustReadType(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return head.Type
}

// The host pushes the always-on engine's bearer token to a dedicated,
// host-only control endpoint (kept off the GraphQL surface). A valid POST
// populates the shared authcreds.Holder; anything malformed leaves it
// untouched so a bad push can't blank a working token.
func TestControlCredentials(t *testing.T) {
	h, creds := server.NewHandler(server.Config{})
	ts := httptest.NewServer(h)
	defer ts.Close()
	url := ts.URL + "/control/credentials"

	// Valid push.
	body := `{"token":"tok-abc","expiresAt":"2030-01-02T03:04:05Z"}`
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if got := creds.Token(); got != "tok-abc" {
		t.Fatalf("holder token = %q, want tok-abc", got)
	}
	if exp := creds.Get().ExpiresAt; exp.IsZero() {
		t.Fatal("expiresAt not parsed into holder")
	}

	// Wrong method: rejected, holder unchanged.
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	got, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	got.Body.Close()
	if got.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", got.StatusCode)
	}

	// Malformed JSON and empty token: rejected, prior token preserved.
	for _, bad := range []string{`{not json`, `{"token":""}`} {
		resp, err := http.Post(url, "application/json", strings.NewReader(bad))
		if err != nil {
			t.Fatalf("POST %q: %v", bad, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST %q status = %d, want 400", bad, resp.StatusCode)
		}
		if got := creds.Token(); got != "tok-abc" {
			t.Fatalf("bad push %q clobbered token: now %q", bad, got)
		}
	}
}
