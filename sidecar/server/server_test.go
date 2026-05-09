package server_test

import (
	"bytes"
	"context"
	"encoding/json"
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
	h := server.NewHandler()
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
	ts := httptest.NewServer(server.NewHandler())
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

	ts := httptest.NewServer(server.NewHandler())
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
