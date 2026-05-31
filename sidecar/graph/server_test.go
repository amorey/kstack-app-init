package graph_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
)

// TestPingQuery is the canary: a fresh server should answer `{ ping }` with "pong".
// If this passes, the gqlgen wiring (schema -> resolver -> handler) is intact.
func TestPingQuery(t *testing.T) {
	h := graph.NewServer(&graph.Resolver{})
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

// TestTickSubscription opens an SSE subscription to `tick` and asserts the
// first two values are 1 and 2. Validates that the SSE transport is wired and
// the Subscription resolver streams `event: next` frames.
func TestTickSubscription(t *testing.T) {
	h := graph.NewServer(&graph.Resolver{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	resp := openSSESubscription(t, ts.URL, "", "subscription { tick }")
	defer resp.Body.Close() // ends the subscription; must run before ts.Close()
	events := sseEvents(resp)

	for want := 1; want <= 2; want++ {
		ev := nextSSE(t, events)
		if ev.event != "next" {
			t.Fatalf("tick %d: want event=next, got %q (data=%s)", want, ev.event, ev.data)
		}
		var msg struct {
			Data struct {
				Tick int `json:"tick"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(ev.data), &msg); err != nil {
			t.Fatalf("decode %s: %v", ev.data, err)
		}
		if msg.Data.Tick != want {
			t.Fatalf("tick %d: got %d (data=%s)", want, msg.Data.Tick, ev.data)
		}
	}
}

// TestResolverErrorIsLogged exercises the error presenter installed in
// NewServer: a query against an unknown field should produce one JSON
// log line at ERROR level via the configured slog default.
func TestResolverErrorIsLogged(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	var buf bytes.Buffer
	slog.SetDefault(logging.Init(&buf, slog.LevelInfo))

	h := graph.NewServer(&graph.Resolver{})
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

// TestGracefulShutdownEndsSSEStream verifies that NotifyShutdown tells every
// active SSE subscription to flush its terminal `event: complete` (via the
// per-request context wired to the server's shutdownCh) and the handler
// returns — instead of the stream being cut mid-frame when the process exits.
// DrainWithContext then returns promptly because the streaming handler has
// fully unwound. This is the GraphQL half of the app's NotifyShutdown /
// DrainWithContext shutdown surface.
func TestGracefulShutdownEndsSSEStream(t *testing.T) {
	srv := graph.NewServer(&graph.Resolver{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp := openSSESubscription(t, ts.URL, "", "subscription { tick }")
	defer resp.Body.Close() // belt-and-suspenders; shutdown ends the stream
	events := sseEvents(resp)

	// Wait for one tick so we know the subscription is established.
	if ev := nextSSE(t, events); ev.event != "next" {
		t.Fatalf("want first event=next, got %q", ev.event)
	}

	// Signal shutdown directly (the http.Server fires this from
	// RegisterOnShutdown in production; here we drive it without binding the
	// real shutdown sequence).
	srv.NotifyShutdown()

	// The stream must terminate with a `complete` event (more ticks may
	// still arrive first), proving the handler flushed its terminal frame
	// on the shutdown-cancelled context rather than being cut mid-stream.
	for {
		ev := nextSSE(t, events)
		if ev.event == "complete" {
			break
		}
		if ev.event != "next" {
			t.Fatalf("unexpected event before complete: %q", ev.event)
		}
	}

	// DrainWithContext must return promptly — proves the streaming handler
	// actually returned (so process exit doesn't drop a half-written frame).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.DrainWithContext(ctx); err != nil {
		t.Errorf("DrainWithContext returned: %v", err)
	}
}

// sseEvent is one parsed Server-Sent Event: gqlgen emits `event: next` with a
// JSON `data:` line per subscription value, and a final `event: complete`.
type sseEvent struct {
	event string
	data  string
}

// openSSESubscription POSTs a subscription to srvURL's /graphql endpoint with
// `Accept: text/event-stream` and returns the live streaming response. The
// caller must `defer resp.Body.Close()` — that cancels the subscription, and
// it must run before the test server's Close() (which blocks on outstanding
// requests), so close the body via defer rather than t.Cleanup. A non-empty
// token is sent as the `Authorization` bearer header — the single auth path
// shared with queries and mutations.
func openSSESubscription(t *testing.T, srvURL, token, query string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"query":%q}`, query)
	req, err := http.NewRequest(http.MethodPost, srvURL+"/graphql", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new SSE request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /graphql (SSE): %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200", resp.StatusCode)
	}
	return resp
}

// sseEvents parses resp.Body into a stream of events, skipping comment-only
// frames (gqlgen's leading `:` and its keep-alive `: ping` lines). The channel
// closes when the body ends.
func sseEvents(resp *http.Response) <-chan sseEvent {
	ch := make(chan sseEvent)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(resp.Body)
		var ev sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if ev.event != "" || ev.data != "" {
					ch <- ev
					ev = sseEvent{}
				}
			case strings.HasPrefix(line, "event:"):
				ev.event = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "data:"):
				ev.data = strings.TrimSpace(line[len("data:"):])
				// `:` comment / keep-alive lines fall through and are ignored.
			}
		}
	}()
	return ch
}

// nextSSE pulls the next event with a deadline so a stalled stream fails the
// test instead of hanging.
func nextSSE(t *testing.T, ch <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("SSE stream closed before next event")
		}
		return ev
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SSE event")
		return sseEvent{}
	}
}
