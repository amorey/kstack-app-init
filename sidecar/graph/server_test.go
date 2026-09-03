package graph_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// TestAuthStateQuery is the canary: a fresh server (signed-out fake auth)
// should answer `{ authState { authenticated } }` with the signed-out state.
// If this passes, the gqlgen wiring (schema -> resolver -> handler) is intact.
func TestAuthStateQuery(t *testing.T) {
	h := graph.NewServer(&graph.Resolver{Auth: newFakeAuth(auth.Identity{})})
	ts := httptest.NewServer(h)
	defer ts.Close()

	body := strings.NewReader(`{"query":"{ authState { authenticated } }"}`)
	resp, err := http.Post(ts.URL+"/graphql", "application/json", body)
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Data struct {
			AuthState struct {
				Authenticated bool `json:"authenticated"`
			} `json:"authState"`
		} `json:"data"`
	}
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if out.Data.AuthState.Authenticated {
		t.Fatalf("want authenticated=false, got true (raw=%s)", raw)
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

// TestErrorLogOmitsVariables pins the other half of the presenter's rule: the
// operation is logged, its variables never are — they can carry auth tokens or
// PII. The sentinel rides $id because ObjectID is the only variable type the
// query surface takes; what it stands for is any coerced variable value, which
// reaches the request and must not reach the log.
func TestErrorLogOmitsVariables(t *testing.T) {
	// A valid ObjectID no fixture defines. Valid matters: a malformed one is
	// echoed by the scalar's own parse error, which would fail this test for a
	// reason that is not the presenter's.
	const sentinel = "8675309000000001"

	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	var buf bytes.Buffer
	slog.SetDefault(logging.Init(&buf, slog.LevelInfo))

	h := graph.NewServer(&graph.Resolver{ClusterSvc: newFakeClusterService(nil)})
	ts := httptest.NewServer(h)
	defer ts.Close()

	// clusterEnabledSet returns a non-null Cluster, so an unknown id is an error
	// rather than a null — the presenter is guaranteed to run.
	body := fmt.Sprintf(
		`{"query":"mutation($id: ObjectID!, $enabled: Boolean!) { clusterEnabledSet(id: $id, enabled: $enabled) { id } }","variables":{"id":%q,"enabled":true}}`,
		sentinel,
	)
	resp, err := http.Post(ts.URL+"/graphql", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	resp.Body.Close()

	// Without this the absence below proves nothing: a request that never errored
	// logs nothing at all, and the sentinel is missing for the wrong reason.
	if !bytes.Contains(buf.Bytes(), []byte(`"level":"ERROR"`)) {
		t.Fatalf("expected the presenter to log an error, got: %s", buf.String())
	}
	if bytes.Contains(buf.Bytes(), []byte(sentinel)) {
		t.Errorf("variable value reached the log: %s", buf.String())
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
	// authStateWatch is the streaming canary: a fake auth emits the current
	// snapshot on subscribe, then keeps the stream open until shutdown.
	srv := graph.NewServer(&graph.Resolver{Auth: newFakeAuth(auth.Identity{})})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp := openSSESubscription(t, ts.URL, "", "subscription { authStateWatch { authenticated } }")
	defer resp.Body.Close() // belt-and-suspenders; shutdown ends the stream
	events := sseEvents(t, resp)

	// Wait for the first frame so we know the subscription is established.
	if ev := nextSSE(t, events); ev.event != "next" {
		t.Fatalf("want first event=next, got %q", ev.event)
	}

	// Signal shutdown directly (the http.Server fires this from
	// RegisterOnShutdown in production; here we drive it without binding the
	// real shutdown sequence).
	srv.NotifyShutdown()

	// The stream must terminate with a `complete` event, proving the handler
	// flushed its terminal frame on the shutdown-cancelled context rather than
	// being cut mid-stream.
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
// closes when the body ends. A scan error (as opposed to a clean EOF) is
// reported via t.Errorf — goroutine-safe, unlike t.Fatal — so a truncated
// stream surfaces as a real failure rather than a mysterious early close.
func sseEvents(t *testing.T, resp *http.Response) <-chan sseEvent {
	t.Helper()
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
		// The expected teardown is a caller's deferred resp.Body.Close()
		// (cancelling the subscription) racing this mid-read. That surfaces
		// two ways depending on which side wins the race: net.ErrClosed when
		// the underlying conn is torn down, or net/http's unexported
		// errReadOnClosedResBody ("http: read on closed response body") when
		// the client closes the body first. Neither is a real scan failure —
		// only something else is worth a test error.
		if err := sc.Err(); err != nil && !isExpectedStreamClose(err) {
			t.Errorf("SSE scan: %v", err)
		}
	}()
	return ch
}

// isExpectedStreamClose reports whether err is the benign result of the
// subscription being torn down mid-read (deferred resp.Body.Close). It covers
// net.ErrClosed (underlying conn gone) and net/http's unexported
// errReadOnClosedResBody, which has no sentinel to match with errors.Is, so it
// is matched by its stable message.
func isExpectedStreamClose(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "read on closed response body")
}

// nextSSE pulls the next event with a deadline so a stalled stream fails the
// test instead of hanging.
func nextSSE(t *testing.T, ch <-chan sseEvent) sseEvent {
	t.Helper()
	return testutil.Recv(t, ch, "an SSE event")
}
