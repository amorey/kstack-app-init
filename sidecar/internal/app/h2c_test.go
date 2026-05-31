package app_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/kubecontextpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/app"
)

// h2cGRPCClient dials the httptest server with insecure credentials — i.e. h2c
// (HTTP/2 prior knowledge, no TLS), the same wire the Rust tonic client uses
// over the UDS — and returns a KubeContextService client.
func h2cGRPCClient(t *testing.T, ts *httptest.Server) kubecontextpb.KubeContextServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(strings.TrimPrefix(ts.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return kubecontextpb.NewKubeContextServiceClient(conn)
}

func serveApp(t *testing.T, a *app.App) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(a)
	t.Cleanup(ts.Close)
	return ts
}

// TestH2C_SSEStillStreams confirms SSE subscriptions keep streaming frames
// incrementally through the h2c wrapper (the HTTP/1.1 ResponseWriter stays
// Flusher-backed). Guards against a buffering regression.
func TestH2C_SSEStillStreams(t *testing.T) {
	a, _ := newTestApp(t)
	ts := serveApp(t, a)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/graphql", strings.NewReader(`{"query":"subscription { tick }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The first non-comment event must arrive well before the stream would
	// complete — proving frames flush incrementally, not buffered to the end.
	sc := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if line := sc.Text(); strings.HasPrefix(line, "event: next") {
			return
		}
		if !sc.Scan() {
			break
		}
	}
	t.Fatal("did not observe an incremental SSE `event: next` frame")
}

// TestH2C_GRPCKubeContext exercises the gRPC surface over h2c on the shared
// listener: Watch yields a snapshot, SetCurrentContext persists, and the
// change is delivered to the live Watch stream.
func TestH2C_GRPCKubeContext(t *testing.T) {
	a, _ := newTestApp(t)
	ts := serveApp(t, a)
	client := h2cGRPCClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Watch(ctx, &kubecontextpb.WatchRequest{})
	require.NoError(t, err)

	snap, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "context-A", snap.GetCurrentContext())
	names := make([]string, 0, len(snap.GetContexts()))
	for _, c := range snap.GetContexts() {
		names = append(names, c.GetName())
	}
	assert.ElementsMatch(t, []string{"context-A", "context-B"}, names)

	_, err = client.SetCurrentContext(ctx, &kubecontextpb.SetCurrentContextRequest{Name: "context-B"})
	require.NoError(t, err)

	next, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "context-B", next.GetCurrentContext())
}

// TestH2C_GRPCSetCurrentContext_Unknown maps an unknown context to a gRPC
// error rather than persisting it.
func TestH2C_GRPCSetCurrentContext_Unknown(t *testing.T) {
	a, _ := newTestApp(t)
	ts := serveApp(t, a)
	client := h2cGRPCClient(t, ts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.SetCurrentContext(ctx, &kubecontextpb.SetCurrentContextRequest{Name: "nope"})
	require.Error(t, err)
}
