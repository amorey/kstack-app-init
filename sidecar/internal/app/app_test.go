package app_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// newTestApp builds an App backed by a kubeconfig with two contexts, then serves
// it over httptest (HTTP/1.1 for GraphQL/SSE, h2c upgrade for gRPC — exactly the
// production split). It does NOT call Start(), so the cluster-cache coordinator
// never runs; the lifecycle surface is what's under test.
func newTestApp(t *testing.T) (*app.App, string) {
	t.Helper()
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`
apiVersion: v1
kind: Config
current-context: context-A
contexts:
- name: context-A
  context: {cluster: c, user: u}
- name: context-B
  context: {cluster: c, user: u}
clusters:
- name: c
  cluster: {server: https://example}
users:
- name: u
  user: {}
`), 0o600))

	a, err := app.New(app.Config{
		KubeconfigPath: kubeconfig,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	return a, kubeconfig
}

// TestAppServesPing is the canary: App is a working http.Handler with the
// GraphQL surface wired through composition.
func TestAppServesPing(t *testing.T) {
	a, _ := newTestApp(t)
	ts := httptest.NewServer(a)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/graphql", "application/json", strings.NewReader(`{"query":"{ ping }"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(raw), `"ping":"pong"`)
}

// With no cloud config (the standalone/test default), the account surface is
// wired through composition but degraded: the authState query answers signed-out
// instead of panicking. Proves the cloud service is threaded into the resolver.
func TestAppAuthStateDegradesSignedOut(t *testing.T) {
	a, _ := newTestApp(t)
	ts := httptest.NewServer(a)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/graphql", "application/json",
		strings.NewReader(`{"query":"{ authState { authenticated identity { sub } } }"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(raw), `"identity":null`)
	assert.Contains(t, string(raw), `"authenticated":false`)
}

// TestAppShutdownDrainsBothTransports is the heart of the lifecycle contract:
// with a live SSE subscription AND a live gRPC Watch stream open, NotifyShutdown
// signals both to close and DrainWithContext returns nil only once both handlers
// have unwound. If either transport were left dangling, the GraphQL WaitGroup or
// the gRPC stream WaitGroup would never reach zero and DrainWithContext would hit
// its deadline instead.
func TestAppShutdownDrainsBothTransports(t *testing.T) {
	a, _ := newTestApp(t)
	ts := httptest.NewServer(a)
	defer ts.Close()

	// Live SSE subscription.
	sseReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/graphql", strings.NewReader(`{"query":"subscription { tick }"}`))
	sseReq.Header.Set("Content-Type", "application/json")
	sseReq.Header.Set("Accept", "text/event-stream")
	sseResp, err := http.DefaultClient.Do(sseReq)
	require.NoError(t, err)
	defer sseResp.Body.Close()
	buf := make([]byte, 1)
	_, err = sseResp.Body.Read(buf) // ensure the stream is established
	require.NoError(t, err)

	// Live gRPC Watch stream over h2c on the same listener.
	conn, err := grpc.NewClient(strings.TrimPrefix(ts.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	stream, err := kubecontextpb.NewKubeContextServiceClient(conn).Watch(context.Background(), &kubecontextpb.WatchRequest{})
	require.NoError(t, err)
	snap, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "context-A", snap.GetCurrentContext())

	grpcRecvErr := make(chan error, 1)
	go func() {
		_, e := stream.Recv()
		grpcRecvErr <- e
	}()

	// The two-line app shutdown surface main.go drives.
	a.NotifyShutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, a.DrainWithContext(ctx))

	// The gRPC stream ended cleanly (OK trailers → EOF), not cut mid-flight.
	select {
	case err := <-grpcRecvErr:
		assert.ErrorIs(t, err, io.EOF)
	case <-time.After(2 * time.Second):
		t.Fatal("gRPC Watch did not drain")
	}
}
