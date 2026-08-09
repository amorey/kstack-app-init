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

	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/app"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
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
		// A real data dir so app.db lands in the per-test temp dir — with an
		// empty DataDir app.New would create app.db relative to the test's
		// working directory (the package dir).
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })

	return a, kubeconfig
}

// DataDir is required: with an empty one New must error rather than create
// app.db relative to whatever the process working directory happens to be.
func TestAppRequiresDataDir(t *testing.T) {
	_, err := app.New(app.Config{})
	require.ErrorContains(t, err, "data dir")
}

// With no cloud config (the standalone/test default), the account surface is
// wired through composition but degraded: the authState query answers signed-out
// instead of panicking. This is also the canary that the composed App is a
// working http.Handler with the GraphQL surface wired through composition.
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
// with a live SSE subscription AND a live gRPC AuthStateWatch stream open,
// NotifyShutdown signals both to close and DrainWithContext returns nil only once
// both handlers have unwound. If either transport were left dangling, the GraphQL
// WaitGroup or the gRPC stream WaitGroup would never reach zero and
// DrainWithContext would hit its deadline instead.
func TestAppShutdownDrainsBothTransports(t *testing.T) {
	a, _ := newTestApp(t)
	ts := httptest.NewServer(a)
	defer ts.Close()

	// Live SSE subscription.
	sseReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/graphql", strings.NewReader(`{"query":"subscription { authStateWatch { authenticated } }"}`))
	sseReq.Header.Set("Content-Type", "application/json")
	sseReq.Header.Set("Accept", "text/event-stream")
	sseResp, err := http.DefaultClient.Do(sseReq)
	require.NoError(t, err)
	defer sseResp.Body.Close()
	buf := make([]byte, 1)
	_, err = sseResp.Body.Read(buf) // ensure the stream is established
	require.NoError(t, err)

	// Live gRPC AuthStateWatch stream over h2c on the same listener.
	conn, err := grpc.NewClient(strings.TrimPrefix(ts.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	stream, err := authpb.NewAuthServiceClient(conn).AuthStateWatch(context.Background(), &authpb.AuthStateWatchRequest{})
	require.NoError(t, err)
	snap, err := stream.Recv()
	require.NoError(t, err)
	assert.False(t, snap.GetAuthenticated())

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
	assert.ErrorIs(t, testutil.Recv(t, grpcRecvErr, "the gRPC Watch to drain"), io.EOF)
}
