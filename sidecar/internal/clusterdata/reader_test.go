package clusterdata

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
)

// seedPod writes a minimal pod row straight into the universal objects table,
// standing in for what the k8ssync upstream would write. Only the columns the
// reader selects need to be meaningful.
func seedPod(ctx context.Context, t *testing.T, cdb *clustercache.ClusterDB, ns, name, status, host string) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx, `
		INSERT INTO objects (uid, api_version, kind, namespace, name,
			resource_version, created_at, updated_at, status_summary, host, raw_json)
		VALUES (?, 'v1', 'Pod', ?, ?, '1', 0, 0, ?, ?, '{}')`,
		ns+"/"+name, ns, name, status, host)
	require.NoError(t, err)
}

func TestReaderPodsSnapshot(t *testing.T) {
	ctx := context.Background()
	cache := clustercache.NewManager(t.TempDir(), nil)
	t.Cleanup(func() { _ = cache.Shutdown(ctx) })

	cdb, err := cache.Open(ctx, "cluster-a")
	require.NoError(t, err)
	seedPod(ctx, t, cdb, "kube-system", "coredns", "Running", "node-1")
	seedPod(ctx, t, cdb, "default", "web", "CrashLoopBackOff", "node-2")

	r := NewReader(cache)
	pods, err := r.Pods(ctx, "cluster-a")
	require.NoError(t, err)
	require.Len(t, pods, 2)
	// Ordered by namespace then name: default/web before kube-system/coredns.
	require.Equal(t, "web", pods[0].Name)
	require.Equal(t, "CrashLoopBackOff", pods[0].Phase)
	require.Equal(t, "node-2", pods[0].NodeName)
	require.Equal(t, "cluster-a", pods[0].ClusterUUID)
	require.Equal(t, "coredns", pods[1].Name)
}

func TestReaderUnopenedClusterIsEmpty(t *testing.T) {
	ctx := context.Background()
	cache := clustercache.NewManager(t.TempDir(), nil)
	t.Cleanup(func() { _ = cache.Shutdown(ctx) })

	// A cluster the registry has not opened (disabled / absent / unknown) must
	// read as empty — and crucially must NOT open it (no stray cache, no sync).
	r := NewReader(cache)
	pods, err := r.Pods(ctx, "never-opened")
	require.NoError(t, err)
	require.Empty(t, pods)
	require.Nil(t, cache.Lookup("never-opened"), "reading must not open the cluster")

	ch, err := r.WatchPods(ctx, "never-opened")
	require.NoError(t, err)
	select {
	case _, ok := <-ch:
		require.False(t, ok, "watch of an unopened cluster is an immediately-closed channel")
	case <-time.After(time.Second):
		t.Fatal("watch channel was not closed")
	}
}

func TestReaderNilRegistryDegrades(t *testing.T) {
	r := NewReader(nil)
	pods, err := r.Pods(context.Background(), "cluster-a")
	require.NoError(t, err)
	require.Empty(t, pods)

	// Watches degrade to an already-closed channel (no error), matching the
	// snapshot queries.
	ch, err := r.WatchPods(context.Background(), "cluster-a")
	require.NoError(t, err)
	select {
	case _, ok := <-ch:
		require.False(t, ok, "expected an already-closed channel")
	case <-time.After(time.Second):
		t.Fatal("nil-registry watch channel was not closed")
	}
}

func TestReaderWatchPodsEmitsOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := clustercache.NewManager(t.TempDir(), nil)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })

	cdb, err := cache.Open(ctx, "cluster-a")
	require.NoError(t, err)

	r := NewReader(cache)
	ch, err := r.WatchPods(ctx, "cluster-a")
	require.NoError(t, err)

	// Initial snapshot: empty.
	require.Empty(t, recvPods(t, ch))

	// A write + Notify wakes the watcher with the fresh snapshot.
	seedPod(ctx, t, cdb, "default", "web", "Running", "node-1")
	cdb.Notify()
	got := recvPods(t, ch)
	require.Len(t, got, 1)
	require.Equal(t, "web", got[0].Name)
}

func recvPods(t *testing.T, ch <-chan []Pod) []Pod {
	t.Helper()
	select {
	case snap, ok := <-ch:
		require.True(t, ok, "watch channel closed early")
		return snap
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch snapshot")
		return nil
	}
}
