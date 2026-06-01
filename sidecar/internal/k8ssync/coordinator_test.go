package k8ssync

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/appdb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clusterregistry"
)

// fakeWatcher adapts a watch.Hub to the configSubscriber interface so the
// coordinator can be driven with hand-published kubeconfigs.
type fakeWatcher struct {
	hub *watch.Hub[*api.Config]
	tx  *watch.Sender[*api.Config]
}

func newFakeWatcher(initial *api.Config) *fakeWatcher {
	h := watch.New(initial)
	return &fakeWatcher{hub: h, tx: h.Sender()}
}

func (f *fakeWatcher) Subscribe() *watch.Receiver[*api.Config] { return f.hub.Receiver() }
func (f *fakeWatcher) publish(cfg *api.Config)                 { _ = f.tx.Send(cfg) }

// kubeconfig builds an *api.Config with the given context names (each pointing
// at a throwaway cluster/authinfo) and current-context.
func kubeconfig(current string, contexts ...string) *api.Config {
	cfg := api.NewConfig()
	cfg.CurrentContext = current
	for _, name := range contexts {
		cfg.Clusters[name] = &api.Cluster{Server: "https://" + name + ".example:6443"}
		cfg.AuthInfos[name] = &api.AuthInfo{Token: "t"}
		cfg.Contexts[name] = &api.Context{Cluster: name, AuthInfo: name}
	}
	return cfg
}

// kubeconfigToken is kubeconfig with an explicit bearer token on every context,
// so a test can change credentials while keeping the same server (same UUID).
func kubeconfigToken(current, name, token string) *api.Config {
	cfg := api.NewConfig()
	cfg.CurrentContext = current
	cfg.Clusters[name] = &api.Cluster{Server: "https://" + name + ".example:6443"}
	cfg.AuthInfos[name] = &api.AuthInfo{Token: token}
	cfg.Contexts[name] = &api.Context{Cluster: name, AuthInfo: name}
	return cfg
}

// uidForHost derives a stable, filesystem-safe pseudo-UUID from a context's
// server host (real UUIDs are clean kube-system namespace UIDs; the test's fake
// hosts contain "/" and ":" which would break the on-disk db path).
func uidForHost(host string) string {
	repl := strings.NewReplacer("/", "-", ":", "-", ".", "-")
	return "uid-" + repl.Replace(host)
}

// uidFor is the UUID the test coordinator assigns to a context name.
func uidFor(name string) string { return uidForHost("https://" + name + ".example:6443") }

// newTestCoordinator builds a coordinator whose identify() maps each context's
// server host to a deterministic, filesystem-safe UUID — no live cluster
// needed. The Upstream's sync runner fails to reach the fake servers and just
// logs; the cache DBs still open, which is all reconciliation cares about.
func newTestCoordinator(t *testing.T, fw *fakeWatcher) (*Coordinator, *clustercache.Manager, *clusterregistry.Registry) {
	t.Helper()
	dir := t.TempDir()
	cache := clustercache.NewManager(dir, nil)
	db, err := appdb.Open(filepath.Join(dir, "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := clusterregistry.New(db.SQL())
	c := NewCoordinator(cache, fw, store)
	c.identify = func(_ context.Context, cfg *rest.Config) (string, error) {
		return uidForHost(cfg.Host), nil
	}
	return c, cache, store
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: %s", msg)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// openUIDs returns the UUIDs the coordinator currently has open (actively
// syncing), sorted.
func openUIDs(c *Coordinator) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.open))
	for uid := range c.open {
		out = append(out, uid)
	}
	sort.Strings(out)
	return out
}

func viewByName(c *Coordinator, name string) (ClusterView, bool) {
	for _, v := range c.Clusters() {
		if v.Name == name {
			return v, true
		}
	}
	return ClusterView{}, false
}

func TestReconcileOpensPresentEnabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a", "b"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })

	go c.Run(ctx)

	waitFor(t, func() bool { return len(openUIDs(c)) == 2 }, "two clusters open")
	require.Equal(t, "a", c.CurrentContext())

	va, ok := viewByName(c, "a")
	require.True(t, ok)
	require.True(t, va.Present && va.Enabled && va.Cached && va.IsCurrent)
	vb, _ := viewByName(c, "b")
	require.False(t, vb.IsCurrent)
}

func TestRemovedContextBecomesOrphan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a", "b"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 2 }, "two open")

	// Remove "b": it closes (no longer syncing) but its record persists as a
	// cached-but-absent orphan — the whole point of the store.
	fw.publish(kubeconfig("a", "a"))
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "b closed")
	require.Equal(t, []string{uidFor("a")}, openUIDs(c))

	require.Len(t, c.Clusters(), 2, "b still listed as an orphan")
	vb, ok := viewByName(c, "b")
	require.True(t, ok)
	require.False(t, vb.Present, "b no longer in kubeconfig")
	require.True(t, vb.Cached, "b's cache file remains")
}

func TestDisableFreezesAndReEnableResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	// Disable: a present cluster is closed (frozen) but stays listed + cached.
	view, err := c.SetEnabled(ctx, uidFor("a"), false)
	require.NoError(t, err)
	require.False(t, view.Enabled)
	require.Empty(t, openUIDs(c), "disabled cluster is not syncing")
	require.True(t, view.Present && view.Cached)

	// Re-enable resumes sync.
	view, err = c.SetEnabled(ctx, uidFor("a"), true)
	require.NoError(t, err)
	require.True(t, view.Enabled)
	require.Equal(t, []string{uidFor("a")}, openUIDs(c))

	_, _, err = c.registry.SetEnabled("nope", true) // sanity: unknown is a no-op at store level
	require.NoError(t, err)
	_, err = c.SetEnabled(ctx, "nope", true)
	require.Error(t, err, "SetEnabled errors for an unknown cluster")
}

func TestDeletePresentDisabledFreesFilesKeepsRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	_, err := c.SetEnabled(ctx, uidFor("a"), false)
	require.NoError(t, err)

	// Deleting a present-but-disabled cluster frees its files but keeps the
	// (still disabled) row — it stays gone (not cached, not syncing) and does
	// NOT silently re-enable.
	require.NoError(t, c.DeleteCache(ctx, uidFor("a")))
	_, cached := cache.CacheBytes(uidFor("a"))
	require.False(t, cached, "cache files removed")
	require.Empty(t, openUIDs(c), "stays frozen")
	v, ok := viewByName(c, "a")
	require.True(t, ok, "row remains for the still-present cluster")
	require.False(t, v.Enabled, "still disabled — not re-enabled by delete")
	require.False(t, v.Cached)
}

func TestDeleteOrphanForgetsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a", "b"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 2 }, "two open")

	// Remove "b" → orphan (absent, cached). Deleting it forgets the row.
	fw.publish(kubeconfig("a", "a"))
	waitFor(t, func() bool { _, ok := viewByName(c, "b"); return ok && len(openUIDs(c)) == 1 }, "b orphaned")

	require.NoError(t, c.DeleteCache(ctx, uidFor("b")))
	_, ok := viewByName(c, "b")
	require.False(t, ok, "orphan fully forgotten")
	require.Len(t, c.Clusters(), 1)
}

func TestDeletePresentClusterReSyncs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	// Deleting a present+enabled cluster wipes it, then reconcile re-discovers
	// and re-opens it (the chosen "allow — it re-syncs" semantics).
	require.NoError(t, c.DeleteCache(ctx, uidFor("a")))
	require.Equal(t, []string{uidFor("a")}, openUIDs(c), "re-opened after delete")
	require.Len(t, c.Clusters(), 1)
}

func TestBackfillSurfacesOrphanedCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	// Pre-seed a stray cache file with no store entry, as if a cluster was
	// removed from kubeconfig in a previous run.
	seed := clustercache.NewManager(dir, nil)
	_, err := seed.Open(context.Background(), "ghost")
	require.NoError(t, err)
	require.NoError(t, seed.Shutdown(context.Background()))

	cache := clustercache.NewManager(dir, nil)
	db, err := appdb.Open(filepath.Join(dir, "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := clusterregistry.New(db.SQL())
	fw := newFakeWatcher(kubeconfig("")) // empty kubeconfig
	c := NewCoordinator(cache, fw, store)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })

	go c.Run(ctx)

	waitFor(t, func() bool { return len(c.Clusters()) == 1 }, "orphan backfilled")
	v := c.Clusters()[0]
	require.Equal(t, "ghost", v.UUID)
	require.False(t, v.Present)
	require.True(t, v.Cached)
}

func TestFreshnessFlushPersistsLastSynced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, store := newTestCoordinator(t, fw)
	c.flushInterval = time.Hour // drive flushes manually
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	// Opening optimistically stamps freshness; flushing persists it.
	c.flushFreshness()
	first, _, err := store.Get(uidFor("a"))
	require.NoError(t, err)
	require.Greater(t, first.LastSyncedAt, int64(0), "open stamps an initial last-synced")

	// A cache change (Notify) advances freshness.
	cache.Lookup(uidFor("a")).Notify()
	waitFor(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.lastSeen[uidFor("a")] > first.LastSyncedAt
	}, "notify advances in-memory freshness")
	c.flushFreshness()
	second, _, err := store.Get(uidFor("a"))
	require.NoError(t, err)
	require.Greater(t, second.LastSyncedAt, first.LastSyncedAt)
}

// A context still in the kubeconfig whose UID probe starts failing (API server
// unreachable, or no permission to read kube-system) must stay Present and keep
// its registry row: kubeconfig presence is name-based, not probe-based. It must
// not flip to "Orphaned", and DeleteCache during the outage must not forget the
// row (which would drop the user's enabled/disabled preference).
func TestProbeOutageKeepsContextPresent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, store := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })

	var fail atomic.Bool
	base := c.identify
	c.identify = func(ctx context.Context, cfg *rest.Config) (string, error) {
		if fail.Load() {
			return "", fmt.Errorf("unreachable")
		}
		return base(ctx, cfg)
	}

	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	// Outage: still in the kubeconfig, but the probe now fails. The cluster
	// closes (we can't sync an unreachable API), but it stays present.
	fail.Store(true)
	fw.publish(kubeconfig("a", "a"))
	waitFor(t, func() bool { return len(openUIDs(c)) == 0 }, "a closed during outage")

	va, ok := viewByName(c, "a")
	require.True(t, ok, "row preserved during outage")
	require.True(t, va.Present, "still present in kubeconfig despite probe failure")

	require.NoError(t, c.DeleteCache(ctx, uidFor("a")))
	_, ok, err := store.Get(uidFor("a"))
	require.NoError(t, err)
	require.True(t, ok, "present context's row survives DeleteCache during outage")
}

// A context whose kubeconfig credentials change but which still resolves to the
// same cluster UUID must have its sync restarted, so the reflectors stop running
// on the stale rest.Config. A restart replaces the cluster's ClusterDB handle.
func TestConfigChangeRestartsSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfigToken("a", "a", "tok-1"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	first := cache.Lookup(uidFor("a"))
	require.NotNil(t, first)

	// Same server (same UUID) with a rotated token → restart.
	fw.publish(kubeconfigToken("a", "a", "tok-2"))
	waitFor(t, func() bool {
		cur := cache.Lookup(uidFor("a"))
		return cur != nil && cur != first
	}, "sync restarted after credential change")

	second := cache.Lookup(uidFor("a"))
	// Re-publishing the identical config must NOT churn the open cluster.
	fw.publish(kubeconfigToken("a", "a", "tok-2"))
	require.Never(t, func() bool { return cache.Lookup(uidFor("a")) != second },
		200*time.Millisecond, 20*time.Millisecond)
}

// When two contexts resolve to one cluster UUID, the winning alias (and thus the
// registry name and isCurrent) must be deterministic — the current context — not
// dependent on probe-goroutine completion order.
func TestDuplicateContextPrefersCurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// "a-primary" sorts before "z-alias", so only current-context preference (not
	// alphabetical order) makes the alias win.
	cfg := api.NewConfig()
	cfg.CurrentContext = "z-alias"
	for _, name := range []string{"a-primary", "z-alias"} {
		cfg.Clusters[name] = &api.Cluster{Server: "https://shared.example:6443"}
		cfg.AuthInfos[name] = &api.AuthInfo{Token: "t"}
		cfg.Contexts[name] = &api.Context{Cluster: name, AuthInfo: name}
	}
	fw := newFakeWatcher(cfg)
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)

	uid := uidForHost("https://shared.example:6443")
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "shared cluster open")

	v, ok := viewByName(c, "z-alias")
	require.True(t, ok, "cluster named after the current context")
	require.True(t, v.IsCurrent)
	require.Equal(t, uid, v.UUID)

	count := 0
	for _, cv := range c.Clusters() {
		if cv.UUID == uid {
			count++
		}
	}
	require.Equal(t, 1, count, "one entry per shared UUID")
}

// DeleteCache must reject a UUID that isn't a known cluster before it reaches the
// filesystem layer, so a traversal string can't delete files outside the cache.
func TestDeleteCacheRejectsUnknownUUID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })
	go c.Run(ctx)
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "a open")

	require.Error(t, c.DeleteCache(ctx, "../../foo"), "path-traversal UUID rejected")
	require.Error(t, c.DeleteCache(ctx, uidFor("does-not-exist")), "unknown UUID rejected")
}

// A probe that errors on first sight keeps the context out until a later
// reconcile re-probes it and succeeds.
func TestProbeFailureSkipsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fw := newFakeWatcher(kubeconfig("a", "a"))
	c, cache, _ := newTestCoordinator(t, fw)
	t.Cleanup(func() { _ = cache.Shutdown(context.Background()) })

	var calls int
	base := c.identify
	c.identify = func(ctx context.Context, cfg *rest.Config) (string, error) {
		calls++
		if calls == 1 {
			return "", fmt.Errorf("unreachable")
		}
		return base(ctx, cfg)
	}

	go c.Run(ctx)
	require.Never(t, func() bool { return len(c.Clusters()) > 0 }, 200*time.Millisecond, 20*time.Millisecond)

	fw.publish(kubeconfig("a", "a"))
	waitFor(t, func() bool { return len(openUIDs(c)) == 1 }, "opens after retry")
}
