package k8ssync

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clusterregistry"
	"github.com/kubetail-org/kstack-app/sidecar/internal/hub"
)

// probeTimeout bounds the kube-system UID probe for one context. A context
// whose API server doesn't answer in this window is treated as unreachable and
// skipped (re-tried on the next kubeconfig change).
const probeTimeout = 5 * time.Second

// freshnessFlushInterval is how often the in-memory per-cluster "last received
// data" timestamps are flushed to the durable store. Coarse on purpose — the
// value is for a human-facing "synced N ago" label, not precise accounting.
const freshnessFlushInterval = 30 * time.Second

// ClusterView is the merged, UI-facing snapshot of one cluster: durable
// registry fields from the store plus live facts (present in kubeconfig, has a
// cache file, current-context). It's what the GraphQL `clusters` surface maps.
type ClusterView struct {
	UUID                   string // empty for a pending context not yet identified by a probe
	Name                   string
	Context                string
	IsCurrent              bool
	Enabled                bool
	Present                bool // context currently in the kubeconfig
	Cached                 bool // a cache file exists on disk
	CacheBytes             int64
	LastSyncedAt           int64
	LastSeenInKubeconfigAt int64
}

// Manager is the read+control surface the GraphQL resolvers depend on:
// the merged cluster list, the current context, a change stream, and the
// enable/delete mutations. The Coordinator implements it; the resolver holds it
// as an interface so a no-cache sidecar degrades gracefully.
type Manager interface {
	Clusters() []ClusterView
	CurrentContext() string
	Subscribe() (<-chan []ClusterView, func())
	SetEnabled(ctx context.Context, uuid string, enabled bool) (ClusterView, error)
	DeleteCache(ctx context.Context, uuid string) error
	RemoveCluster(ctx context.Context, uuid string) error
}

// configSubscriber is the slice of *k8shelpers.KubeConfigWatcher the Coordinator
// needs: a subscription that emits the current *api.Config first, then every
// change. Kept as an interface so tests can drive a plain watch.Hub.
type configSubscriber interface {
	Subscribe() *watch.Receiver[*api.Config]
}

// Coordinator keeps the cluster cache in lockstep with the user's kubeconfig and
// the durable cluster store. On every kubeconfig snapshot it identifies each
// reachable context by its kube-system UID, records it in the store, and opens a
// cache (+ sync runner) for every cluster that is both present and enabled —
// closing the rest. It also owns the merged ClusterView surface the GraphQL
// `clusters`/`clustersWatch` API reads, and the enable/delete mutations.
//
// reconcile and the mutation handlers are serialized by actionMu so they never
// interleave on the registry/store; the finer-grained mu guards the live maps
// that buildViews reads.
type Coordinator struct {
	cache    *clustercache.Manager
	watcher  configSubscriber
	upstream *Upstream
	registry *clusterregistry.Registry
	viewHub  *hub.Hub[[]ClusterView]

	now func() int64 // injectable clock (unix-millis) for tests

	// identify resolves a context's rest.Config to a stable cluster UUID.
	// Swappable in tests so reconciliation can run without a live cluster.
	identify func(ctx context.Context, cfg *rest.Config) (string, error)

	flushInterval time.Duration // freshness flush cadence (overridable in tests)

	// baseCtx is the coordinator's lifetime context (set by Run). Store-mutation
	// reconciles (SetEnabled/DeleteCache) run against it rather than the caller's
	// request context: a GraphQL client disconnecting mid-mutation must not cancel
	// the probes inside reconcile, which would make every context fail to identify,
	// empty the desired set, and close all currently-open clusters.
	baseCtx context.Context

	// actionMu serializes reconcile + SetEnabled + DeleteCache.
	actionMu sync.Mutex

	mu      sync.RWMutex
	current string                // kubeconfig current-context
	lastCfg *api.Config           // last reconciled config (for store-poke re-reconcile)
	open    map[string]*openEntry // uuid → open cluster (identity + freshness teardown)
	// presence maps each kubeconfig context name to its reconcile state. Presence
	// is name-based (not probe-based) so a transient probe failure can't make a
	// live context look orphaned; the per-name probe result disambiguates a name
	// that now resolves to a different UUID. See recordPresent.
	presence   map[string]ctxPresence // context name → presence/probe state
	lastSeen   map[string]int64       // uuid → last cache-change time (unix-millis)
	backfilled bool                   // orphan scan ran once
}

// openEntry is the bookkeeping for one open cluster: its identity/config plus
// the teardown for its freshness subscription. Keeping both in one map (instead
// of parallel maps keyed by uuid) means they can't drift out of sync.
type openEntry struct {
	cl    Cluster
	unsub func()
}

// ctxPresence is one kubeconfig context name's reconcile state: it's in the
// kubeconfig (so the key exists), and — if its probe succeeded — the UUID it
// resolved to. probed=false means the name is present but unprobed (a transient
// probe failure), which still counts as present.
type ctxPresence struct {
	uid    string
	probed bool
}

// NewCoordinator wires a Coordinator to the registry, kube-config watcher and
// durable store. It builds the sync Upstream and registers it on the registry,
// so Open() starts a per-cluster Reflector sync. Call Run to begin reconciling.
func NewCoordinator(cache *clustercache.Manager, watcher configSubscriber, registry *clusterregistry.Registry) *Coordinator {
	up := NewUpstream(cache)
	cache.SetUpstream(up)
	return &Coordinator{
		cache:    cache,
		watcher:  watcher,
		upstream: up,
		registry: registry,
		viewHub:  hub.New[[]ClusterView](),
		now:      func() int64 { return time.Now().UnixMilli() },
		identify: func(ctx context.Context, cfg *rest.Config) (string, error) {
			pctx, cancel := context.WithTimeout(ctx, probeTimeout)
			defer cancel()
			return probeClusterUID(pctx, cfg)
		},
		flushInterval: freshnessFlushInterval,
		open:          make(map[string]*openEntry),
		presence:      make(map[string]ctxPresence),
		lastSeen:      make(map[string]int64),
	}
}

// Run subscribes to the watcher and reconciles on every kubeconfig snapshot, and
// runs the freshness flush loop, until ctx is cancelled. The watcher emits the
// current config first, so the initial cluster set is opened on the first
// iteration.
func (c *Coordinator) Run(ctx context.Context) {
	c.mu.Lock()
	c.baseCtx = ctx
	c.mu.Unlock()

	go c.freshnessLoop(ctx)

	sub := c.watcher.Subscribe()
	defer sub.Close()
	ch := sub.Chan()
	for {
		select {
		case <-ctx.Done():
			return
		case cfg, ok := <-ch:
			if !ok {
				return
			}
			if cfg == nil {
				continue
			}
			c.reconcile(ctx, cfg)
		}
	}
}

// freshnessLoop periodically flushes in-memory per-cluster freshness timestamps
// to the store, with a final flush when ctx is cancelled.
func (c *Coordinator) freshnessLoop(ctx context.Context) {
	t := time.NewTicker(c.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.flushFreshness()
			return
		case <-t.C:
			c.flushFreshness()
		}
	}
}

// reconcile is the actionMu-guarded entry point; see reconcileLocked.
func (c *Coordinator) reconcile(ctx context.Context, cfg *api.Config) {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	c.reconcileLocked(ctx, cfg)
}

// reconcileLocked brings the open cluster set in line with the kubeconfig
// snapshot and the store's enabled flags. Caller holds actionMu.
//
// Steps: backfill orphaned caches once; identify every reachable context
// (probing unknowns concurrently); upsert each into the store and mark it
// present; open clusters that are present AND enabled, close the rest; publish
// the new merged view.
func (c *Coordinator) reconcileLocked(ctx context.Context, cfg *api.Config) {
	c.mu.Lock()
	c.current = cfg.CurrentContext
	c.lastCfg = cfg
	c.mu.Unlock()

	c.backfillOrphansOnce()

	// Resolve every context to a UUID + rest.Config by probing it. We probe on
	// every reconcile rather than caching context name → UUID: a context name is
	// not a stable identity. Editing a context to point elsewhere, or recreating
	// a kind/minikube cluster under the same name, yields a different cluster
	// (and kube-system UID) behind an unchanged name, and a cached UUID would
	// silently mirror the new cluster into the old one's cache. Probes run
	// concurrently so a kubeconfig with N contexts reconciles in roughly one
	// probe timeout rather than N × timeout.
	type resolved struct {
		name string
		uid  string
		cfg  *rest.Config
	}
	var (
		mu      sync.Mutex
		results []resolved
		wg      sync.WaitGroup
	)
	for name := range cfg.Contexts {
		restCfg, err := clientcmd.NewNonInteractiveClientConfig(*cfg, name, &clientcmd.ConfigOverrides{}, nil).ClientConfig()
		if err != nil {
			slog.Warn("k8ssync: skip context (client config)", "context", name, "err", err)
			continue
		}
		wg.Add(1)
		go func(name string, restCfg *rest.Config) {
			defer wg.Done()
			uid, err := c.identify(ctx, restCfg)
			if err != nil {
				slog.Warn("k8ssync: skip context (probe failed)", "context", name, "err", err)
				return
			}
			mu.Lock()
			results = append(results, resolved{name, uid, restCfg})
			mu.Unlock()
		}(name, restCfg)
	}
	wg.Wait()

	// Order results deterministically before any "first context wins" de-dupe:
	// goroutine completion + map iteration would otherwise make the winning alias
	// (and thus the registry name, isCurrent, and sync credentials) flicker
	// across reconciles when two contexts point at one cluster UUID. Prefer the
	// current-context alias, then sort by name for stability.
	sort.Slice(results, func(i, j int) bool {
		ci := results[i].name == cfg.CurrentContext
		cj := results[j].name == cfg.CurrentContext
		if ci != cj {
			return ci
		}
		return results[i].name < results[j].name
	})

	// Upsert the store. First context wins a given UUID (two contexts can point
	// at one cluster).
	seenUIDs := make(map[string]struct{}, len(results))
	seen := make([]clusterregistry.SeenEntry, 0, len(results))
	for _, r := range results {
		if _, dup := seenUIDs[r.uid]; dup {
			continue
		}
		seenUIDs[r.uid] = struct{}{}
		seen = append(seen, clusterregistry.SeenEntry{UUID: r.uid, Name: r.name})
	}
	if err := c.registry.RecordSeenBatch(seen); err != nil { // one write per reconcile
		slog.Warn("k8ssync: record clusters", "err", err)
	}

	// Kubeconfig presence is derived from the context names in the snapshot, not
	// from probe success: a context that's still in the kubeconfig but
	// temporarily unreachable (or whose kube-system we lack permission to read)
	// must stay present, else its registry row is misreported as an orphan and a
	// DeleteCache during the outage would forget the user's enabled/disabled
	// choice. The probe result (when we got one) disambiguates a name now
	// pointing at a different cluster — see recordPresent.
	presence := make(map[string]ctxPresence, len(cfg.Contexts))
	for name := range cfg.Contexts {
		presence[name] = ctxPresence{} // present but unprobed until proven otherwise
	}
	for _, r := range results {
		presence[r.name] = ctxPresence{uid: r.uid, probed: true}
	}
	c.mu.Lock()
	c.presence = presence
	c.mu.Unlock()

	// Desired = present ∧ enabled (the user's disable toggle freezes a cache).
	want := make(map[string]Cluster, len(results))
	for _, r := range results {
		if _, dup := want[r.uid]; dup {
			continue
		}
		rec, ok, err := c.registry.Get(r.uid)
		if err != nil {
			// Registry read failed (not "absent"). Don't fold this cluster into
			// the desired set on a transient error — but log it rather than
			// silently treating it as disabled.
			slog.Warn("k8ssync: registry read failed during reconcile", "uuid", r.uid, "err", err)
			continue
		}
		if !ok || !rec.Enabled {
			continue
		}
		want[r.uid] = Cluster{UUID: r.uid, Name: r.name, Context: r.name, config: r.cfg, fingerprint: configFingerprint(r.cfg, contextProxyURL(cfg, r.name))}
	}

	c.mu.RLock()
	have := make(map[string]Cluster, len(c.open))
	for uid, oc := range c.open {
		have[uid] = oc.cl
	}
	c.mu.RUnlock()

	for uid, cl := range want {
		existing, isOpen := have[uid]
		switch {
		case !isOpen:
			c.openCluster(ctx, cl)
		case cl.fingerprint != existing.fingerprint:
			// Same cluster UUID, but its kubeconfig entry changed (rotated
			// token, new cert, different server URL). The running reflectors
			// were built from the old rest.Config, so restart sync to pick up
			// the new credentials instead of silently using stale ones.
			slog.Info("k8ssync: cluster config changed, restarting sync", "context", cl.Name, "uuid", uid)
			if err := c.closeCluster(ctx, uid, existing.Name); err != nil {
				// Close failed (e.g. the old sync goroutine didn't stop before
				// the deadline, so the cache left its DB open). Re-opening now
				// would run a second sync runner + DB pool against the same
				// SQLite file. Skip the re-open; the next reconcile retries.
				slog.Warn("k8ssync: skipping sync restart, previous close failed", "uuid", uid, "err", err)
				continue
			}
			c.openCluster(ctx, cl)
		}
	}
	for uid, cl := range have {
		if _, ok := want[uid]; !ok {
			c.closeCluster(ctx, uid, cl.Name)
		}
	}

	c.publish()
}

// backfillOrphansOnce records, the first time it runs, any cluster that has a
// cache file on disk but no store entry — so a cache whose kube-context has gone
// is still visible (and cleanable) rather than silently orphaned.
func (c *Coordinator) backfillOrphansOnce() {
	c.mu.Lock()
	if c.backfilled {
		c.mu.Unlock()
		return
	}
	c.backfilled = true
	c.mu.Unlock()

	uuids, err := c.cache.ScanCachedUUIDs()
	if err != nil {
		slog.Warn("k8ssync: scan cached clusters", "err", err)
		return
	}
	for _, uid := range uuids {
		_, ok, err := c.registry.Get(uid)
		if err != nil {
			slog.Warn("k8ssync: registry read failed during backfill", "uuid", uid, "err", err)
			continue
		}
		if ok {
			continue
		}
		if _, err := c.registry.RecordSeen(uid, ""); err != nil { // "" → name unknown
			slog.Warn("k8ssync: backfill cluster", "uuid", uid, "err", err)
		}
	}
}

// openCluster starts a cluster's sync and freshness tracking. Caller holds
// actionMu; takes mu internally.
func (c *Coordinator) openCluster(ctx context.Context, cl Cluster) {
	c.upstream.SetConfig(cl.UUID, cl.config)
	cdb, err := c.cache.Open(ctx, cl.UUID)
	if err != nil {
		slog.Error("k8ssync: open cluster", "context", cl.Name, "uuid", cl.UUID, "err", err)
		c.upstream.RemoveConfig(cl.UUID)
		return
	}
	notify, unsub := cdb.Subscribe()
	go c.trackFreshness(cl.UUID, notify)
	slog.Info("k8ssync: cluster cache opened", "context", cl.Name, "uuid", cl.UUID)

	c.mu.Lock()
	c.open[cl.UUID] = &openEntry{cl: cl, unsub: unsub}
	c.lastSeen[cl.UUID] = c.now() // optimistic: sync just started
	c.mu.Unlock()
}

// closeCluster stops a cluster's sync and freshness tracking (without deleting
// its cache). Caller holds actionMu.
func (c *Coordinator) closeCluster(ctx context.Context, uuid, name string) error {
	err := c.cache.Close(ctx, uuid)
	if err != nil {
		slog.Warn("k8ssync: close cluster", "uuid", uuid, "err", err)
	}
	c.upstream.RemoveConfig(uuid)
	c.forgetOpen(uuid)
	slog.Info("k8ssync: cluster cache closed", "context", name, "uuid", uuid)
	return err
}

// forgetOpen drops a cluster's in-memory open + freshness bookkeeping and stops
// its freshness goroutine. Idempotent.
func (c *Coordinator) forgetOpen(uuid string) {
	c.mu.Lock()
	if oc := c.open[uuid]; oc != nil {
		oc.unsub() // closes the notify channel → trackFreshness returns
		delete(c.open, uuid)
	}
	delete(c.lastSeen, uuid)
	c.mu.Unlock()
}

// trackFreshness stamps the cluster's last-seen time on every cache change. It
// returns when the cluster's notify channel closes (Close/Shutdown/forgetOpen).
func (c *Coordinator) trackFreshness(uuid string, notify <-chan struct{}) {
	for range notify {
		c.mu.Lock()
		if _, open := c.open[uuid]; open {
			c.lastSeen[uuid] = c.now()
		}
		c.mu.Unlock()
	}
}

// flushFreshness persists in-memory last-seen timestamps to the store (which
// keeps only the ones that advanced) in a single write, then publishes if
// anything changed.
func (c *Coordinator) flushFreshness() {
	c.mu.RLock()
	pending := make(map[string]int64, len(c.lastSeen))
	for uid, ts := range c.lastSeen {
		pending[uid] = ts
	}
	c.mu.RUnlock()
	if len(pending) == 0 {
		return
	}

	changed, err := c.registry.SetLastSyncedAtBatch(pending)
	if err != nil {
		slog.Warn("k8ssync: flush freshness", "err", err)
		return
	}
	if changed {
		c.publish()
	}
}

// SetEnabled flips a cluster's enabled flag, reconciles (opening or freezing it
// per the new gate), and returns the updated view. Errors for an unknown UUID.
func (c *Coordinator) SetEnabled(ctx context.Context, uuid string, enabled bool) (ClusterView, error) {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	_, ok, err := c.registry.SetEnabled(uuid, enabled)
	if err != nil {
		return ClusterView{}, err
	}
	if !ok {
		return ClusterView{}, fmt.Errorf("unknown cluster %q", uuid)
	}
	c.reconcileFromLastCfg()
	return c.viewFor(uuid), nil
}

// DeleteCache removes a cluster's cache files but always keeps its registry row,
// so the cluster stays listed (now uncached). The follow-up effect depends on
// the cluster's state:
//   - present + enabled → reconcile re-discovers it and re-syncs from scratch
//     ("delete = wipe and re-fetch", the chosen semantics);
//   - present + disabled → files are freed and the (disabled) row stays;
//   - absent (orphan) → files are freed and the orphan row stays, so the user can
//     still see it (and remove it outright via RemoveCluster).
//
// Keeping the record preserves the user's enabled/disabled choice and keeps
// clear-cache distinct from "forget this cluster" (see RemoveCluster).
func (c *Coordinator) DeleteCache(ctx context.Context, uuid string) error {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	// Only delete caches for clusters we actually know about. Every deletable
	// cluster — including orphaned caches — is recorded in the registry (backfill
	// records them at startup), so an unknown UUID is either gone already or a
	// malformed value; rejecting it before DeleteCacheFiles keeps a string like
	// "../../foo" from reaching the filesystem layer.
	if _, known, err := c.registry.Get(uuid); err != nil {
		return fmt.Errorf("delete cache %q: %w", uuid, err)
	} else if !known {
		return fmt.Errorf("unknown cluster %q", uuid)
	}
	c.forgetOpen(uuid) // drop our bookkeeping so reconcile re-opens cleanly
	if err := c.cache.DeleteCacheFiles(ctx, uuid); err != nil {
		return err
	}
	c.reconcileFromLastCfg()
	return nil
}

// RemoveCluster forgets a cluster entirely: it deletes any cache files and drops
// the registry row, so the cluster disappears from the list. It's meant for an
// orphan (a context gone from the kubeconfig) — a still-present context would be
// re-discovered and re-added by the next reconcile. Errors for an unknown uuid;
// rejecting it before DeleteCacheFiles also keeps a traversal string off the
// filesystem layer.
func (c *Coordinator) RemoveCluster(ctx context.Context, uuid string) error {
	c.actionMu.Lock()
	defer c.actionMu.Unlock()
	if _, known, err := c.registry.Get(uuid); err != nil {
		return fmt.Errorf("remove cluster %q: %w", uuid, err)
	} else if !known {
		return fmt.Errorf("unknown cluster %q", uuid)
	}
	c.forgetOpen(uuid)
	if err := c.cache.DeleteCacheFiles(ctx, uuid); err != nil {
		return err
	}
	if err := c.registry.Delete(uuid); err != nil {
		return err
	}
	c.reconcileFromLastCfg()
	return nil
}

// reconcileFromLastCfg re-runs reconciliation against the most recent kubeconfig
// (so a store mutation takes effect without waiting for a kubeconfig event), or
// just publishes if no config has been seen yet. Caller holds actionMu.
//
// It deliberately reconciles against the coordinator's lifetime context, not the
// mutating caller's request context: a client disconnecting mid-mutation would
// otherwise cancel the in-reconcile probes, drop every context from the desired
// set, and close all open clusters.
func (c *Coordinator) reconcileFromLastCfg() {
	c.mu.RLock()
	cfg := c.lastCfg
	ctx := c.baseCtx
	c.mu.RUnlock()
	if ctx == nil {
		ctx = context.Background() // mutation before Run; should not happen in practice
	}
	if cfg != nil {
		c.reconcileLocked(ctx, cfg)
		return
	}
	c.publish()
}

// Clusters returns the merged view of every known cluster. Implements
// Manager.
func (c *Coordinator) Clusters() []ClusterView { return c.buildViews() }

// CurrentContext returns the kubeconfig's current-context. Implements
// Manager.
func (c *Coordinator) CurrentContext() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

// Subscribe streams the merged view on every change. Implements Manager.
func (c *Coordinator) Subscribe() (<-chan []ClusterView, func()) { return c.viewHub.Subscribe() }

// publish recomputes and fans out the current merged view.
func (c *Coordinator) publish() { c.viewHub.Publish(c.buildViews()) }

// buildViews assembles the merged cluster list from the durable store plus live
// facts (present in kubeconfig, current-context, cache file presence/size). The
// store already returns records sorted by name.
func (c *Coordinator) buildViews() []ClusterView {
	records := c.registry.List()
	// Snapshot the live facts under the lock, then assemble outside it (the
	// per-cluster CacheBytes does disk I/O we don't want to hold the lock for).
	c.mu.RLock()
	current := c.current
	presence := make(map[string]ctxPresence, len(c.presence))
	for name, p := range c.presence {
		presence[name] = p
	}
	c.mu.RUnlock()

	out := make([]ClusterView, 0, len(records))
	have := make(map[string]struct{}, len(records))
	for _, r := range records {
		have[r.Name] = struct{}{}
		out = append(out, c.viewFromRecord(r, current, recordPresent(presence, r)))
	}

	// Surface kubeconfig contexts we've never identified — a context whose
	// kube-system UID probe has never succeeded (e.g. a stopped minikube) has no
	// registry record, so it would otherwise be invisible. Show it as a pending
	// Active row: present in the kubeconfig, but with no stable UUID, nothing
	// cached, and nothing to sync until a probe succeeds. Skip a name that already
	// has a record (a previously-probed context, now unreachable, keeps its row)
	// and a probed alias (it's shown via the registry record it resolved to).
	pending := make([]ClusterView, 0)
	for name, p := range presence {
		if p.probed {
			continue
		}
		if _, ok := have[name]; ok {
			continue
		}
		pending = append(pending, ClusterView{
			Name:      name,
			Context:   name,
			IsCurrent: isCurrentContext(name, current),
			Present:   true,
		})
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Name < pending[j].Name })
	return append(out, pending...)
}

// viewFor builds the single merged view for one UUID (for mutation returns).
func (c *Coordinator) viewFor(uuid string) ClusterView {
	rec, ok, err := c.registry.Get(uuid)
	if err != nil {
		slog.Warn("k8ssync: registry read failed building cluster view", "uuid", uuid, "err", err)
		return ClusterView{UUID: uuid}
	}
	if !ok {
		return ClusterView{UUID: uuid}
	}
	c.mu.RLock()
	current := c.current
	present := recordPresent(c.presence, rec)
	c.mu.RUnlock()
	return c.viewFromRecord(rec, current, present)
}

// recordPresent reports whether a registry record's context is still in the
// kubeconfig. Presence is name-based so a transient probe failure doesn't make a
// live context look orphaned. When the name did probe successfully this
// reconcile, only the record whose UUID matches stays present — that's what
// distinguishes a context re-pointed at a different cluster (recreated under the
// same name) from one that's merely unreachable.
func recordPresent(presence map[string]ctxPresence, r clusterregistry.Record) bool {
	p, inCfg := presence[r.Name]
	return inCfg && (!p.probed || p.uid == r.UUID)
}

// isCurrentContext reports whether a context name is the kubeconfig's
// current-context, treating an unset current ("") as "nothing is current".
func isCurrentContext(name, current string) bool {
	return current != "" && name == current
}

func (c *Coordinator) viewFromRecord(r clusterregistry.Record, current string, isPresent bool) ClusterView {
	bytes, cached := c.cache.CacheBytes(r.UUID)
	return ClusterView{
		UUID:                   r.UUID,
		Name:                   r.Name,
		Context:                r.Name,
		IsCurrent:              isCurrentContext(r.Name, current),
		Enabled:                r.Enabled,
		Present:                isPresent,
		Cached:                 cached,
		CacheBytes:             bytes,
		LastSyncedAt:           r.LastSyncedAt,
		LastSeenInKubeconfigAt: r.LastSeenInKubeconfigAt,
	}
}
