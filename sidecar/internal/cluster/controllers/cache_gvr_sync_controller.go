// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/eventsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// gvrSyncDrainFinalizer gates a sync object's deletion on its worker having stopped — a
// running worker holds the cache's ClusterDB handle, and GC's cascade marks children for
// deletion without ordering their teardown. This is what makes "the sync children are gone"
// mean "their workers have drained". See docs/adr/2026-08-09-beehive-control-plane.md.
const gvrSyncDrainFinalizer = "kstack.io/gvrsync-drain"

// ClusterCacheGVRSyncController owns the worker mirroring ONE Kubernetes kind into the
// cache's shared objects table (plus its edges and catalog entry).
//
// One worker per discovered kind, so a hundred per cache is normal — Events included, which
// differ only in which store their worker writes to (see newSyncStore). Hence two
// properties: the steady state must write nothing (an unchanged condition is suppressed and
// there is no status), and per-kind isolation means a forbidden CRD reports on its own
// object while every other kind keeps syncing.
//
// The lifecycle machinery lives in cache_sync_workers.go. Specific here: the kind identity
// in the spec, and that a deletion takes the kind's cached rows with it.
type ClusterCacheGVRSyncController struct {
	// The extra hop in this kind's owner climb — it hangs off the discovery anchor, not the
	// cache (this object → anchor → cache → cluster).
	discoveryClient beehive.Client[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]
	// Reads the parent ClusterCache's owner edge (the last hop).
	cacheClient  beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus]
	connMgr      *connections.Manager
	cacheManager *store.Manager
	// Resync bus; nil disables poke-driven restarts (tests drive restartWorkers directly).
	pokeSvc *poke.Service
	pokeSub *pokeSubscription

	// Status-write client for the worker sinks, which report off the reconcile path.
	// Injected before the control plane starts.
	ctrlClient beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus]

	// Swapped for a fake in tests so lifecycle and report folding run without a cluster.
	newWorker newGVRWorkerFunc

	workers *workerSet

	// Serializes condition/stats read-modify-writes between the reconcile path and sinks.
	writeMu sync.Mutex
	// Each cache's shared client budget (rate limiter + LIST semaphore). Shared with the
	// discovery controller, since both talk to one cluster on one cache's behalf.
	policies *cacheClientPolicies

	// Per-kind freshness stamps, guarded by writeMu. In memory, not status — nothing in the
	// object graph reacts; see docs/adr/2026-08-09-status-propagation-gauges.md.
	stats map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats
}

// newGVRWorkerFunc constructs one kind's sync worker over an open cache handle.
type newGVRWorkerFunc func(ctx context.Context, cfg *rest.Config, cdb *store.ClusterDB, kind objectsync.Kind, limiter kubesync.ListLimiter, sink kubesync.Sink) (workerHandle, error)

// newLiveGVRWorker is the production constructor, and the one place that knows a kind can
// want a different store; everything else treats Events like any other kind.
//
// The jitter seed carries the kind as well as the cache, so a cache's hundred workers spread
// their re-lists across the interval instead of re-listing the cluster at one instant.
func newLiveGVRWorker(ctx context.Context, cfg *rest.Config, cdb *store.ClusterDB, kind objectsync.Kind, limiter kubesync.ListLimiter, sink kubesync.Sink) (workerHandle, error) {
	gvr, err := kind.GVR()
	if err != nil {
		return nil, err
	}
	src, err := kubesync.NewDynamicSource(cfg, gvr)
	if err != nil {
		return nil, err
	}
	st, err := newSyncStore(cdb, kind)
	if err != nil {
		return nil, err
	}
	// Before the first row lands: the catalog entry is what makes the kind readable at all
	// (store.Objects resolves the plural through it) and what puts it in the dashboard nav.
	if err := st.EnsureCatalog(ctx); err != nil {
		return nil, err
	}
	opts := []kubesync.Option{kubesync.WithListLimiter(limiter)}
	// Events are served without the watch cache, so their watches carry no bookmarks and a
	// quiet cluster's Event stream legitimately goes silent for hours.
	if isEventsKind(kind) {
		opts = append(opts, kubesync.WithoutBookmarks())
	}
	return kubesync.New(src, st, sink, cdb.ID()+"/"+kind.APIVersion+"/"+kind.Resource, opts...)
}

// newSyncStore picks where one kind's rows land. Events get their own table — see
// eventsync.NewStore for why — and everything else the shared objects table.
func newSyncStore(cdb *store.ClusterDB, kind objectsync.Kind) (kubesync.Store, error) {
	if isEventsKind(kind) {
		return eventsync.NewStore(cdb)
	}
	return objectsync.NewStore(cdb, kind)
}

// forgetKindRows drops one kind's cached rows, edges, catalog entry and resume cookie — the
// reap counterpart of newSyncStore, beside it because which store a kind writes to and where
// its rows are reaped from are one decision. Events are exempt: their rows live in their own
// table and the collection is always served, so only the cache file going away ends them.
// Routing it here keeps the three callers free of per-kind knowledge.
func forgetKindRows(ctx context.Context, cdb *store.ClusterDB, kind objectsync.Kind) error {
	if isEventsKind(kind) {
		return nil
	}
	return objectsync.Forget(ctx, cdb, kind)
}

// isEventsKind reports whether a kind is the cluster's Event collection. Keyed on api group
// and plural, NOT the Kind name: "Event" is not reserved, so a CRD named Event in somebody's
// own group would otherwise be routed into the events table and exempted from the sweep.
func isEventsKind(kind objectsync.Kind) bool {
	return kind.APIVersion == domain.EventsAPIVersion && kind.Resource == domain.EventsResource
}

// isAltEventsKind reports whether an api group-version and plural name the NON-canonical
// spelling of the collection isEventsKind names — the same underlying store, served a second
// time under its own group. It is the discovery filter's drop test, keyed on the same pair
// isEventsKind is (and for the same reason: a CRD named Event in somebody's own group is that
// user's kind, not this collection, so it must not be filtered out of the sync set).
func isAltEventsKind(apiVersion, resource string) bool {
	group, _, _ := strings.Cut(apiVersion, "/")
	return group == domain.EventsAltGroup && resource == domain.EventsResource
}

// NewClusterCacheGVRSyncController builds the controller from the shared runtime.
func NewClusterCacheGVRSyncController(rt *controllerRuntime) *ClusterCacheGVRSyncController {
	return &ClusterCacheGVRSyncController{
		discoveryClient: beehive.NewClient[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus](
			rt.bh, domain.ClusterCacheGVRDiscoveryGroupKind),
		cacheClient:  beehive.NewClient[domain.ClusterCacheSpec, domain.ClusterCacheStatus](rt.bh, domain.ClusterCacheGroupKind),
		connMgr:      rt.connMgr,
		cacheManager: rt.cacheManager,
		pokeSvc:      rt.pokeSvc,
		newWorker:    newLiveGVRWorker,
		workers:      newWorkerSet(),
		stats:        make(map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats),
		policies:     rt.policies(),
	}
}

// SetControllerClient injects the status-write client from beehive.Register. Call once,
// before the control plane starts.
func (c *ClusterCacheGVRSyncController) SetControllerClient(cl beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus]) {
	c.ctrlClient = cl
}

// Stats returns one kind's freshness stamps, or ok=false when its worker has reported
// nothing in this process yet. Read on request by the service; never stored.
func (c *ClusterCacheGVRSyncController) Stats(objID beehive.ObjectID) (domain.ClusterCacheGVRSyncStats, bool) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	st, ok := c.stats[domain.ClusterCacheGVRSyncID(objID)]
	return st, ok
}

// StatsSnapshot copies every kind's stamps under ONE lock: the rollup reads a whole cache at
// a time, and per-record Stats() calls would take writeMu a hundred times per fold —
// contending hardest during a cold start, when the condition writers are busiest.
func (c *ClusterCacheGVRSyncController) StatsSnapshot() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	out := make(map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats, len(c.stats))
	for id, st := range c.stats {
		out[id] = st
	}
	return out
}

// forgetStats drops a collected object's stamps, so the map can't outlive the objects it
// describes.
func (c *ClusterCacheGVRSyncController) forgetStats(objID beehive.ObjectID) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	delete(c.stats, domain.ClusterCacheGVRSyncID(objID))
}

// StartPoke subscribes to the resync bus. Call after the control plane has started; pair
// with StopPoke.
func (c *ClusterCacheGVRSyncController) StartPoke() {
	c.pokeSub = startPokeSubscription(c.pokeSvc, c.restartWorkers)
}

// StopPoke halts the poke subscription and joins its goroutine. Call before draining the
// reconcile loops so a resync restart can't race teardown.
func (c *ClusterCacheGVRSyncController) StopPoke() {
	c.pokeSub.stop()
}

// restartWorkers rebuilds every running worker in place — the resync poke's handler. A poke
// means every long-lived watch is probably on a dead connection, and the point is not to
// wait out the client's own detection. Each worker resumes from its persisted
// resourceVersion, so this is cheap. See docs/adr/2026-08-09-poke-resync-fanout.md.
//
// Only touches what is RUNNING: starting a worker for a paused kind would be this path
// deciding what is the reconcile's to decide.
func (c *ClusterCacheGVRSyncController) restartWorkers(ctx context.Context) {
	for _, entry := range c.workers.entries() {
		c.restartWorker(ctx, entry)
	}
}

// restartWorker bounces one entry, holding its lifecycle lock across the WHOLE
// stop-then-start so a reconcile can't act on the gap — a pause or deletion landing there
// would report itself done (clearing the drain finalizer holding the cache file open) while
// this pass puts a worker back for an object that is paused or gone.
func (c *ClusterCacheGVRSyncController) restartWorker(ctx context.Context, entry *syncEntry) {
	mu := c.workers.lifecycleLock(entry.objID)
	mu.Lock()
	defer mu.Unlock()

	// Re-check under the lock: resurrecting an entry sampled before it is the same bug.
	if !c.workers.isCurrent(entry) {
		return
	}
	if err := c.workers.stopBounded(ctx, entry.objID); err != nil {
		slog.Warn("gvrsynccontroller: stop worker for resync", "object", entry.objID, "err", err)
		return // it stays in the set, so the next poke or reconcile retries
	}
	c.startEntry(ctx, entry)
}

// startEntry rebuilds a drained entry's worker, RE-RESOLVING the cache handle rather than
// reusing entry.cdb: that handle may have been closed since (clearing a cache shuts it with
// the file), and a worker on a closed database fails everything while staying registered —
// worse than not restarting, since being registered is what stops a reconcile replacing it.
// Open is idempotent, so the ordinary poke path gets the same handle back.
//
// The caller holds the object's lifecycle lock.
func (c *ClusterCacheGVRSyncController) startEntry(ctx context.Context, entry *syncEntry) {
	cdb, err := c.cacheManager.Open(ctx, entry.ref)
	if err != nil {
		slog.Warn("gvrsynccontroller: reopen cache for restart", "object", entry.objID, "err", err)
		return // nothing registered, so the next reconcile builds one
	}
	if err := c.startWorker(ctx, entry.objID, entry.gen, entry.fingerprint, entry.cfg, cdb, entry.kind, entry.ref); err != nil {
		slog.Warn("gvrsynccontroller: restart worker", "object", entry.objID, "err", err)
	}
}

// RestartCacheWorkers drains every running worker of ONE cache, runs between, and restarts
// them on a freshly opened handle. It is what makes "clear cache" work — a reconcile leaves
// a running worker alone while its connection and kind are unchanged, so nothing else would
// rebuild them.
//
// The ORDER is the point, and why the caller's teardown runs INSIDE this: drain first, so
// the file isn't removed under a live writer; then between; then rebuild against the new
// handle the Manager hands back. Every affected object's lifecycle lock is held across the
// whole sequence, so no reconcile can start a worker on the doomed handle.
//
// A worker that will NOT drain aborts the sequence — running between with a live writer is
// exactly what the drain prevents. The drained workers restart anyway and the error is
// returned, so the caller reports it and the cache is left running rather than empty.
// between's OWN error does not skip the restart: leaving a cache with no workers at all is
// the one outcome nothing recovers from.
func (c *ClusterCacheGVRSyncController) RestartCacheWorkers(ctx context.Context, ref store.CacheRef, between func() error) error {
	// One sequence per cache: overlapping ones deadlock on the lifecycle locks they hold to
	// the end, and the later's snapshot is stale before it runs — see cacheRestartGate.
	releaseGate, err := c.workers.acquireCacheRestart(ctx, ref.CacheID)
	if err != nil {
		return fmt.Errorf("await cache restart %d: %w", ref.CacheID, err)
	}
	defer releaseGate()

	// Mark restarting and snapshot in one step: from here until endCacheRestart no worker of
	// this cache can register, so a reconcile that opened the handle but hadn't registered —
	// invisible to any snapshot — is refused rather than left on the handle between() closes.
	running := c.workers.beginCacheRestart(ref.CacheID)
	// One release per begin, however the sequence ends. Lifted before the restarts (which
	// register through the very putIfAbsent it refuses); the defer covers every earlier exit,
	// so it must be idempotent or an overlapping sequence's hold would drop with ours.
	released := false
	release := func() {
		if !released {
			released = true
			c.workers.endCacheRestart(ref.CacheID)
		}
	}
	defer release()

	var drained []*syncEntry
	var stopErr error
	for _, entry := range running {
		mu := c.workers.lifecycleLock(entry.objID)
		mu.Lock()
		defer mu.Unlock() //nolint:gocritic // deliberately held until the sequence completes

		// holds, not isCurrent: a worker whose earlier stop timed out is still registered
		// and flagged draining, and isCurrent reports that as "not current" — which reads
		// here as "somebody else handled it" and would let between() delete the .db out
		// from under a goroutine still writing to it. It is still ours to drain, so retry
		// the stop and let a second failure abort the sequence below.
		if !c.workers.holds(entry) {
			continue // a reconcile stopped or replaced it while we waited
		}
		if err := c.workers.stopBounded(ctx, entry.objID); err != nil {
			// It stays in the set, draining — which is what keeps the deletion barrier
			// real; the next reconcile retries it (see ensureWorker).
			stopErr = errors.Join(stopErr, fmt.Errorf("drain worker %d: %w", entry.objID, err))
			continue
		}
		drained = append(drained, entry)
	}

	// Lift the barrier before the restarts, not on the deferred path: startEntry registers
	// through the same putIfAbsent the barrier refuses.
	restart := func() {
		release()
		for _, entry := range drained {
			c.startEntry(ctx, entry)
		}
	}
	if stopErr != nil {
		slog.Warn("gvrsynccontroller: cache restart aborted, a worker would not drain",
			"cache", ref.CacheID, "err", stopErr)
		restart()
		return stopErr
	}

	err = between()
	restart()
	return err
}

// StopWorkers stops every running worker and waits for them to unwind. Called by the
// service during shutdown, between beehive's drain and the cache manager's.
func (c *ClusterCacheGVRSyncController) StopWorkers(ctx context.Context) error {
	return c.workers.stopAll(ctx)
}

// Reconcile converges one ClusterCacheGVRSync object: it runs the kind's worker while the
// object is enabled and its cluster has credentials, and stops it otherwise.
func (c *ClusterCacheGVRSyncController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus],
	obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
) (beehive.Result, error) {
	// Hold this object's lifecycle lock across the whole pass: every branch below either
	// stops or (re)starts its worker, and each must be atomic against the resync poke's
	// restart, which stops and starts as one step. Per object, so distinct kinds still
	// reconcile in parallel — a startup pass walks hundreds of them.
	mu := c.workers.lifecycleLock(obj.ID)
	mu.Lock()
	defer mu.Unlock()

	if obj.DeletionRequestedAt != nil {
		return beehive.Result{}, c.finalize(ctx, client, obj)
	}

	if !obj.Spec.Enabled {
		// A paused kind keeps its rows and its catalog entry, so an unpause resumes from
		// the saved position instead of re-listing. Only a deletion — the kind is no
		// longer served — clears them.
		if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
			return beehive.Result{}, err
		}
		return beehive.Result{}, writeSyncGate(ctx, client, &c.writeMu, obj.ID, obj.Generation, domain.ReasonPaused, "Sync is paused")
	}

	ref, clusterObjID, err := c.resolveChain(ctx, client, obj.ID)
	if err != nil {
		return beehive.Result{}, err
	}
	if clusterObjID == 0 {
		// The owner chain is gone (a cascade in flight); our object is being cleaned up too.
		return beehive.Result{}, c.workers.stopBounded(ctx, obj.ID)
	}

	// Depend on the Cluster so its status write on a successful probe — the same converge
	// that fills the connection manager — wakes us.
	if err := client.AddDependency(ctx, obj.ID, clusterObjID); err != nil {
		return beehive.Result{}, err
	}

	// One read for the pair: a rotation between two reads would start a worker on the old
	// config while recording the new fingerprint, and it would then never be restarted.
	restCfg, fingerprint := c.connMgr.Get(domain.ClusterID(clusterObjID))
	if restCfg == nil {
		if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
			return beehive.Result{}, err
		}
		err := writeSyncGate(ctx, client, &c.writeMu, obj.ID, obj.Generation, domain.ReasonNoConnection, "Waiting for a connection to the cluster")
		return beehive.Result{RequeueAfter: cacheSyncConnectRetry}, err
	}

	if err := c.ensureWorker(ctx, obj, ref, restCfg, fingerprint); err != nil {
		return beehive.Result{}, err
	}
	return beehive.Result{RequeueAfter: workerRecheckInterval}, nil
}

// resolveChain climbs this object's owner chain — the sync child → its
// ClusterCacheGVRDiscovery anchor → the ClusterCache → the Cluster. It is the shared
// resolveCacheChain with one extra hop in front, since this kind hangs off the discovery
// anchor rather than the cache directly. A zero cluster id with a nil error means the
// chain is broken (an owner already collected, which happens while a delete cascades).
func (c *ClusterCacheGVRSyncController) resolveChain(
	ctx context.Context,
	client beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus],
	objID beehive.ObjectID,
) (store.CacheRef, beehive.ObjectID, error) {
	discoveryRef, ok, err := client.GetOwner(ctx, objID)
	if err != nil || !ok {
		return store.CacheRef{}, 0, err
	}
	return resolveCacheChain(ctx, c.discoveryClient, c.cacheClient, discoveryRef.ID)
}

// finalize handles the deletion path: drain the worker, drop the kind from the cache, then
// clear the finalizer so GC can collect us — which is what releases the wait above us. A
// drain that fails leaves the finalizer in place, so the next reconcile retries rather
// than letting the .db file be deleted under a live writer.
func (c *ClusterCacheGVRSyncController) finalize(
	ctx context.Context,
	client beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus],
	obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
) error {
	if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
		return err
	}
	c.forgetStats(obj.ID)
	c.forgetKind(ctx, client, obj)
	if !slices.Contains(obj.Finalizers, gvrSyncDrainFinalizer) {
		c.workers.forgetLifecycle(obj.ID)
		return nil
	}
	if err := client.DeleteFinalizer(ctx, obj.ID, gvrSyncDrainFinalizer); err != nil {
		return err
	}
	// Collected, so the lifecycle lock has no further callers. The only reclamation: ids are
	// AUTOINCREMENT, so churning kinds would otherwise leak a mutex per kind per incarnation.
	c.workers.forgetLifecycle(obj.ID)
	return nil
}

// forgetKind removes the kind's rows, edges, catalog entry and cookie, so the dashboard
// can't list a kind frozen at whenever its sync stopped.
//
// BEST EFFORT and deliberately OUTSIDE the deletion barrier: the finalizer exists to prove
// the worker drained, and blocking here would deadlock the common case where the whole cache
// file is going anyway. Lookup, never Open, for the same reason — re-opening mid-teardown
// re-materializes the file the cache controller is about to remove.
func (c *ClusterCacheGVRSyncController) forgetKind(
	ctx context.Context,
	client beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus],
	obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
) {
	ref, clusterObjID, err := c.resolveChain(ctx, client, obj.ID)
	if err != nil || clusterObjID == 0 {
		return // the chain is already gone, so the cache is too
	}
	cdb := c.cacheManager.Lookup(ref.CacheID)
	if cdb == nil {
		return
	}
	if err := forgetKindRows(ctx, cdb, syncKind(obj.Spec)); err != nil {
		slog.Warn("gvrsynccontroller: forget kind", "object", obj.ID, "resource", obj.Spec.Resource, "err", err)
	}
}

// ensureWorker starts a worker if none runs, restarts it when its build inputs moved, and
// otherwise leaves it alone.
//
// **A worker's identity is its connection fingerprint AND its kind.** The fingerprint
// catches a credential rotation; the kind catches a discovery pass respecifying this child,
// which is real because a child is named for its (apiVersion, resource) alone — so a CRD
// recreated with the same plural but a different Kind keeps the SAME child. Comparing only
// the fingerprint would leave the old worker writing rows under the obsolete Kind (the
// objects table is keyed by kind) against a generation that has moved on.
//
// Spec.Enabled is deliberately NOT part of the identity: pausing is not a restart, and
// converge stops the worker before this is reached.
func (c *ClusterCacheGVRSyncController) ensureWorker(
	ctx context.Context,
	obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
	ref store.CacheRef,
	restCfg *rest.Config,
	fingerprint string,
) error {
	kind := syncKind(obj.Spec)
	entry := c.workers.get(obj.ID)
	// A DRAINING entry is not a running worker however well its identity matches — its stop
	// timed out and it stayed registered (which keeps the deletion barrier honest), and its
	// sink drops every report. Leaving it alone would end this kind's syncing for the process
	// lifetime, so fall through: retry the drain and rebuild.
	if entry != nil && !entry.draining.Load() && entry.fingerprint == fingerprint && entry.kind == kind {
		return nil
	}
	if entry != nil {
		if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
			return err
		}
	}

	// Idempotent, so a cache's many children converge on one handle; the Manager owns closing.
	cdb, err := c.cacheManager.Open(ctx, ref)
	if err != nil {
		return fmt.Errorf("open cache for gvr sync: %w", err)
	}

	// Nothing else would ever collect the previous Kind's rows — the child that owned them is
	// this one, and it now describes something else — so the cache would serve a phantom kind
	// beside the real one. Best effort; a failure retries on the next reconcile.
	if entry != nil && entry.kind != kind {
		c.forgetSupersededKind(ctx, obj.ID, cdb, entry.kind)
	}

	return c.startWorker(ctx, obj.ID, obj.Generation, fingerprint, restCfg, cdb, kind, ref)
}

// forgetSupersededKind drops the rows a worker wrote under an identity this child no longer
// has.
func (c *ClusterCacheGVRSyncController) forgetSupersededKind(
	ctx context.Context,
	objID beehive.ObjectID,
	cdb *store.ClusterDB,
	old objectsync.Kind,
) {
	if err := forgetKindRows(ctx, cdb, old); err != nil {
		slog.Warn("gvrsynccontroller: forget superseded kind",
			"object", objID, "kind", old.Kind, "resource", old.Resource, "err", err)
	}
}

// startWorker builds, registers, then starts a worker. Shared by the reconcile and the
// resync poke.
//
// Registers BEFORE starting, and only if the object has no worker: two builders can race,
// and the loser must drop what it built rather than displace a newer worker — safe precisely
// because nothing has been started yet. kubesync.Worker.Stop has the matching latch, so a
// stop landing in that window makes the later Start a no-op.
func (c *ClusterCacheGVRSyncController) startWorker(
	ctx context.Context,
	objID beehive.ObjectID,
	gen int64,
	fingerprint string,
	cfg *rest.Config,
	cdb *store.ClusterDB,
	kind objectsync.Kind,
	ref store.CacheRef,
) error {
	next := &syncEntry{
		fingerprint: fingerprint,
		objID:       objID,
		gen:         gen,
		cfg:         cfg,
		cdb:         cdb,
		kind:        kind,
		ref:         ref,
	}
	// The worker's clients are built from the cache's shared budget, not the raw
	// credentials — see cacheClientPolicy.config.
	policy := c.policies.get(ref.CacheID)
	worker, err := c.newWorker(ctx, policy.config(cfg), cdb, kind, policy.listLimiter, &syncSink{
		set:     c.workers,
		entry:   next,
		writeMu: &c.writeMu,
		apply:   c.applyReport,
		name:    "gvrsynccontroller",
	})
	if err != nil {
		return fmt.Errorf("build gvr sync worker: %w", err)
	}
	next.worker = worker
	ok, err := c.workers.putIfAbsent(next)
	if err != nil {
		// The cache is mid-restart, so cdb is the handle that restart is about to close.
		// Drop the worker unstarted and let the next reconcile build one on the new handle.
		return err
	}
	if !ok {
		return nil // a reconcile got there first; its worker is the live one
	}
	worker.Start()
	return nil
}

// syncKind projects the spec onto the identity the sync packages address a collection by.
func syncKind(spec domain.ClusterCacheGVRSyncSpec) objectsync.Kind {
	return objectsync.Kind{
		APIVersion: spec.APIVersion,
		Kind:       spec.Kind,
		Resource:   spec.Resource,
		Namespaced: spec.Namespaced,
	}
}

// applyReport folds one worker report into the object's condition and event log, and into
// the controller's freshness stamps. Must hold writeMu.
func (c *ClusterCacheGVRSyncController) applyReport(ctx context.Context, entry *syncEntry, st kubesync.Status) error {
	recordSyncTransition(ctx, c.ctrlClient, entry, st)

	statsID := domain.ClusterCacheGVRSyncID(entry.objID)
	prev := c.stats[statsID]
	c.stats[statsID] = domain.ClusterCacheGVRSyncStats{
		LastUpdateAt: keepStamp(st.LastUpdateAt, prev.LastUpdateAt),
		LastLiveAt:   keepStamp(st.LastLiveAt, prev.LastLiveAt),
	}

	// The stamps are published by the same lock that writes the condition, so a reader
	// can't see Watching beside a stamp from before it.
	return reportCondition(ctx, c.ctrlClient, entry.objID, entry.gen, syncedCondition(st, entry.noun()))
}
