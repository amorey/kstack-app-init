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

package cluster

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
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// gvrSyncDrainFinalizer gates one kind's sync object deletion on its worker having
// stopped. A running worker holds the cache's ClusterDB handle, so without the barrier the
// cache controller could delete the .db file while a mid-write worker re-materializes it
// (GC's cascade marks a child for deletion but does not order its teardown). The chain is
// transitive: the cache waits for its own children to be collected, GC won't collect the
// discovery anchor while it still owns sync children, and this finalizer is what makes
// "the sync children are gone" mean "their workers have drained".
const gvrSyncDrainFinalizer = "kstack.io/gvrsync-drain"

// ClusterCacheGVRSyncController reconciles ClusterCacheGVRSync objects: it owns the
// worker that mirrors ONE Kubernetes kind's objects into the cache's shared objects table
// (plus the owner_refs/labels edges and the kind's catalog entry).
//
// It runs one worker per kind the discovery controller found — so a hundred workers per
// cache is the normal case, Events among them (they differ only in which store their
// worker writes to; see newSyncStore). Two things follow from that count. The steady state must write nothing (it does: an
// unchanged condition is suppressed and there is no status to rewrite, so a cache's
// hundred heartbeats wake nobody), and the per-kind isolation is the point — a CRD whose
// watch is forbidden reports on its own object and leaves every other kind syncing.
//
// The lifecycle machinery lives in cache_sync_workers.go: the worker registry, the
// drain-before-forget rule, the out-of-band report guard, and the condition/event
// vocabulary. What is specific here is the kind identity carried in the spec, and that a
// deletion must take the kind's cached rows with it.
type ClusterCacheGVRSyncController struct {
	// discoveryClient is the extra hop in this kind's owner climb: it hangs off the
	// discovery anchor, not the cache, so the climb is one hop longer than a direct child's
	// (this object → its discovery anchor → the cache → the cluster).
	discoveryClient beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	// cacheClient reads the parent ClusterCache's owner edge (the last hop of the climb).
	cacheClient  beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	connMgr      *ConnectionManager
	cacheManager *store.Manager
	// pokeSvc is the resync bus; nil disables poke-driven restarts (tests drive
	// restartWorkers directly).
	pokeSvc *poke.Service
	pokeSub *pokeSubscription

	// ctrlClient is this kind's status-write client, used by the worker sinks — which
	// report out-of-band, off the reconcile path. Injected before the control plane starts.
	ctrlClient beehive.ControllerClient[ClusterCacheGVRSyncStatus]

	// newWorker builds a worker; the package's own tests swap in a fake so the
	// controller's lifecycle and report folding are exercised without a cluster.
	newWorker newGVRWorkerFunc

	workers *workerSet

	// writeMu serializes the condition/stats read-modify-writes between the reconcile path
	// and the worker sinks.
	writeMu sync.Mutex
	// policies hands out each cache's shared client budget — the rate limiter its clients
	// draw from and the LIST semaphore its workers contend for. Shared with the discovery
	// controller via the runtime, since both talk to one cluster on one cache's behalf.
	policies *cacheClientPolicies

	// stats holds each kind's freshness stamps, keyed by its record id, guarded by writeMu.
	// In memory rather than in the object's status because nothing in the object graph
	// reacts to them — see ClusterCacheGVRSyncStats.
	stats map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats
}

// newGVRWorkerFunc constructs one kind's sync worker over an open cache handle.
type newGVRWorkerFunc func(ctx context.Context, cfg *rest.Config, cdb *store.ClusterDB, kind objectsync.Kind, limiter kubesync.ListLimiter, sink kubesync.Sink) (workerHandle, error)

// newLiveGVRWorker is the production constructor, and the one place that knows a kind
// can want a different store. Everything else — the controller, the worker, the list/watch
// state machine — treats Events like any other kind.
//
// The jitter seed carries the kind as well as the cache, so a cache's hundred workers
// spread their periodic re-lists across the interval instead of re-listing the whole
// cluster at one instant.
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
	// Register the kind in the cache catalog before the first row lands: it is what makes
	// the kind readable at all (store.Objects resolves the plural resource through it) and
	// what puts the kind in the dashboard nav.
	if err := st.EnsureCatalog(ctx); err != nil {
		return nil, err
	}
	opts := []kubesync.Option{kubesync.WithListLimiter(limiter)}
	// The api server serves Events without its watch cache, so their watches carry no
	// bookmarks and a quiet cluster's Event stream legitimately goes silent for hours.
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

// forgetKindRows drops one kind's cached rows, edges, catalog entry and resume cookie.
//
// It is the reap counterpart of newSyncStore, and lives beside it for the same reason: which
// store a kind writes to is one decision, so where its rows are reaped from is too. Events
// are the exemption — their rows live in a table of their own, and the collection is always
// served, so the only thing that ends them is the cache file going away. Routing that here
// keeps the three callers (a child deleted, a kind remapped, an orphan swept) free of any
// per-kind knowledge; each used to repeat the check and its rationale.
func forgetKindRows(ctx context.Context, cdb *store.ClusterDB, kind objectsync.Kind) error {
	if isEventsKind(kind) {
		return nil
	}
	return objectsync.Forget(ctx, cdb, kind)
}

// isEventsKind reports whether a kind is the cluster's Event collection.
//
// Keyed on the api group and plural, NOT on the Kind name: "Event" is not reserved, so a
// CRD named Event in somebody's own group would otherwise have its objects routed into
// the events table and be exempted from the deletion sweep. The discovery filter checks
// the same pair.
func isEventsKind(kind objectsync.Kind) bool {
	return kind.APIVersion == eventsAPIVersion && kind.Resource == eventsResource
}

// isAltEventsKind reports whether an api group-version and plural name the NON-canonical
// spelling of the collection isEventsKind names — the same underlying store, served a second
// time under its own group. It is the discovery filter's drop test, keyed on the same pair
// isEventsKind is (and for the same reason: a CRD named Event in somebody's own group is that
// user's kind, not this collection, so it must not be filtered out of the sync set).
func isAltEventsKind(apiVersion, resource string) bool {
	group, _, _ := strings.Cut(apiVersion, "/")
	return group == eventsAltGroup && resource == eventsResource
}

// NewClusterCacheGVRSyncController builds the controller from the shared runtime.
func NewClusterCacheGVRSyncController(rt *controllerRuntime) *ClusterCacheGVRSyncController {
	return &ClusterCacheGVRSyncController{
		discoveryClient: beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](
			rt.bh, ClusterCacheGVRDiscoveryGroupKind),
		cacheClient:  beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](rt.bh, ClusterCacheGroupKind),
		connMgr:      rt.connMgr,
		cacheManager: rt.cacheManager,
		pokeSvc:      rt.pokeSvc,
		newWorker:    newLiveGVRWorker,
		workers:      newWorkerSet(),
		stats:        make(map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats),
		policies:     rt.policies(),
	}
}

// SetControllerClient injects the status-write client from beehive.Register. Call once,
// before the control plane starts.
func (c *ClusterCacheGVRSyncController) SetControllerClient(cl beehive.ControllerClient[ClusterCacheGVRSyncStatus]) {
	c.ctrlClient = cl
}

// Stats returns one kind's freshness stamps, or ok=false when its worker has reported
// nothing in this process yet. Read on request by the service; never stored.
func (c *ClusterCacheGVRSyncController) Stats(objID beehive.ObjectID) (ClusterCacheGVRSyncStats, bool) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	st, ok := c.stats[ClusterCacheGVRSyncID(objID)]
	return st, ok
}

// StatsSnapshot copies every kind's stamps under ONE lock.
//
// The rollup reads a whole cache's worth at a time, and writeMu is held by the condition
// writers across a beehive write — so a per-record Stats() call would take that lock a
// hundred times per fold, contending hardest exactly during a cold start when the writers
// are busiest.
func (c *ClusterCacheGVRSyncController) StatsSnapshot() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	out := make(map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats, len(c.stats))
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
	delete(c.stats, ClusterCacheGVRSyncID(objID))
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

// restartWorkers rebuilds every running worker in place — the resync poke's handler.
//
// A poke means the machine just woke or the network just came back, so every long-lived
// watch is probably talking to a dead connection. The client can take a while to work that
// out on its own (the HTTP/2 keepalive detects a silently-dropped connection in ~15s, and
// a watch that merely stopped delivering ages out on kubesync's stale threshold), so the
// point of this is to not wait: drop the streams and re-establish now. Each worker resumes
// from its persisted resourceVersion, so a restart is cheap — deltas, not a re-list.
//
// It only touches what is RUNNING. A paused kind or one waiting on credentials has no
// worker, and starting one here would be this path deciding something that is the
// reconcile's to decide.
func (c *ClusterCacheGVRSyncController) restartWorkers(ctx context.Context) {
	for _, entry := range c.workers.entries() {
		c.restartWorker(ctx, entry)
	}
}

// restartWorker bounces one entry, holding its object's lifecycle lock across the whole
// stop-then-start so a reconcile can't act on the gap between them. Without it a pause or a
// deletion landing in that window sees no worker to stop, reports itself done — and for a
// deletion, clears the drain finalizer that is holding the cache file open — while this
// pass puts a worker back for an object that is paused or already gone.
func (c *ClusterCacheGVRSyncController) restartWorker(ctx context.Context, entry *syncEntry) {
	mu := c.workers.lifecycleLock(entry.objID)
	mu.Lock()
	defer mu.Unlock()

	// Re-check under the lock: a reconcile may have stopped or replaced this worker while
	// we waited, and resurrecting the entry we sampled before the lock is the same bug.
	if !c.workers.isCurrent(entry) {
		return
	}
	if err := c.workers.stopBounded(ctx, entry.objID); err != nil {
		slog.Warn("gvrsynccontroller: stop worker for resync", "object", entry.objID, "err", err)
		return // it stays in the set, so the next poke or reconcile retries
	}
	c.startEntry(ctx, entry)
}

// startEntry rebuilds a drained entry's worker. It RE-RESOLVES the cache handle rather than
// reusing entry.cdb: the handle a worker was built with may have been closed since — clearing
// a cache shuts it along with the file — and a worker rebuilt on a closed database fails
// every operation while staying registered, which is worse than not restarting at all, since
// a registered worker is exactly what stops a reconcile from replacing it. Open is
// idempotent, so on the ordinary poke path this hands back the very same handle.
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

// RestartCacheWorkers drains every running worker of ONE cache, runs between, and starts
// them again on a freshly opened handle. It is what makes "clear cache" work: nothing else
// would rebuild those workers, since a reconcile leaves a running worker alone while its
// connection and kind are unchanged, so neither the 30s liveness recheck nor the 5-minute
// discovery pass revives one.
//
// The ORDER is the whole point, and it is why the caller's teardown runs inside this rather
// than before it. The workers are drained first, so the cache file is not removed under a
// live writer; between then does the teardown; and each worker is rebuilt afterwards
// against whatever handle the store Manager hands back — a NEW one, since the old was
// closed along with the file. Every affected object's lifecycle lock is held across the
// whole sequence, so no reconcile can slip in and start a worker on the doomed handle.
//
// A worker that will NOT drain aborts the sequence: between is the caller's teardown, and
// running it with a writer still live is the exact thing the drain exists to prevent. The
// workers that did drain are restarted anyway and the drain error returned, so the caller
// (ClearCache) reports the failure and the cache is left running rather than empty.
//
// between's OWN error, by contrast, does not skip the restart: the workers are already
// drained, and leaving a cache with no workers at all is the one outcome nothing would
// recover from.
func (c *ClusterCacheGVRSyncController) RestartCacheWorkers(ctx context.Context, ref store.CacheRef, between func() error) error {
	// One sequence per cache at a time. Overlapping ones deadlock on the lifecycle locks
	// they hold to the end, and the later one's snapshot is stale before it runs — see
	// cacheRestartGate. Waiting honours ctx, so a caller under a timeout still gives up.
	releaseGate, err := c.workers.acquireCacheRestart(ctx, ref.CacheID)
	if err != nil {
		return fmt.Errorf("await cache restart %d: %w", ref.CacheID, err)
	}
	defer releaseGate()

	// Mark the cache restarting and snapshot its workers in one step. From here until
	// endCacheRestart no worker of this cache can register, so a reconcile that opened the
	// handle but had not yet registered — invisible to any snapshot, and holding a
	// lifecycle lock nobody knows to wait for — is refused rather than left running on the
	// handle between() is about to close.
	running := c.workers.beginCacheRestart(ref.CacheID)
	// One release per begin, however the sequence ends. The barrier is lifted before the
	// restarts (they register through the very putIfAbsent it refuses) and the deferred
	// call is the safety net for every path that doesn't get that far — so the release has
	// to be idempotent, or an overlapping sequence's hold would be dropped along with ours.
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
	client beehive.ControllerClient[ClusterCacheGVRSyncStatus],
	obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus],
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
		return beehive.Result{}, writeSyncGate(ctx, client, &c.writeMu, obj.ID, obj.Generation, ReasonPaused, "Sync is paused")
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
	// that fills the ConnectionManager — wakes us.
	if err := client.AddDependency(ctx, obj.ID, clusterObjID); err != nil {
		return beehive.Result{}, err
	}

	// One read for the pair: a rotation between two reads would start a worker on the old
	// config while recording the new fingerprint, and it would then never be restarted.
	restCfg, fingerprint := c.connMgr.Get(ClusterID(clusterObjID))
	if restCfg == nil {
		if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
			return beehive.Result{}, err
		}
		err := writeSyncGate(ctx, client, &c.writeMu, obj.ID, obj.Generation, ReasonNoConnection, "Waiting for a connection to the cluster")
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
	client beehive.ControllerClient[ClusterCacheGVRSyncStatus],
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
	client beehive.ControllerClient[ClusterCacheGVRSyncStatus],
	obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus],
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
	// The object is collected: nothing will reconcile it again, so its lifecycle lock has
	// no further callers. Dropping it here is the only reclamation — the ids are
	// AUTOINCREMENT, so a cluster whose kinds churn would otherwise leak a mutex per kind
	// per incarnation, forever.
	c.workers.forgetLifecycle(obj.ID)
	return nil
}

// forgetKind removes the kind's rows, edges, catalog entry and resume cookie from the
// cache — the cleanup that keeps the dashboard from listing a kind whose contents are
// frozen at whenever its sync stopped.
//
// It is BEST EFFORT, and deliberately not part of the deletion barrier. The finalizer
// exists to prove the worker has drained, nothing more; blocking on this cleanup would
// deadlock the common case where the whole cache is being deleted, since the rows are
// about to go with the file anyway. For the same reason it looks the cache up rather than
// opening it: a closed cache has nothing to clean, and re-opening one mid-teardown would
// re-materialize the very file the cache controller is about to remove.
func (c *ClusterCacheGVRSyncController) forgetKind(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheGVRSyncStatus],
	obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus],
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

// ensureWorker starts the worker if none is running, restarts it when what the worker was
// built from has changed, and otherwise leaves the running one alone.
//
// **A worker's identity is its connection fingerprint AND its kind.** The fingerprint
// catches a credential rotation; the kind catches a discovery pass rewriting this child's
// spec. That second case is real because a child is named for its (apiVersion, resource)
// only — deliberately, since that pair is what the REST path needs and what the API server
// guarantees unique — so a CRD deleted and recreated with the same plural but a different
// Kind or scope keeps the SAME child, which discovery updates in place. Comparing only the
// fingerprint would leave the old worker running: it would keep listing the right REST path
// while writing rows under the obsolete Kind (the objects table is keyed by kind), holding a
// stale catalog entry, naming the old noun in its messages, and reporting conditions against
// a generation that has moved on.
//
// Spec.Enabled is deliberately NOT part of the identity: pausing is not a restart, and the
// converge above stops the worker before this is reached.
func (c *ClusterCacheGVRSyncController) ensureWorker(
	ctx context.Context,
	obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus],
	ref store.CacheRef,
	restCfg *rest.Config,
	fingerprint string,
) error {
	kind := syncKind(obj.Spec)
	entry := c.workers.get(obj.ID)
	// A DRAINING entry is not a running worker, however well its identity matches: it is
	// one whose stop timed out and stayed in the set (which is what keeps the deletion
	// barrier honest). Its sink drops every report, so leaving it alone would silently end
	// this kind's syncing for the process lifetime — every later reconcile no-opping on a
	// dead entry. Fall through instead, so the drain is retried and a worker rebuilt.
	if entry != nil && !entry.draining.Load() && entry.fingerprint == fingerprint && entry.kind == kind {
		return nil
	}
	if entry != nil {
		if err := c.workers.stopBounded(ctx, obj.ID); err != nil {
			return err
		}
	}

	// Opening is idempotent, so a cache's many sync children converge on one handle; the
	// store Manager owns closing it (cache deletion / shutdown).
	cdb, err := c.cacheManager.Open(ctx, ref)
	if err != nil {
		return fmt.Errorf("open cache for gvr sync: %w", err)
	}

	// A kind remap leaves the previous Kind's rows, edges, catalog entry and resume cookie
	// behind — nothing else will ever collect them, since the child that owned them is this
	// one and it now describes something different. Without this the cache would keep
	// serving a kind the cluster no longer has (a phantom row in the dashboard's nav) beside
	// the real one. Best effort, like the deletion path's forget: the worker below is what
	// matters, and a failure here retries on the next reconcile.
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

// startWorker builds a worker for one object and registers it, then starts it. Shared by
// the reconcile and the resync poke, which differ only in where their inputs come from.
//
// It registers BEFORE starting and only if the object has no worker: a poke that drained
// the old one can be raced by a reconcile starting a replacement in the gap, and the loser
// must drop the worker it built rather than displace a newer one. Dropping is safe here
// precisely because nothing has been started yet.
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
func syncKind(spec ClusterCacheGVRSyncSpec) objectsync.Kind {
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

	statsID := ClusterCacheGVRSyncID(entry.objID)
	prev := c.stats[statsID]
	c.stats[statsID] = ClusterCacheGVRSyncStats{
		LastUpdateAt: keepStamp(st.LastUpdateAt, prev.LastUpdateAt),
		LastLiveAt:   keepStamp(st.LastLiveAt, prev.LastLiveAt),
	}

	// The stamps are published by the same lock that writes the condition, so a reader
	// can't see Watching beside a stamp from before it.
	return reportCondition(ctx, c.ctrlClient, entry.objID, entry.gen, syncedCondition(st, entry.noun()))
}
