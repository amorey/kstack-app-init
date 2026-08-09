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
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

const (
	// childRetryInterval is the short requeue converge asks for when a child-object op
	// (ensuring the ClusterCacheGVRDiscovery anchor) failed — small so the subtree
	// converges promptly.
	//
	// It is the ONLY requeue this controller asks for. There is no periodic re-reconcile,
	// because every input converge reads is event-driven: the parent's spec and status
	// both wake us through the DependsOn edge declared in Reconcile (which is what carries
	// eligibility and the active-identity check), our own spec bumps our generation, and
	// WithStartupFullPass covers a restart. Nothing here varies with time — this
	// controller reads no credentials and runs no worker, so it has nothing to poll for.
	childRetryInterval = time.Second

	// cacheSyncConnectRetry is the requeue a cache sync child asks for while it waits on
	// its cluster's first successful probe (credentials live in memory, so there is no
	// object write to wait for). One constant for every child because they all wait on the
	// same event.
	//
	// It is COARSE because the real wake is the DependsOn edge each child declares on the
	// Cluster: a probe that fills the ConnectionManager writes in the same converge, and
	// that write wakes every dependent — so this only has to catch a wake that never came,
	// not to discover the connection. The cost of getting it wrong scales with the kind
	// count: an offline cluster has one waiting child per served kind (100-150), each
	// reconcile doing three owner reads and a dependency write against the shared beehive
	// store, forever. At a few seconds that is tens of reconciles per second per offline
	// cluster and it never stops.
	cacheSyncConnectRetry = time.Minute
)

// cacheFilesFinalizer gates a ClusterCache's deletion on this controller deleting its
// on-disk cache file. It is set at creation (ensureClusterCache) and cleared on the
// deletion reconcile once the file is gone, so GC can't collect the row — and orphan the
// file — before the cleanup runs.
const cacheFilesFinalizer = "kstack.io/cache-files"

// ClusterCacheController reconciles ClusterCache beehive objects: it owns the ClusterCache
// lifecycle (eligibility + active-identity gating, the on-disk cache-file finalizer/
// teardown) and the existence of the cache's sync children.
//
// It reads the parent Cluster to determine eligibility (connection-eligible + SyncEnabled)
// and adds a DependsOn edge so beehive re-queues this cache when the parent's spec changes
// (e.g. SyncEnabled toggled).
//
// **Sync children exist for the cache's whole life; pausing is a spec write, not a delete.**
// This controller is the one place that knows whether a cache should sync — its parent must
// be sync-eligible AND this cache must be the parent's active identity — and it pushes that
// intent *down* into each child's Spec.Enabled rather than expressing it through the child's
// existence. Creation is idempotent and unconditional; the only removal is beehive's GC
// cascade when the cache itself is deleted. That buys three things: a pause keeps the child's
// status/conditions/event history (a deleted anchor takes its worker's history with it), a
// pause/unpause never waits on a GC name release, and stopping a worker stays inside the
// child's own controller instead of becoming a deletion another controller must order against.
// It also mirrors the layer above — ClusterCoreController keeps a ClusterCache across a pause
// and deletes one only on a UID switch. The children are coupled to their controllers only
// through the object graph, not by direct calls.
//
// This kind's status is empty: the cache measures nothing itself, and the whole-cache
// verdict a UI wants is folded from the per-kind records read-side
// (Service.WatchCacheSyncHealth) rather than stored — nothing in the object graph acts on
// it. What the cache reports is its own coarse Synced condition: did it decide to sync?
type ClusterCacheController struct {
	coreClient beehive.Client[ClusterSpec, ClusterStatus]
	// gvrDiscoveryClient creates the ClusterCacheGVRDiscovery child that anchors this
	// cache's sync subtree, and writes its Spec.Enabled pause switch. A full Client (not
	// the status-write ControllerClient) because the cache controller creates and writes
	// the spec of objects of another kind, mirroring how ClusterCoreController drives
	// cacheClient.
	gvrDiscoveryClient beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	cacheManager       *store.Manager

	// writeMu serializes read-modify-write status updates from the reconcile worker.
	writeMu sync.Mutex
}

// NewClusterCacheController builds the controller from the shared runtime. It mints the
// Cluster client (to read the parent for eligibility) and the sync-anchor client (to
// create the GVR-discovery child) from rt.bh; rt.cacheManager owns the
// per-cluster SQLite cache files (shared with the resolver so both see the same open DBs)
// .
func NewClusterCacheController(rt *controllerRuntime) *ClusterCacheController {
	return &ClusterCacheController{
		coreClient:         beehive.NewClient[ClusterSpec, ClusterStatus](rt.bh, ClusterGroupKind),
		gvrDiscoveryClient: beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](rt.bh, ClusterCacheGVRDiscoveryGroupKind),
		cacheManager:       rt.cacheManager,
	}
}

// Reconcile converges one ClusterCache object toward its parent Cluster's spec.
func (c *ClusterCacheController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	// The parent ClusterID is the ClusterCache's owner (its owned_by edge), read from
	// beehive's object graph rather than re-parsed out of the name.
	owner, ownerExists, err := client.GetOwner(ctx, obj.ID)
	if err != nil {
		return beehive.Result{}, err
	}

	// Deletion (a UID-switch prune or a cluster-delete cascade): wait for the sync children
	// to be gone, then delete the on-disk cache file and clear the finalizer so GC can
	// collect the row. This branch *is* the cacheFilesFinalizer's handler — beehive has no
	// separate finalizer callback, so honoring one means doing the cleanup here and only
	// then clearing it. A file-delete error returns without clearing, so the next reconcile
	// retries and the file can't be orphaned.
	//
	// **The wait is the stop-before-delete barrier.** GC's cascade marks the children for
	// deletion but does NOT order their teardown before this file delete, and a sync child's
	// worker holds the cache's ClusterDB handle — so deleting the file under a mid-write
	// worker could leave an orphaned .db behind it. Each sync child carries its own drain
	// finalizer (gvrSyncDrainFinalizer), cleared only once its worker has stopped, so a
	// child that is *gone* is a child whose worker has drained. Deletion is the only path
	// needing this: a pause stops the worker inside the child's own controller, touching no
	// object. The wait is unconditional — it needs no owner, and skipping it is what would
	// release the row out from under a live writer.
	//
	// Only the file locator needs the owner (its id is the per-cluster dir, ours the file).
	// A missing owner is unreachable in practice: owned_by runs child→owner, and gcCollect
	// refuses to delete a row with incoming edges (it discounts only depends_on from a
	// deleting source), so the parent outlives every child. The guard is cheap insurance
	// against clearing the finalizer being blocked by an unformable path.
	if obj.DeletionRequestedAt != nil {
		children, err := client.ListOwned(ctx, obj.ID)
		if err != nil {
			return beehive.Result{}, err
		}
		if len(children) > 0 {
			return beehive.Result{RequeueAfter: childRetryInterval}, nil
		}
		if !ownerExists {
			// The file path is <dataDir>/clusters/<clusterID>/<cacheID>.db, so without the
			// owner there is no path to delete — and clearing the finalizer here would let
			// GC collect the row, leaving the .db (plus -wal/-shm) on disk with nothing left
			// that knows where it is. Requeue instead: a stuck deletion is visible and
			// recoverable, an orphaned file is neither.
			//
			// Unreachable in practice — owned_by runs child→owner and GC won't collect a row
			// with incoming edges, so the parent outlives every child — which is why this
			// logs rather than silently retrying.
			slog.Warn("clustercachecontroller: cache deletion has no owner; retrying rather than orphaning its files",
				"cache", obj.ID)
			return beehive.Result{RequeueAfter: childRetryInterval}, nil
		}
		if err := c.cacheManager.DeleteCacheFiles(ctx, newCacheRef(owner.ID, obj.ID)); err != nil {
			return beehive.Result{}, err
		}
		return beehive.Result{}, c.clearCacheFilesFinalizer(ctx, client, obj)
	}

	if !ownerExists {
		// No owner and not being deleted — the parent was GC'd; our object is being cleaned
		// up too.
		return beehive.Result{}, nil
	}

	// Read the parent Cluster to determine eligibility.
	clusterObj, err := c.coreClient.Get(ctx, owner.ID)
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			// Parent gone (GC race); our object will be cleaned up too.
			return beehive.Result{}, nil
		}
		return beehive.Result{}, err
	}

	// Add the DependsOn edge so beehive re-queues us on parent changes — spec edits (e.g.
	// SyncEnabled toggled) AND status writes (the core controller's live source observation,
	// which drives presence-based eligibility).
	//
	// The read above needs no re-read against a parent stamped mid-pass: beehive covers that
	// race on both sides of this call. Creating the edge stamps a durable reconcile-owed for
	// us atomically with the edge itself, so the pass that first declares the dependency is
	// always followed by one that reads the parent fresh. Once the edge exists, beehive
	// records our dependency watermark from the cursor taken at object *load* rather than at
	// completion — so any target that moved while we were reading stays counted as owed, and
	// the stale-dependents sweep re-queues us.
	if err := client.AddDependency(ctx, obj.ID, clusterObj.ID); err != nil {
		return beehive.Result{}, err
	}

	// This cache is "active" when its UID matches the parent's last-probed kube-system UID.
	// Only the active cache syncs — a cache left behind by a migration is paused, so its
	// discovery subtree never mirrors the new cluster's data into the old file.
	active := cacheIsActive(clusterObj, obj.Spec.ServerUID)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	var conds conditionSet
	requeueAfter := c.converge(ctx, client, obj.ID, active, clusterObj, &conds, obj.Conditions)

	// The pass's whole report is its conditions: this kind's status is empty, because the
	// cache measures nothing itself (its children do, out of band). Settling the generation
	// explicitly is therefore mandatory rather than incidental — a condition write does not
	// advance beehive's handshake, so an unchanged pass would leave the object unsettled and
	// re-enqueued by the owed pass forever.
	return beehive.Result{RequeueAfter: requeueAfter}, reportCondition(ctx, client, obj.ID, obj.Generation, conds...)
}

// clearCacheFilesFinalizer removes cacheFilesFinalizer so GC can collect the row. A no-op
// when the finalizer is absent (e.g. a double reconcile of the deletion).
func (c *ClusterCacheController) clearCacheFilesFinalizer(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) error {
	if !slices.Contains(obj.Finalizers, cacheFilesFinalizer) {
		return nil
	}
	return client.DeleteFinalizer(ctx, obj.ID, cacheFilesFinalizer)
}

// converge decides whether this ClusterCache should sync, pushes that decision into its sync
// children, and reflects it on the Synced condition. active reports whether this cache
// mirrors the cluster's currently-connected identity; an inactive cache (a physical migration
// left it behind) is paused like a sync-ineligible one.
//
// The children are ensured on both paths — only their Spec.Enabled differs. This is the sole
// evaluation of the sync rule; the children never re-derive it.
func (c *ClusterCacheController) converge(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	cacheObjID beehive.ObjectID,
	active bool,
	clusterObj *beehive.Object[ClusterSpec, ClusterStatus],
	conds *conditionSet,
	prev []Condition,
) time.Duration {
	enabled := syncEligible(clusterObj) && active

	// The sync anchor is ensured unconditionally and told whether this cache should sync;
	// everything below it — one child per served kind, Events included — belongs to the
	// discovery controller.
	//
	// This cache's own Synced condition stays coarse (Syncing/Paused) on purpose. The
	// verdict a UI wants is folded from the per-kind records read-side
	// (Service.WatchCacheSyncHealth); it is deliberately not stored here, since nothing in
	// the object graph acts on it — see ClusterCacheSyncHealth.
	retry := c.ensureGVRDiscovery(ctx, client, cacheObjID, enabled)

	// Record SyncStopped for a user-facing pause only — never for a migration prune of a
	// superseded cache (!active), which is an internal hand-over the user didn't ask for and
	// shouldn't see in the timeline. This layer is the one that can still tell the two apart:
	// converge collapses both into the single Spec.Enabled bit it pushes down, so the child
	// sees only "off" and could never make the distinction itself.
	//
	// The running→stopped transition is read off the condition we're about to overwrite,
	// not a runtime flag — so it survives a restart and can't re-fire on every reconcile of
	// an already-paused cache. A cache with no Synced condition yet has never run, so it
	// can't have stopped.
	if syncSwitchedOff(clusterObj) && syncWasRunning(prev) {
		if !c.recordSyncStopped(ctx, client, cacheObjID) {
			// The event didn't land. Do NOT advance the condition: the transition is read
			// off it, so overwriting it now would erase the only evidence that this cache
			// had been running, and the event would be lost for good rather than retried.
			// Carry the previous condition forward (beehive suppresses the unchanged write)
			// and come back.
			if old := FindCondition(prev, ConditionSynced); old != nil {
				conds.set(*old)
			}
			return childRetryInterval
		}
	}

	reason := ReasonSyncing
	if !enabled {
		reason = ReasonPaused
	}
	conds.set(liveCondition(ConditionSynced, ConditionFalse, reason, ""))

	if retry {
		return childRetryInterval
	}
	return 0
}

// syncWasRunning reports whether the Synced condition says this cache was syncing as of
// the last pass — the "running" half of a running→stopped transition. Absent means the
// cache has never synced, so it can't have stopped.
//
// It keys on Reason, not Status, which is what makes it survive a restart: beehive's
// liveness downgrade rewrites a prior process's Status to Unknown but leaves Reason
// alone, and a cache that was syncing when the process died really did stop.
func syncWasRunning(conds []Condition) bool {
	cond := FindCondition(conds, ConditionSynced)
	return cond != nil && cond.Reason == ReasonSyncing
}

// recordSyncStopped appends the terminal SyncStopped event to this cache's own timeline —
// the same timeline the sync-detail panel reads (clusterCacheEventsWatch), so a user pausing
// sync sees it there rather than the log simply going quiet.
//
// Best-effort: a failure is logged and convergence continues. The retry is beehive's — a
// failed status write leaves the condition on Syncing, so the next reconcile records again,
// and beehive coalesces the repeat into the existing run's count rather than appending a
// duplicate. Must hold writeMu.
// recordSyncStopped appends the pause to the cache's timeline, reporting whether it landed.
// The caller needs that answer: this event is recorded on a transition it can only detect
// once, so a failure must hold the condition back rather than be logged and forgotten.
func (c *ClusterCacheController) recordSyncStopped(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], cacheObjID beehive.ObjectID) bool {
	err := client.AddEvent(ctx, cacheObjID, beehive.EventSpec{
		Category: SyncEventCategory,
		Type:     beehive.EventNormal,
		Reason:   ReasonSyncStopped,
		Message:  "Sync stopped",
	})
	if err == nil {
		return true
	}
	if ctx.Err() == nil {
		slog.Warn("clustercachecontroller: record sync stopped", "cache", cacheObjID, "err", err)
	}
	// A cancelled context is a shutdown, not a lost event: there is nothing to retry into.
	return ctx.Err() != nil
}

// ensureGVRDiscovery converges this cache's ClusterCacheGVRDiscovery child — the anchor for
// the per-GVR sync subtree. It creates the anchor if absent, owned by the cache (so GC cascades
// to it when the cache is deleted) and keyed by the deterministic name, and writes spec, whose
// Enabled flag is how a pause reaches the workers below it. The anchor is never deleted here;
// see the type comment. It needs no drain finalizer of its own: the discovery controller runs no
// worker, and the workers below it belong to its own children, which GC cannot collect before
// they have drained (so the cache's wait for its children covers the whole subtree).
//
// GetOrCreate does the read-or-create atomically, so a concurrent reconcile can't duplicate it,
// but it never mutates an existing row — hence the follow-up Update, which is a no-op when the
// marshalled spec already matches (so a steady re-apply writes nothing and wakes nobody) and
// bumps Generation otherwise, requeuing the child to act on it.
//
// The DependsOn edge is the reverse direction: it makes the child's status writes requeue this
// cache, which is what the pending health rollup needs (an owner is NOT woken by its children).
// Since the child outlives every pause, the edge is added once and never removed — a live
// dependent would otherwise pin a deletion-pending child under beehive's RESTRICT. On the
// cache's own deletion GC drops the edge (DeleteFinalizingDependsOn) before collecting.
//
// It returns true to request a short requeue on any store error; the next reconcile retries.
func (c *ClusterCacheController) ensureGVRDiscovery(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	cacheObjID beehive.ObjectID,
	enabled bool,
) (retry bool) {
	spec := ClusterCacheGVRDiscoverySpec{Enabled: enabled}
	obj, created, err := c.gvrDiscoveryClient.GetOrCreate(ctx, ClusterCacheGVRDiscoveryName(cacheObjID), spec,
		beehive.WithOwner(cacheObjID))
	if err != nil {
		slog.Warn("cachecontroller: ensure gvr discovery", "cache", cacheObjID, "err", err)
		return true
	}
	if !created {
		if _, err := c.gvrDiscoveryClient.Update(ctx, obj.ID, spec); err != nil {
			slog.Warn("cachecontroller: update gvr discovery", "cache", cacheObjID, "err", err)
			return true
		}
	}
	if err := client.AddDependency(ctx, cacheObjID, obj.ID); err != nil {
		slog.Warn("cachecontroller: depend on gvr discovery", "cache", cacheObjID, "err", err)
		return true
	}
	return false
}

// syncEligible reports whether a cluster should have its cache synced.
func syncEligible(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return ConnectionEligible(obj) && obj.Spec.SyncEnabled
}

// syncSwitchedOff reports the USER having turned sync off — the two spec switches and
// nothing else. It is deliberately not !syncEligible, which also goes false whenever the
// kubeconfig context is momentarily absent: kubectx or a cloud CLI rewriting ~/.kube/config
// makes a context vanish and return within one write, and keying the SyncStopped event on
// eligibility appended a "Sync stopped" run to the cache's timeline on every such round
// trip, though nobody paused anything. A deletion cascade is excluded for the same reason
// in reverse — the cache is going away, so its timeline has no reader left.
func syncSwitchedOff(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return !obj.Spec.Enabled || !obj.Spec.SyncEnabled
}
