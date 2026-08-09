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
	// Short requeue after a failed child-object op, so the subtree converges promptly.
	//
	// The ONLY requeue this controller asks for: every input is event-driven (the parent's
	// spec and status wake us through the DependsOn edge, our own spec bumps our generation,
	// WithStartupFullPass covers a restart), and nothing here varies with time.
	childRetryInterval = time.Second

	// Requeue while a sync child waits on its cluster's first successful probe (credentials
	// live in memory, so there is no object write to wait for).
	//
	// COARSE deliberately: the real wake is each child's DependsOn edge on the Cluster, so
	// this only catches a wake that never came. The cost of a short value scales with the
	// kind count — an offline cluster has one waiting child per served kind (100-150), each
	// doing owner reads and a dependency write against the shared store, forever.
	cacheSyncConnectRetry = time.Minute
)

// cacheFilesFinalizer gates a ClusterCache's deletion on this controller deleting its
// on-disk file, so GC can't collect the row and orphan the file.
const cacheFilesFinalizer = "kstack.io/cache-files"

// ClusterCacheController reconciles ClusterCache objects: eligibility + active-identity
// gating, the cache-file finalizer/teardown, and the existence of the cache's sync children.
// It reads the parent Cluster for eligibility and declares a DependsOn edge on it.
//
// **Sync children exist for the cache's whole life; pausing is a spec write, not a delete.**
// This is the one place that evaluates the sync rule (parent sync-eligible AND this cache is
// the active identity); it pushes the result down into each child's Spec.Enabled rather than
// through the child's existence. So a pause keeps the child's history, never waits on a GC
// name release, and stopping a worker stays inside the child's own controller.
// See docs/adr/2026-08-09-beehive-control-plane.md.
//
// Status is empty — the cache measures nothing itself, and the whole-cache verdict is folded
// read-side (Service.WatchCacheSyncHealth); see
// docs/adr/2026-08-09-status-propagation-gauges.md. Its own Synced condition stays coarse.
type ClusterCacheController struct {
	coreClient beehive.Client[ClusterSpec, ClusterStatus]
	// Creates the ClusterCacheGVRDiscovery anchor and writes its Spec.Enabled pause switch.
	// A full Client, not a status-only ControllerClient, since it writes another kind's spec.
	gvrDiscoveryClient beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	cacheManager       *store.Manager

	// Serializes read-modify-write status updates from the reconcile worker.
	writeMu sync.Mutex
}

// NewClusterCacheController builds the controller from the shared runtime. rt.cacheManager
// is shared with the resolver so both see the same open DBs.
func NewClusterCacheController(rt *controllerRuntime) *ClusterCacheController {
	return &ClusterCacheController{
		coreClient:         beehive.NewClient[ClusterSpec, ClusterStatus](rt.bh, ClusterGroupKind),
		gvrDiscoveryClient: beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](rt.bh, ClusterCacheGVRDiscoveryGroupKind),
		cacheManager:       rt.cacheManager,
	}
}

// Reconcile converges one ClusterCache object toward its parent Cluster's spec.
func (c *ClusterCacheController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCacheStatus], obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (beehive.Result, error) {
	// Parent read from the owner edge, never re-parsed out of the name.
	owner, ownerExists, err := client.GetOwner(ctx, obj.ID)
	if err != nil {
		return beehive.Result{}, err
	}

	// Deletion (UID-switch prune or cluster cascade): wait for the sync children, delete the
	// file, then clear the finalizer. This branch IS cacheFilesFinalizer's handler (beehive
	// has no finalizer callback). A file-delete error returns without clearing, so the next
	// reconcile retries and the file can't be orphaned.
	//
	// **The wait is the stop-before-delete barrier.** GC's cascade marks children for
	// deletion but does not order their teardown, and each child's worker holds the cache's
	// ClusterDB handle. Each child's gvrSyncDrainFinalizer clears only once its worker
	// stopped, so "children gone" means "workers drained". Unconditional — skipping it
	// releases the row out from under a live writer.
	if obj.DeletionRequestedAt != nil {
		children, err := client.ListOwned(ctx, obj.ID)
		if err != nil {
			return beehive.Result{}, err
		}
		if len(children) > 0 {
			return beehive.Result{RequeueAfter: childRetryInterval}, nil
		}
		if !ownerExists {
			// The owner id is the file's directory, so without it there is nothing to delete
			// and clearing the finalizer would strand the .db (plus -wal/-shm) on disk.
			// Requeue: a stuck deletion is visible and recoverable, an orphan is neither.
			// Unreachable in practice (the parent outlives every child), hence the log.
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
		// Parent GC'd; this object is being cleaned up too.
		return beehive.Result{}, nil
	}

	clusterObj, err := c.coreClient.Get(ctx, owner.ID)
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			// Parent gone (GC race).
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

	// The pass's whole report is its conditions (status is empty), so settling the generation
	// explicitly is mandatory: a condition write does not advance beehive's handshake, and an
	// unchanged pass would stay owed forever. See docs/adr/2026-08-09-liveness-conditions.md.
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

// converge decides whether this cache should sync, pushes it down, and reflects it on the
// Synced condition. An inactive cache (left behind by a migration) is paused like an
// ineligible one. Children are ensured on both paths — only Spec.Enabled differs. This is
// the sole evaluation of the sync rule; children never re-derive it.
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

	// The anchor is ensured unconditionally and told whether to sync; everything below it
	// belongs to the discovery controller.
	retry := c.ensureGVRDiscovery(ctx, client, cacheObjID, enabled)

	// SyncStopped is for a user-facing pause only, never a migration hand-over (!active) the
	// user didn't ask for. This layer is the last that can tell them apart — converge
	// collapses both into one bit. The transition is read off the condition about to be
	// overwritten, not a runtime flag, so it survives a restart and can't re-fire on an
	// already-paused cache.
	if syncSwitchedOff(clusterObj) && syncWasRunning(prev) {
		if !c.recordSyncStopped(ctx, client, cacheObjID) {
			// Do NOT advance the condition: the transition is read off it, so overwriting
			// now erases the only evidence this cache had been running and loses the event
			// for good. Carry the previous one forward and come back.
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

// syncWasRunning reports the "running" half of a running→stopped transition. Keys on
// Reason, not Status: the liveness downgrade rewrites a prior process's Status to Unknown
// but leaves Reason alone, and a cache syncing when the process died really did stop.
// See docs/adr/2026-08-09-liveness-conditions.md.
func syncWasRunning(conds []Condition) bool {
	cond := FindCondition(conds, ConditionSynced)
	return cond != nil && cond.Reason == ReasonSyncing
}

// recordSyncStopped appends the pause to the cache's timeline (the one the sync-detail panel
// reads), reporting whether it landed. The caller needs that answer: the transition is
// detectable only once, so a failure must hold the condition back. Must hold writeMu.
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
	// A cancelled context is a shutdown, not a lost event.
	return ctx.Err() != nil
}

// ensureGVRDiscovery converges this cache's discovery anchor — created owned by the cache
// (so GC cascades) under a deterministic name, its Spec.Enabled carrying the pause down.
// Never deleted here; see the type comment. It needs no drain finalizer: it runs no worker,
// and its children's drain finalizers cover the subtree.
//
// GetOrCreate is atomic but never mutates an existing row, hence the follow-up Update — a
// no-op on a matching spec, so a steady re-apply writes nothing and wakes nobody.
//
// The DependsOn edge runs the other way, so the child's status writes requeue this cache (an
// owner is not woken by its children). Added once and never removed — a live dependent would
// pin a deletion-pending child under beehive's RESTRICT; GC drops it on the cache's deletion.
//
// Returns true to request a short requeue on any store error.
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

// syncSwitchedOff reports the USER having turned sync off — the two spec switches only.
// Deliberately not !syncEligible, whose presence half goes false whenever a kubeconfig
// rewrite makes a context briefly vanish, which is not a pause.
func syncSwitchedOff(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return !obj.Spec.Enabled || !obj.Spec.SyncEnabled
}
