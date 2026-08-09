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
	"time"

	"github.com/amorey/beehive"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

const (
	// Steady-state re-discovery cadence, and the worst-case lag before a new CRD gains a sync
	// child. A poll because discovery cannot be watched — its document carries no
	// resourceVersion, so there is no position to resume from.
	gvrDiscoveryInterval = 5 * time.Minute

	// Bounds one pass. Applied to the REST config, not the context, because client-go's
	// discovery surface takes none — without it an unresponsive APIService holds a worker.
	gvrDiscoveryTimeout = 30 * time.Second
)

// ClusterCacheGVRDiscoveryController asks the cluster which GVRs it serves and converges the
// set of ClusterCacheGVRSync children onto that answer, one per syncable GVR.
//
// **The child set is a projection of the answer, so a pass is a set reconcile, not an
// append**: create what appeared, refresh what remains, delete what the cluster no longer
// serves. Deletion belongs here where a pause does not — an uninstalled CRD has no objects
// to mirror, whereas a pause is temporary and travels as Spec.Enabled on children that stay.
//
// **A partial answer never prunes.** client-go surfaces an unavailable APIService as
// ErrGroupDiscoveryFailed alongside usable results, so a partial pass adds and refreshes
// only; treating "did not answer" as "is gone" would delete a live kind's child and its
// cached objects on a transient outage.
//
// Inputs resolve by climbing the owner chain to the Cluster, with a DependsOn edge on it so
// a successful probe wakes a pass that was waiting for credentials.
type ClusterCacheGVRDiscoveryController struct {
	// Reads the parent ClusterCache's owner edge. A full Client since GetOwner for another
	// kind isn't on our ControllerClient.
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	// Creates, refreshes and deletes the per-GVR children; a full Client since this
	// controller owns their spec.
	gvrSyncClient beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	connMgr       *ConnectionManager
	// Lookup only, never Open: opening here would materialize a file for a cluster that
	// isn't syncing. See docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
	cacheManager *store.Manager
	// Each cache's shared client budget; see cacheClientPolicy.
	policies *cacheClientPolicies

	// Swapped for a fake in tests so the set reconcile runs without a cluster.
	newDiscovery newDiscoveryFunc

	mu sync.Mutex
	// Last-pass gauges per object. In memory, not status — nothing in the object graph
	// reacts to them; see docs/adr/2026-08-09-status-propagation-gauges.md.
	stats map[beehive.ObjectID]ClusterCacheGVRDiscoveryStats
}

// resourceLister is the slice of client-go's discovery surface used here — the test seam.
type resourceLister interface {
	// May return partial results WITH an error.
	ServerPreferredResources() ([]*metav1.APIResourceList, error)
}

// newDiscoveryFunc builds a resourceLister for one cluster's credentials.
type newDiscoveryFunc func(cfg *rest.Config) (resourceLister, error)

// newLiveDiscovery is the production constructor. The config already carries the cache's
// shared client budget; this only adds a request timeout, on its own copy so the deadline
// can't leak back onto the shared config.
func newLiveDiscovery(cfg *rest.Config) (resourceLister, error) {
	bounded := rest.CopyConfig(cfg)
	bounded.Timeout = gvrDiscoveryTimeout
	return discovery.NewDiscoveryClientForConfig(bounded)
}

// NewClusterCacheGVRDiscoveryController builds the controller from the shared runtime.
func NewClusterCacheGVRDiscoveryController(rt *controllerRuntime) *ClusterCacheGVRDiscoveryController {
	return &ClusterCacheGVRDiscoveryController{
		cacheClient:   beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](rt.bh, ClusterCacheGroupKind),
		gvrSyncClient: beehive.NewClient[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus](rt.bh, ClusterCacheGVRSyncGroupKind),
		connMgr:       rt.connMgr,
		cacheManager:  rt.cacheManager,
		policies:      rt.policies(),
		newDiscovery:  newLiveDiscovery,
		stats:         make(map[beehive.ObjectID]ClusterCacheGVRDiscoveryStats),
	}
}

// Stats returns the last pass's gauges, ok=false when this process has run none yet. Read
// on request by the service; never stored.
func (c *ClusterCacheGVRDiscoveryController) Stats(objID beehive.ObjectID) (ClusterCacheGVRDiscoveryStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.stats[objID]
	return st, ok
}

// recordPass stamps the gauges for a pass that reached the API server.
func (c *ClusterCacheGVRDiscoveryController) recordPass(objID beehive.ObjectID, resourceCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats[objID] = ClusterCacheGVRDiscoveryStats{
		LastDiscoveryAt: time.Now().UTC(),
		ResourceCount:   resourceCount,
	}
}

// forgetStats drops a collected object's gauges, so the map can't outlive the objects it
// describes.
func (c *ClusterCacheGVRDiscoveryController) forgetStats(objID beehive.ObjectID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.stats, objID)
}

// Reconcile converges one ClusterCacheGVRDiscovery object: while it is enabled and its
// cluster has credentials, the served GVRs are re-read and the child set converged onto them.
func (c *ClusterCacheGVRDiscoveryController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheGVRDiscoveryStatus],
	obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus],
) (beehive.Result, error) {
	// Deletion (the cache's GC cascade): nothing to unwind here. This controller holds no
	// process-scoped state — no worker, no open cache handle — so it carries no drain
	// finalizer, and GC cascades to the children on its own. The cache's stop-before-delete
	// barrier still covers them: GC refuses to collect a row with owned children, so this
	// object outlives its ClusterCacheGVRSync children and the cache's wait for its own
	// children to be gone transitively waits for theirs.
	if obj.DeletionRequestedAt != nil {
		c.forgetStats(obj.ID)
		return beehive.Result{}, nil
	}

	// Seed the Event child before anything that can fail or wait. Events are an ordinary
	// per-kind sync, and a pass would create this one from the discovery answer anyway —
	// but only after reaching the API server. Events are the highest-value diagnostic data
	// in the cache, so they must not wait on that: on a cluster whose discovery is slow,
	// throttled, or still waiting for credentials, this is the difference between mirroring
	// events and mirroring nothing. The pass converges the same child by the same
	// deterministic name, so the two are idempotent against each other.
	if err := c.ensureEventsChild(ctx, obj.ID, obj.Spec.Enabled); err != nil {
		return beehive.Result{}, err
	}

	if !obj.Spec.Enabled {
		// Relay the pause and stop there: the children stay, holding the last-known kind
		// list, so an unpause resumes without waiting out a discovery pass.
		if err := c.pauseChildren(ctx, obj.ID); err != nil {
			return beehive.Result{}, err
		}
		return beehive.Result{}, c.writeGate(ctx, client, obj, ReasonPaused, "GVR discovery is paused")
	}

	// The cache locator is discarded: this controller opens no cache handle of its own —
	// it only needs the Cluster the credentials are keyed by. Its children open the cache.
	cacheRef, clusterObjID, err := resolveCacheChain(ctx, client, c.cacheClient, obj.ID)
	if err != nil {
		return beehive.Result{}, err
	}
	if clusterObjID == 0 {
		// The owner chain is gone (a cascade in flight); our object is being cleaned up too.
		return beehive.Result{}, nil
	}

	// Depend on the Cluster so its status write on a successful probe — the same converge
	// that fills the ConnectionManager — wakes us.
	if err := client.AddDependency(ctx, obj.ID, clusterObjID); err != nil {
		return beehive.Result{}, err
	}

	restCfg, _ := c.connMgr.Get(ClusterID(clusterObjID))
	if restCfg == nil {
		// Enabled but the cluster hasn't connected yet (or lost its credentials). A normal
		// startup state, not a failure — the children keep whatever list they have.
		err := c.writeGate(ctx, client, obj, ReasonNoConnection, "Waiting for a connection to the cluster")
		return beehive.Result{RequeueAfter: cacheSyncConnectRetry}, err
	}

	// Draw on this cache's shared client budget, so a discovery walk and the cache's kind
	// workers can't each spend a full ceiling.
	resources, partial, err := c.discover(c.policies.get(cacheRef.CacheID).config(restCfg))
	if err != nil {
		// Nothing was learned this pass, so the children are left exactly as they are.
		// Returning the error hands the retry to beehive's backoff.
		if condErr := c.writeGate(ctx, client, obj, ReasonDiscoveryFailed, err.Error()); condErr != nil {
			return beehive.Result{}, errors.Join(err, condErr)
		}
		return beehive.Result{}, fmt.Errorf("discover served GVRs: %w", err)
	}

	// A partial answer adds and refreshes but never prunes — see the type comment.
	children, held, err := c.syncChildren(ctx, obj.ID, resources, !partial)
	if err != nil {
		return beehive.Result{}, err
	}
	if !partial {
		c.sweepOrphanedKinds(ctx, cacheRef, children)
	}
	if held > 0 {
		// Draining children still hold some names, so their replacements couldn't be created:
		// the kind list is right, the child set isn't yet. Retry via beehive's backoff rather
		// than a fixed requeue — each pass costs a full discovery walk plus a list over ~150
		// children, and a repeatedly timing-out drain would otherwise spin a worker slot.
		c.recordPass(obj.ID, len(resources))
		if condErr := c.writeGate(ctx, client, obj, ReasonDiscoveryDraining,
			"Waiting for replaced kinds to finish draining"); condErr != nil {
			return beehive.Result{}, condErr
		}
		return beehive.Result{}, fmt.Errorf("%d replaced kind(s) still draining", held)
	}
	// The gauges are ours to keep, not the store's to propagate.
	c.recordPass(obj.ID, len(resources))

	cond := liveCondition(ConditionDiscovered, ConditionTrue, ReasonDiscovered, "")
	if partial {
		cond = liveCondition(ConditionDiscovered, ConditionFalse, ReasonDiscoveryPartial,
			"Some api groups did not respond — the kind list may be incomplete")
	}
	// A steady pass writes nothing (unchanged condition, no status), so a cache whose kinds
	// haven't moved doesn't wake its dependents every interval.
	return beehive.Result{RequeueAfter: gvrDiscoveryInterval}, reportCondition(ctx, client, obj.ID, obj.Generation, cond)
}

// discover reads the API server's served resources and reduces them to the GVRs worth
// syncing. partial is true when some api groups answered and others did not.
func (c *ClusterCacheGVRDiscoveryController) discover(restCfg *rest.Config) (resources []ClusterCacheGVRSyncSpec, partial bool, err error) {
	cl, err := c.newDiscovery(restCfg)
	if err != nil {
		return nil, false, err
	}
	lists, err := cl.ServerPreferredResources()
	if err != nil {
		// A group-discovery failure carries usable partial results; anything else is total.
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return nil, false, err
		}
		partial = true
	}
	return syncableGVRs(lists), partial, nil
}

// syncableGVRs reduces a discovery answer to the GVRs worth mirroring. It drops
// subresources, anything not both list- and watch-able (e.g. SubjectAccessReview), and the
// NON-canonical Event spelling — one collection served under both `v1` and `events.k8s.io/v1`
// would get two workers fighting over the same uid-keyed rows. See
// docs/adr/2026-08-09-kubesync-watch-poll.md.
//
// Enabled is left false; the caller stamps the parent's intent onto it.
func syncableGVRs(lists []*metav1.APIResourceList) []ClusterCacheGVRSyncSpec {
	total := 0
	for _, list := range lists {
		if list != nil {
			total += len(list.APIResources)
		}
	}
	out := make([]ClusterCacheGVRSyncSpec, 0, total)
	for _, list := range lists {
		if list == nil {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			if isAltEventsKind(list.GroupVersion, r.Name) {
				continue
			}
			if !slices.Contains(r.Verbs, "list") || !slices.Contains(r.Verbs, "watch") {
				continue
			}
			out = append(out, ClusterCacheGVRSyncSpec{
				APIVersion: list.GroupVersion,
				Kind:       r.Kind,
				Resource:   r.Name,
				Namespaced: r.Namespaced,
			})
		}
	}
	return out
}

// syncChildren converges this object's children onto the discovered set: refresh survivors,
// delete kinds that are gone (only when prune is set), create the rest. Matched by
// deterministic name, so the comparison needs no per-child bookkeeping.
//
// Returns the children read PLUS those created (so a caller needing the same view doesn't
// reload several hundred objects), and how many kinds could NOT be created because a
// draining child still holds their name — not an error, but not converged either, so the
// caller must not report Discovered.
func (c *ClusterCacheGVRDiscoveryController) syncChildren(
	ctx context.Context,
	discoveryID beehive.ObjectID,
	desired []ClusterCacheGVRSyncSpec,
	prune bool,
) (existing []*beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus], held int, err error) {
	wanted := make(map[string]ClusterCacheGVRSyncSpec, len(desired))
	for _, spec := range desired {
		// Enabled is true for every child here: this path is only reached from the
		// enabled branch of Reconcile, and the paused branch relays through pauseChildren.
		spec.Enabled = true
		wanted[ClusterCacheGVRSyncName(discoveryID, spec.APIVersion, spec.Resource)] = spec
	}

	existing, err = c.gvrSyncClient.ListOwnedObjects(ctx, discoveryID)
	if err != nil {
		return nil, 0, fmt.Errorf("list gvr-sync children: %w", err)
	}
	for _, child := range existing {
		// A child on its way out is not a live child. Letting it satisfy a wanted kind was
		// the bug: the name was struck off the wanted set, so no replacement was created,
		// the dying row was pointlessly updated, and the pass reported Discovered=True for
		// a kind with no worker and none coming. Leaving it in wanted sends it to the
		// GetOrCreate below, which is written for a name a deleting row still holds.
		// Pruning it again is equally pointless — the deletion is already requested.
		if child.DeletionRequestedAt != nil {
			continue
		}
		spec, keep := wanted[child.Name]
		if !keep {
			if prune {
				if err := c.gvrSyncClient.Delete(ctx, child.ID); err != nil && !errors.Is(err, beehive.ErrNotFound) {
					return nil, 0, fmt.Errorf("delete gvr-sync child: %w", err)
				}
			}
			continue
		}
		delete(wanted, child.Name)
		// Our own spec compare: beehive suppresses the write on matching bytes, but the call
		// is still a load + marshal + transaction per child, and in steady state all match.
		if child.Spec == spec {
			continue
		}
		if _, err := c.gvrSyncClient.Update(ctx, child.ID, spec); err != nil {
			return nil, 0, fmt.Errorf("update gvr-sync child: %w", err)
		}
	}

	for name, spec := range wanted {
		// GetOrCreate, not Create: a child draining from an earlier prune may still hold the
		// name (a reinstalled CRD).
		child, _, err := c.gvrSyncClient.GetOrCreate(ctx, name, spec,
			beehive.WithOwner(discoveryID),
			beehive.WithFinalizers(gvrSyncDrainFinalizer))
		if err != nil {
			return nil, 0, fmt.Errorf("create gvr-sync child: %w", err)
		}
		// A child created here MUST be in the returned set: its worker starts immediately, and
		// omitting it would tell the orphan sweep this kind has no child, dropping the rows of
		// a kind actively syncing into them.
		existing = append(existing, child)
		// A row on its way out, not a usable child; count it so the caller comes back rather
		// than reporting a converged set that is missing this worker.
		if child.DeletionRequestedAt != nil {
			held++
		}
	}
	return existing, held, nil
}

// sweepOrphanedKinds drops cached rows for kinds with no child left.
//
// The per-kind Forget on a child's deletion stays the primary reaper, but it can't cover a
// cluster that changed while the app was down: the next start prunes the CRD's child, and
// Forget finds no OPEN cache to clean (Lookup, never Open, so teardown can't re-materialize
// a file), stranding the rows and catalog entry forever.
//
// Runs only after a COMPLETE discovery, and only for kinds with no child at all — not even a
// deletion-pending one — so no worker can be mid-write into the rows being dropped.
func (c *ClusterCacheGVRDiscoveryController) sweepOrphanedKinds(
	ctx context.Context,
	ref store.CacheRef,
	children []*beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus],
) {
	cdb := c.cacheManager.Lookup(ref.CacheID)
	if cdb == nil {
		return // not open, so it holds nothing this pass could sweep
	}
	// Deletion-pending children count as present: their workers may still be draining.
	haveChild := make(map[string]bool, len(children))
	for _, child := range children {
		haveChild[child.Spec.APIVersion+"/"+child.Spec.Resource] = true
	}

	cached, err := cdb.Kinds(ctx)
	if err != nil {
		slog.Warn("gvrdiscoverycontroller: read kind catalog for orphan sweep", "cache", ref.CacheID, "err", err)
		return
	}
	for _, row := range cached {
		kind := objectsync.Kind{
			APIVersion: row.APIVersion,
			Kind:       row.Kind,
			Resource:   row.Resource,
			Namespaced: row.Scope == "Namespaced",
		}
		if haveChild[row.APIVersion+"/"+row.Resource] {
			continue
		}
		if err := forgetKindRows(ctx, cdb, kind); err != nil {
			slog.Warn("gvrdiscoverycontroller: forget orphaned kind",
				"cache", ref.CacheID, "kind", row.Kind, "resource", row.Resource, "err", err)
			continue
		}
		slog.Info("gvrdiscoverycontroller: forgot a kind with no sync child",
			"cache", ref.CacheID, "apiVersion", row.APIVersion, "resource", row.Resource)
	}
}

// ensureEventsChild seeds the Event kind's sync child, on every pass including those that
// never reach the cluster — events are the highest-value diagnostic data, so this runs ahead
// of anything that can fail or wait. See docs/adr/2026-08-09-kubesync-watch-poll.md.
//
// The Update is load-bearing: GetOrCreate never mutates, and a fresh cache's first pass
// usually runs before its cluster connects, so the child would stay seeded disabled. Guarded
// by the same spec compare syncChildren uses, since the steady state already matches.
func (c *ClusterCacheGVRDiscoveryController) ensureEventsChild(
	ctx context.Context,
	discoveryID beehive.ObjectID,
	enabled bool,
) error {
	spec := eventsSyncSpec(enabled)
	name := ClusterCacheGVRSyncName(discoveryID, spec.APIVersion, spec.Resource)
	obj, created, err := c.gvrSyncClient.GetOrCreate(ctx, name, spec,
		beehive.WithOwner(discoveryID),
		beehive.WithFinalizers(gvrSyncDrainFinalizer))
	if err != nil {
		return fmt.Errorf("ensure Event sync child: %w", err)
	}
	if created || obj.Spec == spec {
		return nil
	}
	if _, err := c.gvrSyncClient.Update(ctx, obj.ID, spec); err != nil {
		return fmt.Errorf("update Event sync child: %w", err)
	}
	return nil
}

// pauseChildren switches every existing child off, leaving their GVR identity untouched.
// The paused path's counterpart to syncChildren, which has no discovery answer to converge
// against — so the children keep the last-known kind list and an unpause resumes without
// waiting out a pass.
func (c *ClusterCacheGVRDiscoveryController) pauseChildren(ctx context.Context, discoveryID beehive.ObjectID) error {
	children, err := c.gvrSyncClient.ListOwnedObjects(ctx, discoveryID)
	if err != nil {
		return fmt.Errorf("list gvr-sync children: %w", err)
	}
	for _, child := range children {
		// A child being deleted is not one to pause — its worker drains with the deletion.
		if child.DeletionRequestedAt != nil {
			continue
		}
		if !child.Spec.Enabled {
			continue
		}
		spec := child.Spec
		spec.Enabled = false
		if _, err := c.gvrSyncClient.Update(ctx, child.ID, spec); err != nil {
			return fmt.Errorf("update gvr-sync child: %w", err)
		}
	}
	return nil
}

// writeGate records a Discovered condition for a pass that learned nothing about the served
// kinds — paused, waiting on a connection, or a failed request. The gauges are left alone:
// a pass that didn't reach the API server hasn't re-measured anything.
func (c *ClusterCacheGVRDiscoveryController) writeGate(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheGVRDiscoveryStatus],
	obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus],
	reason, message string,
) error {
	cond := liveCondition(ConditionDiscovered, ConditionFalse, reason, message)
	return reportCondition(ctx, client, obj.ID, obj.Generation, cond)
}
