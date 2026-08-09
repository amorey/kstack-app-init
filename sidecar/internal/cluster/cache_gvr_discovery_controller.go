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
	// gvrDiscoveryInterval paces the steady-state re-discovery of a cache's kinds. The
	// discovery surface cannot be watched — its document carries no resourceVersion, so
	// there is no replayable position to resume from — which makes this poll the only way
	// a newly installed CRD (or a removed one) is noticed; the interval is the worst-case
	// lag before a new kind gains a sync child. A pass is one aggregated request against
	// any server new enough to serve APIGroupDiscoveryList (client-go negotiates it, and
	// falls back to a request per group-version otherwise).
	gvrDiscoveryInterval = 5 * time.Minute

	// gvrDiscoveryTimeout bounds one discovery pass. It is applied to the REST config
	// rather than through the context because client-go's discovery surface takes no
	// context; without it an unresponsive aggregated APIService could hold a reconcile
	// worker for the transport's default.
	gvrDiscoveryTimeout = 30 * time.Second
)

// ClusterCacheGVRDiscoveryController reconciles ClusterCacheGVRDiscovery objects: it asks the
// cluster API which GVRs it serves and converges the set of ClusterCacheGVRSync children onto
// that answer — one child per syncable GVR, each reconciled independently by
// ClusterCacheGVRSyncController.
//
// **The child set is a projection of the API server's answer, so it is a set reconcile, not
// an append.** Each pass creates the children a kind gained, refreshes the identity of the
// ones that remain, and deletes the ones whose kind the cluster no longer serves (a CRD was
// uninstalled). Deletion is right here and a pause is not: a removed kind has no objects to
// mirror and no history worth keeping, whereas a pause is temporary — which is why pausing
// travels as Spec.Enabled on children that stay put.
//
// **A partial answer never prunes.** An unavailable aggregated APIService fails its own
// group while the rest answer — as a per-group error on the legacy path, and as a
// Stale-freshness group-version in the aggregated document — and client-go surfaces either
// as ErrGroupDiscoveryFailed *alongside usable results*. Treating "this group did not
// answer" as "this group is gone" would delete a live kind's sync child (and, once the sync
// worker lands, its cached objects) on a transient outage, so a partial pass only adds and
// refreshes, and reports itself as partial.
//
// It resolves its inputs by climbing the owner chain (this object → its ClusterCache →
// the Cluster whose id keys the credentials) and adding a DependsOn edge to that Cluster,
// so the core controller's status write on a successful probe wakes a discovery pass that
// was waiting for credentials.
type ClusterCacheGVRDiscoveryController struct {
	// cacheClient reads the parent ClusterCache's owner edge (the second hop of the climb).
	// A full Client because GetOwner for another kind isn't on our own ControllerClient.
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	// gvrSyncClient creates, refreshes and deletes the per-GVR children. A full Client (not
	// the status-write ControllerClient) because this controller owns their spec, mirroring
	// how ClusterCacheController drives eventsSyncClient.
	gvrSyncClient beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	connMgr       *ConnectionManager
	// cacheManager reaches the cache's own tables for the orphan sweep — Lookup only, never
	// Open: a cache nobody has opened holds nothing to sweep, and opening one here would
	// materialize a file for a cluster that isn't syncing.
	cacheManager *store.Manager
	// policies hands out each cache's shared client budget; see cacheClientPolicy.
	policies *cacheClientPolicies

	// newDiscovery builds the discovery client for one pass; the package's own tests swap
	// in a fake so the set reconcile is exercised without a cluster.
	newDiscovery newDiscoveryFunc

	// mu guards stats.
	mu sync.Mutex
	// stats holds each discovery object's last-pass gauges, keyed by its ObjectID.
	// In memory rather than in the object's status because nothing in the object graph
	// reacts to them — see ClusterCacheGVRDiscoveryStatus for why that distinction, not
	// durability, is what decides where a value lives.
	stats map[beehive.ObjectID]ClusterCacheGVRDiscoveryStats
}

// resourceLister is the slice of client-go's discovery surface this controller uses — the
// seam the tests fake.
type resourceLister interface {
	// ServerPreferredResources returns one entry per served resource at the group's
	// preferred version. It may return partial results WITH an error.
	ServerPreferredResources() ([]*metav1.APIResourceList, error)
}

// newDiscoveryFunc builds a resourceLister for one cluster's credentials.
type newDiscoveryFunc func(cfg *rest.Config) (resourceLister, error)

// newLiveDiscovery is the production constructor. The config it is handed already carries
// the cache's shared client budget (the caller applies cacheClientPolicy.config, so the
// walk draws on the same token bucket as that cache's kind workers rather than competing
// with them); this only bounds the pass with a request timeout, on its own copy so the
// deadline can't leak back onto the shared config.
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

// Stats returns the last pass's gauges for one discovery object, or ok=false when this
// process has run no pass for it yet (a fresh start, or before its first pass). Read on
// request by the service; never stored.
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
		// Some kinds' names are still held by children draining from an earlier prune, so
		// their replacements could not be created. The kind list is right, the child set
		// is not yet — say so rather than reporting Discovered on a set that is missing
		// workers.
		//
		// The retry is beehive's, not a fixed requeue: every pass costs a full
		// ServerPreferredResources walk plus a ListOwnedObjects over the cache's ~150
		// children, and a drain that keeps timing out (workerStopTimeout is 10s, and a
		// cache cold-syncing its kinds through one writer connection can take longer) would
		// otherwise spend one of the discovery worker slots on one discovery request per
		// second, indefinitely. Backoff makes a quick drain just as responsive and a wedged
		// one cheap.
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
	// A steady pass writes nothing: the condition is unchanged, so beehive suppresses it,
	// and this kind has no status. That is the point — a cache whose kinds haven't moved
	// does not wake its dependents every interval.
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
		// A group-discovery failure carries usable partial results; anything else is a
		// total failure and lists is meaningless.
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return nil, false, err
		}
		partial = true
	}
	return syncableGVRs(lists), partial, nil
}

// syncableGVRs reduces a discovery answer to the GVRs this cache should mirror, as the specs
// their children carry. It drops:
//
//   - subresources ("pods/log"), which are not listable collections;
//   - anything the API server won't let us list AND watch, which is what a sync worker needs
//     (e.g. SubjectAccessReview, a create-only endpoint);
//   - the NON-canonical Event spelling. Events are synced like any other kind, but the api
//     server serves one underlying store under both `v1` and `events.k8s.io/v1`, so keeping
//     both would give one collection two workers fighting over the same uid-keyed rows.
//     Only `v1/events` survives — see isAltEventsKind, which keys the drop on the api group
//     and plural, the same pair isEventsKind routes on.
//
// Enabled is left false here; the caller stamps the parent's intent onto it.
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

// syncChildren converges this object's ClusterCacheGVRSync children onto the discovered set:
// it refreshes the ones that survive, deletes the ones whose kind is gone (only when prune is
// set), and creates the rest. Children are matched by their deterministic name, so the comparison needs no
// per-child bookkeeping.
// syncChildren returns this cache's children — the ones it read PLUS the ones it created
// (so a caller needing the same view doesn't load several hundred objects a second time) —
// and how many wanted kinds could NOT be created because a child still draining holds their
// name. The latter is not an error — the deletion
// is progressing and the next pass takes them — but not converged either, which the caller
// must not report as Discovered.
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
		// The spec compare is ours, not beehive's: beehive suppresses the write on
		// matching bytes, but the call is still a load + marshal + transaction per child,
		// and the steady state is a few hundred children that all match.
		if child.Spec == spec {
			continue
		}
		if _, err := c.gvrSyncClient.Update(ctx, child.ID, spec); err != nil {
			return nil, 0, fmt.Errorf("update gvr-sync child: %w", err)
		}
	}

	for name, spec := range wanted {
		// GetOrCreate rather than Create: the name may still be held by a child draining
		// from an earlier prune (a CRD reinstalled, say). The drain finalizer is what makes
		// that prune wait for the kind's worker to stop before the row is collected — see
		// gvrSyncDrainFinalizer.
		child, _, err := c.gvrSyncClient.GetOrCreate(ctx, name, spec,
			beehive.WithOwner(discoveryID),
			beehive.WithFinalizers(gvrSyncDrainFinalizer))
		if err != nil {
			return nil, 0, fmt.Errorf("create gvr-sync child: %w", err)
		}
		// A child created here belongs in the returned set as much as one that was already
		// there. Leaving it out told the orphan sweep this kind had no child at all — and
		// its worker starts the moment the child is created, so the sweep dropped the rows,
		// catalog entry and resume cookie of a kind actively syncing into them. That is the
		// one thing "no child means no worker" was supposed to rule out.
		existing = append(existing, child)
		// What came back is the row on its way out, not a child for this kind. Nothing can
		// be done until it is collected and the name frees; count it so the caller comes
		// back rather than reporting a converged set that is missing this worker.
		if child.DeletionRequestedAt != nil {
			held++
		}
	}
	return existing, held, nil
}

// sweepOrphanedKinds drops cached rows for kinds this cache no longer has a child for.
//
// The per-kind Forget on a child's deletion is the ordinary reaper and stays the primary
// one — it runs after that kind's worker has drained, which is the safe moment. But it
// cannot cover the case where the cluster changed while the app was down: on the next
// start, discovery prunes the child of an uninstalled CRD, and Forget then finds no OPEN
// cache to clean (it uses Lookup, deliberately, so a teardown can't re-materialize a file).
// The rows, edges, catalog entry and resume cookie then survive forever, with the dashboard
// nav listing a kind the cluster no longer serves.
//
// It runs only after a COMPLETE discovery, and forgets only kinds with NO child at all —
// not even a deletion-pending one. That is what makes it safe without any coordination: no
// child means no worker, so nothing can be mid-write into the rows being dropped.
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

// ensureEventsChild creates or updates the Event kind's sync child. Unlike syncChildren's
// converge it runs on every pass, including the ones that never reach the cluster.
//
// The Update is not belt-and-braces: GetOrCreate reads-or-creates but never mutates, and
// the first pass for a fresh cache usually runs before its cluster has connected — so the
// child is seeded disabled and would stay that way until a pass got round to it. It is
// guarded by the same spec compare syncChildren uses, since this seed runs on every pass and
// the steady state is that the spec already matches: beehive suppresses the write on matching
// bytes, but the call is still a load + marshal + transaction.
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
		// Same blind spot as syncChildren: a child being deleted is not one to pause. Its
		// worker drains as part of the deletion, and the write would only churn a row on
		// its way out.
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
