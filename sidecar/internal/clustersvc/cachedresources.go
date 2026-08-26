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

// The ClusterCachedResource kind: one record per kind a cache mirrors. Its beehive
// shapes, the record served to resolvers, its delta-watch frame, the CachedResources
// implementation, and its controller. Mirrors the ClusterCachedResource section of
// graph/schema.graphqls.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterCachedResourceGroupKind identifies the per-GVR sync kind: one object per
// served GVR, owned by its ClusterCachedCatalog.
var ClusterCachedResourceGroupKind = beehive.GroupKind{Kind: "ClusterCachedResource"}

// ClusterCachedResourceName returns "cachedresource/{catalogObjID}/{apiVersion}/{resource}" —
// deterministic, so a discovery pass is a set reconcile with no per-child
// bookkeeping. (apiVersion, resource) rather than Kind: the plural is what the
// worker's REST path needs and what the server guarantees unique per group-version.
func ClusterCachedResourceName(catalogID beehive.ObjectID, apiVersion, resource string) string {
	return "cachedresource/" + strconv.FormatInt(int64(catalogID), 10) + "/" + apiVersion + "/" + resource
}

// EventsKind / EventsAPIVersion / EventsResource identify the Event collection — an
// ordinary synced kind, written to its own table. The server serves the same events
// under two spellings backed by one store, so exactly one may be synced: canonical
// `v1`; the discovery sweep (kubecatalog) drops the events.k8s.io spelling.
const (
	EventsKind       = "Event"
	EventsAPIVersion = "v1"
	EventsResource   = "events"
)

// ClusterCachedResourceSpec is the desired sync for one GVR, written wholly from
// above. Enabled is the pause switch relayed down the chain (the child never
// re-derives it); identity fields refresh each discovery pass, so a kind that
// changes shape converges without recreation.
type ClusterCachedResourceSpec struct {
	Enabled bool `json:"enabled"`
	// APIVersion is the group/version this kind is served at, e.g. "apps/v1" — or a bare
	// version ("v1") for the core group, matching the wire form Kubernetes uses.
	APIVersion string `json:"apiVersion"`
	// Kind is the singular Kind name, e.g. "Deployment".
	Kind string `json:"kind"`
	// Resource is the lowercase plural URL segment, e.g. "deployments".
	Resource string `json:"resource"`
	// Namespaced is true when objects of this kind live in a namespace.
	Namespaced bool `json:"namespaced"`
}

// ClusterCachedResourceStatus is the observed sync state for one GVR. Empty placeholder.
type ClusterCachedResourceStatus struct{}

// ClusterCachedResource is the view of one ClusterCachedResource beehive object: one
// Kubernetes kind being mirrored into a cache. Shaped like its sibling sync records —
// {ID, Owner, Spec, Conditions} — but streamed **cache-scoped**, because there is one per
// served kind rather than one per cache and an unscoped stream of a hundred-plus records
// would be a firehose.
type ClusterCachedResource struct {
	ID ClusterCachedResourceID
	// Owner is the ClusterCachedCatalog this kind hangs off — the discovery anchor, not
	// the cache directly, so it is the join key a client already has from the discovery
	// stream.
	Owner ObjectRef
	Spec  ClusterCachedResourceSpec
	// Conditions carry `Synced` — this kind's own verdict, which is the whole reason the
	// record is served: a cache's hundred kinds fail independently, and the coarse
	// cache-level condition can't say which.
	Conditions []Condition
}

// ClusterCachedResourceWatchFrame is one frame on the cache-scoped per-kind sync watch.
// Consumers key on Resource.ID.
type ClusterCachedResourceWatchFrame struct {
	Type     DeltaFrameType
	Resource *ClusterCachedResource
}

// SyncedKindRef identifies one synced kind exactly. The plural alone is what a UI
// renders, but it does not IDENTIFY a kind — a CRD may reuse a built-in's plural
// under another api group — so anything that keys on a kind needs the pair.
type SyncedKindRef struct {
	APIVersion string
	Resource   string
}

// toClusterCachedResource builds the served record from the stored object.
func toClusterCachedResource(obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) (*ClusterCachedResource, error) {
	owner, err := toOwnerRef(obj)
	if err != nil {
		return nil, err
	}
	return &ClusterCachedResource{
		ID:         ClusterCachedResourceID(obj.ID),
		Owner:      owner,
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}, nil
}

// toClusterCachedResources projects a whole read. beehive lists by id, which is creation
// order, and that is the order this family promises — so nothing here sorts.
func toClusterCachedResources(objs []*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) ([]*ClusterCachedResource, error) {
	resources := make([]*ClusterCachedResource, 0, len(objs))
	for _, obj := range objs {
		resource, err := toClusterCachedResource(obj)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (a cachedResourcesAPI) Get(ctx context.Context, id ClusterCachedResourceID) (*ClusterCachedResource, error) {
	obj, err := a.s.resourceClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cached resource %d: %w", id, err)
	}
	return toClusterCachedResource(obj)
}

func (a cachedResourcesAPI) List(ctx context.Context) ([]*ClusterCachedResource, error) {
	objs, err := a.s.resourceClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cached resources: %w", err)
	}
	return toClusterCachedResources(objs)
}

func (a cachedResourcesAPI) Watch(ctx context.Context, id ClusterCachedResourceID) (*Stream[ClusterCachedResourceWatchFrame], error) {
	src, err := a.s.resourceClient.Watch(ctx, beehive.ObjectID(id), loadResourceOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached resource %d: %w", id, err)
	}

	return resourceWatch.streamOne(ctx, src), nil
}

// WatchList is the fleet's largest stream by an order of magnitude — a record per served
// kind per cache. Served because the boundary fills its matrix, but a view scoped to one
// cache opens WatchByCache instead; the schema exposes only the scoped one.
func (a cachedResourcesAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedResourceWatchFrame], error) {
	src, err := a.s.resourceClient.WatchList(ctx, loadResourceOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached resources: %w", err)
	}

	return resourceWatch.streamList(ctx, src), nil
}

func (a cachedResourcesAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedResource, error) {
	catalogID, ok, err := a.s.catalogIDFor(ctx, cacheID)
	if err != nil || !ok {
		return nil, err
	}

	objs, err := a.s.resourceClient.ListOwnedObjects(ctx, catalogID, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cache %d cached resources: %w", cacheID, err)
	}
	return toClusterCachedResources(objs)
}

func (a cachedResourcesAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedResourceWatchFrame], error) {
	catalogID, ok, err := a.s.catalogIDFor(ctx, cacheID)
	if err != nil {
		return nil, err
	}
	if ok {
		src, err := a.s.resourceClient.WatchOwnedObjects(ctx, catalogID, loadResourceOwner)
		if err != nil {
			return nil, fmt.Errorf("watch cache %d cached resources: %w", cacheID, err)
		}
		return resourceWatch.streamList(ctx, src), nil
	}

	// No anchor yet. The collection is empty NOW — the bookmark says so — but a cache's
	// anchor is created by its own pass, so a client subscribing on the frame that
	// announced the cache lands here every time. Ending the stream would leave it at "no
	// kinds" for the life of the subscription.
	return a.watchWhenAnchored(ctx, cacheID), nil
}

// watchWhenAnchored bookmarks an empty snapshot, waits for the cache's anchor to be
// created, and then streams the kinds under it as ordinary changes above that snapshot.
func (a cachedResourcesAPI) watchWhenAnchored(ctx context.Context, cacheID ClusterCacheID) *Stream[ClusterCachedResourceWatchFrame] {
	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCachedResourceWatchFrame) error {
		// Its own context, cancelled the moment the anchor is known: this watch is a
		// wait, and one abandoned mid-stream leaves beehive's producer blocked on a send
		// nobody reads, holding its tailer for the life of the subscription.
		anchorCtx, stopAnchors := context.WithCancel(ctx)
		defer stopAnchors()

		// Opened before the bookmark goes out, so an anchor created in between is carried
		// by this watch rather than missed between the two.
		anchors, err := a.s.catalogClient.WatchOwnedObjects(anchorCtx, beehive.ObjectID(cacheID))
		if err != nil {
			return fmt.Errorf("watch cache %d cached catalog: %w", cacheID, err)
		}
		if !sendFrame(ctx, out, resourceWatch.bookmark) {
			return nil
		}

		catalogID, err := awaitAnchor(anchorCtx, anchors)
		if err != nil || catalogID == 0 {
			return err
		}
		// Found: nothing else is coming off that watch, and the drain below is what lets
		// its producer finish rather than block on a send.
		stopAnchors()
		drainChanges(anchors.Changes)

		src, err := a.s.resourceClient.WatchOwnedObjects(ctx, catalogID, loadResourceOwner)
		if err != nil {
			return fmt.Errorf("watch cache %d cached resources: %w", cacheID, err)
		}
		return resourceWatch.pumpChanges(ctx, out, src)
	})
}

// drainChanges empties a watch's channel until its producer closes it, so a stream this
// stream is done with ends rather than blocking on a send nobody will read.
func drainChanges[Spec, Status any](changes <-chan beehive.ObjectChange[Spec, Status]) {
	go func() {
		//nolint:revive // draining is the whole body
		for range changes {
		}
	}()
}

// awaitAnchor is the id of the cache's catalog, waited for. A zero id with no error is a
// stream that ended first — the subscription going away, or the cache with it.
func awaitAnchor(
	ctx context.Context,
	anchors *beehive.ObjectListStream[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) (beehive.ObjectID, error) {
	for _, obj := range anchors.Objects {
		if obj.DeletionRequestedAt == nil {
			return obj.ID, nil
		}
	}
	for change := range anchors.Changes {
		// An anchor on its way out is not the one to bind to: its kinds are being
		// collected, and the cache's next pass creates the one that replaces it.
		if change.Type != beehive.Deleted && change.Object != nil && change.Object.DeletionRequestedAt == nil {
			return change.Object.ID, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, nil
	}
	return 0, anchors.Err()
}

// Clear empties one kind: stop its worker and wait, drop its rows, then requeue the
// record whose own pass re-arms it. The cache-wide clear per kind — and the same
// ordering, since a worker still running would write into rows this is removing.
func (a cachedResourcesAPI) Clear(ctx context.Context, id ClusterCachedResourceID) (*ClusterCachedResource, error) {
	obj, err := a.s.resourceClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cached resource %d: %w", id, err)
	}

	cacheID, ok, err := a.cacheIDForResource(ctx, obj)
	if err != nil {
		return nil, err
	}
	// Deferred, and outside the hold: no path out of here may leave the worker stopped,
	// and the pass that arms it again is refused until the hold is released.
	//
	defer func() {
		ctx, cancel := afterClear(ctx)
		defer cancel()
		//nolint:errcheck // a lost requeue costs latency; resourceResync is the backstop
		_ = a.s.resourceClient.Requeue(ctx, beehive.ObjectID(id))
	}()

	if !ok {
		// The cache is gone, so its file went with it and there are no rows to clear.
		// The record's own name is the subject id.
		a.s.kubesyncSvc.Forget(obj.Name)
		return toClusterCachedResource(obj)
	}

	// The rows and the cookie go together, so the worker stays stopped across both: one
	// armed in between would resume its watch and never re-list the kind it is serving.
	if err := a.s.kubesyncSvc.WhileStopped(obj.Name, int64(cacheID), func() error {
		return clearKindRows(ctx, a.s.kubestoreMgr, int64(cacheID), obj.Spec)
	}); err != nil {
		return nil, fmt.Errorf("clear cached resource %d rows: %w", id, err)
	}
	return toClusterCachedResource(obj)
}

// cacheIDForResource walks a resource up to the cache holding its rows. A chain that is
// already gone is not an error: the file goes with the cache, so there is nothing left
// to clear per kind.
func (a cachedResourcesAPI) cacheIDForResource(
	ctx context.Context,
	obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus],
) (ClusterCacheID, bool, error) {
	catalogRef, ok, err := obj.Owner()
	if err != nil {
		return 0, false, fmt.Errorf("read cached resource %d owner: %w", obj.ID, err)
	}
	if !ok {
		return 0, false, nil
	}
	cacheRef, ok, err := a.s.catalogClient.GetOwner(ctx, catalogRef.ID)
	if errors.Is(err, beehive.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read cached catalog %d owner: %w", catalogRef.ID, err)
	}
	if !ok {
		return 0, false, nil
	}
	return ClusterCacheID(cacheRef.ID), true, nil
}

// loadResourceOwner eager-loads the owner edge every frame carries as its join key; beehive
// batches the lookup per change batch, so a watch does not become an N+1.
var loadResourceOwner = beehive.WithLoads(beehive.LoadOwner())

// resourceWatch projects this kind into delta frames. The departure carries the spec but no
// owner: the row is gone, so beehive loads no edge for it and reading one would fail the whole
// stream — and a consumer keys the record it is dropping by id anyway.
var resourceWatch = deltaWatch[ClusterCachedResourceSpec, ClusterCachedResourceStatus, ClusterCachedResourceWatchFrame]{
	frame: func(t DeltaFrameType, obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) (ClusterCachedResourceWatchFrame, error) {
		resource, err := toClusterCachedResource(obj)
		if err != nil {
			return ClusterCachedResourceWatchFrame{}, err
		}
		return ClusterCachedResourceWatchFrame{Type: t, Resource: resource}, nil
	},
	departed: func(change beehive.ObjectChange[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) ClusterCachedResourceWatchFrame {
		resource := &ClusterCachedResource{ID: ClusterCachedResourceID(change.ID)}
		if obj := change.Object; obj != nil {
			resource.Spec = obj.Spec
			resource.Conditions = obj.Conditions
		}
		return ClusterCachedResourceWatchFrame{Type: DeltaFrameDeleted, Resource: resource}
	},
	bookmark: ClusterCachedResourceWatchFrame{Type: DeltaFrameBookmark},
}

// resourceResyncInterval paces the fold's backstop pass; the kubesync trigger is
// what makes it prompt (see resourceResync in service.go).
const resourceResyncInterval = 10 * time.Minute

// clusterCachedResourceController reconciles one synced kind: arms the worker
// mirroring it into the cache, folds the worker's standing observation into the
// Synced verdict, and tears the worker and its rows down with the record. No pass
// dials — the sync runs on kubesync's own worker, and the trigger re-runs this fold
// when its answer moves.
type clusterCachedResourceController struct {
	// Start is overridden below; the poke subscription is released by the stop func it
	// returns, so Close stays None's.
	lifecycle.None
	// Every kind's client, not just this one's: a resource reads the catalog, cache,
	// and cluster it hangs off, and reaches the worker fleet and the store through
	// the shared services.
	deps
}

// Start subscribes to the resume poke. A poke is a fan-out, not a cascade: a machine
// waking from sleep has watches that are silently dead, and nothing in the store moves
// to say so. Every worker restarts in place off its cookie — a warm resume, not a
// rebuild — and the restarts run in turn on this goroutine, each waiting for its worker,
// which is what keeps a resume from opening a hundred connections at once.
func (c *clusterCachedResourceController) Start(context.Context) (func(context.Context) error, error) {
	if c.pokeSvc == nil {
		return func(context.Context) error { return nil }, nil
	}
	pokes, unsubscribe := c.pokeSvc.Subscribe()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range pokes {
			c.kubesyncSvc.RestartAll()
		}
	}()

	return func(ctx context.Context) error {
		unsubscribe()
		return drain.WithContext(ctx, func() { <-done })
	}, nil
}

func (c *clusterCachedResourceController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedResourceStatus],
	obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus],
) beehive.ReconcileResult {
	// A record on its way out is about to be collected with no finalizer to clear, so
	// the worker is disarmed and its rows cleared here — the other side of the
	// handshake the catalog's DiscoveryDraining requeue is waiting on.
	if obj.DeletionRequestedAt != nil {
		c.kubesyncSvc.Forget(obj.Name)

		own, err := c.resourceOwnersOf(ctx, client)
		if err != nil {
			return beehive.Fail(err)
		}
		if own.cache != nil {
			if err := clearKindRows(ctx, c.kubestoreMgr, int64(own.cache.ID), obj.Spec); err != nil {
				return beehive.Fail(fmt.Errorf("clear cached resource %d rows: %w", obj.ID, err))
			}
		}
		return beehive.Settled()
	}

	own, err := c.resourceOwnersOf(ctx, client)
	if err != nil {
		return beehive.Fail(err)
	}
	// The subtree above is being collected, which will take this resource with it —
	// and its rows with the cache's whole file, which the cache's own teardown
	// deletes. Clearing per kind here would only write into a file already going.
	if own.cluster == nil {
		c.kubesyncSvc.Forget(obj.Name)
		return beehive.Settled()
	}

	// A paused kind keeps its rows and stops syncing — disarming the worker is what
	// stops it; pause is not clear.
	if !obj.Spec.Enabled {
		c.kubesyncSvc.Forget(obj.Name)
		return observeSynced(ctx, client, obj, syncVerdict{status: ConditionFalse, reason: ReasonPaused})
	}

	contextName, err := clusterContext(own.cluster)
	if err != nil {
		// The record's own state — disabled, deleting, or credential-less — which the
		// cluster pass reports on its own conditions. Nothing can sync, so the
		// subject is dropped.
		c.kubesyncSvc.Forget(obj.Name)
		return observeSynced(ctx, client, obj, syncVerdict{
			status: ConditionFalse, reason: ReasonNoConnection, message: err.Error(),
		})
	}

	// Arming is this pass's other job: the subject exists exactly while the record
	// wants syncing, keyed by the record's own name so the worker's change signal is
	// the requeue.
	c.kubesyncSvc.Track(obj.Name, kubesync.Params{
		CacheID:     int64(own.cache.ID),
		ContextName: contextName,
		ServerUID:   own.cache.Spec.ServerUID,
		APIVersion:  obj.Spec.APIVersion,
		Kind:        obj.Spec.Kind,
		Resource:    obj.Spec.Resource,
	})

	obs, ok := c.kubesyncSvc.Read(obj.Name)
	if !ok {
		// Armed, no answer yet. The trigger re-runs this fold when one lands, and the
		// kind's resync is the backstop — so no requeue here.
		return observeSynced(ctx, client, obj, syncVerdict{status: ConditionFalse, reason: ReasonConnecting})
	}

	status, reason := ConditionFalse, obs.Reason
	switch obs.Reason {
	case kubesync.ReasonWatching:
		status, reason = ConditionTrue, ReasonWatching
	case kubesync.ReasonSyncing:
		reason = ReasonSyncing
	case kubesync.ReasonStale:
		reason = ReasonStale
	case kubesync.ReasonSyncFailed:
		reason = ReasonSyncFailed
	case kubesync.ReasonNoConnection:
		reason = ReasonNoConnection
	case kubesync.ReasonIdentityMismatch:
		reason = ReasonIdentityMismatch
	}
	return observeSynced(ctx, client, obj, syncVerdict{
		status:      status,
		reason:      reason,
		message:     obs.Message,
		resumed:     obs.Resumed,
		objectCount: obs.ObjectCount,
	})
}

// syncVerdict is what one pass concluded about a kind: the condition it writes, plus
// the two facts only the worker knows — whether this run resumed a warm cache, and how
// much it holds — which is what tells a first sync from a resync on the timeline.
type syncVerdict struct {
	status      ConditionStatus
	reason      string
	message     string
	resumed     bool
	objectCount int
}

// clearKindRows drops one kind's rows from the cache holding them. A cache with no file
// has nothing to clear — and opening one would create the very file the clear is
// removing, which is why this goes through OpenExisting rather than claiming a store
// outright.
func clearKindRows(ctx context.Context, mgr kubestoreManager, cacheID int64, spec ClusterCachedResourceSpec) error {
	store, ok, err := mgr.OpenExisting(cacheID)
	if err != nil || !ok {
		return err
	}
	defer store.Release()

	// The record's own Kind, so the teardown never has to ask the catalog which rows are
	// this kind's — that table's writer is the discovery fold, and a kind it has not
	// registered would keep every row.
	return store.ClearKind(ctx, kubestore.Kind{
		APIVersion: spec.APIVersion,
		Kind:       spec.Kind,
		Resource:   spec.Resource,
	})
}

// resourceOwners is the chain above a resource: the catalog it hangs off, the cache
// that catalog anchors, and the cluster that cache mirrors — one level deeper than the
// catalog's own owners, which is why the pass needs a cache in hand to Track (its
// ServerUID and its id) as well as the cluster's context to sync over.
type resourceOwners struct {
	catalog *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]
	cache   *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]
	cluster *beehive.Object[ClusterSpec, ClusterStatus]
}

// resourceOwnersOf walks the three owner edges above a resource. A zero resourceOwners
// with no error means something in that chain is gone or going, which is a cascade
// about to take this resource rather than a failure to retry — the catalog's ownersOf
// idiom, one level deeper.
func (c *clusterCachedResourceController) resourceOwnersOf(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedResourceStatus],
) (resourceOwners, error) {
	// The reconcile load carries no edges, so each owner is a lookup rather than a field.
	catalogRef, ok, err := client.GetOwner(ctx)
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cached resource owner: %w", err)
	}
	if !ok {
		return resourceOwners{}, nil
	}

	catalogObj, err := c.catalogClient.Get(ctx, catalogRef.ID, beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return resourceOwners{}, nil
	}
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cached catalog %d: %w", catalogRef.ID, err)
	}
	if catalogObj.DeletionRequestedAt != nil {
		return resourceOwners{}, nil
	}

	cacheRef, ok, err := catalogObj.Owner()
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cached catalog %d owner: %w", catalogRef.ID, err)
	}
	if !ok {
		return resourceOwners{}, nil
	}

	cacheObj, err := c.cacheClient.Get(ctx, cacheRef.ID, beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return resourceOwners{}, nil
	}
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cluster cache %d: %w", cacheRef.ID, err)
	}
	if cacheObj.DeletionRequestedAt != nil {
		return resourceOwners{}, nil
	}

	clusterRef, ok, err := cacheObj.Owner()
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cluster cache %d owner: %w", cacheRef.ID, err)
	}
	if !ok {
		return resourceOwners{}, nil
	}

	clusterObj, err := c.clusterClient.Get(ctx, clusterRef.ID)
	if errors.Is(err, beehive.ErrNotFound) {
		return resourceOwners{}, nil
	}
	if err != nil {
		return resourceOwners{}, fmt.Errorf("read cluster %d: %w", clusterRef.ID, err)
	}
	return resourceOwners{catalog: catalogObj, cache: cacheObj, cluster: clusterObj}, nil
}

// observeSynced records the Synced verdict, and the transition into it on the sync
// timeline. The pass writes no status of its own — this kind has none — so the
// condition, the event, and the result are the whole report.
//
// The event is the fold's to write because only a ControllerClient can write one: the
// worker knows the moment, but its answer reaches the record through this pass.
func observeSynced(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedResourceStatus],
	obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus],
	v syncVerdict,
) beehive.ReconcileResult {
	cond := LiveCondition(ConditionSynced, v.status, v.reason, v.message)
	event, moved := syncEvent(FindCondition(obj.Conditions, ConditionSynced), v)

	// Grouped, as the cluster pass groups its own: a reader must not find the event
	// without the verdict that explains it.
	if err := client.Within(ctx, func(ctx context.Context) error {
		if err := client.SetCondition(ctx, cond); err != nil {
			return fmt.Errorf("set %s condition: %w", ConditionSynced, err)
		}
		if !moved {
			return nil
		}
		if err := client.AddEvent(ctx, event); err != nil {
			return fmt.Errorf("add %s event: %w", event.Reason, err)
		}
		return nil
	}); err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled()
}

// syncEvent is the timeline entry a verdict's arrival earns, and whether it earned one.
// Only a MOVE records: the fold runs on every resync and on every neighbouring change,
// so a healthy steady state must stay silent.
//
// The start/complete pairs are cold and warm — a resume is a different story from a
// first build, and the message is what makes it readable weeks later.
func syncEvent(prev *Condition, v syncVerdict) (beehive.EventSpec, bool) {
	if prev != nil && prev.Reason == v.reason {
		return beehive.EventSpec{}, false
	}
	event := beehive.EventSpec{Category: SyncEventCategory, Type: beehive.EventNormal}

	switch v.reason {
	case ReasonSyncing:
		event.Reason = ReasonSyncStart
		if v.resumed {
			event.Reason = ReasonResyncStart
			event.Message = fmt.Sprintf("resuming a cache holding %d objects", v.objectCount)
		}
	case ReasonWatching:
		// Caught up. Which milestone it is depends on where it came from: a build that
		// had nothing completes a first sync, a resume completes a resync, and a watch
		// that merely proved itself alive again reports no counts at all.
		switch {
		case prev != nil && prev.Reason == ReasonStale:
			event.Reason = ReasonResyncComplete
			event.Message = "the watch is proving itself alive again"
		case v.resumed:
			event.Reason = ReasonResyncComplete
			event.Message = fmt.Sprintf("caught up with %d objects cached", v.objectCount)
		default:
			event.Reason = ReasonSyncComplete
			event.Message = fmt.Sprintf("cached %d objects", v.objectCount)
		}
	case ReasonSyncFailed:
		event.Reason, event.Type = ReasonSyncDegraded, beehive.EventWarning
		event.Message = v.message
	case ReasonStale:
		event.Reason, event.Type = ReasonSyncStale, beehive.EventWarning
		event.Message = v.message
	case ReasonPaused, ReasonNoConnection:
		// The two branches that disarm the worker. A kind that stopped syncing because
		// it was paused, or because its cluster cannot be reached, has not failed — but
		// one whose first pass finds it already off never started, and the timeline is
		// for what happened.
		if prev == nil {
			return beehive.EventSpec{}, false
		}
		event.Reason = ReasonSyncStopped
		event.Message = v.message
	default:
		// Connecting, IdentityMismatch: waits rather than transitions. The condition
		// carries them; the timeline is for what happened.
		return beehive.EventSpec{}, false
	}
	return event, true
}
