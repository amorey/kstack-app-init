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

// The ClusterCachedCatalog kind: one discovery anchor per cache, naming the kinds
// that cache's cluster serves. Its beehive shapes, the record served to resolvers,
// its delta-watch frame, the CachedCatalogs implementation, and its controller.
// Mirrors the ClusterCachedCatalog section of graph/schema.graphqls.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubecatalog"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterCachedCatalogGroupKind identifies the discovery anchor kind: one
// object per ClusterCache, owned by it; its controller maintains one
// ClusterCachedResource child per served GVR.
var ClusterCachedCatalogGroupKind = beehive.GroupKind{Kind: "ClusterCachedCatalog"}

// ClusterCachedCatalogName returns "cachedcatalog/{cacheObjID}" — exactly one
// per cache, so creation is idempotent under name-uniqueness dedup. A
// creation/dedup key only; the child is enumerated through the owner edge.
func ClusterCachedCatalogName(cacheID beehive.ObjectID) string {
	return "cachedcatalog/" + strconv.FormatInt(int64(cacheID), 10)
}

// ClusterCachedCatalogSpec is the desired discovery for one cache. Enabled is
// the pause switch, evaluated once above and relayed into each child. Existence
// means "has an anchor", NOT "is discovering" — the object lives as long as the
// cache, so its subtree survives a pause.
type ClusterCachedCatalogSpec struct {
	Enabled bool `json:"enabled"`
}

// ClusterCachedCatalogStatus is empty, deliberately: status is a propagation
// channel — for state a dependent reacts to, not for a discovery pass's gauges,
// which nothing in the object graph reads. The Discovered condition is what remains.
type ClusterCachedCatalogStatus struct{}

// ClusterCachedCatalog is the view of one ClusterCachedCatalog beehive object: the anchor a
// cache's discovery runs against. It carries the pause switch its cache pushed down and the
// Discovered verdict — the kinds the sweep found are its ClusterCachedResource children, one
// per kind, and when a sweep last answered is deliberately nowhere, since a timestamp on a
// record re-emits it to every watcher. Streamed standalone via CachedCatalogs().Watch and
// joined onto its cache client-side by Owner.ID. Spec is the stored value served as-is, no
// projection.
type ClusterCachedCatalog struct {
	ID ClusterCachedCatalogID
	// Owner is the ClusterCache this catalog belongs to.
	Owner ObjectRef
	Spec  ClusterCachedCatalogSpec
	// Conditions are beehive object conditions, read off the object rather than out of
	// the status blob — `Discovered`, carrying this component's own verdict. There is no
	// Status field: the kind's status is empty by design.
	Conditions []Condition
}

// ClusterCachedCatalogWatchFrame is one frame on the GVR-discovery watch, the third of
// the parallel object streams (clusters, caches, gvr-discoveries). Binds
// 1:1 to the GraphQL ClusterCachedCatalogWatchFrame; consumers key on Catalog.ID.
type ClusterCachedCatalogWatchFrame struct {
	Type    DeltaFrameType
	Catalog *ClusterCachedCatalog
}

// ensureClusterCachedCatalog gives one cache its discovery anchor, owned by the cache
// so beehive's GC cascades to it, and converges the pause switch onto it. Idempotent:
// the name is the dedup key, and a spec already in the desired state writes nothing.
//
// Called by the cache's reconcile, which is where the pause switch is evaluated; the
// writes live here so the kind's vocabulary stays in the kind's file.
func ensureClusterCachedCatalog(ctx context.Context, client beehive.Client[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus], cacheID ClusterCacheID, enabled bool) error {
	name := ClusterCachedCatalogName(beehive.ObjectID(cacheID))
	spec := ClusterCachedCatalogSpec{Enabled: enabled}

	// One transaction resolves the name and writes; a row awaiting collection is refused
	// rather than rewritten, and its replacement waits for GC to release the name. Same
	// shape as ensureClusterCache's relay.
	_, _, err := client.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(beehive.ObjectID(cacheID)))
	if err != nil && !errors.Is(err, beehive.ErrDeletionPending) {
		return fmt.Errorf("apply cached catalog %s: %w", name, err)
	}
	return nil
}

// toClusterCachedCatalog builds the served record from the stored object.
func toClusterCachedCatalog(obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) (*ClusterCachedCatalog, error) {
	owner, err := toOwnerRef(obj)
	if err != nil {
		return nil, err
	}
	return &ClusterCachedCatalog{
		ID:         ClusterCachedCatalogID(obj.ID),
		Owner:      owner,
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}, nil
}

// toClusterCachedCatalogs projects a whole read. beehive lists by id, which is creation
// order, and that is the order this family promises — so nothing here sorts.
func toClusterCachedCatalogs(objs []*beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) ([]*ClusterCachedCatalog, error) {
	catalogs := make([]*ClusterCachedCatalog, 0, len(objs))
	for _, obj := range objs {
		catalog, err := toClusterCachedCatalog(obj)
		if err != nil {
			return nil, err
		}
		catalogs = append(catalogs, catalog)
	}
	return catalogs, nil
}

func (a cachedCatalogsAPI) Get(ctx context.Context, id ClusterCachedCatalogID) (*ClusterCachedCatalog, error) {
	obj, err := a.s.catalogClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cached catalog %d: %w", id, err)
	}
	return toClusterCachedCatalog(obj)
}

func (a cachedCatalogsAPI) List(ctx context.Context) ([]*ClusterCachedCatalog, error) {
	objs, err := a.s.catalogClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cached catalogs: %w", err)
	}
	return toClusterCachedCatalogs(objs)
}

func (a cachedCatalogsAPI) Watch(ctx context.Context, id ClusterCachedCatalogID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.Watch(ctx, beehive.ObjectID(id), loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached catalog %d: %w", id, err)
	}

	return catalogWatch.streamOne(ctx, src), nil
}

func (a cachedCatalogsAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.WatchList(ctx, loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached catalogs: %w", err)
	}

	return catalogWatch.streamList(ctx, src), nil
}

func (a cachedCatalogsAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedCatalog, error) {
	// One query rather than a Get on the derived name: the owner edge is what the
	// record is enumerated through, and a cache that has not reconciled yet owns none,
	// which reads empty rather than failing.
	objs, err := a.s.catalogClient.ListOwnedObjects(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cache %d cached catalogs: %w", cacheID, err)
	}
	return toClusterCachedCatalogs(objs)
}

func (a cachedCatalogsAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.WatchOwnedObjects(ctx, beehive.ObjectID(cacheID), loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cache %d cached catalogs: %w", cacheID, err)
	}

	return catalogWatch.streamList(ctx, src), nil
}

// loadCatalogOwner eager-loads the owner edge every catalog frame carries as its join
// key; beehive batches the lookup per change batch, so a watch does not become an N+1.
var loadCatalogOwner = beehive.WithLoads(beehive.LoadOwner())

// catalogWatch projects this kind into delta frames. The departure carries the spec but
// no owner: the row is gone, so beehive loads no edge for it and reading one would fail
// the whole stream — and a consumer keys the record it is dropping by id anyway.
var catalogWatch = deltaWatch[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus, ClusterCachedCatalogWatchFrame]{
	frame: func(t DeltaFrameType, obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) (ClusterCachedCatalogWatchFrame, error) {
		catalog, err := toClusterCachedCatalog(obj)
		if err != nil {
			return ClusterCachedCatalogWatchFrame{}, err
		}
		return ClusterCachedCatalogWatchFrame{Type: t, Catalog: catalog}, nil
	},
	departed: func(change beehive.ObjectChange[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) ClusterCachedCatalogWatchFrame {
		catalog := &ClusterCachedCatalog{ID: ClusterCachedCatalogID(change.ID)}
		if obj := change.Object; obj != nil {
			catalog.Spec = obj.Spec
			catalog.Conditions = obj.Conditions
		}
		return ClusterCachedCatalogWatchFrame{Type: DeltaFrameDeleted, Catalog: catalog}
	},
	bookmark: ClusterCachedCatalogWatchFrame{Type: DeltaFrameBookmark},
}

// catalogResyncInterval paces the fold's backstop pass. The third kind whose correctness
// rests on a poll: what it folds is the sweeper's in-memory answer, which the store
// cannot see move — the trigger makes the fold prompt, and this covers a signal that
// went missing.
const catalogResyncInterval = 10 * time.Minute

// catalogRetryInterval is how soon a draining pass comes back: a tombstone releasing its
// name is not an event anything reports, so the wait has a clock rather than a wake.
const catalogRetryInterval = 30 * time.Second

// clusterCachedCatalogController reconciles one cache's kind catalog: it arms the
// sweeper for the record, folds the sweep's standing answer into one
// ClusterCachedResource child per served kind, and reports the verdict. No pass
// dials — the sweep runs on kubecatalog's own engine, and the trigger re-runs this
// fold when its answer moves.
type clusterCachedCatalogController struct {
	lifecycle.None
	// Every kind's client, not just this one's: a catalog reads the cache and cluster it
	// hangs off and writes the per-kind children it owns.
	deps
}

func (c *clusterCachedCatalogController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	// A catalog on its way out is about to be collected with the children it owns, and
	// beehive collects it with no finalizer to clear. The sweep is disarmed with it.
	if obj.DeletionRequestedAt != nil {
		c.kubecatalogSvc.Forget(obj.Name)
		return beehive.Settled()
	}

	own, err := c.ownersOf(ctx, client)
	if err != nil {
		return beehive.Fail(err)
	}
	// The subtree above is being collected, which will take this catalog with it.
	if own.cluster == nil {
		c.kubecatalogSvc.Forget(obj.Name)
		return beehive.Settled()
	}

	// A paused catalog keeps its children and stops discovering — disarming the sweep is
	// what stops it; the anchor lives as long as the cache, so the subtree survives a
	// pause and is not rebuilt on resume. The relay still runs, since the switch
	// reaching the workers is what pausing means.
	if !obj.Spec.Enabled {
		c.kubecatalogSvc.Forget(obj.Name)
		return c.relayPause(ctx, client, obj)
	}

	contextName, err := clusterContext(own.cluster)
	if err != nil {
		// The record's own state — disabled, deleting, or credential-less — which the
		// cluster pass reports on its own conditions. Nothing can sweep, so the
		// subject is dropped.
		c.kubecatalogSvc.Forget(obj.Name)
		return observeDiscovered(ctx, client, ConditionFalse, ReasonNoConnection, err.Error())
	}

	// Arming is this pass's other job: the subject exists exactly while the record
	// wants discovery, keyed by the record's own name so the sweeper's change signal
	// is the requeue.
	c.kubecatalogSvc.Track(obj.Name, kubecatalog.Params{
		CacheID:     int64(own.cache.ID),
		ContextName: contextName,
		ServerUID:   own.cache.Spec.ServerUID,
	})

	obs, ok := c.kubecatalogSvc.Read(obj.Name)
	if !ok || !obs.Known() {
		// Armed, no answer yet. The trigger re-runs this fold when one lands, and the
		// kind's resync is the backstop — so no requeue here.
		//
		// Connecting is the default because most waits here are the first sweep coming.
		// The other two are named because they are not that: a cluster nothing reached,
		// and a connection that will not answer for this cache until the record changes.
		reason := ReasonConnecting
		switch obs.LastAttempt.Reason {
		case kubecatalog.ReasonNoConnection:
			reason = ReasonNoConnection
		case kubecatalog.ReasonIdentityMismatch:
			reason = ReasonIdentityMismatch
		case kubecatalog.ReasonStoreFailed, kubecatalog.ReasonStoreRemoved:
			// A store failure on a cache's very first sweep, which would otherwise read
			// as Connecting for as long as the disk keeps refusing.
			reason = ReasonStoreUnavailable
		}
		return observeDiscovered(ctx, client, ConditionFalse, reason, obs.LastAttempt.Message)
	}
	return c.converge(ctx, client, obj, obs)
}

// owners is the chain above a catalog: the cache it anchors and the cluster that cache
// mirrors. Both, because the pass needs the cluster's context to sweep over and the
// cache's identity to say which server that sweep must answer as.
type owners struct {
	cache   *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]
	cluster *beehive.Object[ClusterSpec, ClusterStatus]
}

// ownersOf walks the two owner edges above a catalog. A zero owners with no error means
// something in that chain is gone or going, which is a cascade about to take this catalog
// rather than a failure to retry.
func (c *clusterCachedCatalogController) ownersOf(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
) (owners, error) {
	// The reconcile load carries no edges, so each owner is a lookup rather than a field.
	cacheRef, ok, err := client.GetOwner(ctx)
	if err != nil {
		return owners{}, fmt.Errorf("read cached catalog owner: %w", err)
	}
	if !ok {
		return owners{}, nil
	}

	cacheObj, err := c.cacheClient.Get(ctx, cacheRef.ID, beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return owners{}, nil
	}
	if err != nil {
		return owners{}, fmt.Errorf("read cluster cache %d: %w", cacheRef.ID, err)
	}
	if cacheObj.DeletionRequestedAt != nil {
		return owners{}, nil
	}

	clusterRef, ok, err := cacheObj.Owner()
	if err != nil {
		return owners{}, fmt.Errorf("read cluster cache %d owner: %w", cacheRef.ID, err)
	}
	if !ok {
		return owners{}, nil
	}

	clusterObj, err := c.clusterClient.Get(ctx, clusterRef.ID)
	if errors.Is(err, beehive.ErrNotFound) {
		return owners{}, nil
	}
	if err != nil {
		return owners{}, fmt.Errorf("read cluster %d: %w", clusterRef.ID, err)
	}
	return owners{cache: cacheObj, cluster: clusterObj}, nil
}

// converge rewrites the per-kind children to match the sweep's standing answer, and
// reports the verdict.
func (c *clusterCachedCatalogController) converge(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
	obs kubecatalog.Observation,
) beehive.ReconcileResult {
	held, err := c.resourceClient.ListOwnedObjects(ctx, obj.ID)
	if err != nil {
		return beehive.Fail(fmt.Errorf("list cached catalog %d resources: %w", obj.ID, err))
	}

	kinds := toResourceSpecs(obs.Value.Kinds)
	draining, err := c.applyKinds(ctx, obj, kinds)
	if err != nil {
		return beehive.Fail(err)
	}

	// Pruning needs a complete answer: a group that did not answer has not stopped being
	// served, and deleting its children would stop live workers over a transient outage.
	if !obs.Value.Partial {
		if err := c.prune(ctx, held, kinds); err != nil {
			return beehive.Fail(err)
		}
	}

	// The verdict follows the last attempt when it did not succeed — read off its
	// reason, never the retained value: the standing partial flag outliving a sweep
	// that then failed outright must not outrank that failure. The children still
	// match the standing answer either way, and the sweep retries on its own ladder,
	// so the fold settles rather than failing.
	status, reason, message := ConditionTrue, ReasonDiscovered, ""
	switch {
	case !obs.OK():
		status, message = ConditionFalse, obs.LastAttempt.Message
		switch obs.LastAttempt.Reason {
		case kubecatalog.ReasonSweepPartial:
			reason = ReasonDiscoveryPartial
		case kubecatalog.ReasonNoConnection:
			reason = ReasonNoConnection
		case kubecatalog.ReasonIdentityMismatch:
			// Not DiscoveryFailed: nothing was asked, and nothing is retrying. Saying the
			// discovery request failed points a reader at the API server when what moved
			// is which cluster the context reaches.
			reason = ReasonIdentityMismatch
		case kubecatalog.ReasonStoreFailed, kubecatalog.ReasonStoreRemoved:
			// The sweep's answer is good and the mirror would not take it; the sweep's own
			// ladder is retrying. A removed store means the cache record is gone, so
			// ownersOf returns before this in practice — but a teardown must not read as a
			// discovery failure on the pass that does get here.
			reason = ReasonStoreUnavailable
		default:
			reason = ReasonDiscoveryFailed
		}
	case draining:
		status, reason = ConditionFalse, ReasonDiscoveryDraining
	}
	res := observeDiscovered(ctx, client, status, reason, message)
	if draining {
		return res.RequeueAfter(catalogRetryInterval)
	}
	return res
}

// toResourceSpecs translates the sweep's native kinds into this kind's spec vocabulary.
// Enabled is left for applyKinds, which relays the catalog's own switch.
func toResourceSpecs(kinds []kubecatalog.Kind) []ClusterCachedResourceSpec {
	specs := make([]ClusterCachedResourceSpec, 0, len(kinds))
	for _, k := range kinds {
		specs = append(specs, ClusterCachedResourceSpec{
			APIVersion: k.GroupVersion,
			Kind:       k.Kind,
			Resource:   k.Resource,
			Namespaced: k.Namespaced,
		})
	}
	return specs
}

// relayPause rewrites each live child's switch and changes nothing else: the pass did
// not look at the server, so the desired set is what is already stored.
func (c *clusterCachedCatalogController) relayPause(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	held, err := c.resourceClient.ListOwnedObjects(ctx, obj.ID)
	if err != nil {
		return beehive.Fail(fmt.Errorf("list cached catalog %d resources: %w", obj.ID, err))
	}
	// Draining cannot arise: every name written here belongs to a live row just listed.
	if _, err := c.applyKinds(ctx, obj, specsOf(held)); err != nil {
		return beehive.Fail(err)
	}
	return observeDiscovered(ctx, client, ConditionFalse, ReasonPaused, "")
}

// applyKinds converges one child per kind, relaying the pause switch into each. draining
// reports a served kind whose name is still held by an earlier prune's tombstone: the
// kind has no live record, which is a state to report and come back to, not a failure.
func (c *clusterCachedCatalogController) applyKinds(
	ctx context.Context,
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
	kinds []ClusterCachedResourceSpec,
) (draining bool, err error) {
	for _, spec := range kinds {
		spec.Enabled = obj.Spec.Enabled
		name := ClusterCachedResourceName(obj.ID, spec.APIVersion, spec.Resource)
		_, _, err := c.resourceClient.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(obj.ID))
		switch {
		case errors.Is(err, beehive.ErrDeletionPending):
			draining = true
		case err != nil:
			return false, fmt.Errorf("apply cached resource %s: %w", name, err)
		}
	}
	return draining, nil
}

// prune deletes the children for kinds the server no longer serves. Deletion is beehive's
// soft one, so the row lingers holding its name — which is the draining case above, and
// why a kind that comes back is reported rather than silently missing.
func (c *clusterCachedCatalogController) prune(ctx context.Context, held []*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus], kinds []ClusterCachedResourceSpec) error {
	served := make(map[SyncedKindRef]struct{}, len(kinds))
	for _, spec := range kinds {
		served[SyncedKindRef{APIVersion: spec.APIVersion, Resource: spec.Resource}] = struct{}{}
	}

	for _, obj := range held {
		ref := SyncedKindRef{APIVersion: obj.Spec.APIVersion, Resource: obj.Spec.Resource}
		if _, ok := served[ref]; ok || obj.DeletionRequestedAt != nil {
			continue
		}
		if err := c.resourceClient.Delete(ctx, obj.ID); err != nil && !errors.Is(err, beehive.ErrNotFound) {
			return fmt.Errorf("delete cached resource %d: %w", obj.ID, err)
		}
	}
	return nil
}

// specsOf reads the stored specs back out, for the pass that has no fresh answer.
func specsOf(objs []*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) []ClusterCachedResourceSpec {
	specs := make([]ClusterCachedResourceSpec, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt == nil {
			specs = append(specs, obj.Spec)
		}
	}
	return specs
}

// observeDiscovered records the verdict and settles. The pass writes no status of its own
// — this kind has none — so the condition and the result are the whole report.
func observeDiscovered(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	status ConditionStatus,
	reason, message string,
) beehive.ReconcileResult {
	cond := LiveCondition(ConditionDiscovered, status, reason, message)
	if err := client.SetCondition(ctx, cond); err != nil {
		return beehive.Fail(fmt.Errorf("set %s condition: %w", ConditionDiscovered, err))
	}
	return beehive.Settled()
}
