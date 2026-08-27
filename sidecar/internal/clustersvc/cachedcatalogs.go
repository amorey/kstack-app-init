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

	"github.com/amorey/beehive"

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
// which nothing in the object graph reads. A verdict belongs on a condition.
type ClusterCachedCatalogStatus struct{}

// ClusterCachedCatalog is the view of one ClusterCachedCatalog beehive object: the anchor a
// cache's discovery runs against. It carries the pause switch its cache pushed down, and the
// kinds a cache serves are its ClusterCachedResource children, one per kind. When discovery
// last answered is deliberately nowhere, since a timestamp on a record re-emits it to every
// watcher. Streamed standalone via CachedCatalogs().Watch and joined onto its cache
// client-side by Owner.ID. Spec is the stored value served as-is, no projection.
type ClusterCachedCatalog struct {
	ID ClusterCachedCatalogID
	// Owner is the ClusterCache this catalog belongs to.
	Owner ObjectRef
	Spec  ClusterCachedCatalogSpec
	// Conditions are beehive object conditions, read off the object rather than out of
	// the status blob. There is no Status field: the kind's status is empty by design.
	// Nothing writes one today — the discovery seam is being redesigned.
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

// clusterCachedCatalogController reconciles one cache's kind catalog. Nothing
// discovers kinds today — the seam between this record, ClusterCachedResource, and the
// per-cache store is being redesigned — so a pass has no work and settles. The record
// itself still exists per cache, created by the cache's own pass.
type clusterCachedCatalogController struct {
	lifecycle.None
	// Every kind's client, not just this one's: a catalog reads the cache and cluster it
	// hangs off and writes the per-kind children it owns.
	deps
}

func (c *clusterCachedCatalogController) Reconcile(
	context.Context,
	beehive.ControllerClient[ClusterCachedCatalogStatus],
	*beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	return beehive.Settled()
}
