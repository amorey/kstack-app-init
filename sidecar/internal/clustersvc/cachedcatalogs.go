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
// which nothing in the object graph reads. The Discovered condition is what remains.
type ClusterCachedCatalogStatus struct{}

// ClusterCachedCatalog is the view of one ClusterCachedCatalog beehive
// object: a cache's kind-catalog record — which kinds the cluster serves, when that
// was last confirmed, and whether the confirmation was complete. Streamed standalone
// via CachedCatalogs().Watch and joined onto its cache client-side by Owner.ID. Spec is the
// stored value served as-is, no projection.
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
//
// The GetByName probe keeps the steady state off the write path: GetOrCreate opens a
// transaction even on the found branch, and the store is single-connection, so each one
// serializes every other reader for its duration. GetOrCreate still closes the
// create-path race.
func ensureClusterCachedCatalog(ctx context.Context, client beehive.Client[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus], cacheID ClusterCacheID, enabled bool) error {
	name := ClusterCachedCatalogName(beehive.ObjectID(cacheID))
	spec := ClusterCachedCatalogSpec{Enabled: enabled}

	obj, err := client.GetByName(ctx, name)
	if err == nil {
		if obj.Spec == spec {
			return nil
		}
		if _, err := client.Update(ctx, obj.ID, spec); err != nil {
			return fmt.Errorf("update cached catalog %s: %w", name, err)
		}
		return nil
	}
	if !errors.Is(err, beehive.ErrNotFound) {
		return fmt.Errorf("look up cached catalog %s: %w", name, err)
	}

	if _, _, err := client.GetOrCreate(ctx, name, spec, beehive.WithOwner(beehive.ObjectID(cacheID))); err != nil {
		return fmt.Errorf("create cached catalog %s: %w", name, err)
	}
	return nil
}

func (a cachedCatalogsAPI) Get(ctx context.Context, id ClusterCachedCatalogID) (*ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) List(ctx context.Context) ([]*ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) Watch(ctx context.Context, id ClusterCachedCatalogID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

// clusterCachedCatalogController reconciles one cache's kind catalog: run the
// discovery pass and maintain a ClusterCachedResource child per served kind. A
// placeholder that reconciles to a no-op.
type clusterCachedCatalogController struct{ lifecycle.None }

func (c *clusterCachedCatalogController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	// A no-op still settles: unsettled, every catalog is re-dispatched — and re-read —
	// on each owed pass for the life of the process.
	return beehive.Settled()
}
