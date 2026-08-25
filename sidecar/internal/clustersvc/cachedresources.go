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
	"strconv"

	"github.com/amorey/beehive"
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

func (a cachedResourcesAPI) Get(ctx context.Context, id ClusterCachedResourceID) (*ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) List(ctx context.Context) ([]*ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) Watch(ctx context.Context, id ClusterCachedResourceID) (*Stream[ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) Clear(ctx context.Context, id ClusterCachedResourceID) (*ClusterCachedResource, error) {
	panic("not implemented")
}

// clusterCachedResourceController reconciles one synced kind: start, stop, and
// resume the worker mirroring it into the cache. A placeholder that reconciles to a
// no-op.
type clusterCachedResourceController struct{ lifecycle.None }

func (c *clusterCachedResourceController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedResourceStatus],
	obj *beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus],
) beehive.ReconcileResult {
	// A no-op still settles: unsettled, every synced kind is re-dispatched on each owed
	// pass — ~100 per cache — for the life of the process.
	return beehive.Settled()
}
