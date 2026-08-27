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

// The ClusterCachedKind kind: one record per kind a cache mirrors. Its beehive
// shapes, the record served to resolvers, its delta-watch frame, the CachedKinds
// implementation, and its controller. Mirrors the ClusterCachedKind section of
// graph/schema.graphqls.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterCachedKindGroupKind identifies the per-GVR sync kind: one object per
// served GVR, owned by its ClusterCache.
var ClusterCachedKindGroupKind = beehive.GroupKind{Kind: "ClusterCachedKind"}

// ClusterCachedKindName returns "cachedkind/{cacheObjID}/{apiVersion}/{resource}" —
// deterministic, so a discovery pass is a set reconcile with no per-child
// bookkeeping, and derivable from the cache id alone, so anything holding a cache and
// a kind can name the record. (apiVersion, resource) rather than Kind: the
// plural is what the worker's REST path needs and what the server guarantees unique
// per group-version.
func ClusterCachedKindName(cacheID beehive.ObjectID, apiVersion, resource string) string {
	return "cachedkind/" + strconv.FormatInt(int64(cacheID), 10) + "/" + apiVersion + "/" + resource
}

// EventsKind / EventsAPIVersion / EventsResource identify the Event collection — an
// ordinary synced kind, written to its own table. The server serves the same events
// under two spellings backed by one store, so exactly one may be synced, and this is
// the canonical spelling: whatever discovers kinds must drop the events.k8s.io one.
const (
	EventsKind       = "Event"
	EventsAPIVersion = "v1"
	EventsResource   = "events"
)

// ClusterCachedKindSpec is the desired sync for one GVR, written wholly from
// above: identity, and nothing else. Whether a kind syncs is its cache's, never
// relayed here. The fields refresh each discovery pass, so a kind that changes shape
// converges without recreation.
type ClusterCachedKindSpec struct {
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

// ClusterCachedKindStatus is the observed sync state for one GVR. Empty placeholder.
type ClusterCachedKindStatus struct{}

// ClusterCachedKind is the view of one ClusterCachedKind beehive object: one
// Kubernetes kind being mirrored into a cache. Shaped like the records above it —
// {ID, Owner, Spec, Conditions} — but streamed **cache-scoped**, because there is one per
// served kind rather than one per cache and an unscoped stream of a hundred-plus records
// would be a firehose.
type ClusterCachedKind struct {
	ID ClusterCachedKindID
	// Owner is the ClusterCache this kind is mirrored into — the join key a client
	// already holds from the cache stream.
	Owner ObjectRef
	Spec  ClusterCachedKindSpec
	// Conditions carry this kind's own verdict, which is the whole reason the record is
	// served: a cache's hundred kinds fail independently, and the coarse cache-level
	// condition can't say which. Nothing writes one today — the sync seam is being
	// redesigned.
	Conditions []Condition
}

// ClusterCachedKindWatchFrame is one frame on the cache-scoped per-kind sync watch.
// Consumers key on Kind.ID.
type ClusterCachedKindWatchFrame struct {
	Type DeltaFrameType
	Kind *ClusterCachedKind
}

// SyncedKindRef identifies one synced kind exactly. The plural alone is what a UI
// renders, but it does not IDENTIFY a kind — a CRD may reuse a built-in's plural
// under another api group — so anything that keys on a kind needs the pair.
type SyncedKindRef struct {
	APIVersion string
	Resource   string
}

// toClusterCachedKind builds the served record from the stored object.
func toClusterCachedKind(obj *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]) (*ClusterCachedKind, error) {
	owner, err := toOwnerRef(obj)
	if err != nil {
		return nil, err
	}
	return &ClusterCachedKind{
		ID:         ClusterCachedKindID(obj.ID),
		Owner:      owner,
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}, nil
}

// toClusterCachedKinds projects a whole read. beehive lists by id, which is creation
// order, and that is the order this family promises — so nothing here sorts.
func toClusterCachedKinds(objs []*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]) ([]*ClusterCachedKind, error) {
	kinds := make([]*ClusterCachedKind, 0, len(objs))
	for _, obj := range objs {
		kind, err := toClusterCachedKind(obj)
		if err != nil {
			return nil, err
		}
		kinds = append(kinds, kind)
	}
	return kinds, nil
}

func (a cachedKindsAPI) Get(ctx context.Context, id ClusterCachedKindID) (*ClusterCachedKind, error) {
	obj, err := a.s.kindClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cached kind %d: %w", id, err)
	}
	return toClusterCachedKind(obj)
}

func (a cachedKindsAPI) List(ctx context.Context) ([]*ClusterCachedKind, error) {
	objs, err := a.s.kindClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cached kinds: %w", err)
	}
	return toClusterCachedKinds(objs)
}

func (a cachedKindsAPI) Watch(ctx context.Context, id ClusterCachedKindID) (*Stream[ClusterCachedKindWatchFrame], error) {
	src, err := a.s.kindClient.Watch(ctx, beehive.ObjectID(id), loadKindOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached kind %d: %w", id, err)
	}

	return kindWatch.streamOne(ctx, src), nil
}

// WatchList is the fleet's largest stream by an order of magnitude — a record per served
// kind per cache. Served because the boundary fills its matrix, but a view scoped to one
// cache opens WatchByCache instead; the schema exposes only the scoped one.
func (a cachedKindsAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedKindWatchFrame], error) {
	src, err := a.s.kindClient.WatchList(ctx, loadKindOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached kinds: %w", err)
	}

	return kindWatch.streamList(ctx, src), nil
}

func (a cachedKindsAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedKind, error) {
	objs, err := a.s.kindClient.ListOwnedObjects(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cache %d cached kinds: %w", cacheID, err)
	}
	return toClusterCachedKinds(objs)
}

func (a cachedKindsAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedKindWatchFrame], error) {
	src, err := a.s.kindClient.WatchOwnedObjects(ctx, beehive.ObjectID(cacheID), loadKindOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cache %d cached kinds: %w", cacheID, err)
	}
	return kindWatch.streamList(ctx, src), nil
}

// Clear drops one kind's rows from the cache holding them — the cache-wide clear,
// scoped to a kind.
func (a cachedKindsAPI) Clear(ctx context.Context, id ClusterCachedKindID) (*ClusterCachedKind, error) {
	obj, err := a.s.kindClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cached kind %d: %w", id, err)
	}

	// The cache is gone when this reports none, so its file went with it and there are
	// no rows left to clear.
	cacheID, ok, err := a.cacheIDForKind(obj)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return toClusterCachedKind(obj)
	}

	if err := clearKindRows(ctx, a.s.kubestoreMgr, int64(cacheID), obj.Spec); err != nil {
		return nil, fmt.Errorf("clear cached kind %d rows: %w", id, err)
	}
	return toClusterCachedKind(obj)
}

// clearKindRows drops one kind's rows from the cache holding them. A cache with no file
// has nothing to clear — and opening one would create the very file the clear is
// removing, which is why this goes through OpenExisting rather than claiming a store
// outright.
func clearKindRows(ctx context.Context, mgr kubestoreManager, cacheID int64, spec ClusterCachedKindSpec) error {
	store, ok, err := mgr.OpenExisting(cacheID)
	if err != nil || !ok {
		return err
	}
	defer store.Release()

	// The record's own Kind, so a clear never has to ask the catalog table which rows are
	// this kind's — a kind that table has not registered would keep every row.
	return store.ClearKind(ctx, kubestore.Kind{
		APIVersion: spec.APIVersion,
		Kind:       spec.Kind,
		Resource:   spec.Resource,
	})
}

// cacheIDForKind is the cache holding a kind's rows, read off the record's owner
// edge. A cache that is already gone is not an error: the file went with it, so there
// is nothing left to clear per kind.
func (a cachedKindsAPI) cacheIDForKind(
	obj *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus],
) (ClusterCacheID, bool, error) {
	cacheRef, ok, err := obj.Owner()
	if err != nil {
		return 0, false, fmt.Errorf("read cached kind %d owner: %w", obj.ID, err)
	}
	if !ok {
		return 0, false, nil
	}
	return ClusterCacheID(cacheRef.ID), true, nil
}

// loadKindOwner eager-loads the owner edge every frame carries as its join key; beehive
// batches the lookup per change batch, so a watch does not become an N+1.
var loadKindOwner = beehive.WithLoads(beehive.LoadOwner())

// kindWatch projects this kind into delta frames. The departure carries the spec but no
// owner: the row is gone, so beehive loads no edge for it and reading one would fail the whole
// stream — and a consumer keys the record it is dropping by id anyway.
var kindWatch = deltaWatch[ClusterCachedKindSpec, ClusterCachedKindStatus, ClusterCachedKindWatchFrame]{
	frame: func(t DeltaFrameType, obj *beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus]) (ClusterCachedKindWatchFrame, error) {
		kind, err := toClusterCachedKind(obj)
		if err != nil {
			return ClusterCachedKindWatchFrame{}, err
		}
		return ClusterCachedKindWatchFrame{Type: t, Kind: kind}, nil
	},
	departed: func(change beehive.ObjectChange[ClusterCachedKindSpec, ClusterCachedKindStatus]) ClusterCachedKindWatchFrame {
		kind := &ClusterCachedKind{ID: ClusterCachedKindID(change.ID)}
		if obj := change.Object; obj != nil {
			kind.Spec = obj.Spec
			kind.Conditions = obj.Conditions
		}
		return ClusterCachedKindWatchFrame{Type: DeltaFrameDeleted, Kind: kind}
	},
	bookmark: ClusterCachedKindWatchFrame{Type: DeltaFrameBookmark},
}

// clusterCachedKindController reconciles one synced kind. Nothing mirrors a kind
// into a cache today — the seam between this record and the per-cache store is being
// redesigned — so a pass has no work and settles.
type clusterCachedKindController struct {
	lifecycle.None
	// Every kind's client, not just this one's: a cached kind reads the cache and cluster
	// it hangs off, and reaches the store through the shared services.
	deps
}

func (c *clusterCachedKindController) Reconcile(
	context.Context,
	beehive.ControllerClient[ClusterCachedKindStatus],
	*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus],
) beehive.ReconcileResult {
	return beehive.Settled()
}
