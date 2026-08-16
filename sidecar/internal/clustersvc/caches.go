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

// The ClusterCache kind: one mirror of one cluster identity. Its beehive shapes, the
// record served to resolvers, its delta-watch frame, the whole-cache gauges, the
// Caches implementation, and its controller. Mirrors the ClusterCache section of
// graph/schema.graphqls.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/amorey/beehive"
)

// ClusterCacheGroupKind identifies the ClusterCache beehive resource kind.
var ClusterCacheGroupKind = beehive.GroupKind{Kind: "ClusterCache"}

// ClusterCacheName returns a ClusterCache's name, "{ClusterID}/{serverUID}":
// one cache per identity per cluster, so a UID migration yields a second
// coexisting cache. A creation/dedup key only — caches are enumerated through the
// owner edge.
func ClusterCacheName(clusterID ClusterID, serverUID string) string {
	return strconv.FormatInt(int64(clusterID), 10) + "/" + serverUID
}

// ClusterCacheSpec: ServerUID is the kube-system UID this cache mirrors — the
// identity a migration turns over. Active-ness is decided by comparing it against
// the parent's last-probed Server.UID; read spec.ServerUID for that, never the name.
type ClusterCacheSpec struct {
	ServerUID string `json:"serverUid"`
}

// ClusterCacheStatus is empty — this kind reports through its conditions alone;
// the rollup a UI wants is served read-side by Caches().WatchHealth.
type ClusterCacheStatus struct{}

// ClusterCache is the view of one ClusterCache beehive object, streamed standalone
// via Caches().Watch and joined to its parent client-side. ID and Owner together
// locate the on-disk cache and carry that join. Active-ness is deliberately not a
// field: it is spec.ServerUID matching the parent's last-probed Server.UID, which
// changes with no cache event, so only the client's live join is correct.
//
// Shaped like its sync siblings — {ID, Owner, Spec, Conditions} — with the stored spec
// served as-is, no projection.
type ClusterCache struct {
	ID ClusterCacheID
	// Owner is the Cluster this cache belongs to.
	Owner ObjectRef
	Spec  ClusterCacheSpec
	// Conditions are beehive object conditions, not part of Status — read off the
	// object rather than out of the status blob.
	Conditions []Condition
}

// ClusterCacheWatchFrame is the ClusterCache-kind counterpart of ClusterWatchFrame, carried
// on the standalone cache watch. Binds 1:1 to the GraphQL ClusterCacheWatchFrame.
type ClusterCacheWatchFrame struct {
	Type  DeltaFrameType
	Cache *ClusterCache
}

// ClusterCacheStats reports a cluster's live on-disk cache statistics.
type ClusterCacheStats struct {
	Exists bool
	Bytes  int64
	// ObjectCount/KindCount are the whole-cache rollup read from the kind
	// catalog's trigger-maintained per-kind counts: the total cached objects and
	// the number of kinds with at least one cached object. Both exclude events
	// (events are not a catalog kind).
	ObjectCount int
	KindCount   int
}

// ClusterCacheHealth is one cache's sync verdict, folded from every kind it syncs.
//
// A READ-SIDE projection, not a stored condition: status is for state a dependent
// reacts to, and nothing in the object graph reacts to this — only a UI does. Storing
// it would wake the cache and every watcher each time any of its hundred-plus children
// changed verdict, to publish a number no controller reads.
//
// The per-kind records can't serve that consumer themselves: there is one per synced
// kind, and no single child's verdict is the cache's — ninety-nine healthy kinds and
// one forbidden CRD is not healthy, and reading any one child would call it either way
// at random.
type ClusterCacheHealth struct {
	CacheID ClusterCacheID
	// Status/Reason mirror a Condition's shape (and its reason vocabulary), so a consumer
	// renders this exactly as it renders a per-kind verdict.
	Status ConditionStatus
	Reason string
	// UnhealthyKindRefs names the kinds behind a non-Watching verdict, sorted — the
	// identity a consumer can act on (look the kind's record up, key a subscription on
	// it), not a rendered phrase. Empty when healthy. It is a list rather than a message
	// because truncation ("+2 more") is a layout decision, and the consumer that needs the
	// first offender must not have to parse it back out of prose.
	UnhealthyKindRefs []SyncedKindRef
	// TotalKinds is how many kinds this cache syncs; UnhealthyKinds how many are not
	// currently Watching (len(UnhealthyKindRefs), which is capped by nothing).
	TotalKinds     int
	UnhealthyKinds int
	// LastUpdateAt is the most recent write across every kind — when data last arrived
	// anywhere in this cache. LastLiveAt is the OLDEST proof among the kinds that have
	// one: the weakest link, since a cache is only as verified as its least-recently
	// proven watch. Both nil until some kind has reported.
	LastUpdateAt *time.Time
	LastLiveAt   *time.Time
}

// ensureClusterCache creates the mirror slot for one probed identity, owned by the
// cluster so beehive's GC cascades to it. Idempotent: the name is the dedup key, and a
// later pass has nothing to update — ServerUID *is* the identity.
//
// Called by the parent's reconcile, since a controller only ever reconciles an object
// that already exists and the identity is the parent's to discover. The writes live
// here so the kind's vocabulary stays in the kind's file.
//
// The GetByName probe keeps the steady state off the write path: GetOrCreate opens a
// transaction even on the found branch, and the store is single-connection, so each one
// serializes every other reader for its duration. GetOrCreate still closes the
// create-path race.
func ensureClusterCache(ctx context.Context, client beehive.Client[ClusterCacheSpec, ClusterCacheStatus], clusterID ClusterID, serverUID string) error {
	name := ClusterCacheName(clusterID, serverUID)
	_, err := client.GetByName(ctx, name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, beehive.ErrNotFound) {
		return fmt.Errorf("look up cluster cache %s: %w", name, err)
	}

	spec := ClusterCacheSpec{ServerUID: serverUID}
	if _, _, err := client.GetOrCreate(ctx, name, spec, beehive.WithOwner(beehive.ObjectID(clusterID))); err != nil {
		return fmt.Errorf("create cluster cache %s: %w", name, err)
	}
	return nil
}

// toClusterCache builds the served record from the stored object.
func toClusterCache(obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (*ClusterCache, error) {
	owner, err := toOwnerRef(obj)
	if err != nil {
		return nil, err
	}
	return &ClusterCache{
		ID:         ClusterCacheID(obj.ID),
		Owner:      owner,
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}, nil
}

func (a cachesAPI) Get(ctx context.Context, id ClusterCacheID) (*ClusterCache, error) {
	obj, err := a.s.cacheClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cluster cache %d: %w", id, err)
	}
	return toClusterCache(obj)
}

func (a cachesAPI) List(ctx context.Context) ([]*ClusterCache, error) {
	objs, err := a.s.cacheClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cluster caches: %w", err)
	}
	return toClusterCaches(objs)
}

// toClusterCaches projects a whole read. beehive lists by id, which is creation order,
// and that is the order this family promises — so nothing here sorts.
func toClusterCaches(objs []*beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) ([]*ClusterCache, error) {
	caches := make([]*ClusterCache, 0, len(objs))
	for _, obj := range objs {
		cache, err := toClusterCache(obj)
		if err != nil {
			return nil, err
		}
		caches = append(caches, cache)
	}
	return caches, nil
}

func (a cachesAPI) Watch(ctx context.Context, id ClusterCacheID) (*Stream[ClusterCacheWatchFrame], error) {
	src, err := a.s.cacheClient.Watch(ctx, beehive.ObjectID(id), loadCacheOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cluster cache %d: %w", id, err)
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheWatchFrame) error {
		// Nil when the id holds nothing yet, which is a snapshot of none rather than a
		// failure: the record may still arrive, and this subscription reports it.
		var snapshot []*beehive.Object[ClusterCacheSpec, ClusterCacheStatus]
		if src.Object != nil {
			snapshot = append(snapshot, src.Object)
		}
		return pumpCacheWatch(ctx, out, snapshot, src.Changes, src.Err)
	}), nil
}

func (a cachesAPI) WatchList(ctx context.Context) (*Stream[ClusterCacheWatchFrame], error) {
	src, err := a.s.cacheClient.WatchList(ctx, loadCacheOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cluster caches: %w", err)
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheWatchFrame) error {
		return pumpCacheWatch(ctx, out, src.Objects, src.Changes, src.Err)
	}), nil
}

// loadCacheOwner eager-loads the owner edge every cache frame carries as its join key;
// beehive batches the lookup per change batch, so a watch does not become an N+1.
var loadCacheOwner = beehive.WithLoads(beehive.LoadOwner())

// pumpCacheWatch streams a snapshot, the bookmark closing it, then every change above
// it. The two cache watches differ only in what the snapshot holds.
//
// beehive hands the snapshot complete as of a resource version, with Changes carrying
// everything above it, so the bookmark lands between the two without holding any frame
// back. srcErr is the upstream's terminal reason, read once its changes run out.
func pumpCacheWatch(
	ctx context.Context,
	out chan<- ClusterCacheWatchFrame,
	snapshot []*beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
	changes <-chan beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus],
	srcErr func() error,
) error {
	for _, obj := range snapshot {
		frame, err := cacheFrame(DeltaFrameAdded, obj)
		if err != nil {
			return err
		}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	if !sendFrame(ctx, out, ClusterCacheWatchFrame{Type: DeltaFrameBookmark}) {
		return nil
	}

	for change := range changes {
		if change.Type == beehive.Deleted {
			if !sendFrame(ctx, out, cacheDeparture(change)) {
				return nil
			}
			continue
		}

		// Only a removal arrives without an object.
		if change.Object == nil {
			continue
		}
		frame, err := cacheFrame(deltaFrameType(change), change.Object)
		if err != nil {
			return err
		}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	return srcErr()
}

// cacheFrame projects one object into a frame of the given type.
func cacheFrame(t DeltaFrameType, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (ClusterCacheWatchFrame, error) {
	cache, err := toClusterCache(obj)
	if err != nil {
		return ClusterCacheWatchFrame{}, err
	}
	return ClusterCacheWatchFrame{Type: t, Cache: cache}, nil
}

// cacheDeparture builds the frame for a removal, which is the one frame built without
// toClusterCache: the row is gone, so beehive loads no owner edge for it and reading one
// would fail the whole stream. The join key is moot on a removal — a consumer keys the
// record it is dropping by id.
//
// The id is what the frame must carry above all: beehive reports a removal whose final
// row it could not decode with no object at all, nothing later in the log mentions a
// deleted id, and a consumer drops a change with no entity rather than folding it — so a
// dropped frame strands the record in its map for the life of the subscription.
func cacheDeparture(change beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus]) ClusterCacheWatchFrame {
	cache := &ClusterCache{ID: ClusterCacheID(change.ID)}
	if obj := change.Object; obj != nil {
		cache.Spec = obj.Spec
		cache.Conditions = obj.Conditions
	}
	return ClusterCacheWatchFrame{Type: DeltaFrameDeleted, Cache: cache}
}

func (a cachesAPI) ListByCluster(ctx context.Context, clusterID ClusterID) ([]*ClusterCache, error) {
	// One query rather than a Get per child, and a cluster owning none reads empty:
	// beehive does not existence-check the owner, so an unprobed cluster and an unknown
	// id answer alike.
	objs, err := a.s.cacheClient.ListOwnedObjects(ctx, beehive.ObjectID(clusterID), beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cluster %d caches: %w", clusterID, err)
	}
	return toClusterCaches(objs)
}

func (a cachesAPI) WatchByCluster(ctx context.Context, clusterID ClusterID) (*Stream[ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) WatchStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterCacheStats, error) {
	panic("not implemented")
}

func (a cachesAPI) WatchHealth(ctx context.Context) (*Stream[ClusterCacheHealth], error) {
	panic("not implemented")
}

func (a cachesAPI) Clear(ctx context.Context, id ClusterID) (*Cluster, error) {
	panic("not implemented")
}

// clusterCacheController reconciles one cache: provision its mirror, gate it on the
// cluster's connection and sync toggles, and own the catalog beneath it. A
// placeholder that reconciles to a no-op.
type clusterCacheController struct{ noBackground }

func (c *clusterCacheController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
) (beehive.Result, error) {
	return beehive.Result{}, nil
}
