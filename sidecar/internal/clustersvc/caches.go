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
// via Caches().Watch and joined to its parent client-side. ID and ClusterID together
// locate the on-disk cache and carry that join. Active-ness is deliberately not a
// field: it is ServerUID matching the parent's last-probed Server.UID, which changes
// with no cache event, so only the client's live join is correct.
type ClusterCache struct {
	ID        ClusterCacheID
	ClusterID ClusterID
	ServerUID string
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

func (a cachesAPI) Get(ctx context.Context, id ClusterCacheID) (*ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) List(ctx context.Context) ([]*ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) Watch(ctx context.Context, id ClusterCacheID) (*Stream[ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) WatchList(ctx context.Context) (*Stream[ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) ListByCluster(ctx context.Context, clusterID ClusterID) ([]*ClusterCache, error) {
	panic("not implemented")
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
