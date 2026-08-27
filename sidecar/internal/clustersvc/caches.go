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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/gobus"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
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

// ClusterCacheHealth is one cache's sync verdict over every kind it syncs. Nothing
// syncs a kind today, so the fields below the verdict itself go unpopulated — the seam
// that fills a cache is being redesigned.
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
	// anywhere in this cache; nil until something has landed. LastLiveAt is the OLDEST
	// proof across every kind, and **nil while any of them has none**: a cache is only as
	// verified as its least proven watch, and a kind nothing has proven live is weaker
	// than any stamp its neighbours carry.
	LastUpdateAt *time.Time
	LastLiveAt   *time.Time
}

// cacheSyncEnabled is the pause switch one cache relays into the subtree it owns: the
// cluster's own toggles, and whether this cache still mirrors the identity the cluster
// is probed at. Evaluated once here rather than re-derived per child, so a paused
// subtree cannot disagree with itself.
func cacheSyncEnabled(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	return clusterObj.Spec.Enabled && clusterObj.Spec.SyncEnabled && CacheIsActive(clusterObj, cacheUID)
}

// ensureClusterCache creates the mirror slot for one probed identity, owned by the
// cluster so beehive's GC cascades to it. Idempotent: the name is the dedup key, and a
// later pass has nothing to update — ServerUID *is* the identity.
//
// Called by the parent's reconcile, since a controller only ever reconciles an object
// that already exists and the identity is the parent's to discover. The writes live
// here so the kind's vocabulary stays in the kind's file.
//
// GetOrCreate rather than CreateOrUpdate: a found row is returned as-is, which is the
// whole contract here — ServerUID *is* the identity, so a later pass has nothing to
// write, and a row awaiting collection holds its name until GC releases it.
func ensureClusterCache(ctx context.Context, client beehive.Client[ClusterCacheSpec, ClusterCacheStatus], clusterID ClusterID, serverUID string) error {
	name := ClusterCacheName(clusterID, serverUID)
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

	return cacheWatch.streamOne(ctx, src), nil
}

func (a cachesAPI) WatchList(ctx context.Context) (*Stream[ClusterCacheWatchFrame], error) {
	src, err := a.s.cacheClient.WatchList(ctx, loadCacheOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cluster caches: %w", err)
	}

	return cacheWatch.streamList(ctx, src), nil
}

// loadCacheOwner eager-loads the owner edge every cache frame carries as its join key;
// beehive batches the lookup per change batch, so a watch does not become an N+1.
var loadCacheOwner = beehive.WithLoads(beehive.LoadOwner())

// cacheWatch projects this kind into delta frames. The departure is the one frame built
// without toClusterCache: the row is gone, so beehive loads no owner edge for it and
// reading one would fail the whole stream. The join key is moot there anyway — a
// consumer keys the record it is dropping by id.
var cacheWatch = deltaWatch[ClusterCacheSpec, ClusterCacheStatus, ClusterCacheWatchFrame]{
	frame: func(t DeltaFrameType, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) (ClusterCacheWatchFrame, error) {
		cache, err := toClusterCache(obj)
		if err != nil {
			return ClusterCacheWatchFrame{}, err
		}
		return ClusterCacheWatchFrame{Type: t, Cache: cache}, nil
	},
	departed: func(change beehive.ObjectChange[ClusterCacheSpec, ClusterCacheStatus]) ClusterCacheWatchFrame {
		cache := &ClusterCache{ID: ClusterCacheID(change.ID)}
		if obj := change.Object; obj != nil {
			cache.Spec = obj.Spec
			cache.Conditions = obj.Conditions
		}
		return ClusterCacheWatchFrame{Type: DeltaFrameDeleted, Cache: cache}
	},
	bookmark: ClusterCacheWatchFrame{Type: DeltaFrameBookmark},
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
	// Bookmark-only for a cluster owning none, the same way ListByCluster reads empty:
	// beehive does not existence-check the owner, so an unprobed cluster and an unknown
	// id behave alike, and either may still gain a cache this stream reports.
	src, err := a.s.cacheClient.WatchOwnedObjects(ctx, beehive.ObjectID(clusterID), loadCacheOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cluster %d caches: %w", clusterID, err)
	}

	return cacheWatch.streamList(ctx, src), nil
}

// defaultGaugeCadence is how often a gauge re-measures when nothing has pinged it.
// Human-paced: what it covers is a number drifting under a settled record, not an
// event, and every watcher pays for each tick.
const defaultGaugeCadence = 5 * time.Second

// WatchStats measures one cache's file and contents. A gauge carries no bookmark and
// emits nothing before its first measurement, so a pair naming no live cache holds
// silent rather than claiming an answer — a caller holding a bad id got it from a watch
// frame, and drops the subscription itself.
//
// It re-measures on the store's own change pings and on a cadence, because the two
// cover different halves: pings carry the writes, and the file's size moves with
// checkpoints that ping nothing.
func (a cachesAPI) WatchStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCacheStats], error) {
	live, err := a.s.cacheBelongsTo(ctx, clusterID, cacheID)
	if err != nil {
		return nil, err
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheStats) error {
		if !live {
			<-ctx.Done()
			return nil
		}
		ticker := time.NewTicker(a.s.gaugeCadence)
		defer ticker.Stop()

		var (
			sub   kubestore.Subscription
			pings <-chan gobus.Event[string, struct{}]
			last  ClusterCacheStats
			sent  bool
		)
		defer func() {
			if sub != nil {
				sub.Close()
			}
		}()

		for {
			// Bind late and re-bind freely: the store opens when a worker arms, and a
			// clear swaps it for a fresh one whose pings the old subscription never
			// carries.
			if sub == nil {
				if changes, ok := a.s.kubestoreMgr.Subscribe(int64(cacheID)); ok {
					sub = changes
					pings = sub.Chan()
				}
			}

			stats, err := a.measureCache(ctx, cacheID)
			if err != nil {
				return err
			}
			if !sent || stats != last {
				if !sendFrame(ctx, out, stats) {
					return nil
				}
				last, sent = stats, true
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			case _, ok := <-pings:
				if !ok {
					// The store closed under us — a clear, or a shutdown. Re-measure and
					// re-bind rather than ending: the cache is still the caller's.
					sub.Close()
					sub, pings = nil, nil
				}
			}
		}
	}), nil
}

// measureCache is one reading of a cache: its file, and what is in it.
func (a cachesAPI) measureCache(ctx context.Context, cacheID ClusterCacheID) (ClusterCacheStats, error) {
	stats, err := a.s.kubestoreMgr.Stats(ctx, int64(cacheID))
	if err != nil {
		return ClusterCacheStats{}, fmt.Errorf("measure cache %d: %w", cacheID, err)
	}
	return ClusterCacheStats{
		Exists:      stats.Exists,
		Bytes:       stats.Bytes,
		ObjectCount: stats.ObjectCount,
		KindCount:   stats.KindCount,
	}, nil
}

// WatchHealth reports each live cache's verdict. A read-side projection, never a stored
// condition: nothing in the object graph reacts to it, and storing it would wake the
// cache and every watcher each time its verdict moved.
//
// Nothing mirrors a kind into a cache today, so every verdict comes from the records —
// see cacheHealth. It re-emits on a cadence, which is what will carry the stamps a
// filled cache reports: those move in healthy steady state precisely when a change
// signal would be silent.
func (a cachesAPI) WatchHealth(ctx context.Context) (*Stream[ClusterCacheHealth], error) {
	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheHealth) error {
		ticker := time.NewTicker(a.s.gaugeCadence)
		defer ticker.Stop()

		sent := map[ClusterCacheID]ClusterCacheHealth{}
		for {
			healths, err := a.cacheHealth(ctx)
			if err != nil {
				return err
			}
			for _, health := range healths {
				if prev, ok := sent[health.CacheID]; ok && sameHealth(prev, health) {
					continue
				}
				if !sendFrame(ctx, out, health) {
					return nil
				}
				sent[health.CacheID] = health
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}), nil
}

// Clear empties a cache's store, swapping the file under whoever holds it open.
func (a cachesAPI) Clear(ctx context.Context, id ClusterCacheID) (*ClusterCache, error) {
	obj, err := a.s.cacheClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster cache %d: %w", id, err)
	}

	if err := a.s.kubestoreMgr.Clear(int64(id)); err != nil {
		return nil, fmt.Errorf("clear cluster cache %d: %w", id, err)
	}
	return toClusterCache(obj)
}

// cacheHealth is the verdict owed by every live cache, read off the records.
//
// It reads them rather than diffing against what a stream has sent, because a
// subscriber that arrives after a cache went quiet never witnessed the transition and
// would otherwise hear nothing about that cache at all. The gauge is latest-value with
// no departure frame, so silence reads as the last verdict — or as no verdict ever.
//
// The verdict comes from the pause switch the records carry (`cacheSyncEnabled`):
// `Paused` when it is off, `Connecting` when it is on, since nothing fills a cache yet.
// A cache being collected is skipped: its own record says it is going.
func (a cachesAPI) cacheHealth(ctx context.Context) ([]ClusterCacheHealth, error) {
	objs, err := a.s.cacheClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cluster caches: %w", err)
	}

	var out []ClusterCacheHealth
	clusters := map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{}
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		cluster, err := a.clusterFor(ctx, obj, clusters)
		if err != nil {
			return nil, err
		}
		if cluster == nil {
			// The cluster is gone, so this cache is going with it.
			continue
		}
		reason := ReasonConnecting
		if !cacheSyncEnabled(cluster, obj.Spec.ServerUID) {
			reason = ReasonPaused
		}
		out = append(out, ClusterCacheHealth{
			CacheID: ClusterCacheID(obj.ID), Status: ConditionFalse, Reason: reason,
		})
	}
	slices.SortFunc(out, func(a, b ClusterCacheHealth) int { return cmp.Compare(a.CacheID, b.CacheID) })
	return out, nil
}

// clusterFor resolves a cache's cluster, memoised for the fold: caches of one cluster
// share it, and this runs on the gauge's cadence.
func (a cachesAPI) clusterFor(
	ctx context.Context,
	cache *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
	seen map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus],
) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	owner, ok, err := cache.Owner()
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d owner: %w", cache.ID, err)
	}
	if !ok {
		return nil, nil
	}
	if cluster, held := seen[owner.ID]; held {
		return cluster, nil
	}

	cluster, err := a.s.clusterClient.Get(ctx, owner.ID)
	if errors.Is(err, beehive.ErrNotFound) {
		seen[owner.ID] = nil
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cluster %d: %w", owner.ID, err)
	}
	seen[owner.ID] = cluster
	return cluster, nil
}

// sameHealth reports whether a cache's verdict moved. The refs are a slice, so the
// struct is not comparable — and re-emitting an unchanged gauge is exactly what the
// cadence must not do.
func sameHealth(a, b ClusterCacheHealth) bool {
	return a.Status == b.Status && a.Reason == b.Reason &&
		a.TotalKinds == b.TotalKinds && a.UnhealthyKinds == b.UnhealthyKinds &&
		slices.Equal(a.UnhealthyKindRefs, b.UnhealthyKindRefs) &&
		sameTime(a.LastUpdateAt, b.LastUpdateAt) && sameTime(a.LastLiveAt, b.LastLiveAt)
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// nilIfZero keeps an unobserved stamp absent rather than reporting the epoch.
func nilIfZero(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// clusterCacheController reconciles one cache: today it creates the discovery anchor
// beneath it, carrying the pause switch its cluster decides. Provisioning the mirror
// itself is still to come.
type clusterCacheController struct {
	lifecycle.None
	// Every kind's client, not just this one's: a cache reads the cluster it hangs off
	// and writes the catalog it owns.
	deps
}

func (c *clusterCacheController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
) beehive.ReconcileResult {
	// A cache on its way out is about to be collected with the subtree it owns, and
	// beehive collects it with no finalizer to clear — so its file is deleted here or
	// never: the ids the paths are named for are gone with the records. The kinds
	// below stop clearing their own rows as soon as this mark lands (their owner walk
	// reads a deletion-pending cache as no cache), so nothing recreates the file.
	if obj.DeletionRequestedAt != nil {
		if err := c.kubestoreMgr.Remove(int64(obj.ID)); err != nil {
			return beehive.Fail(fmt.Errorf("remove cluster cache %d store: %w", obj.ID, err))
		}
		return beehive.Settled()
	}

	// The reconcile load carries no edges, so the owner is a lookup rather than a field.
	owner, ok, err := client.GetOwner(ctx)
	if err != nil {
		return beehive.Fail(fmt.Errorf("read cluster cache %d owner: %w", obj.ID, err))
	}
	if !ok {
		return beehive.Settled()
	}

	clusterObj, err := c.clusterClient.Get(ctx, owner.ID)
	// The cascade that takes this cache next may have collected the cluster already,
	// which is a race rather than a failure worth retrying under backoff.
	if errors.Is(err, beehive.ErrNotFound) {
		return beehive.Settled()
	}
	if err != nil {
		return beehive.Fail(fmt.Errorf("read cluster %d: %w", owner.ID, err))
	}
	// A cluster being torn down cascades here, so its subtree is not worth growing.
	if clusterObj.DeletionRequestedAt != nil {
		return beehive.Settled()
	}

	// The switch below is the cluster's, and an owner edge wakes nothing: without this,
	// a toggle flip would sit unrelayed until something else woke the cache. Beehive
	// records nothing when the edge is already there, so every later pass is free.
	if err := client.AddDependency(ctx, owner.ID); err != nil {
		return beehive.Fail(fmt.Errorf("depend cluster cache %d on its cluster: %w", obj.ID, err))
	}

	enabled := cacheSyncEnabled(clusterObj, obj.Spec.ServerUID)
	if err := ensureClusterCachedCatalog(ctx, c.catalogClient, ClusterCacheID(obj.ID), enabled); err != nil {
		return beehive.Fail(err)
	}
	// This pass writes no status of its own, so nothing else reports it converged, and
	// the owed pass would re-dispatch every cache — four store reads apiece — forever.
	return beehive.Settled()
}
