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
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
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

// ClusterCacheHealth is one cache's sync verdict over every kind it syncs.
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
	// TotalKinds is how many kinds this cache mirrors; UnhealthyKinds how many are not
	// currently Watching (len(UnhealthyKindRefs), which is capped by nothing).
	TotalKinds     int
	UnhealthyKinds int
	// PausedKinds is how many of TotalKinds the user has switched off. Counted separately
	// rather than subtracted from the census: a paused kind is not unhealthy and not
	// missing, and without its own tally it is invisible in every reading — a summary that
	// spends UnhealthyKinds alone would report five kinds syncing when two are paused.
	PausedKinds int
	// LastUpdateAt is the most recent write across every kind — when data last arrived
	// anywhere in this cache; nil until something has landed. LastLiveAt is the OLDEST
	// proof across every kind, and **nil while any of them has none**: a cache is only as
	// verified as its least proven watch, and a kind nothing has proven live is weaker
	// than any stamp its neighbours carry.
	LastUpdateAt *time.Time
	LastLiveAt   *time.Time
}

// ClusterCacheSyncStatus is one cache's sync detail: how discovery is doing, and a row per
// kind actually being mirrored. A gauge, like the health rollup and for the same reason —
// nothing in the object graph reacts to it, and its counts and freshness stamps move while
// every record under it sits still.
//
// Where ClusterCacheHealth folds a cache into one verdict for a fleet view, this expands one
// cache for the panel a user opened.
type ClusterCacheSyncStatus struct {
	CacheID   ClusterCacheID
	Discovery ClusterCacheDiscoveryStatus
	// Kinds is every mirrored kind, sorted by (APIVersion, Resource) so a rendered list is
	// stable across ticks.
	Kinds []ClusterCacheKindSyncStatus
}

// ClusterCacheDiscoveryStatus is the sweep's own verdict, which is not any kind's: a cluster
// whose /apis document will not load has no kinds to report a failure through.
type ClusterCacheDiscoveryStatus struct {
	Reason  string
	Message string
}

// ClusterCacheKindSyncStatus is one mirrored kind's row. Identity, verdict, freshness, and
// the count — assembled from the two places that own them, and stored in neither.
type ClusterCacheKindSyncStatus struct {
	APIVersion string
	Kind       string
	Resource   string

	// Reason and Message are nil-free: a kind whose worker has committed nothing reads as
	// empty, which is what "no answer yet" looks like everywhere in this family.
	Reason  string
	Message string
	// SinceAt is when Reason last moved — "watching since 10:02". Nil until it has.
	SinceAt *time.Time
	// LastUpdateAt is when data last arrived; LastLiveAt the last proof the stream is live.
	LastUpdateAt *time.Time
	LastLiveAt   *time.Time
	// Restarts counts comebacks inside the current healthy stretch — the flapping question a
	// retry streak cannot answer. NextRetryAt is set only while a run is down.
	Restarts    int
	NextRetryAt *time.Time
	// ObjectCount comes off the store's trigger-maintained per-kind counts, never off the
	// sync seam: kubesync knows only the caches it has armed, where kubestore answers for a
	// paused one too.
	ObjectCount int
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
// It re-emits on a cadence, which is what carries the stamps a filled cache reports: those
// move in healthy steady state precisely when a change signal would be silent.
func (a cachesAPI) WatchHealth(ctx context.Context) (*Stream[ClusterCacheHealth], error) {
	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheHealth) error {
		ticker := time.NewTicker(a.s.gaugeCadence)
		defer ticker.Stop()

		sent := map[ClusterCacheID]ClusterCacheHealth{}
		for {
			healths, err := a.readAllCacheHealth(ctx)
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

// readCacheHealth folds one cache's verdict over every kind it mirrors — the records, which
// is what is actually being synced, rather than everything the cluster serves.
//
// **No answer is not an empty answer.** A kind whose worker has committed nothing is not one
// that stopped syncing — a cache still starting, a clear in progress — so it is neither an
// offender nor proof of health: the cache reads as connecting until every kind has spoken.
func (a cachesAPI) readCacheHealth(ctx context.Context, cacheID ClusterCacheID) (ClusterCacheHealth, error) {
	kindObjs, err := a.s.kindClient.ListOwnedObjects(ctx, beehive.ObjectID(cacheID))
	if err != nil {
		return ClusterCacheHealth{}, fmt.Errorf("list cluster cache %d cached kinds: %w", cacheID, err)
	}

	health := ClusterCacheHealth{CacheID: cacheID, Status: ConditionFalse, Reason: ReasonConnecting}
	var (
		anyKindUnanswered bool
		anyKindUnproven   bool
		newestUpdateAt    time.Time
		oldestLiveAt      time.Time
		reasonByRef       = map[SyncedKindRef]string{}
	)
	for _, kindObj := range kindObjs {
		if kindObj.DeletionRequestedAt != nil {
			continue
		}
		health.TotalKinds++
		// Skipped ahead of the state read, not filtered out of the tallies after it: a
		// paused kind is forgotten, so reading its state would report it as unanswered —
		// and one unanswered kind pins the whole cache at Connecting forever.
		if kindObj.Spec.Paused {
			health.PausedKinds++
			continue
		}

		state, ok := a.s.kubesyncSvc.GetKindState(int64(cacheID), toKubestoreKind(kindObj.Spec))
		if !ok {
			// Unproven as well as unanswered: a kind that has said nothing has proved nothing,
			// so it withholds the cache's proof like any other kind carrying none.
			anyKindUnanswered, anyKindUnproven = true, true
			continue
		}
		if state.Reason != kubesync.ReasonWatching {
			ref := syncedKindRefOf(kindObj.Spec)
			health.UnhealthyKindRefs = append(health.UnhealthyKindRefs, ref)
			reasonByRef[ref] = state.Reason
		}
		if state.LastUpdateAt.After(newestUpdateAt) {
			newestUpdateAt = state.LastUpdateAt
		}
		// The OLDEST proof across every kind: a cache is only as verified as its least proven
		// watch. A kind carrying no proof is tracked apart from the minimum rather than folded
		// into it, since a zero time is this loop's "nothing yet" as well as a value — folded
		// in, the next kind's stamp would overwrite it and the answer would turn on the order
		// the list came back in.
		switch {
		case state.LastLiveAt.IsZero():
			anyKindUnproven = true
		case oldestLiveAt.IsZero() || state.LastLiveAt.Before(oldestLiveAt):
			oldestLiveAt = state.LastLiveAt
		}
	}

	health.UnhealthyKinds = len(health.UnhealthyKindRefs)
	slices.SortFunc(health.UnhealthyKindRefs, compareSyncedKindRefs)
	health.LastUpdateAt = optionalTime(newestUpdateAt)
	if !anyKindUnproven {
		health.LastLiveAt = optionalTime(oldestLiveAt)
	}

	switch {
	case health.PausedKinds == health.TotalKinds && health.TotalKinds > 0:
		// Ahead of every arm below: paused kinds are skipped, so a fully paused cache has
		// no offenders and nothing unanswered — and the Watching arm would call it healthy.
		health.Reason = ReasonPaused
	case storeFailed(a.s.kubesyncSvc.GetDiscoveryState(int64(cacheID))):
		// The cache's own verdict, above the per-kind fold: a file that will not open arms
		// nothing, so every kind reads as unanswered and the default below would report a
		// permanently broken cache as still connecting.
		health.Reason = kubesync.ReasonStoreFailed
	case health.TotalKinds == 0 || anyKindUnanswered:
		// Still connecting, which is not Paused: an enabled cluster's cache has no kinds
		// until its sweep has run, and calling that paused would show a user their own
		// enabled cluster as switched off.
	case health.UnhealthyKinds == 0:
		health.Status, health.Reason = ConditionTrue, kubesync.ReasonWatching
	default:
		// The first offender's own reason, so a consumer reading the verdict beside the
		// first ref sees one story.
		health.Reason = reasonByRef[health.UnhealthyKindRefs[0]]
	}
	return health, nil
}

// storeFailed reads the one discovery verdict the rollup folds, in the shape the getter
// answers in.
func storeFailed(state kubesync.DiscoveryState, ok bool) bool {
	return ok && state.Reason == kubesync.ReasonStoreFailed
}

// syncedKindRefOf is the (APIVersion, Resource) pair that identifies a kind on the wire.
func syncedKindRefOf(spec ClusterCachedKindSpec) SyncedKindRef {
	return SyncedKindRef{APIVersion: spec.APIVersion, Resource: spec.Resource}
}

// compareSyncedKindRefs orders kinds by apiVersion, then resource, so rendered lists are
// stable across ticks.
func compareSyncedKindRefs(a, b SyncedKindRef) int {
	return cmp.Or(cmp.Compare(a.APIVersion, b.APIVersion), cmp.Compare(a.Resource, b.Resource))
}

// WatchSyncStatus streams one cache's sync detail as a live gauge: the discovery verdict and
// a row per mirrored kind. The only thing on the wire that carries a per-kind verdict.
//
// It re-reads on the cadence and never on a signal, because its counts and freshness stamps
// move with no reason change — the only thing the sync seam signals on — so a gauge waiting
// for one would go quiet exactly while the cache is healthy.
func (a cachesAPI) WatchSyncStatus(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCacheSyncStatus], error) {
	live, err := a.s.cacheBelongsTo(ctx, clusterID, cacheID)
	if err != nil {
		return nil, err
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterCacheSyncStatus) error {
		if !live {
			<-ctx.Done()
			return nil
		}
		ticker := time.NewTicker(a.s.gaugeCadence)
		defer ticker.Stop()

		var (
			sent    ClusterCacheSyncStatus
			hasSent bool
		)
		for {
			status, err := a.readSyncStatus(ctx, cacheID)
			if err != nil {
				return err
			}
			// Only when it moved: a gauge is current-on-subscribe, so the first read always
			// goes out, and an idle cache then says nothing rather than a frame per tick.
			if !hasSent || !sameSyncStatus(sent, status) {
				if !sendFrame(ctx, out, status) {
					return nil
				}
				sent, hasSent = status, true
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}), nil
}

// readSyncStatus is one reading of a cache's sync detail.
func (a cachesAPI) readSyncStatus(ctx context.Context, cacheID ClusterCacheID) (ClusterCacheSyncStatus, error) {
	kindObjs, err := a.s.kindClient.ListOwnedObjects(ctx, beehive.ObjectID(cacheID))
	if err != nil {
		return ClusterCacheSyncStatus{}, fmt.Errorf("list cluster cache %d cached kinds: %w", cacheID, err)
	}
	objectCounts, err := a.readObjectCountsByKind(ctx, cacheID)
	if err != nil {
		return ClusterCacheSyncStatus{}, err
	}

	status := ClusterCacheSyncStatus{CacheID: cacheID}
	if discovery, ok := a.s.kubesyncSvc.GetDiscoveryState(int64(cacheID)); ok {
		status.Discovery = ClusterCacheDiscoveryStatus{Reason: discovery.Reason, Message: discovery.Message}
	}
	for _, kindObj := range kindObjs {
		if kindObj.DeletionRequestedAt != nil {
			continue
		}
		row := ClusterCacheKindSyncStatus{
			APIVersion:  kindObj.Spec.APIVersion,
			Kind:        kindObj.Spec.Kind,
			Resource:    kindObj.Spec.Resource,
			ObjectCount: objectCounts[syncedKindRefOf(kindObj.Spec)],
		}
		// The record's own verdict first: a paused kind is forgotten, so the read below
		// would come back empty and the row would carry no reason at all.
		if kindObj.Spec.Paused {
			row.Reason = ReasonPaused
		} else if state, ok := a.s.kubesyncSvc.GetKindState(int64(cacheID), toKubestoreKind(kindObj.Spec)); ok {
			row.Reason = state.Reason
			row.Message = state.Message
			row.Restarts = state.Restarts
			row.SinceAt = optionalTime(state.SinceAt)
			row.LastUpdateAt = optionalTime(state.LastUpdateAt)
			row.LastLiveAt = optionalTime(state.LastLiveAt)
			row.NextRetryAt = optionalTime(state.NextRetryAt)
		}
		status.Kinds = append(status.Kinds, row)
	}
	slices.SortFunc(status.Kinds, func(a, b ClusterCacheKindSyncStatus) int {
		return cmp.Or(cmp.Compare(a.APIVersion, b.APIVersion), cmp.Compare(a.Resource, b.Resource))
	})
	return status, nil
}

// readObjectCountsByKind is every mirrored kind's row count in one read. A cache with no file
// has none, which is a paused cache or one nothing has swept — not an error.
func (a cachesAPI) readObjectCountsByKind(ctx context.Context, cacheID ClusterCacheID) (map[SyncedKindRef]int, error) {
	store, ok, err := a.s.kubestoreMgr.OpenExisting(int64(cacheID))
	if err != nil {
		return nil, fmt.Errorf("open cluster cache %d: %w", cacheID, err)
	}
	if !ok {
		return nil, nil
	}
	defer store.Release()

	rows, err := store.Kinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d kinds: %w", cacheID, err)
	}
	counts := make(map[SyncedKindRef]int, len(rows))
	for _, row := range rows {
		counts[SyncedKindRef{APIVersion: row.APIVersion, Resource: row.Resource}] = row.Count
	}
	return counts, nil
}

// optionalTime is a stamp as the wire carries it: absent rather than the zero instant, which
// a consumer would otherwise render as 1 January year 1.
func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// Clear empties a cache's store, swapping the file under whoever holds it open. The workers
// writing through it are stopped for the whole swap and started again after — a clear that
// failed leaves a cache that still syncs.
func (a cachesAPI) Clear(ctx context.Context, id ClusterCacheID) (*ClusterCache, error) {
	obj, err := a.s.cacheClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster cache %d: %w", id, err)
	}

	// Inside the sync's hold: the manager swaps the file under whoever holds it open, and a
	// relist page or a catalog write landing across that swap goes into a file nothing holds.
	// The claims stay taken throughout, which is what the manager reopens a fresh file for.
	if err := a.s.kubesyncSvc.RunWithCacheSyncStopped(int64(id), func() error {
		return a.s.kubestoreMgr.Clear(int64(id))
	}); err != nil {
		return nil, fmt.Errorf("clear cluster cache %d: %w", id, err)
	}
	return toClusterCache(obj)
}

// readAllCacheHealth is every live cache's verdict, read off the records.
//
// It reads them rather than diffing against what a stream has sent, because a subscriber
// that arrives after a cache went quiet never witnessed the transition and would otherwise
// hear nothing about that cache at all. The gauge is latest-value with no departure frame,
// so silence reads as the last verdict — or as no verdict ever.
//
// A cache whose pause switch (`cacheSyncEnabled`) is off reads Paused outright; one that is
// on folds its kinds. A cache being collected is skipped: its own record says it is going.
func (a cachesAPI) readAllCacheHealth(ctx context.Context) ([]ClusterCacheHealth, error) {
	cacheObjs, err := a.s.cacheClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cluster caches: %w", err)
	}

	var healths []ClusterCacheHealth
	clusters := map[beehive.ObjectID]*beehive.Object[ClusterSpec, ClusterStatus]{}
	for _, cacheObj := range cacheObjs {
		if cacheObj.DeletionRequestedAt != nil {
			continue
		}
		cluster, err := a.clusterFor(ctx, cacheObj, clusters)
		if err != nil {
			return nil, err
		}
		if cluster == nil {
			// The cluster is gone, so this cache is going with it.
			continue
		}
		cacheID := ClusterCacheID(cacheObj.ID)
		if !cacheSyncEnabled(cluster, cacheObj.Spec.ServerUID) {
			healths = append(healths, ClusterCacheHealth{CacheID: cacheID, Status: ConditionFalse, Reason: ReasonPaused})
			continue
		}
		health, err := a.readCacheHealth(ctx, cacheID)
		if err != nil {
			return nil, err
		}
		healths = append(healths, health)
	}
	slices.SortFunc(healths, func(a, b ClusterCacheHealth) int { return cmp.Compare(a.CacheID, b.CacheID) })
	return healths, nil
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
		a.PausedKinds == b.PausedKinds &&
		slices.Equal(a.UnhealthyKindRefs, b.UnhealthyKindRefs) &&
		sameTime(a.LastUpdateAt, b.LastUpdateAt) && sameTime(a.LastLiveAt, b.LastLiveAt)
}

// sameSyncStatus is equality for the gauge's dedupe. Hand-written because the row carries
// optional times as pointers, which == would compare by address.
func sameSyncStatus(a, b ClusterCacheSyncStatus) bool {
	return a.CacheID == b.CacheID && a.Discovery == b.Discovery &&
		slices.EqualFunc(a.Kinds, b.Kinds, sameKindSyncStatus)
}

func sameKindSyncStatus(a, b ClusterCacheKindSyncStatus) bool {
	return a.APIVersion == b.APIVersion && a.Kind == b.Kind && a.Resource == b.Resource &&
		a.Reason == b.Reason && a.Message == b.Message &&
		a.Restarts == b.Restarts && a.ObjectCount == b.ObjectCount &&
		sameTime(a.SinceAt, b.SinceAt) && sameTime(a.LastUpdateAt, b.LastUpdateAt) &&
		sameTime(a.LastLiveAt, b.LastLiveAt) && sameTime(a.NextRetryAt, b.NextRetryAt)
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// categoryDiscovery and categorySync name the two timelines this subsystem writes: the
// cache's own sweep verdicts, and one kind's worker transitions. The axis beehive bounds
// retention on, which is why each is a category rather than a reason prefix.
const (
	categoryDiscovery = "discovery"
	categorySync      = "sync"
)

// clusterCacheController arms one cache's sync, mirrors what its cluster serves into the
// per-kind records it owns, and logs the sweep's verdict to its own timeline.
type clusterCacheController struct {
	lifecycle.None
	// Every kind's client, not just this one's: the per-kind records a cache owns are
	// written from here.
	deps
}

func (c *clusterCacheController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
) beehive.ReconcileResult {
	cacheID := int64(obj.ID)

	// A cache on its way out is about to be collected with the subtree it owns, and
	// beehive collects it with no finalizer to clear — so its file is deleted here or
	// never: the ids the paths are named for are gone with the records. Remove
	// tombstones the id, so nothing below can claim the file back after this.
	if obj.DeletionRequestedAt != nil {
		// First, and that is the whole ordering: it returns only once nothing can still
		// write through the file the next line deletes. ForgetCache rather than
		// ForgetDiscovery, because the kinds go with the cache — a pause is what keeps them.
		c.kubesyncSvc.ForgetCache(cacheID)
		if err := c.kubestoreMgr.Remove(cacheID); err != nil {
			return beehive.Fail(fmt.Errorf("remove cluster cache %d store: %w", obj.ID, err))
		}
		return beehive.Settled()
	}

	if err := c.armSync(ctx, client, obj); err != nil {
		return beehive.Fail(err)
	}
	if err := c.mirrorKinds(ctx, obj); err != nil {
		return beehive.Fail(err)
	}
	if err := c.logDiscoveryVerdict(ctx, client, cacheID); err != nil {
		return beehive.Fail(err)
	}
	// Settling is this pass's to report: an object left unsettled is re-dispatched by
	// beehive's owed pass forever.
	return beehive.Settled()
}

// armSync relays the pause switch onto the whole subtree, in one call each way. Arming is
// policy and never interest: nothing a reader does starts a sync, and pausing writes no
// record and requeues none — the kinds stay registered under kubesync, so a resume starts
// every one of them again.
//
// The switch is NOT relayed onto the per-kind records: the two levels AND rather than nest, so
// pausing is one call here rather than a write onto hundreds of children.
func (c *clusterCacheController) armSync(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
) error {
	cacheID := int64(obj.ID)

	clusterObj, err := c.loadCluster(ctx, obj)
	if err != nil {
		return err
	}
	// **A relayed value needs a depends_on edge; the owner edge is not one.** The switch below
	// lives on the cluster and is never written to ClusterCacheSpec, which is identity-only —
	// so without this a paused cluster's cache keeps syncing until something unrelated wakes
	// it. Beehive records nothing when the edge is already there, so every later pass is free.
	if err := client.AddDependency(ctx, clusterObj.ID); err != nil {
		return fmt.Errorf("depend cluster cache %d on its cluster: %w", obj.ID, err)
	}

	// Only a kubeconfig-sourced record has credentials to dial over. Another source's are
	// not this pass's to guess at, so nothing is armed rather than a session with no context.
	kubeconfigSource := clusterObj.Spec.Source.Kubeconfig
	if kubeconfigSource == nil || !cacheSyncEnabled(clusterObj, obj.Spec.ServerUID) {
		c.kubesyncSvc.ForgetDiscovery(cacheID)
		return nil
	}
	c.kubesyncSvc.TrackDiscovery(cacheID, kubesync.Params{
		ContextName: kubeconfigSource.Context,
		ServerUID:   obj.Spec.ServerUID,
	})
	return nil
}

// loadCluster loads the cluster a cache hangs off. A cache with no owner is unreachable
// through the object graph — beehive's GC takes it — so this reports the error rather than
// inventing a cluster whose switches would all read off.
func (c *clusterCacheController) loadCluster(
	ctx context.Context,
	obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus],
) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	owner, ok, err := c.cacheClient.GetOwner(ctx, obj.ID)
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d owner: %w", obj.ID, err)
	}
	if !ok {
		return nil, fmt.Errorf("cluster cache %d has no cluster", obj.ID)
	}
	clusterObj, err := c.clusterClient.Get(ctx, owner.ID)
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d cluster: %w", obj.ID, err)
	}
	return clusterObj, nil
}

// mirrorKinds converges the per-kind records onto what the cluster serves: a record per
// catalog row, and no record for a row that is gone. A set reconcile, since
// ClusterCachedKindName is the dedup key and needs no per-child bookkeeping.
//
// **The desired set comes off disk, never off the seam.** kind_catalog is the one place the
// served set lives, so a partial sweep is a table that kept its rows and a restart has an
// answer before any sweep runs.
func (c *clusterCacheController) mirrorKinds(ctx context.Context, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) error {
	catalogRows, everSwept, err := c.readKindCatalog(ctx, int64(obj.ID))
	if err != nil {
		return err
	}

	desired := make(map[string]ClusterCachedKindSpec, len(catalogRows))
	for _, row := range catalogRows {
		desired[ClusterCachedKindName(obj.ID, row.APIVersion, row.Resource)] = ClusterCachedKindSpec{
			APIVersion: row.APIVersion,
			Kind:       row.Kind,
			Resource:   row.Resource,
			Namespaced: row.Scope == kubestore.ScopeNamespaced,
		}
	}

	// One read for both halves: the sweep runs on a cadence over a cache with hundreds of
	// kinds, and both the write below and the prune answer off the same set of records.
	stored, err := c.storedKinds(ctx, obj.ID)
	if err != nil {
		return err
	}
	if err := c.upsertKinds(ctx, obj.ID, desired, stored); err != nil {
		return err
	}
	// **An unswept table deletes nothing.** No fingerprint means no answer has ever been
	// written there, which is not the same as a cluster that serves nothing — and only the
	// second of those may prune.
	if !everSwept {
		return nil
	}
	return c.pruneKinds(ctx, desired, stored)
}

// storedKinds is every record under a cache, by name — deletion-pending ones included,
// which is what lets the prune below tell a record already going from one to mark.
func (c *clusterCacheController) storedKinds(
	ctx context.Context,
	cacheID beehive.ObjectID,
) (map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus], error) {
	kindObjs, err := c.kindClient.ListOwnedObjects(ctx, cacheID)
	if err != nil {
		return nil, fmt.Errorf("list cluster cache %d cached kinds: %w", cacheID, err)
	}
	stored := make(map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus], len(kindObjs))
	for _, kindObj := range kindObjs {
		stored[kindObj.Name] = kindObj
	}
	return stored, nil
}

// upsertKinds writes one record per desired kind whose catalog fields moved, and nothing
// at all for the rest.
//
// **Writing every pass would un-pause every kind within one discovery interval.** The
// desired spec is built from the catalog row alone, so it carries the zero value of the
// one field the catalog does not own — and this pass runs on a cadence, against a cache
// with hundreds of kinds.
// The carry-forward below reads the record again under the lock rather than trusting the
// snapshot: the pass lists before it locks, so a pause landing in that window is already
// stored and missing from what it holds. Rereading rather than widening the lock over the
// list — that list is hundreds of kinds on a cadence, and holding the setter's lock across
// it would park every user mutation behind a sweep, where the reread costs one read on the
// rare pass where a catalog field actually moved.
func (c *clusterCacheController) upsertKinds(
	ctx context.Context,
	cacheID beehive.ObjectID,
	desired map[string]ClusterCachedKindSpec,
	stored map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus],
) error {
	c.kindSpecMu.Lock()
	defer c.kindSpecMu.Unlock()

	for name, spec := range desired {
		if obj, held := stored[name]; held {
			if sameCatalogFields(obj.Spec, spec) {
				continue
			}
			// The user's switch rides through a catalog change: the desired spec is built
			// from the row alone, so it carries Paused's zero value and writing it whole
			// would resume a kind because its singular was renamed.
			paused, err := c.storedPause(ctx, obj.ID)
			if err != nil {
				return err
			}
			spec.Paused = paused
		}
		// CreateOrUpdate rather than the GetOrCreate that creates a cache, whose name IS its
		// whole spec: a kind's carries data outside its name — the singular, and the scope —
		// so a renamed or re-scoped kind converges in place.
		_, _, err := c.kindClient.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(cacheID))
		// A record already marked for deletion holds its name until GC releases it; the kind
		// is created again by a later pass, off the same catalog row.
		if err != nil && !errors.Is(err, beehive.ErrDeletionPending) {
			return fmt.Errorf("mirror cached kind %s: %w", name, err)
		}
	}
	return nil
}

// storedPause is one record's switch as it stands now. A record collected since the pass
// listed it has no pause to carry, and the write that follows creates it afresh.
func (c *clusterCacheController) storedPause(ctx context.Context, id beehive.ObjectID) (bool, error) {
	obj, err := c.kindClient.Get(ctx, id)
	if errors.Is(err, beehive.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cached kind %d: %w", id, err)
	}
	return obj.Spec.Paused, nil
}

// sameCatalogFields reports whether the four fields the discovery sweep owns match. Paused
// is deliberately absent: it is the user's, and a difference there is not the sweep's to
// converge.
func sameCatalogFields(a, b ClusterCachedKindSpec) bool {
	return a.APIVersion == b.APIVersion && a.Kind == b.Kind &&
		a.Resource == b.Resource && a.Namespaced == b.Namespaced
}

// pruneKinds marks every record the desired set no longer names. Marked, not collected: the
// record's own pass clears the rows behind it first.
func (c *clusterCacheController) pruneKinds(
	ctx context.Context,
	desired map[string]ClusterCachedKindSpec,
	stored map[string]*beehive.Object[ClusterCachedKindSpec, ClusterCachedKindStatus],
) error {
	for name, kindObj := range stored {
		if _, wanted := desired[name]; wanted || kindObj.DeletionRequestedAt != nil {
			continue
		}
		if err := c.kindClient.Delete(ctx, kindObj.ID); err != nil && !errors.Is(err, beehive.ErrNotFound) {
			return fmt.Errorf("drop cached kind %s: %w", name, err)
		}
	}
	return nil
}

// readKindCatalog is the served set as the sweep last wrote it, plus whether one ever has.
//
// Three properties this rests on: OpenExisting never CREATES a file, so a pass before any
// sweep prunes nothing; the fingerprint's absence is the "never swept" bit; and rows and
// fingerprint come out of one read transaction, so a stale fingerprint can never pass its
// check beside a clear's empty table.
func (c *clusterCacheController) readKindCatalog(ctx context.Context, cacheID int64) (rows []kubestore.KindRow, everSwept bool, err error) {
	store, ok, err := c.kubestoreMgr.OpenExisting(cacheID)
	if err != nil {
		return nil, false, fmt.Errorf("open cluster cache %d: %w", cacheID, err)
	}
	if !ok {
		return nil, false, nil
	}
	defer store.Release()

	rows, _, everSwept, err = store.KindsWithFingerprint(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("read cluster cache %d kind catalog: %w", cacheID, err)
	}
	return rows, everSwept, nil
}

// logDiscoveryVerdict records the sweep's verdict on the cache's own timeline. Every pass,
// because repeating a run's (Category, Type, Reason) extends that run rather than appending —
// so a flapping sweep costs one row per transition and a settled one costs nothing.
//
// A session suspended for want of a connection writes nothing: that fact is the CLUSTER's,
// already on its own timeline, and logging it per cache is the same news twice.
func (c *clusterCacheController) logDiscoveryVerdict(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCacheStatus],
	cacheID int64,
) error {
	state, ok := c.kubesyncSvc.GetDiscoveryState(cacheID)
	if !ok || state.Reason == kubesync.ReasonNoConnection || state.Reason == kubesync.ReasonIdentityMismatch {
		return nil
	}
	if err := client.AddEvent(ctx, beehive.EventSpec{
		Category: categoryDiscovery,
		Type:     discoveryEventType(state.Reason),
		Reason:   state.Reason,
		Message:  state.Message,
	}); err != nil {
		return fmt.Errorf("log cluster cache %d discovery: %w", cacheID, err)
	}
	return nil
}

// discoveryEventType grades a sweep verdict. Partial is a warning rather than a failure: the
// catalog refreshed, and one group-version did not answer.
func discoveryEventType(reason string) beehive.EventType {
	switch reason {
	case kubesync.ReasonDiscoveryFailed, kubesync.ReasonPartial:
		return beehive.EventWarning
	default:
		return beehive.EventNormal
	}
}
