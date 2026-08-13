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

package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// Watch implements Caches — the ClusterCache-kind counterpart of
// Clusters().Watch, parent ClusterID resolved from the eager-loaded owner edge;
// the caller joins by ClusterID.
func (a cachesAPI) Watch(ctx context.Context) (<-chan domain.ClusterCacheWatchFrame, error) {
	snap, src, err := a.s.cacheClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "ClusterCache", snap, src,
		func(t domain.ChangeType, id beehive.ObjectID, obj *beehive.Object[domain.ClusterCacheSpec, domain.ClusterCacheStatus]) domain.ClusterCacheWatchFrame {
			if t == domain.ChangeBookmark {
				return domain.ClusterCacheWatchFrame{Type: t}
			}
			if obj == nil {
				return domain.ClusterCacheWatchFrame{Type: t, Cache: &domain.ClusterCache{ID: domain.ClusterCacheID(id)}}
			}
			cc := buildClusterCache(obj)
			return domain.ClusterCacheWatchFrame{Type: t, Cache: &cc}
		}), nil
}

// Get implements Caches. An unknown or deletion-pending id is (nil, nil), not an
// error — the same posture as Clusters().Get. The owner edge is eager-loaded so
// the record carries its ClusterID.
func (a cachesAPI) Get(ctx context.Context, id domain.ClusterCacheID) (*domain.ClusterCache, error) {
	obj, err := a.s.cacheClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if obj.DeletionRequestedAt != nil {
		return nil, nil
	}
	cc := buildClusterCache(obj)
	return &cc, nil
}

// List implements Caches — one cluster's caches off the owner edge, or every tracked
// cache when clusterID is nil. A cluster usually has one; a UID migration leaves the
// superseded cache in place until its subtree drains, which is why this is a list.
// Deletion-pending records are omitted, matching Clusters().List.
func (a cachesAPI) List(ctx context.Context, clusterID *domain.ClusterID) ([]*domain.ClusterCache, error) {
	var (
		objs []*beehive.Object[domain.ClusterCacheSpec, domain.ClusterCacheStatus]
		err  error
	)
	if clusterID != nil {
		objs, err = a.s.cacheClient.ListOwnedObjects(ctx, beehive.ObjectID(*clusterID), beehive.LoadOwner())
	} else {
		objs, err = a.s.cacheClient.List(ctx, beehive.LoadOwner())
	}
	if err != nil {
		return nil, err
	}
	caches := make([]*domain.ClusterCache, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		cc := buildClusterCache(obj)
		caches = append(caches, &cc)
	}
	return caches, nil
}

// buildClusterCache assembles a domain ClusterCache; parent ClusterID comes off
// the eager-loaded owner edge (see ownerObjectID).
func buildClusterCache(obj *beehive.Object[domain.ClusterCacheSpec, domain.ClusterCacheStatus]) domain.ClusterCache {
	return domain.ClusterCache{
		ID:         domain.ClusterCacheID(obj.ID),
		ClusterID:  domain.ClusterID(ownerObjectID(obj)),
		ServerUID:  obj.Spec.ServerUID,
		Conditions: obj.Conditions,
	}
}

// GetStats implements Caches. It stats the exact ClusterCache asked about —
// active or migrated-away — never "the" cache for a cluster.
func (a cachesAPI) GetStats(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (*domain.ClusterCacheStats, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	bytes, exists := a.s.cacheManager.CacheBytes(ref)
	if !exists {
		return &domain.ClusterCacheStats{}, nil
	}
	db := a.s.cacheManager.Lookup(ref.CacheID)
	if db == nil {
		return &domain.ClusterCacheStats{Exists: true, Bytes: bytes}, nil
	}
	st, err := readCacheStats(ctx, db, bytes)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// readCacheStats rolls up the kind catalog's trigger-maintained per-kind counts
// (O(kinds), no object scan): total objects and the number of non-empty kinds. The
// catalog lists advertised-but-empty kinds too, so KindCount only counts count > 0.
func readCacheStats(ctx context.Context, db *store.ClusterDB, bytes int64) (domain.ClusterCacheStats, error) {
	rows, err := db.Kinds(ctx)
	if err != nil {
		return domain.ClusterCacheStats{}, err
	}
	objectCount, kindCount := 0, 0
	for _, r := range rows {
		// Events carry a real kind_counts value but live in their own table and are
		// not objects — excluded from the whole-cache totals.
		if r.APIVersion == domain.EventsAPIVersion && r.Kind == domain.EventsKind {
			continue
		}
		if r.Count > 0 {
			objectCount += r.Count
			kindCount++
		}
	}
	return domain.ClusterCacheStats{
		Exists:      true,
		Bytes:       bytes,
		ObjectCount: objectCount,
		KindCount:   kindCount,
	}, nil
}

// WatchStats implements Caches — one cache's contents as a live gauge. Its own
// stream because a ClusterCache field would freeze at subscribe time; see
// docs/adr/2026-08-09-status-propagation-gauges.md.
func (a cachesAPI) WatchStats(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheStats, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheGaugeWatch(ctx, a.s.cacheManager, ref.CacheID, a.s.cacheStatsDebounce,
		// Keyless object-write broker: every kind's writes move this total.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) { return db.ObjectsSubscribe() },
		func(ctx context.Context, db *store.ClusterDB) (domain.ClusterCacheStats, error) {
			// Re-stat the file each read; nothing else carries its growth.
			bytes, _ := a.s.cacheManager.CacheBytes(ref)
			return readCacheStats(ctx, db, bytes)
		},
		// A closed cache reports what is on DISK, not zeroes: an open handle only
		// means something is syncing now, and reporting a closed file as
		// nonexistent would disable Clear cache on the rows the Orphaned group
		// exists to reclaim. Counts need an open handle, hence the separate
		// Exists field.
		func() domain.ClusterCacheStats {
			bytes, exists := a.s.cacheManager.CacheBytes(ref)
			return domain.ClusterCacheStats{Exists: exists, Bytes: bytes}
		},
	), nil
}

// ListEvents implements Caches — the ClusterCache-kind entrypoint to the
// generic event reader (over the cache client).
func (a cachesAPI) ListEvents(ctx context.Context, id domain.ClusterCacheID, category *string, limit *int) ([]domain.Event, error) {
	return a.s.events(ctx, a.s.cacheClient, beehive.ObjectID(id), category, limit)
}

// WatchEvents implements Caches — the ClusterCache-kind entrypoint to the
// generic event watch (over the cache client).
func (a cachesAPI) WatchEvents(ctx context.Context, id domain.ClusterCacheID, category *string) (<-chan domain.Event, error) {
	return a.s.watchEvents(ctx, a.s.cacheClient, beehive.ObjectID(id), category)
}

// clearCacheTimeout bounds Clear's detached drain → delete → restart sequence —
// generous, there to stop a wedged step, not to pace anything.
const clearCacheTimeout = 2 * time.Minute

// Clear implements Caches: validate the cluster exists, delete the on-disk
// cache, restart its syncs so it rebuilds.
func (a cachesAPI) Clear(ctx context.Context, id domain.ClusterID) (*domain.Cluster, error) {
	obj, err := a.s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	// No ClusterCache object yet → no files to delete; skip to the (no-op) restart.
	ref, found, err := a.s.cacheRef(ctx, id)
	if err != nil {
		return nil, err
	}
	if found {
		// Delete INSIDE the worker restart: DeleteCacheFiles closes the ClusterDB the
		// workers hold, so they must drain first and rebuild on the Manager's new
		// handle — a registered worker on the dead handle would block the reconcile
		// from replacing it, and nothing else ever rebuilds workers (a reconcile
		// leaves a running one alone). Detached from the request context: once the
		// drain begins the sequence must run to its end, or an abandoned mutation
		// leaves files deleted and no worker rebuilt. Bounded by clearCacheTimeout.
		clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), clearCacheTimeout)
		defer cancel()

		deleteFiles := func() error { return a.s.cacheManager.DeleteCacheFiles(clearCtx, ref) }
		if a.s.gvrSyncCtrl == nil {
			if derr := deleteFiles(); derr != nil {
				return nil, derr
			}
		} else if derr := a.s.gvrSyncCtrl.RestartCacheWorkers(clearCtx, ref, deleteFiles); derr != nil {
			return nil, derr
		}
	}
	c := a.s.buildCluster(obj)
	return &c, nil
}
