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
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the boundary between the frontend (GraphQL today, gRPC
// later) and the cluster backend. Every beehive detail — slugs, the
// ClusterSource → Cluster → ClusterCache owner chain, the spec/status split, the
// ClusterCache status join, the two-channel watch merge — lives behind it, so
// callers deal only in the domain Cluster type.
type ClusterService interface {
	// List returns every tracked cluster that is not deletion-pending, each with
	// its ClusterCache sync status joined in.
	List(ctx context.Context) ([]*Cluster, error)
	// Get returns one cluster by id, or (nil, nil) when it is unknown or
	// deletion-pending.
	Get(ctx context.Context, id ClusterID) (*Cluster, error)
	// Watch emits the current cluster list on subscribe, then re-emits the full
	// list on every Cluster or ClusterCache change. The channel closes when ctx
	// ends.
	Watch(ctx context.Context) (<-chan []*Cluster, error)
	// CacheStats returns live per-cluster cache statistics.
	CacheStats(ctx context.Context, id ClusterID) (*CacheStats, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// RetryConnection forces an immediate re-probe (resetting the connection
	// controller's failure backoff). The outcome lands on the record's
	// conditions and reaches watchers through Watch.
	RetryConnection(ctx context.Context, id ClusterID) error
	// ClearCache deletes the on-disk cache and bounces the sync engine; the
	// (returned) record stays.
	ClearCache(ctx context.Context, id ClusterID) (*Cluster, error)
	// Delete removes the cluster by deleting its parent ClusterSource so beehive
	// GC cascades to Cluster → ClusterCache.
	Delete(ctx context.Context, id ClusterID) error
}

// Service is the concrete ClusterService and the whole cluster control plane: it
// owns the beehive store + instance, the kubeconfig watcher, the three beehive
// clients, the three controllers (registered with beehive in New), the kubeconfig
// importer, the poke→sync forwarder, and the per-cluster cache manager.
type Service struct {
	bh      *beehive.Beehive
	bhStore beehive.Store
	bhStop  func(context.Context) error

	watcher *k8shelpers.KubeConfigWatcher

	srcClient     beehive.Client[ClusterSourceSpec, ClusterSourceObjStatus]
	clusterClient beehive.Client[ClusterSpec, ClusterConnectionStatus]
	cacheClient   beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	cacheManager  *store.Manager

	importer *KubeconfigImporter
	pokeSvc  *poke.Service

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ ClusterService = (*Service)(nil)

// New builds the cluster control plane: the kubeconfig watcher (over
// kubeconfigPath), the beehive store + instance (at <dataDir>/beehive.db), the
// three beehive clients, the three registered controllers (ClusterSource,
// Cluster, ClusterCache), the kubeconfig importer, and the per-cluster cache
// manager (rooted at <dataDir>/clusters/). The returned *Service is both the
// GraphQL boundary and the control plane: it owns the entire watcher + beehive +
// importer + forwarder + cache lifecycle via Start/Close. The watcher and beehive
// are cluster-only, so the service owns them outright; if a non-cluster consumer
// ever needs either, hoist it back up to the composition root and inject it.
func New(dataDir, kubeconfigPath string, pokeSvc *poke.Service) (*Service, error) {
	// The kubeconfig watcher publishes *api.Config snapshots; the importer and
	// ClusterController consume it (through the KubeConfigSource interface).
	watcher, err := k8shelpers.NewKubeConfigWatcher(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("init kubeconfig watcher: %w", err)
	}

	// Open the beehive SQLite store at <dataDir>/beehive.db. The beehive instance
	// owns the three resource kinds and drives their controllers level-triggered.
	bhStore, err := beehivesqlite.Open(filepath.Join(dataDir, "beehive.db"))
	if err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("open beehive store: %w", err)
	}
	bh, err := beehive.New(bhStore)
	if err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("init beehive: %w", err)
	}

	srcClient := beehive.NewClient[ClusterSourceSpec, ClusterSourceObjStatus](bh, ClusterSourceGroupKind)
	clusterClient := beehive.NewClient[ClusterSpec, ClusterConnectionStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	// The cache manager owns the per-cluster SQLite cache files under
	// <dataDir>/clusters/ — wholly a cluster concern, so the service owns it.
	cacheManager := store.NewManager(dataDir)

	srcCtrl := NewClusterSourceController(clusterClient)
	clusterCtrl := NewClusterController(watcher, cacheClient, nil, nil)
	cacheCtrl := NewClusterCacheController(watcher, clusterClient, cacheManager)

	if err := errors.Join(
		beehive.Register(bh, ClusterSourceGroupKind, srcCtrl),
		beehive.Register(bh, ClusterGroupKind, clusterCtrl),
		beehive.Register(bh, ClusterCacheGroupKind, cacheCtrl),
	); err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}

	return &Service{
		bh:            bh,
		bhStore:       bhStore,
		watcher:       watcher,
		srcClient:     srcClient,
		clusterClient: clusterClient,
		cacheClient:   cacheClient,
		cacheManager:  cacheManager,
		importer:      NewKubeconfigImporter(watcher, srcClient),
		pokeSvc:       pokeSvc,
	}, nil
}

// Start launches beehive (the controller harness + store subscription loop) and
// the kubeconfig watcher's fsnotify loop, then the kubeconfig importer and the
// poke→sync forwarder (the forwarder bumps PokeSyncGeneration on every Cluster so
// running sync engines bounce). ctx bounds beehive startup and scopes the
// forwarder goroutine.
func (s *Service) Start(ctx context.Context) error {
	stop, err := s.bh.Start(ctx)
	if err != nil {
		return fmt.Errorf("start beehive: %w", err)
	}
	s.bhStop = stop

	// Start the watcher (fsnotify loop) before the importer, which subscribes to
	// its snapshots current-on-subscribe.
	s.watcher.Start()
	s.importer.Start()

	if s.pokeSvc != nil {
		fctx, cancel := context.WithCancel(ctx)
		s.cancel = cancel
		ch, unsub := s.pokeSvc.Subscribe()
		s.wg.Go(func() {
			defer unsub()
			for {
				select {
				case <-fctx.Done():
					return
				case _, ok := <-ch:
					if !ok {
						return
					}
					if err := s.pokeAllSync(fctx); err != nil {
						slog.Warn("cluster service: poke forwarder failed to bump sync generation", "err", err)
					}
				}
			}
		})
	}
	return nil
}

// Close tears the cluster control plane down in dependency order, so the
// composition root doesn't have to sequence it: close the kubeconfig watcher
// (ending the importer's snapshot stream) and stop the writers (importer + poke
// forwarder, joining both), stop beehive (which stops every controller and sync
// engine), shut down the per-cluster cache (safe only now the engines that write
// into it are gone), then close the beehive store. ctx bounds the beehive stop
// and the cache shutdown.
func (s *Service) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	watcherErr := s.watcher.Close()
	s.importer.Stop()
	s.wg.Wait()

	var bhErr error
	if s.bhStop != nil {
		bhErr = s.bhStop(ctx)
	}
	cacheErr := s.cacheManager.Shutdown(ctx)
	storeErr := s.bhStore.Close()
	return errors.Join(watcherErr, bhErr, cacheErr, storeErr)
}

// List implements ClusterService.
func (s *Service) List(ctx context.Context) ([]*Cluster, error) {
	return s.listClusters(ctx)
}

// listClusters reads every Cluster, drops the deletion-pending ones, and builds
// each into a domain Cluster (joining its ClusterCache sync status). Shared by
// List and Watch's seed + re-emit.
func (s *Service) listClusters(ctx context.Context) ([]*Cluster, error) {
	objs, err := s.clusterClient.List(ctx)
	if err != nil {
		return nil, err
	}
	clusters := make([]*Cluster, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		c := s.buildCluster(ctx, obj)
		clusters = append(clusters, &c)
	}
	return clusters, nil
}

// Get implements ClusterService. An untracked or deletion-pending id is (nil,
// nil), not an error.
func (s *Service) Get(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if obj.DeletionRequestedAt != nil {
		return nil, nil
	}
	c := s.buildCluster(ctx, obj)
	return &c, nil
}

// CacheStats implements ClusterService.
func (s *Service) CacheStats(ctx context.Context, id ClusterID) (*CacheStats, error) {
	bytes, exists := s.cacheManager.CacheBytes(string(id))
	if !exists {
		return &CacheStats{}, nil
	}
	db := s.cacheManager.Lookup(string(id))
	if db == nil {
		return &CacheStats{Exists: true, Bytes: bytes}, nil
	}
	rss, err := db.ResourceStats(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]CachedResourceStats, len(rss))
	for i, rs := range rss {
		resources[i] = CachedResourceStats{
			Resource:      rs.Resource,
			Count:         rs.Count,
			LastUpdatedAt: rs.LastUpdatedAt,
		}
	}
	return &CacheStats{Exists: true, Bytes: bytes, Resources: resources}, nil
}

// SetSyncEnabled implements ClusterService.
func (s *Service) SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	spec := obj.Spec
	spec.IsSyncEnabled = enabled
	updated, err := s.clusterClient.Update(ctx, obj.ID, spec)
	if err != nil {
		return nil, err
	}
	c := s.buildCluster(ctx, updated)
	return &c, nil
}

// RetryConnection implements ClusterService.
func (s *Service) RetryConnection(ctx context.Context, id ClusterID) error {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	spec := obj.Spec
	spec.RetryGeneration++
	_, err = s.clusterClient.Update(ctx, obj.ID, spec)
	return err
}

// ClearCache implements ClusterService. It validates the cluster exists before
// touching disk, deletes the on-disk cache, then bumps PokeSyncGeneration to
// bounce the running engine.
func (s *Service) ClearCache(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.cacheManager.DeleteCacheFiles(ctx, string(id)); err != nil {
		return nil, err
	}
	spec := obj.Spec
	spec.PokeSyncGeneration++
	updated, err := s.clusterClient.Update(ctx, obj.ID, spec)
	if err != nil {
		return nil, err
	}
	c := s.buildCluster(ctx, updated)
	return &c, nil
}

// Delete implements ClusterService.
func (s *Service) Delete(ctx context.Context, id ClusterID) error {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	// Find and delete the parent ClusterSource; GC cascades to Cluster + ClusterCache.
	if obj.Spec.Source.Kubeconfig != nil {
		srcObj, srcErr := s.srcClient.GetBySlug(ctx, ClusterSourceSlug(obj.Spec.Source.Kubeconfig.Context))
		if srcErr == nil {
			return s.srcClient.Delete(ctx, srcObj.ID)
		}
	}
	// Fallback: delete the Cluster directly (no parent source or lookup failed).
	return s.clusterClient.Delete(ctx, obj.ID)
}

// Watch implements ClusterService.
func (s *Service) Watch(ctx context.Context) (<-chan []*Cluster, error) {
	seed, err := s.listClusters(ctx)
	if err != nil {
		return nil, err
	}

	clusterCh, err := s.clusterClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}
	cacheCh, err := s.cacheClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan []*Cluster, 1)
	out <- seed

	go func() {
		defer close(out)

		emit := func() {
			clusters, err := s.listClusters(ctx)
			if err != nil {
				return
			}
			select {
			case out <- clusters:
			case <-ctx.Done():
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-clusterCh:
				if !ok {
					return
				}
				emit()
			case _, ok := <-cacheCh:
				if !ok {
					return
				}
				emit()
			}
		}
	}()
	return out, nil
}

// pokeAllSync increments PokeSyncGeneration on every Cluster object so the
// ClusterCacheController bounces all running sync engines.
func (s *Service) pokeAllSync(ctx context.Context) error {
	objs, err := s.clusterClient.List(ctx)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		spec := obj.Spec
		spec.PokeSyncGeneration++
		if _, err := s.clusterClient.Update(ctx, obj.ID, spec); err != nil {
			return err
		}
	}
	return nil
}

// buildCluster assembles a domain Cluster from a Cluster beehive object, joining
// in ClusterCache sync status from the cache client.
func (s *Service) buildCluster(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterConnectionStatus]) Cluster {
	var id ClusterID
	if obj.Slug != nil {
		id = ClusterIDFromSlug(*obj.Slug)
	}

	c := Cluster{
		ID:         id,
		Generation: obj.Generation,
		CreatedAt:  obj.CreatedAt,
		Spec:       obj.Spec,
	}
	if obj.DeletionRequestedAt != nil {
		t := *obj.DeletionRequestedAt
		c.DeletedAt = &t
	}
	if obj.Status != nil {
		c.Status.Source = obj.Status.Source
		c.Status.Server = obj.Status.Server
		c.Status.Principal = obj.Status.Principal
		c.Status.LastConnectedAt = obj.Status.LastConnectedAt
		c.Status.Conditions = obj.Status.Conditions
	}

	// Join in ClusterCache sync status.
	cacheObj, err := s.cacheClient.GetBySlug(ctx, ClusterCacheSlug(id))
	if err == nil && cacheObj.Status != nil {
		c.Status.SyncStatus = *cacheObj.Status
	}

	return c
}

// clusterByID fetches a Cluster object by id, mapping beehive.ErrNotFound to the
// package's ErrNotFound.
func (s *Service) clusterByID(ctx context.Context, id ClusterID) (*beehive.Object[ClusterSpec, ClusterConnectionStatus], error) {
	obj, err := s.clusterClient.GetBySlug(ctx, ClusterSlug(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, ErrNotFound
	}
	return obj, err
}
