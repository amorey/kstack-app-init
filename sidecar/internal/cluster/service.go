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
	"path/filepath"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the boundary between the frontend (GraphQL today, gRPC
// later) and the cluster backend. Every beehive detail — slugs, the
// Cluster → ClusterCache owner chain, the spec/status split, the ClusterCache
// status join, the two-channel watch merge — lives behind it, so callers deal
// only in the domain Cluster type.
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
	// ClusterCacheStats returns live per-cluster cache statistics.
	CacheStats(ctx context.Context, id ClusterID) (*ClusterCacheStats, error)
	// SetEnabled enables or disables a cluster in the app (connection eligibility
	// + visibility in pickers) and returns the updated record.
	SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// RetryConnection forces an immediate out-of-band re-probe of the cluster's
	// connection. The outcome lands on the record's conditions and reaches
	// watchers through Watch.
	RetryConnection(ctx context.Context, id ClusterID) error
	// ClearCache deletes the on-disk cache and bounces the sync engine; the
	// (returned) record stays.
	ClearCache(ctx context.Context, id ClusterID) (*Cluster, error)
	// Delete removes the cluster by deleting the Cluster object so beehive GC
	// cascades to ClusterCache.
	Delete(ctx context.Context, id ClusterID) error
	// GetConnection returns the live REST config for id, or nil if the cluster
	// is not currently connected.
	GetConnection(id ClusterID) *rest.Config
}

// Service is the concrete ClusterService and the whole cluster control plane: it
// owns the beehive store + instance, the kubeconfig watcher, the two beehive
// clients, the two controllers (registered with beehive in New), the kubeconfig
// importer, the connection manager, and the per-cluster cache manager.
type Service struct {
	bh      *beehive.Beehive
	bhStore beehive.Store

	watcher *k8shelpers.KubeConfigWatcher

	coreClient   beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient  beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	cacheManager *store.Manager
	connMgr      *ConnectionManager
	coreCtrl     *ClusterCoreController
	cacheCtrl    *ClusterCacheController

	importer *KubeconfigImporter
	pokeSvc  *poke.Service
}

var _ ClusterService = (*Service)(nil)

// New builds the cluster control plane: the kubeconfig watcher (over
// kubeconfigPath), the beehive store + instance (at <dataDir>/beehive.db), the
// two beehive clients, the two registered controllers (Cluster, ClusterCache),
// the kubeconfig importer, and the per-cluster cache manager (rooted at
// <dataDir>/clusters/). The returned *Service is both the
// GraphQL boundary and the control plane: it owns the entire watcher + beehive +
// importer + forwarder + cache lifecycle via Start/Close. The watcher and beehive
// are cluster-only, so the service owns them outright; if a non-cluster consumer
// ever needs either, hoist it back up to the composition root and inject it.
func New(dataDir, kubeconfigPath string, pokeSvc *poke.Service) (*Service, error) {
	// The kubeconfig watcher publishes *api.Config snapshots; the importer and
	// ClusterCoreController consume it (through the KubeConfigSource interface).
	watcher, err := k8shelpers.NewKubeConfigWatcher(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("init kubeconfig watcher: %w", err)
	}

	// Open the beehive SQLite store at <dataDir>/beehive.db. The beehive instance
	// owns the two resource kinds and drives their controllers level-triggered.
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

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	// The cache manager owns the per-cluster SQLite cache files under
	// <dataDir>/clusters/ — wholly a cluster concern, so the service owns it.
	cacheManager := store.NewManager(dataDir)

	connMgr := NewConnectionManager()

	coreCtrl := NewClusterCoreController(watcher, coreClient, cacheClient, connMgr, pokeSvc, nil, nil)
	cacheCtrl := NewClusterCacheController(watcher, coreClient, cacheManager, connMgr, pokeSvc)

	// Register returns each kind's status-write ControllerClient. The reconcile
	// path receives it as a Reconcile argument, but the controllers also write
	// status out-of-band (the connection controller's poke re-probe, the cache
	// controller's engine sink), so we inject it now — before bh.Start, since a
	// startup reconcile may spawn an engine that reports immediately.
	coreCC, errCluster := beehive.Register(bh, ClusterGroupKind, coreCtrl)
	cacheCC, errCache := beehive.Register(bh, ClusterCacheGroupKind, cacheCtrl)
	if err := errors.Join(errCluster, errCache); err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}
	coreCtrl.SetControllerClient(coreCC)
	cacheCtrl.SetControllerClient(cacheCC)

	return &Service{
		bh:           bh,
		bhStore:      bhStore,
		watcher:      watcher,
		coreClient:   coreClient,
		cacheClient:  cacheClient,
		cacheManager: cacheManager,
		connMgr:      connMgr,
		coreCtrl:     coreCtrl,
		cacheCtrl:    cacheCtrl,
		importer:     NewKubeconfigImporter(watcher, coreClient),
		pokeSvc:      pokeSvc,
	}, nil
}

// Start launches beehive (the controller harness + store subscription loop) and
// the kubeconfig watcher's fsnotify loop, then the kubeconfig importer. ctx
// bounds beehive startup only; the returned stop func accepts a drain-deadline
// context and blocks until all background work finishes. Call stop before Close.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	bhStop, err := s.bh.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start beehive: %w", err)
	}

	// The controllers' background work (poke-driven re-probe / engine restart) is
	// the service's to drive now that beehive owns only the reconcile lifecycle.
	// Start it once the control plane is running.
	// The core controller's background worker drives both the targeted-retry bus
	// (RetryConnection) and the poke bus; the cache controller reacts to pokes
	// only. Both write status, so they live in the same start/drain window.
	s.coreCtrl.StartBackground()
	s.cacheCtrl.StartPoke()

	// Start the watcher (fsnotify loop) before the importer, which subscribes to
	// its snapshots current-on-subscribe.
	s.watcher.Start()
	s.importer.Start()

	stop := func(ctx context.Context) error {
		s.watcher.Close()
		s.importer.Stop()

		// Stop out-of-band re-probe / engine-restart work before draining so none
		// of it races the teardown.
		s.coreCtrl.StopBackground()
		s.cacheCtrl.StopPoke()

		// Then drain the reconcile loops, tear down the engines (no reconcile can
		// spawn a fresh one now), and only then shut the cache they wrote into.
		bhErr := bhStop(ctx)
		engErr := s.cacheCtrl.StopEngines()
		cacheErr := s.cacheManager.Shutdown(ctx)
		return errors.Join(bhErr, engErr, cacheErr)
	}
	return stop, nil
}

// Close releases the beehive store after Stop has finished. Call after Stop.
func (s *Service) Close() error {
	return s.bhStore.Close()
}

// List implements ClusterService.
func (s *Service) List(ctx context.Context) ([]*Cluster, error) {
	return s.listClusters(ctx)
}

// listClusters reads every Cluster, drops the deletion-pending ones, and builds
// each into a domain Cluster (joining its ClusterCache sync status). Shared by
// List and Watch's seed + re-emit.
func (s *Service) listClusters(ctx context.Context) ([]*Cluster, error) {
	objs, err := s.coreClient.List(ctx)
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
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
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

// ClusterCacheStats implements ClusterService.
func (s *Service) CacheStats(ctx context.Context, id ClusterID) (*ClusterCacheStats, error) {
	ref, found, err := s.cacheRef(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return &ClusterCacheStats{}, nil
	}
	bytes, exists := s.cacheManager.CacheBytes(ref)
	if !exists {
		return &ClusterCacheStats{}, nil
	}
	db := s.cacheManager.Lookup(ref.CacheID)
	if db == nil {
		return &ClusterCacheStats{Exists: true, Bytes: bytes}, nil
	}
	rss, err := db.ResourceStats(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]CachedResource, len(rss))
	for i, rs := range rss {
		resources[i] = CachedResource{
			Resource:      rs.Resource,
			Count:         rs.Count,
			LastUpdatedAt: rs.LastUpdatedAt,
		}
	}
	return &ClusterCacheStats{Exists: true, Bytes: bytes, Resources: resources}, nil
}

// updateSpec applies mutate to a copy of the cluster's spec and persists it. The
// spec write bumps the generation, so the connection + cache controllers
// re-reconcile and the change propagates to watchers. It backs the spec-toggle
// mutations below.
func (s *Service) updateSpec(ctx context.Context, id ClusterID, mutate func(*ClusterSpec)) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	spec := obj.Spec
	mutate(&spec)
	updated, err := s.coreClient.Update(ctx, obj.ID, spec)
	if err != nil {
		return nil, err
	}
	c := s.buildCluster(ctx, updated)
	return &c, nil
}

// SetEnabled implements ClusterService.
func (s *Service) SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	return s.updateSpec(ctx, id, func(spec *ClusterSpec) { spec.Enabled = enabled })
}

// SetSyncEnabled implements ClusterService.
func (s *Service) SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	return s.updateSpec(ctx, id, func(spec *ClusterSpec) { spec.SyncEnabled = enabled })
}

// RetryConnection implements ClusterService. It validates the cluster exists,
// then dispatches an immediate out-of-band re-probe to the connection controller
// via its in-process retry bus — no spec write. The outcome lands on the record's
// conditions and reaches watchers through Watch.
func (s *Service) RetryConnection(ctx context.Context, id ClusterID) error {
	if _, err := s.clusterByID(ctx, id); err != nil {
		return err
	}
	if s.coreCtrl != nil {
		s.coreCtrl.Reprobe(id)
	}
	return nil
}

// ClearCache implements ClusterService. It validates the cluster exists before
// touching disk, deletes the on-disk cache, then restarts the running engine so
// it rebuilds.
func (s *Service) ClearCache(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	// Resolve the cache child to locate its on-disk files. If there is no
	// ClusterCache object yet (cluster never became sync-eligible), there are no
	// files to delete — skip straight to the engine restart (also a no-op).
	ref, found, err := s.cacheRef(ctx, id)
	if err != nil {
		return nil, err
	}
	if found {
		if derr := s.cacheManager.DeleteCacheFiles(ctx, ref); derr != nil {
			return nil, derr
		}
	}
	// Restart the running engine so it rebuilds against the now-empty cache. A
	// no-op if no engine is running (it cold-syncs next time it starts).
	if s.cacheCtrl != nil {
		s.cacheCtrl.RestartEngine(id)
	}
	c := s.buildCluster(ctx, obj)
	return &c, nil
}

// Delete implements ClusterService. It deletes the Cluster object; beehive GC
// cascades to its ClusterCache. If the kube-context still exists, the importer
// will re-create the cluster on its next reconcile — with the same
// "kubeconfig/{context}" slug, since that is the context's natural key.
func (s *Service) Delete(ctx context.Context, id ClusterID) error {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	return s.coreClient.Delete(ctx, obj.ID)
}

// Watch implements ClusterService.
func (s *Service) Watch(ctx context.Context) (<-chan []*Cluster, error) {
	seed, err := s.listClusters(ctx)
	if err != nil {
		return nil, err
	}

	clusterCh, err := s.coreClient.WatchList(ctx)
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

// GetConnection implements ClusterService.
func (s *Service) GetConnection(id ClusterID) *rest.Config {
	return s.connMgr.Get(id)
}

// buildCluster assembles a domain Cluster from a Cluster beehive object, joining
// in ClusterCache sync status from the cache client.
func (s *Service) buildCluster(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterStatus]) Cluster {
	id := ClusterID(obj.ID)

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
		c.Status = *obj.Status
	}

	// Join in the ClusterCache child: its sync status plus the ID the live-stats
	// resolver needs.
	c.Cache.ID = id
	cacheObj, err := s.cacheClient.GetBySlug(ctx, ClusterCacheSlug(id))
	if err == nil && cacheObj.Status != nil {
		c.Cache.Status = *cacheObj.Status
	}

	return c
}

// clusterByID fetches a Cluster object by id, mapping beehive.ErrNotFound to the
// package's ErrNotFound.
func (s *Service) clusterByID(ctx context.Context, id ClusterID) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, ErrNotFound
	}
	return obj, err
}

// cacheRef resolves the on-disk cache locator for a cluster: the beehive
// ObjectIDs of the parent Cluster (directory) and its ClusterCache child (file).
// found is false when the ClusterCache is missing — the cluster has no cache
// files (it never became sync-eligible, or is being torn down), which callers
// treat as "no cache" rather than an error.
//
// One lookup: the ClusterID IS the parent Cluster's ObjectID (the directory), so
// only the ClusterCache child (the file) needs fetching — by its slug, which is
// keyed on that same parent ObjectID.
func (s *Service) cacheRef(ctx context.Context, id ClusterID) (store.CacheRef, bool, error) {
	cacheObj, err := s.cacheClient.GetBySlug(ctx, ClusterCacheSlug(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	return newCacheRef(beehive.ObjectID(id), cacheObj.ID), true, nil
}
