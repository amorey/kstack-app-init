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

// Package clustersvc is the sidecar's Kubernetes boundary: Service and its five
// record families (Clusters, Caches, CachedCatalogs, CachedResources, Data).
//
// Stripped to a shell pending a rebuild: these interfaces and the types they carry
// are the whole package, and every method but the lifecycle pair panics. They hold
// the GraphQL and gRPC surfaces steady meanwhile. Rebuilding a family means
// implementing it in its own file (clusters.go, caches.go, …) and deleting its stub
// below.
package clustersvc

import (
	"context"

	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/types"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// Service is the frontend-facing boundary: every beehive detail (names, owner chain,
// spec/status split, delta-watch mapping) lives behind it. Each record family hangs
// off its own sub-interface; only the connection surface, which reads no beehive
// object, sits at the top level. Delta watches follow
// docs/adr/2026-08-09-delta-watch-protocol.md and close when ctx ends.
//
// A single-object Watch runs the same protocol as its WatchList, over one id:
// snapshot, one Bookmark, then deltas. An id holding nothing yet gets the Bookmark
// alone and streams Added if it appears; a removal is Deleted and does not end the
// stream. Absence is never an error — watching a record before it exists is an
// ordinary subscription, which is what lets a view open one on an id it expects.
type Service interface {
	// Start launches the background work and returns the func that drains it. ctx
	// bounds startup; the stop func takes a drain deadline. Call stop before Close.
	Start(ctx context.Context) (func(context.Context) error, error)
	// Close releases the boundary's resources. Call after the stop func returns.
	Close() error

	Clusters() Clusters
	Caches() Caches
	CachedCatalogs() CachedCatalogs
	CachedResources() CachedResources
	CachedData() CachedData

	// GetConnection returns the live REST config for id, or nil when not connected.
	GetConnection(id types.ClusterID) *rest.Config
	// RetryConnection forces an out-of-band re-probe. The outcome lands on the
	// record's conditions and reaches watchers through Clusters().Watch, not here.
	RetryConnection(ctx context.Context, id types.ClusterID) error

	// The event timeline of any record that has one — Cluster, ClusterCache,
	// ClusterCachedResource today. Top-level, not per family: an event carries no kind
	// of its own, so one reader serves every timeline and the id is the whole key.
	// Both are off the record watches, so event chatter never re-emits a record.
	//
	// ListEvents returns newest run first, optionally filtered by category and
	// bounded by limit. WatchEvents streams the same log — snapshot, one Bookmark,
	// then growth. Distinct from CachedData().WatchEvents, which streams the *cluster's*
	// cached Kubernetes Events rather than kstack's own log.
	ListEvents(ctx context.Context, id types.ObjectID, category *string, limit *int) ([]types.Event, error)
	WatchEvents(ctx context.Context, id types.ObjectID, category *string) (*Stream[types.EventWatchFrame], error)
}

// Clusters is the Cluster record surface: the tracked clusters, their spec
// toggles, and the per-cluster streams that deliberately do not ride the record
// watch.
type Clusters interface {
	// Get returns one cluster by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id types.ClusterID) (*types.Cluster, error)
	// List returns every tracked, non-deletion-pending cluster. Cache sync status is
	// the caller's join from Caches().WatchList.
	List(ctx context.Context) ([]*types.Cluster, error)

	// Watch streams one cluster as a delta watch. Bookmark-only while the id names
	// no tracked cluster — one dropped from the kubeconfig and re-added streams
	// Deleted then Added on the same subscription.
	Watch(ctx context.Context, id types.ClusterID) (*Stream[types.ClusterWatchFrame], error)
	// WatchList streams every cluster as a delta watch; deletion-pending surfaces
	// as Deleted.
	WatchList(ctx context.Context) (*Stream[types.ClusterWatchFrame], error)

	// WatchSchedule streams a cluster's next-requeue gauge. A stream because a
	// scheduling change fires no object WatchList, so it cannot ride Watch.
	WatchSchedule(ctx context.Context, id types.ClusterID) (<-chan types.Schedule, error)
	// SetEnabled enables or disables a cluster and returns the updated record.
	SetEnabled(ctx context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error)

	// Delete deletes the Cluster object; beehive GC cascades to ClusterCache.
	Delete(ctx context.Context, id types.ClusterID) error
}

// Caches is the ClusterCache record surface. A cluster owns zero or one cache at
// steady state, but a UID migration leaves the old one behind — so every
// per-cache read names the exact cache it means, never "the" cache of a cluster.
type Caches interface {
	// Get returns one cache by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id types.ClusterCacheID) (*types.ClusterCache, error)
	// List returns every cache in creation order, non-deletion-pending. Which one is
	// active is the caller's live join (types.CacheIsActive), not a property here.
	List(ctx context.Context) ([]*types.ClusterCache, error)

	// Watch streams one cache as a delta watch. Bookmark-only until the cluster has
	// been probed and its cache created, so a caller may open this on a cache a
	// migration has not produced yet.
	Watch(ctx context.Context, id types.ClusterCacheID) (*Stream[types.ClusterCacheWatchFrame], error)
	// WatchList streams every cache as a delta watch parallel to
	// Clusters().WatchList; the caller joins caches onto clusters by ClusterID.
	WatchList(ctx context.Context) (*Stream[types.ClusterCacheWatchFrame], error)

	// ListByCluster returns one cluster's caches, in the same order.
	ListByCluster(ctx context.Context, clusterID types.ClusterID) ([]*types.ClusterCache, error)
	// WatchByCluster streams one cluster's caches as a delta watch — what a view
	// scoped to a single cluster opens instead of filtering WatchList.
	WatchByCluster(ctx context.Context, clusterID types.ClusterID) (*Stream[types.ClusterCacheWatchFrame], error)

	// WatchStats streams one cache's contents as a live gauge. A stream, not a
	// ClusterCache field: a settled cache's object never changes, so a field would
	// freeze at subscribe time.
	WatchStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCacheStats, error)
	// WatchHealth streams every cache's sync verdict, folded from its per-kind
	// records. Unscoped where the rest of this family is per-cache: one fold serves
	// the fleet. A gauge, but a failable one — the fold reads watches of its own.
	WatchHealth(ctx context.Context) (*Stream[types.ClusterCacheHealth], error)

	// Clear deletes the on-disk cache and bounces its syncs; the record stays.
	Clear(ctx context.Context, id types.ClusterID) (*types.Cluster, error)
}

// CachedCatalogs is the ClusterCachedCatalog surface: one catalog per cache,
// listing the kinds that cache's cluster serves and owning a CachedResource per kind.
type CachedCatalogs interface {
	// Get returns one catalog by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id types.ClusterCachedCatalogID) (*types.ClusterCachedCatalog, error)
	// List returns every catalog in creation order, non-deletion-pending. A cache
	// gets its catalog only once discovery lands, so an empty result is a wait, not
	// an error.
	List(ctx context.Context) ([]*types.ClusterCachedCatalog, error)

	// Watch streams one catalog as a delta watch. Bookmark-only until the cache's
	// first discovery pass lands, which is the wait List describes.
	Watch(ctx context.Context, id types.ClusterCachedCatalogID) (*Stream[types.ClusterCachedCatalogWatchFrame], error)
	// WatchList streams every cache's catalog, joined onto caches by CacheID.
	WatchList(ctx context.Context) (*Stream[types.ClusterCachedCatalogWatchFrame], error)

	// ListByCache returns one cache's catalog — at most one record, as a slice so it
	// reads like its siblings.
	ListByCache(ctx context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedCatalog, error)
	// WatchByCache streams one cache's catalog as a delta watch — what a view scoped
	// to a single cache opens instead of filtering WatchList.
	WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCachedCatalogWatchFrame], error)
}

// CachedResources is the ClusterCachedResource surface — one record per kind a
// cache mirrors. Distinct from Data, which serves the mirrored content itself;
// these are the control-plane records describing what is mirrored.
//
// This family is the fleet's largest by an order of magnitude: a record per served
// kind per cache, so hundreds per cluster. Scope every read that can be scoped.
type CachedResources interface {
	// Get returns one record by id, or (nil, nil) when unknown or
	// deletion-pending.
	Get(ctx context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error)
	// List returns every per-kind sync record in creation order.
	List(ctx context.Context) ([]*types.ClusterCachedResource, error)

	// Watch streams one per-kind record as a delta watch. Bookmark-only until the
	// kind enters its cache's catalog; a kind the cluster stops serving is Deleted.
	Watch(ctx context.Context, id types.ClusterCachedResourceID) (*Stream[types.ClusterCachedResourceWatchFrame], error)
	// WatchList streams every per-kind record across every cache — the fleet's
	// widest stream, and one a view scoped to a cache wants WatchByCache for
	// instead. For a reader that genuinely spans caches: the sync-health rollup.
	WatchList(ctx context.Context) (*Stream[types.ClusterCachedResourceWatchFrame], error)

	// ListByCache returns one cache's per-kind records. Scoped by the CACHE — the
	// catalog between them is resolved here. A cache with no catalog yet owns none,
	// which is empty rather than an error.
	ListByCache(ctx context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedResource, error)
	// WatchByCache streams one cache's per-kind records, resolving the catalog the
	// same way ListByCache does.
	WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCachedResourceWatchFrame], error)

	// Clear drops one kind's cached objects and restarts its sync from an empty
	// mirror; the record stays and resyncs. Caches().Clear is the whole-cache form.
	Clear(ctx context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error)
}

// CachedData is the cached Kubernetes content in one cache's db — the only family whose
// reads leave beehive entirely. Every method degrades to empty (no error, no
// frames) while that cache's db isn't open: never synced, or sync paused.
type CachedData interface {
	// ListKinds returns one cache's discovered kind catalog.
	ListKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) ([]types.ClusterCachedDataKind, error)
	// WatchKinds streams one cache's kind catalog as a delta watch (per-kind counts
	// update live).
	WatchKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCachedDataKindWatchFrame, error)
	// WatchObjects streams one kind's cached objects as a delta watch keyed by UID.
	// No frames while that kind hasn't synced.
	WatchObjects(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID, apiVersion, resource string) (<-chan types.ClusterCachedDataObjectWatchFrame, error)
	// WatchEvents streams one cache's cached Kubernetes Events (newest window) as a
	// delta watch keyed by event UID. Woken separately from WatchKinds, so an event
	// burst never drives the kind-catalog re-read.
	WatchEvents(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCachedDataEventWatchFrame, error)
}

// The family accessors are stateless views onto the one *service: the split is
// about the shape of the API, not a split of the control plane behind it.
type (
	clustersAPI        struct{ s *service }
	cachesAPI          struct{ s *service }
	cachedCatalogsAPI  struct{ s *service }
	cachedResourcesAPI struct{ s *service }
	cachedDataAPI      struct{ s *service }
)

func (s *service) Clusters() Clusters { return clustersAPI{s} }

func (s *service) Caches() Caches { return cachesAPI{s} }

func (s *service) CachedCatalogs() CachedCatalogs { return cachedCatalogsAPI{s} }

func (s *service) CachedResources() CachedResources { return cachedResourcesAPI{s} }

func (s *service) CachedData() CachedData { return cachedDataAPI{s} }

// Each family is asserted separately: satisfying Service only proves the accessors
// exist, not that any family is fully implemented.
var (
	_ Service         = (*service)(nil)
	_ Clusters        = clustersAPI{}
	_ Caches          = cachesAPI{}
	_ CachedCatalogs  = cachedCatalogsAPI{}
	_ CachedResources = cachedResourcesAPI{}
	_ CachedData      = cachedDataAPI{}
)

// service is the concrete Service. It holds no state; the rebuild decides what
// control plane sits behind it.
type service struct{}

// New builds the cluster boundary. The arguments are what a real control plane
// needs — where its state lives, which kubeconfig it follows, the resync bus it
// listens on — and are kept so wiring one up is not a change to internal/app.
func New(dataDir, kubeconfigPath string, pokeSvc *poke.Service) (Service, error) {
	return &service{}, nil
}

func (s *service) Start(ctx context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

func (s *service) Close() error {
	return nil
}

func (s *service) RetryConnection(ctx context.Context, id types.ClusterID) error {
	panic("not implemented")
}

func (s *service) GetConnection(id types.ClusterID) *rest.Config {
	panic("not implemented")
}

func (s *service) ListEvents(ctx context.Context, id types.ObjectID, category *string, limit *int) ([]types.Event, error) {
	panic("not implemented")
}

func (s *service) WatchEvents(ctx context.Context, id types.ObjectID, category *string) (*Stream[types.EventWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) List(ctx context.Context) ([]*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Get(ctx context.Context, id types.ClusterID) (*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Watch(ctx context.Context, id types.ClusterID) (*Stream[types.ClusterWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) WatchList(ctx context.Context) (*Stream[types.ClusterWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) WatchSchedule(ctx context.Context, id types.ClusterID) (<-chan types.Schedule, error) {
	panic("not implemented")
}

func (a clustersAPI) SetEnabled(ctx context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) SetSyncEnabled(ctx context.Context, id types.ClusterID, enabled bool) (*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Delete(ctx context.Context, id types.ClusterID) error {
	panic("not implemented")
}

func (a cachesAPI) Get(ctx context.Context, id types.ClusterCacheID) (*types.ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) List(ctx context.Context) ([]*types.ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) ListByCluster(ctx context.Context, clusterID types.ClusterID) ([]*types.ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) WatchByCluster(ctx context.Context, clusterID types.ClusterID) (*Stream[types.ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) Watch(ctx context.Context, id types.ClusterCacheID) (*Stream[types.ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) WatchList(ctx context.Context) (*Stream[types.ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) WatchStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCacheStats, error) {
	panic("not implemented")
}

func (a cachesAPI) Clear(ctx context.Context, id types.ClusterID) (*types.Cluster, error) {
	panic("not implemented")
}

func (a cachesAPI) WatchHealth(ctx context.Context) (*Stream[types.ClusterCacheHealth], error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) Get(ctx context.Context, id types.ClusterCachedCatalogID) (*types.ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) List(ctx context.Context) ([]*types.ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) ListByCache(ctx context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedCatalog, error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) Watch(ctx context.Context, id types.ClusterCachedCatalogID) (*Stream[types.ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

func (a cachedCatalogsAPI) WatchList(ctx context.Context) (*Stream[types.ClusterCachedCatalogWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) Get(ctx context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) List(ctx context.Context) ([]*types.ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) ListByCache(ctx context.Context, cacheID types.ClusterCacheID) ([]*types.ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) Watch(ctx context.Context, id types.ClusterCachedResourceID) (*Stream[types.ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) WatchList(ctx context.Context) (*Stream[types.ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) WatchByCache(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCachedResourceWatchFrame], error) {
	panic("not implemented")
}

func (a cachedResourcesAPI) Clear(ctx context.Context, id types.ClusterCachedResourceID) (*types.ClusterCachedResource, error) {
	panic("not implemented")
}

func (a cachedDataAPI) ListKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) ([]types.ClusterCachedDataKind, error) {
	panic("not implemented")
}

func (a cachedDataAPI) WatchKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCachedDataKindWatchFrame, error) {
	panic("not implemented")
}

func (a cachedDataAPI) WatchEvents(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCachedDataEventWatchFrame, error) {
	panic("not implemented")
}

func (a cachedDataAPI) WatchObjects(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID, apiVersion, resource string) (<-chan types.ClusterCachedDataObjectWatchFrame, error) {
	panic("not implemented")
}
