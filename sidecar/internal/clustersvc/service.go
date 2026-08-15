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

// Package clustersvc is the sidecar's Kubernetes boundary: ClusterService and its
// five record families (Clusters, Caches, Discovery, Syncs, Data).
//
// Stripped to a shell pending a rebuild: these interfaces and the types they carry
// are the whole package, and every method panics. They hold the GraphQL and gRPC
// surfaces steady meanwhile. Rebuilding a family means implementing it in its own
// file (clusters.go, caches.go, …) and deleting its stub below.
package clustersvc

import (
	"context"

	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/types"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the frontend-facing boundary: every beehive detail (names,
// owner chain, spec/status split, delta-watch mapping) lives behind it. Each record
// family hangs off its own sub-interface; only the connection surface, which reads
// no beehive object, sits at the top level. Delta watches follow
// docs/adr/2026-08-09-delta-watch-protocol.md and close when ctx ends.
type ClusterService interface {
	Clusters() Clusters
	Caches() Caches
	Discovery() Discovery
	Syncs() Syncs
	Data() Data

	// RetryConnection forces an out-of-band re-probe. The outcome lands on the
	// record's conditions and reaches watchers through Clusters().Watch, not here.
	RetryConnection(ctx context.Context, id types.ClusterID) error
	// GetConnection returns the live REST config for id, or nil when not connected.
	GetConnection(id types.ClusterID) *rest.Config

	// WatchObjectEvents streams any object's event timeline — snapshot, one bookmark,
	// then growth — whatever kind the id names. Top-level because an event carries no
	// kind of its own; the per-record point reads stay on their families.
	WatchObjectEvents(ctx context.Context, id types.ObjectID, category *string) (*Stream[types.EventWatchFrame], error)
}

// Clusters is the Cluster record surface: the tracked clusters, their spec
// toggles, and the per-cluster streams that deliberately do not ride the record
// watch.
type Clusters interface {
	// List returns every tracked, non-deletion-pending cluster. Cache sync status
	// is the caller's join from Caches().Watch.
	List(ctx context.Context) ([]*types.Cluster, error)
	// Get returns one cluster by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id types.ClusterID) (*types.Cluster, error)
	// Watch streams the cluster list as a delta watch; deletion-pending surfaces
	// as Deleted.
	Watch(ctx context.Context) (*Stream[types.ClusterWatchFrame], error)
	// ListEvents returns a cluster's event timeline (newest run first), optionally
	// filtered by category and bounded by limit. Off the record watch, so event
	// chatter never re-emits the cluster.
	ListEvents(ctx context.Context, id types.ClusterID, category *string, limit *int) ([]types.Event, error)
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
	// List returns caches in creation order, non-deletion-pending: one cluster's when
	// clusterID is set, every tracked cache when nil. Which one is active is the
	// caller's live join (types.CacheIsActive), not a property here.
	List(ctx context.Context, clusterID *types.ClusterID) ([]*types.ClusterCache, error)
	// Watch streams cache records as a delta watch parallel to Clusters().Watch;
	// the caller joins caches onto clusters by ClusterID.
	Watch(ctx context.Context) (*Stream[types.ClusterCacheWatchFrame], error)
	// GetStats returns live on-disk statistics for one ClusterCache.
	GetStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (*types.ClusterCacheStats, error)
	// WatchStats streams one cache's contents as a live gauge. A stream, not a
	// ClusterCache field: a settled cache's object never changes, so a field would
	// freeze at subscribe time.
	WatchStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCacheStats, error)
	// ListEvents is the ClusterCache-kind counterpart of Clusters().ListEvents.
	ListEvents(ctx context.Context, id types.ClusterCacheID, category *string, limit *int) ([]types.Event, error)
	// Clear deletes the on-disk cache and bounces its syncs; the record stays.
	Clear(ctx context.Context, id types.ClusterID) (*types.Cluster, error)
	// WatchSyncHealth streams every cache's sync verdict, folded from its per-kind
	// records. Unscoped where the rest of this family is per-cache: one fold serves
	// the fleet. A gauge, but a failable one — the fold reads watches of its own.
	WatchSyncHealth(ctx context.Context) (*Stream[types.ClusterCacheSyncHealth], error)
}

// Discovery is the per-cache GVR-discovery anchor: exactly one per cache, naming
// the kinds that cache's cluster serves.
type Discovery interface {
	// Watch streams the caches' discovery anchors, joined onto caches by CacheID.
	Watch(ctx context.Context) (*Stream[types.ClusterCacheGVRDiscoveryWatchFrame], error)
	// GetStats returns one anchor's live gauges, nil before this process's first
	// pass. Out of status so a pass never wakes dependents for a UI-only number.
	GetStats(ctx context.Context, id types.ClusterCacheGVRDiscoveryID) (*types.ClusterCacheGVRDiscoveryStats, error)
}

// Syncs is the per-kind sync-record surface — one record per kind a cache serves,
// which is why Watch is cache-scoped where its siblings in other families are
// fleet-wide.
type Syncs interface {
	// Get returns one sync record by id, or (nil, nil) when unknown or
	// deletion-pending.
	Get(ctx context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSync, error)
	// List returns per-kind sync records in creation order: one cache's when cacheID
	// is set, every tracked record when nil. Scoped by the CACHE — the discovery
	// anchor between them is resolved here. A cache with no anchor yet owns none,
	// which is empty rather than an error.
	List(ctx context.Context, cacheID *types.ClusterCacheID) ([]*types.ClusterCacheGVRSync, error)
	// Watch streams one cache's per-kind sync records. Cache-scoped: one record per
	// served kind, so an unscoped stream would be a firehose.
	Watch(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCacheGVRSyncWatchFrame], error)
	// GetStats returns one synced kind's freshness stamps.
	GetStats(ctx context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSyncStats, error)
	// SnapshotStats returns every synced kind's stamps under one lock — what the
	// sync-health rollup folds.
	SnapshotStats() map[types.ClusterCacheGVRSyncID]types.ClusterCacheGVRSyncStats
	// ListEvents returns one synced kind's event timeline.
	ListEvents(ctx context.Context, id types.ClusterCacheGVRSyncID, category *string, limit *int) ([]types.Event, error)
}

// Data is the cached Kubernetes content in one cache's db — the only family whose
// reads leave beehive entirely. Every method degrades to empty (no error, no
// frames) while that cache's db isn't open: never synced, or sync paused.
type Data interface {
	// ListKinds returns one cache's discovered kind catalog.
	ListKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) ([]types.ClusterDataKind, error)
	// WatchKinds streams one cache's kind catalog as a delta watch (per-kind counts
	// update live).
	WatchKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterDataKindWatchFrame, error)
	// WatchEvents streams one cache's cached Kubernetes Events (newest window) as a
	// delta watch keyed by event UID. Woken separately from WatchKinds, so an event
	// burst never drives the kind-catalog re-read.
	WatchEvents(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterDataEventWatchFrame, error)
	// WatchObjects streams one kind's cached objects as a delta watch keyed by UID.
	// No frames while that kind hasn't synced.
	WatchObjects(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID, apiVersion, resource string) (<-chan types.ClusterDataObjectWatchFrame, error)
}

// The family accessors are stateless views onto the one *Service: the split is
// about the shape of the API, not a split of the control plane behind it.
type (
	clustersAPI  struct{ s *Service }
	cachesAPI    struct{ s *Service }
	discoveryAPI struct{ s *Service }
	syncsAPI     struct{ s *Service }
	dataAPI      struct{ s *Service }
)

func (s *Service) Clusters() Clusters { return clustersAPI{s} }

func (s *Service) Caches() Caches { return cachesAPI{s} }

func (s *Service) Discovery() Discovery { return discoveryAPI{s} }

func (s *Service) Syncs() Syncs { return syncsAPI{s} }

func (s *Service) Data() Data { return dataAPI{s} }

// Each family is asserted separately: satisfying ClusterService only proves the
// accessors exist, not that any family is fully implemented.
var (
	_ ClusterService = (*Service)(nil)
	_ Clusters       = clustersAPI{}
	_ Caches         = cachesAPI{}
	_ Discovery      = discoveryAPI{}
	_ Syncs          = syncsAPI{}
	_ Data           = dataAPI{}
)

// Service is the concrete ClusterService. It holds no state; the rebuild decides
// what control plane sits behind it.
type Service struct{}

// New builds the cluster boundary. The arguments are what a real control plane
// needs — where its state lives, which kubeconfig it follows, the resync bus it
// listens on — and are kept so wiring one up is not a change to internal/app.
func New(dataDir, kubeconfigPath string, pokeSvc *poke.Service) (*Service, error) {
	return &Service{}, nil
}

// Start launches the background work and returns the func that drains it. ctx
// bounds startup; the stop func takes a drain deadline. Call stop before Close.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close releases the boundary's resources. Call after the stop func returns.
func (s *Service) Close() error {
	return nil
}

func (s *Service) RetryConnection(ctx context.Context, id types.ClusterID) error {
	panic("not implemented")
}

func (s *Service) GetConnection(id types.ClusterID) *rest.Config {
	panic("not implemented")
}

func (s *Service) WatchObjectEvents(ctx context.Context, id types.ObjectID, category *string) (*Stream[types.EventWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) List(ctx context.Context) ([]*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Get(ctx context.Context, id types.ClusterID) (*types.Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Watch(ctx context.Context) (*Stream[types.ClusterWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) ListEvents(ctx context.Context, id types.ClusterID, category *string, limit *int) ([]types.Event, error) {
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

func (a cachesAPI) List(ctx context.Context, clusterID *types.ClusterID) ([]*types.ClusterCache, error) {
	panic("not implemented")
}

func (a cachesAPI) Watch(ctx context.Context) (*Stream[types.ClusterCacheWatchFrame], error) {
	panic("not implemented")
}

func (a cachesAPI) GetStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (*types.ClusterCacheStats, error) {
	panic("not implemented")
}

func (a cachesAPI) WatchStats(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterCacheStats, error) {
	panic("not implemented")
}

func (a cachesAPI) ListEvents(ctx context.Context, id types.ClusterCacheID, category *string, limit *int) ([]types.Event, error) {
	panic("not implemented")
}

func (a cachesAPI) Clear(ctx context.Context, id types.ClusterID) (*types.Cluster, error) {
	panic("not implemented")
}

func (a cachesAPI) WatchSyncHealth(ctx context.Context) (*Stream[types.ClusterCacheSyncHealth], error) {
	panic("not implemented")
}

func (a discoveryAPI) Watch(ctx context.Context) (*Stream[types.ClusterCacheGVRDiscoveryWatchFrame], error) {
	panic("not implemented")
}

func (a discoveryAPI) GetStats(ctx context.Context, id types.ClusterCacheGVRDiscoveryID) (*types.ClusterCacheGVRDiscoveryStats, error) {
	panic("not implemented")
}

func (a syncsAPI) Get(ctx context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSync, error) {
	panic("not implemented")
}

func (a syncsAPI) List(ctx context.Context, cacheID *types.ClusterCacheID) ([]*types.ClusterCacheGVRSync, error) {
	panic("not implemented")
}

func (a syncsAPI) Watch(ctx context.Context, cacheID types.ClusterCacheID) (*Stream[types.ClusterCacheGVRSyncWatchFrame], error) {
	panic("not implemented")
}

func (a syncsAPI) GetStats(ctx context.Context, id types.ClusterCacheGVRSyncID) (*types.ClusterCacheGVRSyncStats, error) {
	panic("not implemented")
}

func (a syncsAPI) SnapshotStats() map[types.ClusterCacheGVRSyncID]types.ClusterCacheGVRSyncStats {
	panic("not implemented")
}

func (a syncsAPI) ListEvents(ctx context.Context, id types.ClusterCacheGVRSyncID, category *string, limit *int) ([]types.Event, error) {
	panic("not implemented")
}

func (a dataAPI) ListKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) ([]types.ClusterDataKind, error) {
	panic("not implemented")
}

func (a dataAPI) WatchKinds(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterDataKindWatchFrame, error) {
	panic("not implemented")
}

func (a dataAPI) WatchEvents(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID) (<-chan types.ClusterDataEventWatchFrame, error) {
	panic("not implemented")
}

func (a dataAPI) WatchObjects(ctx context.Context, clusterID types.ClusterID, cacheID types.ClusterCacheID, apiVersion, resource string) (<-chan types.ClusterDataObjectWatchFrame, error) {
	panic("not implemented")
}
