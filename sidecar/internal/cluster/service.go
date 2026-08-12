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

// Package cluster is the kstack sidecar's Kubernetes boundary: ClusterService and
// its five record families (Clusters, Caches, Discovery, Syncs, Data). Every beehive
// detail — the store, names, the owner chain, the spec/status split, the delta-watch
// mapping — and the on-disk cache files live behind it.
//
// Below it: domain (the shared vocabulary), controllers (the reconcile loops), and
// cache/store (the per-cluster SQLite mirrors).
package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gochan/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the frontend-facing boundary of the cluster backend; every
// beehive detail (names, owner chain, spec/status split, delta-watch mapping)
// lives behind it. Each record family hangs off its own sub-interface; only the
// connection surface, which reads no beehive object, sits at the top level.
// Delta watches follow docs/adr/2026-08-09-delta-watch-protocol.md; watch
// channels close when ctx ends.
type ClusterService interface {
	Clusters() Clusters
	Caches() Caches
	Discovery() Discovery
	Syncs() Syncs
	Data() Data

	// RetryConnection forces an immediate out-of-band re-probe; the outcome lands
	// on the record's conditions and reaches watchers through Clusters().Watch.
	RetryConnection(ctx context.Context, id domain.ClusterID) error
	// GetConnection returns the live REST config for id, or nil when not connected.
	GetConnection(id domain.ClusterID) *rest.Config
}

// Clusters is the Cluster record surface: the tracked clusters, their spec
// toggles, and the per-cluster streams that deliberately do not ride the record
// watch.
type Clusters interface {
	// List returns every tracked, non-deletion-pending cluster. Cache sync status
	// is joined by the caller via Caches().Watch.
	List(ctx context.Context) ([]*domain.Cluster, error)
	// Get returns one cluster by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id domain.ClusterID) (*domain.Cluster, error)
	// Watch streams the cluster list as a delta watch; deletion-pending surfaces
	// as Deleted.
	Watch(ctx context.Context) (<-chan domain.ClusterChange, error)
	// ListEvents returns a cluster's beehive event timeline (newest run first),
	// optionally filtered by category and bounded by limit. Decoupled from Watch —
	// event chatter never re-emits the cluster.
	ListEvents(ctx context.Context, id domain.ClusterID, category *string, limit *int) ([]domain.Event, error)
	// WatchEvents streams a cluster's event log as bare runs (snapshot then live;
	// the consumer upserts by Event.ID). Independent of Watch.
	WatchEvents(ctx context.Context, id domain.ClusterID, category *string) (<-chan domain.Event, error)
	// WatchSchedule streams a cluster's reconcile-schedule gauge (next requeue
	// time) — the live source for the UI's next-attempt countdown, since a
	// scheduling change fires no object WatchList.
	WatchSchedule(ctx context.Context, id domain.ClusterID) (<-chan domain.Schedule, error)
	// SetEnabled enables or disables a cluster and returns the updated record.
	SetEnabled(ctx context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error)
	// Delete deletes the Cluster object; beehive GC cascades to ClusterCache.
	Delete(ctx context.Context, id domain.ClusterID) error
}

// Caches is the ClusterCache record surface. A cluster owns zero or one cache at
// steady state, but a UID migration leaves the old one behind — so every
// per-cache read names the exact cache it means, never "the" cache of a cluster.
type Caches interface {
	// Watch streams cache records as a delta watch parallel to Clusters().Watch;
	// the caller joins caches onto clusters by ClusterID.
	Watch(ctx context.Context) (<-chan domain.ClusterCacheChange, error)
	// GetStats returns live on-disk statistics for one ClusterCache.
	GetStats(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (*domain.ClusterCacheStats, error)
	// WatchStats streams one cache's contents as a live gauge. A stream, not a
	// ClusterCache field: a settled cache's object never changes, so a field would
	// freeze at subscribe time.
	WatchStats(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheStats, error)
	// ListEvents is the ClusterCache-kind counterpart of Clusters().ListEvents.
	ListEvents(ctx context.Context, id domain.ClusterCacheID, category *string, limit *int) ([]domain.Event, error)
	// WatchEvents is the ClusterCache-kind counterpart of Clusters().WatchEvents.
	WatchEvents(ctx context.Context, id domain.ClusterCacheID, category *string) (<-chan domain.Event, error)
	// Clear deletes the on-disk cache and bounces its syncs; the record stays.
	Clear(ctx context.Context, id domain.ClusterID) (*domain.Cluster, error)
	// WatchSyncHealth streams every cache's sync verdict folded from its per-kind
	// records — the whole-cache rollup an always-mounted consumer carries. Unscoped
	// where the rest of this family is per-cache: one fold serves the whole fleet.
	WatchSyncHealth(ctx context.Context) (<-chan domain.ClusterCacheSyncHealth, error)
}

// Discovery is the per-cache GVR-discovery anchor: exactly one per cache, naming
// the kinds that cache's cluster serves.
type Discovery interface {
	// Watch streams the caches' discovery anchors, joined onto caches by CacheID.
	Watch(ctx context.Context) (<-chan domain.ClusterCacheGVRDiscoveryChange, error)
	// GetStats returns one anchor's live gauges from controller memory; nil before
	// this process's first pass. Out of status so a pass never wakes dependents for
	// a UI-only number (see ClusterCacheGVRDiscoveryStatus).
	GetStats(ctx context.Context, id domain.ClusterCacheGVRDiscoveryID) (*domain.ClusterCacheGVRDiscoveryStats, error)
}

// Syncs is the per-kind sync-record surface — one record per kind a cache serves,
// which is why Watch is cache-scoped where its siblings in other families are
// fleet-wide.
type Syncs interface {
	// Watch streams one cache's per-kind sync records. Cache-scoped: one record per
	// served kind, so an unscoped stream would be a firehose.
	Watch(ctx context.Context, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheGVRSyncChange, error)
	// GetStats returns one synced kind's freshness stamps (out of band from the
	// object watch; see ClusterCacheGVRSyncStats).
	GetStats(ctx context.Context, id domain.ClusterCacheGVRSyncID) (*domain.ClusterCacheGVRSyncStats, error)
	// SnapshotStats returns every synced kind's stamps under one lock — what the
	// sync-health rollup folds.
	SnapshotStats() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats
	// ListEvents returns one synced kind's beehive event timeline.
	ListEvents(ctx context.Context, id domain.ClusterCacheGVRSyncID, category *string, limit *int) ([]domain.Event, error)
	// WatchEvents streams one synced kind's event log as bare runs.
	WatchEvents(ctx context.Context, id domain.ClusterCacheGVRSyncID, category *string) (<-chan domain.Event, error)
}

// Data is the cached Kubernetes content in one cache's db — the only family whose
// reads leave beehive entirely. Every method degrades to empty (no error, no
// frames) while that cache's db isn't open: never synced, or sync paused.
type Data interface {
	// ListKinds returns one cache's discovered kind catalog.
	ListKinds(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) ([]domain.ClusterDataKind, error)
	// WatchKinds streams one cache's kind catalog as a delta watch (per-kind counts
	// update live).
	WatchKinds(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterDataKindChange, error)
	// WatchEvents streams one cache's cached Kubernetes Events (newest window) as a
	// delta watch, keyed by event UID. Wakes on the events-only broker, so an event
	// burst never drives the kind-catalog re-read.
	WatchEvents(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterDataEventChange, error)
	// WatchObjects streams one kind's cached objects as a delta watch keyed by UID.
	// No frames while that kind hasn't synced.
	WatchObjects(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID, apiVersion, resource string) (<-chan domain.ClusterDataObjectChange, error)
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

// coreController is the subset of *ClusterCoreController that Service drives.
// An interface so white-box service tests inject a fake (no production nil-guard).
type coreController interface {
	StartBackground()
	StopBackground()
	Reprobe(domain.ClusterID)
	// WatchProbe streams whether a cluster's connection probe is in flight
	// (current-on-subscribe, then per transition); merged into Clusters().WatchSchedule.
	WatchProbe(ctx context.Context, id domain.ClusterID) <-chan bool
}

// Service is the concrete ClusterService and the whole cluster control plane:
// beehive store + instance, kubeconfig watcher, beehive clients, controllers,
// importer, connection manager, and per-cluster cache manager.
type Service struct {
	bh      *beehive.Beehive
	bhStore beehive.Store

	watcher *k8shelpers.KubeConfigWatcher

	coreClient  beehive.Client[domain.ClusterSpec, domain.ClusterStatus]
	cacheClient beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus]
	// gvrDiscoveryClient and gvrSyncClient are read-only here; their specs belong
	// to the controllers that own them.
	gvrDiscoveryClient beehive.Client[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]
	gvrSyncClient      beehive.Client[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]
	cacheManager       *store.Manager
	connMgr            *connections.Manager
	coreCtrl           coreController
	// gvrDiscoveryCtrl is read for its in-memory live gauges (GVRDiscoveryStats).
	gvrDiscoveryCtrl *controllers.ClusterCacheGVRDiscoveryController
	// gvrSyncCtrl is held for its worker drain at shutdown.
	gvrSyncCtrl *controllers.ClusterCacheGVRSyncController

	// syncHealth is the shared sync-verdict fold, started on first subscriber and running
	// until Close. Guarded by syncHealthMu, which covers the lazy start only.
	syncHealthMu   sync.Mutex
	syncHealth     *watch.Hub[syncHealthSnapshot]
	syncHealthStop context.CancelFunc
	// syncHealthDone closes when the fold goroutine has fully unwound — its WatchList
	// leases release in its defers, so shutdown must wait for it before beehive drains.
	syncHealthDone chan struct{}
	// syncHealthFoldExit is a test seam run as the fold's last act; nil in production.
	syncHealthFoldExit func()
	// syncHealthClosed latches at shutdown so a late subscriber can't lazily start a
	// new, unstoppable fold against a beehive being torn down.
	syncHealthClosed bool

	importer *controllers.KubeconfigImporter
	pokeSvc  *poke.Service

	// dataKindsDebounce / dataEventsDebounce / dataObjectsDebounce / cacheStatsDebounce
	// bound each watch's trailing-edge re-read rate — a burst of write pings collapses
	// into one re-read per interval.
	dataKindsDebounce   time.Duration
	dataEventsDebounce  time.Duration
	dataObjectsDebounce time.Duration
	cacheStatsDebounce  time.Duration
}

// Floors the kind-catalog re-read interval: live enough for the nav's counts, coarse enough
// to collapse a high-churn cluster's write pings.
const defaultDataKindsDebounce = 250 * time.Millisecond

// Floors the events-watch re-read interval. Coarser than the catalog's, since events are the
// highest-volume stream.
const defaultDataEventsDebounce = 500 * time.Millisecond

// Floors the objects-watch re-read interval; matches the kind-catalog cadence, since object
// writes drive the same broker.
const defaultDataObjectsDebounce = 250 * time.Millisecond

// Floors the cache-summary re-read interval. Deliberately coarsest — it backs one
// "N objects across M kinds" line, and the gauge exists to stop being stale, not to be instant.
const defaultCacheStatsDebounce = time.Second

// New builds the cluster control plane rooted at dataDir (beehive.db, clusters/).
// The returned *Service owns the watcher + beehive + importer + cache lifecycle via
// Start/Close; both are cluster-only, so the service owns them outright.
func New(dataDir, kubeconfigPath string, pokeSvc *poke.Service) (*Service, error) {
	// Publishes *api.Config snapshots; consumed by the importer and core controller.
	watcher, err := k8shelpers.NewKubeConfigWatcher(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("init kubeconfig watcher: %w", err)
	}

	bhStore, err := beehivesqlite.Open(filepath.Join(dataDir, "beehive.db"))
	if err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("open beehive store: %w", err)
	}
	// WithEventRetention bounds each (object, category) timeline to maxEventRuns runs.
	// No kind runs a periodic full pass: controllers re-arm via RequeueAfter and the
	// out-of-band buses cover the rest (see the Register calls for the startup pass).
	bh, err := beehive.New(bhStore, beehive.WithEventRetention(maxEventRuns, 0))
	if err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("init beehive: %w", err)
	}

	// The Service's own clients back the GraphQL-facing reads/watches; controllers
	// mint their own from the runtime.
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	cacheClient := beehive.NewClient[domain.ClusterCacheSpec, domain.ClusterCacheStatus](bh, domain.ClusterCacheGroupKind)
	gvrDiscoveryClient := beehive.NewClient[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus](bh, domain.ClusterCacheGVRDiscoveryGroupKind)
	gvrSyncClient := beehive.NewClient[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus](bh, domain.ClusterCacheGVRSyncGroupKind)

	// Owns the per-cluster SQLite cache files under <dataDir>/clusters/.
	cacheManager := store.NewManager(dataDir)

	connMgr := connections.NewManager()

	set, err := controllers.Install(bh, watcher, cacheManager, connMgr, pokeSvc)
	if err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}

	return &Service{
		bh:                  bh,
		bhStore:             bhStore,
		watcher:             watcher,
		coreClient:          coreClient,
		cacheClient:         cacheClient,
		gvrDiscoveryClient:  gvrDiscoveryClient,
		gvrSyncClient:       gvrSyncClient,
		cacheManager:        cacheManager,
		connMgr:             connMgr,
		coreCtrl:            set.Core,
		gvrDiscoveryCtrl:    set.Discovery,
		gvrSyncCtrl:         set.Sync,
		importer:            controllers.NewKubeconfigImporter(watcher, coreClient),
		pokeSvc:             pokeSvc,
		dataKindsDebounce:   defaultDataKindsDebounce,
		dataEventsDebounce:  defaultDataEventsDebounce,
		dataObjectsDebounce: defaultDataObjectsDebounce,
		cacheStatsDebounce:  defaultCacheStatsDebounce,
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

	// Background work: the core controller drives the retry + poke buses; the sync
	// controller restarts live workers on poke. Both write status, so they share
	// the same start/drain window.
	s.coreCtrl.StartBackground()
	s.gvrSyncCtrl.StartPoke()

	// Watcher before importer: the importer subscribes current-on-subscribe.
	s.watcher.Start()
	s.importer.Start()

	stop := func(ctx context.Context) error {
		s.watcher.Close()
		s.importer.Stop()

		// Stop out-of-band re-probe/resync work before draining.
		s.coreCtrl.StopBackground()
		s.gvrSyncCtrl.StopPoke()

		// Order is load-bearing: the read-only sync-health fold first (its watches
		// close on our terms), then beehive drains (no reconcile can start another
		// worker behind us), then the workers stop, then the cache manager closes
		// the ClusterDB handles they write into.
		s.stopSyncHealthFold(ctx)
		bhErr := bhStop(ctx)
		workerErr := s.gvrSyncCtrl.StopWorkers(ctx)
		cacheErr := s.cacheManager.Shutdown(ctx)
		return errors.Join(bhErr, workerErr, cacheErr)
	}
	return stop, nil
}

// Close releases the beehive store after Stop has finished. Call after Stop.
func (s *Service) Close() error {
	return s.bhStore.Close()
}

// RetryConnection implements ClusterService: fire-and-forget dispatch onto the
// retry bus after an existence check — no spec write, and backoff-neutral (a
// failed manual probe leaves beehive's reconnect ladder untouched); see
// docs/adr/2026-08-09-connection-probing.md.
func (s *Service) RetryConnection(ctx context.Context, id domain.ClusterID) error {
	if _, err := s.clusterByID(ctx, id); err != nil {
		return err
	}
	s.coreCtrl.Reprobe(id)
	return nil
}

// GetConnection implements ClusterService.
func (s *Service) GetConnection(id domain.ClusterID) *rest.Config {
	cfg, _ := s.connMgr.Get(id)
	return cfg
}

// clusterByID fetches a Cluster object by id, mapping beehive.ErrNotFound to the
// package's ErrNotFound.
func (s *Service) clusterByID(ctx context.Context, id domain.ClusterID) (*beehive.Object[domain.ClusterSpec, domain.ClusterStatus], error) {
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, domain.ErrNotFound
	}
	return obj, err
}

// cacheRef resolves the on-disk locator for a cluster's active cache. found is
// false when there is no active cache (never probed, or the UID's cache is
// missing/torn-down) — "no cache", not an error.
func (s *Service) cacheRef(ctx context.Context, id domain.ClusterID) (store.CacheRef, bool, error) {
	clusterObj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	activeUID := domain.ClusterActiveUID(clusterObj)
	if activeUID == "" {
		return store.CacheRef{}, false, nil
	}
	cacheObj, err := s.cacheClient.GetByName(ctx, domain.ClusterCacheName(id, activeUID))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	return domain.NewCacheRef(beehive.ObjectID(id), cacheObj.ID), true, nil
}
