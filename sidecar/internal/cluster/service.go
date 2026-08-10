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
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gochan/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the frontend-facing boundary of the cluster backend; every
// beehive detail (names, owner chain, spec/status split, delta-watch mapping)
// lives behind it. Delta watches follow docs/adr/2026-08-09-delta-watch-protocol.md;
// watch channels close when ctx ends.
type ClusterService interface {
	// List returns every tracked, non-deletion-pending cluster. Cache sync status
	// is joined by the caller via WatchCaches.
	List(ctx context.Context) ([]*Cluster, error)
	// Get returns one cluster by id, or (nil, nil) when unknown or deletion-pending.
	Get(ctx context.Context, id ClusterID) (*Cluster, error)
	// Watch streams the cluster list as a delta watch; deletion-pending surfaces
	// as Deleted.
	Watch(ctx context.Context) (<-chan ClusterChange, error)
	// WatchCaches streams cache records as a parallel delta watch; the caller
	// joins caches onto clusters by ClusterID.
	WatchCaches(ctx context.Context) (<-chan ClusterCacheChange, error)
	// WatchGVRDiscoveries streams the caches' GVR-discovery children, joined onto
	// caches by CacheID. Unscoped: exactly one per cache.
	WatchGVRDiscoveries(ctx context.Context) (<-chan ClusterCacheGVRDiscoveryChange, error)
	// WatchCacheSyncHealth streams every cache's sync verdict folded from its
	// per-kind records — the whole-cache rollup an always-mounted consumer carries.
	WatchCacheSyncHealth(ctx context.Context) (<-chan ClusterCacheSyncHealth, error)
	// ClusterCacheGVRSyncEvents returns one synced kind's beehive event timeline.
	ClusterCacheGVRSyncEvents(ctx context.Context, id ClusterCacheGVRSyncID, category *string, limit *int) ([]Event, error)
	// ClusterCacheGVRSyncEventsWatch streams one synced kind's event log as bare runs.
	ClusterCacheGVRSyncEventsWatch(ctx context.Context, id ClusterCacheGVRSyncID, category *string) (<-chan Event, error)
	// WatchGVRSyncs streams one cache's per-kind sync records. Cache-scoped: one
	// record per served kind, so an unscoped stream would be a firehose.
	WatchGVRSyncs(ctx context.Context, cacheID ClusterCacheID) (<-chan ClusterCacheGVRSyncChange, error)
	// ClusterCacheStatsWatch streams one cache's contents as a live gauge. A
	// stream, not a ClusterCache field: a settled cache's object never changes,
	// so a field would freeze at subscribe time.
	ClusterCacheStatsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterCacheStats, error)
	// GVRSyncStats returns one synced kind's freshness stamps (out of band from
	// the object watch; see ClusterCacheGVRSyncStats).
	GVRSyncStats(ctx context.Context, id ClusterCacheGVRSyncID) (*ClusterCacheGVRSyncStats, error)
	// GVRSyncStatsSnapshot returns every synced kind's stamps under one lock —
	// what the sync-health rollup folds.
	GVRSyncStatsSnapshot() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats
	// GVRDiscoveryStats returns one discovery record's live gauges from controller
	// memory; nil before this process's first pass. Out of status so a pass never
	// wakes dependents for a UI-only number (see ClusterCacheGVRDiscoveryStatus).
	GVRDiscoveryStats(ctx context.Context, id ClusterCacheGVRDiscoveryID) (*ClusterCacheGVRDiscoveryStats, error)
	// CacheStats returns live on-disk statistics for one ClusterCache (per-cache,
	// not per-cluster: a cluster can own several caches).
	CacheStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*ClusterCacheStats, error)
	// ClusterDataKinds returns one cache's discovered kind catalog; empty when
	// that cache's db isn't open.
	ClusterDataKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) ([]ClusterDataKind, error)
	// ClusterDataKindsWatch streams one cache's kind catalog as a delta watch
	// (per-kind counts update live). No frames while the cache's db isn't open.
	ClusterDataKindsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataKindChange, error)
	// ClusterDataEventsWatch streams one cache's cached Kubernetes Events (newest
	// window) as a delta watch, keyed by event UID. Wakes on the events-only
	// broker, so an event burst never drives the kind-catalog re-read. No frames
	// while the db isn't open.
	ClusterDataEventsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataEventChange, error)
	// ClusterDataObjectsWatch streams one kind's cached objects as a delta watch
	// keyed by UID. No frames while the db isn't open or the kind hasn't synced.
	ClusterDataObjectsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (<-chan ClusterDataObjectChange, error)
	// ClusterEvents returns a cluster's beehive event timeline (newest run first),
	// optionally filtered by category and bounded by limit. Decoupled from Watch —
	// event chatter never re-emits the cluster.
	ClusterEvents(ctx context.Context, id ClusterID, category *string, limit *int) ([]Event, error)
	// ClusterEventsWatch streams a cluster's event log as bare runs (snapshot then
	// live; the consumer upserts by Event.ID). Independent of Watch.
	ClusterEventsWatch(ctx context.Context, id ClusterID, category *string) (<-chan Event, error)
	// ClusterCacheEvents is the ClusterCache-kind counterpart of ClusterEvents.
	ClusterCacheEvents(ctx context.Context, id ClusterCacheID, category *string, limit *int) ([]Event, error)
	// ClusterCacheEventsWatch is the ClusterCache-kind counterpart of ClusterEventsWatch.
	ClusterCacheEventsWatch(ctx context.Context, id ClusterCacheID, category *string) (<-chan Event, error)
	// ClusterScheduleWatch streams a cluster's reconcile-schedule gauge (next
	// requeue time) — the live source for the UI's next-attempt countdown, since
	// a scheduling change fires no object WatchList.
	ClusterScheduleWatch(ctx context.Context, id ClusterID) (<-chan Schedule, error)
	// SetEnabled enables or disables a cluster and returns the updated record.
	SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// RetryConnection forces an immediate out-of-band re-probe; the outcome lands
	// on the record's conditions and reaches watchers through Watch.
	RetryConnection(ctx context.Context, id ClusterID) error
	// ClearCache deletes the on-disk cache and bounces its syncs; the record stays.
	ClearCache(ctx context.Context, id ClusterID) (*Cluster, error)
	// Delete deletes the Cluster object; beehive GC cascades to ClusterCache.
	Delete(ctx context.Context, id ClusterID) error
	// GetConnection returns the live REST config for id, or nil when not connected.
	GetConnection(id ClusterID) *rest.Config
}

// coreController is the subset of *ClusterCoreController that Service drives.
// An interface so white-box service tests inject a fake (no production nil-guard).
type coreController interface {
	StartBackground()
	StopBackground()
	Reprobe(ClusterID)
	// WatchProbe streams whether a cluster's connection probe is in flight
	// (current-on-subscribe, then per transition); merged into ClusterScheduleWatch.
	WatchProbe(ctx context.Context, id ClusterID) <-chan bool
}

// controllerRuntime is the shared controller environment (beehive's Manager
// analogue): controller constructors take one *controllerRuntime plus their own
// specifics. It holds the beehive instance, not per-kind clients — each controller
// mints the typed clients it needs from rt.bh, keeping its kinds explicit. Any
// field may be nil in a test that doesn't exercise it.
type controllerRuntime struct {
	bh           *beehive.Beehive
	connMgr      *ConnectionManager
	cacheManager *store.Manager
	pokeSvc      *poke.Service
	// cachePolicies is the per-cache client budget (rate limiter + LIST semaphore),
	// shared by the sync and discovery controllers; lazily created so a bare test
	// runtime still gets one.
	cachePolicies *cacheClientPolicies
}

// policies returns the runtime's per-cache client budgets, created on first use
// so every controller built from one runtime shares them.
func (rt *controllerRuntime) policies() *cacheClientPolicies {
	if rt.cachePolicies == nil {
		rt.cachePolicies = newCacheClientPolicies()
	}
	return rt.cachePolicies
}

// Service is the concrete ClusterService and the whole cluster control plane:
// beehive store + instance, kubeconfig watcher, beehive clients, controllers,
// importer, connection manager, and per-cluster cache manager.
type Service struct {
	bh      *beehive.Beehive
	bhStore beehive.Store

	watcher *k8shelpers.KubeConfigWatcher

	coreClient  beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	// gvrDiscoveryClient and gvrSyncClient are read-only here; their specs belong
	// to the controllers that own them.
	gvrDiscoveryClient beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	gvrSyncClient      beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	cacheManager       *store.Manager
	connMgr            *ConnectionManager
	coreCtrl           coreController
	cacheCtrl          *ClusterCacheController
	// gvrDiscoveryCtrl is read for its in-memory live gauges (GVRDiscoveryStats).
	gvrDiscoveryCtrl *ClusterCacheGVRDiscoveryController
	// gvrSyncCtrl is held for its worker drain at shutdown.
	gvrSyncCtrl *ClusterCacheGVRSyncController

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

	importer *KubeconfigImporter
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

var _ ClusterService = (*Service)(nil)

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
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	gvrDiscoveryClient := beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](bh, ClusterCacheGVRDiscoveryGroupKind)
	gvrSyncClient := beehive.NewClient[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus](bh, ClusterCacheGVRSyncGroupKind)

	// Owns the per-cluster SQLite cache files under <dataDir>/clusters/.
	cacheManager := store.NewManager(dataDir)

	connMgr := NewConnectionManager()

	rt := &controllerRuntime{bh: bh, connMgr: connMgr, cacheManager: cacheManager, pokeSvc: pokeSvc}

	coreCtrl := NewClusterCoreController(rt, watcher, nil, nil)
	cacheCtrl := NewClusterCacheController(rt)
	// GVR discovery creates one ClusterCacheGVRSync per served kind (Events included);
	// the sync controller runs one worker per record. Pausing is Spec.Enabled, never
	// a deletion; a worker is only ever started/stopped by its own controller.
	cacheGVRDiscoveryCtrl := NewClusterCacheGVRDiscoveryController(rt)
	cacheGVRSyncCtrl := NewClusterCacheGVRSyncController(rt)

	// Register returns each kind's status-write ControllerClient; inject it before
	// bh.Start (a startup reconcile may write immediately) since controllers also
	// write status out-of-band. WithMaxRetryInterval caps the reconnect backoff.
	//
	// WithStartupFullPass is per kind: each of the four owns process-scoped state a
	// restart invalidates that the store never recorded (connections + sentinels,
	// running workers, in-memory RequeueAfter re-discovery), so beehive's owed pass
	// alone would leave settled objects unreconciled. See
	// docs/adr/2026-08-09-beehive-control-plane.md.
	//
	// WithConcurrency: a Cluster reconcile is mostly one network probe, so a single
	// worker would serialize every cluster behind one dial timeout; per-cluster
	// locks make concurrent reconciles of distinct clusters safe.
	coreCC, errCluster := beehive.Register(bh, ClusterGroupKind, coreCtrl,
		beehive.WithMaxRetryInterval(connectionMaxBackoff),
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	_, errCache := beehive.Register(bh, ClusterCacheGroupKind, cacheCtrl,
		beehive.WithStartupFullPass(true),
	)
	// Same concurrency rationale as Cluster: a pass is mostly one discovery request.
	_, errDiscovery := beehive.Register(bh, ClusterCacheGVRDiscoveryGroupKind, cacheGVRDiscoveryCtrl,
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	// Here the concurrency is about volume: hundreds of per-kind records per cache
	// on every startup pass.
	gvrSyncCC, errGVRSync := beehive.Register(bh, ClusterCacheGVRSyncGroupKind, cacheGVRSyncCtrl,
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(gvrSyncConcurrency),
	)
	if err := errors.Join(errCluster, errCache, errDiscovery, errGVRSync); err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}
	coreCtrl.SetControllerClient(coreCC)
	cacheGVRSyncCtrl.SetControllerClient(gvrSyncCC)

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
		coreCtrl:            coreCtrl,
		cacheCtrl:           cacheCtrl,
		gvrDiscoveryCtrl:    cacheGVRDiscoveryCtrl,
		gvrSyncCtrl:         cacheGVRSyncCtrl,
		importer:            NewKubeconfigImporter(watcher, coreClient),
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

// List implements ClusterService.
func (s *Service) List(ctx context.Context) ([]*Cluster, error) {
	return s.listClusters(ctx)
}

// listClusters reads every non-deletion-pending Cluster as a domain Cluster.
// Shared by List and Watch's seed + re-emit.
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
		c := s.buildCluster(obj)
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
	c := s.buildCluster(obj)
	return &c, nil
}

// CacheStats implements ClusterService. It stats the exact ClusterCache asked
// about — active or migrated-away — never "the" cache for a cluster.
func (s *Service) CacheStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*ClusterCacheStats, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	bytes, exists := s.cacheManager.CacheBytes(ref)
	if !exists {
		return &ClusterCacheStats{}, nil
	}
	db := s.cacheManager.Lookup(ref.CacheID)
	if db == nil {
		return &ClusterCacheStats{Exists: true, Bytes: bytes}, nil
	}
	st, err := readCacheStats(ctx, db, bytes)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// ClusterCacheStatsWatch implements ClusterService — one cache's contents as a
// live gauge. Its own stream because a ClusterCache field would freeze at
// subscribe time; see docs/adr/2026-08-09-status-propagation-gauges.md.
func (s *Service) ClusterCacheStatsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterCacheStats, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheGaugeWatch(ctx, s.cacheManager, ref.CacheID, s.cacheStatsDebounce,
		// Keyless object-write broker: every kind's writes move this total.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) { return db.ObjectsSubscribe() },
		func(ctx context.Context, db *store.ClusterDB) (ClusterCacheStats, error) {
			// Re-stat the file each read; nothing else carries its growth.
			bytes, _ := s.cacheManager.CacheBytes(ref)
			return readCacheStats(ctx, db, bytes)
		},
		// A closed cache reports what is on DISK, not zeroes: an open handle only
		// means something is syncing now, and reporting a closed file as
		// nonexistent would disable Clear cache on the rows the Orphaned group
		// exists to reclaim. Counts need an open handle, hence the separate
		// Exists field.
		func() ClusterCacheStats {
			bytes, exists := s.cacheManager.CacheBytes(ref)
			return ClusterCacheStats{Exists: exists, Bytes: bytes}
		},
	), nil
}

// readCacheStats rolls up the kind catalog's trigger-maintained per-kind counts
// (O(kinds), no object scan): total objects and the number of non-empty kinds. The
// catalog lists advertised-but-empty kinds too, so KindCount only counts count > 0.
func readCacheStats(ctx context.Context, db *store.ClusterDB, bytes int64) (ClusterCacheStats, error) {
	rows, err := db.Kinds(ctx)
	if err != nil {
		return ClusterCacheStats{}, err
	}
	objectCount, kindCount := 0, 0
	for _, r := range rows {
		// Events carry a real kind_counts value but live in their own table and are
		// not objects — excluded from the whole-cache totals.
		if r.APIVersion == eventsAPIVersion && r.Kind == eventsKind {
			continue
		}
		if r.Count > 0 {
			objectCount += r.Count
			kindCount++
		}
	}
	return ClusterCacheStats{
		Exists:      true,
		Bytes:       bytes,
		ObjectCount: objectCount,
		KindCount:   kindCount,
	}, nil
}

// ClusterDataKinds implements ClusterService. The caller names the exact cache
// whose catalog it wants; nil when that cache's db isn't open (never synced /
// sync paused), matching CacheStats' degrade-to-empty posture.
func (s *Service) ClusterDataKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) ([]ClusterDataKind, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	db := s.cacheManager.Lookup(ref.CacheID)
	if db == nil {
		return nil, nil
	}
	rows, err := db.Kinds(ctx)
	if err != nil {
		return nil, err
	}
	return toDataKinds(rows), nil
}

// toDataKinds maps a catalog read onto the domain records, preserving the reader's
// (api_version, kind) order the delta watch's Added burst relies on. Shared by the
// query and its live counterpart so the projections can't disagree.
func toDataKinds(rows []store.KindRow) []ClusterDataKind {
	kinds := make([]ClusterDataKind, len(rows))
	for i, r := range rows {
		kinds[i] = toDataKind(r)
	}
	return kinds
}

// toDataKind maps a store KindRow onto the domain ClusterDataKind 1:1.
func toDataKind(r store.KindRow) ClusterDataKind {
	return ClusterDataKind{
		APIVersion: r.APIVersion,
		Kind:       r.Kind,
		Resource:   r.Resource,
		Scope:      r.Scope,
		IsCRD:      r.IsCRD,
		Count:      r.Count,
	}
}

// dataKindKey keys the diff: APIVersion + Resource is unique per cache.
func dataKindKey(k ClusterDataKind) string {
	return k.APIVersion + "/" + k.Resource
}

// ClusterDataKindsWatch implements ClusterService: the kind catalog as a delta
// watch (an object write that changes a count re-emits its kind as Modified).
// cacheDeltaWatch owns the cache-lifecycle + coalescing loop.
func (s *Service) ClusterDataKindsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataKindChange, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, s.cacheManager, ref.CacheID, s.dataKindsDebounce,
		catalogSubscribe,
		func(ctx context.Context, db *store.ClusterDB) ([]ClusterDataKind, error) {
			rows, err := db.Kinds(ctx)
			if err != nil {
				return nil, err
			}
			return toDataKinds(rows), nil
		},
		dataKindKey,
		func(t ChangeType, k ClusterDataKind) ClusterDataKindChange {
			return ClusterDataKindChange{Type: t, Kind: k, CacheID: cacheID}
		},
	), nil
}

// ClusterDataEventsWatch implements ClusterService: the newest events window as a
// delta watch keyed by UID, on the events-only broker. The read is a bounded
// window, so an event aging out surfaces as Deleted even though its row may still
// exist — fine for a "latest events" table.
func (s *Service) ClusterDataEventsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataEventChange, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, s.cacheManager, ref.CacheID, s.dataEventsDebounce,
		(*store.ClusterDB).EventsSubscribe,
		func(ctx context.Context, db *store.ClusterDB) ([]ClusterDataEvent, error) {
			rows, err := db.Events(ctx, 0) // 0 → store's default window; ordered by last_seen DESC
			if err != nil {
				return nil, err
			}
			events := make([]ClusterDataEvent, len(rows))
			for i, r := range rows {
				events[i] = toDataEvent(r)
			}
			return events, nil
		},
		func(e ClusterDataEvent) string { return e.UID },
		func(t ChangeType, e ClusterDataEvent) ClusterDataEventChange {
			return ClusterDataEventChange{Type: t, Event: e, CacheID: cacheID}
		},
	), nil
}

// ClusterDataObjectsWatch implements ClusterService: one kind's cached objects as
// a delta watch keyed by UID. Each row carries the native body, so an in-place
// edit surfaces as Modified.
func (s *Service) ClusterDataObjectsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (<-chan ClusterDataObjectChange, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, s.cacheManager, ref.CacheID, s.dataObjectsDebounce,
		// Keyed to (apiVersion, resource) so unrelated writes don't wake it. The
		// broker routes by plural resource, not Kind, so the key stays valid across
		// a CRD Kind remap; see docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource(apiVersion, resource)
		},
		func(ctx context.Context, db *store.ClusterDB) ([]ClusterDataObject, error) {
			rows, err := db.Objects(ctx, apiVersion, resource) // ordered by (namespace, name)
			if err != nil {
				return nil, err
			}
			objects := make([]ClusterDataObject, len(rows))
			for i, r := range rows {
				objects[i] = toDataObject(r)
			}
			return objects, nil
		},
		func(o ClusterDataObject) string { return o.UID },
		func(t ChangeType, o ClusterDataObject) ClusterDataObjectChange {
			return ClusterDataObjectChange{Type: t, Object: o, CacheID: cacheID, APIVersion: apiVersion, Resource: resource}
		},
	), nil
}

// toDataObject maps a store ObjectRow onto ClusterDataObject 1:1 (unix-millis 0 →
// zero time, consistent across reads so the watch diff compares equal). RawJSON is
// part of the struct, so an in-place edit surfaces as Modified.
func toDataObject(r store.ObjectRow) ClusterDataObject {
	return ClusterDataObject{
		UID:               r.UID,
		APIVersion:        r.APIVersion,
		Kind:              r.Kind,
		Namespace:         r.Namespace,
		Name:              r.Name,
		CreationTimestamp: millisToTime(r.CreatedAt),
		RawJSON:           RawJSON(r.RawJSON),
	}
}

// toDataEvent maps a store EventRow onto ClusterDataEvent 1:1 (unix-millis 0 →
// zero time).
func toDataEvent(r store.EventRow) ClusterDataEvent {
	return ClusterDataEvent{
		UID:               r.UID,
		Type:              r.Type,
		Reason:            r.Reason,
		Message:           r.Message,
		Count:             r.Count,
		FirstSeen:         millisToTime(r.FirstSeen),
		LastSeen:          millisToTime(r.LastSeen),
		InvolvedKind:      r.InvolvedKind,
		InvolvedNamespace: r.InvolvedNS,
		InvolvedName:      r.InvolvedName,
	}
}

// millisToTime converts unix-millis to time.Time (0 → zero Time), built so two
// reads of the same row compare equal in the watch diff.
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// catalogSubscribe wakes on a write to EITHER broker: the catalog's counts span
// both tables (object triggers + event triggers), and object-broker-only would
// freeze the Events badge on an event-busy, object-quiet cluster. This doesn't
// undercut the broker split — the split protects the EXPENSIVE per-kind object
// re-reads; Kinds is O(kinds) and debounced besides.
func catalogSubscribe(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
	objects, stopObjects := db.ObjectsSubscribe()
	events, stopEvents := db.EventsSubscribe()

	out := make(chan store.WriteWake, 1)
	done := make(chan struct{})
	go func() {
		// Closing out signals the db went away — but only when BOTH brokers have
		// closed; one alone still leaves the other's writes worth reporting.
		defer close(out)
		for {
			select {
			case <-done:
				return
			case _, ok := <-objects:
				if !ok {
					objects = nil // the broker closed; keep serving the other
					if events == nil {
						return
					}
					continue
				}
			case _, ok := <-events:
				if !ok {
					events = nil
					if objects == nil {
						return
					}
					continue
				}
			}
			// Coalesce: a buffered ping already says "something changed".
			select {
			case out <- store.WriteWake{}:
			default:
			}
		}
	}()
	return out, func() {
		close(done)
		stopObjects()
		stopEvents()
	}
}

// cacheDeltaWatch is the shared machinery behind the three ClusterData*Watch
// streams: it follows one cache's db across its lifecycle (WatchDB bind/rebind),
// coalesces write pings on a trailing-edge debounce, and on each fire re-reads a
// keyed snapshot and diffs it against the last — Added/Modified/Deleted per key,
// and Deleted for every held key when the cache closes. `snapshot` must return an
// ordered slice (its order is the Added-burst order); T must be comparable so a
// changed value is detected by ==. Closes when ctx ends or the store shuts down.
func cacheDeltaWatch[T comparable, C any](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	snapshot func(context.Context, *store.ClusterDB) ([]T, error),
	keyOf func(T) string,
	mkChange func(ChangeType, T) C,
) <-chan C {
	out := make(chan C, 1)
	prev := map[string]T{}

	// emit diffs a fresh snapshot against prev: Added/Modified in snapshot order,
	// then Deleted for vanished keys (map order).
	emit := func(db *store.ClusterDB) (bool, bool) {
		items, err := snapshot(ctx, db)
		if err != nil {
			if ctx.Err() != nil {
				return false, false
			}
			// Keep the stream and ask for a retry; waiting for the next write ping
			// would strand a kind nobody writes to.
			slog.Warn("clusterservice: cache watch read failed", "cache", cacheID, "err", err)
			return true, true
		}
		next := make(map[string]T, len(items))
		for _, v := range items {
			key := keyOf(v)
			next[key] = v
			old, existed := prev[key]
			switch {
			case !existed:
				if !send(ctx, out, mkChange(ChangeAdded, v)) {
					return false, false
				}
			case old != v:
				if !send(ctx, out, mkChange(ChangeModified, v)) {
					return false, false
				}
			}
		}
		for key, v := range prev {
			if _, ok := next[key]; !ok {
				if !send(ctx, out, mkChange(ChangeDeleted, v)) {
					return false, false
				}
			}
		}
		prev = next
		return true, false
	}

	// emitEmpty sends one Deleted per held value and clears prev — run when the
	// cache closes so a never-reopened cache leaves no stale rows.
	emitEmpty := func() bool {
		for _, v := range prev {
			if !send(ctx, out, mkChange(ChangeDeleted, v)) {
				return false
			}
		}
		prev = map[string]T{}
		return true
	}

	go func() {
		defer close(out)
		cacheWatchLoop(ctx, mgr, cacheID, debounceDur, subscribe, emit, emitEmpty)
	}()
	return out
}

// cacheGaugeWatch is cacheDeltaWatch's gauge counterpart: one whole-cache
// measurement, re-read on the same cadence, emitted only when it CHANGES (the
// dedupe is what makes per-write pings affordable). `closed` supplies the value
// to emit when the cache goes away. See
// docs/adr/2026-08-09-status-propagation-gauges.md.
func cacheGaugeWatch[T comparable](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	read func(context.Context, *store.ClusterDB) (T, error),
	closed func() T,
) <-chan T {
	out := make(chan T, 1)
	var (
		last T
		sent bool
	)

	emitIfChanged := func(v T) bool {
		if sent && v == last {
			return true
		}
		if !send(ctx, out, v) {
			return false
		}
		last, sent = v, true
		return true
	}

	go func() {
		defer close(out)
		cacheWatchLoop(ctx, mgr, cacheID, debounceDur, subscribe,
			func(db *store.ClusterDB) (bool, bool) {
				v, err := read(ctx, db)
				if err != nil {
					if ctx.Err() != nil {
						return false, false
					}
					// Keep the stream and retry; a failed read that waited for the
					// next ping would freeze the deduped value with no way to tell.
					slog.Warn("clusterservice: cache gauge read failed", "cache", cacheID, "err", err)
					return true, true
				}
				return emitIfChanged(v), false
			},
			func() bool { return emitIfChanged(closed()) },
		)
	}()
	return out
}

// cacheWatchLoop is the shared half of every per-cache stream: it binds to the
// cache's db via WatchDB (binding when it opens, rebinding on delete+reopen) and
// coalesces write pings on a trailing-edge debounce. onFire runs once per bind and
// once per debounced burst; onClosed runs when the cache goes away; either
// returning false ends the stream.
func cacheWatchLoop(
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	onFire func(*store.ClusterDB) (keep bool, retry bool),
	onClosed func() bool,
) {
	handles, cancelHandles := mgr.WatchDB(cacheID)
	defer cancelHandles()

	// db/pings track the bound handle and its ping stream; nil while no cache is open.
	var (
		db        *store.ClusterDB
		pings     <-chan store.WriteWake
		cancelSub func()
	)
	defer func() {
		if cancelSub != nil {
			cancelSub()
		}
	}()

	// `armed` tracks a pending re-read; timer starts disarmed (Go timers deliver no
	// stale tick after Stop/Reset, so no manual drain).
	debounce := time.NewTimer(debounceDur)
	debounce.Stop()
	defer debounce.Stop()
	// A failed read schedules its OWN retry: the broker is resource-keyed, so a
	// static kind may not ping for hours and a transient error would otherwise show
	// an empty table indefinitely.
	retry := time.NewTimer(cacheWatchRetryInterval)
	retry.Stop()
	defer retry.Stop()
	armRetry := func() { retry.Reset(cacheWatchRetryInterval) }
	armed := false
	arm := func() {
		if !armed {
			debounce.Reset(debounceDur)
			armed = true
		}
	}
	disarm := func() {
		if armed {
			debounce.Stop()
			armed = false
		}
	}

	bind := func(next *store.ClusterDB) bool {
		disarm() // a fresh read happens below; drop any re-read pending for the old handle
		if cancelSub != nil {
			cancelSub()
			cancelSub = nil
			pings = nil
		}
		db = next
		if db == nil {
			return onClosed()
		}
		pings, cancelSub = subscribe(db)
		keep, again := onFire(db)
		if again {
			armRetry()
		}
		return keep
	}

	for {
		select {
		case <-ctx.Done():
			return
		case h, ok := <-handles:
			if !ok {
				return // store shutting down
			}
			if !bind(h) {
				return
			}
		case _, ok := <-pings:
			if !ok {
				// The bound db closed under us (e.g. Clear-cache): release the stale
				// sub and wait for WatchDB's new handle. Cancel, don't just drop —
				// a composite subscribe (catalogSubscribe) needs its goroutine stopped.
				disarm()
				if cancelSub != nil {
					cancelSub()
				}
				pings, cancelSub, db = nil, nil, nil
				continue
			}
			arm() // coalesce; the debounce fire runs the actual re-read
		case <-debounce.C:
			armed = false
			keep, again := onFire(db)
			if again {
				armRetry()
			}
			if !keep {
				return
			}
		case <-retry.C:
			// A read failed; drive the re-read from here until it succeeds.
			if db == nil {
				continue
			}
			keep, again := onFire(db)
			if again {
				armRetry()
			}
			if !keep {
				return
			}
		}
	}
}

// send delivers one value on out, honoring ctx cancellation. Returns false if ctx
// ended before the send completed.
func send[T any](ctx context.Context, out chan<- T, v T) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
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
	c := s.buildCluster(updated)
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

// RetryConnection implements ClusterService: fire-and-forget dispatch onto the
// retry bus after an existence check — no spec write, and backoff-neutral (a
// failed manual probe leaves beehive's reconnect ladder untouched); see
// docs/adr/2026-08-09-connection-probing.md.
func (s *Service) RetryConnection(ctx context.Context, id ClusterID) error {
	if _, err := s.clusterByID(ctx, id); err != nil {
		return err
	}
	s.coreCtrl.Reprobe(id)
	return nil
}

// clearCacheTimeout bounds ClearCache's detached drain → delete → restart
// sequence — generous, there to stop a wedged step, not to pace anything.
const clearCacheTimeout = 2 * time.Minute

// ClearCache implements ClusterService: validate the cluster exists, delete the
// on-disk cache, restart its syncs so it rebuilds.

func (s *Service) ClearCache(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	// No ClusterCache object yet → no files to delete; skip to the (no-op) restart.
	ref, found, err := s.cacheRef(ctx, id)
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

		deleteFiles := func() error { return s.cacheManager.DeleteCacheFiles(clearCtx, ref) }
		if s.gvrSyncCtrl == nil {
			if derr := deleteFiles(); derr != nil {
				return nil, derr
			}
		} else if derr := s.gvrSyncCtrl.RestartCacheWorkers(clearCtx, ref, deleteFiles); derr != nil {
			return nil, derr
		}
	}
	c := s.buildCluster(obj)
	return &c, nil
}

// Delete implements ClusterService. Beehive GC cascades to the ClusterCache; a
// still-present kube-context is re-created by the importer under the same
// "kubeconfig/{context}" name.
func (s *Service) Delete(ctx context.Context, id ClusterID) error {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	return s.coreClient.Delete(ctx, obj.ID)
}

// watchListChan folds a beehive kind watch (snapshot + change stream) into one
// Kubernetes-style delta stream. A deletion-pending object is collapsed to Deleted
// (List/Get hide tombstones, so the watch removes it at once; the trailing hard
// Deleted repeats idempotently). beehive's terminal Failed change ends the stream
// after a log line. fn's obj is nil only on a Deleted whose final state could not
// be decoded; the removal is still reported by id. Out closes on exit.
func watchListChan[Spec, Status, Out any](
	ctx context.Context,
	kind string,
	snap beehive.ObjectListSnapshot[Spec, Status],
	src <-chan beehive.ObjectChange[Spec, Status],
	fn func(ChangeType, beehive.ObjectID, *beehive.Object[Spec, Status]) Out,
) <-chan Out {
	out := make(chan Out, 1)
	go func() {
		defer close(out)
		// beehive.ChangeType and ChangeType share string values by construction.
		domainType := func(t beehive.ChangeType, obj *beehive.Object[Spec, Status]) ChangeType {
			if obj != nil && obj.DeletionRequestedAt != nil {
				return ChangeDeleted
			}
			return ChangeType(t)
		}
		for _, obj := range snap.Objects {
			if !send(ctx, out, fn(domainType(beehive.Added, obj), obj.ID, obj)) {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-src:
				if !ok {
					return
				}
				if ev.Type == beehive.Failed {
					if ctx.Err() == nil {
						slog.Warn("clusterservice: object watch ended", "kind", kind, "err", ev.Err)
					}
					return
				}
				if !send(ctx, out, fn(domainType(ev.Type, ev.Object), ev.ID, ev.Object)) {
					return
				}
			}
		}
	}()
	return out
}

// Watch implements ClusterService: beehive's Cluster-kind WatchList as a delta
// stream (conflated per object, so a slow client converges). Per-probe chatter,
// the countdown, and cache sync status deliberately stream elsewhere
// (clusterEventsWatch / clusterScheduleWatch / WatchCaches), so a settled
// disconnected cluster produces no churn here.
func (s *Service) Watch(ctx context.Context) (<-chan ClusterChange, error) {
	snap, src, err := s.coreClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "Cluster", snap, src,
		func(t ChangeType, id beehive.ObjectID, obj *beehive.Object[ClusterSpec, ClusterStatus]) ClusterChange {
			if obj == nil {
				return ClusterChange{Type: t, Cluster: &Cluster{ID: ClusterID(id)}}
			}
			c := s.buildCluster(obj)
			return ClusterChange{Type: t, Cluster: &c}
		}), nil
}

// WatchCaches implements ClusterService — the ClusterCache-kind counterpart of
// Watch, parent ClusterID resolved from the eager-loaded owner edge; the caller
// joins by ClusterID.
func (s *Service) WatchCaches(ctx context.Context) (<-chan ClusterCacheChange, error) {
	snap, src, err := s.cacheClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "ClusterCache", snap, src,
		func(t ChangeType, id beehive.ObjectID, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) ClusterCacheChange {
			if obj == nil {
				return ClusterCacheChange{Type: t, Cache: &ClusterCache{ID: ClusterCacheID(id)}}
			}
			cc := buildClusterCache(obj)
			return ClusterCacheChange{Type: t, Cache: &cc}
		}), nil
}

// WatchGVRDiscoveries implements ClusterService — the discovery counterpart of
// WatchCaches, parent CacheID resolved from the owner edge.
func (s *Service) WatchGVRDiscoveries(ctx context.Context) (<-chan ClusterCacheGVRDiscoveryChange, error) {
	snap, src, err := s.gvrDiscoveryClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "ClusterCacheGVRDiscovery", snap, src,
		func(t ChangeType, id beehive.ObjectID, obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]) ClusterCacheGVRDiscoveryChange {
			if obj == nil {
				return ClusterCacheGVRDiscoveryChange{Type: t, Discovery: &ClusterCacheGVRDiscovery{ID: ClusterCacheGVRDiscoveryID(id)}}
			}
			d := buildGVRDiscovery(obj)
			return ClusterCacheGVRDiscoveryChange{Type: t, Discovery: &d}
		}), nil
}

// gvrSyncAnchorFilter keeps only the sync records owned by one cache's discovery
// anchor (resolve returns it; 0 = no anchor yet; onErr fires on failed reads).
// The underlying watch is fleet-wide, so it memoizes the anchors it has RULED OUT —
// otherwise each frame costs a point query while our anchor is unresolved. The memo
// is licensed because a verdict never flips: anchor ids are AUTOINCREMENT and never
// reused, so a not-ours anchor cannot become ours.
func gvrSyncAnchorFilter(
	resolve func() (beehive.ObjectID, error),
	onErr func(error),
) func(ClusterCacheGVRSyncChange) []ClusterCacheGVRSyncChange {
	notOurs := map[beehive.ObjectID]bool{}
	// Frames received while the anchor could not be read are HELD, not dropped:
	// beehive re-emits an object only when it changes, so a frame lost in a
	// transient read error would leave that kind invisible for the subscription's
	// life. Released once a read succeeds.
	var undecided []ClusterCacheGVRSyncChange

	return func(c ClusterCacheGVRSyncChange) []ClusterCacheGVRSyncChange {
		// A hard Deleted carries no owner edge; forward on id alone (removal of an
		// id the client never added is a no-op).
		if c.Sync.DiscoveryID == 0 {
			return []ClusterCacheGVRSyncChange{c}
		}
		theirs := beehive.ObjectID(c.Sync.DiscoveryID)
		// Take the read whenever frames are held: held frames need an anchor to be
		// judged against, and post-recovery traffic is mostly ruled-out ids, so
		// short-circuiting on the memo alone would hold them forever.
		if notOurs[theirs] && len(undecided) == 0 {
			return nil
		}
		anchor, err := resolve()
		if err != nil {
			onErr(err)
			// A failed read must not rule the id out, nor lose the frame — unless
			// it's already known not-ours.
			if !notOurs[theirs] && len(undecided) < maxUndecidedSyncFrames {
				undecided = append(undecided, c)
			}
			return nil
		}

		// Judge everything held, oldest first — the consumer upserts by id, so a
		// superseded frame ahead of its successor is harmless.
		var out []ClusterCacheGVRSyncChange
		for _, held := range undecided {
			if beehive.ObjectID(held.Sync.DiscoveryID) == anchor {
				out = append(out, held)
			}
		}
		undecided = nil

		if anchor == theirs {
			return append(out, c)
		}
		notOurs[theirs] = true
		return out
	}
}

// maxUndecidedSyncFrames bounds held frames: covers a cold start's burst, caps the
// memory a permanently broken read can cost.
const maxUndecidedSyncFrames = 4096

func (s *Service) WatchGVRSyncs(ctx context.Context, cacheID ClusterCacheID) (<-chan ClusterCacheGVRSyncChange, error) {
	// The anchor is resolved lazily and re-resolved as the stream runs — a
	// subscribe-time miss (a just-created cache) must not leave the stream
	// permanently empty; while unresolved, dropping frames is correct (an
	// anchorless cache owns no sync records).
	anchorName := ClusterCacheGVRDiscoveryName(beehive.ObjectID(cacheID))
	var (
		discoveryID beehive.ObjectID
		loggedErr   bool
	)
	// resolveAnchor separates "no anchor yet" (normal) from "the read failed"
	// (surfaced: fail at subscribe, log once mid-stream — a silently dropped frame
	// is never replayed). A resolved id is kept for the stream's life: an anchor
	// lives as long as its cache and ids are never reused, so there is no
	// "same cache, different anchor" to invalidate for.
	resolveAnchor := func() (beehive.ObjectID, error) {
		if discoveryID != 0 {
			return discoveryID, nil
		}
		anchor, err := s.gvrDiscoveryClient.GetByName(ctx, anchorName)
		switch {
		case err == nil:
			discoveryID = anchor.ID
			return discoveryID, nil
		case errors.Is(err, beehive.ErrNotFound):
			return 0, nil // not created yet; try again on the next frame
		default:
			return 0, err
		}
	}
	if _, err := resolveAnchor(); err != nil {
		return nil, err
	}

	snap, src, err := s.gvrSyncClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return filterChan(ctx, watchListChan(ctx, "ClusterCacheGVRSync", snap, src,
		func(t ChangeType, id beehive.ObjectID, obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]) ClusterCacheGVRSyncChange {
			if obj == nil {
				// A hard Deleted carries no object; forwarded — a stray removal is a
				// no-op for the client.
				return ClusterCacheGVRSyncChange{Type: t, Sync: &ClusterCacheGVRSync{ID: ClusterCacheGVRSyncID(id)}}
			}
			gs := buildGVRSync(obj)
			return ClusterCacheGVRSyncChange{Type: t, Sync: &gs}
		}), gvrSyncAnchorFilter(resolveAnchor, func(err error) {
		if loggedErr {
			return
		}
		loggedErr = true
		slog.Warn("clusterservice: resolve cache discovery anchor", "cache", cacheID, "err", err)
	})), nil
}

// cacheWatchRetryInterval paces re-reads after a failed one — needed because the
// write-ping that would otherwise drive recovery is resource-keyed, so a kind
// nobody writes to would never send another.
const cacheWatchRetryInterval = 2 * time.Second

// syncHealthTick paces the rollup's periodic recompute: the freshness stamps it
// folds live in controller memory and move with no frame at all. Emission stays
// change-gated, so a quiet fleet sends nothing.
const syncHealthTick = 10 * time.Second

// syncHealthSnapshot is every cache's current verdict, keyed by cache id — the
// fold's whole output as one latest value. Published snapshots are immutable
// (fresh map per publish; subscribers read concurrently).
type syncHealthSnapshot map[ClusterCacheID]ClusterCacheSyncHealth

// WatchCacheSyncHealth implements ClusterService — every cache's sync verdict as a
// latest-value stream. Subscribers share ONE fold (syncHealthReceiver); this
// adapter is the per-subscriber half, sending only what changed for THIS
// subscriber — which is what gives a late joiner every cache on its first read.
// See docs/adr/2026-08-09-status-propagation-gauges.md.
func (s *Service) WatchCacheSyncHealth(ctx context.Context) (<-chan ClusterCacheSyncHealth, error) {
	rx, err := s.syncHealthReceiver()
	if err != nil {
		return nil, err
	}
	out := make(chan ClusterCacheSyncHealth, 1)
	go func() {
		defer close(out)
		defer rx.Close()
		sent := syncHealthSnapshot{}
		for {
			snap, err := rx.RecvContext(ctx)
			if err != nil {
				return // ctx ended, or the hub closed at shutdown
			}
			// Forget caches that left the snapshot. No delete FRAME: this is a gauge;
			// the consumer drops a verdict when the cache leaves clusterCachesWatch,
			// which owns that lifecycle.
			for cacheID := range sent {
				if _, ok := snap[cacheID]; !ok {
					delete(sent, cacheID)
				}
			}
			for cacheID, health := range snap {
				if prev, ok := sent[cacheID]; ok && syncHealthEqual(prev, health) {
					continue
				}
				sent[cacheID] = health
				if !send(ctx, out, health) {
					return
				}
			}
		}
	}()
	return out, nil
}

// syncHealthReceiver returns a receiver on the shared fold, starting it on first
// use — lazy so an unwatched fleet isn't folded, and the fold outlives any one
// subscriber. Once started it runs until Close (no refcount: little gain, a
// teardown race).
func (s *Service) syncHealthReceiver() (*watch.Receiver[syncHealthSnapshot], error) {
	s.syncHealthMu.Lock()
	defer s.syncHealthMu.Unlock()
	if s.syncHealthClosed {
		return nil, errors.New("cluster: sync-health fold is shut down")
	}
	if s.syncHealth == nil {
		hub, stop, done, err := s.startSyncHealthFold()
		if err != nil {
			return nil, err // nothing cached: the next subscriber retries
		}
		s.syncHealth, s.syncHealthStop, s.syncHealthDone = hub, stop, done
	}
	return s.syncHealth.Receiver(), nil
}

// startSyncHealthFold opens the two watches the fold reads and runs it, publishing
// each recomputed snapshot to a latest-value hub. The hub carries the whole map,
// not deltas — the output is tiny (one per cache), a new subscriber's first read is
// "every cache, right now", and a slow subscriber coalesces to the newest (correct
// for a gauge, where a dropped delta would be lost state).
func (s *Service) startSyncHealthFold() (*watch.Hub[syncHealthSnapshot], context.CancelFunc, chan struct{}, error) {
	// Background, not a caller's: the fold outlives every subscriber. Cancelled by
	// stopSyncHealthFold, or by the fold itself on any other exit.
	ctx, cancel := context.WithCancel(context.Background())
	syncSnap, syncSrc, err := s.gvrSyncClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	discSnap, discSrc, err := s.gvrDiscoveryClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	hub := watch.New(syncHealthSnapshot{})
	done := make(chan struct{})
	go func() {
		// Declared first so it runs LAST: done means every defer below has run —
		// see stopSyncHealthFold.
		defer close(done)
		defer func() {
			if s.syncHealthFoldExit != nil {
				s.syncHealthFoldExit()
			}
		}()
		// Cancel our OWN context on any exit: a fold can end with nobody calling the
		// stop func (beehive terminating a watch, a source closing), and the two
		// fleet-wide WatchList leases are registered against this context alone —
		// forgetSyncHealthFold clears the cached stop func, so nothing else would
		// ever release them.
		defer cancel()
		defer hub.Close()
		// Forget this hub on the way out: a closed hub hands later subscribers
		// pre-closed receivers, so a self-terminated fold (e.g. ErrWatchTooOld
		// during a cold-sync write storm) would otherwise silently end sync status
		// until restart. Clearing lets the next subscriber start a fresh fold.
		defer s.forgetSyncHealthFold(hub)
		f := &syncHealthFold{
			syncs:         map[beehive.ObjectID]gvrSyncRec{},
			cacheOf:       map[ClusterCacheGVRDiscoveryID]ClusterCacheID{},
			byDiscovery:   map[ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}{},
			discoveriesOf: map[ClusterCacheID]map[ClusterCacheGVRDiscoveryID]struct{}{},
			published:     syncHealthSnapshot{},
			dirty:         map[ClusterCacheID]struct{}{},
			stats:         s.GVRSyncStatsSnapshot,
			out:           hub.Sender(),
		}
		for _, obj := range discSnap.Objects {
			f.putDiscovery(obj)
		}
		for _, obj := range syncSnap.Objects {
			f.putSync(obj)
		}
		// A cache with an anchor but no kinds still needs its "nothing observed" verdict.
		f.markAll()
		f.flush()

		tick := time.NewTicker(syncHealthTick)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case ch, ok := <-discSrc:
				if !ok {
					return
				}
				if endSyncHealthWatch(ctx, "ClusterCacheGVRDiscovery", ch.Type, ch.Err) {
					return
				}
				f.applyDiscovery(ch)
			case ch, ok := <-syncSrc:
				if !ok {
					return
				}
				if endSyncHealthWatch(ctx, "ClusterCacheGVRSync", ch.Type, ch.Err) {
					return
				}
				f.applySync(ch)
			case <-tick.C:
				// The only thing that refreshes freshness on a settled cache — the
				// stamps move with no frame behind them.
				f.markAll()
			}
			f.flush()
		}
	}()
	return hub, cancel, done, nil
}

// endSyncHealthWatch reports whether a change frame is beehive's terminal one.
// Check BEFORE folding: a Failed carries no object, which apply* would read as a
// deletion.
func endSyncHealthWatch(ctx context.Context, kind string, t beehive.ChangeType, err error) bool {
	if t != beehive.Failed {
		return false
	}
	if ctx.Err() == nil {
		slog.Warn("clusterservice: sync-health watch ended", "kind", kind, "err", err)
	}
	return true
}

// forgetSyncHealthFold drops the cached hub when its fold has ended. It clears
// only if the cache still points at THIS hub, and leaves the shutdown latch alone.
func (s *Service) forgetSyncHealthFold(hub *watch.Hub[syncHealthSnapshot]) {
	s.syncHealthMu.Lock()
	defer s.syncHealthMu.Unlock()
	if s.syncHealth != hub {
		return
	}
	s.syncHealth = nil
	s.syncHealthStop = nil
}

// stopSyncHealthFold cancels the fold and closes its hub, ending every subscriber.
func (s *Service) stopSyncHealthFold(ctx context.Context) {
	s.syncHealthMu.Lock()
	s.syncHealthClosed = true
	stop, done := s.syncHealthStop, s.syncHealthDone
	s.syncHealth, s.syncHealthStop, s.syncHealthDone = nil, nil, nil
	// Unlock BEFORE the wait: the fold's teardown takes this same lock.
	s.syncHealthMu.Unlock()
	if stop == nil {
		return
	}
	stop()
	// Join it: the WatchList leases come back in its defers, so returning early
	// would let beehive drain while the fold is mid-flush. Bounded by the caller's
	// drain deadline.
	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("cluster: sync-health fold did not unwind before the drain deadline")
	}
}

// gvrSyncRec is the slice of one per-kind record the fold needs.
type gvrSyncRec struct {
	discoveryID ClusterCacheGVRDiscoveryID
	apiVersion  string
	resource    string
	conditions  []Condition
}

// kindRef is this record's kind identity, as the verdict reports it.
func (r gvrSyncRec) kindRef() SyncedKindRef {
	return SyncedKindRef{APIVersion: r.apiVersion, Resource: r.resource}
}

// compareKindRefs orders kind refs by the plural first, since that is what a UI shows;
// the api group only breaks a tie between two kinds sharing one.
func compareKindRefs(a, b SyncedKindRef) int {
	if c := cmp.Compare(a.Resource, b.Resource); c != 0 {
		return c
	}
	return cmp.Compare(a.APIVersion, b.APIVersion)
}

// syncHealthFold accumulates the two watches into a per-cache verdict. Single-goroutine;
// no locking.
type syncHealthFold struct {
	syncs   map[beehive.ObjectID]gvrSyncRec
	cacheOf map[ClusterCacheGVRDiscoveryID]ClusterCacheID
	// byDiscovery / discoveriesOf are the reverse indexes of cacheOf, so a flush
	// walks only the dirty caches' records — without them the dirty set scopes
	// which verdicts recompute but not how much is read (the quadratic part).
	byDiscovery   map[ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}
	discoveriesOf map[ClusterCacheID]map[ClusterCacheGVRDiscoveryID]struct{}
	// published is the last value sent per cache, so an unchanged recompute sends nothing.
	published map[ClusterCacheID]ClusterCacheSyncHealth
	// dirty scopes recompute to caches that may have moved — at cold start each of a
	// cache's hundred-plus kinds delivers its own frame.
	dirty map[ClusterCacheID]struct{}
	// stats reads every kind's freshness in one lock acquisition — see StatsSnapshot.
	stats func() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats
	// out publishes each recomputed snapshot; latest-value, so a burst collapses.
	out *watch.Sender[syncHealthSnapshot]
}

// mark flags one cache for recompute; markAll flags every cache the fold knows about
// (used by the tick, which re-reads stamps nothing else announces).
func (f *syncHealthFold) mark(cacheID ClusterCacheID) {
	if cacheID != 0 {
		f.dirty[cacheID] = struct{}{}
	}
}

func (f *syncHealthFold) markAll() {
	for _, cacheID := range f.cacheOf {
		f.mark(cacheID)
	}
	for cacheID := range f.published {
		f.mark(cacheID)
	}
}

func (f *syncHealthFold) putDiscovery(obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]) {
	id := ClusterCacheGVRDiscoveryID(obj.ID)
	cacheID := ClusterCacheID(ownerObjectID(obj))
	// An anchor that moved caches must leave the one it came from, or its records
	// would count toward both.
	if prev, ok := f.cacheOf[id]; ok && prev != cacheID {
		f.unlinkDiscovery(id, prev)
	}
	f.cacheOf[id] = cacheID
	if f.discoveriesOf[cacheID] == nil {
		f.discoveriesOf[cacheID] = map[ClusterCacheGVRDiscoveryID]struct{}{}
	}
	f.discoveriesOf[cacheID][id] = struct{}{}
	f.mark(cacheID)
}

// unlinkDiscovery drops one anchor from its cache's index, and the cache's entry with it
// once empty, so neither map outlives the objects it describes.
func (f *syncHealthFold) unlinkDiscovery(id ClusterCacheGVRDiscoveryID, cacheID ClusterCacheID) {
	ids := f.discoveriesOf[cacheID]
	delete(ids, id)
	if len(ids) == 0 {
		delete(f.discoveriesOf, cacheID)
	}
	f.mark(cacheID)
}

func (f *syncHealthFold) applyDiscovery(ch beehive.ObjectChange[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]) {
	// Deletion-pending counts as gone (same rule as watchListChan): a child on its
	// way out must not keep counting toward its cache's verdict.
	if ch.Object == nil || ch.Type == beehive.Deleted || ch.Object.DeletionRequestedAt != nil {
		id := ClusterCacheGVRDiscoveryID(ch.ID)
		f.unlinkDiscovery(id, f.cacheOf[id])
		delete(f.cacheOf, id)
		return
	}
	f.putDiscovery(ch.Object)
}

func (f *syncHealthFold) putSync(obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]) {
	rec := gvrSyncRec{
		discoveryID: ownerObjectID(obj),
		apiVersion:  obj.Spec.APIVersion,
		resource:    obj.Spec.Resource,
		conditions:  obj.Conditions,
	}
	// Mark both the old and the new cache: a record that moved anchors leaves the one it
	// came from with a stale count.
	if prev, ok := f.syncs[obj.ID]; ok && prev.discoveryID != rec.discoveryID {
		f.unlinkSync(obj.ID, prev.discoveryID)
	}
	f.syncs[obj.ID] = rec
	if f.byDiscovery[rec.discoveryID] == nil {
		f.byDiscovery[rec.discoveryID] = map[beehive.ObjectID]struct{}{}
	}
	f.byDiscovery[rec.discoveryID][obj.ID] = struct{}{}
	f.mark(f.cacheOf[rec.discoveryID])
}

// unlinkSync drops one per-kind record from its anchor's index, marking the cache it left.
func (f *syncHealthFold) unlinkSync(id beehive.ObjectID, discoveryID ClusterCacheGVRDiscoveryID) {
	ids := f.byDiscovery[discoveryID]
	delete(ids, id)
	if len(ids) == 0 {
		delete(f.byDiscovery, discoveryID)
	}
	f.mark(f.cacheOf[discoveryID])
}

func (f *syncHealthFold) applySync(ch beehive.ObjectChange[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]) {
	// Deletion-pending counts as gone — see applyDiscovery.
	if ch.Object == nil || ch.Type == beehive.Deleted || ch.Object.DeletionRequestedAt != nil {
		if prev, ok := f.syncs[ch.ID]; ok {
			f.unlinkSync(ch.ID, prev.discoveryID)
		}
		delete(f.syncs, ch.ID)
		return
	}
	f.putSync(ch.Object)
}

// flush recomputes the caches marked dirty and republishes when any of them moved.
func (f *syncHealthFold) flush() {
	if len(f.dirty) == 0 {
		return
	}
	// One lock acquisition for the whole flush — see StatsSnapshot.
	stats := f.stats()

	changed := false
	for cacheID := range f.dirty {
		anchors := f.discoveriesOf[cacheID]
		// No anchor left ⇒ the cache is gone (the controller ensures the anchor for
		// a cache's whole life), so its verdict is DROPPED, not recomputed —
		// recomputing would republish a permanent "no kinds yet" Unknown forever and
		// grow every snapshot without bound.
		if len(anchors) == 0 {
			if _, ok := f.published[cacheID]; ok {
				delete(f.published, cacheID)
				changed = true
			}
			continue
		}

		acc := &syncHealthAcc{}
		for discoveryID := range anchors {
			for id := range f.byDiscovery[discoveryID] {
				acc.add(f.syncs[id], stats[ClusterCacheGVRSyncID(id)])
			}
		}
		health := acc.verdict(cacheID)
		if prev, ok := f.published[cacheID]; ok && syncHealthEqual(prev, health) {
			continue
		}
		f.published[cacheID] = health
		changed = true
	}
	clear(f.dirty)

	if !changed {
		return
	}
	// Fresh map: the published one is read concurrently.
	next := make(syncHealthSnapshot, len(f.published))
	maps.Copy(next, f.published)
	_ = f.out.Send(next) // only fails once the hub is closed, which ends the fold anyway
}

// syncHealthEqual compares two rollups, dereferencing the stamps (pointers to equal times
// are still equal readings).
func syncHealthEqual(a, b ClusterCacheSyncHealth) bool {
	return a.Status == b.Status && a.Reason == b.Reason &&
		slices.Equal(a.UnhealthyKindRefs, b.UnhealthyKindRefs) &&
		a.TotalKinds == b.TotalKinds && a.UnhealthyKinds == b.UnhealthyKinds &&
		timePtrEqual(a.LastUpdateAt, b.LastUpdateAt) && timePtrEqual(a.LastLiveAt, b.LastLiveAt)
}

// GVRSyncStatsSnapshot implements ClusterService — every synced kind's stamps under one
// lock, for a caller folding a whole cache's worth at once.
func (s *Service) GVRSyncStatsSnapshot() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats {
	if s.gvrSyncCtrl == nil {
		return nil
	}
	return s.gvrSyncCtrl.StatsSnapshot()
}

// GVRSyncStats implements ClusterService — one synced kind's freshness stamps, read
// straight from the controller whose worker reported them. A nil controller (a test
// service with no control plane) reads as "nothing reported".
func (s *Service) GVRSyncStats(_ context.Context, id ClusterCacheGVRSyncID) (*ClusterCacheGVRSyncStats, error) {
	if s.gvrSyncCtrl == nil {
		return nil, nil
	}
	st, ok := s.gvrSyncCtrl.Stats(beehive.ObjectID(id))
	if !ok {
		return nil, nil
	}
	return &st, nil
}

// buildGVRSync assembles a domain ClusterCacheGVRSync from a single beehive object, its
// owning discovery anchor read off the eager-loaded owner edge.
func buildGVRSync(obj *beehive.Object[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]) ClusterCacheGVRSync {
	return ClusterCacheGVRSync{
		ID:          ClusterCacheGVRSyncID(obj.ID),
		DiscoveryID: ownerObjectID(obj),
		Spec:        obj.Spec,
		Conditions:  obj.Conditions,
	}
}

// filterChan forwards what decide returns for each value of in — zero or MORE,
// because a filter may not be able to decide a frame yet, and a dropped watch
// frame is gone for good (beehive re-emits only on change); holding undecided
// frames keeps that from being permanent. Closes out when in closes or ctx ends.
func filterChan[T any](ctx context.Context, in <-chan T, decide func(T) []T) <-chan T {
	out := make(chan T, 1)
	go func() {
		defer close(out)
		for v := range in {
			// send, not a bare write: a consumer that stops draining (closed sync
			// dialog) would otherwise park this goroutine forever.
			for _, o := range decide(v) {
				if !send(ctx, out, o) {
					return
				}
			}
		}
	}()
	return out
}

// GVRDiscoveryStats implements ClusterService — the discovery record's live gauges, read
// straight from the controller that measured them. A nil controller (a test service with
// no control plane) reads as "nothing measured", the same as a record whose first pass
// hasn't run.
func (s *Service) GVRDiscoveryStats(_ context.Context, id ClusterCacheGVRDiscoveryID) (*ClusterCacheGVRDiscoveryStats, error) {
	if s.gvrDiscoveryCtrl == nil {
		return nil, nil
	}
	st, ok := s.gvrDiscoveryCtrl.Stats(beehive.ObjectID(id))
	if !ok {
		return nil, nil
	}
	return &st, nil
}

// GetConnection implements ClusterService.
func (s *Service) GetConnection(id ClusterID) *rest.Config {
	cfg, _ := s.connMgr.Get(id)
	return cfg
}

// buildCluster assembles a domain Cluster from one beehive object. Cache children
// are not joined (they stream standalone via WatchCaches), so no ctx-bound reads.
func (s *Service) buildCluster(obj *beehive.Object[ClusterSpec, ClusterStatus]) Cluster {
	c := Cluster{
		ID:         ClusterID(obj.ID),
		Generation: obj.Generation,
		CreatedAt:  obj.CreatedAt,
		Spec:       obj.Spec,
		// Already liveness-downgraded by the store: a previous process's
		// Connected=True arrives here as Unknown.
		Conditions: obj.Conditions,
	}
	if obj.DeletionRequestedAt != nil {
		t := *obj.DeletionRequestedAt
		c.DeletionRequestedAt = &t
	}
	c.Status = derefOrZero(obj.Status)
	return c
}

// buildClusterCache assembles a domain ClusterCache; parent ClusterID comes off
// the eager-loaded owner edge (see ownerObjectID).
func buildClusterCache(obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) ClusterCache {
	return ClusterCache{
		ID:         ClusterCacheID(obj.ID),
		ClusterID:  ClusterID(ownerObjectID(obj)),
		ServerUID:  obj.Spec.ServerUID,
		Conditions: obj.Conditions,
	}
}

// buildGVRDiscovery assembles a domain ClusterCacheGVRDiscovery; parent CacheID
// comes off the eager-loaded owner edge.
func buildGVRDiscovery(obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]) ClusterCacheGVRDiscovery {
	return ClusterCacheGVRDiscovery{
		ID:         ClusterCacheGVRDiscoveryID(obj.ID),
		CacheID:    ClusterCacheID(ownerObjectID(obj)),
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}
}

// eventClient is the event-log slice of a beehive kind client; every kind client
// satisfies it, so one reader serves every kind's events surface.
type eventClient interface {
	ListEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) ([]beehive.Event, error)
	WatchEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) (*beehive.EventStream, error)
}

// eventOpts builds the beehive event options: optional category filter + limit.
func eventOpts(category *string, limit int) []beehive.EventOption {
	opts := []beehive.EventOption{beehive.WithEventLimit(limit)}
	if category != nil {
		opts = append(opts, beehive.WithEventCategory(*category))
	}
	return opts
}

// toDomainEvent maps one beehive run to the wire shape.
func toDomainEvent(e beehive.Event) Event {
	return Event{
		ID:       ObjectID(e.ID),
		Category: e.Category,
		Type:     e.Type,
		Reason:   e.Reason,
		Message:  e.Message,
		Count:    e.Count,
		FirstAt:  e.FirstAt,
		LastAt:   e.LastAt,
	}
}

// events reads one object's event timeline (newest first; nil/<=0 limit uses
// defaultEventLimit). A read error yields nil, not an error — a partial status
// read beats none.
func (s *Service) events(ctx context.Context, c eventClient, id beehive.ObjectID, category *string, limit *int) ([]Event, error) {
	n := defaultEventLimit
	if limit != nil && *limit > 0 {
		n = *limit
	}
	evs, err := c.ListEvents(ctx, id, eventOpts(category, n)...)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("clusterservice: list events", "object", id, "category", category, "err", err)
		}
		return nil, nil
	}
	out := make([]Event, 0, len(evs))
	for _, e := range evs {
		out = append(out, toDomainEvent(e))
	}
	return out, nil
}

// scheduleClient is the schedule-gauge slice of a beehive kind client; tests fake it.
type scheduleClient interface {
	WatchSchedule(ctx context.Context, id beehive.ObjectID) (<-chan beehive.Schedule, error)
}

// toSchedule maps the beehive Schedule gauge to the domain view (zero time → nil).
func toSchedule(s beehive.Schedule) Schedule {
	if s.NextRequeueAt.IsZero() {
		return Schedule{}
	}
	at := s.NextRequeueAt
	return Schedule{NextRequeueAt: &at}
}

// mapChan streams src through fn until src closes or ctx ends (out closes on exit).
func mapChan[A, B any](ctx context.Context, src <-chan A, fn func(A) B) <-chan B {
	out := make(chan B, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
				if !ok {
					return
				}
				select {
				case out <- fn(v):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// scheduleWatch streams one object's reconcile-schedule gauge — the live source
// for the next-attempt countdown, since a scheduling change fires no WatchList.
func (s *Service) scheduleWatch(ctx context.Context, c scheduleClient, id beehive.ObjectID) (<-chan Schedule, error) {
	src, err := c.WatchSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, toSchedule), nil
}

// ClusterScheduleWatch implements ClusterService: the schedule gauge merged with
// the controller's in-flight probe signal — the scheduler owns NextRequeueAt, the
// core controller owns Probing; both are current-on-subscribe.
func (s *Service) ClusterScheduleWatch(ctx context.Context, id ClusterID) (<-chan Schedule, error) {
	schedSrc, err := s.scheduleWatch(ctx, s.coreClient, beehive.ObjectID(id))
	if err != nil {
		return nil, err
	}
	probeSrc := s.coreCtrl.WatchProbe(ctx, id)
	return mergeSchedule(ctx, schedSrc, probeSrc), nil
}

// mergeSchedule folds the schedule gauge and the probe signal into one Schedule
// stream, re-emitting the combined latest as either side moves. A closed
// sub-source is nil'd, not stream-ending; out closes when both close or ctx ends.
func mergeSchedule(ctx context.Context, schedSrc <-chan Schedule, probeSrc <-chan bool) <-chan Schedule {
	out := make(chan Schedule, 1)
	go func() {
		defer close(out)
		var cur Schedule
		emit := func() bool { return send(ctx, out, cur) }
		for schedSrc != nil || probeSrc != nil {
			select {
			case <-ctx.Done():
				return
			case sc, ok := <-schedSrc:
				if !ok {
					schedSrc = nil
					continue
				}
				cur.NextRequeueAt = sc.NextRequeueAt
				if !emit() {
					return
				}
			case p, ok := <-probeSrc:
				if !ok {
					probeSrc = nil
					continue
				}
				cur.Probing = p
				if !emit() {
					return
				}
			}
		}
	}()
	return out
}

// ClusterEvents implements ClusterService — the Cluster-kind entrypoint to the
// generic event reader.
func (s *Service) ClusterEvents(ctx context.Context, id ClusterID, category *string, limit *int) ([]Event, error) {
	return s.events(ctx, s.coreClient, beehive.ObjectID(id), category, limit)
}

// ClusterEventsWatch implements ClusterService — the Cluster-kind entrypoint to
// the generic event watch.
func (s *Service) ClusterEventsWatch(ctx context.Context, id ClusterID, category *string) (<-chan Event, error) {
	return s.watchEvents(ctx, s.coreClient, beehive.ObjectID(id), category)
}

// ClusterCacheEvents implements ClusterService — the ClusterCache-kind entrypoint
// to the generic event reader (over the cache client).
func (s *Service) ClusterCacheEvents(ctx context.Context, id ClusterCacheID, category *string, limit *int) ([]Event, error) {
	return s.events(ctx, s.cacheClient, beehive.ObjectID(id), category, limit)
}

// ClusterCacheEventsWatch implements ClusterService — the ClusterCache-kind
// entrypoint to the generic event watch (over the cache client).
func (s *Service) ClusterCacheEventsWatch(ctx context.Context, id ClusterCacheID, category *string) (<-chan Event, error) {
	return s.watchEvents(ctx, s.cacheClient, beehive.ObjectID(id), category)
}

// ClusterCacheGVRSyncEvents implements ClusterService — the sync-transition
// history lives on the per-kind child, so the caller keys on the child's id.
func (s *Service) ClusterCacheGVRSyncEvents(ctx context.Context, id ClusterCacheGVRSyncID, category *string, limit *int) ([]Event, error) {
	return s.events(ctx, s.gvrSyncClient, beehive.ObjectID(id), category, limit)
}

// ClusterCacheGVRSyncEventsWatch implements ClusterService — one synced kind's
// entrypoint to the generic event watch.
func (s *Service) ClusterCacheGVRSyncEventsWatch(ctx context.Context, id ClusterCacheGVRSyncID, category *string) (<-chan Event, error) {
	return s.watchEvents(ctx, s.gvrSyncClient, beehive.ObjectID(id), category)
}

// watchEvents streams one object's event log as bare runs: snapshot runs first,
// then growth. beehive conflates per run id, so the consumer upserts by Event.ID —
// no add/modify classification. The stream's terminal error is logged.
func (s *Service) watchEvents(ctx context.Context, c eventClient, id beehive.ObjectID, category *string) (<-chan Event, error) {
	stream, err := c.WatchEvents(ctx, id, eventOpts(category, defaultEventLimit)...)
	if err != nil {
		return nil, err
	}
	out := make(chan Event, 1)
	go func() {
		defer close(out)
		emit := func(e beehive.Event) bool { return send(ctx, out, toDomainEvent(e)) }
		for _, e := range stream.Runs {
			if !emit(e) {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-stream.Events:
				if !ok {
					if err := stream.Err(); err != nil && ctx.Err() == nil {
						slog.Warn("clusterservice: event watch ended", "object", id, "err", err)
					}
					return
				}
				if !emit(e) {
					return
				}
			}
		}
	}()
	return out, nil
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

// cacheRef resolves the on-disk locator for a cluster's active cache. found is
// false when there is no active cache (never probed, or the UID's cache is
// missing/torn-down) — "no cache", not an error.
func (s *Service) cacheRef(ctx context.Context, id ClusterID) (store.CacheRef, bool, error) {
	clusterObj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	activeUID := clusterActiveUID(clusterObj)
	if activeUID == "" {
		return store.CacheRef{}, false, nil
	}
	cacheObj, err := s.cacheClient.GetByName(ctx, ClusterCacheName(id, activeUID))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	return newCacheRef(beehive.ObjectID(id), cacheObj.ID), true, nil
}

// syncHealthAcc folds one cache's per-kind records into a single verdict.
type syncHealthAcc struct {
	total int
	// worstRank/worstReason track the most severe reason seen; offender names the
	// kinds behind it (the reason is carried along, not mapped back from the rank).
	worstRank   int
	worstReason string
	offender    []SyncedKindRef
	// paused names (not counts) the kinds observed Paused: a pause relays to a
	// cache's children one at a time, so a partly-paused cache is a state a UI sees.
	paused []SyncedKindRef
	// notWatching counts every kind not observed Watching — ranked AND paused; the
	// offender list can't answer this (it resets to one name when a worse rank appears).
	notWatching int
	// unconfirmed counts kinds nobody in THIS process has observed — neither healthy
	// nor broken, and the verdict must not round them to either.
	unconfirmed int

	lastUpdateAt *time.Time
	lastLiveAt   *time.Time
}

// syncReasonRank orders per-kind reasons by how much they dominate a cache's
// verdict: failure > stall > wait > catch-up. A pause is not in the ladder — it is
// the mildest reading, reached only when nothing ranked or unobserved is left.
func syncReasonRank(reason string) int {
	switch reason {
	case ReasonSyncFailed:
		return 4
	case ReasonStale:
		return 3
	case ReasonNoConnection:
		return 2
	case ReasonSyncing:
		return 1
	case ReasonWatching, ReasonPaused:
		// Not faults; never reached (add handles both first). Named so default
		// means "unrecognized".
		return 0
	default:
		// An unfamiliar spelling is DEGRADED, not healthy (per the schema); ranked
		// at the bottom so it registers without masking a known reason.
		return 1
	}
}

func (a *syncHealthAcc) add(rec gvrSyncRec, st ClusterCacheGVRSyncStats) {
	a.total++
	{
		// Newest write anywhere in the cache; oldest proof among the kinds that have one.
		if st.LastUpdateAt != nil && (a.lastUpdateAt == nil || st.LastUpdateAt.After(*a.lastUpdateAt)) {
			a.lastUpdateAt = st.LastUpdateAt
		}
		if st.LastLiveAt != nil && (a.lastLiveAt == nil || st.LastLiveAt.Before(*a.lastLiveAt)) {
			a.lastLiveAt = st.LastLiveAt
		}
	}

	cond := FindCondition(rec.conditions, ConditionSynced)
	// Only an OBSERVED Watching is health: an unconfirmed or absent condition
	// counts toward notWatching, or UnhealthyKinds would disagree with an Unknown
	// verdict right after a restart. See docs/adr/2026-08-09-liveness-conditions.md.
	if cond != nil && !cond.Unconfirmed && cond.Reason == ReasonWatching {
		return
	}
	a.notWatching++

	// Neither healthy nor broken — never assert a pre-restart verdict, nor health
	// on its absence. See verdict.
	if cond == nil || cond.Unconfirmed {
		a.unconfirmed++
		return
	}
	if cond.Reason == ReasonPaused {
		a.paused = append(a.paused, rec.kindRef())
		return
	}
	rank := syncReasonRank(cond.Reason)
	switch {
	case rank > a.worstRank:
		a.worstRank, a.worstReason, a.offender = rank, cond.Reason, []SyncedKindRef{rec.kindRef()}
	case rank == a.worstRank && rank > 0:
		a.offender = append(a.offender, rec.kindRef())
	}
}

// verdict renders the fold. It reads like a per-kind condition on purpose, so a consumer
// renders a cache exactly as it renders one kind.
func (a *syncHealthAcc) verdict(cacheID ClusterCacheID) ClusterCacheSyncHealth {
	h := ClusterCacheSyncHealth{
		CacheID:        cacheID,
		TotalKinds:     a.total,
		UnhealthyKinds: a.notWatching,
		LastUpdateAt:   a.lastUpdateAt,
		LastLiveAt:     a.lastLiveAt,
	}
	switch {
	case a.total == 0:
		// No kinds yet (discovery hasn't landed) — neither fault nor health.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case a.worstRank > 0:
		h.Status = ConditionFalse
		h.Reason = a.worstReason
		// Sorted so "the first offender" stays stable while kinds stream in.
		slices.SortFunc(a.offender, compareKindRefs)
		h.UnhealthyKindRefs = a.offender
	case a.unconfirmed > 0:
		// Some kind is still unobserved in this process; Healthy is a claim about
		// EVERY kind, so it can't be made yet. Ranked below a real failure — an
		// observed fault is worth surfacing while others report in.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case len(a.paused) > 0:
		// Partial pause counts: calling in-between frames Watching would publish a
		// healthy status beside a non-zero unhealthyKinds with no names behind it.
		h.Status, h.Reason = ConditionFalse, ReasonPaused
		slices.SortFunc(a.paused, compareKindRefs)
		h.UnhealthyKindRefs = a.paused
	default:
		h.Status, h.Reason = ConditionTrue, ReasonWatching
	}
	return h
}
