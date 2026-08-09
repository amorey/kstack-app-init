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

// ClusterService is the boundary between the frontend (GraphQL today, gRPC
// later) and the cluster backend. Every beehive detail — names, the
// Cluster → ClusterCache owner chain, the spec/status split, the delta-watch
// mapping — lives behind it, so callers deal only in the domain Cluster /
// ClusterCache types.
type ClusterService interface {
	// List returns every tracked cluster that is not deletion-pending. Cache sync
	// status is not joined in — it is read via the cache watch and joined by the
	// caller.
	List(ctx context.Context) ([]*Cluster, error)
	// Get returns one cluster by id, or (nil, nil) when it is unknown or
	// deletion-pending.
	Get(ctx context.Context, id ClusterID) (*Cluster, error)
	// Watch streams the cluster list as a Kubernetes-style delta watch: the current
	// set as Added changes on subscribe (the snapshot), then one Added/Modified/
	// Deleted change per cluster. A deletion-pending cluster is surfaced as Deleted.
	// The channel closes when ctx ends.
	Watch(ctx context.Context) (<-chan ClusterChange, error)
	// WatchCaches streams cache records as a Kubernetes-style delta watch parallel to
	// Watch — the current set as Added changes, then per-cache Added/Modified/Deleted.
	// Standalone (no parent join); the caller joins caches onto clusters by
	// ClusterID. The channel closes when ctx ends.
	WatchCaches(ctx context.Context) (<-chan ClusterCacheChange, error)
	// WatchGVRDiscoveries streams the caches' GVR-discovery children as a third
	// independent delta watch — one per cache, joined onto caches by CacheID the
	// same way, and one stream per child kind for the reason given on
	// ClusterCacheGVRDiscovery. Unscoped because there is exactly one per cache.
	WatchGVRDiscoveries(ctx context.Context) (<-chan ClusterCacheGVRDiscoveryChange, error)
	// WatchCacheSyncHealth streams every cache's sync verdict, folded from its per-kind
	// records — the whole-cache rollup an always-mounted consumer can carry, since the
	// per-kind stream is a hundred-plus records per cache.
	WatchCacheSyncHealth(ctx context.Context) (<-chan ClusterCacheSyncHealth, error)
	// ClusterCacheGVRSyncEvents returns one synced kind's beehive event timeline.
	ClusterCacheGVRSyncEvents(ctx context.Context, id ClusterCacheGVRSyncID, category *string, limit *int) ([]Event, error)
	// ClusterCacheGVRSyncEventsWatch streams one synced kind's event log as bare runs.
	ClusterCacheGVRSyncEventsWatch(ctx context.Context, id ClusterCacheGVRSyncID, category *string) (<-chan Event, error)
	// WatchGVRSyncs streams one cache's per-kind sync records as a delta stream. Scoped
	// to a cache — unlike the watches above — because there is one record per served
	// kind, so an unscoped stream would be a firehose for an always-mounted consumer.
	WatchGVRSyncs(ctx context.Context, cacheID ClusterCacheID) (<-chan ClusterCacheGVRSyncChange, error)
	// ClusterCacheStatsWatch streams one cache's contents as a live gauge — the current
	// measurement on subscribe, then a fresh one whenever it changes. A stream rather
	// than a field on ClusterCache because a settled cache's object never changes, so a
	// field there would freeze at whatever the cache held when the client subscribed.
	ClusterCacheStatsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterCacheStats, error)
	// GVRSyncStats returns one synced kind's freshness stamps, out of band from the
	// object watch for the reason on ClusterCacheGVRSyncStats.
	GVRSyncStats(ctx context.Context, id ClusterCacheGVRSyncID) (*ClusterCacheGVRSyncStats, error)
	// GVRSyncStatsSnapshot returns every synced kind's stamps in one read — what the
	// sync-health rollup folds, taken under a single lock.
	GVRSyncStatsSnapshot() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats
	// GVRDiscoveryStats returns one discovery record's live gauges — when its last pass
	// reached the API server and how many kinds it saw — read on request from the
	// controller's memory. nil when this process has run no pass for that record yet.
	// Out of band from the object watch on purpose: nothing in the object graph reacts to
	// these, so putting them in status would wake the record's dependents every pass to
	// propagate a number only a UI reads (see ClusterCacheGVRDiscoveryStatus).
	GVRDiscoveryStats(ctx context.Context, id ClusterCacheGVRDiscoveryID) (*ClusterCacheGVRDiscoveryStats, error)
	// CacheStats returns live on-disk statistics for one ClusterCache, located by
	// its parent ClusterID (the cache directory) and its own ClusterCacheID (the
	// cache file). A cluster can own several caches, so stats are per-cache, not
	// per-cluster.
	CacheStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*ClusterCacheStats, error)
	// ClusterDataKinds returns the kinds a cluster's API server advertises, read from one
	// ClusterCache's discovered catalog (by parent ClusterID + cache id, like
	// CacheStats). Empty when that cache's db isn't open (never synced / sync paused).
	// Read on demand (not streamed).
	ClusterDataKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) ([]ClusterDataKind, error)
	// ClusterDataKindsWatch streams one ClusterCache's kind catalog as a delta watch:
	// the current catalog as an Added burst on subscribe, then Added/Modified/Deleted
	// changes as the syncs write objects (so per-kind counts update live). Empty
	// (no frames) when that cache's db isn't open, mirroring ClusterDataKinds' posture.
	ClusterDataKindsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataKindChange, error)
	// ClusterDataEventsWatch streams one ClusterCache's cached Kubernetes Events as a
	// delta watch: the newest window of events as an Added burst on subscribe, then
	// Added/Modified/Deleted changes as the Event sync writes. Empty (no
	// frames) when that cache's db isn't open, mirroring ClusterDataKindsWatch's
	// posture. Wakes on the events-only store broker, so an event burst never drives
	// the kind-catalog re-read.
	ClusterDataEventsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataEventChange, error)
	// ClusterDataObjectsWatch streams one kind's cached objects from a ClusterCache as a
	// delta watch (by parent ClusterID + cache id, plus apiVersion + resource): the
	// current object set for the kind as an Added burst on subscribe, then
	// Added/Modified/Deleted per object keyed by UID. Empty (no frames) when that cache's
	// db isn't open or the kind hasn't synced, mirroring ClusterDataEventsWatch's posture.
	ClusterDataObjectsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (<-chan ClusterDataObjectChange, error)
	// ClusterEvents returns a cluster's beehive event timeline (newest run first),
	// optionally filtered to one category and bounded by limit. Decoupled from the
	// cluster/list watch — event chatter never re-emits the cluster.
	ClusterEvents(ctx context.Context, id ClusterID, category *string, limit *int) ([]Event, error)
	// ClusterEventsWatch streams a cluster's event log as bare runs — the matching
	// runs as a snapshot, then live runs, conflating per run id (the consumer
	// upserts by Event.ID). Independent of Watch. The channel closes when ctx ends.
	ClusterEventsWatch(ctx context.Context, id ClusterID, category *string) (<-chan Event, error)
	// ClusterCacheEvents returns a ClusterCache's beehive event timeline (newest run
	// first), the ClusterCache-kind counterpart of ClusterEvents — keyed by the
	// cache's own ClusterCacheID (e.g. the sync-event history, category "sync").
	ClusterCacheEvents(ctx context.Context, id ClusterCacheID, category *string, limit *int) ([]Event, error)
	// ClusterCacheEventsWatch streams a ClusterCache's event log as bare runs, the
	// ClusterCache-kind counterpart of ClusterEventsWatch. The channel closes when
	// ctx ends.
	ClusterCacheEventsWatch(ctx context.Context, id ClusterCacheID, category *string) (<-chan Event, error)
	// ClusterScheduleWatch streams a cluster's reconcile-schedule gauge (the next
	// requeue time), current-on-subscribe then on every (re)schedule. A scheduling
	// change fires no object WatchList, so this is the live source for the UI's
	// next-attempt countdown. The channel closes when ctx ends.
	ClusterScheduleWatch(ctx context.Context, id ClusterID) (<-chan Schedule, error)
	// SetEnabled enables or disables a cluster in the app (connection eligibility
	// + visibility in pickers) and returns the updated record.
	SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// RetryConnection forces an immediate out-of-band re-probe of the cluster's
	// connection. The outcome lands on the record's conditions and reaches
	// watchers through Watch.
	RetryConnection(ctx context.Context, id ClusterID) error
	// ClearCache deletes the on-disk cache and bounces its syncs; the
	// (returned) record stays.
	ClearCache(ctx context.Context, id ClusterID) (*Cluster, error)
	// Delete removes the cluster by deleting the Cluster object so beehive GC
	// cascades to ClusterCache.
	Delete(ctx context.Context, id ClusterID) error
	// GetConnection returns the live REST config for id, or nil if the cluster
	// is not currently connected.
	GetConnection(id ClusterID) *rest.Config
}

// coreController is the subset of *ClusterCoreController that Service drives: the
// background-worker lifecycle and the targeted out-of-band re-probe entry point.
// Holding it behind an interface lets the white-box service tests inject a fake
// (so the dispatch path has no production nil-guard).
type coreController interface {
	StartBackground()
	StopBackground()
	Reprobe(ClusterID)
	// WatchProbe streams whether a cluster's connection probe is in flight
	// (current-on-subscribe, then per transition); merged into ClusterScheduleWatch.
	WatchProbe(ctx context.Context, id ClusterID) <-chan bool
}

// controllerRuntime is the cluster subsystem's shared controller environment — beehive's
// Manager analogue. It bundles the singletons every controller draws from, so a
// controller constructor takes one *controllerRuntime plus only its own specifics (a config
// source, probe fakes, …) instead of threading the shared infra through each call,
// and adding a new shared dep is a one-field change rather than a churn of every
// constructor signature.
//
// It deliberately holds the beehive instance rather than a bag of every kind's typed
// client: beehive.NewClient is a trivial {bh, gk} wrapper, so a controller mints
// exactly the typed clients it needs from rt.bh — keeping the kinds a controller
// touches explicit in its own constructor, instead of a shared struct that hides them
// (and that every controller test would have to fully populate). connMgr / cacheManager
// / pokeSvc are true singletons shared broadly, so they live here directly. Any field
// may be nil in a test that doesn't exercise the paths using it.
type controllerRuntime struct {
	bh           *beehive.Beehive
	connMgr      *ConnectionManager
	cacheManager *store.Manager
	pokeSvc      *poke.Service
	// cachePolicies is the per-cache client budget (rate limiter + LIST semaphore), shared
	// by the sync and discovery controllers because they talk to the same cluster on the
	// same cache's behalf. Lazily created by whichever controller is built first, so a test
	// that constructs a bare runtime still gets one.
	cachePolicies *cacheClientPolicies
}

// policies returns the runtime's per-cache client budgets, creating the registry on first
// use so every controller built from one runtime shares it.
func (rt *controllerRuntime) policies() *cacheClientPolicies {
	if rt.cachePolicies == nil {
		rt.cachePolicies = newCacheClientPolicies()
	}
	return rt.cachePolicies
}

// Service is the concrete ClusterService and the whole cluster control plane: it
// owns the beehive store + instance, the kubeconfig watcher, the two beehive
// clients, the two controllers (registered with beehive in New), the kubeconfig
// importer, the connection manager, and the per-cluster cache manager.
type Service struct {
	bh      *beehive.Beehive
	bhStore beehive.Store

	watcher *k8shelpers.KubeConfigWatcher

	coreClient  beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	// gvrDiscoveryClient and gvrSyncClient read the cache's sync subtree for the watches
	// below. Read-only here: their specs belong to the controllers that own them.
	gvrDiscoveryClient beehive.Client[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]
	gvrSyncClient      beehive.Client[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus]
	cacheManager       *store.Manager
	connMgr            *ConnectionManager
	coreCtrl           coreController
	cacheCtrl          *ClusterCacheController
	// gvrDiscoveryCtrl is read for its live gauges (GVRDiscoveryStats) — the controller
	// owns them in memory, so the read goes through it rather than the store.
	gvrDiscoveryCtrl *ClusterCacheGVRDiscoveryController
	// gvrSyncCtrl is held for its worker drain at shutdown.
	gvrSyncCtrl *ClusterCacheGVRSyncController

	// syncHealth is the shared sync-verdict fold, started on first subscriber and running
	// until Close. Guarded by syncHealthMu, which covers the lazy start only.
	syncHealthMu   sync.Mutex
	syncHealth     *watch.Hub[syncHealthSnapshot]
	syncHealthStop context.CancelFunc
	// syncHealthDone closes when the fold goroutine has fully unwound, which is what makes
	// "the fold goes first" at shutdown true: its two fleet-wide WatchList leases are
	// released in its defers, so cancelling and moving on would let beehive begin draining
	// while the fold was still inside a flush.
	syncHealthDone chan struct{}
	// syncHealthFoldExit is a test seam: when set it runs as the fold's last act, just
	// before it signals that it has unwound. Nil in production.
	syncHealthFoldExit func()
	// syncHealthClosed latches at shutdown. Without it the lazy start would treat "no hub"
	// as "not started yet" and build a NEW fold for a subscriber that arrived between the
	// stop and beehive's — one anchored to a fresh background context whose canceller had
	// just been discarded, holding beehive watches while beehive is being torn down, with
	// nothing left that would ever stop it.
	syncHealthClosed bool

	importer *KubeconfigImporter
	pokeSvc  *poke.Service

	// dataKindsDebounce bounds how often ClusterDataKindsWatch re-reads and diffs the
	// kind catalog. A busy cluster pings the store on every object write; each re-read
	// runs Kinds' count-join over the object index, so a burst of pings is
	// coalesced into one re-read per interval (trailing edge) to keep the reader from
	// aggregating continuously.
	dataKindsDebounce time.Duration

	// dataEventsDebounce bounds how often ClusterDataEventsWatch re-reads and diffs the
	// events window. Events are high-volume, so a burst of event-write pings is coalesced
	// into one re-read per interval (trailing edge) rather than a re-read per event.
	dataEventsDebounce time.Duration

	// dataObjectsDebounce bounds how often ClusterDataObjectsWatch re-reads and diffs one
	// kind's cached objects. Object writes share one broker, so a burst (a relist, a
	// churny kind) is coalesced into one re-read per interval (trailing edge).
	dataObjectsDebounce time.Duration

	// cacheStatsDebounce bounds how often ClusterCacheStatsWatch re-rolls the cache's
	// whole-cache totals. It is the coarsest of the four: a summary line's freshness is
	// worth far less than a table's, and during a cold sync every one of a cache's
	// hundred workers is writing at once.
	cacheStatsDebounce time.Duration
}

// defaultDataKindsDebounce floors the kind-catalog re-read interval — small enough
// that the dashboard nav's counts still read as live, large enough to collapse a
// high-churn cluster's write pings into a bounded aggregation rate.
const defaultDataKindsDebounce = 250 * time.Millisecond

// defaultDataEventsDebounce floors the events-watch re-read interval. Events are the
// highest-volume stream, so this is a touch coarser than the kind-catalog debounce —
// still live for a table, but collapsing an event storm into a bounded re-read rate.
const defaultDataEventsDebounce = 500 * time.Millisecond

// defaultDataObjectsDebounce floors the objects-watch re-read interval. Object writes
// drive the same broker the kind catalog follows; a per-kind objects table wants a
// live-but-bounded re-read, so it matches the kind-catalog cadence.
const defaultDataObjectsDebounce = 250 * time.Millisecond

// defaultCacheStatsDebounce floors the cache-summary re-read interval. Deliberately the
// coarsest: it backs one "N objects across M kinds" line, and the whole point of the
// gauge is to stop being stale, not to be instant.
const defaultCacheStatsDebounce = time.Second

var _ ClusterService = (*Service)(nil)

// New builds the cluster control plane: the kubeconfig watcher (over kubeconfigPath),
// the beehive store + instance (at <dataDir>/beehive.db), the two beehive clients, the
// two registered controllers, the kubeconfig importer, and the per-cluster cache
// manager (rooted at <dataDir>/clusters/). The returned *Service is both the GraphQL
// boundary and the control plane, owning the whole watcher + beehive + importer + cache
// lifecycle via Start/Close. The watcher and beehive are cluster-only, so the service
// owns them outright; if a non-cluster consumer ever needs either, hoist it up to the
// composition root and inject it.
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
	// WithEventRetention bounds each object's connection event timeline to the
	// newest maxEventRuns runs per (object, category) timeline, GC-swept. The startup pass
	// is declared per kind at Register, not here — see the registrations below.
	// No kind runs a *periodic* full pass: once started, each controller re-arms
	// itself with Result.RequeueAfter, and the out-of-band triggers (kubeconfig
	// change, resync poke, retry bus) cover the rest.
	bh, err := beehive.New(bhStore, beehive.WithEventRetention(maxEventRuns, 0))
	if err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("init beehive: %w", err)
	}

	// The Service keeps its own clients for the GraphQL-facing reads/watches — one per
	// kind it serves; the controllers mint whatever clients they need from the runtime's
	// beehive instance.
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	gvrDiscoveryClient := beehive.NewClient[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus](bh, ClusterCacheGVRDiscoveryGroupKind)
	gvrSyncClient := beehive.NewClient[ClusterCacheGVRSyncSpec, ClusterCacheGVRSyncStatus](bh, ClusterCacheGVRSyncGroupKind)

	// The cache manager owns the per-cluster SQLite cache files under
	// <dataDir>/clusters/ — wholly a cluster concern, so the service owns it.
	cacheManager := store.NewManager(dataDir)

	connMgr := NewConnectionManager()

	// The shared controller environment (beehive's Manager analogue): every controller
	// draws its clients + shared singletons from here.
	rt := &controllerRuntime{bh: bh, connMgr: connMgr, cacheManager: cacheManager, pokeSvc: pokeSvc}

	coreCtrl := NewClusterCoreController(rt, watcher, nil, nil)
	cacheCtrl := NewClusterCacheController(rt)
	// The cache-sync controllers reconcile the subtree of each cache: GVR discovery creates
	// one ClusterCacheGVRSync per served kind — Events included, differing only in which
	// store their worker writes to — and cacheGVRSyncCtrl runs one worker per record. Each
	// carries a Spec.Enabled the cache controller pushes down, so pausing is that flag and
	// never a deletion; a worker is only ever started and stopped by its own controller.
	cacheGVRDiscoveryCtrl := NewClusterCacheGVRDiscoveryController(rt)
	cacheGVRSyncCtrl := NewClusterCacheGVRSyncController(rt)

	// Register returns each kind's status-write ControllerClient. The reconcile path gets it
	// as an argument, but the controllers also write status out-of-band (the connection
	// controller's poke re-probe), so inject it now — before bh.Start, since a startup
	// reconcile may write immediately. WithMaxRetryInterval caps the connection controller's
	// exponential reconnect backoff at connectionMaxBackoff.
	//
	// WithStartupFullPass is declared per kind, not globally, so it says which controllers
	// actually need it: Cluster and ClusterCache both own process-scoped state a restart
	// invalidates and the store never recorded — live connections + liveness sentinels, and
	// running sync workers — so beehive's owed pass (which sees only unconverged specs)
	// would leave a settled cluster unreconciled and therefore unconnected. GVR discovery
	// takes it for a related reason: its periodic re-discovery is a RequeueAfter, which is
	// in-memory, so without the startup pass a settled discovery object would never look at
	// the cluster again after a restart. ClusterCacheGVRSync owns a running worker per kind,
	// which is process-scoped state a restart invalidates, so it takes the pass for the same
	// reason the cache and cluster kinds do.
	//
	// WithConcurrency lets beehive run clusterProbeConcurrency Cluster reconciles at once.
	// A reconcile is mostly one cluster's network probe, and beehive's default of a single
	// worker would let one unreachable cluster's dial timeout delay every cluster behind it
	// in the startup pass. ClusterCoreController.Reconcile locks per cluster, so concurrent
	// reconciles of distinct clusters are safe.
	coreCC, errCluster := beehive.Register(bh, ClusterGroupKind, coreCtrl,
		beehive.WithMaxRetryInterval(connectionMaxBackoff),
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	_, errCache := beehive.Register(bh, ClusterCacheGroupKind, cacheCtrl,
		beehive.WithStartupFullPass(true),
	)
	// Discovery gets the same concurrency as the Cluster kind, and for the same reason: a
	// pass is mostly one cluster's discovery request, so a single worker would let one
	// unresponsive API server delay every other cache's discovery behind it.
	_, errDiscovery := beehive.Register(bh, ClusterCacheGVRDiscoveryGroupKind, cacheGVRDiscoveryCtrl,
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	// The per-kind syncs are the one place the concurrency is about volume rather than
	// latency: a cache has one of these per served kind — a hundred or more — and each
	// reconcile opens the cache and starts a worker, so a single beehive worker would walk
	// them one at a time on every startup pass.
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

	// Start the controllers' background work now the control plane is running. The core
	// controller's worker drives the targeted-retry bus (RetryConnection) and the poke bus;
	// the per-kind sync controller reacts to pokes by restarting its live workers, which is
	// where the watches that a resume invalidates actually live. Both write status, so they
	// share the same start/drain window.
	s.coreCtrl.StartBackground()
	s.gvrSyncCtrl.StartPoke()

	// Start the watcher (fsnotify loop) before the importer, which subscribes to
	// its snapshots current-on-subscribe.
	s.watcher.Start()
	s.importer.Start()

	stop := func(ctx context.Context) error {
		s.watcher.Close()
		s.importer.Stop()

		// Stop out-of-band re-probe / resync work before draining so none
		// of it races the teardown.
		s.coreCtrl.StopBackground()
		s.gvrSyncCtrl.StopPoke()

		// Then drain the reconcile loops, stop the sync workers they started, and only
		// then shut the cache those workers write into. The order is load-bearing: beehive
		// must drain first so no reconcile can start another worker behind us, and the
		// workers must stop before the cache manager closes their ClusterDB handles. One
		// controller owns every worker — Events included — so one drain covers them.
		//
		// The sync-health fold goes first: it only reads, so nothing depends on it, and
		// ending it before beehive stops means its watches close on our terms rather than
		// under it.
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

// CacheStats implements ClusterService. It stats one specific ClusterCache (by
// parent ClusterID + cache id), so the live-stats resolver can report the cache it
// was asked about — active or a migrated-away one — without re-resolving "the"
// cache for a cluster.
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

// ClusterCacheStatsWatch implements ClusterService — one cache's contents as a live
// gauge: the current measurement on subscribe, then a fresh one whenever it changes.
//
// This exists because the measurement is NOT on the ClusterCache object. Serving it as a
// resolver field there made its freshness depend on that object changing, and a settled
// cache never changes — so a consumer rendered whatever the cache held the moment it
// subscribed, which during a cold sync is a tiny fraction of what lands seconds later.
// A gauge nothing propagates needs a stream of its own.
func (s *Service) ClusterCacheStatsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterCacheStats, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheGaugeWatch(ctx, s.cacheManager, ref.CacheID, s.cacheStatsDebounce,
		// The object-write broker, keyless: every kind's writes move this total, and the
		// catalog registrations that announce a new kind ping it too.
		func(db *store.ClusterDB) (<-chan struct{}, func()) { return db.ObjectsSubscribe() },
		func(ctx context.Context, db *store.ClusterDB) (ClusterCacheStats, error) {
			// Re-stat the file each read: it grows with the rows, and nothing else would
			// carry that change.
			bytes, _ := s.cacheManager.CacheBytes(ref)
			return readCacheStats(ctx, db, bytes)
		},
		// A closed cache reports what is on DISK, not zeroes. Whether a db handle is open
		// says only whether something is syncing right now, and plenty of caches have a
		// file with nobody holding it: a cluster whose kube-context left the kubeconfig
		// (never eligible, so no worker ever opens it), a paused one, one whose workers are
		// mid-restart. Reporting those as nonexistent is what disables Clear cache on the
		// very rows the Orphaned group exists to let the user reclaim. The counts stay zero
		// — they can only be read through an open handle — which is why exists is a
		// separate field from them.
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
		// Events carry a real kind_counts value (triggers on the events table maintain the
		// ('v1','Event') row so the dashboard nav badge is accurate), but they live in
		// their own table and are not objects — exclude them from the whole-cache totals.
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

// ClusterDataKinds implements ClusterService. It reads the kind catalog from one
// specific ClusterCache (by parent ClusterID + cache id), mirroring CacheStats — so
// the caller names the exact cache whose catalog it wants (its active cache), and a
// cache/identity swap under the same cluster reads the new cache's catalog rather
// than "the" cluster's. Returns nil (empty) when that cache's db isn't open (never
// synced / sync paused), matching CacheStats' degrade-to-empty posture.
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

// toDataKinds maps a catalog read onto the domain records, preserving the reader's order
// ((api_version, kind)) — which the delta watch's Added burst relies on being stable. It is
// shared by the query and its live counterpart: those two must never disagree about the
// projection, so they must not each spell it out.
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

// dataKindKey is a kind's identity within a catalog: APIVersion + Resource is unique
// per cache (the group/version plus the plural resource name), so it keys the diff.
func dataKindKey(k ClusterDataKind) string {
	return k.APIVersion + "/" + k.Resource
}

// ClusterDataKindsWatch implements ClusterService. It streams one ClusterCache's kind
// catalog as a delta watch: the current catalog as an Added burst on subscribe, then
// one Added/Modified/Deleted change per kind as the syncs write objects and
// pings the store (Count is a live LEFT JOIN, so an object write that changes a count
// re-emits its kind as Modified). It follows the object-write broker (ObjectsSubscribe)
// and re-reads Kinds on each debounced ping, diffing against the last snapshot;
// cacheDeltaWatch owns the whole cache-lifecycle + coalescing loop.
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

// ClusterDataEventsWatch implements ClusterService. It streams one ClusterCache's cached
// Kubernetes Events as a delta watch: the newest window of events (Events' default limit)
// as an Added burst on subscribe, then Added/Modified/Deleted changes as the Event sync
// writes events. It follows the events-only broker (EventsSubscribe) and re-reads Events
// on each debounced ping, keyed by event UID — so an event burst never drives the
// kind-catalog re-read. Because the read is a bounded window, an event aging out of the
// window as newer ones arrive surfaces as Deleted even though its row may still exist;
// that is acceptable for a "latest events" table. cacheDeltaWatch owns the cache-lifecycle
// + coalescing loop, so this matches ClusterDataKindsWatch's empty/rebind posture exactly.
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

// ClusterDataObjectsWatch implements ClusterService. It streams one kind's cached objects
// from a ClusterCache as a delta watch: the current object set for the kind as an Added
// burst on subscribe, then Added/Modified/Deleted changes as the kind's sync writes
// objects. It mirrors ClusterDataEventsWatch's shape but follows the object-write broker
// (ObjectsSubscribe) — the same broker the kind catalog watches — re-reading a new
// store.Objects(apiVersion, resource) snapshot on each debounced ping, keyed by UID and
// projecting each row to a ClusterDataObject (universal identity + the native body, so an
// in-place edit surfaces as Modified). cacheDeltaWatch owns the whole cache-lifecycle + coalescing
// loop, so this matches ClusterDataKindsWatch's empty/rebind posture exactly (no open
// cache → no frames until it opens or ctx ends).
func (s *Service) ClusterDataObjectsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (<-chan ClusterDataObjectChange, error) {
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, s.cacheManager, ref.CacheID, s.dataObjectsDebounce,
		// Subscribe keyed to this watch's (apiVersion, resource) so an unrelated resource's
		// writes don't wake (and re-read) it. The broker routes object writes by the same
		// plural resource, so no resolution is needed and — crucially — the key is stable
		// across a CRD Kind remap: a CRD recreated with the same (apiVersion, resource) but a
		// new Kind keeps this key, so the subscription tracks the replacement driver's writes
		// (which notify by the same resource) rather than going stale against the dead Kind.
		func(db *store.ClusterDB) (<-chan struct{}, func()) {
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

// toDataObject maps a store ObjectRow onto the domain ClusterDataObject 1:1, decoding the
// stored unix-millis creationTimestamp (0 → zero time) consistently with the events read
// so two reads of the same row compare equal in the watch diff. RawJSON carries the
// decompressed native body verbatim; because it's part of the struct, an in-place edit
// (which rewrites the body) differs across reads and surfaces as Modified.
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

// toDataEvent maps a store EventRow onto the domain ClusterDataEvent 1:1, decoding the
// stored unix-millis timestamps (0 → zero time).
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

// millisToTime converts unix-millis to a time.Time, mapping 0 (no timestamp) to the
// zero Time. Built consistently so two reads of the same row compare equal in the watch
// diff (time.Time equality is stable for values with no monotonic reading).
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// catalogSubscribe wakes on a write to EITHER table, because the kind catalog spans both:
// every synced kind's count comes from the objects triggers, and the Event kind's from the
// events triggers. Following only the object-write broker froze the Events badge on a
// cluster that is event-busy but object-quiet — which is exactly the cluster whose event
// count a user is watching.
//
// The two brokers stay separate, and this is not a hole in that. Their point is that an
// event burst must not drive the EXPENSIVE re-reads — the per-kind objects watch, which
// decompresses a whole collection. This read is the cheap one: Kinds is a point join over
// the trigger-maintained aggregates, O(kinds) and never an object scan, and the caller
// debounces it besides.
func catalogSubscribe(db *store.ClusterDB) (<-chan struct{}, func()) {
	objects, stopObjects := db.ObjectsSubscribe()
	events, stopEvents := db.EventsSubscribe()

	out := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		// Closing out is how a caller learns the db went away through the ping path, the
		// same way a bare broker subscription tells it. Only when BOTH brokers have closed:
		// one closing alone still leaves the other's writes worth reporting.
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
			// Coalescing, like the brokers themselves: a buffered ping already says
			// "something changed", which is all this signal carries.
			select {
			case out <- struct{}{}:
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

// cacheDeltaWatch is the shared machinery behind ClusterDataKindsWatch, ClusterDataEventsWatch,
// and ClusterDataObjectsWatch. It follows one ClusterCache's on-disk db across its whole
// lifecycle via the store Manager's WatchDB (binding when the cache opens, rebinding on a
// Clear-cache delete+reopen), coalesces the db's write pings on a trailing-edge debounce,
// and on each fire re-reads a keyed snapshot and diffs it against the last one it emitted —
// sending Added for a new key, Modified for a changed value, Deleted for a vanished key
// (and Deleted for every held key when the cache closes, so a never-reopened cache doesn't
// retain stale rows). `subscribe` selects which of the db's brokers to follow (object
// writes vs event writes); `snapshot` reads the current rows as an **ordered** slice
// (the reader's order is the emit order, so the on-subscribe Added burst is stable, e.g.
// Kinds' (api_version, kind)); `keyOf` derives each value's identity (the diff and
// map key); `mkChange` adapts one (ChangeType, value) into the caller's domain change. The
// returned channel closes when ctx ends or the store shuts down. T must be comparable so a
// changed value is detected by ==; the diff sends the last-known value on a Deleted.
func cacheDeltaWatch[T comparable, C any](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan struct{}, func()),
	snapshot func(context.Context, *store.ClusterDB) ([]T, error),
	keyOf func(T) string,
	mkChange func(ChangeType, T) C,
) <-chan C {
	out := make(chan C, 1)
	prev := map[string]T{}

	// emit diffs db's freshly-read snapshot against prev, sending one change per
	// difference (Added/Modified/Deleted) and updating prev. Added/Modified are emitted
	// in the snapshot's slice order (stable, from the reader's ORDER BY); Deleted follows
	// in map order (only vanished keys, unordered).
	emit := func(db *store.ClusterDB) (bool, bool) {
		items, err := snapshot(ctx, db)
		if err != nil {
			if ctx.Err() != nil {
				return false, false
			}
			// Keep the stream and ask for a retry. Silently waiting for the next write ping
			// would strand the subscription on a kind that isn't being written to.
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

	// emitEmpty reconciles prev against an empty snapshot — one Deleted per held value,
	// then clears prev. Called when the cache closes so a cache that never reopens doesn't
	// leave stale rows.
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

// cacheGaugeWatch streams one whole-cache measurement, re-read on the same cadence
// cacheDeltaWatch uses and emitted only when it CHANGES.
//
// It is the gauge counterpart of the delta watch: a value the UI reads but no controller
// acts on, so it belongs on its own stream rather than on an object's status — see the
// status-is-propagation rule. The dedupe is what makes that affordable: a busy cluster
// pings on every write, but a measurement that reads the same twice sends nothing.
//
// zero is what to emit when the cache closes, so a consumer isn't left rendering the
// contents of a cache that is gone.
func cacheGaugeWatch[T comparable](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan struct{}, func()),
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
					// Keep the stream and retry: this gauge dedupes, so a failed read that
					// waited for the next ping would freeze the value with no way to tell.
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

// cacheWatchLoop follows one ClusterCache's on-disk db across its whole lifecycle and
// drives a caller's re-read from it. It is the shared half of every per-cache stream:
// binding via the store Manager's WatchDB (so a subscriber that opens before the cache
// does binds when it opens, and a Clear-cache delete+reopen rebinds), subscribing to the
// db's write pings, and coalescing those pings on a trailing-edge debounce so a
// high-churn cluster can't keep the read running back to back.
//
// onFire runs once per bind and once per debounced burst; onClosed runs when the cache
// goes away. Either returning false ends the stream (the caller's send saw ctx end).
// What is read and what is emitted is entirely the caller's business — cacheDeltaWatch
// diffs a keyed snapshot, cacheGaugeWatch dedupes a single value.
func cacheWatchLoop(
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan struct{}, func()),
	onFire func(*store.ClusterDB) (keep bool, retry bool),
	onClosed func() bool,
) {
	handles, cancelHandles := mgr.WatchDB(cacheID)
	defer cancelHandles()

	// db/pings track the currently-bound handle and its write-ping stream; both are nil
	// while no cache is open.
	var (
		db        *store.ClusterDB
		pings     <-chan struct{}
		cancelSub func()
	)
	defer func() {
		if cancelSub != nil {
			cancelSub()
		}
	}()

	// `armed` tracks whether a re-read is pending; the timer starts disarmed. (Go's timer
	// guarantees no stale tick after Stop/Reset, so the channel never needs a manual drain.)
	debounce := time.NewTimer(debounceDur)
	debounce.Stop()
	defer debounce.Stop()
	// A failed read schedules its OWN retry rather than waiting for the next write ping.
	// Since the object-write broker is resource-keyed, a static kind (Namespaces, an idle
	// CRD) may not ping again for hours — so a transient read error would otherwise leave
	// the subscription showing an empty table indefinitely, which a client cannot tell
	// apart from a genuinely empty kind.
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
				// The bound db closed out from under us (e.g. a Clear-cache delete); release
				// the stale sub (and any pending re-read) and wait for WatchDB to deliver
				// the new handle. Cancelling rather than merely dropping the func: a
				// subscribe may be a composite (catalogSubscribe fans two brokers into one
				// channel through a goroutine), so the func is what releases the
				// registrations and stops that goroutine.
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
			// A read failed. Nothing else is coming for a kind nobody writes to, so drive
			// the re-read from here until it succeeds.
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

// RetryConnection implements ClusterService. It forces an immediate out-of-band
// re-probe via the core controller's retry bus (Reprobe) — no spec write. Running
// off-worker, the reprobe is backoff-neutral: a failed manual probe leaves beehive's
// reconnect ladder untouched, so manual clicks don't perturb the automatic cadence.
// (Routing through Client.Requeue would ride beehive's worker and participate in
// backoff.) Dispatch is fire-and-forget, after a read-only existence check that
// surfaces ErrNotFound for a just-deleted cluster. The outcome lands on the record's
// conditions and reaches watchers through Watch.
func (s *Service) RetryConnection(ctx context.Context, id ClusterID) error {
	if _, err := s.clusterByID(ctx, id); err != nil {
		return err
	}
	s.coreCtrl.Reprobe(id)
	return nil
}

// ClearCache implements ClusterService. It validates the cluster exists before
// touching disk, deletes the on-disk cache, then restarts the cache's syncs so
// it rebuilds.
// clearCacheTimeout bounds the detached drain → delete → restart sequence. Generous: it
// covers a whole cache's workers draining (each bounded by workerStopTimeout in turn), and
// it exists to stop a wedged step running forever, not to pace anything.
const clearCacheTimeout = 2 * time.Minute

func (s *Service) ClearCache(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	// Resolve the cache child to locate its on-disk files. If there is no
	// ClusterCache object yet (cluster never became sync-eligible), there are no
	// files to delete — skip straight to the restart (also a no-op).
	ref, found, err := s.cacheRef(ctx, id)
	if err != nil {
		return nil, err
	}
	if found {
		// Delete INSIDE the worker restart, not before it. DeleteCacheFiles closes the
		// ClusterDB every one of this cache's workers holds, so they must be drained first
		// (nothing may be mid-write when the file goes) and rebuilt afterwards on the
		// handle the Manager hands back — a new one, since the old was closed with the
		// file. Rebuilding them on the old handle would leave every worker failing every
		// database operation while still registered, and a registered worker is exactly
		// what stops a reconcile from replacing it: the cleared cache would never refill.
		//
		// Nothing else would rebuild them either — a reconcile leaves a running worker
		// alone while its connection and kind are unchanged, so neither the 30s liveness
		// recheck nor the 5-minute discovery pass revives one. Each comes back and
		// cold-syncs its kind into the empty file, its resume cookie having gone with the
		// old one.
		// Detached from the request context, and deliberately. Once the drain has begun
		// this sequence must run to its end: a client that abandons the mutation midway —
		// a closed window, a navigation — would otherwise leave the cache drained, its
		// files deleted, and not one worker rebuilt, with nothing else that ever would.
		// Bounded, so a wedged step can't hold the goroutine for the process's life.
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

// Delete implements ClusterService. It deletes the Cluster object; beehive GC
// cascades to its ClusterCache. If the kube-context still exists, the importer
// will re-create the cluster on its next reconcile — with the same
// "kubeconfig/{context}" name, since that is the context's natural key.
func (s *Service) Delete(ctx context.Context, id ClusterID) error {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	return s.coreClient.Delete(ctx, obj.ID)
}

// watchListChan folds a beehive kind watch — a snapshot plus the change stream
// strictly above it — into one Kubernetes-style delta stream: the snapshot replayed as
// Added changes, then one change per object write. A deletion-pending object (a
// Modified carrying a soft-delete tombstone) is collapsed to Deleted: callers treat a
// tombstoned record as gone (List/Get hide it), so the watch removes it from the
// client's view at once, and the trailing hard Deleted repeats idempotently. beehive's
// terminal Failed change (retention passed the stream, or the beehive stopped) ends the
// stream after a log line — it is always the last value. fn is handed the object's id
// alongside the object, which is nil only on a Deleted whose final state could not be
// decoded; the removal is still reported, keyed by id. Generic over both kinds. The out
// channel is closed on exit.
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
		// beehive.ChangeType and ChangeType share their string values by construction,
		// so the conversion below is value-preserving, not a remap.
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

// Watch implements ClusterService. It forwards beehive's Cluster-kind WatchList as a
// delta stream: the on-subscribe snapshot as Added changes, then one change per object
// write. beehive conflates per object, so a slow client converges to each cluster's
// latest state rather than lagging. Per-probe chatter and the next-attempt countdown
// are deliberately NOT here — probe history streams via clusterEventsWatch and the
// schedule gauge via clusterScheduleWatch — so a settled disconnected cluster produces
// no churn. Cache sync status streams standalone via WatchCaches, so cache changes
// never re-emit a cluster.
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

// WatchCaches implements ClusterService. The ClusterCache-kind counterpart of Watch:
// beehive's cache WatchList forwarded as a standalone delta stream (snapshot as Added,
// then per-cache changes), each object built into a domain ClusterCache with its
// parent ClusterID resolved from the owner edge. The caller joins these onto clusters
// by ClusterID; deletion-pending caches are remapped to Deleted, same as Watch.
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

// WatchGVRDiscoveries implements ClusterService. The discovery counterpart of
// WatchCaches: beehive's ClusterCacheGVRDiscovery WatchList forwarded as a
// standalone delta stream, each object built into a domain ClusterCacheGVRDiscovery
// with its parent CacheID resolved from the owner edge.
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

// WatchGVRSyncs implements ClusterService — one cache's per-kind sync records as a delta
// stream, so a consumer can see which of a cache's kinds are caught up and which are not.
//
// **Cache-scoped, unlike every other object watch here.** There is one of these per served
// kind rather than one per cache, so an unscoped stream is a hundred-plus records that the
// always-mounted cluster provider would carry for every cache at once; this one is opened
// by whoever is actually looking at a cache.
//
// The scoping is done on the owner edge rather than by a store query, since beehive's
// WatchList is per-kind: the cache's discovery anchor has a deterministic name, so its id
// is one lookup, and each object's owner arrives eager-loaded (WithLoads(LoadOwner())) so
// the filter is a field read rather than a query per object.
// gvrSyncAnchorFilter keeps only the sync records owned by one cache's discovery anchor,
// which resolve reports (0 = no anchor yet). onErr is called for the first failed read only.
//
// It memoizes the anchors it has RULED OUT, and that is what makes it affordable: the
// underlying watch is fleet-wide, so this runs on every sync record of every cache — and
// while our own anchor is unresolved there is nothing cached to compare against, so each of
// those frames would cost its own point query (~1500 to drain a ten-cluster snapshot).
//
// A verdict never flips, which is what licenses the memo. An anchor that is not ours cannot
// become ours; and an anchor seen while ours does not yet exist cannot be it either, since
// the one created later gets a fresh id (they are AUTOINCREMENT and never reused). So one
// lookup per DISTINCT anchor decides, and the set holds one entry per cache in the fleet.
func gvrSyncAnchorFilter(
	resolve func() (beehive.ObjectID, error),
	onErr func(error),
) func(ClusterCacheGVRSyncChange) []ClusterCacheGVRSyncChange {
	notOurs := map[beehive.ObjectID]bool{}
	// Frames received while the anchor could not be read. They are HELD, not dropped:
	// beehive re-emits an object only when it changes, so a kind whose one frame fell in a
	// "database is locked" moment during cold start would stay invisible to this
	// subscription for its whole life. Released once a read succeeds and can judge them.
	var undecided []ClusterCacheGVRSyncChange

	return func(c ClusterCacheGVRSyncChange) []ClusterCacheGVRSyncChange {
		// A hard Deleted carries no owner edge, so it can't be attributed. Forwarded on its
		// id alone: the client keys removal on that, and an id it never added is a no-op.
		if c.Sync.DiscoveryID == 0 {
			return []ClusterCacheGVRSyncChange{c}
		}
		theirs := beehive.ObjectID(c.Sync.DiscoveryID)
		// The memo already answers THIS frame, but held frames need an anchor to be judged
		// against, and on a multi-cluster fleet the traffic after the read recovers is
		// mostly ids already ruled out. Short-circuiting on the memo alone therefore left
		// anything held during the error window held forever — and since beehive re-emits
		// an object only when it changes, a kind whose one frame landed in that window
		// stayed invisible for the subscription's life. So take the read when something is
		// waiting on it; the memo still covers the steady state, where nothing is.
		if notOurs[theirs] && len(undecided) == 0 {
			return nil
		}
		anchor, err := resolve()
		if err != nil {
			onErr(err)
			// A failed read must not rule the id out, and must not lose the frame either —
			// unless the frame is one we already know isn't ours.
			if !notOurs[theirs] && len(undecided) < maxUndecidedSyncFrames {
				undecided = append(undecided, c)
			}
			return nil
		}

		// The read worked, so everything held can be judged now, oldest first — the
		// consumer upserts by id, so replaying a superseded frame ahead of its successor
		// is harmless.
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

// maxUndecidedSyncFrames bounds the frames held while a cache's anchor cannot be read. High
// enough to cover a cold start's whole burst for a fleet of caches, low enough that a read
// broken for the process's life costs a bounded amount of memory.
const maxUndecidedSyncFrames = 4096

func (s *Service) WatchGVRSyncs(ctx context.Context, cacheID ClusterCacheID) (<-chan ClusterCacheGVRSyncChange, error) {
	// The anchor is resolved LAZILY and re-resolved as the stream runs, never latched at
	// subscribe — see anchorResolver. A cache that has no anchor yet is the normal state of
	// one just created (it gains one within a reconcile), so a subscribe-time miss must not
	// make the stream permanently empty; the name is deterministic, so the lookup is a cheap
	// idempotent point query.
	//
	// While unresolved every frame is dropped, which is correct: a cache with no anchor owns
	// no sync records, so nothing that arrives can be ours.
	//
	// The lookup separates "no anchor yet" from "the read failed". Only the first is normal,
	// and treating a read error the same way silently drops frames that will never be
	// replayed — so it is surfaced: at subscribe by failing outright (the client can retry),
	// and mid-stream by logging once rather than per frame.
	anchorName := ClusterCacheGVRDiscoveryName(beehive.ObjectID(cacheID))
	var (
		discoveryID beehive.ObjectID
		loggedErr   bool
	)
	// resolveAnchor separates "no anchor yet" from "the read failed". Only the first is
	// normal — a cache gains its anchor within a reconcile — and treating a read error the
	// same way silently drops frames that will never be replayed, so it is surfaced: at
	// subscribe by failing outright (the client can retry), and mid-stream by logging once
	// rather than per frame.
	//
	// A resolved id is kept for the stream's life, which is safe because the anchor's name
	// is derived from the cache id: an anchor lives as long as its cache, and collecting the
	// cache takes the id with it (they are AUTOINCREMENT and never reused). There is no
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
				// A hard Deleted carries no object, so its owner can't be checked. It is
				// forwarded rather than dropped: the client keys removal on the id, and a
				// stray id it never added is a no-op.
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

// cacheWatchRetryInterval paces a cache watch's re-read after a failed one. Slow enough
// that a persistently broken read isn't a hot loop, quick enough that a transient one
// (a busy writer, a moment of contention) is invisible. It exists because the write-ping
// that would otherwise drive recovery is resource-keyed: a kind nobody writes to would
// never send another.
const cacheWatchRetryInterval = 2 * time.Second

// syncHealthTick paces the sync-health rollup's periodic recompute. The verdict moves on
// watch frames, but the freshness stamps it folds live in controller memory and change
// with no frame at all (a worker's ~30s heartbeat writes nothing by design) — so without
// a tick the rollup would freeze exactly the way the cache stats field did. Emission is
// still change-gated, so a quiet fleet sends nothing.
const syncHealthTick = 10 * time.Second

// syncHealthSnapshot is every cache's current verdict, keyed by cache id — the whole
// output of the fold, published as one latest value.
//
// A published snapshot is immutable: the fold builds a fresh map each time rather than
// mutating the last, because subscribers read it concurrently.
type syncHealthSnapshot map[ClusterCacheID]ClusterCacheSyncHealth

// WatchCacheSyncHealth implements ClusterService — every cache's sync verdict, folded from
// its per-kind records, as a latest-value stream keyed by cache id.
//
// Unscoped and one record per cache, so the always-mounted cluster registry can carry it —
// which is the whole point: the per-kind stream it folds is a hundred-plus records per
// cache and is only ever opened for the cache being looked at.
//
// Subscribers share ONE fold (see syncHealthReceiver). This adapter is the per-subscriber
// half: it turns the shared snapshot into the per-cache frames the wire speaks, sending
// only what changed for THIS subscriber — which is what makes a late joiner's first read
// deliver every cache rather than nothing.
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
			// Forget the caches that left the snapshot, so a subscriber's memory tracks
			// the live fleet rather than every cache this process has ever seen. There is
			// no delete FRAME to send: this stream is a gauge keyed by cacheID, and the
			// consumer drops a cache's verdict when the cache itself leaves
			// clusterCachesWatch, which is the stream that owns that lifecycle.
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

// syncHealthReceiver returns a receiver on the shared fold, starting it on first use.
//
// Lazily rather than in Start for two reasons: a fleet nobody is watching should not be
// folded at all, and the fold outlives any one subscriber so it cannot hang off a
// subscriber's context. Once started it runs until Close — there is no refcount, which
// would buy little and add a teardown race for a goroutine that is idle when nothing
// changes.
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

// startSyncHealthFold opens the two watches the fold reads and runs it, publishing every
// recomputed snapshot to a latest-value hub.
//
// The hub carries the whole map rather than per-cache deltas, and that is the load-bearing
// choice. The fold's INPUT is large (a hundred-plus records per cache) but its OUTPUT is
// tiny (one per cache), so publishing the whole output is cheap — and it buys two
// properties a delta broadcast would have to reimplement. A new subscriber's first read
// returns the current value, which is exactly the "every cache, right now" a window needs
// on open. And a slow subscriber coalesces: it skips intermediate maps and gets the
// newest, which is correct for a gauge, where a dropped delta would instead be lost state.
func (s *Service) startSyncHealthFold() (*watch.Hub[syncHealthSnapshot], context.CancelFunc, chan struct{}, error) {
	// Background, not a caller's: the fold outlives every subscriber. It is cancelled by
	// stopSyncHealthFold, and by the fold goroutine itself on any other exit.
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
		// Declared first so it runs LAST: done means every defer below has run, which is
		// the whole point of waiting on it — see stopSyncHealthFold.
		defer close(done)
		defer func() {
			if s.syncHealthFoldExit != nil {
				s.syncHealthFoldExit()
			}
		}()
		// Cancel our OWN context on the way out, whatever ended us. A fold can end without
		// anybody calling the stop func — beehive terminating a watch (ErrWatchTooOld during
		// a cold-sync write storm), or either source channel closing — and the two
		// fleet-wide WatchList subscribers are registered against this context alone. Since
		// forgetSyncHealthFold then clears the cached stop func, nothing else would ever
		// call it: each self-termination would strand another pair for the process's life.
		defer cancel()
		defer hub.Close()
		// Forget this hub on the way out, so a fold that ended on its own — a watch that
		// beehive terminated, not our Close — doesn't leave the cached hub in place. A
		// closed hub hands every later subscriber a pre-closed receiver, so without this a
		// single ErrWatchTooOld during a cold-sync write storm would silently end sync
		// status for every window until the process restarts. Clearing it makes the next
		// subscriber start a fresh fold, which is how the per-subscriber watches already
		// recover.
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
				// The stamps live in controller memory and move with no frame behind them,
				// so this is the only thing that refreshes freshness on a settled cache.
				f.markAll()
			}
			f.flush()
		}
	}()
	return hub, cancel, done, nil
}

// endSyncHealthWatch reports whether a change frame is beehive's terminal one, logging why.
// It must be checked before the frame is folded: a Failed carries no object, which
// applyDiscovery/applySync would otherwise read as a deletion and silently drop every
// record of that kind on the way out.
func endSyncHealthWatch(ctx context.Context, kind string, t beehive.ChangeType, err error) bool {
	if t != beehive.Failed {
		return false
	}
	if ctx.Err() == nil {
		slog.Warn("clusterservice: sync-health watch ended", "kind", kind, "err", err)
	}
	return true
}

// forgetSyncHealthFold drops the cached hub when the fold that owns it has ended, so the
// next subscriber starts a new one. It clears only if the cache still points at THIS hub —
// a stop that already replaced or cleared it must not be undone — and leaves the shutdown
// latch alone, since a fold ending after Close must stay ended.
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
	// Released BEFORE the wait: the fold's own teardown calls forgetSyncHealthFold, which
	// takes this same lock, so holding it across the join would deadlock the shutdown.
	s.syncHealthMu.Unlock()
	if stop == nil {
		return
	}
	stop()
	// Join it. Cancelling alone only asks it to stop — the two fleet-wide WatchList leases
	// come back in its defers, so returning here would let beehive start draining while the
	// fold was still mid-flush, which is exactly what going first is meant to avoid. Bounded
	// by the caller's drain deadline so a wedged fold can't hold up the whole shutdown.
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
	// byDiscovery indexes the per-kind records by their anchor, and discoveriesOf the
	// anchors by their cache — together the reverse of cacheOf, so a flush can walk just
	// the dirty caches' records instead of the whole fleet's. Without them the dirty set
	// would scope which verdicts get recomputed but not how much is read to recompute
	// them, which is the quadratic part.
	byDiscovery   map[ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}
	discoveriesOf map[ClusterCacheID]map[ClusterCacheGVRDiscoveryID]struct{}
	// published is the last value sent per cache, so an unchanged recompute sends nothing.
	published map[ClusterCacheID]ClusterCacheSyncHealth
	// dirty is the set of caches whose fold may have moved since the last flush. Scoping
	// matters at cold start: each of a cache's hundred-plus kinds delivers its own frame,
	// and recomputing every cache on each of them is quadratic in the kind count.
	dirty map[ClusterCacheID]struct{}
	// stats reads every kind's freshness from the controller that measured them, in one
	// lock acquisition — see StatsSnapshot.
	stats func() map[ClusterCacheGVRSyncID]ClusterCacheGVRSyncStats
	// out publishes each recomputed snapshot. Latest-value, so a burst collapses.
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
	// An anchor that moved caches (not a thing today, but the map allows it) must leave
	// the one it came from, or its records would count toward both.
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
	// A deletion-pending object is gone as far as a consumer is concerned, the same rule
	// watchListChan applies — a child on its way out must not keep counting toward its
	// cache's verdict while GC works through the finalizers.
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
	// One lock acquisition for the whole flush rather than one per record — see
	// StatsSnapshot.
	stats := f.stats()

	// Only the dirty caches' own records are read: a record whose anchor hasn't streamed
	// in yet is in no cache's index and simply waits for the frame that anchors it.
	changed := false
	for cacheID := range f.dirty {
		anchors := f.discoveriesOf[cacheID]
		// No discovery anchor left: the cache is gone (its subtree was collected, or the
		// whole cluster was), so its verdict is DROPPED rather than recomputed. Recomputing
		// would fold zero records into a permanent "no kinds yet" Unknown and republish it
		// forever — every later subscriber would be told about caches that no longer exist,
		// and a delete/recreate cycle would grow the fold and every snapshot without bound.
		//
		// A cache is only in this map because an anchor was once seen (putDiscovery is the
		// sole entry point), and the cache controller ensures the anchor for a cache's whole
		// life, so "no anchors" means gone rather than "not yet". Nothing is published for a
		// cache that was never known.
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
	// A fresh map: the published one is read concurrently by every subscriber.
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

// filterChan forwards what decide returns for each value of in — nothing, that value, or
// that value behind a backlog decide had been holding. Closes out when in closes or ctx
// ends.
//
// Zero-or-MORE rather than a bool because a filter may not be able to decide a frame yet,
// and a watch frame dropped is a frame gone: beehive re-emits an object only when it
// changes, so a consumer that never sees it never sees it. Holding the undecided ones and
// releasing them once the answer arrives is what keeps that from being permanent.
func filterChan[T any](ctx context.Context, in <-chan T, decide func(T) []T) <-chan T {
	out := make(chan T, 1)
	go func() {
		defer close(out)
		for v := range in {
			// send, not a bare channel write: the consumer is a subscription that stops
			// draining the moment its client goes away (a closed sync dialog), and the
			// upstream watch's own ctx teardown can't unblock a goroutine already parked
			// in a send. Without this each open/close of a per-cache watch parks one
			// goroutine forever holding the change it was mid-forward on.
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

// buildCluster assembles a domain Cluster from a single Cluster beehive object. It
// does not join cache children — those stream standalone via WatchCaches and are
// joined by the caller — so it needs no ctx-bound reads.
func (s *Service) buildCluster(obj *beehive.Object[ClusterSpec, ClusterStatus]) Cluster {
	c := Cluster{
		ID:         ClusterID(obj.ID),
		Generation: obj.Generation,
		CreatedAt:  obj.CreatedAt,
		Spec:       obj.Spec,
		// Conditions are beehive object rows, read off the object rather than out of the
		// status blob. Already liveness-downgraded by the store, so a previous process's
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

// buildClusterCache assembles a domain ClusterCache from a single ClusterCache beehive
// object. Its parent ClusterID comes off the eager-loaded owner edge — see ownerObjectID
// for why that read is best-effort and why it must not hit the store.
func buildClusterCache(obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) ClusterCache {
	return ClusterCache{
		ID:         ClusterCacheID(obj.ID),
		ClusterID:  ClusterID(ownerObjectID(obj)),
		ServerUID:  obj.Spec.ServerUID,
		Conditions: obj.Conditions,
	}
}

// buildGVRDiscovery assembles a domain ClusterCacheGVRDiscovery from a single beehive
// object, its parent CacheID read off the eager-loaded owner edge.
func buildGVRDiscovery(obj *beehive.Object[ClusterCacheGVRDiscoverySpec, ClusterCacheGVRDiscoveryStatus]) ClusterCacheGVRDiscovery {
	return ClusterCacheGVRDiscovery{
		ID:         ClusterCacheGVRDiscoveryID(obj.ID),
		CacheID:    ClusterCacheID(ownerObjectID(obj)),
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}
}

// eventClient is the slice of a beehive kind client that reads its objects'
// event logs. Every kind client satisfies it (ListEvents/WatchEvents don't mention
// Spec/Status), so one reader serves every kind's events surface.
type eventClient interface {
	ListEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) ([]beehive.Event, error)
	WatchEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) (*beehive.EventStream, error)
}

// eventOpts builds the beehive read/watch options for an events query: an
// optional category filter plus a limit bound.
func eventOpts(category *string, limit int) []beehive.EventOption {
	opts := []beehive.EventOption{beehive.WithEventLimit(limit)}
	if category != nil {
		opts = append(opts, beehive.WithEventCategory(*category))
	}
	return opts
}

// toDomainEvent maps one beehive run to the wire shape, kept trivial by reusing the
// ObjectID scalar for the run id and binding Type straight to beehive.EventType.
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

// events reads one object's event timeline (newest run first) through the given
// kind client, optionally filtered to one category and bounded by limit (nil or
// <= 0 uses defaultEventLimit). A read error yields nil, not an error — a partial
// status read beats none.
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

// scheduleClient is the slice of a beehive kind client the schedule watch needs —
// the live reconcile-schedule gauge. The real coreClient satisfies it; tests fake it.
type scheduleClient interface {
	WatchSchedule(ctx context.Context, id beehive.ObjectID) (<-chan beehive.Schedule, error)
}

// toSchedule maps the beehive Schedule gauge to the domain view: a non-zero
// NextRequeueAt becomes a pointer, the zero time (nothing scheduled) becomes nil.
func toSchedule(s beehive.Schedule) Schedule {
	if s.NextRequeueAt.IsZero() {
		return Schedule{}
	}
	at := s.NextRequeueAt
	return Schedule{NextRequeueAt: &at}
}

// mapChan streams every value from src onto a fresh buffered channel, applying fn,
// until src closes or ctx ends (out is closed on exit). The shared body of the
// per-object gauge/log pumps (scheduleWatch, watchEvents).
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

// scheduleWatch streams one object's reconcile-schedule gauge through the given
// client, mapping each beehive.Schedule to the domain Schedule. A scheduling change
// fires no object WatchList, so this is the live source for the next-attempt
// countdown. The channel closes when the source closes or ctx ends.
func (s *Service) scheduleWatch(ctx context.Context, c scheduleClient, id beehive.ObjectID) (<-chan Schedule, error) {
	src, err := c.WatchSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, toSchedule), nil
}

// ClusterScheduleWatch implements ClusterService — the Cluster-kind entrypoint to the
// reconcile-schedule gauge, merged with the controller's in-flight probe signal so one
// subscription drives both the "Next check" countdown and the "checking now" state. The
// scheduler owns NextRequeueAt, the core controller owns Probing; both sub-sources are
// current-on-subscribe, so the merged stream emits a fresh Schedule as either moves.
func (s *Service) ClusterScheduleWatch(ctx context.Context, id ClusterID) (<-chan Schedule, error) {
	schedSrc, err := s.scheduleWatch(ctx, s.coreClient, beehive.ObjectID(id))
	if err != nil {
		return nil, err
	}
	probeSrc := s.coreCtrl.WatchProbe(ctx, id)
	return mergeSchedule(ctx, schedSrc, probeSrc), nil
}

// mergeSchedule folds the schedule gauge (NextRequeueAt) and the in-flight probe
// signal (Probing) into one Schedule stream, re-emitting the combined latest each
// time either side changes. A closed sub-source is dropped (nil'd) rather than
// ending the stream, so the other keeps flowing; the goroutine exits when both
// close or ctx ends. The out channel closes on exit.
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

// ClusterCacheGVRSyncEvents implements ClusterService — one synced kind's
// entrypoint to the generic event reader. This is where the sync-transition history
// lives: each worker report is recorded on its own child's timeline, so the caller keys
// on the child's id, not its cache's.
func (s *Service) ClusterCacheGVRSyncEvents(ctx context.Context, id ClusterCacheGVRSyncID, category *string, limit *int) ([]Event, error) {
	return s.events(ctx, s.gvrSyncClient, beehive.ObjectID(id), category, limit)
}

// ClusterCacheGVRSyncEventsWatch implements ClusterService — one synced kind's
// entrypoint to the generic event watch.
func (s *Service) ClusterCacheGVRSyncEventsWatch(ctx context.Context, id ClusterCacheGVRSyncID, category *string) (<-chan Event, error) {
	return s.watchEvents(ctx, s.gvrSyncClient, beehive.ObjectID(id), category)
}

// watchEvents streams one object's event log as bare runs through the given kind
// client, mirroring beehive's WatchEvents: its snapshot runs first, then the runs the
// log grows by. beehive conflates per run id, so the consumer upserts by Event.ID (a
// re-delivered id is an updated run, a new id a new run) — no add/modify classification
// needed. The channel closes when the source closes or ctx ends; the stream's terminal
// error (retention passed it, the object was collected, the beehive stopped) is logged.
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

// cacheRef resolves the on-disk locator for a cluster's active cache: the beehive
// ObjectIDs of the parent Cluster (directory) and the ClusterCache for its
// currently-connected identity (file). found is false when there is no active cache
// (never probed, or the cache for that UID is missing/torn-down), which callers treat
// as "no cache" rather than an error. Resolving takes the parent read (for Server.UID)
// plus a name lookup.
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
	// worstRank/worstReason track the most severe reason seen; offender names the kinds
	// behind it. Rank exists only to compare — the reason itself is carried along rather
	// than mapped back from an int, so there is one ordering to keep in step, not two.
	worstRank   int
	worstReason string
	offender    []SyncedKindRef
	// paused names the kinds observed Paused. Names, not a count, because a paused cache
	// reports them as its unhealthy resources — a pause is relayed to a cache's
	// hundred-plus children one at a time, so a partly-paused cache is a state a UI sees.
	paused []SyncedKindRef
	// notWatching counts every kind whose observed reason is not Watching — the ranked
	// ones AND the paused. It is what UnhealthyKinds reports, per the schema's "how many
	// of them are not currently Watching": the offender list can't answer that, since it
	// resets to a single name whenever a worse rank appears (one SyncFailed beside twenty
	// Stale would report one).
	notWatching int
	// unconfirmed counts the kinds nobody in THIS process has observed yet — no Synced
	// condition, or one a previous process wrote that beehive is serving downgraded. They
	// are neither healthy nor broken, and the verdict must not round them to either.
	unconfirmed int

	lastUpdateAt *time.Time
	lastLiveAt   *time.Time
}

// syncReasonRank orders the per-kind reasons by how much they should dominate a cache's
// verdict. A hard failure beats a stall beats a wait beats a catch-up. A pause is not in
// the ladder at all — it is the mildest reading, and verdict reaches it only when nothing
// ranked and nothing unobserved is left.
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
		// Not faults, and never reached: add handles both before it ranks anything. Named
		// so the default below can mean "unrecognized" rather than "healthy or unknown".
		return 0
	default:
		// An unfamiliar spelling is DEGRADED, not healthy. The schema tells consumers to
		// treat an unknown reason that way, and the fold is a consumer: ranking it 0 would
		// have made it invisible here — counted toward the total, matching no branch, and
		// so folding into a Watching verdict. Ranked at the bottom so it registers without
		// masking a reason this process does understand.
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
	// Only an OBSERVED Watching is health. A kind with no condition yet, or one a previous
	// process wrote that beehive is serving downgraded, has not been seen Watching by
	// anybody here — so it belongs in notWatching along with the outright faults, which is
	// what makes UnhealthyKinds mean what the schema says it means. Right after a restart
	// every kind is in exactly this state, and reporting 0 unhealthy beside an Unknown
	// verdict was a frame whose two halves disagreed.
	if cond != nil && !cond.Unconfirmed && cond.Reason == ReasonWatching {
		return
	}
	a.notWatching++

	// Neither healthy nor broken: asserting a pre-restart verdict is what the Unconfirmed
	// flag exists to prevent, and so is asserting HEALTH on its absence. See verdict.
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
		// No kinds yet — the discovery pass hasn't landed. Not a fault, and not health
		// either: nobody has observed anything to report.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case a.worstRank > 0:
		h.Status = ConditionFalse
		h.Reason = a.worstReason
		// Sorted so a consumer following "the first offender" (to pick which kind's
		// timeline to show) keeps that choice stable while a hundred kinds stream in.
		slices.SortFunc(a.offender, compareKindRefs)
		h.UnhealthyKindRefs = a.offender
	case a.unconfirmed > 0:
		// Some kind's verdict is still unobserved in this process. Healthy is a claim about
		// EVERY kind, so it can't be made yet — most visibly right after a restart, when
		// beehive serves every liveness condition downgraded until its worker re-confirms
		// it, and this branch is the difference between "still catching up" and a cache
		// reported fully synced before a single worker has run. Ranked below a real
		// failure: a kind observed to be broken is a fact worth surfacing even while others
		// are still reporting in.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case len(a.paused) > 0:
		// Some kinds are paused and nothing worse is known. Partial counts: a pause travels
		// down a cache's children one at a time, so the in-between frames are real, and
		// calling them Watching published a healthy status beside a non-zero
		// unhealthyKinds and no names to explain it. Naming the paused kinds keeps the
		// count and the list saying the same thing.
		h.Status, h.Reason = ConditionFalse, ReasonPaused
		slices.SortFunc(a.paused, compareKindRefs)
		h.UnhealthyKindRefs = a.paused
	default:
		h.Status, h.Reason = ConditionTrue, ReasonWatching
	}
	return h
}
