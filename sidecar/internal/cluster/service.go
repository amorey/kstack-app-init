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
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterService is the boundary between the frontend (GraphQL today, gRPC
// later) and the cluster backend. Every beehive detail — slugs, the
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
	// changes as the sync engine writes objects (so per-kind counts update live). Empty
	// (no frames) when that cache's db isn't open, mirroring ClusterDataKinds' posture.
	ClusterDataKindsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataKindChange, error)
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
	coreCtrl     coreController
	cacheCtrl    *ClusterCacheController

	importer *KubeconfigImporter
	pokeSvc  *poke.Service

	// dataKindsDebounce bounds how often ClusterDataKindsWatch re-reads and diffs the
	// kind catalog. A busy cluster pings the store on every object write; each re-read
	// runs KindCatalog's grouped count-join over the whole object index, so without a
	// floor sustained watch traffic would keep the reader continuously aggregating.
	// This coalesces a burst of pings into a single re-read per interval (trailing
	// edge), keeping the live counts responsive without scanning per object event.
	dataKindsDebounce time.Duration
}

// defaultDataKindsDebounce floors the kind-catalog re-read interval — small enough
// that the dashboard nav's counts still read as live, large enough to collapse a
// high-churn cluster's per-object write pings into a bounded aggregation rate.
const defaultDataKindsDebounce = 250 * time.Millisecond

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
	// WithEventRetention bounds each object's connection event timeline to the
	// newest maxConnectionAttempts runs (per category), GC-swept.
	bh, err := beehive.New(bhStore, beehive.WithEventRetention(maxConnectionAttempts, 0))
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
	// Cap the connection controller's exponential reconnect backoff (base 1s, ×2)
	// at connectionMaxBackoff — a failed probe returns an error, so beehive owns the
	// doubling and resets it on the next success.
	coreCC, errCluster := beehive.Register(bh, ClusterGroupKind, coreCtrl, beehive.WithMaxRetryInterval(connectionMaxBackoff))
	cacheCC, errCache := beehive.Register(bh, ClusterCacheGroupKind, cacheCtrl)
	if err := errors.Join(errCluster, errCache); err != nil {
		bhStore.Close()
		_ = watcher.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}
	coreCtrl.SetControllerClient(coreCC)
	cacheCtrl.SetControllerClient(cacheCC)

	return &Service{
		bh:                bh,
		bhStore:           bhStore,
		watcher:           watcher,
		coreClient:        coreClient,
		cacheClient:       cacheClient,
		cacheManager:      cacheManager,
		connMgr:           connMgr,
		coreCtrl:          coreCtrl,
		cacheCtrl:         cacheCtrl,
		importer:          NewKubeconfigImporter(watcher, coreClient),
		pokeSvc:           pokeSvc,
		dataKindsDebounce: defaultDataKindsDebounce,
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
	// Roll up the kind catalog's trigger-maintained per-kind counts (O(kinds), no
	// object scan): total objects and the number of non-empty kinds. The catalog
	// lists advertised-but-empty kinds too, so KindCount only counts count > 0.
	rows, err := db.KindCatalog(ctx)
	if err != nil {
		return nil, err
	}
	objectCount, kindCount := 0, 0
	for _, r := range rows {
		if r.Count > 0 {
			objectCount += r.Count
			kindCount++
		}
	}
	return &ClusterCacheStats{
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
	rows, err := db.KindCatalog(ctx)
	if err != nil {
		return nil, err
	}
	kinds := make([]ClusterDataKind, len(rows))
	for i, r := range rows {
		kinds[i] = toDataKind(r)
	}
	return kinds, nil
}

// toDataKind maps a store KindCatalogRow onto the domain ClusterDataKind 1:1.
func toDataKind(r store.KindCatalogRow) ClusterDataKind {
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
// catalog as a Kubernetes-style delta watch: the current catalog as an Added burst on
// subscribe, then one Added/Modified/Deleted change per kind as the sync engine writes
// objects and pings the store (Count is a live LEFT JOIN, so an object write that
// changes a kind's count re-emits it as Modified). The store's coalescing Subscribe
// drives the re-read; the goroutine diffs each fresh catalog against the last snapshot
// it emitted.
//
// The stream follows the cache's on-disk db across its whole lifecycle via the store
// Manager's WatchDB, rather than binding once to the handle present at subscribe: if
// the cache isn't open yet (subscribed before the sync engine opens it — the common
// case for an unsynced cluster), the goroutine waits and binds when it opens; if the
// db is replaced under the same CacheID (a Clear-cache delete+reopen), it rebinds to
// the new handle and keeps diffing (the emptied catalog surfaces as Deletes, then the
// rebuild as Adds). An unopened cache simply emits nothing until it opens or ctx ends,
// matching ClusterDataKinds' empty posture.
func (s *Service) ClusterDataKindsWatch(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (<-chan ClusterDataKindChange, error) {
	out := make(chan ClusterDataKindChange, 1)
	ref := newCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	handles, cancelHandles := s.cacheManager.WatchDB(ref.CacheID)
	go func() {
		defer close(out)
		defer cancelHandles()

		prev := map[string]ClusterDataKind{}
		// emit diffs db's freshly-read catalog against prev, sending one change per
		// difference (Added/Modified/Deleted) and updating prev. Returns false if ctx
		// ended mid-send so the goroutine can exit.
		emit := func(db *store.ClusterDB) bool {
			rows, err := db.KindCatalog(ctx)
			if err != nil {
				return ctx.Err() == nil // transient read error: keep the stream, retry on next ping
			}
			next := make(map[string]ClusterDataKind, len(rows))
			for _, r := range rows {
				k := toDataKind(r)
				key := dataKindKey(k)
				next[key] = k
				old, existed := prev[key]
				switch {
				case !existed:
					if !send(ctx, out, ClusterDataKindChange{Type: ChangeAdded, Kind: k, CacheID: cacheID}) {
						return false
					}
				case old != k:
					if !send(ctx, out, ClusterDataKindChange{Type: ChangeModified, Kind: k, CacheID: cacheID}) {
						return false
					}
				}
			}
			for key, k := range prev {
				if _, ok := next[key]; !ok {
					if !send(ctx, out, ClusterDataKindChange{Type: ChangeDeleted, Kind: k, CacheID: cacheID}) {
						return false
					}
				}
			}
			prev = next
			return true
		}

		// emitEmpty reconciles prev against an empty catalog — one Deleted per held
		// kind, then clears prev. Called when the cache closes so a cache that never
		// reopens (a paused/cleared cache whose engine doesn't restart) doesn't leave
		// the dashboard permanently showing the closed cache's stale kinds. Returns
		// false if ctx ended mid-send. (No-op when prev is already empty, so an
		// unopened cache still emits nothing.)
		emitEmpty := func() bool {
			for _, k := range prev {
				if !send(ctx, out, ClusterDataKindChange{Type: ChangeDeleted, Kind: k, CacheID: cacheID}) {
					return false
				}
			}
			prev = map[string]ClusterDataKind{}
			return true
		}

		// db/pings track the currently-bound handle and its write-ping stream; both are
		// nil while no cache is open. bind swaps to a new handle (nil = closed),
		// resubscribing to its pings and emitting its current catalog as the new baseline.
		var (
			db        *store.ClusterDB
			pings     <-chan struct{}
			cancelSub func()
		)

		// A write ping arms a debounce timer instead of re-reading the catalog inline:
		// its fire drains a coalesced burst of per-object pings into a single re-read,
		// so a high-churn cluster can't keep KindCatalog's whole-index count-join
		// running back-to-back. `armed` tracks whether a re-read is pending; the timer
		// starts disarmed. (Go's timer guarantees no stale tick after Stop/Reset, so
		// the channel never needs a manual drain.)
		debounce := time.NewTimer(s.dataKindsDebounce)
		debounce.Stop()
		defer debounce.Stop()
		armed := false
		arm := func() {
			if !armed {
				debounce.Reset(s.dataKindsDebounce)
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
			disarm() // a fresh baseline is emitted below; drop any re-read pending for the old handle
			if cancelSub != nil {
				cancelSub()
				cancelSub = nil
				pings = nil
			}
			db = next
			if db == nil {
				return emitEmpty() // cache closed; reconcile against empty so a never-reopened cache doesn't retain stale kinds
			}
			pings, cancelSub = db.Subscribe()
			return emit(db)
		}
		defer func() {
			if cancelSub != nil {
				cancelSub()
			}
		}()

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
					// The bound db closed out from under us (e.g. a Clear-cache delete);
					// drop the stale sub (and any pending re-read) and wait for WatchDB
					// to deliver the new handle.
					disarm()
					pings, cancelSub, db = nil, nil, nil
					continue
				}
				arm() // coalesce; the debounce fire runs the actual re-read + diff
			case <-debounce.C:
				armed = false
				if !emit(db) {
					return
				}
			}
		}
	}()
	return out, nil
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
// re-probe via the core controller's retry bus (Reprobe) — no spec write. The
// reprobe is backoff-neutral: running off-worker, a failed manual probe leaves
// beehive's reconnect ladder untouched (it neither resets to the 1s ramp nor
// advances the schedule), so manual clicks don't perturb the automatic cadence.
// (Routing through Client.Requeue instead would ride beehive's worker and so
// participate in backoff.) Dispatch is fire-and-forget, after a cheap read-only
// existence check that surfaces ErrNotFound for a just-deleted cluster. The
// outcome lands on the record's conditions and reaches watchers through Watch.
func (s *Service) RetryConnection(ctx context.Context, id ClusterID) error {
	if _, err := s.clusterByID(ctx, id); err != nil {
		return err
	}
	s.coreCtrl.Reprobe(id)
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
	c := s.buildCluster(obj)
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

// changeType maps a beehive change to the domain ChangeType, collapsing a
// deletion-pending object (a Modified carrying a soft-delete tombstone) to Deleted:
// beehive keeps the object around, tombstoned, until its finalizers clear, but
// callers treat a tombstoned record as gone (List/Get hide it), so the watch removes
// it from the client's view at once. The trailing hard Deleted then repeats
// idempotently. A real Deleted stays Deleted. Generic over both kinds — it reads only
// the change type + the object's tombstone, which every kind carries.
func changeType[Spec, Status any](ev beehive.Change[Spec, Status]) ChangeType {
	if ev.Object.DeletionRequestedAt != nil {
		return ChangeDeleted
	}
	// beehive.ChangeType and ChangeType share their string values by construction
	// (both mirror the k8s Added/Modified/Deleted words), so this is a value-preserving
	// conversion, not a remap.
	return ChangeType(ev.Type)
}

// Watch implements ClusterService. It forwards beehive's Cluster-kind WatchList as a
// delta stream: the on-subscribe snapshot arrives as Added changes, then one change
// per object write. beehive conflates per object (a Deleted still carries the final
// body), so a slow client converges to each cluster's latest state rather than
// lagging as loss. Per-probe chatter and the next-attempt countdown are deliberately
// NOT here — probe history streams via clusterEventsWatch and the schedule gauge via
// clusterScheduleWatch, both per-cluster — so a steadily-disconnected cluster whose
// status has settled produces no churn. Cache sync status streams standalone via
// WatchCaches (joined client-side), so cache changes never re-emit a cluster.
func (s *Service) Watch(ctx context.Context) (<-chan ClusterChange, error) {
	src, err := s.coreClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, func(ev beehive.Change[ClusterSpec, ClusterStatus]) ClusterChange {
		c := s.buildCluster(ev.Object)
		return ClusterChange{Type: changeType(ev), Cluster: &c}
	}), nil
}

// WatchCaches implements ClusterService. The ClusterCache-kind counterpart of Watch:
// beehive's cache WatchList forwarded as a standalone delta stream (snapshot as Added,
// then per-cache changes), each object built into a domain ClusterCache with its
// parent ClusterID resolved from the owner edge. The caller joins these onto clusters
// by ClusterID; deletion-pending caches are remapped to Deleted, same as Watch.
func (s *Service) WatchCaches(ctx context.Context) (<-chan ClusterCacheChange, error) {
	src, err := s.cacheClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, func(ev beehive.Change[ClusterCacheSpec, ClusterCacheStatus]) ClusterCacheChange {
		cc := s.buildClusterCache(ctx, ev.Object)
		return ClusterCacheChange{Type: changeType(ev), Cache: &cc}
	}), nil
}

// GetConnection implements ClusterService.
func (s *Service) GetConnection(id ClusterID) *rest.Config {
	return s.connMgr.Get(id)
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
	}
	if obj.DeletionRequestedAt != nil {
		t := *obj.DeletionRequestedAt
		c.DeletionRequestedAt = &t
	}
	if obj.Status != nil {
		c.Status = *obj.Status
	}
	return c
}

// buildClusterCache assembles a domain ClusterCache from a single ClusterCache
// beehive object, resolving its parent ClusterID via the owner edge (best-effort: a
// hard-deleted cache has no edge left, but by then the client has already dropped it
// from the soft-delete → Deleted change, so a zero ClusterID on the trailing hard
// Deleted is harmless — the client keys removal on the cache id).
func (s *Service) buildClusterCache(ctx context.Context, obj *beehive.Object[ClusterCacheSpec, ClusterCacheStatus]) ClusterCache {
	cc := ClusterCache{
		ID:        ClusterCacheID(obj.ID),
		ServerUID: obj.Spec.ServerUID,
	}
	if owner, ok, err := s.cacheClient.GetOwner(ctx, obj.ID); err == nil && ok {
		cc.ClusterID = ClusterID(owner.ID)
	}
	if obj.Status != nil {
		cc.Status = *obj.Status
	}
	return cc
}

// eventClient is the slice of a beehive kind client that reads its objects'
// event logs. coreClient and cacheClient both satisfy it (ListEvents/WatchEvents
// don't mention Spec/Status), so one reader serves every kind's events surface.
type eventClient interface {
	ListEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) ([]beehive.Event, error)
	WatchEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) (<-chan beehive.Event, error)
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

// toDomainEvent maps one beehive run to the wire shape — the beehive→domain
// crossing (like buildCluster/listCaches), kept trivial by reusing the ObjectID
// scalar for the run id and binding Type straight to beehive.EventType.
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

// mapChan streams every value from src onto a fresh buffered channel, applying
// fn, until src closes or ctx ends; the out channel is closed on exit. It's the
// shared body of the per-object gauge/log pumps (scheduleWatch, watchEvents),
// which differ only in element type and mapper.
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

// ClusterScheduleWatch implements ClusterService — the Cluster-kind entrypoint to
// the (generic) reconcile-schedule gauge, merged with the controller's in-flight
// probe signal so a single subscription drives both the "Next check" countdown
// and the "checking now" state. The scheduler owns NextRequeueAt; the core
// controller owns Probing (it holds writeMu across the probe). Both sub-sources
// are current-on-subscribe, so the merged stream emits a fresh Schedule as either
// side moves.
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
		emit := func() bool {
			select {
			case out <- cur:
				return true
			case <-ctx.Done():
				return false
			}
		}
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

// watchEvents streams one object's event log as bare runs through the given kind
// client, mirroring beehive's own WatchEvents (which streams <-chan Event, not a
// delta). beehive replays the matching runs as a snapshot (oldest-first) then live
// runs, conflating per run id — a lagging subscriber converges to each run's latest
// state — so the consumer upserts by Event.ID (a re-delivered run id is an updated
// run, a new id is a new run); no add/modify classification is needed here. The
// returned channel closes when the source closes or ctx ends.
func (s *Service) watchEvents(ctx context.Context, c eventClient, id beehive.ObjectID, category *string) (<-chan Event, error) {
	src, err := c.WatchEvents(ctx, id, eventOpts(category, defaultEventLimit)...)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, toDomainEvent), nil
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

// cacheRef resolves the on-disk cache locator for a cluster's *active* cache: the
// beehive ObjectIDs of the parent Cluster (directory) and the ClusterCache for its
// currently-connected identity (file). found is false when there is no active cache
// — the cluster has never successfully probed (no Server.UID yet), or the cache for
// that UID is missing/torn-down — which callers treat as "no cache" rather than an
// error. The active identity's UID keys the cache slug, so resolving it takes the
// parent read (for Server.UID) plus a slug lookup.
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
	cacheObj, err := s.cacheClient.GetBySlug(ctx, ClusterCacheSlug(id, activeUID))
	if errors.Is(err, beehive.ErrNotFound) {
		return store.CacheRef{}, false, nil
	}
	if err != nil {
		return store.CacheRef{}, false, err
	}
	return newCacheRef(beehive.ObjectID(id), cacheObj.ID), true, nil
}
