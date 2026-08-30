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

// Package clustersvc is the sidecar's Kubernetes boundary: Service and its four
// record families (Clusters, Caches, CachedKinds, CachedData).
//
// This file specifies the whole API — Service and the four family interfaces — and
// bootstraps beehive. One file per family implements it and holds everything else
// about that kind: its beehive shapes, the record served to GraphQL, its delta-watch
// frame, and the controller that writes it. shared.go holds the vocabulary every
// family reuses. Every beehive detail lives behind the Service interface.
//
// Layering: a controller holds policy only, and the mechanisms it drives are leaves
// under internal/ — private to this package by compiler rule. The leaves speak native
// vocabulary (GVRs, a rest.Config, cache rows) and never the records above; the
// controllers translate. A leaf that reaches for one of these types gets an import
// cycle, which is the enforcement. The connection surface is the exception: it reads no
// beehive object, so its types alias straight through and the leaf's exported shape is
// the boundary's. Mechanism growing in a controller instead is the signal to extract
// another leaf: this package's tests stay fast only while the controllers do no I/O of
// their own.
//
// The beehive kinds and their ownership chain:
//
//	Cluster                 (name: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache            (name: "{ClusterID}/{serverUID}")
//	    ↓ owns
//	ClusterCachedKind       (name: "cachedkind/{CacheID}/{apiVersion}/{resource}")
//
// A name is a per-kind reconcile key, never an identity. There is one Cluster kind
// and each source owns a disjoint name namespace inside it, so a source reconciles by
// name under beehive's name-uniqueness; the on-disk cache is keyed by ObjectID
// instead, so a name's arbitrary text never reaches the filesystem.
//
// A kind's GroupKind.Kind string is its Go type name, and both the Kind and the name
// prefixes above are persisted. Renaming one is a store migration the moment anything
// writes.
//
// Cluster carries connection status (Connected, Identified conditions + server/principal
// facts); its ClusterCache child carries sync status, folded per kind from the
// ClusterCachedKind records below it.
//
// The read side is mid-rebuild: the Cluster kind is served, ClusterCache is served for
// its point reads (Get, List, ListByCluster), every other family method panics, and the
// two cache controllers reconcile to a no-op. The interfaces and the types they carry
// hold the GraphQL and gRPC surfaces steady meanwhile.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// Service is the frontend-facing boundary: every beehive detail (names, owner chain,
// spec/status split, delta-watch mapping) lives behind it. Each record family hangs
// off its own sub-interface; only the connection surface, which reads no beehive
// object, sits at the top level. Delta watches follow
// docs/adr/2026-08-09-delta-watch-protocol.md and close when ctx ends.
//
// A single-object Watch runs the same protocol as its WatchList, over one id:
// snapshot, one Bookmark, then deltas. Absence is never an error — an id holding
// nothing gets the Bookmark alone, which is what lets a view open a subscription on an
// id it expects to be filled. A removal is Deleted; it does not end the stream, but it
// is the last frame that id will carry, because an id is never reused.
type Service interface {
	// Start launches the background work and returns the func that drains it. ctx
	// bounds startup; the stop func takes a drain deadline. Call stop before Close.
	Start(ctx context.Context) (func(context.Context) error, error)
	// Close releases the boundary's resources. Call after the stop func returns.
	Close() error

	Clusters() Clusters
	Caches() Caches
	CachedKinds() CachedKinds
	CachedData() CachedData

	// AcquireConnection claims id's connection and arms its probe cadence. It does not
	// dial — Lease.Conn waits — so a caller may hold a claim across a cluster being
	// down without a retry loop of its own. Release the claim.
	//
	// Where the record gates a dial: an id naming nothing, a disabled cluster, and one
	// awaiting deletion are refused here rather than handed a connection.
	AcquireConnection(ctx context.Context, id ClusterID) (Lease, error)
	// RetryConnection re-probes now and returns when that probe has finished, so it
	// blocks for the probe's round trip. The outcome lands on the record's conditions
	// and reaches watchers through Clusters().Watch, not here.
	RetryConnection(ctx context.Context, id ClusterID) error

	// The event timeline of any record that has one — Cluster, ClusterCache,
	// ClusterCachedKind today. Top-level, not per family: an event carries no kind
	// of its own, so one reader serves every timeline and the id is the whole key.
	// Both are off the record watches, so event chatter never re-emits a record.
	//
	// ListEvents returns newest run first, optionally filtered by category and
	// bounded by limit. WatchEvents streams the same log — snapshot, one Bookmark,
	// then growth. Distinct from CachedData().WatchEvents, which streams the *cluster's*
	// cached Kubernetes Events rather than kstack's own log.
	ListEvents(ctx context.Context, id ObjectID, category *string, limit *int) ([]Event, error)
	WatchEvents(ctx context.Context, id ObjectID, category *string) (*Stream[EventWatchFrame], error)
}

// The connection vocabulary is the leaf's, aliased rather than copied: a connection is native
// vocabulary all the way down, so there is nothing here to translate. Aliases and not plain
// return types because an internal package's type cannot be named by the packages implementing
// this interface — the resolver tests' fake among them. The Conn prefix belongs to this level,
// where a bare State or Identity would read as a record's.
type (
	Lease                 = kubeconn.Lease
	Connection            = kubeconn.Connection
	ConnIdentity          = kubeconn.Identity
	ConnState             = kubeconn.State
	ConnStateSubscription = kubeconn.StateSubscription
)

// The four families are specified together, apart from the kinds implementing them,
// because their rules are rules ACROSS the set: VerbNoun with the noun elided when it
// equals the family's subject, one method per scope rather than a scope argument, and
// By* naming the scope the caller passes. A violation is visible when the four read
// side by side and invisible when they don't.
//
// **A read reports the store as it is, and never filters.** A record awaiting deletion
// is served like any other, carrying the tombstone the consumer decides what to do
// with — a UI that renders it "Deleting…" is as valid as one that hides it, and only
// the consumer knows which. Deleted therefore means what beehive means by it: the row
// is gone. Filtering here instead cost an invariant every read, every watch, and every
// mutation had to maintain in agreement, and each place that forgot was a bug the
// types could not catch.

// Clusters is the Cluster record surface: the tracked clusters, their spec
// toggles, and the per-cluster streams that deliberately do not ride the record
// watch.
type Clusters interface {
	// Get returns one cluster by id, or (nil, nil) when the id names nothing.
	Get(ctx context.Context, id ClusterID) (*Cluster, error)
	// List returns every tracked cluster. Cache sync status is the caller's join from
	// Caches().WatchList.
	List(ctx context.Context) ([]*Cluster, error)

	// Watch streams one cluster as a delta watch, scoped to the record's id.
	// Bookmark-only while the id names no tracked cluster.
	//
	// Deleted is terminal for the id, since an id is never reused: a context still in
	// the kubeconfig is re-imported under a fresh one, which a caller has to watch
	// itself. A context merely dropped from the kubeconfig is not a deletion at all —
	// the record is orphaned in place (IsPresent=false), which arrives as Modified.
	Watch(ctx context.Context, id ClusterID) (*Stream[ClusterWatchFrame], error)
	// WatchList streams every cluster as a delta watch. The mark that starts a record's
	// deletion is an ordinary Modified — the row is still there, wearing a tombstone —
	// and Deleted follows when it is collected.
	WatchList(ctx context.Context) (*Stream[ClusterWatchFrame], error)

	// WatchSchedule streams a cluster's next-probe gauge. A stream because a
	// scheduling change fires no object WatchList, so it cannot ride Watch.
	// Refuses the records AcquireConnection refuses: an unprobed cluster has no
	// cadence.
	WatchSchedule(ctx context.Context, id ClusterID) (<-chan Schedule, error)
	// SetEnabled enables or disables a cluster and returns the updated record.
	SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)
	// SetSyncEnabled toggles a cluster's sync and returns the updated record.
	SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error)

	// Delete deletes the Cluster object; beehive GC cascades to ClusterCache.
	Delete(ctx context.Context, id ClusterID) error
}

// Caches is the ClusterCache record surface. A cluster owns zero or one cache at
// steady state, but a UID migration leaves the old one behind — so every
// per-cache read names the exact cache it means, never "the" cache of a cluster.
type Caches interface {
	// Get returns one cache by id, or (nil, nil) when the id names nothing.
	Get(ctx context.Context, id ClusterCacheID) (*ClusterCache, error)
	// List returns every cache in creation order. Which one is active is the caller's
	// live join (CacheIsActive), not a property here.
	List(ctx context.Context) ([]*ClusterCache, error)

	// Watch streams one cache as a delta watch. Bookmark-only until the cluster has
	// been probed and its cache created, so a caller may open this on a cache a
	// migration has not produced yet.
	Watch(ctx context.Context, id ClusterCacheID) (*Stream[ClusterCacheWatchFrame], error)
	// WatchList streams every cache as a delta watch parallel to
	// Clusters().WatchList; the caller joins caches onto clusters by Owner.ID.
	WatchList(ctx context.Context) (*Stream[ClusterCacheWatchFrame], error)

	// ListByCluster returns one cluster's caches, in the same order.
	ListByCluster(ctx context.Context, clusterID ClusterID) ([]*ClusterCache, error)
	// WatchByCluster streams one cluster's caches as a delta watch — what a view
	// scoped to a single cluster opens instead of filtering WatchList.
	WatchByCluster(ctx context.Context, clusterID ClusterID) (*Stream[ClusterCacheWatchFrame], error)

	// WatchStats streams one cache's contents as a live gauge. A stream, not a
	// ClusterCache field: a settled cache's object never changes, so a field would
	// freeze at subscribe time.
	WatchStats(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCacheStats], error)
	// WatchHealth streams every cache's sync verdict, folded from its per-kind
	// records. Unscoped where the rest of this family is per-cache: one fold serves
	// the fleet. A gauge, but a failable one — the fold reads watches of its own.
	WatchHealth(ctx context.Context) (*Stream[ClusterCacheHealth], error)
	// WatchSyncStatus streams one cache's sync detail — the discovery verdict and a row
	// per mirrored kind. What WatchHealth folds for a fleet, expanded for one cache.
	WatchSyncStatus(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCacheSyncStatus], error)

	// Clear deletes one cache's on-disk file and restarts its syncs; the record stays.
	Clear(ctx context.Context, id ClusterCacheID) (*ClusterCache, error)
}

// CachedKinds is the ClusterCachedKind surface — one record per kind a
// cache mirrors. Distinct from CachedData, which serves the mirrored content itself;
// these are the control-plane records describing what is mirrored.
//
// This family is the fleet's largest by an order of magnitude: a record per served
// kind per cache, so hundreds per cluster. Scope every read that can be scoped.
type CachedKinds interface {
	// Get returns one record by id, or (nil, nil) when the id names nothing.
	Get(ctx context.Context, id ClusterCachedKindID) (*ClusterCachedKind, error)
	// List returns every per-kind sync record in creation order.
	List(ctx context.Context) ([]*ClusterCachedKind, error)

	// Watch streams one per-kind record as a delta watch. Bookmark-only until the
	// kind is discovered; a kind the cluster stops serving is Deleted.
	Watch(ctx context.Context, id ClusterCachedKindID) (*Stream[ClusterCachedKindWatchFrame], error)
	// WatchList streams every per-kind record across every cache — the fleet's
	// widest stream, and one a view scoped to a cache wants WatchByCache for
	// instead. For a reader that genuinely spans caches: the sync-health rollup.
	WatchList(ctx context.Context) (*Stream[ClusterCachedKindWatchFrame], error)

	// ListByCache returns one cache's per-kind records — the owner edge, so a cache
	// nothing has discovered kinds for yet reads empty rather than as an error.
	ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedKind, error)
	// WatchByCache streams one cache's per-kind records as a delta watch — what a view
	// scoped to a single cache opens instead of filtering WatchList.
	WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedKindWatchFrame], error)

	// Clear drops one kind's cached objects and restarts its sync from an empty
	// mirror; the record stays and resyncs. Caches().Clear is the whole-cache form.
	Clear(ctx context.Context, id ClusterCachedKindID) (*ClusterCachedKind, error)

	// SetSyncEnabled stops or resumes one kind's sync and returns the updated record.
	// Pausing KEEPS the cached rows — readable throughout, and reconciled into rather
	// than rebuilt on the resume. Clear is the form that throws them away.
	SetSyncEnabled(ctx context.Context, id ClusterCachedKindID, syncEnabled bool) (*ClusterCachedKind, error)
}

// CachedData is the cached Kubernetes content in one cache's db — the only family whose
// reads leave beehive entirely.
//
// **A read binds to what is open and never creates a file**, so a cache with nothing
// synced yet answers the Bookmark alone and goes live when a writer opens one. A cache
// that goes away under a live watch ends it CLEANLY — Err is nil, and the client
// reconnects into the fresh snapshot — because a clear is a user pressing a button, not a
// watch breaking.
type CachedData interface {
	// ListKinds returns one cache's discovered kind catalog.
	ListKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) ([]ClusterCachedDataKind, error)
	// WatchKinds streams one cache's kind catalog as a delta watch (per-kind counts
	// update live).
	WatchKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCachedDataKindWatchFrame], error)
	// WatchObjects streams one kind's cached objects as a delta watch keyed by UID.
	// No frames while that kind hasn't synced.
	WatchObjects(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (*Stream[ClusterCachedDataObjectWatchFrame], error)
	// WatchEvents streams one cache's cached Kubernetes Events, newest first, as a
	// delta watch keyed by event UID. Woken separately from WatchKinds, so an event
	// burst never drives the kind-catalog re-read.
	WatchEvents(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCachedDataEventWatchFrame], error)
}

// The family accessors are stateless views onto the one *service: the split is
// about the shape of the API, not a split of the control plane behind it.
type (
	clustersAPI    struct{ s *service }
	cachesAPI      struct{ s *service }
	cachedKindsAPI struct{ s *service }
	cachedDataAPI  struct{ s *service }
)

func (s *service) Clusters() Clusters { return clustersAPI{s} }

func (s *service) Caches() Caches { return cachesAPI{s} }

func (s *service) CachedKinds() CachedKinds { return cachedKindsAPI{s} }

func (s *service) CachedData() CachedData { return cachedDataAPI{s} }

// Each family is asserted separately: satisfying Service only proves the accessors
// exist, not that any family is fully implemented.
var (
	_ Service     = (*service)(nil)
	_ Clusters    = clustersAPI{}
	_ Caches      = cachesAPI{}
	_ CachedKinds = cachedKindsAPI{}
	_ CachedData  = cachedDataAPI{}
)

// deps is what everything in this package draws from: one beehive client per kind, and
// the process-wide services. Built once in New and embedded by each owner, so a family
// reads through its own kind's client and a controller writing a kind it owns takes
// that kind's from here. A new kind or a new shared service is a field here, never
// another constructor parameter.
type deps struct {
	clusterClient beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient   beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	kindClient    beehive.Client[ClusterCachedKindSpec, ClusterCachedKindStatus]
	sourceClient  beehive.Client[ClusterSourceSpec, ClusterSourceStatus]

	kubeconfigSvc kubeconfigService
	kubeconnSvc   kubeconnService
	kubestoreMgr  kubestoreManager
	kubesyncSvc   kubesyncService
	pokeSvc       *poke.Service

	// kindSpecMu serializes the read-modify-write on a ClusterCachedKind spec, which two
	// writers share: the sync-enabled setter, and the sweep converging a catalog change
	// onto a record whose pause it has to carry forward. beehive's Update takes the whole
	// spec and offers no compare-and-swap, so without this the later write restores what
	// the earlier one changed. A pointer because deps is copied by value.
	kindSpecMu *sync.Mutex
}

func newDeps(bh *beehive.Beehive, kubeconfigSvc kubeconfigService, kubeconnSvc kubeconnService, kubestoreMgr kubestoreManager, kubesyncSvc kubesyncService, pokeSvc *poke.Service) deps {
	return deps{
		clusterClient: beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind),
		cacheClient:   beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind),
		kindClient:    beehive.NewClient[ClusterCachedKindSpec, ClusterCachedKindStatus](bh, ClusterCachedKindGroupKind),
		sourceClient:  beehive.NewClient[ClusterSourceSpec, ClusterSourceStatus](bh, ClusterSourceGroupKind),
		kubeconfigSvc: kubeconfigSvc,
		kubeconnSvc:   kubeconnSvc,
		kubestoreMgr:  kubestoreMgr,
		kubesyncSvc:   kubesyncSvc,
		pokeSvc:       pokeSvc,
		kindSpecMu:    &sync.Mutex{},
	}
}

// service is the concrete Service: the shared deps, plus what the boundary owns on
// top of them.
type service struct {
	deps
	// clusterSpecMu serializes the spec read-modify-write behind the Cluster setters,
	// which beehive cannot do for them: Update takes a whole spec and offers no
	// compare-and-swap.
	clusterSpecMu sync.Mutex

	// The server cache a reconcile reads, beehive, the anchors it must be up to create,
	// the controllers in registration order, then the triggers. Order is the whole
	// contract: beehive starts before anything that reaches the store and, being last
	// to stop and close among them, outlives every reconcile and every poke that could
	// still touch it.
	parts []lifecycle.Part
	// gaugeCadence re-measures the gauges. Both carry numbers that move while their
	// record settles — a file's size, a freshness stamp — so a gauge that only woke on
	// a change signal would go quiet exactly when it is healthy. A parameter so a test
	// picks its own timescale.
	gaugeCadence time.Duration
}

// beehiveRuntime gives beehive the lifecycle.StartCloser shape. Start already matches; the
// only thing missing is Close, because what closes is the store rather than the
// runtime on top of it.
type beehiveRuntime struct {
	bh    *beehive.Beehive
	store beehive.Store
}

func (r beehiveRuntime) Start(ctx context.Context) (func(context.Context) error, error) {
	return r.bh.Start(ctx)
}

func (r beehiveRuntime) Close() error { return r.store.Close() }

// maxEventRuns bounds retention per (object, category) timeline — counted in
// aggregated RUNS (state transitions), not occurrences. Global (set at beehive.New).
const maxEventRuns = 20

// New builds the cluster boundary rooted at dataDir (beehive.db, clusters/): it opens
// the store, registers the controllers, and leaves everything stopped until Start.
func New(dataDir string, kubeconfigSvc kubeconfigService, pokeSvc *poke.Service) (Service, error) {
	// Self-sufficient rather than order-dependent: this is the first thing the
	// composition root builds, so nothing else has made dataDir yet.
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	bhStore, err := beehivesqlite.Open(filepath.Join(dataDir, "beehive.db"))
	if err != nil {
		return nil, fmt.Errorf("open beehive store: %w", err)
	}
	// WithEventRetention bounds each (object, category) timeline to maxEventRuns runs.
	// The pass cadences are declared per kind at registration; see startupPass.
	bh, err := beehive.New(bhStore, beehive.WithEventRetention(maxEventRuns, 0))
	if err != nil {
		bhStore.Close()
		return nil, fmt.Errorf("init beehive: %w", err)
	}

	// The one that names credentials: everything above it asks about a kube-context or a
	// cluster, and this is what turns one into the credentials a probe dials.
	kubeconnSvc := kubeconn.New(kubeconfigSvc)
	// One store per cache under the registry, which the boundary reads, clears, and
	// removes.
	kubestoreMgr := kubestore.NewManager(filepath.Join(dataDir, "caches"), kubestore.DefaultRetention)
	// What fills them: it discovers what each cluster serves and mirrors every served kind
	// into that cluster's file. Armed by the cache and kind passes, never by a reader.
	kubesyncSvc := kubesync.New(kubeconnSvc, kubestoreMgr)
	d := newDeps(bh, kubeconfigSvc, kubeconnSvc, kubestoreMgr, kubesyncSvc, pokeSvc)

	controllers, err := registerControllers(bh, d)
	if err != nil {
		bhStore.Close()
		return nil, fmt.Errorf("register cluster controllers: %w", err)
	}

	parts := []lifecycle.Part{
		// Ahead of beehive: closing drops sockets and open files, and both have to
		// outlive every pass that could still be reaching for one. Stop and close
		// reverse the slice.
		{Name: "kubeconn", StartCloser: kubeconnSvc},
		{Name: "kubestore", StartCloser: kubestoreMgr},
		// Between the store and beehive: no pass can arm a cache that is stopping, and no
		// worker outlives the file it writes into.
		{Name: "kubesync", StartCloser: kubesyncSvc},
		{Name: "beehive", StartCloser: beehiveRuntime{bh: bh, store: bhStore}},
		clusterSourceBootstrap(d),
	}
	parts = append(parts, controllers...)
	parts = append(parts, restartSyncsOnResume(kubesyncSvc, pokeSvc))

	return &service{deps: d, parts: parts, gaugeCadence: defaultGaugeCadence}, nil
}

// clusterSourceBootstrap creates the discovery anchors once beehive is up. A Part
// rather than a step inside Start so a failure unwinds through StartAll and reports
// under a name, like every other participant; it has no background work to stop.
func clusterSourceBootstrap(d deps) lifecycle.Part {
	return lifecycle.Part{
		Name: "cluster source records",
		StartCloser: lifecycle.StartFunc(func(ctx context.Context) (func(context.Context) error, error) {
			return func(context.Context) error { return nil }, ensureClusterSources(ctx, d.sourceClient)
		}),
	}
}

// restartSyncsOnResume restarts every sync on a resume. A watch that died under a sleeping
// machine reports nothing, so nothing else would notice; a nil poke service — which a test
// builds against — subscribes to nothing and has nothing to stop.
func restartSyncsOnResume(kubesyncSvc kubesyncService, pokeSvc *poke.Service) lifecycle.Part {
	return lifecycle.Part{
		Name: "kubesync resume poke",
		StartCloser: lifecycle.StartFunc(func(context.Context) (func(context.Context) error, error) {
			if pokeSvc == nil {
				return func(context.Context) error { return nil }, nil
			}
			signals, unsubscribe := pokeSvc.Subscribe()
			var wg sync.WaitGroup
			wg.Go(func() {
				for range signals {
					kubesyncSvc.RestartAll()
				}
			})
			return func(ctx context.Context) error {
				// Unsubscribing closes the channel, which is what ends the loop above.
				unsubscribe()
				return drain.WithContext(ctx, wg.Wait)
			}, nil
		}),
	}
}

// startupPass reconciles every object of a kind once per process. Each owns state a
// restart invalidates and the store cannot report as owed: a live connection, an
// in-memory schedule. The store reads settled, because the generation was observed by a
// process that is gone.
var startupPass = beehive.WithStartupFullPass(true)

// sourceResync re-runs the discovery pass, each anchor timed from the end of its own
// last pass. Only ClusterSource takes one: it is the kind whose correctness rests on a
// poll, since what it reads is a file the store cannot see and a lost trigger poke is
// a change nothing else would report. Every other kind is woken by a spec write or a
// dependency edge, which is what a pass here would be re-deriving.
var sourceResync = beehive.WithIndividualPassInterval(clusterSourceResyncInterval)

// clusterResync re-probes each cluster, timed from the end of its own last pass. The
// second kind whose correctness rests on a poll: what it reports is a remote server's,
// so nothing in the store moves when the answer does. Per object rather than a sweep of
// the kind, which is what keeps a fleet from dialing in one burst.
var clusterResync = beehive.WithIndividualPassInterval(clusterProbeInterval)

// registerControllers builds and registers each kind's controller, which lives in that
// kind's file, and returns them in registration order. Together here rather than four
// calls spread across those files: the options are the whole subsystem's concurrency
// and retry budget, and it only reads as a budget in one place.
func registerControllers(bh *beehive.Beehive, d deps) ([]lifecycle.Part, error) {
	// Built here because a trigger is a registration option like any other, and its feed
	// is what makes the option mean anything. Each returns below as a Part after the
	// controllers, so nothing pokes a kind before there is something to poke.
	kubeconfigTrigger := newKubeconfigTrigger(d.kubeconfigSvc)
	kubeconnTrigger := newKubeconnTrigger(d.kubeconnSvc)
	// One per registration, because a trigger wakes a record for every value its feed carries:
	// one feed carrying both would wake a cache for each of its hundreds of kinds.
	syncCacheTrigger := newKubesyncDiscoveryTrigger(d.kubesyncSvc)
	syncKindTrigger := newKubesyncKindTrigger(d.kubesyncSvc)

	source := &clusterSourceController{deps: d}
	cluster := &clusterController{deps: d}
	cache := &clusterCacheController{deps: d}
	resource := &clusterCachedKindController{deps: d}

	errSource := beehive.Register(bh, ClusterSourceGroupKind, source, startupPass, sourceResync, beehive.WithTriggerByName(kubeconfigTrigger.Wakes()))
	errCluster := beehive.Register(bh, ClusterGroupKind, cluster, startupPass, clusterResync, beehive.WithTriggerByName(kubeconnTrigger.Wakes()))
	errCache := beehive.Register(bh, ClusterCacheGroupKind, cache, startupPass, beehive.WithTriggerByID(syncCacheTrigger.Wakes()))
	errResource := beehive.Register(bh, ClusterCachedKindGroupKind, resource, startupPass, beehive.WithTriggerByName(syncKindTrigger.Wakes()))
	if err := errors.Join(errSource, errCluster, errCache, errResource); err != nil {
		return nil, err
	}
	return []lifecycle.Part{
		{Name: "cluster source controller", StartCloser: source},
		{Name: "cluster controller", StartCloser: cluster},
		{Name: "cache controller", StartCloser: cache},
		{Name: "cached-resource controller", StartCloser: resource},
		{Name: "kubeconfig trigger", StartCloser: lifecycle.StartFunc(kubeconfigTrigger.Start)},
		{Name: "kubeconn trigger", StartCloser: lifecycle.StartFunc(kubeconnTrigger.Start)},
		{Name: "kubesync cache trigger", StartCloser: lifecycle.StartFunc(syncCacheTrigger.Start)},
		{Name: "kubesync kind trigger", StartCloser: lifecycle.StartFunc(syncKindTrigger.Start)},
	}, nil
}

// Start launches beehive (the controller harness + store subscription loop), then
// each controller's own background work. ctx bounds startup only; the returned stop
// func takes a drain deadline and blocks until the background work finishes. Call
// stop before Close.
func (s *service) Start(ctx context.Context) (func(context.Context) error, error) {
	return lifecycle.StartAll(ctx, s.parts)
}

// Close releases the controllers' resources and the beehive store. Call after the
// stop func returns.
func (s *service) Close() error {
	return lifecycle.CloseAll(s.parts)
}

// AcquireConnection claims the pool's connection for id's context. The claim is the
// caller's own, refcounted alongside the one clusterController holds, so releasing it
// never stops the cluster being probed.
func (s *service) AcquireConnection(ctx context.Context, id ClusterID) (Lease, error) {
	contextName, err := s.connectableContext(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.kubeconnSvc.Acquire(contextName), nil
}

// RetryConnection re-probes id's context and returns when that probe has finished, so a
// caller showing the retry as busy shows it for exactly as long as the probe took. What
// the probe found lands on the record's conditions, which is where a caller reads it.
//
// The pool claims the context for the wait, so a cluster nothing else holds is probed too.
// A probe already mid-run does not answer for this one: the wait is for the run the ask
// bought, which is the next one to begin.
func (s *service) RetryConnection(ctx context.Context, id ClusterID) error {
	contextName, err := s.connectableContext(ctx, id)
	if err != nil {
		return err
	}
	return s.kubeconnSvc.RetryAndWait(ctx, contextName)
}

// connectableContext looks id up and reads clusterContext off it. Shared by both methods
// above so no caller finds one of them willing to act on a record the other refuses.
func (s *service) connectableContext(ctx context.Context, id ClusterID) (string, error) {
	obj, err := s.clusterClient.Get(ctx, beehive.ObjectID(id))
	if err != nil {
		return "", wrapClusterErr("get", id, err)
	}
	return clusterContext(obj)
}

// clusterContext is the kube-context behind a record, or why the record will not be
// connected. Taken off an object a caller already holds, since a pass reaching for a
// connection has read the cluster for other reasons anyway.
//
// The three refusals are the record's own state, never the cluster's: whether the server
// answers is the probe's to find out and report, so an unreachable cluster is claimed and
// retried like any other.
func clusterContext(obj *beehive.Object[ClusterSpec, ClusterStatus]) (string, error) {
	switch {
	case obj.DeletionRequestedAt != nil:
		return "", fmt.Errorf("cluster %d is being deleted: %w", obj.ID, ErrNotConnectable)
	case !obj.Spec.Enabled:
		return "", fmt.Errorf("cluster %d is disabled: %w", obj.ID, ErrNotConnectable)
	case obj.Spec.Source.Kubeconfig == nil:
		// Another source's credentials are not this package's to resolve, the same rule
		// the cluster pass follows when it declines to claim one.
		return "", fmt.Errorf("cluster %d has no kubeconfig credentials: %w", obj.ID, ErrNotConnectable)
	}
	return obj.Spec.Source.Kubeconfig.Context, nil
}

// The one resolution more than one family needs. A helper only one family uses is that
// family's own, on its *API type in its kind's file; this is here because a caller in
// another file would otherwise reach across kinds for it.

// cacheBelongsTo reports whether cacheID names a live cache owned by clusterID — the
// gate every per-cache read shares. An absent or mismatched pair is not an error: a
// caller holds ids from watch frames, so a record collected in between is a race.
func (s *service) cacheBelongsTo(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (bool, error) {
	obj, err := s.cacheClient.Get(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get cluster cache %d: %w", cacheID, err)
	}
	owner, ok, err := obj.Owner()
	if err != nil {
		return false, fmt.Errorf("read cluster cache %d owner: %w", cacheID, err)
	}
	return ok && owner.ID == beehive.ObjectID(clusterID), nil
}
