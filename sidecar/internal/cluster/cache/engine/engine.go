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

// Package engine mirrors one real Kubernetes cluster into its local SQLite cache
// (internal/cluster/cache/store).
//
// The Engine walks /apis discovery and spins up one per-GVR kindDriver, so one
// code path serves every Kind — built-ins and CRDs alike. Events get their own
// store and table (their access pattern differs); everything else lands in the
// universal `objects` table.
//
// Each driver (driver.go) resumes cheaply on a wake: it seeds a RetryWatcher from
// the kind's persisted resourceVersion (apply deltas, no LIST) and only on a 410
// or a cold cache falls back to a metadata-first full re-sync. A stock Reflector
// can't be seeded with a stored RV, so it always re-LISTs every body.
//
// One Engine runs per synced cluster, owned by the cluster package's sync
// controller, which decides when it starts, stops, or restarts (spec changes,
// credential rotation, resync pokes); the engine reports its coarse state back
// through the Sink it was constructed with.
package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"
	toolswatch "k8s.io/client-go/tools/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// init routes client-go's API-server warning headers (deprecation notices, etc.)
// through slog at Debug level. The default handler logs them as INFO/WARN, which
// is noisy whenever the cluster uses any deprecated API — and the warnings are
// about the cluster's contents, not anything the sidecar can act on.
func init() {
	rest.SetDefaultWarningHandler(slogWarningHandler{})
}

type slogWarningHandler struct{}

func (slogWarningHandler) HandleWarningHeader(code int, agent, message string) {
	if message == "" {
		return
	}
	slog.Debug("k8s API warning", "code", code, "agent", agent, "message", message)
}

// EngineState is the engine's coarse phase, reported through the Sink.
type EngineState string

const (
	// EngineSyncing means the engine is starting or catching the cache up:
	// discovery walk, or at least one driver still pre-first-watch.
	EngineSyncing EngineState = "Syncing"
	// EngineWatching means the initial discovery cohort has all entered their
	// watch phase — the cache is caught up on the kinds present when the run
	// started and is streaming deltas. A kind discovered mid-run (a CRD or
	// aggregated API installed after catch-up) joins WITHOUT regressing this
	// state: it doesn't gate the milestone, so the engine stays Watching while
	// that late driver does its first LIST. The liveness monitor gives each such
	// driver a bounded startup grace (staleLaggards) and flips the engine to
	// EngineStale only if it never reaches its watch phase — so a late driver
	// that can't LIST/watch surfaces as Stale, not as a silently-Watching gap.
	EngineWatching EngineState = "Watching"
	// EngineErrored means the engine hit an engine-level failure (discovery,
	// client construction) and is retrying with backoff.
	EngineErrored EngineState = "Errored"
	// EngineStale means the engine was caught up (Watching) but at least one
	// driver's watch stopped proving it's alive (no delta, bookmark, or reconnect)
	// past the staleness threshold — the cache may be behind. Distinct from
	// Errored (a hard engine failure): the engine is still running, just not
	// demonstrably current.
	EngineStale EngineState = "Stale"
)

// EngineStatus is one coherent snapshot of the engine's reportable state.
// Per-driver errors are deliberately not folded in — they're logged; the
// state is a coarse human-facing label.
type EngineStatus struct {
	State EngineState
	// LastError is the engine-level failure message; non-empty only for
	// EngineErrored.
	LastError string
	// LastSyncedAt is when the cache last received fresh data; nil if not
	// yet this engine run. Flushed coarsely (~30s), not per write.
	LastSyncedAt *time.Time

	// The following describe the catch-up milestone and feed the controller's event
	// message; which reports carry each field varies (see the per-field notes). All
	// are reset (clearCatchUp) when the engine re-enters EngineSyncing.
	//
	// ColdStart is true for the cache's first-ever sync, false when it resumed an
	// already-populated cache — the signal separating SyncStart/SyncComplete from
	// ResyncStart/ResyncComplete.
	ColdStart bool
	// SyncedObjects/SyncedKinds are how many objects across how many kinds the sync
	// involves; CaughtUpIn is how long catch-up took. They power the "…N objects
	// across M kinds in Ds" messages. Set on the caught-up EngineWatching report;
	// SyncedObjects/SyncedKinds are also set on a warm EngineSyncing start report
	// (the warm cache being resumed, CaughtUpIn still zero). Zero otherwise.
	SyncedObjects int
	SyncedKinds   int
	CaughtUpIn    time.Duration

	// ResyncedKinds/ResyncedObjects break down how much of a warm resume was real
	// work rather than a clean reconnect: how many kinds fell back to a full re-sync
	// and how many bodies those re-pulled. Aggregated across drivers, set only on the
	// caught-up EngineWatching report; zero on a pure reconnect and on a cold build.
	// The controller renders them into the ResyncComplete message.
	ResyncedKinds   int
	ResyncedObjects int

	// StaleKinds names the kinds whose watch went quiet past the threshold; set
	// only on EngineStale reports (the "no watch heartbeat for X" message), zero
	// otherwise.
	StaleKinds []string
}

// clearCatchUp zeroes the catch-up milestone facts so a later report (a
// heartbeat, an error, or a liveness recovery) can't carry a prior run's stale
// counts. Called wherever the engine leaves the caught-up state.
func (s *EngineStatus) clearCatchUp() {
	s.ColdStart = false
	s.SyncedObjects, s.SyncedKinds, s.CaughtUpIn = 0, 0, 0
	s.ResyncedKinds, s.ResyncedObjects = 0, 0
	s.StaleKinds = nil
}

// Sink receives engine status snapshots; implemented by the kube package's
// sync controller, which folds them onto the cluster record. Report must be
// safe for concurrent use (the run loop and freshness loop both report).
type Sink interface {
	Report(s EngineStatus)
}

// Engine mirrors one cluster into its cache for as long as it runs: a
// supervision loop around discovery + per-GVR drivers, plus a freshness
// tracker. Construct with NewEngine, then Start once; Stop tears it down.
type Engine struct {
	cfg  *rest.Config
	cdb  *store.ClusterDB
	sink Sink

	// status is the engine's current snapshot; reports merge into it under
	// mu so the two reporting goroutines (run loop, freshness loop) always
	// hand the sink a coherent EngineStatus.
	mu     sync.Mutex
	status EngineStatus

	backoffInit        time.Duration
	backoffMax         time.Duration
	flushInterval      time.Duration
	staleThreshold     time.Duration
	staleCheckInterval time.Duration
	discoveryDebounce  time.Duration
	// discoveryPoll is the cadence of the pull-based discovery backstop: it re-walks
	// discovery on a timer so anything the best-effort trigger watches miss (a denied
	// watch, a built-in API surface change with no watchable object) is still reconciled.
	discoveryPoll time.Duration
	// resyncPeriod is the per-driver periodic full-resync interval — the object-level
	// pull-based backstop, passed to each kindDriver (see kindDriver.resyncPeriod).
	resyncPeriod time.Duration
	// sleep/now are test seams (deterministic backoff and freshness stamps).
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time
	// newDebounceTimer builds the resettable timer debounceTriggers drives; a test
	// seam (defaults to a real time.Timer) so the trailing window can be fired
	// deterministically instead of by waiting real wall-clock time.
	newDebounceTimer func(time.Duration) resettableTimer

	baseCtx       context.Context
	baseCtxCancel context.CancelFunc
	wg            sync.WaitGroup
}

// Engine retry backoff: if a run exits while the engine is still wanted (a
// startup error, or discovery transiently yielding nothing), the engine
// retries itself — the sync controller deliberately doesn't watch for engine
// death, only for record changes. Capped exponential, reset on a run that
// reaches its drivers.
const (
	engineBackoffInit = 1 * time.Second
	engineBackoffMax  = 30 * time.Second

	// freshnessFlushInterval is how often the in-memory "last received data"
	// timestamp is flushed through the sink. Coarse on purpose — the value
	// backs a human-facing "synced N ago" label, not precise accounting.
	freshnessFlushInterval = 30 * time.Second

	// staleThreshold is how long a caught-up driver may go without proving its watch
	// is alive (delta, bookmark, or reconnect) before the engine flags the cache
	// stale. Comfortably above the API server's ~1min bookmark cadence so a few
	// missed bookmarks don't trip it; the connection sentinel catches hard
	// disconnects far faster, so this is the slow backstop for a silently-wedged
	// watch. (A rare API server honouring no bookmarks could false-positive — an
	// accepted limitation.)
	staleThreshold = 5 * time.Minute
	// staleCheckInterval is how often the liveness monitor re-evaluates.
	staleCheckInterval = 30 * time.Second

	// discoveryDebounceDefault is the trailing window the discovery reconciler waits
	// after a trigger before re-walking /apis, so an operator installing a bundle of
	// CRDs at once collapses into a single discovery pass instead of one per CRD.
	discoveryDebounceDefault = 1 * time.Second

	// discoveryPollDefault is the discovery backstop cadence: how often the engine
	// re-walks discovery regardless of trigger activity, catching anything the watch
	// triggers miss. Fast enough that a stale driver set self-heals promptly, slow
	// enough that the /apis walk is negligible load.
	discoveryPollDefault = 5 * time.Minute
	// resyncPeriodDefault is the per-driver object-resync backstop cadence: each driver
	// forces a full re-sync this often (jittered), reconciling any object drift a
	// best-effort watch missed. A long interval — the watch handles the steady state;
	// this is only the safety net — matching a client-go informer's resyncPeriod.
	resyncPeriodDefault = 30 * time.Minute
)

// engineOption configures an Engine's test seams. Production never tunes
// them, so NewEngine exposes none — the unexported-option pattern shared
// with the driver and internal/cloud/prefsync.
type engineOption func(*Engine)

func withEngineSleep(fn func(context.Context, time.Duration) error) engineOption {
	return func(e *Engine) { e.sleep = fn }
}

func withFlushInterval(d time.Duration) engineOption {
	return func(e *Engine) { e.flushInterval = d }
}

func withEngineNow(fn func() time.Time) engineOption {
	return func(e *Engine) { e.now = fn }
}

func withStaleThreshold(d time.Duration) engineOption {
	return func(e *Engine) { e.staleThreshold = d }
}

func withStaleCheckInterval(d time.Duration) engineOption {
	return func(e *Engine) { e.staleCheckInterval = d }
}

func withDiscoveryDebounce(d time.Duration) engineOption {
	return func(e *Engine) { e.discoveryDebounce = d }
}

func withDiscoveryPoll(d time.Duration) engineOption {
	return func(e *Engine) { e.discoveryPoll = d }
}

func withEngineResyncPeriod(d time.Duration) engineOption {
	return func(e *Engine) { e.resyncPeriod = d }
}

func withDebounceTimer(fn func(time.Duration) resettableTimer) engineOption {
	return func(e *Engine) { e.newDebounceTimer = fn }
}

// NewEngine builds an engine over resolved credentials, an open cluster
// cache, and the status sink. It starts nothing; call Start.
func NewEngine(cfg *rest.Config, cdb *store.ClusterDB, sink Sink) *Engine {
	return newEngineWithOptions(cfg, cdb, sink)
}

func newEngineWithOptions(cfg *rest.Config, cdb *store.ClusterDB, sink Sink, opts ...engineOption) *Engine {
	baseCtx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:                cfg,
		cdb:                cdb,
		sink:               sink,
		backoffInit:        engineBackoffInit,
		backoffMax:         engineBackoffMax,
		flushInterval:      freshnessFlushInterval,
		staleThreshold:     staleThreshold,
		staleCheckInterval: staleCheckInterval,
		discoveryDebounce:  discoveryDebounceDefault,
		discoveryPoll:      discoveryPollDefault,
		resyncPeriod:       resyncPeriodDefault,
		sleep:              ctxSleep,
		now:                time.Now,
		newDebounceTimer:   realResettableTimer,
		baseCtx:            baseCtx,
		baseCtxCancel:      cancel,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Start brings the engine online: the supervised run loop and the freshness
// tracker. The freshness subscription is taken synchronously so no write ping
// from the run loop's drivers can slip past it. Call once.
func (e *Engine) Start() {
	pings, cancelSub := e.cdb.Subscribe()
	e.wg.Go(func() { e.runLoop(e.baseCtx) })
	e.wg.Go(func() {
		defer cancelSub()
		e.freshnessLoop(e.baseCtx, pings)
	})
}

// Stop tears the engine down: cancels both loops and waits for them to join,
// bounded by ctx (a stuck driver mid-SQL write joins as soon as its statement
// returns). Returns ctx.Err() if the deadline expires first.
func (e *Engine) Stop(ctx context.Context) error {
	e.baseCtxCancel()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// report merges a partial update into the engine's status under mu and
// forwards the full snapshot to the sink.
func (e *Engine) report(update func(*EngineStatus)) {
	e.mu.Lock()
	update(&e.status)
	snapshot := e.status
	e.mu.Unlock()
	e.sink.Report(snapshot)
}

// reportCaughtUp emits the catch-up milestone — every driver has entered its watch
// phase. It stamps elapsed time and carries the drivers' aggregated re-sync
// breakdown so the controller can compose the SyncComplete/ResyncComplete message.
// Only a cold build's message reports the object total, so the whole-table count
// runs on that path alone; a count failure is logged and reported as zero.
func (e *Engine) reportCaughtUp(ctx context.Context, coldStart bool, kinds int, startedAt time.Time, resyncedKinds, resyncedObjects int) {
	objects := 0
	if coldStart {
		objects = e.countOrZero(ctx, cachedObjectCount, "count cached objects")
	}
	elapsed := e.now().Sub(startedAt)
	e.report(func(s *EngineStatus) {
		s.State, s.LastError = EngineWatching, ""
		s.ColdStart = coldStart
		s.SyncedKinds = kinds
		s.SyncedObjects = objects
		s.CaughtUpIn = elapsed
		s.ResyncedKinds = resyncedKinds
		s.ResyncedObjects = resyncedObjects
	})
}

// cacheHasData reports whether any kind has persisted a resume cookie — i.e. the
// cache holds a prior sync's state. Read once at run start so the catch-up
// milestone can tell a cold cache from a resume; keyed on the resume cookie, not
// an object count, so an empty cluster's resume isn't misread as a cold start.
func cacheHasData(ctx context.Context, cdb *store.ClusterDB) (bool, error) {
	var has bool
	err := cdb.Reader().QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cluster_meta WHERE key LIKE ?)`,
		"%"+lastListRVSuffix).Scan(&has)
	return has, err
}

// countOrZero runs a cache-count query, logging and returning 0 on error so a
// failed count degrades the event message rather than failing the sync.
func (e *Engine) countOrZero(ctx context.Context, count func(context.Context, *store.ClusterDB) (int, error), what string) int {
	n, err := count(ctx, e.cdb)
	if err != nil {
		slog.Warn("clustersync: "+what, "id", e.cdb.ID(), "err", err)
		return 0
	}
	return n
}

// cachedObjectCount totals the objects mirrored across every kind (the universal
// objects table plus events), reported at catch-up for the "…N objects across M
// kinds" message (and at a warm ResyncStart for the warm-cache size).
func cachedObjectCount(ctx context.Context, cdb *store.ClusterDB) (int, error) {
	var total int
	err := cdb.Reader().QueryRowContext(ctx,
		`SELECT (SELECT COUNT(*) FROM objects) + (SELECT COUNT(*) FROM events)`).Scan(&total)
	return total, err
}

// cachedKindCount totals the distinct kinds the cache already tracks — the prior
// run's kind_catalog, still present at a warm resume's start (discovery rebuilds
// it later, inside run) — for the ResyncStart "across M kinds" warm-cache size.
func cachedKindCount(ctx context.Context, cdb *store.ClusterDB) (int, error) {
	var n int
	err := cdb.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM kind_catalog`).Scan(&n)
	return n, err
}

// runLoop supervises run: any return while the engine is still wanted (a
// startup error, or discovery transiently yielding nothing) is abnormal —
// report Errored and retry with capped backoff until Stop.
func (e *Engine) runLoop(ctx context.Context) {
	backoff := e.backoffInit
	for {
		// Decide cold-vs-resume before anything repopulates the cache, so the Syncing
		// report carries ColdStart and run() reuses the same verdict for the
		// catch-up milestone.
		coldStart := true
		if has, err := cacheHasData(ctx, e.cdb); err != nil {
			slog.Warn("clustersync: read cache state", "id", e.cdb.ID(), "err", err)
		} else {
			coldStart = !has
		}
		// For a warm resume, read the warm cache's size so the ResyncStart event can
		// report what it's resuming from. A cold start's cache is empty, so no counts.
		var warmObjects, warmKinds int
		if !coldStart {
			warmObjects = e.countOrZero(ctx, cachedObjectCount, "count cached objects")
			warmKinds = e.countOrZero(ctx, cachedKindCount, "count cached kinds")
		}
		// Re-entering Syncing clears the previous run's catch-up facts so a later
		// snapshot can't carry stale counts; the warm-cache size (resume only) then
		// rides the ResyncStart report.
		e.report(func(s *EngineStatus) {
			s.State, s.LastError = EngineSyncing, ""
			s.clearCatchUp()
			s.ColdStart = coldStart
			s.SyncedObjects, s.SyncedKinds = warmObjects, warmKinds
		})
		err := e.run(ctx, coldStart)
		if ctx.Err() != nil {
			return
		}
		msg := "sync engine exited unexpectedly"
		if err != nil {
			msg = err.Error()
		}
		slog.Error("clustersync: engine run exited, retrying",
			"id", e.cdb.ID(), "err", err, "backoff", backoff)
		e.report(func(s *EngineStatus) { s.State, s.LastError = EngineErrored, msg })
		if e.sleep(ctx, backoff) != nil {
			return
		}
		backoff = stepBackoff(backoff, e.backoffMax)
	}
}

// driverSet tracks the live per-GVK drivers of one engine run. It lets the
// discovery reconciler tell which kinds are already mirrored (skip them), add
// drivers for newly-appeared kinds, and stop drivers for kinds that disappear, while
// the liveness monitor reads the current set and run() joins them all at shutdown.
// Each driver runs on its own context derived from the run's, so a kind removed
// mid-run (an uninstalled CRD) can be stopped individually — otherwise its wedged
// watch would keep the liveness monitor reporting the whole engine stale forever.
type driverSet struct {
	mu    sync.Mutex
	byGVK map[schema.GroupVersionKind]driverHandle
	wg    sync.WaitGroup
}

// driverHandle is a running driver plus the machinery to stop just it: cancel its
// context, wait for its goroutine to finish (done), and its catch-up token (retire —
// the once-guarded milestone obligation of an initial driver; nil for a mid-run driver,
// which owes nothing). remove returns the token so a caller can either fire it (a
// vanished kind: retire(true)) or transfer it to a replacement (a GVR repoint).
type driverHandle struct {
	driver *kindDriver
	cancel context.CancelFunc
	done   chan struct{}
	retire func(bool)
}

func newDriverSet() *driverSet {
	return &driverSet{byGVK: make(map[schema.GroupVersionKind]driverHandle)}
}

// launch registers d and starts its Run on a child of ctx, tracking it for wait().
// Called synchronously for the initial set, then from the discovery reconciler for
// new kinds. The child context lets remove() stop this one driver independently;
// retire (may be nil) is the driver's catch-up token, surfaced back through remove().
func (s *driverSet) launch(ctx context.Context, d *kindDriver, onExit func(error), retire func(bool)) {
	dctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.mu.Lock()
	s.byGVK[d.gvk] = driverHandle{driver: d, cancel: cancel, done: done, retire: retire}
	s.mu.Unlock()
	s.wg.Go(func() {
		defer close(done)
		onExit(d.Run(dctx))
	})
}

// has reports whether a driver for gvk is already running — the reconciler's skip test.
func (s *driverSet) has(gvk schema.GroupVersionKind) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byGVK[gvk]
	return ok
}

// gvr returns the resource endpoint the running driver for gvk watches (and whether
// one is running). The reconciler compares it against freshly-discovered GVRs so it
// can repoint a driver whose GVR changed under a stable GVK — a CRD recreated with a
// different resource plural — rather than leaving it watching a removed endpoint.
func (s *driverSet) gvr(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.byGVK[gvk]
	if !ok {
		return schema.GroupVersionResource{}, false
	}
	return h.driver.gvr, true
}

// remove stops the driver for gvk and drops it from the set. It is SYNCHRONOUS: it
// cancels the driver's context and blocks until its goroutine has fully returned
// (quiesced) before returning, so a caller can safely prune that kind's cached rows
// (or clear its resume cookie) afterward without an in-flight watch event racing it.
// It RETURNS the driver's catch-up token rather than firing it, so the caller controls
// its fate: a vanished kind fires it as gone (retire(true)) AFTER pruning, while a GVR
// repoint transfers the still-live token to the replacement so the milestone waits for
// its catch-up. Returns nil if gvk isn't running or the driver held no token.
func (s *driverSet) remove(gvk schema.GroupVersionKind) func(bool) {
	s.mu.Lock()
	h, ok := s.byGVK[gvk]
	delete(s.byGVK, gvk)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	h.cancel()
	<-h.done // quiesce: the driver writes nothing more once this returns
	return h.retire
}

// gvks returns the currently-running kinds — the reconciler diffs discovery against
// it to find kinds to remove.
func (s *driverSet) gvks() []schema.GroupVersionKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]schema.GroupVersionKind, 0, len(s.byGVK))
	for gvk := range s.byGVK {
		out = append(out, gvk)
	}
	return out
}

// snapshot returns the current drivers; the liveness monitor calls it each tick so
// a kind added mid-run is monitored too (and a removed one drops out).
func (s *driverSet) snapshot() []*kindDriver {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*kindDriver, 0, len(s.byGVK))
	for _, h := range s.byGVK {
		out = append(out, h.driver)
	}
	return out
}

// wait blocks until every launched driver's Run has returned.
func (s *driverSet) wait() { s.wg.Wait() }

// run blocks for one engine generation: build clients, walk discovery, and
// drive one kindDriver per discovered GVR until ctx is cancelled. The state
// flips to Watching once every driver in the initial discovery cohort has entered
// its watch phase (a kind discovered mid-run doesn't gate this — see EngineWatching).
// It also watches the objects that mint GVRs (CRDs, APIServices) so a kind installed
// mid-run starts mirroring without a restart (see watchDiscoveryTriggers / discoveryLoop).
func (e *Engine) run(ctx context.Context, coldStart bool) error {
	dc, err := discovery.NewDiscoveryClientForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	md, err := metadata.NewForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}

	writer := e.cdb.Writer()
	// The initial discovery starts drivers for whatever it found; a partial result
	// (some group down) is fine here — the next reconcile completes the set.
	entries, complete, err := discoverGVRs(ctx, dc, e.cdb)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	if len(entries) == 0 {
		// Zero drivers would make run block on nothing and report Watching
		// for a cluster that mirrors no data; treat as a transient failure.
		return errors.New("discovery returned no syncable resources")
	}
	// Evict cached objects for kinds uninstalled while the engine was down (a warm
	// resume). Safe here without quiescing — no drivers are running yet — and gated on
	// a complete discovery so a transiently-unavailable group isn't wrongly pruned.
	if complete {
		e.pruneOrphanedObjects(ctx, entries)
	}
	slog.Info("clustersync: discovered syncable GVRs on API server", "id", e.cdb.ID(), "count", len(entries))

	// coldStart was decided by runLoop before the drivers repopulate anything; stamp
	// the run's start so the catch-up milestone can report elapsed time.
	startedAt := e.now()

	// runCtx scopes the drivers, liveness monitor, and discovery watchers to this run:
	// it's cancelled when the run winds down even if the parent ctx hasn't, so none
	// of them outlives the run.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	var pending atomic.Int64
	pending.Store(int64(len(entries)))
	// reportedKinds is the catch-up milestone's kind count: it starts at the initial
	// cohort size and is decremented when an initial driver is removed before it ever
	// reaches its watch phase (an uninstalled CRD), so the SyncedKinds the report emits
	// reflects the kinds that actually synced, not the never-synced removed ones.
	var reportedKinds atomic.Int64
	reportedKinds.Store(int64(len(entries)))
	// Aggregate the per-driver re-sync work as each driver reaches its watch phase.
	// Each contributes its snapshot once (from its Run goroutine) into these atomics
	// rather than being read back from the driver struct, so a driver's later
	// full-resync retry can't race the aggregation or bleed into the first counts.
	var resyncedKinds, resyncedObjects atomic.Int64
	caughtUp := make(chan struct{})
	var caughtUpOnce sync.Once
	drivers := newDriverSet()

	// newRetire mints one catch-up token — a once-guarded closure that discharges an
	// initial driver's Syncing→Watching obligation exactly once. onWatch fires it as
	// synced (gone=false: the kind synced, so it stays in the reported cohort); a
	// removal fires it as gone (gone=true: it never synced, so it drops out of the
	// reported cohort). When pending hits zero the milestone flips to Watching. Each
	// initial kind owns exactly one token, so `pending`/`reportedKinds` are sized to the
	// cohort; a repoint TRANSFERS the token to the replacement rather than minting a new
	// one (see reconcileDiscovery), keeping the count conserved and the milestone gated
	// on the replacement's own catch-up.
	newRetire := func() func(bool) {
		var once sync.Once
		return func(gone bool) {
			once.Do(func() {
				if gone {
					reportedKinds.Add(-1)
				}
				if pending.Add(-1) == 0 {
					e.reportCaughtUp(runCtx, coldStart, int(reportedKinds.Load()), startedAt,
						int(resyncedKinds.Load()), int(resyncedObjects.Load()))
					caughtUpOnce.Do(func() { close(caughtUp) })
				}
			})
		}
	}

	// startDriver builds and launches one driver for a GVR. `retire` is the driver's
	// catch-up token (see newRetire), or nil for a kind that owes nothing — a kind
	// discovered mid-run joins AFTER catch-up, so it doesn't participate in the milestone
	// and just streams. A driver holding a token counts down Syncing→Watching and feeds
	// the re-sync aggregation when it reaches its watch phase.
	startDriver := func(entry gvrEntry, retire func(bool), forceCold bool) {
		var kstore kindStore
		if isEventGVK(entry.GVK) {
			kstore = newEventsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		} else {
			kstore = newObjectsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		}
		// Seed the driver from the kind's persisted resourceVersion so a wake resumes
		// instead of re-LISTing. It starts COLD (full LIST) when the caller forces it (a
		// GVR repoint, whose cookie is for the old endpoint), or when the store deems the
		// cookie ineligible (never synced, or it outlived the kind's cached data — see
		// kindStore.ResumeRV). That guard, plus the periodic resync backstop, is what makes
		// a stale cookie harmless — no exact cookie surgery on the removal/repoint paths.
		seedRV := ""
		if !forceCold {
			rv, err := kstore.ResumeRV(ctx)
			if err != nil {
				slog.Warn("clustersync: read resume rv, starting cold", "gvk", entry.GVK.String(), "err", err)
			}
			seedRV = rv
		}
		d := newKindDriverWithOptions(newLiveSource(dyn, md, entry), kstore, entry.GVK, seedRV,
			withNow(e.now), withGVR(entry.GVR), withResyncPeriod(e.resyncPeriod))
		// A tokenless driver (a mid-run discovery) can't participate in the milestone —
		// it just streams. The liveness monitor gives it a bounded startup grace (from
		// createdAt) before flagging it, via staleLaggards.
		if retire != nil {
			d.onWatch = func(resynced bool, objects int) {
				if resynced {
					resyncedKinds.Add(1)
					resyncedObjects.Add(int64(objects))
				}
				retire(false)
			}
		}
		slog.Debug("clustersync: starting driver", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "seedRV", seedRV)
		drivers.launch(runCtx, d, func(err error) {
			// A cancellation is an intentional stop — shutdown (runCtx) or a mid-run
			// removal (this driver's own child ctx) — not a fault, so don't warn on it.
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("clustersync: driver exited", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "err", err)
			}
		}, retire)
	}

	for _, entry := range entries {
		startDriver(entry, newRetire(), false)
	}

	var monWg sync.WaitGroup
	monWg.Go(func() { e.livenessMonitor(runCtx, drivers.snapshot, caughtUp) })

	// Detect GVRs that appear or vanish mid-run. Two layers feed one reconcile path:
	// (1) a best-effort ACCELERATOR — watches of the objects that mint GVRs
	// (CustomResourceDefinitions, APIServices) — signals a re-walk the moment one
	// changes, for low latency; and (2) the authoritative BACKSTOP — discoveryPollLoop
	// — re-walks on a fixed cadence regardless, so anything the watches miss (a denied
	// watch, a built-in API surface change with no watchable object) is still reconciled
	// within one poll interval. The trigger channel is a coalescing dirty-flag (cap 1);
	// the reconciler debounces it so a burst of CRDs collapses into one discovery pass.
	triggers := make(chan struct{}, 1)
	signal := func() {
		select {
		case triggers <- struct{}{}:
		default:
		}
	}
	sources := make([]kubeSource, 0, len(discoveryTriggerGVRs))
	for _, gvr := range discoveryTriggerGVRs {
		sources = append(sources, newLiveSource(dyn, md, gvrEntry{GVR: gvr}))
	}

	var watchWg sync.WaitGroup
	watchWg.Go(func() { e.watchDiscoveryTriggers(runCtx, sources, signal) })
	watchWg.Go(func() { e.discoveryPollLoop(runCtx, signal) })
	watchWg.Go(func() {
		e.discoveryLoop(runCtx, dc, drivers, startDriver, triggers)
	})

	// Block until shutdown. Drivers loop until their context is cancelled, so this
	// returns only when the parent ctx is done. The ordering then stops the discovery
	// watchers — the reconciler being the sole concurrent adder — BEFORE joining the
	// drivers, so no driver is launched into a set that's already being waited on.
	<-ctx.Done()
	runCancel()
	watchWg.Wait()
	drivers.wait()
	monWg.Wait()
	return ctx.Err()
}

// discoveryTriggerGVRs are the objects whose churn can change the set of served
// resources: a CustomResourceDefinition install/removal, or an APIService (an
// aggregated API server) coming online / going away. Watching these is how a live
// run learns about a new GVR without polling — /apis discovery itself isn't
// watchable, so we watch the things that mint GVRs and re-discover on a change.
var discoveryTriggerGVRs = []schema.GroupVersionResource{
	{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"},
	{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"},
}

// discoveryPollLoop is the pull-based discovery backstop: it fires a trigger on a
// fixed cadence so the reconciler re-walks discovery regardless of watch activity.
// The trigger watches (watchDiscoveryTriggers) make detection fast in the common
// case; this makes it CORRECT in every case — a kind that appeared or vanished while
// a trigger watch was denied, backed off, or blind to the change (a built-in API
// surface change has no watchable object) is still reconciled within one interval. It
// shares the coalescing trigger channel, so a poll that coincides with watch activity
// collapses into one pass.
func (e *Engine) discoveryPollLoop(ctx context.Context, signal func()) {
	ticker := time.NewTicker(e.discoveryPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			signal()
		}
	}
}

// discoveryLoop reconciles discovery whenever a trigger fires (from a trigger watch or
// the poll backstop), so a GVR that appeared or vanished after startup — a freshly-
// applied CRD, an aggregated API coming online, an uninstalled CRD — is reflected in
// the running driver set without an engine restart. Each pass re-runs discoverGVRs
// (which also refreshes kind_catalog, notifying the dashboard-nav watch) via
// reconcileDiscovery.
//
// A trigger is debounced by a trailing quiet window so a bundle of CRDs installed
// together collapses into one discovery pass. A trigger that lands during a pass is
// held by the cap-1 channel and reconciled on the next iteration.
func (e *Engine) discoveryLoop(ctx context.Context, dc discovery.DiscoveryInterface, drivers *driverSet, startDriver func(gvrEntry, func(bool), bool), triggers <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-triggers:
			if !e.debounceTriggers(ctx, triggers) {
				return
			}
			e.reconcileDiscovery(ctx, dc, drivers, startDriver)
		}
	}
}

// resettableTimer is the Stop/Reset timer debounceTriggers drives, abstracted so a
// test can fire the trailing window deterministically. realResettableTimer wraps a
// *time.Timer; its contract mirrors the standard library's exactly.
type resettableTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

func realResettableTimer(d time.Duration) resettableTimer { return realTimer{time.NewTimer(d)} }

// debounceTriggers waits out the trailing debounce window, RESETTING it on every
// further trigger so a burst reconciles once — discoveryDebounce after the LAST
// trigger, not a fixed delay from the first. So an operator applying a long CRD bundle
// collapses into a single discovery pass after the burst ends instead of one expensive
// /apis walk + catalog rewrite mid-burst. Returns false if ctx was cancelled while
// waiting (the caller should stop).
func (e *Engine) debounceTriggers(ctx context.Context, triggers <-chan struct{}) bool {
	timer := e.newDebounceTimer(e.discoveryDebounce)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-triggers:
			// Another trigger in the same burst — restart the quiet window. Stop-and-drain
			// before Reset so a timer that fired between the select and here can't leave a
			// stale tick in timer.C().
			if !timer.Stop() {
				<-timer.C()
			}
			timer.Reset(e.discoveryDebounce)
		case <-timer.C():
			return true
		}
	}
}

// reconcileDiscovery re-walks discovery ONCE and reconciles the running drivers toward
// it: launch a driver for every served GVR not already running, repoint one whose GVR
// changed (a CRD recreated under a new resource plural), and — on a COMPLETE pass — stop
// the driver for every kind that no longer exists and prune its cached rows.
//
// It is a SINGLE PASS with no internal retry. A discovery error or a PARTIAL result
// (some API group transiently down) just launches what it saw and returns; the next
// trigger or the discovery poll re-walks. Removal and the object prune gate on a COMPLETE
// result so a momentarily-unavailable group doesn't tear down and re-add live drivers
// (churn) — but correctness never rides on any one pass being exact: the poll backstop
// re-derives the driver set on a cadence, and the per-driver periodic resync re-derives
// object state, so anything a pass drops self-heals.
//
// A repoint removes the old driver and relaunches COLD (forceCold): the replacement
// full-LISTs the new endpoint, and the old cookie is left inert — the resume-eligibility
// guard and periodic resync make a stale cookie harmless, so there is no cookie surgery.
func (e *Engine) reconcileDiscovery(ctx context.Context, dc discovery.DiscoveryInterface, drivers *driverSet, startDriver func(gvrEntry, func(bool), bool)) {
	entries, complete, err := discoverGVRs(ctx, dc, e.cdb)
	if err != nil {
		if ctx.Err() == nil {
			slog.Debug("clustersync: mid-run discovery failed; next trigger or poll retries", "id", e.cdb.ID(), "err", err)
		}
		return
	}
	discovered := make(map[schema.GroupVersionKind]struct{}, len(entries))
	for _, entry := range entries {
		discovered[entry.GVK] = struct{}{}
		cur, running := drivers.gvr(entry.GVK)
		switch {
		case !running:
			// A kind discovered mid-run joins AFTER catch-up, so it owes no catch-up
			// obligation — launch it with a nil token.
			slog.Info("clustersync: new GVR discovered mid-run", "id", e.cdb.ID(), "gvk", entry.GVK.String())
			startDriver(entry, nil, false)
		case cur != entry.GVR:
			// Same kind, different resource endpoint (a CRD recreated under a new resource
			// plural): the running driver still watches the removed GVR, so stop it and
			// relaunch COLD against the new endpoint. remove blocks until the old goroutine
			// stops (so nothing re-writes the cookie below).
			slog.Info("clustersync: GVR changed mid-run, restarting driver",
				"id", e.cdb.ID(), "gvk", entry.GVK.String(), "from", cur.String(), "to", entry.GVR.String())
			retire := drivers.remove(entry.GVK)
			// Durably invalidate the resume cookie before relaunching. The old rows survive
			// the repoint (same GVK, so the prune keeps them), so without this a restart in
			// the relaunch window would lose the in-process forceCold, and ResumeRV — seeing
			// the surviving rows + cookie — would resume the new endpoint from the old
			// cluster-global RV and skip the initial LIST until the periodic resync. The
			// delete makes the cold start durable; forceCold keeps this run cold even if it
			// fails (best-effort — a failed delete then a crash falls back to the resync).
			if err := deleteResumeRV(ctx, e.cdb.Writer(), entry.GVK); err != nil {
				slog.Warn("clustersync: clear resume cookie on repoint", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "err", err)
			}
			// TRANSFER the old driver's catch-up token to the replacement rather than firing
			// it: if the old driver was an initial one still pre-watch, its Syncing→Watching
			// obligation carries over so the milestone waits for the replacement's own full
			// LIST instead of flipping to Watching while the old endpoint's rows are still
			// present. A spent (already caught up) or nil (mid-run) token transfers harmlessly.
			startDriver(entry, retire, true)
		}
	}
	if !complete {
		return // partial: don't remove/prune; the next pass (trigger or poll) completes it
	}
	// A complete pass is authoritative. Stop the drivers for vanished kinds (each remove
	// blocks until the goroutine stops), then prune their cached rows and resume cookies.
	// Fire each removed driver's catch-up token as gone AFTER the prune, so a removal that
	// closes out the milestone can't report catch-up while stale rows are still present.
	var retires []func(bool)
	for _, gvk := range drivers.gvks() {
		if _, ok := discovered[gvk]; !ok {
			slog.Info("clustersync: GVR removed mid-run, stopping driver", "id", e.cdb.ID(), "gvk", gvk.String())
			if retire := drivers.remove(gvk); retire != nil {
				retires = append(retires, retire)
			}
		}
	}
	e.pruneOrphanedObjects(ctx, entries)
	for _, retire := range retires {
		retire(true)
	}
}

// watchDiscoveryTriggers watches each discovery-trigger source (CRDs, APIServices)
// concurrently, calling signal whenever one changes so the reconciler re-walks
// discovery. It returns when ctx is cancelled. This is the best-effort accelerator: if
// a source can't be watched (e.g. missing RBAC on CRDs) its watcher just backs off, and
// mid-run detection for that source falls to the discovery poll — the authoritative
// backstop — rather than being lost until the engine restarts.
func (e *Engine) watchDiscoveryTriggers(ctx context.Context, sources []kubeSource, signal func()) {
	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Go(func() { e.watchTriggerSource(ctx, src, signal) })
	}
	wg.Wait()
}

// watchTriggerSource keeps one trigger object kind (a CRD or APIService) under watch
// and signals a re-discovery on each delta — the low-latency ACCELERATOR. It carries no
// correctness obligation: the discovery poll re-walks on a cadence regardless, so this
// only needs to make the common case fast. It LISTs for a fresh resourceVersion, then
// runs a RetryWatcher from it (bookmarks keep the resume point current); on any end it
// re-LISTs and re-watches. It signals ONLY on a delta — a denied watch, a 410, a plain
// reconnect, or a close are left to the poll, so there is no denial/recovery/startup-gap
// state to track. It backs off when a watch never delivered anything (a list-allowed/
// watch-denied RBAC split terminates without progress) so the re-LIST+re-watch can't spin
// the API server, and resets on a delivering watch.
func (e *Engine) watchTriggerSource(ctx context.Context, src kubeSource, signal func()) {
	backoff := e.backoffInit
	for {
		if ctx.Err() != nil {
			return
		}
		_, rv, err := src.List(ctx, metav1.ListOptions{})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Debug("clustersync: discovery-trigger list failed, backing off", "id", e.cdb.ID(), "err", err)
			if e.sleep(ctx, backoff) != nil {
				return
			}
			backoff = stepBackoff(backoff, e.backoffMax)
			continue
		}
		progressed, err := e.streamTriggerEvents(ctx, src, rv, signal)
		if err != nil {
			return // ctx cancelled
		}
		if progressed {
			backoff = e.backoffInit // a delivering watch — reset
			continue
		}
		// Nothing delivered (e.g. watch-denied RBAC, or a clean idle close): back off so
		// the re-LIST+re-watch doesn't hot-loop. The poll covers anything missed meanwhile.
		if e.sleep(ctx, backoff) != nil {
			return
		}
		backoff = stepBackoff(backoff, e.backoffMax)
	}
}

// streamTriggerEvents runs a RetryWatcher from rv and signals on each object delta.
// Bookmarks are consumed by RetryWatcher (not forwarded), so a quiet source produces no
// spurious signals while its resume point still advances. It returns (progressed, err):
// err is ctx.Err() on cancellation; otherwise the watch ended and the caller re-LISTs.
// progressed = a delta arrived, which the caller uses only to reset its backoff — a 410,
// a denial, and a clean close are all just "watch ended, re-LIST" (the discovery poll,
// not this watch, is the backstop for anything they skip).
func (e *Engine) streamTriggerEvents(ctx context.Context, src kubeSource, rv string, signal func()) (progressed bool, err error) {
	watcher := watcherFor(src, func() {}, func(watch.Event) {})
	rw, err := toolswatch.NewRetryWatcherWithContext(ctx, rv, watcher)
	if err != nil {
		return false, nil // unusable RV; caller re-LISTs for a fresh one
	}
	defer rw.Stop()
	for {
		select {
		case <-ctx.Done():
			return progressed, ctx.Err()
		case ev, ok := <-rw.ResultChan():
			if !ok {
				return progressed, nil
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				progressed = true
				signal()
			}
		}
	}
}

// livenessMonitor flags the cache stale once a driver stops proving its watch is
// alive past staleThreshold, and recovers once liveness returns. It only judges
// staleness after catch-up (before that the engine is legitimately Syncing). It
// re-reports whenever the wedged set changes — not just on the healthy/stale edge —
// so a multi-kind cache never keeps naming a recovered kind (or omits a newly-wedged
// one) while stale overall; an unchanged set doesn't re-emit every tick. The
// recovery report carries no catch-up counts (a liveness resume, not a fresh sync),
// which the controller renders as "watch recovered".
func (e *Engine) livenessMonitor(ctx context.Context, drivers func() []*kindDriver, caughtUp <-chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-caughtUp:
	}
	ticker := time.NewTicker(e.staleCheckInterval)
	defer ticker.Stop()
	// reported is the laggard set last published as stale; nil while watching. A
	// non-nil, non-equal set means the wedged kinds shifted and must be re-reported.
	var reported []string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			laggards := staleLaggards(drivers(), e.now(), e.staleThreshold)
			switch {
			case len(laggards) > 0 && !slices.Equal(laggards, reported):
				reported = laggards
				e.report(func(s *EngineStatus) {
					s.State, s.LastError, s.StaleKinds = EngineStale, "", laggards
				})
			case len(laggards) == 0 && reported != nil:
				reported = nil
				e.report(func(s *EngineStatus) {
					s.State = EngineWatching
					s.clearCatchUp()
				})
			}
		}
	}
}

// staleLaggards names the kinds whose watch hasn't proven liveness within
// threshold. Liveness is a delta, a bookmark, or a reconnect (each stamps the
// driver), so a genuinely quiet-but-healthy kind — still receiving the server's
// periodic bookmarks — never appears here; only a silently-wedged watch does.
//
// A driver that hasn't entered its watch phase yet (zero liveAt) is judged against
// its createdAt instead: it gets a startup grace of `threshold` to connect — its
// initial sync or connection attempts may legitimately take a while — but once that
// grace expires it IS flagged, so a kind that can never LIST/watch (forbidden or a
// perpetually-unavailable API) surfaces as stale rather than hiding the engine as
// healthy forever. The initial cohort never reaches this branch: it's shielded by the
// caughtUp gate (the monitor starts only once every initial driver has reached watch
// phase, so they all have a non-zero liveAt); only a kind discovered mid-run, which
// has no such gate, relies on the createdAt grace.
//
// The result is sorted: the driver set is a map (random iteration order), so an
// unsorted slice would let an unchanged laggard set permute between snapshots and
// defeat livenessMonitor's slices.Equal dedup — re-emitting the same stale report
// with reordered kind names.
func staleLaggards(drivers []*kindDriver, now time.Time, threshold time.Duration) []string {
	var laggards []string
	for _, d := range drivers {
		// Judge a never-watched driver from creation, a watched one from its last
		// liveness proof; either way it's stale once `threshold` has elapsed.
		since := d.liveAt()
		if since.IsZero() {
			since = d.createdAt
		}
		if now.Sub(since) > threshold {
			laggards = append(laggards, d.gvk.Kind)
		}
	}
	sort.Strings(laggards)
	return laggards
}

// freshnessLoop tracks when the cache last received data (via the ClusterDB's
// write pings) and flushes the timestamp through the sink on a coarse cadence
// — per-write reporting would turn every watch delta into a store write.
func (e *Engine) freshnessLoop(ctx context.Context, pings <-chan struct{}) {
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	var last time.Time
	dirty := false
	flush := func() {
		if !dirty {
			return
		}
		dirty = false
		at := last
		e.report(func(s *EngineStatus) { s.LastSyncedAt = &at })
	}
	for {
		select {
		case <-ctx.Done():
			// A ping delivered just before shutdown may still sit in the
			// subscription's slot; drain it so the final flush carries it.
			select {
			case _, ok := <-pings:
				if ok {
					last = e.now().UTC()
					dirty = true
				}
			default:
			}
			flush() // the final stamp must not be lost to the 30s cadence
			return
		case _, ok := <-pings:
			if !ok {
				flush()
				return
			}
			last = e.now().UTC()
			dirty = true
		case <-ticker.C:
			flush()
		}
	}
}

// --- credential fingerprinting (the sync controller's restart trigger) ---

// ConfigFingerprint hashes the connection- and credential-relevant fields of a
// rest.Config. Two configs with the same fingerprint connect and authenticate
// identically, so the running drivers need no restart; a different fingerprint
// (rotated token, new cert, changed CA/server URL, or an edited exec/auth-provider/
// impersonation block) means the open engine is on stale config and must restart.
// We hash the *static* exec/auth-provider config — runtime token minting is the
// transport's job, but editing how tokens are obtained must invalidate it.
//
// proxyURL is the kubeconfig cluster's proxy-url. clientcmd compiles it into
// rest.Config.Proxy (an opaque func we can't hash), so the caller passes the raw
// string (ContextProxyURL): a changed proxy must restart the drivers even when
// every other field is identical.
func ConfigFingerprint(cfg *rest.Config, proxyURL string) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// NUL-separate every field so boundaries can't be aliased by concatenation.
	write := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	writeBytes := func(b []byte) { h.Write(b); h.Write([]byte{0}) }

	write(proxyURL)

	t := cfg.TLSClientConfig
	for _, s := range []string{
		cfg.Host, cfg.APIPath, cfg.Username, cfg.Password,
		cfg.BearerToken, cfg.BearerTokenFile,
		t.ServerName, t.CAFile, t.CertFile, t.KeyFile,
		strconv.FormatBool(t.Insecure),
	} {
		write(s)
	}
	writeBytes(t.CAData)
	writeBytes(t.CertData)
	writeBytes(t.KeyData)

	// Impersonation.
	im := cfg.Impersonate
	write(im.UserName)
	write(im.UID)
	for _, g := range im.Groups {
		write(g)
	}
	for _, k := range sortedKeys(im.Extra) {
		write(k)
		for _, v := range im.Extra[k] {
			write(v)
		}
	}

	// Auth-provider plugin (name + static config).
	if ap := cfg.AuthProvider; ap != nil {
		write(ap.Name)
		for _, k := range sortedKeys(ap.Config) {
			write(k)
			write(ap.Config[k])
		}
	}

	// Exec credential plugin (command/args/env/apiVersion).
	if ep := cfg.ExecProvider; ep != nil {
		write(ep.Command)
		write(ep.APIVersion)
		for _, a := range ep.Args {
			write(a)
		}
		for _, e := range ep.Env {
			write(e.Name)
			write(e.Value)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ContextProxyURL returns the proxy-url of the cluster a kubeconfig context
// points at, or "" if the context, its cluster, or the field is absent. The
// sync controller folds it into the config fingerprint because clientcmd
// compiles it into rest.Config.Proxy, an opaque func the fingerprint can't
// otherwise see.
func ContextProxyURL(cfg *api.Config, ctxName string) string {
	ctx, ok := cfg.Contexts[ctxName]
	if !ok || ctx == nil {
		return ""
	}
	cluster, ok := cfg.Clusters[ctx.Cluster]
	if !ok || cluster == nil {
		return ""
	}
	return cluster.ProxyURL
}

// sortedKeys returns a map's keys in deterministic order, so hashing a map
// doesn't depend on Go's randomized iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- discovery ------------------------------------------------------------

// gvrEntry packages everything needed to start one driver.
type gvrEntry struct {
	GVR        schema.GroupVersionResource
	GVK        schema.GroupVersionKind
	Namespaced bool
	IsCRD      bool
}

// Group prefixes whose resources we never want to mirror. They expose
// query APIs (metrics) or replicate state we already capture elsewhere
// (events.k8s.io duplicates core/v1 Event semantics; we keep core/v1
// only to avoid double-counting).
var skipGroups = map[string]bool{
	"metrics.k8s.io":          true,
	"external.metrics.k8s.io": true,
	"custom.metrics.k8s.io":   true,
	"events.k8s.io":           true,
}

// Specific (group, resource) entries to skip even though they pass the generic
// filters. v1 Endpoints — deprecated in k8s 1.33+ for discovery.k8s.io/v1
// EndpointSlice, which we already mirror — holds the same data, so keeping both
// wastes a watch and draws a deprecation warning on every LIST.
var skipResources = map[string]map[string]bool{
	"": {"endpoints": true}, // core/v1 endpoints
}

func isEventGVK(g schema.GroupVersionKind) bool {
	return g.Kind == "Event" && (g.Group == "" || g.Group == "events.k8s.io")
}

// discoverGVRs walks /apis, returns one entry per list/watchable resource
// (preferred version only), and populates kind_catalog so the agent (and UI) can
// ask "what kinds exist on this cluster?" without re-doing discovery. The second
// return reports whether discovery was complete (all API groups answered) — false on
// a partial result, which a caller must not treat as authoritative for removals
// (an omitted kind may just be in a transiently-unavailable group).
//
// It uses ServerPreferredResources, not ServerGroupsAndResources: the latter
// returns every version of every resource, which would start one driver per
// (resource, version) — duplicating watches and drawing deprecation warnings on
// every alpha/beta version. Preferred-only gives one driver per logical resource.
func discoverGVRs(ctx context.Context, dc discovery.DiscoveryInterface, cdb *store.ClusterDB) ([]gvrEntry, bool, error) {
	writer := cdb.Writer()
	complete := true
	lists, err := dc.ServerPreferredResources()
	if err != nil {
		if len(lists) == 0 {
			// Nothing usable came back. Returning zero entries would start no drivers
			// yet report success, leaving an open cluster that never mirrors data;
			// fail so the sync surfaces the error.
			return nil, false, fmt.Errorf("discovery returned no resources: %w", err)
		}
		// Partial discovery errors are common when an aggregated API server is down;
		// the returned lists are still usable, so log and continue — but mark
		// discovery incomplete so the prune below doesn't evict a kind that's merely
		// in a transiently-unavailable group.
		complete = false
		slog.Warn("clustersync: partial discovery", "err", err)
	}

	// Pull the CRD list once so kind_catalog can record CRD schemas. It is the ONLY
	// source of is_crd + schema_json, so a transient failure here (the apiextensions
	// read fails even though ServerPreferredResources succeeded) must not rewrite
	// every CRD as is_crd=0 with no schema, erasing previously valid metadata. When
	// it fails we DON'T mark the pass incomplete — kind existence comes from
	// ServerPreferredResources, so pruning uninstalled kinds stays correct even on a
	// cluster that never grants CRD read (RBAC); instead we backfill each row's CRD
	// metadata from the existing catalog below (see crdListOK) so the rewrite keeps
	// it until a pass with a good CRD list makes those columns authoritative again.
	crds, crdErr := listCRDs(ctx, dc)
	crdListOK := crdErr == nil
	if crdErr != nil {
		slog.Warn("clustersync: list CRDs failed; preserving existing CRD metadata", "err", crdErr)
	}

	var out []gvrEntry
	type catalogRow struct {
		apiVersion, kind, resource, scope string
		isCRD                             bool
		schemaJSON                        string
	}
	var catalog []catalogRow

	for _, list := range lists {
		if list == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if skipGroups[gv.Group] {
			continue
		}
		for _, r := range list.APIResources {
			// Skip subresources (pods/status, pods/exec, ...).
			if strings.Contains(r.Name, "/") {
				continue
			}
			if !hasVerb(r.Verbs, "list") || !hasVerb(r.Verbs, "watch") {
				continue
			}
			if skipResources[gv.Group][r.Name] {
				continue
			}
			gvk := schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: r.Kind}
			gvr := gv.WithResource(r.Name)
			isCRD := isCRDKind(crds, gvk)
			out = append(out, gvrEntry{GVR: gvr, GVK: gvk, Namespaced: r.Namespaced, IsCRD: isCRD})

			scope := "Cluster"
			if r.Namespaced {
				scope = "Namespaced"
			}
			schemaJSON := ""
			if isCRD {
				schemaJSON = crdSchemaJSON(crds, gvk)
			}
			catalog = append(catalog, catalogRow{
				apiVersion: gv.String(), kind: r.Kind, resource: r.Name,
				scope: scope, isCRD: isCRD, schemaJSON: schemaJSON,
			})
		}
	}

	// Persist the kind catalog. On a COMPLETE discovery the catalog is authoritative,
	// so truncate-and-rewrite to reflect exactly what exists now (dropping uninstalled
	// kinds). On a PARTIAL result, do NOT truncate: the momentarily-unavailable group's
	// kinds are absent from `catalog`, and wiping them would make the dashboard lose
	// them (potentially for a long time); upsert only, preserving the prior rows until a
	// complete pass makes the catalog authoritative again.
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return out, complete, err
	}
	defer tx.Rollback() //nolint:errcheck
	// When the CRD list was unavailable, backfill each row's is_crd/schema_json from
	// the existing catalog (read BEFORE the truncate below) so the rewrite preserves
	// it rather than resetting every CRD to is_crd=0 with no schema. Kinds not already
	// known default to is_crd=0 and self-correct on the next pass with a good CRD list.
	if !crdListOK {
		prior, err := loadCRDMetadata(ctx, tx)
		if err != nil {
			return out, complete, err
		}
		for i := range catalog {
			if m, ok := prior[[2]string{catalog[i].apiVersion, catalog[i].kind}]; ok {
				catalog[i].isCRD, catalog[i].schemaJSON = m.isCRD, m.schemaJSON
			}
		}
	}
	if complete {
		if _, err := tx.ExecContext(ctx, `DELETE FROM kind_catalog`); err != nil {
			return out, complete, err
		}
	}
	for _, c := range catalog {
		isCRDInt := 0
		if c.isCRD {
			isCRDInt = 1
		}
		var schemaArg any
		if c.schemaJSON != "" {
			schemaArg = c.schemaJSON
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, ?, ?)`,
			c.apiVersion, c.kind, c.resource, c.scope, isCRDInt, schemaArg); err != nil {
			return out, complete, err
		}
	}
	if err := tx.Commit(); err != nil {
		return out, complete, err
	}

	// Object eviction for uninstalled kinds is NOT done here: the caller must first
	// quiesce the removed kinds' drivers so an in-flight watch event can't reinsert a
	// row after the prune (see pruneOrphanedObjects, called by run/reconcileDiscovery).

	// Signal catalog subscribers (e.g. ClusterDataKindsWatch). The catalog rewrite
	// doesn't go through the per-object stores that ping on write, so without this a
	// kind added/removed since the last run — most visibly a CRD uninstalled during an
	// in-place restart, where the db handle doesn't change — would stay stale until the
	// next unrelated write pinged.
	cdb.Notify()

	return out, complete, nil
}

// crdMeta is the CRD-derived portion of a kind_catalog row (its only two columns
// that come from the CRD list, not from /apis discovery).
type crdMeta struct {
	isCRD      bool
	schemaJSON string
}

// loadCRDMetadata reads the existing is_crd/schema_json for every catalogued kind,
// keyed by [api_version, kind]. discoverGVRs uses it to carry CRD metadata across a
// catalog rewrite when the CRD list was momentarily unavailable, so a transient
// apiextensions read failure doesn't erase previously valid CRD schemas.
func loadCRDMetadata(ctx context.Context, tx *sql.Tx) (map[[2]string]crdMeta, error) {
	rows, err := tx.QueryContext(ctx, `SELECT api_version, kind, is_crd, schema_json FROM kind_catalog`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := make(map[[2]string]crdMeta)
	for rows.Next() {
		var apiVersion, kind string
		var isCRD int
		var schemaJSON sql.NullString
		if err := rows.Scan(&apiVersion, &kind, &isCRD, &schemaJSON); err != nil {
			return nil, err
		}
		out[[2]string{apiVersion, kind}] = crdMeta{isCRD: isCRD != 0, schemaJSON: schemaJSON.String}
	}
	return out, rows.Err()
}

// pruneOrphanedObjects evicts the cached state of kinds not in `entries` — a kind that
// no longer exists on the cluster, chiefly an uninstalled CRD / removed APIService,
// which nothing else reaps. It sweeps two kinds of stale state: the cached object rows
// (via pruneOrphanedKinds) AND the kind's resume cookie (via pruneOrphanedResumeCookies)
// — the latter because a re-registered GVR whose server kept its objects would otherwise
// resume from the stale RV and skip its initial LIST, leaving the cache permanently
// empty for that kind. Callers must invoke it ONLY after a complete discovery AND only
// once the removed kinds' drivers have stopped (drivers.remove quiesces them), so an
// in-flight watch event can't reinsert a row (or re-write a cookie) after the prune.
// Non-fatal on error — a failed prune just leaves stale state for the next complete pass.
func (e *Engine) pruneOrphanedObjects(ctx context.Context, entries []gvrEntry) {
	keep := make(map[kindKey]struct{}, len(entries))
	keepCookies := make(map[string]struct{}, 2*len(entries))
	for _, en := range entries {
		gv := schema.GroupVersion{Group: en.GVK.Group, Version: en.GVK.Version}
		keep[kindKey{kind: en.GVK.Kind, apiVersion: gv.String()}] = struct{}{}
		keepCookies[lastListRVKey(en.GVK)] = struct{}{}
		keepCookies[lastListAtKey(en.GVK)] = struct{}{}
	}
	n, err := pruneOrphanedKinds(ctx, e.cdb.Writer(), keep)
	if err != nil {
		slog.Warn("clustersync: prune orphaned kinds failed", "id", e.cdb.ID(), "err", err)
		return
	}
	if n > 0 {
		slog.Info("clustersync: pruned orphaned objects", "id", e.cdb.ID(), "rows", n)
		e.cdb.Notify() // the raw DELETE bypasses the per-object store ping
	}
	// Sweep resume cookies too, so a re-added GVR can't resume-skip its initial LIST.
	if c, err := pruneOrphanedResumeCookies(ctx, e.cdb.Writer(), keepCookies); err != nil {
		slog.Warn("clustersync: prune orphaned resume cookies failed", "id", e.cdb.ID(), "err", err)
	} else if c > 0 {
		slog.Info("clustersync: pruned orphaned resume cookies", "id", e.cdb.ID(), "rows", c)
	}
}

func hasVerb(vs []string, want string) bool {
	return slices.Contains(vs, want)
}

// listCRDs fetches the cluster's CRDs so we can tag kind_catalog and
// record OpenAPI schemas. Returns an empty list (not an error) if the
// caller lacks RBAC for apiextensions — most resources are still
// discoverable; we just won't know which are CRDs vs built-ins.
func listCRDs(ctx context.Context, dc discovery.DiscoveryInterface) ([]apiextensionsv1.CustomResourceDefinition, error) {
	// The discovery client doesn't expose CRDs directly; reach the REST
	// client under it to read apiextensions.k8s.io.
	type restable interface {
		RESTClient() rest.Interface
	}
	rc, ok := dc.(restable)
	if !ok {
		return nil, nil
	}
	raw, err := rc.RESTClient().Get().AbsPath("/apis/apiextensions.k8s.io/v1/customresourcedefinitions").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var out apiextensionsv1.CustomResourceDefinitionList
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

func isCRDKind(crds []apiextensionsv1.CustomResourceDefinition, gvk schema.GroupVersionKind) bool {
	for _, c := range crds {
		if c.Spec.Group != gvk.Group {
			continue
		}
		if c.Spec.Names.Kind != gvk.Kind {
			continue
		}
		for _, v := range c.Spec.Versions {
			if v.Name == gvk.Version {
				return true
			}
		}
	}
	return false
}

func crdSchemaJSON(crds []apiextensionsv1.CustomResourceDefinition, gvk schema.GroupVersionKind) string {
	for _, c := range crds {
		if c.Spec.Group != gvk.Group || c.Spec.Names.Kind != gvk.Kind {
			continue
		}
		for _, v := range c.Spec.Versions {
			if v.Name == gvk.Version && v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
				b, _ := json.Marshal(v.Schema.OpenAPIV3Schema)
				return string(b)
			}
		}
	}
	return ""
}
