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
	// EngineWatching means the initial discovery cohort has all resolved — each driver
	// either entered its watch phase OR exhausted its resync error budget (stuck) — and
	// the cache is streaming deltas for the kinds that synced. A stuck initial kind
	// doesn't wedge the engine in Syncing: it retires its milestone token so the engine
	// reaches Watching for the healthy kinds, and the liveness monitor then surfaces the
	// stuck kind as EngineStale (naming it). A kind discovered mid-run joins WITHOUT
	// regressing this state — it doesn't gate the milestone — and likewise surfaces as
	// EngineStale if it gets stuck, so a kind that can't LIST/watch is never a
	// silently-Watching gap.
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

// StaleCause categorizes WHY a single kind isn't current — the per-kind detail behind
// an EngineStale report. It rides in KindStatus so the controller's Stale message can
// name the actual failure per kind ("Pods: watch failed") instead of a bare list, which
// is what makes a list-but-not-watch RBAC gap actionable. It stays per-kind: the
// cache-wide condition reason is the coarse honest Stale; the cause lives in the detail.
type StaleCause string

const (
	// CauseListFailed: the kind can't complete a full LIST — RBAC `list` denied, or a
	// kind too large to paginate within the continue-token lifetime. It has no data, or
	// data that can no longer be refreshed.
	CauseListFailed StaleCause = "ListFailed"
	// CauseWatchFailed: the kind can LIST but can't establish or keep a watch — RBAC
	// `watch` denied, or an aggregated API that rejects watch. It has a snapshot but no
	// live updates.
	CauseWatchFailed StaleCause = "WatchFailed"
	// CauseWatchStalled: the watch was live and went quiet past the freshness threshold
	// (wedged) without the driver's error budget being spent. Usually transient.
	CauseWatchStalled StaleCause = "WatchStalled"
)

// KindStatus names one not-current kind and why — the per-kind element of an
// EngineStale report's StaleKinds. Comparable (all string fields), so a StaleKinds
// slice compares by value for livenessMonitor's dedup.
type KindStatus struct {
	Kind  string
	Cause StaleCause
}

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

	// StaleKinds names the kinds that aren't current, each with its cause — a watch
	// that went quiet past the threshold (CauseWatchStalled) or a kind stuck after
	// exhausting its resync error budget (CauseListFailed / CauseWatchFailed, see
	// staleLaggards). Set only on EngineStale reports, zero otherwise.
	StaleKinds []KindStatus
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

	// listConcurrency bounds how many drivers may run a full LIST/resync at once across
	// one engine run. A cold driver goes straight to a full (paginated) LIST, so without
	// a bound cold-start peak memory scales with the kind count — the sum of every kind's
	// in-flight list pages at once. The limiter caps that to N drivers listing
	// concurrently (the rest queue), making cold-start peak O(N pages) regardless of how
	// many kinds the cluster serves — trading a little cold-start latency for a flat
	// memory ceiling. Initial AND mid-run-discovered drivers share the one limiter (both
	// go through the same startDriver), so a CRD bundle installing mid-run can't blow the
	// ceiling either, and the periodic resync backstop is bounded by it too.
	//
	// 16 balances the two costs: peak memory (and API-server burst) grows linearly with
	// it, while cold-start latency barely improves past it — a typical cluster's ~50-80
	// kinds are mostly empty (they LIST in one fast page), so the long pole is a handful
	// of large kinds we want bounded for memory anyway. With listPageSize (500) bodies
	// in flight per slot, 16 keeps the ceiling to tens of MB while clearing the small
	// kinds in a couple of waves.
	listConcurrency = 16
)

// listLimiter bounds concurrent full LISTs across a run's drivers (see
// listConcurrency). It is a counting semaphore over a buffered channel; a nil limiter
// imposes no bound (drivers built directly in unit tests).
type listLimiter chan struct{}

func newListLimiter(n int) listLimiter { return make(listLimiter, n) }

// acquire takes a slot, blocking until one is free or ctx is cancelled. It returns a
// release func that returns the slot; the func is a safe no-op on a nil limiter or a
// cancelled acquire, so callers can `defer release()` unconditionally.
func (l listLimiter) acquire(ctx context.Context) (release func(), err error) {
	if l == nil {
		return func() {}, nil
	}
	select {
	case <-ctx.Done():
		return func() {}, ctx.Err()
	case l <- struct{}{}:
		return func() { <-l }, nil
	}
}

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

// reportCaughtUp emits the caught-up milestone — every initial driver is watching.
// syncedKinds is the count of kinds that actually REACHED a watch (the milestone's live
// tally, incremented per driver in onWatch), not a cache query: at milestone time
// kind_catalog is NOT the synced set — a partial initial discovery preserves catalog rows
// for unavailable API groups whose drivers never started, and a mid-run discovery can add a
// tokenless kind before the initial cohort finishes, both of which would inflate a
// COUNT(*). For a cold build it also QUERIES the object total (a count failure is logged and
// reported as zero). It carries the drivers' aggregated re-sync breakdown so the controller
// can compose the SyncComplete/ResyncComplete message.
func (e *Engine) reportCaughtUp(ctx context.Context, coldStart bool, startedAt time.Time, syncedKinds, resyncedKinds, resyncedObjects int) {
	objects := 0
	if coldStart {
		objects = e.countOrZero(ctx, cachedObjectCount, "count cached objects")
	}
	elapsed := e.now().Sub(startedAt)
	e.report(func(s *EngineStatus) {
		s.State, s.LastError = EngineWatching, ""
		s.ColdStart = coldStart
		s.SyncedKinds = syncedKinds
		s.SyncedObjects = objects
		s.CaughtUpIn = elapsed
		s.ResyncedKinds = resyncedKinds
		s.ResyncedObjects = resyncedObjects
		// A caught-up report has, by definition, nothing stale. Clearing this matters on
		// the livenessMonitor's cold-completion recovery path, which calls reportCaughtUp
		// AFTER a prior reportStale left StaleKinds populated.
		s.StaleKinds = nil
	})
}

// reportCaughtUpOrStale emits the milestone the last initial driver closes out. Every
// initial driver has by now either watched or given up (stuck) — but "the cohort is
// resolved" is not "the cohort synced": a stuck kind never synced, so reporting
// EngineWatching/SyncComplete here would be a false success (corrected only on the
// liveness monitor's next tick). So if any driver is stuck it reports EngineStale naming
// them and skips the caught-up counts; only a fully-healthy cohort reports EngineWatching.
// The monitor (started once caughtUp closes) then owns the stale↔recovered tracking.
func (e *Engine) reportCaughtUpOrStale(ctx context.Context, drivers []*kindDriver, coldStart bool, startedAt time.Time, syncedKinds, resyncedKinds, resyncedObjects int) {
	if laggards := staleLaggards(drivers, e.now(), e.staleThreshold); len(laggards) > 0 {
		e.reportStale(laggards)
		return
	}
	e.reportCaughtUp(ctx, coldStart, startedAt, syncedKinds, resyncedKinds, resyncedObjects)
}

// reportStale publishes EngineStale naming the kinds that aren't current — shared by the
// catch-up milestone (reportCaughtUpOrStale) and the livenessMonitor.
func (e *Engine) reportStale(laggards []KindStatus) {
	e.report(func(s *EngineStatus) {
		// Stale is a departure from the caught-up state, so clear the prior report's
		// catch-up counts (clearCatchUp also nils StaleKinds) before naming the
		// laggards — otherwise a Stale report would ride along a superseded run's
		// SyncedObjects/CaughtUpIn.
		s.clearCatchUp()
		s.State, s.LastError, s.StaleKinds = EngineStale, "", laggards
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
// context, wait for its goroutine to finish (done), and its catch-up token (retire — the
// milestone obligation of an initial driver; nil for a mid-run driver, which owes
// nothing). remove returns the token so a caller can either fire it (a vanished kind:
// retire()) or transfer it to a replacement (a GVR repoint).
type driverHandle struct {
	driver *kindDriver
	cancel context.CancelFunc
	done   chan struct{}
	retire *milestoneToken
}

func newDriverSet() *driverSet {
	return &driverSet{byGVK: make(map[schema.GroupVersionKind]driverHandle)}
}

// launch registers d and starts its Run on a child of ctx, tracking it for wait().
// Called synchronously for the initial set, then from the discovery reconciler for
// new kinds. The child context lets remove() stop this one driver independently;
// retire (may be nil) is the driver's catch-up token, surfaced back through remove().
func (s *driverSet) launch(ctx context.Context, d *kindDriver, onExit func(error), retire *milestoneToken) {
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
// its fate: a vanished kind resolves it as removed (retire.resolve(false)) AFTER pruning,
// while a GVR repoint re-arms it and transfers it to the replacement (retire.rearm(), so the
// milestone waits for the replacement's own catch-up). Returns nil if gvk isn't running or
// the driver held no token.
func (s *driverSet) remove(gvk schema.GroupVersionKind) *milestoneToken {
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

// milestone tracks the Syncing→Watching catch-up of one run's initial driver cohort. Each
// initial kind holds one token (token()); the milestone fires ONCE, when every token has
// released its gate — reached a watch, gone stuck, or been removed. Whether it reports
// Watching or Stale is decided at fire time from live driver state (the fire callback →
// reportCaughtUpOrStale); the caughtUp tally (kinds currently resolved by REACHING a watch)
// is the SyncComplete kind count, handed to fire so the callback needn't re-lock.
//
// A token is deliberately NOT a permanent one-shot. A stuck token releases the gate (so a
// permanently-broken kind — forbidden, un-paginatable — can't wedge the milestone in
// Syncing) yet stays RE-GATE-ABLE, because a stuck initial kind can still change before the
// milestone fires: it can recover (onWatch → re-counted), or discovery can repoint it
// (rearm() re-holds the gate for the fresh replacement). Without re-gating, transferring a
// stuck kind's spent release to a never-watched replacement would let another kind's
// resolution fire a false EngineWatching while the replacement is still listing. All token
// state lives under milestone.mu; the counts are plain ints, not atomics, because every
// transition is a compare-and-adjust across pending+caughtUp that must be atomic together.
// resyncTally aggregates the per-driver re-sync work (did the kind fall back to a full
// re-list, and how many bodies it re-pulled) that feeds the ResyncComplete message,
// keyed by GVK. Keying by kind — rather than summing into free atomics — makes a repeat
// idempotent: a kind that reports more than once (a stuck→recovered driver re-firing
// onWatch, or a GVR repoint whose forceCold replacement re-does the removed driver's
// re-sync) overwrites its own entry (last-write-wins) instead of double-counting into
// the run totals. record runs from each driver's Run goroutine and totals from the
// milestone fire callback, so both take mu.
type resyncTally struct {
	mu     sync.Mutex
	byKind map[schema.GroupVersionKind]int // objects re-pulled, per kind that re-synced
}

func newResyncTally() *resyncTally {
	return &resyncTally{byKind: make(map[schema.GroupVersionKind]int)}
}

// record sets (or, for a clean resume, clears) this kind's re-sync contribution. Keyed by
// GVK, so a second call for the same kind replaces the first rather than adding to it.
func (t *resyncTally) record(gvk schema.GroupVersionKind, resynced bool, objects int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if resynced {
		t.byKind[gvk] = objects
	} else {
		delete(t.byKind, gvk)
	}
}

// totals returns how many kinds re-synced and the total bodies they re-pulled.
func (t *resyncTally) totals() (kinds, objects int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, n := range t.byKind {
		kinds++
		objects += n
	}
	return kinds, objects
}

type milestone struct {
	mu       sync.Mutex
	pending  int // tokens still holding the gate; fires when this hits 0
	caughtUp int // tokens currently resolved by reaching a watch
	fired    bool
	fire     func(syncedKinds int)
}

func newMilestone(n int, fire func(syncedKinds int)) *milestone {
	return &milestone{pending: n, fire: fire}
}

// milestoneToken is one initial kind's contribution to the milestone. Its fields are touched
// only under milestone.mu.
type milestoneToken struct {
	m        *milestone
	holding  bool // holds the gate? (unresolved, or re-armed after a repoint); starts true
	caughtUp bool // currently counted toward the milestone's caughtUp tally?
}

// token mints one initial kind's obligation. Unlike a bare one-shot, it can be resolved
// repeatedly as the driver's state changes, and re-armed on a repoint (see milestone).
func (m *milestone) token() *milestoneToken {
	return &milestoneToken{m: m, holding: true}
}

// resolve records HOW this kind resolved: reachedWatch=true (onWatch — now watching) counts
// it toward caughtUp; false (onStuck / removal) releases the gate without counting. It is
// idempotent per state and re-entrant across transitions (stuck→recovered, recovered→
// re-stuck) until the milestone fires. The fire callback runs AFTER unlocking so the
// catch-up report isn't held under the milestone mutex.
func (t *milestoneToken) resolve(reachedWatch bool) {
	t.m.mu.Lock()
	fire, syncedKinds := t.applyLocked(reachedWatch)
	t.m.mu.Unlock()
	if fire != nil {
		fire(syncedKinds)
	}
}

// applyLocked adjusts this token's caughtUp contribution and releases its gate, returning
// the fire callback + caught-up count when this resolution clears the LAST gate (so the
// caller can fire it after unlocking), or (nil, 0) otherwise.
func (t *milestoneToken) applyLocked(reachedWatch bool) (func(int), int) {
	if t.m.fired {
		return nil, 0
	}
	t.setCaughtUpLocked(reachedWatch)
	if !t.holding {
		return nil, 0 // already released; a later re-resolve only updates the count
	}
	t.holding = false
	t.m.pending--
	if t.m.pending > 0 {
		return nil, 0
	}
	t.m.fired = true
	return t.m.fire, t.m.caughtUp
}

// setCaughtUpLocked moves this token in/out of the caughtUp tally to match its latest state.
func (t *milestoneToken) setCaughtUpLocked(reachedWatch bool) {
	if reachedWatch == t.caughtUp {
		return
	}
	if reachedWatch {
		t.m.caughtUp++
	} else {
		t.m.caughtUp--
	}
	t.caughtUp = reachedWatch
}

// rearm re-holds the gate for a token transferred to a fresh replacement driver on a GVR
// repoint: the replacement hasn't synced, so the milestone must wait for it rather than
// inherit the old driver's release. A caught-up token also drops its count until the
// replacement re-earns it. No-op once the milestone has fired.
func (t *milestoneToken) rearm() {
	t.m.mu.Lock()
	defer t.m.mu.Unlock()
	if t.m.fired {
		return
	}
	t.setCaughtUpLocked(false)
	if !t.holding {
		t.holding = true
		t.m.pending++
	}
}

// caughtUpCount is how many initial kinds are currently resolved by reaching a watch. The
// fire callback gets this value passed in; tests read it after the milestone fires.
func (m *milestone) caughtUpCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.caughtUp
}

// run blocks for one engine generation: build clients, walk discovery, and
// drive one kindDriver per discovered GVR until ctx is cancelled. The state
// flips to Watching once every driver in the initial discovery cohort has entered
// its watch phase (a kind discovered mid-run doesn't gate this — see EngineWatching).
// It also watches the objects that mint GVRs (CRDs, APIServices) so a kind installed
// mid-run starts mirroring without a restart (see watchDiscoveryTriggers / discoveryLoop).
func (e *Engine) run(ctx context.Context, coldStart bool) error {
	// Every client gets a read-inactivity timeout (see idleTimeoutRoundTripper): a wedged
	// LIST/GET is cancelled so it can't hold a limiter slot — or block the discovery walk
	// — forever, while a slow-but-progressing transfer runs as long as it keeps streaming.
	// Discovery needs it too: a registered-but-unreachable aggregated APIService (a hung
	// /apis/<group>/<version>) is exactly the same wedge, on the equally hang-prone
	// discovery path. The wrapper exempts watches, so the same dyn/md clients still serve
	// the long-lived watch path.
	idleCfg := rest.CopyConfig(e.cfg)
	idleCfg.Wrap(newIdleTimeoutWrapper(defaultListIdleTimeout))
	dc, err := discovery.NewDiscoveryClientForConfig(idleCfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(idleCfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	md, err := metadata.NewForConfig(idleCfg)
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

	// Aggregate the per-driver re-sync work as each driver reaches its watch phase. Each
	// contributes from its Run goroutine, keyed by GVK, rather than being read back from the
	// driver struct — so a driver's later full-resync retry can't race the aggregation, and
	// a kind that reports twice (recovery or a repoint's replacement) counts once. (The count
	// of kinds that reached a watch — the SyncComplete "M kinds" — is the milestone's own
	// caughtUp tally, deduped on the one-shot token so a repointed kind counts once; see
	// milestone.token.)
	tally := newResyncTally()
	caughtUp := make(chan struct{})
	drivers := newDriverSet()

	// One full-LIST concurrency limiter shared by every driver this run starts —
	// initial cohort and mid-run discoveries alike — so their combined in-flight LISTs
	// never exceed listConcurrency, bounding cold-start peak memory (see listConcurrency).
	listLim := newListLimiter(listConcurrency)

	// The catch-up milestone tracks the initial cohort's Syncing→Watching. Each initial
	// kind holds one one-shot token (ms.token(), see milestone); the fire callback runs
	// once, when every kind has caught up / been removed / gone stuck, reporting
	// EngineWatching if all healthy or EngineStale if a stuck kind closed it out (decided
	// freshly from driver state; the kind count is the milestone's caughtUp tally, passed
	// to fire). A repoint TRANSFERS and re-arms the token on the replacement (see
	// reconcileDiscovery).
	ms := newMilestone(len(entries), func(syncedKinds int) {
		resyncedKinds, resyncedObjects := tally.totals()
		e.reportCaughtUpOrStale(runCtx, drivers.snapshot(), coldStart, startedAt,
			syncedKinds, resyncedKinds, resyncedObjects)
		close(caughtUp)
	})

	// startDriver builds and launches one driver for a GVR. `retire` is the driver's
	// catch-up token (see milestone.token), or nil for a kind that owes nothing — a kind
	// discovered mid-run joins AFTER catch-up, so it doesn't participate in the milestone
	// and just streams. A driver holding a token counts down Syncing→Watching and feeds
	// the re-sync aggregation when it reaches its watch phase.
	startDriver := func(entry gvrEntry, retire *milestoneToken, forceCold bool) {
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
			withNow(e.now), withGVR(entry.GVR), withResyncPeriod(e.resyncPeriod), withListLimiter(listLim))
		// A tokenless driver (a mid-run discovery) can't participate in the milestone — it
		// just streams; the liveness monitor still surfaces it if it gets stuck. An initial
		// driver resolves its token when it reaches watch (onWatch) OR when it exhausts its
		// error budget (onStuck), so a kind that can never sync stops blocking the
		// Syncing→Watching milestone instead of wedging Syncing. The token is one-shot;
		// whether a stuck kind reads as Stale is decided freshly at fire/monitor time from
		// driver state, not the token.
		if retire != nil {
			d.onWatch = func(resynced bool, objects int) {
				// Record this kind's re-sync contribution keyed by GVK. onWatch can fire again
				// after a stuck→recovered transition (the driver re-arms it so the recovery
				// re-counts the token below), and a GVR repoint's replacement re-does the removed
				// driver's re-sync — the GVK key makes both idempotent (last-write-wins) instead
				// of double-counting into the run totals.
				tally.record(entry.GVK, resynced, objects)
				// Re-entrant: (re)counts this kind toward the caught-up tally each time its
				// watch proves live, including a recovery after onStuck de-counted it.
				retire.resolve(true)
			}
			d.onStuck = func() { retire.resolve(false) } // resolved as stuck, not caught up
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

	// The initial cohort's GVKs — the kinds whose sync the SyncComplete count reports. Kept
	// so the cold-completion recovery counts only this cohort's live kinds, not any kind
	// discovered mid-run (which is not part of the initial sync).
	initialGVKs := make(map[schema.GroupVersionKind]struct{}, len(entries))
	for _, entry := range entries {
		initialGVKs[entry.GVK] = struct{}{}
		startDriver(entry, ms.token(), false)
	}

	var monWg sync.WaitGroup
	monWg.Go(func() { e.livenessMonitor(runCtx, drivers.snapshot, caughtUp, coldStart, startedAt, initialGVKs) })

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
func (e *Engine) discoveryLoop(ctx context.Context, dc discovery.DiscoveryInterface, drivers *driverSet, startDriver func(gvrEntry, *milestoneToken, bool), triggers <-chan struct{}) {
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
func (e *Engine) reconcileDiscovery(ctx context.Context, dc discovery.DiscoveryInterface, drivers *driverSet, startDriver func(gvrEntry, *milestoneToken, bool)) {
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
			// TRANSFER the old driver's catch-up token to the replacement and RE-ARM it, so the
			// milestone waits for the replacement's own full LIST rather than inheriting the old
			// driver's resolution. This matters most when the old driver had already resolved
			// (caught up, or gone stuck and released the gate): rearm re-holds the gate (and
			// drops any caught-up count) so another kind's resolution can't fire a false
			// EngineWatching while the never-watched replacement is still listing. A still-
			// unresolved token is left holding (rearm is a no-op); a nil (mid-run) token owes
			// nothing.
			if retire != nil {
				retire.rearm()
			}
			startDriver(entry, retire, true)
		}
	}
	if !complete {
		return // partial: don't remove/prune; the next pass (trigger or poll) completes it
	}
	// A complete pass is authoritative. Stop the drivers for vanished kinds (each remove
	// blocks until the goroutine stops), then prune their cached rows and resume cookies.
	// Fire each removed driver's catch-up token as removed AFTER the prune, so a removal
	// that closes out the milestone can't report catch-up while stale rows are still present.
	var retires []*milestoneToken
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
		retire.resolve(false) // removed, not caught up — releases the gate without counting
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
		// Only the collection's resourceVersion is needed to seed the trigger watch,
		// and the list RV is the same at any page size — so cap the LIST at one item.
		// Without the cap this would materialize the entire CRD/APIService collection
		// (CRD bodies embed OpenAPI schemas, 100s of KB each) on every (re)connect and
		// backoff cycle just to read the collection resourceVersion.
		_, _, rv, err := src.List(ctx, metav1.ListOptions{Limit: 1})
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
// recovery report normally carries no catch-up counts (a liveness resume, not a fresh
// sync), which the controller renders as "watch recovered".
//
// The one exception is a COLD start whose catch-up milestone reported EngineStale
// because a kind went stuck before ever watching (so the cold SyncComplete never
// fired): coldStart+startedAt let the monitor emit that skipped initial-sync
// completion when the cohort finally goes live, rather than a bare warm recovery that
// would misreport the cache's first-ever sync.
func (e *Engine) livenessMonitor(ctx context.Context, drivers func() []*kindDriver, caughtUp <-chan struct{}, coldStart bool, startedAt time.Time, initialGVKs map[schema.GroupVersionKind]struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-caughtUp:
	}
	ticker := time.NewTicker(e.staleCheckInterval)
	defer ticker.Stop()
	// reported is the laggard set last published as stale; nil while watching. A
	// non-nil, non-equal set means the wedged kinds shifted and must be re-reported.
	// Seed it from the state the milestone published (reportCaughtUpOrStale): if a stuck
	// kind closed out catch-up it already published EngineStale naming it, so adopt that
	// as the baseline — otherwise the monitor couldn't detect that kind's later recovery
	// (it would think nothing was stale) and would leave the engine wrongly Stale.
	//
	// coldPending records that this cold build's SyncComplete is still owed: the milestone
	// fired EngineStale instead of the caught-up EngineWatching, so the first recovery must
	// emit the completion (with reconstructed counts) rather than clear the catch-up facts.
	e.mu.Lock()
	reported := slices.Clone(e.status.StaleKinds)
	coldPending := coldStart && e.status.State == EngineStale
	e.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			laggards := staleLaggards(drivers(), e.now(), e.staleThreshold)
			switch {
			case len(laggards) > 0 && !slices.Equal(laggards, reported):
				reported = laggards
				e.reportStale(laggards)
			case len(laggards) == 0 && reported != nil:
				reported = nil
				if coldPending {
					// The cold build's SyncComplete never fired (a kind went stuck before ever
					// watching, so the milestone reported EngineStale). Now that every kind is
					// live, emit the initial-sync completion the milestone skipped — with counts
					// reconstructed from live driver state, since the milestone's caughtUp tally
					// is frozen at fire time — instead of a bare "watch recovered".
					coldPending = false
					e.reportCaughtUp(ctx, true, startedAt, syncedKindCount(drivers(), initialGVKs), 0, 0)
				} else {
					e.report(func(s *EngineStatus) {
						s.State = EngineWatching
						s.clearCatchUp()
					})
				}
			}
		}
	}
}

// syncedKindCount counts INITIAL-cohort drivers that have reached a watch (non-zero
// liveness) — how many of the initial kinds are actually synced right now. livenessMonitor
// uses it to reconstruct the SyncComplete kind tally on the cold-completion recovery path,
// where the milestone's own caughtUp tally is frozen at fire time and can't be reused. It's
// filtered to initialGVKs so a kind discovered mid-run (a CRD installed after startup, which
// is not part of the initial sync) can't inflate the reported "M kinds" of the first-ever
// sync. At recovery the cohort's initial drivers have all resolved (the milestone fired), so
// a previously-stuck kind that recovered now stamps liveness like the rest.
func syncedKindCount(drivers []*kindDriver, initialGVKs map[schema.GroupVersionKind]struct{}) int {
	n := 0
	for _, d := range drivers {
		if _, ok := initialGVKs[d.gvk]; !ok {
			continue // discovered mid-run — not part of the initial sync
		}
		if !d.liveAt().IsZero() {
			n++
		}
	}
	return n
}

// staleLaggards names the kinds that are either stuck or whose watch hasn't proven
// liveness within threshold. Two independent signals:
//
//   - Stuck (isStuck): the driver exhausted its resync error budget — it can't LIST/watch
//     at all (forbidden, un-paginatable within the token lifetime, perpetually stalled).
//     Surfaced regardless of watch state, since it may never have listed. A never-watched
//     driver is judged by whether it's actually failing, not by how long it's been
//     trying — so a large but *progressing* initial/mid-run LIST (which the transport
//     idle timeout lets run arbitrarily long) is never falsely flagged.
//   - Wedged watch: a driver that DID reach its watch phase (non-zero liveAt) but whose
//     last liveness proof — a delta, bookmark, or reconnect — is older than threshold.
//     A quiet-but-healthy kind still gets the server's periodic bookmarks, so only a
//     silently-wedged watch appears here.
//
// A never-watched-but-not-stuck driver is legitimately still syncing and is NOT flagged.
//
// Each laggard carries its cause: a stuck driver reports the phase that spent its budget
// (stuckCause — CauseListFailed vs CauseWatchFailed), a wedged-but-not-stuck watch is
// CauseWatchStalled.
//
// The result is sorted by kind: the driver set is a map (random iteration order), so an
// unsorted slice would let an unchanged laggard set permute between snapshots and
// defeat livenessMonitor's slices.Equal dedup — re-emitting the same stale report
// with reordered kind names.
func staleLaggards(drivers []*kindDriver, now time.Time, threshold time.Duration) []KindStatus {
	var laggards []KindStatus
	for _, d := range drivers {
		live := d.liveAt()
		switch {
		case d.isStuck():
			laggards = append(laggards, KindStatus{Kind: d.gvk.Kind, Cause: d.stuckCause()})
		case live.IsZero():
			// Never watched but not stuck — still legitimately syncing; don't flag.
		case bookmarkless(d.gvk):
			// The kube-apiserver serves this kind without its watch cache (Events),
			// so it never sends watch bookmarks — regardless of AllowWatchBookmarks.
			// On a quiet cluster the watch then delivers no deltas AND no bookmarks
			// for arbitrarily long, so lastLiveAt legitimately goes cold even though
			// the watch is fine. Skip the wedged-watch check: connection death is
			// still caught (HTTP/2 keepalive drops the conn → reconnect stamps
			// liveness), a genuinely broken kind still surfaces via isStuck above,
			// and the periodic resync reconciles any missed drift. A stall we can't
			// distinguish from healthy silence is not worth a false Stale.
		case now.Sub(live) > threshold:
			laggards = append(laggards, KindStatus{Kind: d.gvk.Kind, Cause: CauseWatchStalled})
		}
	}
	sort.Slice(laggards, func(i, j int) bool { return laggards[i].Kind < laggards[j].Kind })
	return laggards
}

// bookmarkless reports whether the kube-apiserver serves gvk without a watch cache
// and so never emits watch bookmarks for it. Today that's Events — both the core
// group ("") and events.k8s.io host a Kind "Event", and the apiserver disables the
// watch cache for events by default (they're high-churn and TTL'd), so their watches
// stream straight from etcd with no bookmarks. staleLaggards exempts these kinds from
// the wedged-watch (CauseWatchStalled) check, since bookmark silence on them is
// indistinguishable from a healthy quiet watch. There's no API that advertises this
// property, so the set is hardcoded to the known real-world case.
func bookmarkless(gvk schema.GroupVersionKind) bool {
	return gvk.Kind == "Event" && (gvk.Group == "" || gvk.Group == "events.k8s.io")
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
