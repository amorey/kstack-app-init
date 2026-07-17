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

// Package engine mirrors one real Kubernetes cluster into its local
// SQLite cache (internal/cluster/cache/store).
//
// Discovery + dynamic/metadata clients + one per-GVR kindDriver means one
// code path serves every Kind on every cluster — built-ins and CRDs alike.
// The Engine walks /apis discovery, picks every list/watchable resource,
// and spins up one driver per (group, version, resource) feeding the
// SQLite-backed stores. Events get their own store (and table) because their
// access pattern differs; everything else lands in the universal `objects`
// table.
//
// Each driver (driver.go) is built instead of a raw client-go Reflector so a
// wake can resume cheaply: it seeds a RetryWatcher from the kind's persisted
// resourceVersion (resume — apply deltas, no LIST) and only on a 410 (RV too
// old) or a cold cache falls back to a metadata-first full re-sync (list
// metadata, fetch bodies for just the changed objects). The stock Reflector
// can't be seeded with a stored RV, so it always re-LISTs every body.
//
// One Engine runs per synced cluster, owned by the kube package's sync
// controller: the controller decides when an engine starts, stops, or
// restarts (spec changes, credential rotation, resync pokes); the engine
// reports its coarse state back through the Sink it was constructed with.
package engine

import (
	"context"
	"crypto/sha256"
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// init routes client-go's API-server warning headers (deprecation
// notices, etc.) through slog at Debug level instead of stderr. The
// default handler logs them as INFO/WARN, which makes the app logs
// noisy whenever the cluster has any deprecated API in use — the
// signal-to-noise ratio is poor because the warnings are about the
// cluster's contents, not anything the sidecar can act on.
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
	// EngineWatching means every driver has entered its watch phase at least
	// once — the cache is caught up and streaming deltas.
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
	// message; which reports carry each field varies, so see the per-field notes
	// below (the counts, notably, also ride the warm EngineSyncing start report).
	// All are reset (clearCatchUp) when the engine re-enters EngineSyncing.
	//
	// ColdStart is true when this was the cache's first-ever sync (no prior
	// state), false when it resumed an already-populated cache — the signal that
	// separates SyncStart/SyncComplete from ResyncStart/ResyncComplete.
	ColdStart bool
	// SyncedObjects/SyncedKinds are how many objects across how many kinds the sync
	// involves, and CaughtUpIn is how long the catch-up took. They power the
	// human-facing "…N objects across M kinds in Ds" messages. Set on the caught-up
	// EngineWatching report (the mirrored total, with CaughtUpIn); SyncedObjects/
	// SyncedKinds are also set on a *warm* EngineSyncing start report (the warm
	// cache being resumed, CaughtUpIn still zero). Zero on every other report.
	SyncedObjects int
	SyncedKinds   int
	CaughtUpIn    time.Duration

	// ResyncedKinds/ResyncedObjects break down how much of a warm resume was real
	// work rather than a clean reconnect: how many kinds fell back to a full
	// re-sync (saved resourceVersion missing or expired) and how many object bodies
	// those re-pulled. Aggregated across the drivers, set only on the caught-up
	// EngineWatching report; zero on a pure reconnect (every kind resumed its watch
	// directly) and on a cold build. The controller renders them into the
	// ResyncComplete message.
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
	// sleep/now are test seams (deterministic backoff and freshness stamps).
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

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

	// staleThreshold is how long a caught-up driver may go without proving its
	// watch is alive (delta, bookmark, or reconnect) before the engine flags the
	// cache stale. Comfortably above the API server's ~1min bookmark cadence so a
	// few missed bookmarks don't trip it; the connection sentinel catches hard
	// disconnects far faster, so this is the slow backstop for a silently-wedged
	// watch. (A rare API server that honours no bookmarks and holds watches open
	// for long stretches could false-positive — an accepted limitation.)
	staleThreshold = 5 * time.Minute
	// staleCheckInterval is how often the liveness monitor re-evaluates.
	staleCheckInterval = 30 * time.Second
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
		sleep:              ctxSleep,
		now:                time.Now,
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

// reportCaughtUp emits the catch-up milestone — every driver has entered its
// watch phase. It stamps the elapsed time and carries the drivers' aggregated
// re-sync breakdown (resyncedKinds/resyncedObjects) so the controller can
// compose the SyncComplete/ResyncComplete message. Only a cold build's message
// reports the object total, so the whole-table count runs on that path alone (a
// warm resume's message is watch-oriented and never reads SyncedObjects); a
// count failure is logged and reported as zero rather than failing the milestone.
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

// cacheHasData reports whether this cache already holds a prior sync's state —
// any kind has persisted a resume cookie. Read once at run start (before the
// drivers repopulate anything), so the catch-up milestone can tell a cold cache
// (first-ever sync) from a resume — keyed on the resume cookie rather than an
// object count so an empty cluster's resume isn't misread as a cold start.
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
		// Decide cold-vs-resume before anything repopulates the cache, so the
		// in-progress Syncing report carries ColdStart (a first-ever build is a
		// SyncStart transition, a resume is a ResyncStart) and run() reuses
		// the same verdict for the catch-up milestone.
		coldStart := true
		if has, err := cacheHasData(ctx, e.cdb); err != nil {
			slog.Warn("clustersync: read cache state", "id", e.cdb.ID(), "err", err)
		} else {
			coldStart = !has
		}
		// For a warm resume, read the warm cache's size so the ResyncStart event can
		// report what it's resuming from. A cold start's cache is empty, so it needs
		// no counts (its message is the static "Starting initial sync").
		var warmObjects, warmKinds int
		if !coldStart {
			warmObjects = e.countOrZero(ctx, cachedObjectCount, "count cached objects")
			warmKinds = e.countOrZero(ctx, cachedKindCount, "count cached kinds")
		}
		// Re-entering Syncing clears the previous run's catch-up facts so a later
		// snapshot (a heartbeat or an error) can't carry stale counts; the warm-cache
		// size (set only for a resume) then rides the ResyncStart report.
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
		if backoff *= 2; backoff > e.backoffMax {
			backoff = e.backoffMax
		}
	}
}

// run blocks for one engine generation: build clients, walk discovery, and
// drive one kindDriver per discovered GVR until ctx is cancelled. The state
// flips to Watching once every driver has entered its watch phase.
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
	entries, err := discoverGVRs(ctx, dc, e.cdb)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	if len(entries) == 0 {
		// Zero drivers would make run block on nothing and report Watching
		// for a cluster that mirrors no data; treat as a transient failure.
		return errors.New("discovery returned no syncable resources")
	}
	slog.Info("clustersync: discovered syncable GVRs on API server", "id", e.cdb.ID(), "count", len(entries))

	// coldStart was decided by runLoop before this report chain began (before the
	// drivers repopulate anything); stamp the run's start so the catch-up milestone
	// can report elapsed time alongside it.
	startedAt := e.now()
	kinds := len(entries)

	// runCtx scopes the liveness monitor to this run: it's cancelled when the
	// drivers finish even if the parent ctx hasn't (a driver-exhausted run retries),
	// so the monitor never outlives the drivers it watches.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	var pending atomic.Int64
	pending.Store(int64(len(entries)))
	// Aggregate the per-driver re-sync work as each driver reaches its watch phase.
	// Each driver contributes its own snapshot once (from its Run goroutine, at first
	// watch entry) — folded into these atomics rather than read back from the driver
	// structs, so a driver's later full-resync retry can't race the aggregation or
	// bleed post-catch-up work into the first ResyncComplete counts.
	var resyncedKinds, resyncedObjects atomic.Int64
	caughtUp := make(chan struct{})
	var caughtUpOnce sync.Once
	drivers := make([]*kindDriver, 0, len(entries))

	var wg sync.WaitGroup
	for _, entry := range entries {
		var store kindStore
		if isEventGVK(entry.GVK) {
			store = newEventsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		} else {
			store = newObjectsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		}
		// Seed the driver from the kind's persisted resourceVersion so it resumes
		// the watch instead of re-LISTing every body; "" (never synced, or a read
		// error) just means start with a full re-sync.
		seedRV, err := readLastListRV(ctx, writer, entry.GVK)
		if err != nil {
			slog.Warn("clustersync: read resume rv", "gvk", entry.GVK.String(), "err", err)
			seedRV = ""
		}
		d := newKindDriver(newLiveSource(dyn, md, entry), store, entry.GVK, seedRV)
		d.now = e.now // one clock: driver liveness is judged against the engine's now
		d.onWatch = func(resynced bool, objects int) {
			if resynced {
				resyncedKinds.Add(1)
				resyncedObjects.Add(int64(objects))
			}
			if pending.Add(-1) == 0 {
				e.reportCaughtUp(runCtx, coldStart, kinds, startedAt,
					int(resyncedKinds.Load()), int(resyncedObjects.Load()))
				caughtUpOnce.Do(func() { close(caughtUp) })
			}
		}
		drivers = append(drivers, d)
		slog.Debug("clustersync: starting driver", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "seedRV", seedRV)
		wg.Go(func() {
			if err := d.Run(runCtx); err != nil && runCtx.Err() == nil {
				slog.Warn("clustersync: driver exited", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "err", err)
			}
		})
	}

	var monWg sync.WaitGroup
	monWg.Go(func() { e.livenessMonitor(runCtx, drivers, caughtUp) })

	wg.Wait()
	runCancel()  // drivers done → stop the monitor
	monWg.Wait() // and join it before the run returns
	return ctx.Err()
}

// livenessMonitor flags the cache stale once a driver stops proving its watch is
// alive past staleThreshold, and recovers it once liveness returns. It only judges
// staleness after catch-up (before that the engine is legitimately still Syncing).
// It re-reports whenever the wedged set changes — not just on the healthy/stale
// edge — so a multi-kind cache never keeps naming a kind that has recovered (or
// omits a newly-wedged one) while it stays stale overall; an unchanged set doesn't
// re-emit every tick. The recovery report carries no catch-up counts — it's a
// liveness resume, not a fresh sync — which the controller renders as "watch
// recovered".
func (e *Engine) livenessMonitor(ctx context.Context, drivers []*kindDriver, caughtUp <-chan struct{}) {
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
			laggards := staleLaggards(drivers, e.now(), e.staleThreshold)
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
func staleLaggards(drivers []*kindDriver, now time.Time, threshold time.Duration) []string {
	var laggards []string
	for _, d := range drivers {
		if now.Sub(d.liveAt()) > threshold {
			laggards = append(laggards, d.gvk.Kind)
		}
	}
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
// identically, so the cluster's running drivers don't need restarting; a
// different fingerprint (rotated token, new client cert, changed CA/server URL,
// or an edited exec/auth-provider/impersonation block) means the open engine is
// running on stale config and must be restarted. We hash the *static* exec/
// auth-provider config (command/args/env/plugin settings) — runtime token
// minting is the transport's job, but editing how tokens are obtained must
// invalidate the fingerprint.
//
// proxyURL is the kubeconfig cluster's proxy-url. clientcmd compiles it into
// rest.Config.Proxy (an opaque func we can't hash), so the caller passes the raw
// string (ContextProxyURL): a changed proxy routes the connection differently
// and must restart the drivers, even when every other field is identical.
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

// Specific (group, resource) entries to skip even though they pass the
// generic filters. Right now: v1 Endpoints — deprecated in k8s 1.33+ in
// favor of discovery.k8s.io/v1 EndpointSlice, which we already mirror.
// The two resources hold the same data; keeping both wastes a watch and
// makes the API server emit a deprecation warning on every LIST.
var skipResources = map[string]map[string]bool{
	"": {"endpoints": true}, // core/v1 endpoints
}

func isEventGVK(g schema.GroupVersionKind) bool {
	return g.Kind == "Event" && (g.Group == "" || g.Group == "events.k8s.io")
}

// discoverGVRs walks /apis, returns one entry per list/watchable
// resource (preferred version only), and populates kind_catalog so the
// agent (and UI) can ask "what kinds exist on this cluster?" without
// re-doing discovery.
//
// We use ServerPreferredResources rather than ServerGroupsAndResources
// because the latter returns every version of every resource, which
// means we'd start one driver per (resource, version) — duplicating
// watches against the same underlying data and getting deprecation
// warnings on every alpha/beta version we accidentally watched.
// Preferred-only gives us one driver per logical resource.
func discoverGVRs(ctx context.Context, dc discovery.DiscoveryInterface, cdb *store.ClusterDB) ([]gvrEntry, error) {
	writer := cdb.Writer()
	complete := true
	lists, err := dc.ServerPreferredResources()
	if err != nil {
		if len(lists) == 0 {
			// Nothing usable came back (e.g. a transient discovery
			// failure or unreachable API server). Returning zero entries
			// here would make run start no drivers and report success,
			// leaving an open cluster that never mirrors data until the
			// next reconcile. Fail so the sync surfaces the error.
			return nil, fmt.Errorf("discovery returned no resources: %w", err)
		}
		// Partial discovery errors are common when an aggregated API
		// server is down; the returned lists are still usable. Log and
		// continue rather than fail the whole cluster — but mark discovery
		// incomplete so we don't prune objects for a kind that's merely in a
		// transiently-unavailable group (see the prune below).
		complete = false
		slog.Warn("clustersync: partial discovery", "err", err)
	}

	// Pull the CRD list once so kind_catalog can record CRD schemas.
	crds, _ := listCRDs(ctx, dc)

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

	// Persist the kind catalog. Truncate-and-rewrite is correct because
	// CRDs can be installed/uninstalled between sidecar runs and we want
	// the catalog to reflect "what exists right now".
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `DELETE FROM kind_catalog`); err != nil {
		return out, err
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
			return out, err
		}
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}

	// Evict objects whose kind no longer exists on the cluster (e.g. an
	// uninstalled CRD). Only safe after a complete discovery — a partial
	// result omits kinds that are still live. Build the keep-set from the
	// same catalog we just persisted so the two stay consistent.
	if complete {
		keep := make(map[kindKey]struct{}, len(catalog))
		for _, c := range catalog {
			keep[kindKey{kind: c.kind, apiVersion: c.apiVersion}] = struct{}{}
		}
		if n, err := pruneOrphanedKinds(ctx, writer, keep); err != nil {
			// Non-fatal: a failed prune just leaves stale rows for the next
			// discovery to retry. Don't fail the whole sync over it.
			slog.Warn("clustersync: prune orphaned kinds failed", "err", err)
		} else if n > 0 {
			slog.Info("clustersync: pruned orphaned objects", "rows", n)
		}
	}

	// Signal catalog subscribers (e.g. ClusterDataKindsWatch). The truncate-and-rewrite
	// above, plus the orphan prune, don't go through the per-object stores that ping on
	// write, so without this a kind added/removed since the last run — most visibly a
	// CRD uninstalled during an in-place engine restart, where the db handle doesn't
	// change — would stay stale until the next unrelated object write happened to ping.
	cdb.Notify()

	return out, nil
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
