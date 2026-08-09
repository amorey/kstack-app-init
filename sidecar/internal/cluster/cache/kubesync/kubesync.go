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

// Package kubesync mirrors one Kubernetes collection into a local table, deliberately
// ignorant of WHICH: the caller supplies a Source (list/watch) and a Store (rows), and
// this package owns everything between — the resume/full-list state machine (driver.go),
// the error budget, the graded liveness proofs, and the status reports.
//
// eventsync and objectsync build on it, differing only in Source and Store.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
package kubesync

import (
	"context"
	"errors"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

const (
	// monitorInterval is the liveness re-evaluation tick; it doubles as the freshness
	// heartbeat, each tick re-reporting the current state.
	monitorInterval = 30 * time.Second

	// staleThreshold is how long a caught-up watch may go without a proof (delta,
	// bookmark, completed LIST) before the cache is called stale. A bare reconnect is not
	// a proof, so a re-establishing-but-silent watch ages into this instead of resetting
	// it; periodic api-server bookmarks keep a quiet-but-healthy collection out of it.
	staleThreshold = 5 * time.Minute
)

// Source is one GVR-scoped upstream collection. NewDynamicSource is production; tests
// supply a fake.
type Source interface {
	// List returns one page of items, the continue token (non-empty while more pages
	// remain), and the list's resourceVersion.
	List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error)
	// ListMetadata is List over identities only — the cheap read the diff resync
	// compares against the cache.
	ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]ObjectMeta, string, string, error)
	// Get fetches one full object (empty namespace = cluster-scoped), for the objects
	// the diff found moved.
	Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error)
	// Watch opens a watch; RetryWatcher supplies RV + AllowWatchBookmarks in opts.
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// ObjectMeta is what a metadata list yields: enough to tell whether the cache's copy is
// current, and to address it if not.
type ObjectMeta struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
}

// MetadataDiffStore is the optional extension for the metadata-first diff resync (see
// driver.resync). Optional because it is a real capability question: the events store
// keys by uid with no per-object resourceVersion, so "which of my rows moved" has no
// answer there. The driver type-asserts and falls back to a full LIST.
type MetadataDiffStore interface {
	Store
	// SnapshotRVs returns the cached uid -> resourceVersion.
	SnapshotRVs(ctx context.Context) (map[string]string, error)
	// DeleteByUIDs removes the named objects and their edges; batched because a whole
	// relist's vanished objects arrive at once on the cache's shared writer.
	DeleteByUIDs(ctx context.Context, uids []string) error
	// ApplyDiff lands one fetched object WITHOUT advancing the cookie, and ClearRV drops
	// the cookie — together preserving "a cookie means a COMPLETED pass". Per-object
	// advances would break it: the diff GETs objects in arbitrary RV order, so a crash
	// mid-pass could leave the cookie ahead of changes never applied.
	ApplyDiff(ctx context.Context, u *unstructured.Unstructured) error
	ClearRV(ctx context.Context) error
}

// Store is where the driver lands rows and where the resume cookie lives. Pruning,
// notification, and the body→columns projection are the implementation's business.
type Store interface {
	// EnsureCatalog idempotently registers the collection in the cache's kind catalog.
	// Called before the worker starts: a kind must be advertised before its first row,
	// not after its first successful LIST.
	EnsureCatalog(ctx context.Context) error
	// ResumeRV returns the persisted resourceVersion to seed the watch from, or "" to
	// force a cold full LIST.
	ResumeRV(ctx context.Context) (string, error)
	// Count returns the rows already held — the warm-cache size a resume reports.
	Count(ctx context.Context) (int, error)
	// ApplyChange lands one watch delta AND advances the cookie to the delta's own
	// resourceVersion, in the row's own transaction. A row behind a stale cookie gets
	// re-listed; a cookie ahead of its row skips it forever.
	ApplyChange(ctx context.Context, t watch.EventType, u *unstructured.Unstructured) error
	// PersistRV advances the cookie without touching rows — the bookmark path only.
	PersistRV(ctx context.Context, rv string) error
	// BeginReplace opens a streaming full-LIST reconcile — see ReplaceSession.
	BeginReplace() ReplaceSession
}

// ReplaceSession streams a paginated full LIST: each page lands as it arrives (memory
// bounded to one page) and Commit reconciles to the union of every page, pruning what no
// page carried. An abandoned session needs no teardown.
type ReplaceSession interface {
	WritePage(ctx context.Context, items []*unstructured.Unstructured) error
	// Commit reports rows pruned — part of what the pass CHANGED, since a pass that
	// lists nothing still empties a collection the server dropped.
	Commit(ctx context.Context, resourceVersion string) (pruned int, err error)
}

// State is the coarse sync state the worker reports.
type State string

const (
	// StateSyncing: starting up or catching up — no proven-live watch yet.
	StateSyncing State = "Syncing"
	// StateWatching: caught up, watch proven live.
	StateWatching State = "Watching"
	// StateErrored: the worker itself failed to start or exited unexpectedly — distinct
	// from a merely struggling sync, which is Stale.
	StateErrored State = "Errored"
	// StateStale: caught up but no longer proving liveness, or the error budget spent
	// before ever getting going. Cause says which.
	StateStale State = "Stale"
)

// Cause explains a StateStale report, so the message can be actionable ("grant watch on
// this resource") rather than a bare "not receiving updates".
type Cause string

const (
	// CauseListFailed: LIST can't complete — `list` denied, or unpaginatable within the
	// continue token's lifetime.
	CauseListFailed Cause = "ListFailed"
	// CauseWatchFailed: LIST works but the watch can't establish.
	CauseWatchFailed Cause = "WatchFailed"
	// CauseWatchStalled: the watch was live and went quiet past staleThreshold.
	CauseWatchStalled Cause = "WatchStalled"
	// CauseWatchNotStreaming: established but never streamed since the connect that
	// opened the episode. An open-then-error loop re-establishes every cycle, so this is
	// the only thing separating it from a healthy watch — without it such a loop reports
	// Watching forever while nothing arrives.
	CauseWatchNotStreaming Cause = "WatchNotStreaming"
)

// Status is one worker report, folded by the owning controller into its object's
// condition + event log. Counts are unit-neutral ("items"): this package doesn't know
// what it mirrors, and each controller names the unit it renders.
type Status struct {
	State State
	// LastError is set on StateErrored.
	LastError string
	// LastUpdateAt is when items were last durably WRITTEN — the only stamp that means
	// data actually arrived. Nil before the first write, possibly indefinitely.
	LastUpdateAt *time.Time
	// LastLiveAt is the last strong proof (write, bookmark, completed LIST) — the
	// freshness the staleness rule runs on. Excludes a bare watch (re)connect.
	LastLiveAt *time.Time
	// ColdStart reports a run that began with no resume cookie. It splits each phase
	// into a start/complete pair in the event log (Sync* cold, Resync* warm).
	ColdStart bool
	// CachedItems is the warm-cache size, set on a warm StateSyncing start report.
	CachedItems int
	// SyncedItems is what the catch-up LIST pulled, set on a cold StateWatching report.
	SyncedItems int
	// Resynced reports a warm run that had to fall back to a full LIST (expired/missing
	// cookie); ResyncedItems counts what it pulled. Both zero on a clean resume.
	Resynced      bool
	ResyncedItems int
	// CaughtUpIn is the catch-up duration, ZERO on the monitor's periodic re-report —
	// which is how a heartbeat or stale→live recovery is told from a real catch-up.
	CaughtUpIn time.Duration
	// Cause is set on StateStale.
	Cause Cause
}

// Sink receives status reports. Must be safe for concurrent use: the run goroutine and
// the liveness monitor both report.
type Sink interface {
	Report(Status)
}

// Worker mirrors one Kubernetes collection into one store. The owning controller creates
// one per enabled object and owns its lifecycle.
type Worker struct {
	src   Source
	store Store
	sink  Sink
	// jitterSeed spreads the periodic re-list across workers (see newDriver).
	jitterSeed string

	// bookmarkless suppresses the wedged-watch verdict — see WithoutBookmarks.
	bookmarkless bool

	// Seams, defaulted in New and overridden only by this package's tests.
	now             func() time.Time
	monitorInterval time.Duration
	staleThreshold  time.Duration
	driverOpts      []driverOption

	// lifeMu guards the run handle and the stopped latch. A worker is registered with its
	// owner BEFORE Start (that ordering decides which of two racing builders wins), so a
	// Stop can genuinely precede the Start and both ends must be safe under it.
	lifeMu  sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool

	// reportMu serializes a report's CONSTRUCTION with its delivery; mu can't, being
	// released before Report. The controller's fold is last-writer-wins, so a descheduled
	// Stale tick could otherwise overwrite the recovery that followed it and stick the
	// kind on Stale. Never taken while holding mu; always the outer of the two.
	reportMu sync.Mutex

	// mu guards the reporting state below, shared by the run goroutine (catch-up, stuck)
	// and the monitor goroutine (heartbeat, stale/recovery).
	mu        sync.Mutex
	drv       *driver
	coldStart bool
	caughtUp  bool
	startedAt time.Time
}

// Option configures a Worker. Production tunes nothing, so the concrete options are
// unexported test seams — the same pattern as internal/poke and internal/cloud/prefsync.
type Option func(*Worker)

func withMonitorInterval(d time.Duration) Option {
	return func(w *Worker) { w.monitorInterval = d }
}

func withStaleThreshold(d time.Duration) Option {
	return func(w *Worker) { w.staleThreshold = d }
}

func withClock(fn func() time.Time) Option {
	return func(w *Worker) { w.now = fn }
}

// WithListLimiter shares one LIST-phase budget across the workers built with it, the
// bound being a property of the SET of workers, not of any one. Give every worker of a
// cache the same limiter; see ListLimiter for why the slot covers only the LIST phase.
func WithListLimiter(l ListLimiter) Option {
	return withDriverOptions(withListLimiter(l))
}

// WithoutBookmarks marks a collection the api server serves without its watch cache, and
// so without bookmarks regardless of AllowWatchBookmarks — today only Events, which are
// high-churn and TTL'd, so their watches stream straight from etcd.
//
// Such a collection produces no proof while quiet, making silence indistinguishable from
// a healthy idle watch, so this drops the wedged-watch verdict only: a broken kind still
// surfaces via the spent error budget, HTTP/2 keepalive, and the periodic re-list. No
// API advertises the property, so the caller hardcodes the known case.
func WithoutBookmarks() Option {
	return func(w *Worker) { w.bookmarkless = true }
}

func withDriverOptions(opts ...driverOption) Option {
	return func(w *Worker) { w.driverOpts = append(w.driverOpts, opts...) }
}

// New builds a worker mirroring src into st, reporting to sink. jitterSeed spreads the
// periodic re-list across workers started together — pass something stable and distinct
// per worker (the cache id, or cache id plus GVR).
func New(src Source, st Store, sink Sink, jitterSeed string, opts ...Option) (*Worker, error) {
	if src == nil {
		return nil, errors.New("kubesync: nil source")
	}
	if st == nil {
		return nil, errors.New("kubesync: nil store")
	}
	if sink == nil {
		return nil, errors.New("kubesync: nil sink")
	}
	w := &Worker{
		src:             src,
		store:           st,
		sink:            sink,
		jitterSeed:      jitterSeed,
		now:             time.Now,
		monitorInterval: monitorInterval,
		staleThreshold:  staleThreshold,
	}
	for _, o := range opts {
		o(w)
	}
	return w, nil
}

// Start launches the sync in the background. Pair with Stop; calling it twice is a
// programming error.
//
// A Start after a Stop does nothing: a worker is handed to its owner before it starts, so
// a stop can land in between, and launching anyway would leave a goroutine nobody holds
// writing into a cache file whose teardown believes every writer has drained.
func (w *Worker) Start() {
	w.lifeMu.Lock()
	defer w.lifeMu.Unlock()
	if w.stopped || w.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		w.run(ctx)
	}()
}

// Stop cancels the sync and waits for it to unwind, bounded by ctx (returning ctx's error
// if the drain outlives it). A clean return means the run goroutine has exited — the
// barrier cache-file teardown needs, since a worker must not be mid-write on delete.
func (w *Worker) Stop(ctx context.Context) error {
	w.lifeMu.Lock()
	first := !w.stopped
	// Latch first, so a racing Start is a no-op rather than an unstoppable goroutine.
	w.stopped = true
	cancel, done := w.cancel, w.done
	w.lifeMu.Unlock()

	if cancel == nil {
		return nil // never started, and the latch keeps it that way
	}
	if first {
		cancel()
	}
	// A repeat Stop re-waits rather than answering "already stopping", which would let a
	// caller retrying a timed-out drain delete the cache file under a live writer.
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run does the one-time start-up reads, reports the opening Syncing state, then drives
// the state machine until ctx is cancelled.
func (w *Worker) run(ctx context.Context) {
	seedRV, err := w.store.ResumeRV(ctx)
	if err != nil {
		// An unreadable cookie is recoverable: treat it as absent and cold-LIST.
		seedRV = ""
	}
	cold := seedRV == ""

	cached := 0
	if !cold {
		if n, err := w.store.Count(ctx); err == nil {
			cached = n
		}
	}

	w.mu.Lock()
	w.coldStart = cold
	w.startedAt = w.now()
	w.drv = newDriver(w.src, w.store, seedRV, w.jitterSeed, w.driverOpts...)
	w.drv.onCaughtUp = w.reportCaughtUp
	w.drv.onStuck = w.reportStuck
	w.mu.Unlock()

	w.emit(func() (Status, bool) {
		return Status{State: StateSyncing, ColdStart: cold, CachedItems: cached}, true
	})

	// The monitor gets its own cancel, not just ctx: Run can also return a seam failure
	// while ctx is alive, and waiting on a ctx-only monitor would hang run forever —
	// done never closes, Stop blocks, and the cache file's deletion barrier sticks.
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	defer stopMonitor()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.monitor(monitorCtx)
	}()

	err = w.drv.Run(ctx)
	stopMonitor()
	wg.Wait()

	// Run returns only on cancellation; anything else is a bug or unrecoverable seam
	// failure, better surfaced than left as a silently stopped sync.
	if err != nil && ctx.Err() == nil {
		w.emit(func() (Status, bool) {
			return Status{State: StateErrored, LastError: err.Error()}, true
		})
	}
}

// emit builds and delivers a report under one lock, so the sink sees reports in the order
// they were BUILT. build takes mu itself and returns false to report nothing.
func (w *Worker) emit(build func() (Status, bool)) {
	w.reportMu.Lock()
	defer w.reportMu.Unlock()
	if st, ok := build(); ok {
		w.sink.Report(st)
	}
}

// reportCaughtUp is the driver's catch-up callback (on its Run goroutine): the watch has
// proven live, so flip to Watching with the facts of how we got here.
func (w *Worker) reportCaughtUp(resynced bool, items int) {
	w.emit(func() (Status, bool) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.caughtUp = true
		st := Status{
			State:      StateWatching,
			ColdStart:  w.coldStart,
			CaughtUpIn: w.now().Sub(w.startedAt),
		}
		stamp(&st, w.drv.liveness())
		if w.coldStart {
			st.SyncedItems = items
		} else if resynced {
			st.Resynced, st.ResyncedItems = true, items
		}
		return st, true
	})
}

// reportStuck is the driver's error-budget callback (on its Run goroutine): surface the
// spent budget immediately rather than waiting for the monitor's next tick.
func (w *Worker) reportStuck(cause Cause) {
	w.emit(func() (Status, bool) {
		w.mu.Lock()
		defer w.mu.Unlock()
		st := Status{
			State:     StateStale,
			Cause:     cause,
			ColdStart: w.coldStart,
		}
		stamp(&st, w.drv.liveness())
		return st, true
	})
}

// monitor re-evaluates liveness on a fixed tick: surfaces a quiet watch, reports its
// recovery, and otherwise re-reports current state to keep the freshness stamps fresh.
// A re-report leaves CaughtUpIn zero, which is how the controller tells a heartbeat from
// a real catch-up and skips a redundant event.
func (w *Worker) monitor(ctx context.Context) {
	t := time.NewTicker(w.monitorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.emit(w.evaluate)
		}
	}
}

// evaluate builds the monitor's periodic report, or nothing while still catching up (a
// not-yet-live watch is working, not stale).
//
// A STUCK worker is the exception, and why this isn't a bare !caughtUp check: reportStuck
// fires only on the exact threshold crossing, so without this a lost report would leave a
// permanently broken kind claiming Syncing for the process lifetime.
func (w *Worker) evaluate() (Status, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.caughtUp && !w.drv.isStuck() {
		return Status{}, false
	}
	// One snapshot for the whole report — see stamp.
	live := w.drv.liveness()
	st := Status{ColdStart: w.coldStart}
	stamp(&st, live)
	if cause, stale := w.stalenessLocked(live); stale {
		st.State, st.Cause = StateStale, cause
		return st, true
	}
	st.State = StateWatching
	return st, true
}

// stalenessLocked decides whether the sync is failing to keep up, and why. A spent error
// budget dominates: it names the phase that broke, which is more actionable than "no
// updates".
//
// Freshness runs on STRONG proofs only; a bare (re)connect is excluded, so with nothing
// streamed the clock runs from the FIRST connect of the current no-proof episode and a
// reconnect can't push it forward — which is what ages an open-then-error loop into
// staleness. See docs/adr/2026-08-09-kubesync-watch-poll.md.
func (w *Worker) stalenessLocked(live liveness) (Cause, bool) {
	if w.drv.isStuck() {
		return w.drv.stuckCause(), true
	}
	// A bookmarkless collection proves nothing by being quiet — see WithoutBookmarks.
	if w.bookmarkless {
		return "", false
	}

	base := live.proof
	if base.IsZero() {
		// Nothing has ever streamed: measure from the first connect, since a cold start
		// legitimately sits here until the api server's first periodic bookmark.
		base = live.firstConnect
	}
	if base.IsZero() || w.now().Sub(base) <= w.staleThreshold {
		return "", false
	}
	// Established since the last proof but never streamed over it is the more specific
	// finding: a connection that never delivers, not a healthy stream gone quiet. A proof
	// clears the episode, so a genuinely quiet watch reports the plain stall.
	if live.connectedWithoutProof() {
		return CauseWatchNotStreaming, true
	}
	return CauseWatchStalled, true
}

// stamp fills a report's timestamps from one liveness snapshot: LastUpdateAt from writes,
// LastLiveAt from strong proofs. It takes the snapshot rather than reading one, so stamps
// and verdict come from the SAME instant — otherwise a bookmark landing between two reads
// could publish a fresh LastLiveAt beside a Stale verdict.
func stamp(st *Status, live liveness) {
	st.LastUpdateAt = timePtr(live.write)
	st.LastLiveAt = timePtr(live.proof)
}

// timePtr returns a pointer to t, or nil for the zero time (it never happened).
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
