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

// Package kubesync mirrors one Kubernetes collection into a local table for as long as
// it runs. It is a leaf, and it is deliberately ignorant of WHICH collection: the caller
// supplies a Source (where to list/watch) and a Store (where the rows land), and this
// package owns everything between them — the resume/full-list state machine (driver.go),
// the error budget, the graded liveness proofs, and the status reports a controller folds
// into an object's conditions.
//
// Two packages build on it: eventsync (the cluster's Events, one fixed kind) and
// objectsync (one discovered GVR, one worker per ClusterCacheGVRSync object). They differ
// only in their Source and Store — the sync itself is the same problem, so it is solved
// once here.
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
	// monitorInterval is how often the liveness monitor re-evaluates whether the watch is
	// still proving itself alive. It doubles as the freshness heartbeat: each tick
	// re-reports the current state so the freshness stamps stay current in the UI.
	monitorInterval = 30 * time.Second

	// staleThreshold is how long a caught-up watch may go without proving liveness — no
	// delta, no bookmark, no completed LIST — before the cache is called stale. A bare
	// reconnect is NOT a proof (see the driver's liveness type), so a watch that keeps
	// re-establishing without streaming ages into this rather than resetting it. The api
	// server sends periodic bookmarks, which the driver taps, so a quiet-but-healthy
	// collection keeps proving itself and only a genuinely wedged watch trips this.
	staleThreshold = 5 * time.Minute
)

// Source is the upstream the driver pulls from — one Kubernetes collection, already
// scoped to its GVR by whoever built it. NewDynamicSource is the production
// implementation; tests supply a fake.
type Source interface {
	// List returns one page of items, the list's continue token (non-empty while more
	// pages remain — the caller loops on it via opts.Continue), and the list's
	// resourceVersion.
	List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error)
	// ListMetadata returns one page of identities only (uid/namespace/name/
	// resourceVersion), the page's continue token, and the list's resourceVersion — the
	// cheap read the diff resync compares against the cache. Paginated like List.
	ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]ObjectMeta, string, string, error)
	// Get fetches one full object; an empty namespace addresses a cluster-scoped kind.
	// The diff resync calls it for the objects that actually moved.
	Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error)
	// Watch opens a watch; the caller (RetryWatcher) supplies RV + AllowWatchBookmarks
	// in opts.
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// ObjectMeta is the identity a metadata list yields — everything the diff resync needs to
// decide whether the cache's copy of an object is current, and to address it if not.
type ObjectMeta struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
}

// MetadataDiffStore is the optional extension a store implements to take part in the
// metadata-first diff resync (see driver.resync): it must be able to say what it already
// holds, and to drop one object by uid.
//
// Optional rather than part of Store because it is a real capability question, not a
// formality: the events store keys one table by uid with no per-object resourceVersion and
// mirrors the server's retention by pruning, so "which of my rows moved" has no answer
// there. The driver type-asserts and falls back to a full LIST, which every store supports.
type MetadataDiffStore interface {
	Store
	// SnapshotRVs returns this collection's cached uid -> resourceVersion.
	SnapshotRVs(ctx context.Context) (map[string]string, error)
	// DeleteByUIDs removes the named objects and their edges. Batched because the caller
	// hands it a whole relist's worth of vanished objects at once, on a writer connection
	// the whole cache shares.
	DeleteByUIDs(ctx context.Context, uids []string) error
	// ApplyDiff lands one object the diff fetched WITHOUT advancing the resume cookie, and
	// ClearRV drops the cookie outright. Together they give the diff pass the same
	// invariant the full LIST has — a cookie means a COMPLETED pass — which per-object
	// advances would break: a diff's objects arrive by GET in arbitrary order, each with
	// its own resourceVersion, so a crash mid-pass could leave the cookie ahead of changes
	// the pass never applied, and the next watch would resume past them.
	ApplyDiff(ctx context.Context, u *unstructured.Unstructured) error
	ClearRV(ctx context.Context) error
}

// Store is where the driver lands what it pulls, and where the resume cookie lives. An
// implementation owns one collection's rows: pruning, notification, and the projection
// from an object body to columns are all its business, not the driver's.
type Store interface {
	// EnsureCatalog registers the collection in the cache's kind catalog. Called once
	// before the worker starts, and idempotent: the catalog is what the cache advertises
	// it holds, so a kind must appear there before its first row rather than after its
	// first successful LIST.
	EnsureCatalog(ctx context.Context) error
	// ResumeRV returns the persisted resourceVersion to seed the watch from, or "" to
	// force a cold full LIST.
	ResumeRV(ctx context.Context) (string, error)
	// Count returns how many rows the store already holds — the warm-cache size a resume
	// report describes itself with.
	Count(ctx context.Context) (int, error)
	// ApplyChange lands one watch delta — Added/Modified upsert, Deleted removes — AND
	// advances the resume cookie to the delta's own resourceVersion, atomically with the
	// row. Both halves are the implementation's job because both belong in one
	// transaction: a row durable behind a stale cookie would be re-listed on the next
	// start, and a cookie durable ahead of its row would skip it forever.
	ApplyChange(ctx context.Context, t watch.EventType, u *unstructured.Unstructured) error
	// PersistRV advances the resume cookie without touching rows — the bookmark path,
	// where the server proves currency with nothing to write.
	PersistRV(ctx context.Context, rv string) error
	// BeginReplace opens a streaming full-LIST reconcile — see ReplaceSession.
	BeginReplace() ReplaceSession
}

// ReplaceSession streams a paginated full LIST into the store: each page lands as it
// arrives (bounding memory to one page of bodies) and Commit reconciles the store to the
// union of every page's items, pruning what no page carried. An abandoned session needs
// no teardown — the driver just drops it.
type ReplaceSession interface {
	WritePage(ctx context.Context, items []*unstructured.Unstructured) error
	// Commit reports how many rows the prune removed. That count is part of what the pass
	// CHANGED: a pass that lists nothing still empties a collection the server has dropped,
	// and reporting it as a no-op left "last update received" ageing from before it.
	Commit(ctx context.Context, resourceVersion string) (pruned int, err error)
}

// State is the coarse sync state the worker reports.
type State string

const (
	// StateSyncing: starting up or catching the cache up — no proven-live watch yet.
	StateSyncing State = "Syncing"
	// StateWatching: caught up, with a watch that has proven itself live.
	StateWatching State = "Watching"
	// StateErrored: the worker itself failed (it could not start, or its run loop exited
	// unexpectedly). Distinct from a sync that is merely struggling, which is Stale.
	StateErrored State = "Errored"
	// StateStale: the worker was caught up but the watch stopped proving liveness, or the
	// sync spent its error budget without ever getting going. Cause says which.
	StateStale State = "Stale"
)

// Cause explains a StateStale report so the message can be actionable (e.g. "grant watch
// on this resource") rather than a bare "not receiving updates".
type Cause string

const (
	// CauseListFailed: the LIST can't complete — RBAC `list` denied, or too many items
	// to paginate within the continue token's lifetime.
	CauseListFailed Cause = "ListFailed"
	// CauseWatchFailed: LIST works but the watch can't establish — RBAC `watch` denied,
	// or an api server refusing the watch.
	CauseWatchFailed Cause = "WatchFailed"
	// CauseWatchStalled: the watch was live and went quiet past staleThreshold.
	CauseWatchStalled Cause = "WatchStalled"
	// CauseWatchNotStreaming: the watch has established but never streamed — no delta and
	// no bookmark since the connect that opened the episode, whether that's one dead
	// connection or a reconnect loop. A watch that opens and immediately errors
	// re-establishes every cycle, which resets the error budget, so this is the only
	// signal that separates it from a healthy watch; without it such a loop reports
	// Watching forever while the cache receives nothing.
	CauseWatchNotStreaming Cause = "WatchNotStreaming"
)

// Status is one report from the worker. It is comparable except for its timestamps, and
// the owning controller folds it into its object's condition + event log.
//
// The item counts are deliberately unit-neutral ("items", not "events" or "objects"):
// this package doesn't know what it is mirroring, and each controller names the unit in
// the message it renders.
type Status struct {
	State State
	// LastError is set on StateErrored.
	LastError string
	// LastUpdateAt is when items were last durably written to the cache (an applied watch
	// delta, or a LIST that landed rows) — the one stamp that means data actually arrived,
	// so it's what a "last update received" line may honestly show. Nil before the first
	// write, which on a quiet collection can be indefinitely.
	LastUpdateAt *time.Time
	// LastLiveAt is when the sync last PROVED it's alive: a write, a bookmark, or a
	// completed LIST. It's the freshness the staleness rule runs on, and it deliberately
	// excludes a bare watch (re)connect — see the driver's liveness type. Nil before the
	// first proof.
	LastLiveAt *time.Time
	// ColdStart reports whether this run began with no resume cookie (a cold build)
	// rather than resuming a warm cache. It splits every phase into a start/complete pair
	// in the event log: SyncStart/SyncComplete when cold, ResyncStart/ResyncComplete when
	// warm.
	ColdStart bool
	// CachedItems is how many rows the store already held — the warm-cache size, set on a
	// warm StateSyncing start report so the resume describes what it's resuming from.
	CachedItems int
	// SyncedItems is how many items the catch-up LIST pulled, set on a cold StateWatching
	// report.
	SyncedItems int
	// Resynced reports whether a warm run had to fall back to a full LIST (an expired or
	// missing cookie) rather than resuming its watch directly; ResyncedItems counts the
	// bodies that re-list pulled. Both zero on a clean resume.
	Resynced      bool
	ResyncedItems int
	// CaughtUpIn is how long the catch-up took. It is ZERO on the monitor's periodic
	// re-report — the heartbeat and the stale→live recovery, neither of which caught
	// anything up — which is what distinguishes those from a real catch-up.
	CaughtUpIn time.Duration
	// Cause is set on StateStale.
	Cause Cause
}

// Sink receives the worker's status reports. Implementations must be safe for concurrent
// use: the run goroutine and the liveness monitor both report.
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

	// bookmarkless suppresses the wedged-watch verdict for a collection the api server
	// serves without bookmarks — see WithoutBookmarks.
	bookmarkless bool

	// Seams, defaulted in New and overridden only by this package's own tests.
	now             func() time.Time
	monitorInterval time.Duration
	staleThreshold  time.Duration
	driverOpts      []driverOption

	// lifeMu guards the run goroutine's handle (cancel/done) and the stopped latch. A
	// worker is registered with its owner BEFORE it is started — that ordering is what
	// decides which of two racing builders wins — so a Stop can genuinely arrive before
	// the Start, and both ends must be safe under it.
	lifeMu  sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool

	// reportMu serializes a report's CONSTRUCTION with its delivery. mu alone cannot: it is
	// released before Report is called, so two goroutines can build in one order and deliver
	// in the other — and the controller's fold is last-writer-wins, so a monitor tick that
	// computed Stale and was then descheduled would overwrite the recovery that came after
	// it, sticking the kind on Stale (and a bogus SyncStale event) until the next tick.
	// Never taken while holding mu; always the outer of the two.
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
// unexported and reachable only from this package's tests — mirroring the pattern in
// internal/poke and internal/cloud/prefsync.
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

// WithListLimiter shares one LIST-phase budget across the workers built with it — the one
// production Option, since the bound is a property of the SET of workers a caller runs, not
// of any single one. Every worker of one cache should be given the same limiter; see
// ListLimiter for why the slot covers the LIST phase only.
func WithListLimiter(l ListLimiter) Option {
	return withDriverOptions(withListLimiter(l))
}

// WithoutBookmarks marks a collection the api server serves WITHOUT its watch cache, and
// so without watch bookmarks — regardless of AllowWatchBookmarks. Today that is Events:
// they are high-churn and TTL'd, so the apiserver disables the watch cache for them by
// default and their watches stream straight from etcd.
//
// Freshness is measured on proofs (a delta or a bookmark), and for these kinds a quiet
// collection produces neither for arbitrarily long — silence is indistinguishable from a
// healthy idle watch, so the wedged-watch verdict would fire on every cluster that simply
// isn't doing anything. This drops that verdict only: a genuinely broken kind still
// surfaces through the driver's spent error budget (isStuck), a dead connection is still
// caught by the HTTP/2 keepalive, and the periodic re-list reconciles any missed drift.
//
// There is no API that advertises this property, so the caller hardcodes the known case.
func WithoutBookmarks() Option {
	return func(w *Worker) { w.bookmarkless = true }
}

func withDriverOptions(opts ...driverOption) Option {
	return func(w *Worker) { w.driverOpts = append(w.driverOpts, opts...) }
}

// New builds a worker that mirrors src into st, reporting to sink. jitterSeed spreads the
// periodic re-list across workers started together — pass something stable and distinct
// per worker (the cache id, or the cache id plus the GVR).
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
// A Start that follows a Stop does nothing. That is not a convenience: a worker is handed
// to its owner before it is started, so a shutdown (or any stop that doesn't hold the
// owner's lifecycle lock) can land in between — and launching the goroutine anyway would
// leave it running with nobody holding a handle to it, writing into a cache file whose
// teardown believes every writer has drained.
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

// Stop cancels the sync and waits for it to unwind, bounded by ctx. It returns ctx's
// error if the drain outlives it. Once Stop returns cleanly the run goroutine has exited,
// which is the barrier the cache-file teardown needs — a worker must not be mid-write
// when its cache file is deleted.
func (w *Worker) Stop(ctx context.Context) error {
	w.lifeMu.Lock()
	first := !w.stopped
	// Latch first, so a Start racing this one is a no-op rather than a goroutine nobody
	// will ever stop. Stopping a worker that was never started is therefore honest: it
	// really will not run.
	w.stopped = true
	cancel, done := w.cancel, w.done
	w.lifeMu.Unlock()

	if cancel == nil {
		return nil // never started, and the latch keeps it that way
	}
	if first {
		cancel()
	}
	// A repeat Stop waits for the drain again rather than returning early. Callers retry a
	// drain that timed out, and the whole point of the barrier is that a clean return means
	// the run goroutine has exited — answering "already stopping" would let the caller
	// delete a cache file out from under a goroutine still writing to it.
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run performs the one-time start-up reads, reports the opening Syncing state, then
// drives the state machine until ctx is cancelled.
func (w *Worker) run(ctx context.Context) {
	seedRV, err := w.store.ResumeRV(ctx)
	if err != nil {
		// A cookie we can't read is not fatal — treat it as absent and cold-LIST. Reporting
		// Errored here would hide a perfectly recoverable sync behind a scary state.
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

	// The monitor gets its own cancel rather than riding ctx alone. Run normally returns
	// only on cancellation — where ctx would stop the monitor too — but it can also return
	// a seam failure while ctx is very much alive, and then waiting on a monitor that only
	// watches ctx would hang here forever: run would never return, done would never close,
	// and Stop would block until its deadline, leaving the drain finalizer set and the
	// cache file's deletion barrier stuck behind it.
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

	// Run only returns on cancellation; anything else is a bug or an unrecoverable seam
	// failure, and the user deserves to see it rather than a silently stopped sync.
	if err != nil && ctx.Err() == nil {
		w.emit(func() (Status, bool) {
			return Status{State: StateErrored, LastError: err.Error()}, true
		})
	}
}

// emit builds a report and delivers it under one lock, so the sink sees reports in the
// order they were BUILT. build reads the reporting state (taking mu itself) and returns
// false to report nothing.
func (w *Worker) emit(build func() (Status, bool)) {
	w.reportMu.Lock()
	defer w.reportMu.Unlock()
	if st, ok := build(); ok {
		w.sink.Report(st)
	}
}

// reportCaughtUp is the driver's catch-up callback: the watch has proven live, so flip to
// Watching with the facts of how we got here. Runs on the driver's Run goroutine.
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

// reportStuck is the driver's error-budget callback: the sync has failed enough times in
// a row to be worth surfacing, so report it immediately rather than waiting out the
// monitor's next tick. Runs on the driver's Run goroutine.
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

// monitor re-evaluates liveness on a fixed tick: it surfaces a watch that has gone quiet,
// reports the recovery when it comes back, and otherwise re-reports the current state so
// the freshness stamps stay fresh. The re-report carries no catch-up facts (CaughtUpIn stays
// zero), which is how the controller tells a heartbeat from a real catch-up and avoids
// appending a redundant event for it.
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

// evaluate builds the monitor's periodic report, or reports nothing while the worker is
// still catching up (the opening Syncing report already says so, and a not-yet-live watch
// isn't stale — it's working).
//
// A STUCK worker is the exception, and the reason this isn't a bare !caughtUp check: one
// that spent its error budget before ever catching up is not working, and reportStuck fires
// only on the exact threshold crossing. Without this that single report is the only one the
// worker ever makes about a permanently broken kind — so a transient failure to apply it
// (the controller only logs one) would leave the child claiming Syncing for the process
// lifetime, with nothing scheduled to correct it.
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

// stalenessLocked decides whether the sync is currently failing to keep up, and why. A
// spent error budget dominates: it names the phase that broke (list vs watch), which is
// more actionable than the bare "no updates" a quiet watch produces.
//
// Freshness is measured on the STRONG proofs only (a delta or a bookmark). A bare watch
// (re)connect is excluded deliberately — see the driver's liveness type — so when nothing
// has streamed the clock runs from the FIRST connect of the current no-proof episode,
// which a reconnect cannot push forward. That is what makes an open-then-error loop age
// into staleness instead of refreshing itself into looking healthy every cycle.
func (w *Worker) stalenessLocked(live liveness) (Cause, bool) {
	if w.drv.isStuck() {
		return w.drv.stuckCause(), true
	}
	// A collection served without bookmarks proves nothing by being quiet, so the whole
	// no-proof verdict below is uninformative for it — see WithoutBookmarks.
	if w.bookmarkless {
		return "", false
	}

	base := live.proof
	if base.IsZero() {
		// Nothing has ever streamed on this watch. Measure from the first connect rather
		// than calling it stale outright: a cold start legitimately sits here until the
		// api server's first bookmark (it sends them periodically, which is what keeps a
		// quiet collection proving itself).
		base = live.firstConnect
	}
	if base.IsZero() || w.now().Sub(base) <= w.staleThreshold {
		return "", false
	}
	// A watch that has established since the last proof but streamed nothing over it is
	// the more specific — and more actionable — finding: it isn't a healthy stream gone
	// quiet, it's a connection that never delivers (an accepted-then-reset stream, or one
	// re-establishing in a loop). A proof clears the episode, so a genuinely quiet watch
	// that simply stopped reports the plain stall instead.
	if live.connectedWithoutProof() {
		return CauseWatchNotStreaming, true
	}
	return CauseWatchStalled, true
}

// stamp fills a report's two timestamps from one liveness snapshot: LastUpdateAt from
// writes alone, LastLiveAt from the strong proofs. Both nil before their first proof.
//
// It takes the snapshot rather than reading one, so a report's stamps and its
// state/cause verdict are always computed from the SAME instant — a bookmark landing
// between two reads would otherwise let a report publish a fresh LastLiveAt beside a
// Stale verdict decided from the older read.
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
