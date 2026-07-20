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

package engine

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	toolswatch "k8s.io/client-go/tools/watch"
)

// One kindDriver per (cluster, GVK). Unlike a client-go Reflector — which can't
// be seeded with a stored resourceVersion and so relists every body on every wake
// — the driver:
//
//  1. RESUMES from the kind's persisted resourceVersion via a RetryWatcher: on a
//     wake the server usually still has that RV, so we apply only deltas, no LIST.
//  2. On 410 Gone (RV too old) or a cold cache, falls back to a METADATA-FIRST
//     full re-sync: list metadata only, diff by uid+resourceVersion, fetch full
//     bodies only for new/changed objects, delete the ones that vanished.
//
// RetryWatcher owns the watch-phase reconnect/backoff; the only explicit backoff
// here guards fullResync's list/get calls.

// defaultDiffThreshold is the changed-object count past which a single full LIST
// beats N individual GETs in the metadata-diff resync. A wake typically changes
// a handful of objects, so the diff path wins; a churned cluster (or an empty
// cache) takes the LIST.
const defaultDiffThreshold = 200

// defaultResumeGrace is how long a first watch seeded from the saved cookie waits
// to prove usable before onWatch reports it as a clean resume. A stale-cookie 410
// arrives within a round trip, so this need only clear network/tail latency; it's
// the one-time delay a warm resume's Syncing→Watching flip pays (drivers wait it
// concurrently, so it isn't summed).
const defaultResumeGrace = 2 * time.Second

// defaultStuckThreshold is how many consecutive fullResync failures mark a kind
// "stuck" — enough to ride out a transient blip (an aggregated API restarting), few
// enough that a genuinely broken kind (forbidden, un-paginatable within the token
// lifetime, perpetually stalled) surfaces to the user within seconds instead of sitting
// silently in Syncing. defaultStuckRetryInterval is the slow retry cadence a stuck kind
// drops to, so a permanently-broken kind stops hammering the API server and re-taking a
// limiter slot every backoff step.
const (
	defaultStuckThreshold     = 5
	defaultStuckRetryInterval = 3 * time.Minute
)

// defaultEstablishTimeout bounds how long one watch phase waits for the watch to actually
// CONNECT before treating the attempt as failed. RetryWatcher retries retriable errors (a
// 5xx, an EOF, a timeout) internally without ever closing its ResultChan, and a hung watch
// request (headers never arrive) never returns — so without this bound a watch that can't
// connect would block watchPhase forever: the driver would never return, never spend its
// error budget, and hold its catch-up token, wedging the whole engine in Syncing. On timeout
// the phase returns so Run charges a CauseWatchFailed; enough failures surface the kind as
// stuck. It's generous — a healthy watch connects in well under a second — so a brief
// API-server blip isn't mistaken for a broken watch; this is a monitoring app, so a
// persistent failure is surfaced to the user rather than retried forever.
const defaultEstablishTimeout = 30 * time.Second

// realGraceTimer is the production graceTimer: a time.Timer's channel plus its Stop.
func realGraceTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// errExpired means the watch's resourceVersion is too old (410 Gone); the only
// recovery is a full re-sync to obtain a fresh RV.
var errExpired = errors.New("clustersync: watch resourceVersion expired")

// errWatchNotEstablished means the watch phase gave up waiting for the watch to connect
// (see defaultEstablishTimeout). Run charges it against the error budget like any other
// watch failure so a never-connecting kind surfaces as stuck instead of wedging Syncing.
var errWatchNotEstablished = errors.New("clustersync: watch failed to establish within timeout")

// objMeta is the minimal identity the metadata-diff needs from a metadata list.
type objMeta struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
}

// kubeSource is the per-GVR upstream the driver pulls from. liveSource wraps the
// dynamic + metadata clients; tests supply a fake.
type kubeSource interface {
	// List returns one page of full unstructured objects, the list's continue
	// token (non-empty while more pages remain — the caller loops on it via
	// opts.Continue), and the list's resourceVersion.
	List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error)
	// ListMetadata returns metadata-only identities plus the list's resourceVersion.
	ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]objMeta, string, error)
	// Get fetches one full object (namespace "" = cluster-scoped).
	Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error)
	// Watch opens a watch; the caller (RetryWatcher) supplies RV + AllowWatchBookmarks in opts.
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// kindStore is the store seam every driver writes through. Both objectsStore and
// eventsStore satisfy it (see store.go); the driver never touches SQL directly.
type kindStore interface {
	// ApplyChange lands one watch delta: Added/Modified upsert the object,
	// Deleted removes it by uid.
	ApplyChange(t watch.EventType, u *unstructured.Unstructured) error
	// BeginReplace opens a streaming full-LIST reconcile — the driver streams each
	// page through the returned session, so a whole kind's bodies never sit in
	// memory at once (see kindDriver.fullList). The session's first WritePage durably
	// DELETES the kind's resume cookie (in the same transaction as the first rows),
	// so the cookie means "a full LIST completed on disk": a pass that fails after
	// writing a page leaves no cookie (Commit rewrites it only on success), so a
	// restart re-lists in full instead of resuming from an RV that predates the
	// partially-written rows. Clearing on the first write, not on open, means a pass
	// that fails before any page is written leaves the untouched snapshot's cookie
	// intact — the next start still resumes cheaply. An abandoned session needs no
	// teardown — just drop it (its pages stay durable; see each implementation for
	// how a later pass reconciles them).
	BeginReplace(ctx context.Context) (replaceSession, error)
	// PersistRV advances the kind's resume cookie without touching object rows —
	// called on every watch delta so a wake resumes from the latest position.
	PersistRV(ctx context.Context, rv string) error
	// ResumeRV returns the resume cookie to seed a RetryWatcher from, or "" to
	// force a cold full-LIST. The store owns the "should I resume?" decision:
	// objectsStore returns it only while the kind's objects still exist (a cookie
	// that outlived its objects can't apply deltas), while eventsStore — its own
	// table — resumes unguarded.
	ResumeRV(ctx context.Context) (string, error)
}

// replaceSession streams a paginated full LIST into a store — a whole kind's
// bodies never sit in memory at once. Whether it prunes rows absent from the LIST
// is a per-implementation choice: objectsReplaceSession does (the cache should
// mirror the cluster); eventsReplaceSession deliberately does not (see its doc —
// pruning against the apiserver's already-GC'd view would defeat the cache's
// longer event retention).
type replaceSession interface {
	// WritePage lands one page in its own transaction.
	WritePage(items []*unstructured.Unstructured) error
	// Commit finalizes the pass — pruning rows absent from the LIST when the
	// implementation does that — and persists the resume cookie at rv.
	Commit(rv string) error
}

// metadataDiffStore is the optional extension for kinds that participate in the
// metadata-first diff resync. Only objectsStore implements it — events have no
// resource_version column and no delete-missing, so fullResync type-asserts this
// rather than carrying SnapshotRVs/DeleteByUID (and a capability bool) on every
// store.
type metadataDiffStore interface {
	kindStore
	SnapshotRVs(ctx context.Context) (map[string]string, error)
	DeleteByUID(ctx context.Context, uid string) error
}

var (
	_ kindStore         = (*objectsStore)(nil)
	_ kindStore         = (*eventsStore)(nil)
	_ metadataDiffStore = (*objectsStore)(nil)
)

// kindDriver owns the resume/full-resync state machine for one GVR.
type kindDriver struct {
	src   kubeSource
	store kindStore
	gvk   schema.GroupVersionKind
	gvr   schema.GroupVersionResource

	seedRV string // persisted resume point at construction

	// onWatch, when set, fires when this driver's watch first proves live — reporting
	// whether it fell back to a full re-sync and how many bodies it re-pulled. The engine
	// counts these to flip Syncing→Watching and aggregates the re-sync work for the
	// ResyncComplete message. Fired by fireOnWatch (see watchPhase), guarded to once per
	// catch-up episode by sawWatch. It is NOT strictly once-per-Run: a stuck transition
	// re-arms it (recordFailure clears sawWatch) so a kind that caught up, went stuck, then
	// recovered re-fires onWatch to re-count its milestone token — the engine's re-sync
	// aggregation guards itself against double-counting the repeat.
	onWatch  func(resynced bool, objects int)
	sawWatch bool
	// onStuck, when set, fires when this driver exhausts its sync error budget
	// (stuckThreshold consecutive failures — a failed fullResync OR a watch that never
	// established). The engine wires it to retire the driver's catch-up token as
	// not-synced, so a stuck INITIAL driver stops blocking the Syncing→Watching milestone
	// (nil for a mid-run driver, which owes no token — the liveness monitor still surfaces
	// it via the stuck flag). It need not be one-shot: the production callback is the
	// already-idempotent retire token, and Run fires it only on the exact threshold
	// crossing (once per stuck episode).
	onStuck func()
	// stuck is set when the error budget is spent, cleared when the watch next proves
	// usable (fireOnWatch — a delta, a clean-resume grace, or a fresh-RV connect), NOT on
	// the bare watch open, so an open-then-error stream can't clear it. The liveness
	// monitor (staleLaggards) reads it from another goroutine, so it's atomic; the
	// consecutive-failure count that trips it is a Run-goroutine local.
	stuck atomic.Bool
	// failCause records the most recent sync failure's cause (CauseListFailed vs
	// CauseWatchFailed) so a stuck driver can report WHY it's stuck (see stuckCause,
	// read by staleLaggards from the monitor goroutine). Written on every recordFailure,
	// so by the time stuck flips it holds the failure that spent the budget. atomic.Value
	// because run and monitor goroutines touch it.
	failCause          atomic.Value // StaleCause
	stuckThreshold     int
	stuckRetryInterval time.Duration
	diffThreshold      int
	backoffInit        time.Duration
	backoffMax         time.Duration
	// resumeGrace bounds how long the first cookie-seeded watch waits to prove usable
	// before onWatch reports a clean resume — long enough that a prompt 410 reliably
	// arrives first. Armed from the watch CONNECTING, not phase entry (see watchPhase).
	resumeGrace time.Duration
	// establishTimeout bounds how long a watch phase waits for the watch to connect before
	// giving up (see defaultEstablishTimeout) so a never-connecting watch can't block forever.
	establishTimeout time.Duration
	// resyncPeriod, when > 0, forces a periodic full re-sync: a watch alive this long
	// ends itself so Run falls back to fullResync, reconciling any drift the watch
	// silently missed (a dropped delta, a stale cookie). This is the pull-based backstop
	// — the correctness guarantee behind the best-effort watch. 0 disables it (tests /
	// unconfigured). resyncDelay is the jittered interval derived from it once at
	// construction (see gvkJitterFraction) so drivers don't relist in lockstep.
	resyncPeriod time.Duration
	resyncDelay  time.Duration
	// limiter, when non-nil, bounds how many drivers run a full LIST/resync at once
	// across the engine run (see listConcurrency) — acquired around fullResync so the
	// slot is held only for the list-heavy work, not the indefinite watch phase, keeping
	// cold-start peak memory O(N pages) instead of scaling with the kind count. nil in
	// unit tests that drive one driver directly.
	limiter listLimiter
	// graceTimer starts the resumeGrace countdown, returning its fire channel and a
	// stop func; a test seam (defaults to realGraceTimer over time.NewTimer).
	graceTimer func(time.Duration) (<-chan time.Time, func())
	// establishTimer starts the establishTimeout countdown; a separate test seam from
	// graceTimer (both default to realGraceTimer) so a test can drive the establishment
	// timeout deterministically without racing the resume grace on a shared seam.
	establishTimer func(time.Duration) (<-chan time.Time, func())
	// sleep is a ctx-aware backoff sleep; a test seam so backoff is deterministic.
	sleep func(ctx context.Context, d time.Duration) error
	// now stamps the liveness time; a test seam (defaults to time.Now).
	now func() time.Time

	// lastLiveAt is when the watch last proved it's alive — a delta or a bookmark.
	// Bookmarks matter because a quiet-but-healthy cluster delivers no deltas yet
	// the server's periodic bookmarks keep this fresh, so liveAt() tells quiet from
	// wedged. Guarded by liveMu: a delta (run goroutine) and a bookmark (watch-tap
	// goroutine) can stamp it concurrently.
	liveMu     sync.Mutex
	lastLiveAt time.Time

	// cookieMu + watchEpoch fence a straggler bookmark from resurrecting the resume
	// cookie. The bookmark tap runs in watch.Filter's forwarding goroutine, which
	// nothing joins (rw.Stop doesn't wait for it), so a buffered bookmark can surface
	// after watchPhase returned and Run began a fullList whose first WritePage deletes
	// that cookie — a straggler PersistRV would resurrect it over half-written rows,
	// and a later failed pass would then wrongly resume from it. Each watch phase
	// captures the current watchEpoch and bumps it on return (before Run reaches
	// fullList, in program order); onBookmark persists only while its captured epoch
	// is still current, with the check+PersistRV held under cookieMu so it can't
	// interleave with the epoch bump. A straggler that passes the check therefore
	// always precedes the bump — and hence the fullList delete — so the delete wins.
	cookieMu   sync.Mutex
	watchEpoch int64 // guarded by cookieMu

	// didResync records whether this driver fell back to a full re-sync (its saved
	// resourceVersion was missing or expired) rather than resuming its watch
	// directly; resyncObjects counts the bodies it re-pulled. The engine reads them
	// at the catch-up handoff to tell a clean reconnect from a resume that re-listed.
	// Written only from the Run goroutine, so no locking.
	didResync     bool
	resyncObjects int
}

// option configures a kindDriver's test seams. Production never tunes them, so
// newKindDriver exposes none — mirroring the unexported-option pattern in
// internal/cloud/prefsync and internal/poke.
type option func(*kindDriver)

func withSleep(fn func(context.Context, time.Duration) error) option {
	return func(d *kindDriver) { d.sleep = fn }
}

func withDiffThreshold(n int) option {
	return func(d *kindDriver) { d.diffThreshold = n }
}

func withNow(fn func() time.Time) option {
	return func(d *kindDriver) { d.now = fn }
}

func withGraceTimer(fn func(time.Duration) (<-chan time.Time, func())) option {
	return func(d *kindDriver) { d.graceTimer = fn }
}

// withEstablishTimer injects the establishment-timeout timer so a test can drive it
// deterministically (defaults to realGraceTimer).
func withEstablishTimer(fn func(time.Duration) (<-chan time.Time, func())) option {
	return func(d *kindDriver) { d.establishTimer = fn }
}

// withGVR sets the driver's resource endpoint — the reconciler's repoint key. Both
// production (from the discovered gvrEntry) and tests that seed drivers into a
// driverSet pass it, so a seeded driver carries its GVR like a real one.
func withGVR(gvr schema.GroupVersionResource) option {
	return func(d *kindDriver) { d.gvr = gvr }
}

// withResyncPeriod sets the periodic full-resync interval (see resyncPeriod). The
// engine sets production's period; tests use a short one to exercise the resync path.
func withResyncPeriod(d time.Duration) option {
	return func(kd *kindDriver) { kd.resyncPeriod = d }
}

// withListLimiter shares the engine run's full-LIST concurrency limiter with this
// driver. The engine hands the same limiter to every driver it starts (initial and
// mid-run), so they collectively never exceed listConcurrency in-flight LISTs.
func withListLimiter(l listLimiter) option {
	return func(d *kindDriver) { d.limiter = l }
}

// withStuckThreshold tunes the resync error budget; tests set a small threshold to reach
// the stuck state quickly.
func withStuckThreshold(n int) option {
	return func(d *kindDriver) { d.stuckThreshold = n }
}

func newKindDriver(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string) *kindDriver {
	return newKindDriverWithOptions(src, store, gvk, seedRV)
}

func newKindDriverWithOptions(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string, opts ...option) *kindDriver {
	d := &kindDriver{
		src:                src,
		store:              store,
		gvk:                gvk,
		seedRV:             seedRV,
		diffThreshold:      defaultDiffThreshold,
		backoffInit:        1 * time.Second,
		backoffMax:         30 * time.Second,
		resumeGrace:        defaultResumeGrace,
		establishTimeout:   defaultEstablishTimeout,
		stuckThreshold:     defaultStuckThreshold,
		stuckRetryInterval: defaultStuckRetryInterval,
		graceTimer:         realGraceTimer,
		establishTimer:     realGraceTimer,
		sleep:              ctxSleep,
		now:                time.Now,
	}
	for _, o := range opts {
		o(d)
	}
	if d.resyncPeriod > 0 {
		// Deterministic per-kind jitter (up to 25% earlier) so drivers created together
		// don't all relist at the same instant. Computed once — it depends only on the
		// GVK and resyncPeriod, both fixed here.
		d.resyncDelay = d.resyncPeriod - time.Duration(float64(d.resyncPeriod)*0.25*gvkJitterFraction(d.gvk))
	}
	return d
}

// gvkJitterFraction maps a GVK to a stable value in [0,1) — deterministic (hashed, not
// random) so the resync jitter is reproducible in tests and stable across restarts.
func gvkJitterFraction(gvk schema.GroupVersionKind) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(gvk.String()))
	return float64(h.Sum32()) / (1 << 32)
}

// Run blocks until ctx is cancelled, resuming from the seed RV when possible and
// falling back to a metadata-first full re-sync otherwise.
func (d *kindDriver) Run(ctx context.Context) error {
	rv := d.seedRV
	backoff := d.backoffInit
	failures := 0 // consecutive sync failures (failed resync OR never-established watch) — the error budget
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// A full re-sync is needed when we have no resume point (cold cache or a
		// prior 410). RetryWatcher rejects RV "" / "0", so a re-sync that yields
		// no usable RV is treated as a failure and backs off like any other.
		if rv == "" || rv == "0" {
			newRV, err := d.fullResync(ctx)
			if err == nil && (newRV == "" || newRV == "0") {
				err = errors.New("clustersync: re-sync returned no usable resourceVersion")
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// The LIST failed — charge the error budget (markStuck on the threshold
				// crossing) and back off. Stuck drops to the slow cadence so a broken kind
				// doesn't hammer the API server or re-take a limiter slot every step.
				failures = d.recordFailure(failures, CauseListFailed)
				slog.Warn("clustersync: full re-sync failed, backing off",
					"gvk", d.gvk.String(), "err", err, "failures", failures)
				if serr := d.sleep(ctx, d.retryDelay(backoff)); serr != nil {
					return serr
				}
				backoff = d.nextBackoff(backoff)
				continue
			}
			rv = newRV
		}

		progressed, established, err := d.watchPhase(ctx, rv)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The watch establishing lets it make progress, so reset the consecutive-failure
		// budget (the stuck FLAG clears separately, only once the watch proves usable —
		// see fireOnWatch). A watch that NEVER established (LIST works but
		// WATCH is denied/refused — list-but-not-watch RBAC, an aggregated API rejecting
		// watch) is charged: fullResync alone can't keep the cache current, so the kind
		// must eventually surface as stuck rather than look healthy while frozen.
		if established {
			failures = 0
		} else {
			failures = d.recordFailure(failures, CauseWatchFailed)
		}
		// A watch that delivered events is healthy — reset the backoff and re-sync
		// immediately. Otherwise back off (the slow stuck cadence once stuck) so we don't
		// hot-loop full LISTs.
		if progressed {
			backoff = d.backoffInit
		} else {
			switch {
			case errors.Is(err, errExpired):
				slog.Debug("clustersync: watch RV expired, full re-sync", "gvk", d.gvk.String())
			case errors.Is(err, errWatchNotEstablished):
				slog.Warn("clustersync: watch failed to establish, backing off",
					"gvk", d.gvk.String(), "timeout", d.establishTimeout)
			default:
				slog.Warn("clustersync: watch ended without progress, backing off", "gvk", d.gvk.String())
			}
			if serr := d.sleep(ctx, d.retryDelay(backoff)); serr != nil {
				return serr
			}
			backoff = d.nextBackoff(backoff)
		}
		// Clear the seed RV so the next iteration re-syncs — EXCEPT when the watch only
		// failed to ESTABLISH. There the RV we just held is still valid (nothing expired
		// it; the connection never came up), so keep it and retry the watch from it next
		// iteration rather than re-running a full metadata re-list of the whole kind every
		// cycle — which, once the kind is stuck at the slow retry cadence, would otherwise
		// repeat forever for a list-OK/watch-denied kind. A watch that connected and then
		// expired (errExpired) or dropped without progress still clears it; a genuinely
		// stale retained RV surfaces as a 410 on the retry and re-syncs then.
		if !errors.Is(err, errWatchNotEstablished) {
			rv = ""
		}
	}
}

func (d *kindDriver) nextBackoff(b time.Duration) time.Duration {
	return stepBackoff(b, d.backoffMax)
}

// retryDelay is the sleep between resync attempts: the normal exponential backoff until
// the kind is stuck, then the slow stuck cadence so a permanently-broken kind stops
// hammering the API server (and re-taking a limiter slot) every backoff step.
func (d *kindDriver) retryDelay(backoff time.Duration) time.Duration {
	if d.stuck.Load() {
		return d.stuckRetryInterval
	}
	return backoff
}

// recordFailure charges one sync failure — a failed fullResync (CauseListFailed), or a
// watch that never established (CauseWatchFailed) — against the error budget, returning
// the new consecutive-failure count. It records the cause each time (see failCause /
// stuckCause) so a kind that goes stuck reports the phase that spent its budget. On the
// exact threshold crossing it marks the kind stuck (the flag the liveness monitor reads
// to surface it, see staleLaggards) and fires onStuck, which releases the driver's
// catch-up token so a stuck INITIAL driver stops blocking the Syncing→Watching milestone.
// The flag is cleared when the watch next proves usable (fireOnWatch, re-armed below via
// sawWatch); onStuck (the idempotent retire token) is moot after catch-up. Firing on the
// exact crossing keeps it once-per stuck episode without a sync.Once.
func (d *kindDriver) recordFailure(failures int, cause StaleCause) int {
	failures++
	d.failCause.Store(cause)
	if failures == d.stuckThreshold {
		d.stuck.Store(true)
		slog.Warn("clustersync: kind stuck after repeated sync failures",
			"gvk", d.gvk.String(), "failures", failures)
		// Re-arm the catch-up fire. onStuck (below) de-counts this kind's milestone token
		// (retire.resolve(false)); if the watch later re-establishes before the milestone
		// fires, the next watchPhase must fire onWatch again to re-count it — but fireOnWatch
		// is one-shot via sawWatch, so clear it here. Harmless when the kind never caught up
		// (sawWatch already false); the engine's onWatch aggregation guards against
		// double-counting the re-sync facts across the repeat. All in the Run goroutine.
		d.sawWatch = false
		if d.onStuck != nil {
			d.onStuck()
		}
	}
	return failures
}

// isStuck reports whether the kind has exhausted its resync error budget; read by the
// liveness monitor from another goroutine.
func (d *kindDriver) isStuck() bool { return d.stuck.Load() }

// stuckCause returns the cause of the current stuck episode — the most recent failure's
// phase (see failCause). Meaningful only while isStuck. Returns the zero StaleCause ("")
// when no failure has been recorded — an honestly-empty "unknown" rather than a specific
// but possibly-wrong cause. A stuck driver always has a recorded cause, so the empty case
// is unreachable in practice; keeping it honest guards a future path that sets stuck
// without recording a failure.
func (d *kindDriver) stuckCause() StaleCause {
	v, _ := d.failCause.Load().(StaleCause)
	return v
}

// stepBackoff doubles b and caps it at max — the package's one exponential-backoff
// step, shared by the driver, the engine run loop, and the discovery-trigger watch.
func stepBackoff(b, max time.Duration) time.Duration {
	if b *= 2; b > max {
		b = max
	}
	return b
}

// fireOnWatch reports the catch-up milestone for this driver — once per catch-up
// episode, guarded by sawWatch (which recordFailure re-arms on a stuck transition so a
// recovery re-counts) — snapshotting its resync facts (didResync/resyncObjects) in the
// Run goroutine so the engine's aggregation never races a later re-sync write.
//
// This is the driver's usability proof — the watch is proven live (a delta, a clean
// resume grace, or a fresh-RV connect) — so it is also where the stuck flag clears, NOT
// on the bare connect (onConnect). recordFailure re-arms sawWatch when it marks a kind
// stuck, so stuck⟹sawWatch=false ⟹ pendingFire on the next phase ⟹ fireOnWatch runs on
// recovery and clears stuck; a kind that connects but never proves usable stays stuck.
func (d *kindDriver) fireOnWatch() {
	d.sawWatch = true
	d.stuck.Store(false) // proven usable — clear any surfaced-stale
	if d.onWatch != nil {
		d.onWatch(d.didResync, d.resyncObjects)
	}
}

// watchPhase resumes the watch from rv via a RetryWatcher and applies deltas until
// ctx cancellation, a 410 (errExpired), or the RetryWatcher giving up (nil — Run
// re-syncs either way). It reports `progressed` (applied at least one delta) and
// `established` (the watch actually connected this phase). Run uses `progressed` to tell
// a healthy watch that dropped from one that never worked, and `established` to charge a
// never-connecting watch (list-but-not-watch RBAC) against the error budget. RetryWatcher
// requests bookmarks but doesn't forward them, so the driver taps the underlying watch
// ahead of it (watcherFor's tapEvent) to observe them — bumping liveness and persisting
// the bookmark RV, but only once the deltas before it have applied (see onBookmark).
// Object deltas persist their RV inline below.
func (d *kindDriver) watchPhase(ctx context.Context, rv string) (progressed, established bool, err error) {
	// Capture this phase's epoch and retire it on return (before Run moves on to a
	// fullList, in program order) so a straggler bookmark from this phase's tap can't
	// resurrect the resume cookie — see cookieMu/watchEpoch.
	d.cookieMu.Lock()
	epoch := d.watchEpoch
	d.cookieMu.Unlock()
	defer func() {
		d.cookieMu.Lock()
		d.watchEpoch++
		d.cookieMu.Unlock()
	}()

	// connected records whether the watch established this phase — onConnect fires only
	// after src.Watch succeeds, so a watch that's consistently denied/refused leaves it
	// false. It's the liveness signal too: we stamp `lastLiveAt` here (on real
	// establishment) and on each delta/bookmark, NOT merely on entering the phase — so a
	// kind that can LIST but never WATCH doesn't keep refreshing its liveness every backoff
	// cycle and hide as healthy. Set from the RetryWatcher goroutine, hence atomic.
	var connected atomic.Bool
	// connectedC wakes the select loop the instant the watch establishes. onConnect runs in
	// the RetryWatcher goroutine, so it can't call fireOnWatch (which must snapshot the
	// resync facts in the Run goroutine) directly — it signals here instead. Buffered and
	// non-blocking so a reconnect never blocks onConnect; the loop coalesces repeats via
	// pendingFire.
	connectedC := make(chan struct{}, 1)
	onConnect := func() {
		connected.Store(true)
		// NOTE: do NOT clear the stuck flag here. A watch that merely OPENS the HTTP
		// stream hasn't proven usable — it can 410 or error immediately (an expired
		// cookie, a flapping aggregated API), and clearing stuck on the bare open would
		// let the milestone / liveness monitor report Watching for a kind that still
		// can't watch, and let an open-then-error loop keep a stuck kind looking healthy.
		// The stuck flag clears at the same usability proof that fires onWatch (a delta,
		// a clean-resume grace, or a fresh-RV connect) — see fireOnWatch. markLive stays,
		// though: a successful (re)open is the liveness signal a quiet cluster needs (see
		// watcherFor), and a genuinely stuck kind is still surfaced by staleLaggards'
		// isStuck() branch, which dominates the liveness check.
		d.markLive()
		select {
		case connectedC <- struct{}{}:
		default:
		}
	}
	// Per-phase delta accounting. deltaSeen counts object deltas the tap has forwarded;
	// deltaApplied counts those THIS phase has durably applied+persisted. A bookmark
	// advances the resume cookie only once applied has caught up to seen, so a restart
	// never resumes past a delta the cache hasn't stored yet. Scoped to this phase
	// (captured by the tap closure and the apply loop below) rather than the driver: a
	// straggler event from a prior phase — running in watch.Filter's un-joined forwarding
	// goroutine — bumps its own now-dead counters, never these, so it can't leave a
	// permanent seen>applied gap that would wedge cookie advancement for the rest of the
	// run. Both are touched from the tap and Run goroutines, hence atomic.
	var deltaSeen, deltaApplied atomic.Int64
	watcher := watcherFor(d.src, onConnect, func(ev watch.Event) { d.tapEvent(ctx, epoch, &deltaSeen, &deltaApplied, ev) })
	rw, err := toolswatch.NewRetryWatcherWithContext(ctx, rv, watcher)
	if err != nil {
		// e.g. an unusable RV; let Run fall to a full re-sync.
		return false, connected.Load(), err
	}
	defer rw.Stop()

	// The periodic pull-based backstop: a watch alive this long ends itself so Run
	// falls back to fullResync, reconciling any drift the best-effort watch silently
	// missed (a dropped delta, a stale resume cookie). Recreated fresh each watch phase,
	// so a natural drop+resync resets the clock — resync fires resyncPeriod after the
	// last sync, like a client-go informer. Disabled (nil channel) when resyncPeriod is 0.
	var resyncC <-chan time.Time
	if d.resyncPeriod > 0 {
		rt := time.NewTimer(d.resyncDelay)
		defer rt.Stop()
		resyncC = rt.C
	}

	// Bound the wait for the watch to CONNECT this phase. RetryWatcher retries retriable
	// errors (5xx, EOF, timeout) internally without ever closing ResultChan, and a hung watch
	// request never returns, so without this the phase could block forever — never returning,
	// never spending the error budget, holding its catch-up token and wedging the engine in
	// Syncing. On timeout we return so Run charges a CauseWatchFailed and, after enough
	// failures, surfaces the kind as stuck. Disarmed the moment the watch connects; a
	// post-connect reconnect wedge is the liveness monitor's job (CauseWatchStalled), not this.
	establishC, establishStop := d.establishTimer(d.establishTimeout)
	defer establishStop()

	// Fire onWatch (the catch-up milestone + resync-facts snapshot) exactly once per Run, but
	// never before the watch is proven live — retiring the token early would report a clean
	// sync a kind never achieved. Both proofs start from the watch CONNECTING (onConnect →
	// connectedC); the resume-grace timer is armed from that connection, not phase entry, so a
	// slow or denied watch can't fire a false clean-resume before any watch exists:
	//
	//   - A re-sync already ran this iteration (cold start, or the re-entry after a 410): the
	//     RV is fresh and can't 410, so connecting IS the catch-up proof — fire on connect.
	//     Firing on entry instead would retire the token even when LIST is allowed but WATCH is
	//     forbidden (list-but-not-watch RBAC), reporting Watching for a kind with no watch.
	//   - Resuming straight from the saved cookie: an accepted watch can still 410 (expired
	//     cookie), so connecting isn't enough — on connect, wait a resumeGrace for the first
	//     delta or a quiet interval with no 410 before reporting a clean resume, and skip the
	//     fire entirely if the watch 410s first (Run re-syncs and re-enters didResync=true).
	var graceC <-chan time.Time
	var graceStop func()
	defer func() {
		if graceStop != nil {
			graceStop()
		}
	}()
	pendingFire := !d.sawWatch

	sawExpired := false
	for {
		select {
		case <-ctx.Done():
			return progressed, connected.Load(), ctx.Err()
		case <-establishC:
			// The watch didn't connect within establishTimeout (RetryWatcher may be looping on
			// a retriable error, or the request is wedged). Return so Run charges the failure
			// rather than blocking here forever. Guard the connect race: if onConnect landed as
			// the timer fired, treat it as connected and carry on.
			if connected.Load() {
				establishC = nil
				continue
			}
			return progressed, false, errWatchNotEstablished
		case <-graceC:
			// Connected, then no delta and no 410 within the grace — a clean resume. Fire.
			d.fireOnWatch()
			pendingFire, graceC = false, nil
		case <-connectedC:
			// The watch connected. Disarm the establishment timeout and start the fire path.
			establishC = nil
			if pendingFire {
				if d.didResync {
					// Fresh post-resync RV can't 410 — connecting IS the catch-up proof.
					d.fireOnWatch()
					pendingFire = false
				} else if graceC == nil {
					// Cookie resume: arm the grace FROM the connection. A delta or a quiet grace
					// with no 410 then reports the clean resume; a 410 first disqualifies it.
					graceC, graceStop = d.graceTimer(d.resumeGrace)
				}
			}
		case <-resyncC:
			// Periodic full re-sync fell due (the pull-based backstop). End the watch so
			// Run re-syncs. Report progress so Run re-syncs immediately without backoff —
			// a watch alive this long is healthy by definition.
			return true, connected.Load(), nil
		case ev, ok := <-rw.ResultChan():
			if !ok {
				if sawExpired {
					return progressed, connected.Load(), errExpired
				}
				return progressed, connected.Load(), nil
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				// The first forwarded delta proves the watch is live; fire a still-pending
				// onWatch now rather than waiting out the grace.
				if pendingFire {
					d.fireOnWatch()
					pendingFire, graceC = false, nil
				}
				u, ok := ev.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if err := d.store.ApplyChange(ev.Type, u); err != nil {
					slog.Warn("clustersync: apply watch change", "gvk", d.gvk.String(), "err", err)
					continue
				}
				progressed = true
				d.markLive()
				if rvNow := u.GetResourceVersion(); rvNow != "" {
					if err := d.store.PersistRV(ctx, rvNow); err != nil {
						slog.Warn("clustersync: persist rv", "gvk", d.gvk.String(), "err", err)
					}
				}
				// This delta is now durable — let a pending bookmark advance past it.
				deltaApplied.Add(1)
			case watch.Error:
				// RetryWatcher forwards the Status then closes the channel. A 410
				// means our RV expired; anything else it treats as fatal too.
				if isExpired(ev.Object) {
					sawExpired = true
				}
			}
		}
	}
}

// fullResync reconciles the whole kind. For kinds that support the metadata diff
// (everything but events) it lists metadata only, fetches bodies for just the
// new/changed objects, and deletes the vanished ones — unless the changed set is
// large or the cache is empty, where one full LIST is cheaper. Events take the
// plain full-LIST path. Returns the resourceVersion to seed the watch from.
func (d *kindDriver) fullResync(ctx context.Context) (string, error) {
	// Bound concurrent full LISTs across the run's drivers: a cold driver goes straight
	// to a (paginated) full LIST — and even the metadata-diff path below lists all
	// metadata at once — so without this cold-start peak memory would scale with the
	// kind count. The slot is held only for this list-heavy work (released before Run
	// re-enters the indefinite watch phase), so N drivers list at a time and the rest
	// queue. A nil limiter (unit tests) imposes no bound.
	release, err := d.limiter.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	// Reaching fullResync means this kind couldn't resume its watch directly (no
	// saved resource-version, or it expired) — record that for the catch-up report.
	d.didResync = true
	md, ok := d.store.(metadataDiffStore)
	if !ok {
		return d.fullList(ctx) // events: no metadata diff
	}

	have, err := md.SnapshotRVs(ctx)
	if err != nil {
		return "", err
	}
	// Cold cache (initial sync): a full LIST is strictly cheaper than a metadata
	// list plus N GETs, so skip the metadata round trip entirely.
	if len(have) == 0 {
		return d.fullList(ctx)
	}

	metas, listRV, err := d.src.ListMetadata(ctx, metav1.ListOptions{})
	if err != nil {
		// Some aggregated APIs don't serve a metadata endpoint; fall back to a
		// full LIST rather than failing the kind.
		slog.Debug("clustersync: metadata list failed, full LIST", "gvk", d.gvk.String(), "err", err)
		return d.fullList(ctx)
	}

	var changed []objMeta
	seen := make(map[string]struct{}, len(metas))
	for _, m := range metas {
		seen[m.UID] = struct{}{}
		if have[m.UID] != m.ResourceVersion {
			changed = append(changed, m)
		}
	}

	// A big delta: one LIST beats N GETs.
	if len(changed) > d.diffThreshold {
		return d.fullList(ctx)
	}

	for _, m := range changed {
		u, err := d.src.Get(ctx, m.Namespace, m.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Raced a delete between the metadata list and the GET; the
				// missing-pass (next cycle) reconciles it.
				continue
			}
			return "", err
		}
		if err := d.store.ApplyChange(watch.Modified, u); err != nil {
			return "", err
		}
		d.resyncObjects++
	}
	for uid := range have {
		if _, ok := seen[uid]; !ok {
			if err := md.DeleteByUID(ctx, uid); err != nil {
				return "", err
			}
		}
	}
	if err := d.store.PersistRV(ctx, listRV); err != nil {
		return "", err
	}
	return listRV, nil
}

// listPageSize bounds how many objects one cold-LIST page pulls, so a kind's full
// bodies stream into the store a page at a time instead of materializing at once —
// the memory bound that makes a cold start / large-churn relist safe. maxListRestarts
// caps the Continue-token-expiry restarts before fullList gives up (errListRestartBudget),
// so persistent expiry surfaces to Run's error budget rather than looping forever.
const (
	listPageSize    = 500
	maxListRestarts = 3
)

// errListRestartBudget means a paginated full LIST couldn't finish within the
// continue-token lifetime even after maxListRestarts re-lists from the top — a kind so
// large (relative to the cluster's token TTL) that pagination can't complete. We return
// this rather than falling back to a single unpaginated LIST: that would load the whole
// kind's bodies into memory at once, the exact blow-up pagination exists to avoid, and
// on a multi-million-object kind could OOM the sidecar. Run counts it toward the kind's
// error budget (see recordFailure) and, once spent, marks the kind stuck so it's
// surfaced to the user instead of retried hot.
var errListRestartBudget = errors.New("clustersync: continue token kept expiring; kind too large to paginate within its lifetime")

// fullList streams a paginated full LIST into the store: each page lands as it
// arrives (bounding memory to one page of bodies), and a final Commit finalizes the
// pass (pruning rows absent from every page, for stores that do — see
// replaceSession) and persists the resume cookie. A Continue token can expire (410)
// mid-pagination — the driver discards the partial pass and re-lists from the top
// (matching a client-go Reflector), bounded by maxListRestarts. Once that budget is
// spent (a token that keeps expiring at the same point — a kind whose full paginated
// pass outlives the token lifetime), it returns errListRestartBudget rather than
// loading the whole kind unpaginated; the caller's error budget then surfaces the kind.
func (d *kindDriver) fullList(ctx context.Context) (string, error) {
	// BeginReplace opens the session (whose first WritePage invalidates the resume
	// cookie, rewritten only by a successful Commit), so an exit at any point below —
	// error return or 410 restart — just drops the session: any cookie left is
	// consistent with whatever pages were written, the next start cold-LISTs when
	// partial pages exist, and (for objects) that re-list's Commit prune reconciles
	// them. A warm big-delta relist that fails after writing a page drops to cold next
	// time — correctness over latency, bounded by resyncPeriod.
	sess, err := d.store.BeginReplace(ctx)
	if err != nil {
		return "", err
	}

	opts := metav1.ListOptions{Limit: listPageSize}
	var lastRV string
	listed, restarts := 0, 0
	for {
		// The request's read-inactivity timeout lives in the driver clients' transport
		// (see idleTimeoutRoundTripper), so a slow-but-progressing page runs as long as it
		// keeps streaming while a wedged one is cancelled — no wall-clock deadline here.
		items, cont, rv, err := d.src.List(ctx, opts)
		if err != nil {
			if listExpired(err) && opts.Continue != "" {
				if restarts >= maxListRestarts {
					// The paginated pass can't finish inside the token lifetime. Give up —
					// the error budget surfaces the kind rather than us loading it whole.
					return "", errListRestartBudget
				}
				// Discard the partial pass and re-list from the top with a fresh session.
				if sess, err = d.store.BeginReplace(ctx); err != nil {
					return "", err
				}
				opts.Continue, listed = "", 0
				restarts++
				continue
			}
			return "", err
		}
		if err := sess.WritePage(items); err != nil {
			return "", err
		}
		listed += len(items)
		lastRV = rv
		if cont == "" {
			break
		}
		opts.Continue = cont
	}
	if err := sess.Commit(lastRV); err != nil {
		return "", err
	}
	d.resyncObjects += listed
	return lastRV, nil
}

// listExpired reports whether a LIST error is a continue-token expiry (410). A stock
// kube-apiserver returns ResourceExpired (reason "Expired"); a nonconforming
// intermediary may answer an expired continue token with a bare 410 Gone, so accept
// that too — matching the watch path's isExpired.
func listExpired(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// isExpired reports whether a watch.Error event object is a 410 Gone /
// ResourceExpired status (our RV is too old to resume from).
func isExpired(obj runtime.Object) bool {
	err := apierrors.FromObject(obj)
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// markLive stamps the driver's liveness time — the watch just proved it's alive
// (a delta, a bookmark, or a fresh (re)connect). Safe to call from the run and
// watch-tap goroutines.
func (d *kindDriver) markLive() {
	t := d.now()
	d.liveMu.Lock()
	d.lastLiveAt = t
	d.liveMu.Unlock()
}

// liveAt returns when the watch last proved it's alive; the zero time until the
// first delta or bookmark. The engine reads this across all drivers to decide
// whether the cache is still current (freshness) or has gone stale.
func (d *kindDriver) liveAt() time.Time {
	d.liveMu.Lock()
	defer d.liveMu.Unlock()
	return d.lastLiveAt
}

// tapEvent inspects one raw watch event the tap observed ahead of RetryWatcher
// (which is why it runs in the tap's goroutine, not the apply loop). Object
// deltas bump seen — the ordered high-water mark a bookmark checks against —
// while bookmarks stamp liveness and conditionally advance the resume cookie. seen
// and applied are the current watch phase's counters (see watchPhase), so a
// straggler tap from an already-ended phase touches only that phase's counters.
func (d *kindDriver) tapEvent(ctx context.Context, epoch int64, seen, applied *atomic.Int64, ev watch.Event) {
	switch ev.Type {
	case watch.Added, watch.Modified, watch.Deleted:
		seen.Add(1)
	case watch.Bookmark:
		d.onBookmark(ctx, epoch, seen, applied, resourceVersionOf(ev.Object))
	}
}

// onBookmark handles a bookmark the watch tap observed: stamp liveness and advance
// the persisted resume cookie (so a cold restart resumes closer to head even on a
// quiet cluster). The cookie only moves once every delta the tap forwarded before
// this bookmark has applied (applied caught up to the seen snapshot): the server
// places a bookmark at/after all prior events, so advancing while a delta is
// un-applied would let a restart skip it permanently. Deltas in flight just defer
// the advance to the next bookmark. Liveness is stamped regardless. seen/applied are
// this watch phase's counters, so a straggler bookmark from an ended phase compares
// that phase's own (dead) counts and never wedges a later phase's advancement.
// Best-effort persistence — a write error is logged and swallowed. epoch is the
// watch phase that spawned this tap; the cookie only moves while that phase is still
// current (see cookieMu/watchEpoch), so a straggler surfacing after watchPhase
// returned can't resurrect a cookie a concurrent fullList deleted.
func (d *kindDriver) onBookmark(ctx context.Context, epoch int64, seen, applied *atomic.Int64, rv string) {
	if rv == "" {
		return
	}
	// Snapshot the high-water mark before marking live. The tap is single-threaded,
	// so no new delta is forwarded while this runs — seen is frozen at exactly
	// the count of deltas preceding this bookmark.
	n := seen.Load()
	d.markLive()
	if applied.Load() < n {
		return
	}
	d.cookieMu.Lock()
	defer d.cookieMu.Unlock()
	if d.watchEpoch != epoch {
		// This tap's watch phase ended; Run may be mid-fullList (cookie deleted).
		// Persisting now would resurrect a stale cookie over half-written rows.
		return
	}
	if err := d.store.PersistRV(ctx, rv); err != nil {
		slog.Warn("clustersync: persist bookmark rv", "gvk", d.gvk.String(), "err", err)
	}
}

// watcherFor adapts a kubeSource to the cache.WatcherWithContext that
// NewRetryWatcherWithContext consumes (RetryWatcher fills in RV + bookmarks). It
// wraps each opened watch in a tap so the driver observes every event — including
// the bookmarks RetryWatcher swallows — ahead of RetryWatcher, passing each through
// unchanged so its reconnect/RV bookkeeping is untouched. onConnect fires on every
// successful (re)open: RetryWatcher reconnects by re-invoking this func, so a fresh
// replacement watch is itself proof of liveness before its first event — what a
// quiet cluster needs so a benign reconnect isn't misread as stale.
func watcherFor(src kubeSource, onConnect func(), tap func(watch.Event)) cache.WatcherWithContext {
	return watcherFunc(func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
		w, err := src.Watch(ctx, opts)
		if err != nil {
			return nil, err
		}
		onConnect()
		return watch.Filter(w, func(in watch.Event) (watch.Event, bool) {
			tap(in)
			return in, true
		}), nil
	})
}

// resourceVersionOf extracts an object's resourceVersion, or "" if it has none /
// isn't a metadata-bearing object.
func resourceVersionOf(obj runtime.Object) string {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return ""
	}
	return acc.GetResourceVersion()
}

type watcherFunc func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)

func (f watcherFunc) WatchWithContext(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return f(ctx, opts)
}

// ctxSleep sleeps for d or until ctx is cancelled (returning ctx.Err()).
func ctxSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
