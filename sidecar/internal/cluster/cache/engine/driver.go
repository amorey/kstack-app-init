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

// realGraceTimer is the production graceTimer: a time.Timer's channel plus its Stop.
func realGraceTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// errExpired means the watch's resourceVersion is too old (410 Gone); the only
// recovery is a full re-sync to obtain a fresh RV.
var errExpired = errors.New("clustersync: watch resourceVersion expired")

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

	// onWatch, when set, fires once per Run — reporting whether this driver fell
	// back to a full re-sync and how many bodies it re-pulled. The engine counts
	// these down to flip Syncing→Watching and aggregates the re-sync work for the
	// ResyncComplete message. Fired by fireOnWatch (see watchPhase), guarded to
	// once by sawWatch.
	onWatch       func(resynced bool, objects int)
	sawWatch      bool
	diffThreshold int
	backoffInit   time.Duration
	backoffMax    time.Duration
	// resumeGrace bounds how long the first cookie-seeded watch waits to prove usable
	// before onWatch reports a clean resume — long enough that a prompt 410 reliably
	// arrives first. See watchPhase.
	resumeGrace time.Duration
	// resyncPeriod, when > 0, forces a periodic full re-sync: a watch alive this long
	// ends itself so Run falls back to fullResync, reconciling any drift the watch
	// silently missed (a dropped delta, a stale cookie). This is the pull-based backstop
	// — the correctness guarantee behind the best-effort watch. 0 disables it (tests /
	// unconfigured). resyncDelay is the jittered interval derived from it once at
	// construction (see gvkJitterFraction) so drivers don't relist in lockstep.
	resyncPeriod time.Duration
	resyncDelay  time.Duration
	// graceTimer starts the resumeGrace countdown, returning its fire channel and a
	// stop func; a test seam (defaults to realGraceTimer over time.NewTimer).
	graceTimer func(time.Duration) (<-chan time.Time, func())
	// sleep is a ctx-aware backoff sleep; a test seam so backoff is deterministic.
	sleep func(ctx context.Context, d time.Duration) error
	// now stamps the liveness time; a test seam (defaults to time.Now).
	now func() time.Time

	// createdAt is when the driver was constructed — the start of its startup grace.
	// A driver that never reaches its watch phase keeps a zero lastLiveAt; the liveness
	// monitor gives it grace from here rather than flagging it immediately, but only
	// until the grace expires (see staleLaggards) so a kind that can never LIST/watch
	// (forbidden, perpetually-unavailable API) doesn't hide as healthy forever.
	createdAt time.Time

	// lastLiveAt is when the watch last proved it's alive — a delta or a bookmark.
	// Bookmarks matter because a quiet-but-healthy cluster delivers no deltas yet
	// the server's periodic bookmarks keep this fresh, so liveAt() tells quiet from
	// wedged. Guarded by liveMu: a delta (run goroutine) and a bookmark (watch-tap
	// goroutine) can stamp it concurrently.
	liveMu     sync.Mutex
	lastLiveAt time.Time

	// deltaSeen counts object deltas the watch tap has forwarded downstream;
	// deltaApplied counts those the driver has durably applied+persisted. A bookmark
	// advances the resume cookie only once applied has caught up to seen, so a crash
	// or restart never resumes past a delta the cache hasn't stored yet. Both are
	// monotonic and touched from the tap and run goroutines, hence atomic.
	deltaSeen    atomic.Int64
	deltaApplied atomic.Int64

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

func newKindDriver(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string) *kindDriver {
	return newKindDriverWithOptions(src, store, gvk, seedRV)
}

func newKindDriverWithOptions(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string, opts ...option) *kindDriver {
	d := &kindDriver{
		src:           src,
		store:         store,
		gvk:           gvk,
		seedRV:        seedRV,
		diffThreshold: defaultDiffThreshold,
		backoffInit:   1 * time.Second,
		backoffMax:    30 * time.Second,
		resumeGrace:   defaultResumeGrace,
		graceTimer:    realGraceTimer,
		sleep:         ctxSleep,
		now:           time.Now,
	}
	for _, o := range opts {
		o(d)
	}
	d.createdAt = d.now() // start the startup grace (after opts set the now seam)
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
				slog.Warn("clustersync: full re-sync failed, backing off", "gvk", d.gvk.String(), "err", err)
				if serr := d.sleep(ctx, backoff); serr != nil {
					return serr
				}
				backoff = d.nextBackoff(backoff)
				continue
			}
			rv = newRV
		}

		progressed, err := d.watchPhase(ctx, rv)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Resume ended; recompute via a re-sync. A watch that delivered events is
		// healthy — reset the backoff and re-sync immediately. A watch that ended
		// without progress (e.g. list-but-not-watch RBAC, or an aggregated API that
		// rejects watch) backs off so we don't hot-loop full LISTs.
		if progressed {
			backoff = d.backoffInit
		} else {
			if errors.Is(err, errExpired) {
				slog.Debug("clustersync: watch RV expired, full re-sync", "gvk", d.gvk.String())
			} else {
				slog.Warn("clustersync: watch ended without progress, backing off", "gvk", d.gvk.String())
			}
			if serr := d.sleep(ctx, backoff); serr != nil {
				return serr
			}
			backoff = d.nextBackoff(backoff)
		}
		rv = ""
	}
}

func (d *kindDriver) nextBackoff(b time.Duration) time.Duration {
	return stepBackoff(b, d.backoffMax)
}

// stepBackoff doubles b and caps it at max — the package's one exponential-backoff
// step, shared by the driver, the engine run loop, and the discovery-trigger watch.
func stepBackoff(b, max time.Duration) time.Duration {
	if b *= 2; b > max {
		b = max
	}
	return b
}

// fireOnWatch reports the catch-up milestone for this driver — once per Run,
// guarded by sawWatch — snapshotting its resync facts (didResync/resyncObjects) in
// the Run goroutine so the engine's aggregation never races a later re-sync write.
func (d *kindDriver) fireOnWatch() {
	d.sawWatch = true
	if d.onWatch != nil {
		d.onWatch(d.didResync, d.resyncObjects)
	}
}

// watchPhase resumes the watch from rv via a RetryWatcher and applies deltas until
// ctx cancellation, a 410 (errExpired), or the RetryWatcher giving up (nil — Run
// re-syncs either way). It reports whether the watch made progress (applied at
// least one delta), which Run uses to tell a healthy watch that dropped from one
// that never worked. RetryWatcher requests bookmarks but doesn't forward them, so
// the driver taps the underlying watch ahead of it (watcherFor's tapEvent) to
// observe them — bumping liveness and persisting the bookmark RV, but only once the
// deltas before it have applied (see onBookmark). Object deltas persist their RV
// inline below.
func (d *kindDriver) watchPhase(ctx context.Context, rv string) (progressed bool, err error) {
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

	watcher := watcherFor(d.src, d.markLive, func(ev watch.Event) { d.tapEvent(ctx, epoch, ev) })
	rw, err := toolswatch.NewRetryWatcherWithContext(ctx, rv, watcher)
	if err != nil {
		// e.g. an unusable RV; let Run fall to a full re-sync.
		return false, err
	}
	defer rw.Stop()

	// Entering the watch phase is itself proof the watch is alive — seed liveness at
	// catch-up so a quiet kind isn't judged stale before its first bookmark arrives.
	d.markLive()

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

	// Fire onWatch (the catch-up milestone + resync-facts snapshot) exactly once per
	// Run. When a re-sync already ran this iteration (cold start, or the re-entry
	// after a 410), the RV is fresh and can't expire, so fire immediately. When
	// resuming straight from the saved cookie, the server can accept the watch and
	// only then 410 (expired cookie), so firing on entry would misreport that re-list
	// as a clean resume. So defer until the watch proves usable — the first delta, or
	// a resumeGrace with no 410 — and skip it entirely if the watch 410s first; Run
	// then re-syncs and re-enters with didResync=true, taking the immediate-fire path.
	var graceC <-chan time.Time
	pendingFire := false
	if !d.sawWatch {
		if d.didResync {
			d.fireOnWatch()
		} else {
			pendingFire = true
			ch, stop := d.graceTimer(d.resumeGrace)
			defer stop()
			graceC = ch
		}
	}

	sawExpired := false
	for {
		select {
		case <-ctx.Done():
			return progressed, ctx.Err()
		case <-graceC:
			// No delta and no 410 within the grace — the watch accepted the cookie and
			// is merely quiet, so this is a clean resume. Fire and stop watching grace.
			d.fireOnWatch()
			pendingFire, graceC = false, nil
		case <-resyncC:
			// Periodic full re-sync fell due (the pull-based backstop). End the watch so
			// Run re-syncs. Report progress so Run re-syncs immediately without backoff —
			// a watch alive this long is healthy by definition.
			return true, nil
		case ev, ok := <-rw.ResultChan():
			if !ok {
				if sawExpired {
					return progressed, errExpired
				}
				return progressed, nil
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				// The first forwarded delta proves the cookie was accepted; fire a still-
				// pending onWatch now rather than waiting out the grace.
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
				d.deltaApplied.Add(1)
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
// caps the Continue-token-expiry restarts so persistent expiry surfaces to Run's
// backoff rather than looping forever.
const (
	listPageSize    = 500
	maxListRestarts = 3
)

// fullList streams a paginated full LIST into the store: each page lands as it
// arrives (bounding memory to one page of bodies), and a final Commit finalizes the
// pass (pruning rows absent from every page, for stores that do — see
// replaceSession) and persists the resume cookie. A Continue token can expire (410)
// mid-pagination — the driver discards the partial pass and re-lists from the top
// (matching a client-go Reflector), bounded by maxListRestarts. Once that budget is
// spent (a token that keeps expiring at the same point — a kind whose full paginated
// pass outlives the token lifetime), it falls back once to an unpaginated LIST
// (client-go's FullListIfExpired): trade the per-page memory bound once for
// guaranteed completion, so the kind doesn't wedge in an endless heavy-LIST cycle.
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
	fellBack := false // whether we've already dropped to the unpaginated fallback
	for {
		items, cont, rv, err := d.src.List(ctx, opts)
		if err != nil {
			if listExpired(err) && opts.Continue != "" && !fellBack {
				// Fresh session either way — the partial pass is discarded and re-listed
				// from the top.
				if sess, err = d.store.BeginReplace(ctx); err != nil {
					return "", err
				}
				if restarts < maxListRestarts {
					opts.Continue, listed = "", 0
					restarts++
					continue
				}
				// Restart budget spent: the paginated pass can't finish inside the
				// token lifetime. Fall back once to a single unpaginated LIST (no Limit,
				// no Continue) so the kind completes instead of looping heavy LISTs
				// forever (Syncing → EngineStale, cold on disk).
				opts, listed, fellBack = metav1.ListOptions{}, 0, true
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
// deltas bump deltaSeen — the ordered high-water mark a bookmark checks against —
// while bookmarks stamp liveness and conditionally advance the resume cookie.
func (d *kindDriver) tapEvent(ctx context.Context, epoch int64, ev watch.Event) {
	switch ev.Type {
	case watch.Added, watch.Modified, watch.Deleted:
		d.deltaSeen.Add(1)
	case watch.Bookmark:
		d.onBookmark(ctx, epoch, resourceVersionOf(ev.Object))
	}
}

// onBookmark handles a bookmark the watch tap observed: stamp liveness and advance
// the persisted resume cookie (so a cold restart resumes closer to head even on a
// quiet cluster). The cookie only moves once every delta the tap forwarded before
// this bookmark has applied (deltaApplied caught up to the deltaSeen snapshot):
// the server places a bookmark at/after all prior events, so advancing while a
// delta is un-applied would let a restart skip it permanently. Deltas in flight
// just defer the advance to the next bookmark. Liveness is stamped regardless.
// Best-effort persistence — a write error is logged and swallowed. epoch is the
// watch phase that spawned this tap; the cookie only moves while that phase is still
// current (see cookieMu/watchEpoch), so a straggler surfacing after watchPhase
// returned can't resurrect a cookie a concurrent fullList deleted.
func (d *kindDriver) onBookmark(ctx context.Context, epoch int64, rv string) {
	if rv == "" {
		return
	}
	// Snapshot the high-water mark before marking live. The tap is single-threaded,
	// so no new delta is forwarded while this runs — deltaSeen is frozen at exactly
	// the count of deltas preceding this bookmark.
	seen := d.deltaSeen.Load()
	d.markLive()
	if d.deltaApplied.Load() < seen {
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
