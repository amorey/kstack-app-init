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

package kubesync

import (
	"context"
	"errors"
	"fmt"
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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	toolswatch "k8s.io/client-go/tools/watch"
)

// The driver is one collection's sync state machine: it resumes from the persisted
// cookie via a RetryWatcher (no LIST on a warm wake, unlike a client-go Reflector) and
// falls back to a paginated, pruning full LIST on a 410 or a cold cache.
//
// Shape: watch-for-latency, poll-for-correctness — the lossy watch path is backstopped
// by the periodic resync and the re-list on failure; edge cases self-heal within one
// interval. See docs/adr/2026-08-09-kubesync-watch-poll.md.
//
// RetryWatcher owns the watch-phase reconnect/backoff; the explicit backoff here guards
// the LIST.

const (
	// listPageSize bounds one LIST (or diff-metadata) page so a whole collection never
	// sits in memory; maxListRestarts bounds continue-token-expiry restarts before the
	// pass is declared un-paginatable.
	listPageSize    = 500
	maxListRestarts = 3

	// defaultDiffThreshold caps how many objects the metadata diff GETs one-by-one before
	// falling back to one paginated LIST — past a few hundred, N round trips lose to a LIST.
	defaultDiffThreshold = 200

	// defaultResumeGrace is how long a cookie-seeded watch must run 410-free before the
	// driver reports a clean resume; a stale-cookie 410 arrives within a round trip.
	defaultResumeGrace = 2 * time.Second

	// defaultEstablishTimeout bounds the wait for a watch to CONNECT. RetryWatcher never
	// closes its ResultChan on retriable errors and a hung request never returns, so
	// without this a never-connecting watch wedges the phase in Syncing forever, spending
	// no error budget.
	defaultEstablishTimeout = 30 * time.Second

	// defaultStuckThreshold: consecutive sync failures before the worker is stuck —
	// riding out a blip, but surfacing a broken sync in seconds. Once stuck, retries drop
	// to defaultStuckRetryInterval so a permanently-broken sync stops hammering the server.
	defaultStuckThreshold     = 5
	defaultStuckRetryInterval = 3 * time.Minute

	// defaultResyncPeriod is the pull-based backstop: a watch alive this long ends itself
	// so the driver re-lists, reconciling drift the watch missed. Jittered per cache
	// (see jitterFraction) so caches don't re-list in lockstep.
	defaultResyncPeriod = 30 * time.Minute

	backoffInit = 1 * time.Second
	backoffMax  = 30 * time.Second
)

// errExpired means the watch's resourceVersion is too old (410 Gone); the only recovery
// is a full LIST to obtain a fresh RV.
var errExpired = errors.New("kubesync: watch resourceVersion expired")

// errWatchNotEstablished means the watch phase gave up waiting for the watch to connect
// (defaultEstablishTimeout). Charged against the error budget so a never-connecting
// watch surfaces as stuck instead of wedging Syncing.
var errWatchNotEstablished = errors.New("kubesync: watch failed to establish within timeout")

// errListRestartBudget means the paginated LIST couldn't finish inside the continue
// token's lifetime. Returned rather than falling back to one unpaginated LIST — that
// would load every body at once, the exact blow-up pagination avoids.
var errListRestartBudget = errors.New("kubesync: continue token kept expiring; too many items to paginate within its lifetime")

// realTimer is the production timer seam: a time.Timer's channel plus its Stop.
func realTimer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// driver owns the resume/full-list state machine for one collection.
type driver struct {
	src   Source
	store Store

	// onCaughtUp fires when the watch first proves live (the Syncing→Watching flip),
	// reporting whether the pass re-listed and how many items it pulled. Once per
	// episode via sawWatch, which recordFailure re-arms so a recovery re-reports.
	onCaughtUp func(resynced bool, items int)
	sawWatch   bool
	// onStuck fires on the exact error-budget threshold crossing (once per stuck
	// episode), so a stuck sync surfaces without waiting for the monitor's next tick.
	onStuck func(cause Cause)

	// stuck is set when the error budget is spent, cleared when the watch next proves
	// usable — NOT on a bare connect, so an open-then-error stream can't clear it.
	// Atomic: read by the monitor goroutine; the failure count that trips it is
	// Run-goroutine local.
	stuck atomic.Bool
	// failCause is the most recent failure's cause; by the time stuck flips it holds the
	// failure that spent the budget.
	failCause atomic.Value // Cause

	seedRV             string
	stuckThreshold     int
	stuckRetryInterval time.Duration
	resumeGrace        time.Duration
	establishTimeout   time.Duration
	resyncPeriod       time.Duration
	resyncDelay        time.Duration
	// listLimiter bounds how many workers may be in their LIST phase at once, shared
	// across one cache's kinds. Held around the list-heavy work only — see ListLimiter.
	listLimiter ListLimiter
	// diffThreshold is how many changed objects the metadata diff will fetch one-by-one
	// before giving up and re-listing the whole collection — see resync.
	diffThreshold int

	// Test seams: deterministic timers, backoff sleep, and liveness clock.
	graceTimer     func(time.Duration) (<-chan time.Time, func())
	establishTimer func(time.Duration) (<-chan time.Time, func())
	sleep          func(ctx context.Context, d time.Duration) error
	now            func() time.Time

	// Liveness stamps, guarded by liveMu (the run goroutine and the watch tap both stamp).
	// They are kept apart rather than collapsed into one "last alive" time because the
	// three proofs differ in strength, and the weakest must not be able to carry the
	// freshness claim on its own — see the liveness type.
	liveMu         sync.Mutex
	lastWriteAt    time.Time
	lastProofAt    time.Time
	firstConnectAt time.Time

	// proofs counts strong proofs (writes and bookmarks) for the driver's whole life. Run
	// compares it across a watch phase to tell a stream that actually carried something
	// from one that merely opened. A counter rather than the lastProofAt stamp because two
	// proofs a fraction of a tick apart — or any two under a test's frozen clock — are
	// indistinguishable by time, and "did anything land" must not depend on clock
	// resolution. Bumped under liveMu, read from the Run goroutine, hence atomic.
	proofs atomic.Uint64

	// cookieMu + watchEpoch fence a straggler bookmark from resurrecting the resume
	// cookie. The bookmark tap runs in watch.Filter's forwarding goroutine, which nothing
	// joins (rw.Stop doesn't wait for it), so a buffered bookmark can surface after
	// watchPhase returned and Run began a fullList whose first WritePage deletes that
	// cookie — a straggler PersistRV would resurrect it over half-written rows, and a
	// later failed pass would then wrongly resume from it. Each phase captures the epoch
	// and bumps it on return (before Run reaches fullList, in program order); onBookmark
	// persists only while its captured epoch is current, checked under cookieMu so it
	// can't interleave with the bump.
	//
	// That is also what makes the WORKER's drain barrier sound without joining the tap
	// goroutine, which nothing can join. onBookmark holds cookieMu across both the epoch
	// check AND the PersistRV, so the bump — which takes the same lock — cannot complete
	// while a persist is in flight, and every bookmark after it is fenced. By the time
	// watchPhase returns (so before run returns and Stop unblocks) no tap write is running
	// or can start, which is the guarantee the cache-file teardown needs.
	cookieMu   sync.Mutex
	watchEpoch int64 // guarded by cookieMu

	// didResync records whether a pass has re-listed since the last catch-up report, and
	// resyncItems how many bodies THAT pass pulled. Read at the catch-up handoff to tell a
	// clean reconnect from a resume that re-listed, and cleared once reported — a catch-up
	// describes how it got here, and the fire can repeat (recordFailure re-arms it on every
	// stuck crossing), so carrying the facts forward would credit a bare reconnect with an
	// old pass's work. Written only from the Run goroutine.
	didResync   bool
	resyncItems int

	// rvFresh records whether the RV the next watch phase resumes from came from a LIST
	// that just ran. It decides which catch-up proof applies: a fresh post-LIST RV cannot
	// 410, so connecting IS the proof, while any older position has to survive a resume
	// grace first. Distinct from didResync, which describes how the current episode got
	// here and stays true for the whole of it — a RETAINED RV goes stale while didResync
	// does not. Run-goroutine only.
	rvFresh bool

	// resyncAt is when the pull-based backstop next falls due — the deadline the whole
	// poll-for-correctness guarantee rests on. It lives on the driver, not in watchPhase's
	// timer alone, because that timer only fires while a watch is STREAMING: a phase that
	// gives up waiting for the watch to connect returns long before it, and Run then
	// retries the watch from the same RV forever. A kind whose LIST works and whose WATCH
	// hangs would re-list never. Refreshed by each completed pass ("resyncPeriod after the
	// last sync", like a client-go informer). Run-goroutine only.
	resyncAt time.Time
}

type driverOption func(*driver)

func withSleep(fn func(context.Context, time.Duration) error) driverOption {
	return func(d *driver) { d.sleep = fn }
}

func withNow(fn func() time.Time) driverOption {
	return func(d *driver) { d.now = fn }
}

func withGraceTimer(fn func(time.Duration) (<-chan time.Time, func())) driverOption {
	return func(d *driver) { d.graceTimer = fn }
}

// withEstablishTimer injects the establishment-timeout timer as a seam separate from
// graceTimer, so a test can drive it without racing the resume grace.
func withEstablishTimer(fn func(time.Duration) (<-chan time.Time, func())) driverOption {
	return func(d *driver) { d.establishTimer = fn }
}

func withResyncPeriod(p time.Duration) driverOption {
	return func(d *driver) { d.resyncPeriod = p }
}

// withDiffThreshold tunes when the metadata diff gives up in favour of a full LIST.
func withDiffThreshold(n int) driverOption {
	return func(d *driver) { d.diffThreshold = n }
}

// withListLimiter shares one LIST-phase budget across a set of drivers.
func withListLimiter(l ListLimiter) driverOption {
	return func(d *driver) { d.listLimiter = l }
}

func withStuckThreshold(n int) driverOption {
	return func(d *driver) { d.stuckThreshold = n }
}

// newDriver builds the state machine. seedRV is the persisted resume cookie ("" forces a
// cold LIST); jitterSeed spreads the periodic resync across caches.
func newDriver(src Source, st Store, seedRV, jitterSeed string, opts ...driverOption) *driver {
	d := &driver{
		src:                src,
		store:              st,
		seedRV:             seedRV,
		stuckThreshold:     defaultStuckThreshold,
		stuckRetryInterval: defaultStuckRetryInterval,
		resumeGrace:        defaultResumeGrace,
		establishTimeout:   defaultEstablishTimeout,
		resyncPeriod:       defaultResyncPeriod,
		diffThreshold:      defaultDiffThreshold,
		graceTimer:         realTimer,
		establishTimer:     realTimer,
		sleep:              ctxSleep,
		now:                time.Now,
	}
	for _, o := range opts {
		o(d)
	}
	if d.resyncPeriod > 0 {
		// Deterministic per-cache jitter (up to 25% earlier) so workers started together
		// — every cluster resuming after a laptop wake, say — don't all re-list at the
		// same instant. Hashed, not random, so it's reproducible and stable across
		// restarts.
		d.resyncDelay = d.resyncPeriod - time.Duration(float64(d.resyncPeriod)*0.25*jitterFraction(jitterSeed))
		d.resyncAt = d.now().Add(d.resyncDelay)
	}
	return d
}

// jitterFraction maps a seed to a stable value in [0,1).
func jitterFraction(seed string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	return float64(h.Sum32()) / (1 << 32)
}

// Run blocks until ctx is cancelled, resuming from the seed RV when possible and falling
// back to a full LIST otherwise.
func (d *driver) Run(ctx context.Context) error {
	rv := d.seedRV
	backoff := backoffInit
	failures := 0 // consecutive sync failures — the error budget
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// A full LIST is needed when there's no resume point (cold cache or a prior 410).
		// RetryWatcher rejects RV "" / "0", so a LIST that yields no usable RV is treated
		// as a failure and backs off like any other.
		if !usableRV(rv) {
			newRV, err := d.resync(ctx)
			if err == nil && !usableRV(newRV) {
				err = errors.New("kubesync: list returned no usable resourceVersion")
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				failures = d.recordFailure(failures, CauseListFailed)
				slog.Warn("kubesync: full list failed, backing off", "err", err, "failures", failures)
				if serr := d.sleep(ctx, d.retryDelay(backoff)); serr != nil {
					return serr
				}
				backoff = stepBackoff(backoff)
				continue
			}
			rv, d.rvFresh = newRV, true
		}

		proofsBefore := d.proofs.Load()
		progressed, established, err := d.watchPhase(ctx, rv)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A phase clears the failure budget only if the stream CARRIED something — an
		// applied delta, a bookmark, or staying alive to the periodic re-list (which reports
		// progress by definition). The stuck FLAG clears separately, only once the watch
		// proves usable — see fireCaughtUp.
		//
		// Merely ESTABLISHING is not enough, and that distinction is the whole point: a
		// watch the server accepts and then ends without streaming — an aggregated
		// APIService, a CRD behind a broken conversion webhook, a malformed object
		// RetryWatcher can't resume past — re-establishes every cycle, so crediting the open
		// would refill the budget forever. The sync could never become stuck, never drop to
		// the slow retry cadence, and would re-LIST the whole collection every backoff step
		// for as long as the process ran. It is the same grading the liveness stamps follow,
		// where a bare connect is deliberately the weak proof.
		switch {
		case progressed || d.proofs.Load() != proofsBefore:
			failures = 0
		case established:
			// It opened and gave us nothing — a different fault from one that never opened,
			// and the cause the user is shown should say so.
			failures = d.recordFailure(failures, CauseWatchNotStreaming)
		default:
			failures = d.recordFailure(failures, CauseWatchFailed)
		}
		// A watch that delivered deltas is healthy — reset the backoff and re-sync at
		// once. Otherwise back off (the slow stuck cadence once stuck) so we don't
		// hot-loop full LISTs.
		if progressed {
			backoff = backoffInit
		} else {
			switch {
			case errors.Is(err, errExpired):
				slog.Debug("kubesync: watch rv expired, re-listing")
			case errors.Is(err, errWatchNotEstablished):
				slog.Warn("kubesync: watch failed to establish, backing off", "timeout", d.establishTimeout)
			default:
				slog.Warn("kubesync: watch ended without progress, backing off")
			}
			if serr := d.sleep(ctx, d.retryDelay(backoff)); serr != nil {
				return serr
			}
			backoff = stepBackoff(backoff)
		}
		// Clear the RV so the next iteration re-lists — EXCEPT when the watch only failed
		// to ESTABLISH. There the RV still holds (nothing expired it; the connection never
		// came up), so keep it and retry the watch from it rather than re-listing every
		// event on every cycle — which, once stuck at the slow cadence, would repeat
		// forever for a list-OK/watch-denied cluster. A watch that connected and then
		// expired, or dropped without progress, still clears it; a genuinely stale
		// retained RV surfaces as a 410 on the retry and re-lists then.
		if !errors.Is(err, errWatchNotEstablished) {
			rv = ""
			continue
		}

		// Retaining the RV. It is no longer FRESH, whatever it was when the LIST produced
		// it: this loop can run for hours, so the next connect has to prove the position
		// still resumes rather than being trusted on sight.
		d.rvFresh = false
		if d.resyncDue() {
			// And once the re-list is overdue, stop retaining it at all. watchPhase's
			// resync timer is only reached while a phase is running, and this branch is
			// the case where every phase ended early: a kind whose LIST works and whose
			// WATCH hangs (connection accepted, headers never arrive) would otherwise
			// retry the watch from the retained RV forever and never pull again, so its
			// rows drift from the cluster with nothing to reconcile them.
			rv = ""
		}
	}
}

// untilResync is how long the periodic re-list still has to run, floored at zero so an
// already-overdue deadline fires at once rather than arming a negative timer.
func (d *driver) untilResync() time.Duration {
	if left := d.resyncAt.Sub(d.now()); left > 0 {
		return left
	}
	return 0
}

// resyncDue reports whether the pull-based backstop has come due. Off entirely when the
// periodic re-list is disabled.
func (d *driver) resyncDue() bool {
	return d.resyncPeriod > 0 && !d.now().Before(d.resyncAt)
}

// retryDelay is the sleep between attempts: normal exponential backoff until stuck, then
// the slow stuck cadence so a permanently-broken sync stops hammering the API server.
func (d *driver) retryDelay(backoff time.Duration) time.Duration {
	if d.stuck.Load() {
		return d.stuckRetryInterval
	}
	return backoff
}

func stepBackoff(b time.Duration) time.Duration {
	if b *= 2; b > backoffMax {
		b = backoffMax
	}
	return b
}

// recordFailure charges one sync failure against the error budget, returning the new
// consecutive count. It records the cause each time so a stuck sync reports the phase
// that spent its budget. On the exact threshold crossing it marks stuck (the flag the
// liveness monitor reads) and fires onStuck. Firing on the exact crossing keeps that
// once-per-episode without a sync.Once.
func (d *driver) recordFailure(failures int, cause Cause) int {
	failures++
	d.failCause.Store(cause)
	if failures == d.stuckThreshold {
		d.stuck.Store(true)
		slog.Warn("kubesync: stuck after repeated sync failures", "failures", failures, "cause", cause)
		// Re-arm the catch-up fire: if the watch later re-establishes, the next phase must
		// report it again so the worker can flip back out of Stale. Run-goroutine only.
		d.sawWatch = false
		if d.onStuck != nil {
			d.onStuck(cause)
		}
	}
	return failures
}

// isStuck reports whether the error budget is spent; read by the liveness monitor.
func (d *driver) isStuck() bool { return d.stuck.Load() }

// stuckCause returns the cause of the current stuck episode. Meaningful only while
// isStuck; the zero Cause when no failure has been recorded — honestly "unknown" rather
// than a specific but possibly-wrong cause.
func (d *driver) stuckCause() Cause {
	v, _ := d.failCause.Load().(Cause)
	return v
}

// fireCaughtUp reports the catch-up milestone — once per episode, guarded by sawWatch
// (which recordFailure re-arms on a stuck transition so a recovery re-reports) —
// snapshotting the resync facts in the Run goroutine so the worker never races a later
// write.
//
// This is the usability proof — the watch is live (a delta, a clean resume grace, or a
// fresh-RV connect) — so it's also where the stuck flag clears, NOT on the bare connect.
func (d *driver) fireCaughtUp() {
	d.sawWatch = true
	d.stuck.Store(false)
	if d.onCaughtUp != nil {
		d.onCaughtUp(d.didResync, d.resyncItems)
	}
	// Reported, so spent: the next catch-up describes the passes after this one, not these.
	d.didResync, d.resyncItems = false, 0
}

// watchPhase resumes the watch from rv via a RetryWatcher and applies deltas until ctx
// cancellation, a 410 (errExpired), the periodic resync falling due, or the RetryWatcher
// giving up. It reports progressed (applied at least one delta) and established (the
// watch actually connected this phase): Run uses the first to tell a healthy watch that
// dropped from one that never worked, and the second only to name the CAUSE of a phase
// that carried nothing — a watch that never opened against one that opened and then
// ended. Neither an open nor anything short of a strong proof refills the error budget;
// see Run. RetryWatcher requests bookmarks but doesn't forward them, so
// the driver taps the underlying watch ahead of it to observe them.
func (d *driver) watchPhase(ctx context.Context, rv string) (progressed, established bool, err error) {
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
	// after src.Watch succeeds, so a consistently denied watch leaves it false. Stamping
	// follows the same rule: a connect is recorded only on real establishment, never on
	// merely entering the phase, so a cluster that can LIST but never WATCH doesn't touch
	// its stamps every backoff cycle. Set from the RetryWatcher goroutine, hence atomic.
	var connected atomic.Bool
	// connectedC wakes the select loop the instant the watch establishes. onConnect runs
	// in the RetryWatcher goroutine, so it can't call fireCaughtUp (which must snapshot
	// the resync facts in the Run goroutine) directly — it signals here instead. Buffered
	// and non-blocking so a reconnect never blocks onConnect; the loop coalesces repeats
	// via pendingFire.
	connectedC := make(chan struct{}, 1)
	onConnect := func() {
		connected.Store(true)
		// Do NOT clear the stuck flag here. A watch that merely OPENS hasn't proven
		// usable — it can 410 or error immediately — and clearing on the bare open would
		// let an open-then-error loop keep a stuck sync looking healthy. The flag clears
		// at the same usability proof that fires the catch-up (see fireCaughtUp). The
		// stamp is recorded as a CONNECT, not as freshness: it feeds the staleness
		// cause (quiet watch vs one that keeps reconnecting without streaming) and
		// cannot refresh the freshness a delta or bookmark earns — see the liveness type.
		d.markConnect()
		select {
		case connectedC <- struct{}{}:
		default:
		}
	}
	// Per-phase delta accounting. deltaSeen counts deltas the tap forwarded; deltaApplied
	// counts those THIS phase durably applied. A bookmark advances the cookie only once
	// applied has caught up to seen, so a restart never resumes past a delta the cache
	// hasn't stored. Scoped to this phase rather than the driver: a straggler from an
	// ended phase bumps its own now-dead counters, so it can't leave a permanent
	// seen>applied gap that would wedge cookie advancement for the rest of the run.
	var deltaSeen, deltaApplied atomic.Int64
	watcher := watcherFor(d.src, onConnect, func(ev watch.Event) { d.tapEvent(ctx, epoch, &deltaSeen, &deltaApplied, ev) })
	rw, err := toolswatch.NewRetryWatcherWithContext(ctx, rv, watcher)
	if err != nil {
		// e.g. an unusable RV; let Run fall to a full LIST.
		return false, connected.Load(), err
	}
	defer rw.Stop()

	// The pull-based backstop: a watch alive past the deadline ends itself so Run re-lists,
	// reconciling drift the best-effort watch silently missed. Armed for the time LEFT on
	// the driver's deadline rather than a fresh full period, so a phase that ends early
	// can't keep pushing the re-list out. Disabled (nil channel) when resyncPeriod is 0.
	var resyncC <-chan time.Time
	if d.resyncPeriod > 0 {
		rt := time.NewTimer(d.untilResync())
		defer rt.Stop()
		resyncC = rt.C
	}

	// Bound the wait for the watch to CONNECT this phase (see defaultEstablishTimeout).
	// Disarmed the moment it connects; a post-connect reconnect wedge is the liveness
	// monitor's job, not this.
	establishC, establishStop := d.establishTimer(d.establishTimeout)
	defer establishStop()

	// Fire the catch-up exactly once per episode, but never before the watch is proven
	// live — reporting early would claim a sync that never happened. Both proofs start
	// from the watch CONNECTING (onConnect → connectedC); the resume grace is armed from
	// that connection, not phase entry, so a slow or denied watch can't fire a false
	// clean-resume before any watch exists:
	//
	//   - A LIST already ran this iteration (cold start, or re-entry after a 410): the RV
	//     is fresh and can't 410, so connecting IS the proof — fire on connect. Firing on
	//     entry instead would report a clean sync even when LIST is allowed but WATCH is
	//     forbidden.
	//   - Resuming straight from the saved cookie: an accepted watch can still 410, so
	//     connecting isn't enough — wait a resumeGrace for the first delta or a quiet
	//     interval with no 410, and skip the fire entirely if the watch 410s first.
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
			// The watch didn't connect within establishTimeout. Return so Run charges the
			// failure rather than blocking here forever. Guard the connect race: if
			// onConnect landed as the timer fired, treat it as connected and carry on.
			if connected.Load() {
				establishC = nil
				continue
			}
			return progressed, false, errWatchNotEstablished
		case <-graceC:
			// Connected, then no delta and no 410 within the grace — a clean resume.
			d.fireCaughtUp()
			pendingFire, graceC = false, nil
		case <-connectedC:
			establishC = nil
			if pendingFire {
				if d.rvFresh {
					// Fresh post-LIST RV can't 410 — connecting IS the proof.
					d.fireCaughtUp()
					pendingFire = false
				} else if graceC == nil {
					graceC, graceStop = d.graceTimer(d.resumeGrace)
				}
			}
		case <-resyncC:
			// The periodic re-list fell due. End the watch so Run re-lists. Report
			// progress so Run re-lists immediately without backoff — a watch alive this
			// long is healthy by definition.
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
				// catch-up now rather than waiting out the grace.
				if pendingFire {
					d.fireCaughtUp()
					pendingFire, graceC = false, nil
				}
				// Not a delta this store can land. The tap declines to count these too, so
				// seen and applied stay comparable — see tapEvent.
				u, ok := ev.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				// ApplyChange advances the resume cookie itself, in the same transaction
				// as the row — see Store.ApplyChange. A second call here would double the
				// writer-lock acquisitions on the hottest path in the system, and leave a
				// window where the row is durable but the cookie still points behind it.
				if err := d.store.ApplyChange(ctx, ev.Type, u); err != nil {
					// End the phase. Carrying on would leave this object behind
					// PERMANENTLY: the cookie is advanced by ApplyChange itself, so the
					// next delta that does land moves the resume position past the one
					// that didn't, and a restart resumes after it. The object then keeps
					// its stale body until the next periodic re-list — or forever on a
					// quiet collection whose store can't diff-resync. Ending here instead
					// re-lists from a fresh position, which reconciles it.
					slog.Warn("kubesync: apply watch change", "err", err)
					return progressed, connected.Load(), fmt.Errorf("apply watch change: %w", err)
				}
				progressed = true
				d.markWrite()
				// This delta is now durable — let a pending bookmark advance past it.
				deltaApplied.Add(1)
			case watch.Error:
				// RetryWatcher forwards the Status then closes the channel. A 410 means our
				// RV expired; anything else it treats as fatal too.
				if isExpired(ev.Object) {
					sawExpired = true
				}
			}
		}
	}
}

// resync reconciles the whole collection and returns the resourceVersion to seed the next
// watch from. It runs when the watch can't be resumed — a cold cache, an expired cookie, or
// the periodic re-list that ends a long-lived watch on purpose.
//
// It picks between two strategies. Where the store can report what it already holds (a
// MetadataDiffStore) and the cache is warm, it lists IDENTITIES only, fetches bodies for
// just the objects whose resourceVersion moved, and deletes the ones that vanished — on a
// quiet cluster, which is the steady state, that is a small response and no body fetches at
// all, versus re-downloading every object every resyncPeriod. Otherwise it falls back to
// the paginated full LIST: a cold cache (where the diff would be one metadata list plus a
// GET per object, strictly worse), a store that doesn't participate (events), a metadata
// endpoint the server doesn't serve, or a delta so large that N GETs lose to one LIST.
//
// The LIST-phase concurrency slot is taken here, around whichever strategy runs, because
// both are list-heavy; Run re-enters its indefinite watch after this returns, holding
// nothing. See ListLimiter.
func (d *driver) resync(ctx context.Context) (string, error) {
	release, err := d.listLimiter.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	md, ok := d.store.(MetadataDiffStore)
	if !ok {
		return d.fullList(ctx)
	}
	have, err := md.SnapshotRVs(ctx)
	if err != nil {
		return "", err
	}
	// Cold cache: every object would have to be fetched anyway, and one LIST beats a
	// metadata list plus N GETs.
	if len(have) == 0 {
		return d.fullList(ctx)
	}

	// Paginated like the full LIST, and for the same reason: the metadata for a 20k-object
	// kind is small per item but not small in aggregate, and up to cacheListConcurrency
	// kinds resync at once. Each page folds straight into the diff, so only one page is
	// ever resident.
	//
	// `have` is CONSUMED as the listed set is matched: whatever remains once every page is
	// folded is exactly the set the cluster no longer has. That saves carrying a second
	// whole-collection map just to subtract one from the other.
	var changed []ObjectMeta
	var listRV string
	opts := metav1.ListOptions{Limit: listPageSize}
	for {
		metas, cont, rv, err := d.src.ListMetadata(ctx, opts)
		if err != nil {
			// Some aggregated API servers don't serve a metadata endpoint, and a continue
			// token can expire mid-pagination. Fall back rather than failing the kind —
			// the full LIST is always available, and it re-lists from the top anyway.
			slog.Debug("kubesync: metadata list failed, falling back to full list", "err", err)
			return d.fullList(ctx)
		}
		listRV = rv
		for _, m := range metas {
			if have[m.UID] != m.ResourceVersion {
				changed = append(changed, m)
			}
			delete(have, m.UID)
		}
		// A big delta: one paginated LIST beats N round trips, and there is no point
		// paging the rest of the metadata to confirm it.
		if len(changed) > d.diffThreshold {
			return d.fullList(ctx)
		}
		if cont == "" {
			break
		}
		opts.Continue = cont
	}

	// Clear the cookie before the first write, and rewrite it only once everything is
	// reconciled (below). An interrupted pass then leaves no position rather than one its
	// rows don't back — the same invariant fullList keeps by clearing on its first page.
	if len(changed) > 0 || len(have) > 0 {
		if err := md.ClearRV(ctx); err != nil {
			return "", err
		}
	}

	applied := 0
	for _, m := range changed {
		u, err := d.src.Get(ctx, m.Namespace, m.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Raced a delete between the metadata list and this GET. The object is
				// absent from the cluster, so the next pass's diff reconciles it — and it
				// was already removed from `have`, so this pass won't delete it either.
				// Harmless: one stale row for at most one resync period.
				continue
			}
			return "", err
		}
		// ApplyDiff, not ApplyChange: a GET-fetched object's resourceVersion is its own,
		// not this pass's position, so advancing the cookie to it would put the cookie
		// ahead of changes still to be applied.
		if err := md.ApplyDiff(ctx, u); err != nil {
			return "", err
		}
		applied++
	}

	// Whatever is left in `have` was not listed, so the cluster no longer has it. One
	// batched delete rather than one transaction per uid: these land on the writer
	// connection the WHOLE cache shares, so a kind that lost thousands of objects would
	// otherwise block every other kind's worker behind thousands of commits.
	vanished := make([]string, 0, len(have))
	for uid := range have {
		vanished = append(vanished, uid)
	}
	if len(vanished) > 0 {
		if err := md.DeleteByUIDs(ctx, vanished); err != nil {
			return "", err
		}
	}

	// The diff wrote through ApplyChange, which advances the cookie per object — but a
	// pass with no changed objects advances nothing, and the deletes carry no
	// resourceVersion of their own. Persist the list's RV so the next watch resumes from
	// the position this pass actually reconciled to.
	//
	// Only if the server gave us one. Persist is an unconditional upsert, and Run rejects
	// an empty or "0" RV as unusable — so writing it would trade a cookie that still
	// resumes cheaply for one the next start has to throw away and cold-LIST past, on a
	// quiet pass that changed nothing (a pass that DID write cleared the cookie above, and
	// leaving it cleared is the honest outcome).
	if usableRV(listRV) {
		if err := d.store.PersistRV(ctx, listRV); err != nil {
			return "", err
		}
	}

	// A deletion is a change but not an item pulled, so the two counts differ here.
	d.recordPass(applied+len(vanished), applied)
	return listRV, nil
}

// fullList streams a paginated full LIST into the store: each page lands as it arrives
// (bounding memory to one page of bodies), and a final Commit prunes the rows no page
// carried and persists the resume cookie. A continue token can expire (410)
// mid-pagination — the driver discards the partial pass and re-lists from the top
// (matching a client-go Reflector), bounded by maxListRestarts.
func (d *driver) fullList(ctx context.Context) (string, error) {
	// The session's first WritePage invalidates the resume cookie (rewritten only by a
	// successful Commit), so an exit at any point below — error return or 410 restart —
	// just drops the session: any cookie left is consistent with whatever pages were
	// written, and the next start cold-LISTs when partial pages exist, its prune
	// reconciling them.
	sess := d.store.BeginReplace()

	opts := metav1.ListOptions{Limit: listPageSize}
	var lastRV string
	listed, restarts := 0, 0
	for {
		items, cont, rv, err := d.src.List(ctx, opts)
		if err != nil {
			if listExpired(err) && opts.Continue != "" {
				if restarts >= maxListRestarts {
					return "", errListRestartBudget
				}
				// Discard the partial pass and re-list from the top with a fresh session.
				sess = d.store.BeginReplace()
				opts.Continue, listed = "", 0
				restarts++
				continue
			}
			return "", err
		}
		if err := sess.WritePage(ctx, items); err != nil {
			return "", err
		}
		listed += len(items)
		lastRV = rv
		if cont == "" {
			break
		}
		opts.Continue = cont
	}
	pruned, err := sess.Commit(ctx, lastRV)
	if err != nil {
		return "", err
	}
	// The prune counts as a change: mark-and-sweep is how this path DELETES, so a pass that
	// listed nothing but emptied the table did land an update — the diff path counts its
	// own vanished set for the same reason.
	d.recordPass(listed+pruned, listed)
	return lastRV, nil
}

// recordPass closes out a completed resync pass — a full LIST or a metadata diff alike.
//
// changed is what the pass actually altered in the store and items is what it announces as
// pulled; they differ only for the diff, which counts a deletion as a change but not as an
// item fetched. A completed pass is a strong PROOF: the server answered with the current
// set. It counts as a WRITE only when rows landed — an empty collection's LIST proves
// liveness but received no update, and conflating the two would let "last update received"
// tick on a cluster where nothing has happened.
//
// didResync/resyncItems are what the catch-up report reads: a pass that re-listed and left
// them unset is announced as a clean watch resume, and watchPhase then waits out the whole
// resumeGrace for a resume that never happened. They are consumed by the report — see
// fireCaughtUp.
func (d *driver) recordPass(changed, items int) {
	if d.resyncPeriod > 0 {
		d.resyncAt = d.now().Add(d.resyncDelay)
	}
	if changed > 0 {
		d.markWrite()
	} else {
		d.markProof()
	}
	d.didResync = true
	// This pass's count, not a running total. Accumulating reported the worker's whole
	// lifetime of re-pulls as one pass's work — "re-synced 2000 objects" for a pass that
	// moved 1000 — verbatim in the user-visible event log.
	d.resyncItems = items
}

// usableRV reports whether a resourceVersion can seed a watch. RetryWatcher rejects "" and
// "0" (the latter means "any version" — resuming from it would replay the collection as
// Added and re-apply everything), so both are treated as "we have no position".
func usableRV(rv string) bool { return rv != "" && rv != "0" }

// listExpired reports whether a LIST error is a continue-token expiry. A stock
// kube-apiserver returns ResourceExpired (reason "Expired"); a nonconforming intermediary
// may answer an expired token with a bare 410 Gone, so accept that too — matching the
// watch path's isExpired.
func listExpired(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// isExpired reports whether a watch.Error event object is a 410 Gone / ResourceExpired
// status (our RV is too old to resume from).
func isExpired(obj runtime.Object) bool {
	err := apierrors.FromObject(obj)
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// liveness is one consistent read of the driver's four stamps — taken under one lock so
// the staleness rule can't compare stamps from two different instants.
//
// Proofs are graded, not interchangeable, which is why they're kept apart:
//
//   - write: the server sent data and we durably wrote it — an applied watch delta, or a
//     full LIST that landed rows. The only proof that the cache is actually being filled,
//     so it alone backs the user-facing "last update received".
//   - proof: the server answered and is current, with nothing to write — a watch bookmark,
//     or a full LIST that found nothing new. As strong as a write for liveness purposes,
//     which is what lets a quiet-but-healthy cluster (bookmarks flowing, no events) stay
//     healthy instead of aging into the stale threshold.
//   - connect: a watch request was accepted. WEAK — no data flowed, and the stream can
//     error one frame later. A watch that opens and immediately errors re-establishes
//     each cycle, so if a bare connect could refresh the freshness stamp such a loop would
//     report Watching forever while the cache received nothing. Connects therefore inform
//     the CAUSE, never the freshness — the same grading Run applies to the error budget,
//     which only a strong proof refills.
type liveness struct {
	// write is the last time data landed; proof the last strong proof of ANY kind, so
	// proof >= write always (a write stamps both) and proof alone is the freshness the
	// staleness rule and the published liveness stamp use.
	write time.Time
	proof time.Time
	// firstConnect is the first connect since the last strong proof — the instant the
	// current no-proof episode opened. The stale clock runs from it when nothing has
	// streamed yet, precisely because later reconnects can't push it forward. Zero
	// whenever the stream has proven itself, which is what makes it the "connected but
	// not streaming" signal.
	firstConnect time.Time
}

// connectedWithoutProof reports whether the watch has established since the last strong
// proof — it is connected (or keeps reconnecting) yet nothing has streamed over it. A
// proof clears the episode, so on a healthy watch this is false between bookmarks.
func (l liveness) connectedWithoutProof() bool {
	return !l.firstConnect.IsZero()
}

// markWrite stamps fresh data landing in the cache — an applied watch delta, or a LIST
// that wrote rows. Safe from the run and watch-tap goroutines.
func (d *driver) markWrite() {
	t := d.now()
	d.liveMu.Lock()
	d.lastWriteAt = t
	d.markProofLocked(t)
	d.liveMu.Unlock()
}

// markProof stamps the server proving itself with nothing to write — a bookmark, or a
// LIST that found nothing new.
func (d *driver) markProof() {
	t := d.now()
	d.liveMu.Lock()
	d.markProofLocked(t)
	d.liveMu.Unlock()
}

// markProofLocked records a strong proof and ends the current no-proof episode: the
// server has answered, so the next connect starts a fresh one. The episode reset is
// guarded because this runs once per applied item on a high-volume stream, where the
// episode is already closed and re-zeroing it every time would be a dirty store per
// event to no effect.
func (d *driver) markProofLocked(t time.Time) {
	d.lastProofAt = t
	d.proofs.Add(1)
	if !d.firstConnectAt.IsZero() {
		d.firstConnectAt = time.Time{}
	}
}

// markConnect stamps a watch establishing. It only OPENS a no-proof episode — a later
// connect within the same episode changes nothing — so a reconnect loop can't reset the
// stale clock by re-establishing.
func (d *driver) markConnect() {
	t := d.now()
	d.liveMu.Lock()
	if d.firstConnectAt.IsZero() {
		d.firstConnectAt = t
	}
	d.liveMu.Unlock()
}

// liveness snapshots the stamps under one lock.
func (d *driver) liveness() liveness {
	d.liveMu.Lock()
	defer d.liveMu.Unlock()
	return liveness{
		write:        d.lastWriteAt,
		proof:        d.lastProofAt,
		firstConnect: d.firstConnectAt,
	}
}

// tapEvent inspects one raw watch event the tap observed ahead of RetryWatcher (which is
// why it runs in the tap's goroutine, not the apply loop). Deltas bump seen — the ordered
// high-water mark a bookmark checks against — while bookmarks stamp liveness and
// conditionally advance the resume cookie.
func (d *driver) tapEvent(ctx context.Context, epoch int64, seen, applied *atomic.Int64, ev watch.Event) {
	switch ev.Type {
	case watch.Added, watch.Modified, watch.Deleted:
		// Only deltas the apply loop CAN apply are counted. It discards anything that is
		// not an unstructured object, and a delta counted here but never applied leaves
		// applied permanently behind seen — which makes every later bookmark decline to
		// advance the cookie, freezing the resume position for the rest of the watch phase
		// (up to the 30m re-list) and turning the next resume into a stale-RV 410 and a
		// cold re-LIST. The two must apply the same test.
		if _, ok := ev.Object.(*unstructured.Unstructured); !ok {
			return
		}
		seen.Add(1)
	case watch.Bookmark:
		d.onBookmark(ctx, epoch, seen, applied, resourceVersionOf(ev.Object))
	}
}

// onBookmark handles a bookmark the tap observed: stamp liveness and advance the
// persisted cookie (so a cold restart resumes closer to head even on a quiet cluster).
// The cookie only moves once every delta the tap forwarded before this bookmark has
// applied: the server places a bookmark at or after all prior events, so advancing while
// a delta is un-applied would let a restart skip it permanently. Deltas in flight just
// defer the advance to the next bookmark. Liveness is stamped regardless.
func (d *driver) onBookmark(ctx context.Context, epoch int64, seen, applied *atomic.Int64, rv string) {
	if rv == "" {
		return
	}
	// Snapshot the high-water mark before stamping. The tap is single-threaded, so no
	// new delta is forwarded while this runs — seen is frozen at exactly the count of
	// deltas preceding this bookmark.
	n := seen.Load()
	d.markProof()
	if applied.Load() < n {
		return
	}
	d.cookieMu.Lock()
	defer d.cookieMu.Unlock()
	if d.watchEpoch != epoch {
		// This tap's phase ended; Run may be mid-fullList (cookie deleted). Persisting now
		// would resurrect a stale cookie over half-written rows.
		return
	}
	if err := d.store.PersistRV(ctx, rv); err != nil {
		slog.Warn("kubesync: persist bookmark rv", "err", err)
	}
}

// watcherFor adapts a Source to the cache.WatcherWithContext that
// NewRetryWatcherWithContext consumes (RetryWatcher fills in RV + bookmarks). It wraps
// each opened watch in a tap so the driver observes every event — including the bookmarks
// RetryWatcher swallows — ahead of RetryWatcher, passing each through unchanged so its
// reconnect/RV bookkeeping is untouched. onConnect fires on every successful (re)open:
// RetryWatcher reconnects by re-invoking this func, so a fresh replacement watch is itself
// proof of liveness before its first event — what a quiet collection needs so a benign
// reconnect isn't misread as stale.
func watcherFor(src Source, onConnect func(), tap func(watch.Event)) cache.WatcherWithContext {
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

// resourceVersionOf extracts an object's resourceVersion, or "" if it has none.
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
