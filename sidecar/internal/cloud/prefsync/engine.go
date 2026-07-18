// Package prefsync is the always-on settings reconciliation engine. It owns one
// supervised cloud connection — snapshot, apply locally, stream events — and
// reconnects with exponential backoff on any end/error. Wake/resync signals
// arrive via the shared poke.Service passed to New.
//
// Local-first: Set writes the local store and queues the patch immediately, so
// an edit survives offline/restart; on reconnect the engine drains the queue to
// the cloud. Until acked, a queued patch is re-layered (prefs.Merge) over every
// incoming snapshot/event, so a stale cloud snapshot can't clobber it.
package prefsync

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// Upstream is the slice of the cloud client the engine needs: Snapshot (a
// point-in-time read), Watch (open a stream), Update (push a local patch).
//
// Watch's settings channel closes on upstream end or ctx cancel; the second
// channel carries the stream's terminal error (buffered, exactly one value): nil
// for a clean end/cancel, non-nil for a read/transport failure. It distinguishes
// an errored close (record it, keep backing off) from a healthy one — a nil
// channel is treated as a clean end.
type Upstream interface {
	Snapshot(ctx context.Context) (prefs.Settings, error)
	Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error)
	Update(ctx context.Context, patch prefs.Settings) (prefs.Settings, error)
}

// State is the engine's connection lifecycle.
type State int

const (
	StateConnecting State = iota
	StateLive
	StateBackoff
	StateOffline
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateLive:
		return "live"
	case StateBackoff:
		return "backoff"
	case StateOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// Status is an immutable snapshot of engine health.
type Status struct {
	State        State
	LastError    string
	RetryAt      time.Time // meaningful when State == StateBackoff/Offline
	LastSyncedAt time.Time
}

// engineOpts holds the backoff knobs and injectable seams. Zero values get
// defaults. Production calls New (defaults only); white-box tests inject
// deterministic seams via the option functions below.
type engineOpts struct {
	baseBackoff time.Duration
	maxBackoff  time.Duration

	now    func() time.Time
	sleep  func(context.Context, time.Duration) error
	jitter func(time.Duration) time.Duration
}

func (o *engineOpts) applyDefaults() {
	if o.baseBackoff <= 0 {
		o.baseBackoff = time.Second
	}
	if o.maxBackoff <= 0 {
		o.maxBackoff = 30 * time.Second
	}
	if o.now == nil {
		o.now = time.Now
	}
	if o.sleep == nil {
		o.sleep = func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		}
	}
	if o.jitter == nil {
		// Full-ish jitter: a random point in [d/2, d].
		o.jitter = func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
		}
	}
}

// option is an unexported build seam for newWithOptions, constructible only by
// this package's white-box tests. Mirrors the auth/cloud option pattern.
type option func(*engineOpts)

// withBackoff overrides the base/max backoff (defaults: 1s/30s).
func withBackoff(base, max time.Duration) option {
	return func(o *engineOpts) { o.baseBackoff, o.maxBackoff = base, max }
}

// withNow overrides the clock seam.
func withNow(f func() time.Time) option { return func(o *engineOpts) { o.now = f } }

// withSleep overrides the backoff-sleep seam.
func withSleep(f func(context.Context, time.Duration) error) option {
	return func(o *engineOpts) { o.sleep = f }
}

// withJitter overrides the backoff-jitter seam.
func withJitter(f func(time.Duration) time.Duration) option {
	return func(o *engineOpts) { o.jitter = f }
}

// Engine reconciles the settings resource. Construct with New, run with Run.
type Engine struct {
	up      Upstream
	store   *prefs.Store
	queue   *mutationqueue.Queue
	pokeSvc *poke.Service // resync source; nil disables external wake signals
	opt     engineOpts

	mu     sync.Mutex
	status Status

	// writeMu serializes the store's read-merge-write (Set and apply), so two
	// overlapping patches can't merge from the same base and drop a field.
	writeMu sync.Mutex

	// activeCancel cancels the in-flight attempt so a poke forces a reconnect.
	activeCancel context.CancelFunc
	// pokeCh wakes an in-flight backoff so Run retries immediately.
	pokeCh chan struct{}
}

// New builds an Engine. It starts nothing; call Run. pokeSvc is the resync
// source; pass nil to disable external wake signals (tests, degraded deployments).
func New(up Upstream, store *prefs.Store, queue *mutationqueue.Queue, pokeSvc *poke.Service) *Engine {
	return newWithOptions(up, store, queue, pokeSvc)
}

// newWithOptions is the build entry point accepting the unexported test seams;
// New wraps it with no options.
func newWithOptions(up Upstream, store *prefs.Store, queue *mutationqueue.Queue, pokeSvc *poke.Service, opts ...option) *Engine {
	var o engineOpts
	for _, opt := range opts {
		opt(&o)
	}
	o.applyDefaults()
	return &Engine{
		up:      up,
		store:   store,
		queue:   queue,
		pokeSvc: pokeSvc,
		opt:     o,
		pokeCh:  make(chan struct{}, 1),
	}
}

// Set applies a patch locally and queues it for the cloud (local-first): the
// store is updated and published immediately, the patch durably enqueued so it
// survives a restart, and a Poke nudges the engine to push it now if live.
func (e *Engine) Set(patch prefs.Settings) error {
	// Serialize the whole read-merge-write so an overlapping Set or incoming
	// cloud apply can't merge from the same base and drop one patch's field.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	merged := prefs.Merge(e.store.Get(), patch)
	// Enqueue BEFORE exposing the local change: a persisted edit must never exist
	// without a queued mutation to carry it to the cloud, else the next snapshot
	// silently clobbers it.
	ent, err := e.queue.Enqueue(patch)
	if err != nil {
		return err
	}
	if _, err := e.store.Set(merged); err != nil {
		// Local persistence failed: roll the queued mutation back so we never
		// send an edit that was never applied locally. If the rollback also
		// fails, surface both — the entry may still be queued, so the caller
		// must know local write and queue state diverged.
		if ackErr := e.queue.Ack(ent.ID); ackErr != nil {
			return errors.Join(err, ackErr)
		}
		return err
	}
	e.Poke()
	return nil
}

// Status returns the current health snapshot.
func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status
}

// Poke triggers an immediate resync: cancels the active watch and wakes any
// in-flight backoff. Idempotent, never blocks, safe from any goroutine.
func (e *Engine) Poke() {
	select {
	case e.pokeCh <- struct{}{}:
	default: // a poke is already pending; coalesce
	}
	e.mu.Lock()
	cancel := e.activeCancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Run blocks until ctx is cancelled, supervising the upstream connection.
func (e *Engine) Run(ctx context.Context) {
	if e.pokeSvc != nil {
		ch, cancel := e.pokeSvc.Subscribe()
		defer cancel()
		go func() {
			for range ch {
				e.Poke()
			}
		}()
	}

	attempt := 0
	for ctx.Err() == nil {
		e.setStatus(func(s *Status) { s.State = StateConnecting })

		// Install the per-attempt cancel BEFORE any upstream call, so a Poke
		// (e.g. from sign-out) cancels whatever authenticated work is in flight
		// — Snapshot, the drain's Updates, the Watch open, or the live stream —
		// not just an already-open watch.
		attemptCtx, cancel := context.WithCancel(ctx)
		e.setActiveCancel(cancel)

		snap, err := e.up.Snapshot(attemptCtx)
		if err != nil {
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}
		// A poke (sign-out) may have cancelled between Snapshot returning and
		// applying it: don't apply a cloud snapshot or mark it synced as the user
		// signs out; abandon and reconnect.
		if attemptCtx.Err() != nil {
			if e.abandonAttempt(ctx, cancel) {
				return
			}
			continue
		}
		if err := e.apply(snap, true); err != nil {
			// Couldn't persist the snapshot: don't mark it synced or open the
			// watch; back off and retry.
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}

		// Drain queued local edits AFTER the snapshot but BEFORE the watch.
		// Opening the watch after the drain protects an acked edit: the stream's
		// on-subscribe state already reflects the pushed edit, so no stale
		// pre-update event can clobber it once the queue stops re-layering it.
		// Draining after the snapshot means a persistently-rejected Update can't
		// block pulling cloud changes.
		if err := e.drain(attemptCtx); err != nil {
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}

		ch, streamErrCh, err := e.up.Watch(attemptCtx)
		if err != nil {
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}

		e.setStatus(func(s *Status) {
			s.State = StateLive
			s.LastError = ""
		})

		progressed := false
		var applyErr error
		for ev := range ch {
			if err := e.apply(ev, false); err != nil {
				// A store failure mid-stream is a sync failure: tear the watch
				// down and back off rather than stream against unpersisted state.
				applyErr = err
				break
			}
			progressed = true
		}
		e.clearActive(cancel)

		if ctx.Err() != nil {
			return
		}
		// Fold in the stream's terminal error: a read/transport failure is a sync
		// failure, not a clean end — surface it and don't reset the backoff.
		streamErr := applyErr
		if streamErr == nil && streamErrCh != nil {
			streamErr = <-streamErrCh
		}
		if streamErr != nil {
			if e.enterWaiting(ctx, &attempt, StateBackoff, streamErr.Error()) {
				return
			}
			continue
		}
		if progressed {
			attempt = 0
		}
		if e.enterWaiting(ctx, &attempt, StateBackoff, "") {
			return
		}
	}
}

// drain pushes each queued patch to the cloud in FIFO order, acking on success.
// It stops at the first failure (leaving that entry and the rest queued) and
// propagates the error, so the supervisor backs off and retries next cycle.
func (e *Engine) drain(ctx context.Context) error {
	for _, ent := range e.queue.Pending() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := e.up.Update(ctx, ent.Patch); err != nil {
			return err
		}
		// A poke (sign-out) racing right after the cloud accepted the update must
		// not let us ack it: bail, leaving the entry queued. The cloud's
		// deep-merge makes re-sending it next cycle idempotent, so nothing is lost.
		if err := ctx.Err(); err != nil {
			return err
		}
		// If the ack can't be persisted, stop: the entry is still queued, so
		// continuing would treat an already-sent patch as drained while a restart
		// would replay it. Retry next cycle.
		if err := e.queue.Ack(ent.ID); err != nil {
			return err
		}
	}
	return nil
}

// apply reconciles an incoming cloud value: still-pending local patches are
// re-layered on top (so a snapshot can't clobber an unacked edit), then written
// to the store (which dedups + publishes only on change). snapshot==true stamps
// LastSyncedAt. A store failure is returned so the caller treats it as a sync
// failure rather than marking unpersisted state as synced.
func (e *Engine) apply(v prefs.Settings, snapshot bool) error {
	// Serialize against Set (and other applies) so the re-layer + store write is
	// atomic and a concurrent local edit can't be lost.
	e.writeMu.Lock()
	merged := v
	for _, ent := range e.queue.Pending() {
		merged = prefs.Merge(merged, ent.Patch)
	}
	_, err := e.store.Set(merged)
	e.writeMu.Unlock()
	if err != nil {
		return err
	}
	if snapshot {
		now := e.opt.now()
		e.setStatus(func(s *Status) { s.LastSyncedAt = now })
	}
	return nil
}

func (e *Engine) setStatus(mut func(*Status)) {
	e.mu.Lock()
	mut(&e.status)
	e.mu.Unlock()
}

func (e *Engine) setActiveCancel(c context.CancelFunc) {
	e.mu.Lock()
	e.activeCancel = c
	e.mu.Unlock()
}

// clearActive unregisters and cancels the per-attempt cancel, releasing the
// context so a later Poke doesn't cancel a stale one.
func (e *Engine) clearActive(cancel context.CancelFunc) {
	e.setActiveCancel(nil)
	cancel()
}

// connectFailed handles a failure of a pre-Live connection step (snapshot,
// snapshot-apply, drain, or watch open). If a poke cancelled the per-attempt
// context while the parent ctx is still alive, that's an intentional restart —
// routed to abandonAttempt for an immediate reconnect, no error recorded, no
// backoff. Otherwise it tears down the attempt and enters Offline backoff.
// Returns true if ctx ended during the wait (Run should exit).
func (e *Engine) connectFailed(ctx, attemptCtx context.Context, cancel context.CancelFunc, attempt *int, err error) bool {
	if ctx.Err() == nil && attemptCtx.Err() != nil {
		return e.abandonAttempt(ctx, cancel)
	}
	e.clearActive(cancel)
	return e.enterWaiting(ctx, attempt, StateOffline, err.Error())
}

// abandonAttempt tears down an attempt a poke cancelled mid-step (no failure to
// record, no backoff escalation). It drains the pending poke token — the
// immediate reconnect already serves the poke, so the token must not also
// short-circuit a later genuine backoff. Returns true if the parent ctx ended.
func (e *Engine) abandonAttempt(ctx context.Context, cancel context.CancelFunc) bool {
	e.clearActive(cancel)
	select {
	case <-e.pokeCh:
	default:
	}
	return ctx.Err() != nil
}

// backoffDelay is the exponential-with-jitter delay for an attempt, clamped
// to MaxBackoff.
func (e *Engine) backoffDelay(attempt int) time.Duration {
	d := e.opt.baseBackoff << attempt
	if d <= 0 || d > e.opt.maxBackoff {
		d = e.opt.maxBackoff
	}
	return e.opt.jitter(d)
}

// enterWaiting records the retry state and sleeps the backoff delay, racing the
// sleep against a poke. attempt stops incrementing once the delay hits the cap.
// Returns true if ctx ended during the wait.
func (e *Engine) enterWaiting(ctx context.Context, attempt *int, st State, errMsg string) bool {
	d := e.backoffDelay(*attempt)
	if e.opt.baseBackoff<<*attempt < e.opt.maxBackoff {
		*attempt++
	}
	e.setStatus(func(s *Status) {
		s.State = st
		s.LastError = errMsg
		s.RetryAt = e.opt.now().Add(d)
	})

	sctx, scancel := context.WithCancel(ctx)
	defer scancel()
	slept := make(chan struct{})
	go func() {
		_ = e.opt.sleep(sctx, d)
		close(slept)
	}()
	select {
	case <-ctx.Done():
		return true
	case <-e.pokeCh:
		return false
	case <-slept:
		return ctx.Err() != nil
	}
}
