// Package prefsync is the always-on settings reconciliation engine: one supervised cloud
// connection (snapshot, apply, stream), reconnecting with exponential backoff. Wake
// signals arrive via the shared poke.Service.
//
// Local-first: Set writes the store and queues the patch immediately, so an edit survives
// offline/restart, and until acked it is re-layered (prefs.Merge) over every incoming
// snapshot so a stale one can't clobber it.
// See docs/adr/2026-08-09-local-first-auth-settings.md.
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

// Upstream is the slice of the cloud client the engine needs. Watch's settings channel
// closes on upstream end or cancel; its second channel carries exactly one terminal error
// (nil for a clean end), which is what distinguishes an errored close — record it, keep
// backing off — from a healthy one. A nil channel reads as a clean end.
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

// engineOpts holds the backoff knobs and seams; zero values get defaults. Production
// calls New; white-box tests inject deterministic seams via the options below.
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
		// A random point in [d/2, d].
		o.jitter = func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
		}
	}
}

// option is an unexported build seam, constructible only by this package's white-box
// tests. Same pattern as auth/cloud.
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

	// writeMu serializes the store's read-merge-write (Set and apply), so overlapping
	// patches can't merge from the same base and drop a field.
	writeMu sync.Mutex

	// activeCancel cancels the in-flight attempt so a poke forces a reconnect.
	activeCancel context.CancelFunc
	// pokeCh wakes an in-flight backoff so Run retries immediately.
	pokeCh chan struct{}
}

// New builds an Engine; it starts nothing — call Run. A nil pokeSvc disables external
// wake signals.
func New(up Upstream, store *prefs.Store, queue *mutationqueue.Queue, pokeSvc *poke.Service) *Engine {
	return newWithOptions(up, store, queue, pokeSvc)
}

// newWithOptions is New plus the unexported test seams.
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

// Set applies a patch locally and queues it for the cloud, then pokes the engine to push
// it if live.
func (e *Engine) Set(patch prefs.Settings) error {
	// Serialize the whole read-merge-write against an overlapping Set or apply.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	merged := prefs.Merge(e.store.Get(), patch)
	// Enqueue BEFORE exposing the local change: a persisted edit with no queued
	// mutation to carry it up is silently clobbered by the next snapshot.
	ent, err := e.queue.Enqueue(patch)
	if err != nil {
		return err
	}
	if _, err := e.store.Set(merged); err != nil {
		// Roll the mutation back so we never send an edit that never applied
		// locally; if the rollback also fails, surface both — write and queue state
		// have diverged.
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

// Poke forces an immediate resync: cancels the active attempt and wakes an in-flight
// backoff. Idempotent, never blocks.
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

		// Install the per-attempt cancel BEFORE any upstream call, so a sign-out
		// poke cancels whatever authenticated work is in flight, not just an
		// already-open watch.
		attemptCtx, cancel := context.WithCancel(ctx)
		e.setActiveCancel(cancel)

		snap, err := e.up.Snapshot(attemptCtx)
		if err != nil {
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}
		// A sign-out poke may have landed between Snapshot and its apply: don't
		// apply or mark synced; abandon and reconnect.
		if attemptCtx.Err() != nil {
			if e.abandonAttempt(ctx, cancel) {
				return
			}
			continue
		}
		if err := e.apply(snap, true); err != nil {
			// An unpersisted snapshot is not synced: back off rather than watch.
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}

		// Drain AFTER the snapshot (so a persistently-rejected Update can't block
		// pulling cloud changes) but BEFORE the watch (so the stream's
		// on-subscribe state already reflects the pushed edit, and no stale event
		// clobbers it once the queue stops re-layering it).
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
				// Never stream against unpersisted state: tear down and back off.
				applyErr = err
				break
			}
			progressed = true
		}
		e.clearActive(cancel)

		if ctx.Err() != nil {
			return
		}
		// A terminal read/transport error is a sync failure, not a clean end — it
		// must not reset the backoff.
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

// drain pushes queued patches in FIFO order, acking each. It stops at the first failure,
// leaving that entry and the rest queued for the next cycle.
func (e *Engine) drain(ctx context.Context) error {
	for _, ent := range e.queue.Pending() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := e.up.Update(ctx, ent.Patch); err != nil {
			return err
		}
		// A sign-out poke racing the accepted update must not let us ack it; the
		// cloud's deep-merge makes re-sending next cycle idempotent.
		if err := ctx.Err(); err != nil {
			return err
		}
		// An unpersisted ack must stop the drain: continuing would treat a sent
		// patch as drained while a restart replays it.
		if err := e.queue.Ack(ent.ID); err != nil {
			return err
		}
	}
	return nil
}

// apply reconciles an incoming cloud value, re-layering still-pending local patches on
// top so a snapshot can't clobber an unacked edit. snapshot==true stamps LastSyncedAt; a
// store failure is returned so the caller never marks unpersisted state synced.
func (e *Engine) apply(v prefs.Settings, snapshot bool) error {
	// Serialize against Set and other applies, so re-layer + write is atomic.
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

// clearActive unregisters and cancels the per-attempt cancel, so a later Poke can't
// cancel a stale one.
func (e *Engine) clearActive(cancel context.CancelFunc) {
	e.setActiveCancel(nil)
	cancel()
}

// connectFailed handles a pre-Live step's failure. A poke that cancelled the attempt
// while the parent ctx lives is an intentional restart — abandonAttempt, no error, no
// backoff; otherwise enter Offline backoff. True means ctx ended and Run should exit.
func (e *Engine) connectFailed(ctx, attemptCtx context.Context, cancel context.CancelFunc, attempt *int, err error) bool {
	if ctx.Err() == nil && attemptCtx.Err() != nil {
		return e.abandonAttempt(ctx, cancel)
	}
	e.clearActive(cancel)
	return e.enterWaiting(ctx, attempt, StateOffline, err.Error())
}

// abandonAttempt tears down an attempt a poke cancelled mid-step, with no failure
// recorded. It drains the poke token: the immediate reconnect already serves the poke, so
// the token must not also short-circuit a later genuine backoff.
func (e *Engine) abandonAttempt(ctx context.Context, cancel context.CancelFunc) bool {
	e.clearActive(cancel)
	select {
	case <-e.pokeCh:
	default:
	}
	return ctx.Err() != nil
}

// backoffDelay is the jittered exponential delay, clamped to maxBackoff.
func (e *Engine) backoffDelay(attempt int) time.Duration {
	d := e.opt.baseBackoff << attempt
	if d <= 0 || d > e.opt.maxBackoff {
		d = e.opt.maxBackoff
	}
	return e.opt.jitter(d)
}

// enterWaiting records the retry state and sleeps the backoff, racing it against a poke;
// attempt stops incrementing at the cap. True means ctx ended during the wait.
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
