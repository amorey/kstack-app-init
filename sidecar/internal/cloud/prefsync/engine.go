// Package prefsync is the sidecar's always-on settings reconciliation
// engine. It owns one supervised connection to the cloud: take a snapshot,
// apply it locally, then stream events. On any upstream end/error it
// reconnects with exponential backoff; on a detected wall-clock gap (machine
// sleep/resume) it forces an immediate resync.
//
// The path is local-first: Set writes the local store immediately and queues
// the patch, so an edit takes effect (and survives a restart) even offline.
// On reconnect the engine drains that queue to the cloud. Until a queued
// patch is acked it is re-layered over every incoming snapshot/event
// (prefs.Merge), so a cloud snapshot that predates a local edit can't clobber
// it.
package prefsync

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
)

// Upstream is the slice of the cloud client the engine needs. Snapshot is a
// point-in-time read; Watch opens a stream; Update pushes a local patch. Kept
// minimal so tests can fake it.
//
// Watch's settings channel closes when the upstream ends or ctx is cancelled.
// The second channel carries the stream's terminal error (buffered, exactly one
// value): nil for a clean end or ctx cancel, non-nil for a read/transport
// failure. It lets the engine distinguish an errored close (record it, keep
// backing off) from a healthy one — a nil channel is treated as a clean end, so
// fakes that don't model stream errors may return nil.
type Upstream interface {
	Snapshot(ctx context.Context) (prefs.Settings, error)
	Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error)
	Update(ctx context.Context, patch prefs.Settings) (prefs.Settings, error)
}

// State is the engine's connection lifecycle (exposed for a later
// syncStatus surface).
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

// Options configures an Engine. Zero values get sane defaults; the
// injectable seams (Now/Sleep/Jitter) keep tests deterministic.
type Options struct {
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Tick        time.Duration // wake-detector heartbeat interval
	GapFactor   float64       // wall gap > Tick*GapFactor ⇒ treat as resume

	Now    func() time.Time
	Sleep  func(context.Context, time.Duration) error
	Jitter func(time.Duration) time.Duration
}

func (o *Options) applyDefaults() {
	if o.BaseBackoff <= 0 {
		o.BaseBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.Tick <= 0 {
		o.Tick = 15 * time.Second
	}
	if o.GapFactor <= 0 {
		o.GapFactor = 2
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = func(ctx context.Context, d time.Duration) error {
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
	if o.Jitter == nil {
		// Full-ish jitter: a random point in [d/2, d].
		o.Jitter = func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
		}
	}
}

// Engine reconciles the settings resource. Construct with New, run with Run.
type Engine struct {
	up    Upstream
	store *prefs.Store
	queue *mutationqueue.Queue
	opt   Options

	mu     sync.Mutex
	status Status

	// writeMu serializes the read-merge-write of the store (Set and apply), so
	// two overlapping patches can't both merge from the same base and have the
	// later write drop the earlier's field.
	writeMu sync.Mutex

	// activeCancel cancels the in-flight watch so a poke forces a reconnect.
	activeCancel context.CancelFunc
	// pokeCh wakes an in-flight backoff so Run retries immediately.
	pokeCh chan struct{}
}

// New builds an Engine. It does not start anything; call Run.
func New(up Upstream, store *prefs.Store, queue *mutationqueue.Queue, opt Options) *Engine {
	opt.applyDefaults()
	return &Engine{
		up:     up,
		store:  store,
		queue:  queue,
		opt:    opt,
		pokeCh: make(chan struct{}, 1),
	}
}

// Set applies a patch locally and queues it for the cloud (local-first). The
// store is updated and published immediately; the patch is durably enqueued
// so it survives a restart; a Poke nudges the engine to push it now if live.
// Offline, the patch simply waits in the queue.
func (e *Engine) Set(patch prefs.Settings) error {
	// Serialize the whole read-merge-write so an overlapping Set or incoming
	// cloud apply can't merge from the same base and drop one patch's field.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	merged := prefs.Merge(e.store.Get(), patch)
	// Durably enqueue BEFORE exposing the local change: a published/persisted
	// edit must never exist without a queued mutation to carry it to the cloud,
	// otherwise a failed enqueue would leave a local edit the next snapshot can
	// silently clobber.
	ent, err := e.queue.Enqueue(patch)
	if err != nil {
		return err
	}
	if _, err := e.store.Set(merged); err != nil {
		// Local persistence failed: roll the queued mutation back so we never
		// send/ack an edit to the cloud that was never applied locally. If the
		// rollback itself fails, surface both errors — the entry may still be
		// queued (and would reconcile via the cloud), so the caller must know
		// the local write and queue state diverged rather than assume a clean
		// no-op.
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
	go e.wakeLoop(ctx)

	attempt := 0
	for ctx.Err() == nil {
		e.setStatus(func(s *Status) { s.State = StateConnecting })

		// Install the per-attempt cancel BEFORE any upstream call, so a Poke
		// (e.g. from sign-out) cancels whatever authenticated work is in flight
		// — Snapshot, the drain's Updates, the Watch open, or the live stream —
		// not just an already-open watch. Without this, a sign-out racing the
		// connect could let a Snapshot/Update complete and mutate/ack after the
		// user signed out.
		attemptCtx, cancel := context.WithCancel(ctx)
		e.setActiveCancel(cancel)

		snap, err := e.up.Snapshot(attemptCtx)
		if err != nil {
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}
		// A poke (sign-out) may have cancelled the attempt between Snapshot
		// returning and applying it. Don't apply a cloud snapshot or mark it
		// synced after the user is signing out; abandon and reconnect (which
		// then fails as signed-out and idles).
		if attemptCtx.Err() != nil {
			if e.abandonAttempt(ctx, cancel) {
				return
			}
			continue
		}
		if err := e.apply(snap, true); err != nil {
			// Couldn't persist the snapshot locally — don't mark it synced or
			// open the watch; back off and retry.
			if e.connectFailed(ctx, attemptCtx, cancel, &attempt, err) {
				return
			}
			continue
		}

		// Drain queued local edits to the cloud AFTER pulling the snapshot but
		// BEFORE opening the watch. Opening the watch after the drain is what
		// protects an acked edit: the stream's initial/on-subscribe state then
		// already reflects the pushed edit, so there is no pre-update (stale)
		// event left in flight that could clobber it once the queue stops
		// re-layering it. Draining after the snapshot (not before) means a
		// persistently-rejected Update can't block pulling cloud changes.
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
				// A local store failure mid-stream is a sync failure: tear the
				// watch down and back off rather than streaming against
				// unpersisted state.
				applyErr = err
				break
			}
			progressed = true
		}
		e.clearActive(cancel)

		if ctx.Err() != nil {
			return
		}
		// Fold in the stream's terminal error: a read/transport failure that
		// closed the stream is a sync failure too, not a clean end — surface it
		// and don't reset the backoff (a flapping connection must keep escalating).
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
// It runs synchronously after the snapshot and before the watch each connection
// cycle, so a failure simply propagates up and the supervisor backs off and
// retries the still-queued entries on the next cycle. It stops at the first
// failure (the connection likely dropped), leaving that entry and the rest
// queued.
func (e *Engine) drain(ctx context.Context) error {
	for _, ent := range e.queue.Pending() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := e.up.Update(ctx, ent.Patch); err != nil {
			return err
		}
		// A poke (sign-out) racing right after the cloud accepted the update must
		// not let us ack it: bail with the cancellation, leaving the entry queued.
		// The cloud's deep-merge makes re-sending it next cycle idempotent, so
		// nothing is lost — and we avoid acking a mutation as the user signs out.
		if err := ctx.Err(); err != nil {
			return err
		}
		// If the ack can't be durably persisted, stop draining: the entry is
		// still in the queue, so continuing would let us consider already-sent
		// patches drained while a restart would replay them. Retry next cycle.
		if err := e.queue.Ack(ent.ID); err != nil {
			return err
		}
	}
	return nil
}

// apply reconciles an incoming cloud value: still-pending local patches are
// re-layered on top (so a snapshot can't clobber an unacked edit), then the
// result is written to the store (which dedups + publishes only on change).
// snapshot==true stamps LastSyncedAt. A store failure is returned so the
// caller treats it as a sync failure rather than marking unpersisted state as
// synced and draining queued mutations against it.
func (e *Engine) apply(v prefs.Settings, snapshot bool) error {
	// Serialize against Set (and other applies): the re-layer + store write must
	// be atomic so a concurrent local edit can't be lost.
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
		now := e.opt.Now()
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

// clearActive unregisters the per-attempt cancel and cancels it, so the
// attempt's context is released and a later Poke doesn't cancel a stale one.
func (e *Engine) clearActive(cancel context.CancelFunc) {
	e.setActiveCancel(nil)
	cancel()
}

// connectFailed handles a failure of a pre-Live connection step (snapshot,
// snapshot-apply, drain, or watch open). A poke (sign-out / forced resync)
// cancels the per-attempt context while the parent ctx is still alive; that's
// an intentional restart, not a sync failure, so it must not record an error or
// ratchet backoff — it's routed to abandonAttempt for an immediate reconnect.
// Otherwise it tears down the in-flight attempt and enters the Offline backoff.
// It returns true if ctx ended during the wait (Run should exit); the caller
// otherwise continues to the next attempt.
func (e *Engine) connectFailed(ctx, attemptCtx context.Context, cancel context.CancelFunc, attempt *int, err error) bool {
	if ctx.Err() == nil && attemptCtx.Err() != nil {
		return e.abandonAttempt(ctx, cancel)
	}
	e.clearActive(cancel)
	return e.enterWaiting(ctx, attempt, StateOffline, err.Error())
}

// abandonAttempt tears down an attempt that a poke cancelled mid-step: there is
// no failure to record and backoff must not escalate. It drains the pending
// poke token — the immediate reconnect already serves the poke's intent, so the
// token must not also short-circuit a later genuine backoff. Returns true if
// the parent ctx ended (Run should exit).
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
	d := e.opt.BaseBackoff << attempt
	if d <= 0 || d > e.opt.MaxBackoff {
		d = e.opt.MaxBackoff
	}
	return e.opt.Jitter(d)
}

// enterWaiting records the retry state and sleeps the backoff delay, racing
// the (injectable) sleep against a poke. attempt stops incrementing once the
// delay reaches the cap. Returns true if ctx ended during the wait.
func (e *Engine) enterWaiting(ctx context.Context, attempt *int, st State, errMsg string) bool {
	d := e.backoffDelay(*attempt)
	if e.opt.BaseBackoff<<*attempt < e.opt.MaxBackoff {
		*attempt++
	}
	e.setStatus(func(s *Status) {
		s.State = st
		s.LastError = errMsg
		s.RetryAt = e.opt.Now().Add(d)
	})

	sctx, scancel := context.WithCancel(ctx)
	defer scancel()
	slept := make(chan struct{})
	go func() {
		_ = e.opt.Sleep(sctx, d)
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

// wakeLoop is the wall-clock resume backstop: the ticker can't fire while the
// process is frozen, so a tick arriving far past the heartbeat interval means
// a sleep/resume — poke an immediate resync.
func (e *Engine) wakeLoop(ctx context.Context) {
	t := time.NewTicker(e.opt.Tick)
	defer t.Stop()

	lastSeen := e.opt.Now()
	threshold := time.Duration(float64(e.opt.Tick) * e.opt.GapFactor)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := e.opt.Now()
			if now.Sub(lastSeen) > threshold {
				e.Poke()
			}
			lastSeen = now
		}
	}
}
