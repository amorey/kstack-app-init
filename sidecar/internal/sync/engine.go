// Package sync is the sidecar's always-on reconciliation engine. It owns a
// single supervised connection to the upstream (the kstack cloud): take a
// snapshot, persist it to the syncstore, then stream events into the local
// prefs.Hub. On any upstream end/error it reconnects with exponential
// backoff; on a detected wall-clock gap (machine sleep/resume) it forces an
// immediate resync so stale state can't linger after wake.
//
// The engine is resource-agnostic by construction (it talks to an Upstream
// interface, not *cloud.Client) so cluster resources — and later an
// alerting layer hanging off the same applied-state stream — can reuse it
// without rework. This package is a pure library: nothing here starts it;
// wiring into the process lifecycle is a later step.
package sync

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
)

// Upstream is the slice of the cloud client the engine needs. Snapshot is a
// point-in-time read; Watch opens a stream whose channel closes when the
// upstream ends or ctx is cancelled. Kept minimal so tests can fake it and
// non-Settings resources can implement it later.
type Upstream interface {
	Snapshot(ctx context.Context) (prefs.Settings, error)
	Watch(ctx context.Context) (<-chan prefs.Settings, error)
}

// State is the engine's connection lifecycle, exposed for the (later)
// syncStatus GraphQL surface.
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
	RetryAt      time.Time // meaningful when State == StateBackoff
	LastSyncedAt time.Time
}

// Options configures an Engine. All zero values get sane defaults; the
// injectable seams (Now/Sleep/Jitter) exist so tests stay deterministic.
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
		// Full-ish jitter: a random point in [d/2, d]. Spreads reconnect
		// storms without ever waiting longer than the computed ceiling.
		o.Jitter = func(d time.Duration) time.Duration {
			if d <= 0 {
				return 0
			}
			return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
		}
	}
}

// Engine reconciles one resource. Construct with New, run with Run.
type Engine struct {
	up    Upstream
	store *syncstore.Store[prefs.Settings]
	hub   *prefs.Hub
	opt   Options

	mu     sync.Mutex
	status Status
	ver    uint64
	// lastEnv is the in-memory mirror of what's on disk. Holding it avoids
	// a Load() per event just to carry sibling timestamps forward, and lets
	// apply skip the disk write + Hub publish when nothing changed (a
	// reconnect/wake re-snapshot is otherwise a no-op storm). seeded is
	// false until the first real value is applied (or restored from disk),
	// so the initial snapshot is never deduped against the zero value.
	lastEnv syncstore.Envelope[prefs.Settings]
	seeded  bool

	// activeCancel cancels the in-flight watch so the wake detector can
	// force a reconnect without waiting for the upstream to end. Nil
	// between watches.
	activeCancel context.CancelFunc

	// statusSubs fans every status transition out to syncStatusWatch
	// subscribers. Same shape as prefs.Hub (buffered, drop-on-full) so a
	// slow consumer can't stall the supervisor loop.
	subsMu     sync.Mutex
	statusSubs map[chan Status]struct{}
}

// New builds an Engine. It does not start anything; call Run.
func New(up Upstream, store *syncstore.Store[prefs.Settings], hub *prefs.Hub, opt Options) *Engine {
	opt.applyDefaults()
	return &Engine{
		up:         up,
		store:      store,
		hub:        hub,
		opt:        opt,
		statusSubs: make(map[chan Status]struct{}),
	}
}

// WatchStatus returns a buffered channel of status transitions and an
// unsubscribe func (closed on unsub; idempotent). It streams *changes*;
// callers that need the current value read Status() first (the
// syncStatusWatch resolver emits that snapshot before streaming).
func (e *Engine) WatchStatus() (<-chan Status, func()) {
	ch := make(chan Status, 4)
	e.subsMu.Lock()
	e.statusSubs[ch] = struct{}{}
	e.subsMu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			e.subsMu.Lock()
			if _, ok := e.statusSubs[ch]; ok {
				delete(e.statusSubs, ch)
				close(ch)
			}
			e.subsMu.Unlock()
		})
	}
}

func (e *Engine) broadcastStatus(s Status) {
	e.subsMu.Lock()
	defer e.subsMu.Unlock()
	for ch := range e.statusSubs {
		select {
		case ch <- s:
		default:
		}
	}
}

// Status returns the current health snapshot. Safe for concurrent use.
func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status
}

// setStatus applies mut and, only if the resulting Status actually
// changed, fans it out. Called exclusively from the supervisor goroutine
// (Run and its callees), so transitions are serialized — broadcast order
// matches mutation order without holding e.mu across the fan-out. The
// change guard stops a reconnect storm (Connecting→Offline→Connecting…)
// from spamming subscribers with byte-identical statuses.
func (e *Engine) setStatus(mut func(*Status)) {
	e.mu.Lock()
	prev := e.status
	mut(&e.status)
	s := e.status
	e.mu.Unlock()
	if s != prev {
		e.broadcastStatus(s)
	}
}

// Run blocks until ctx is cancelled, supervising the upstream connection.
func (e *Engine) Run(ctx context.Context) {
	e.restore()
	go e.wakeLoop(ctx)

	attempt := 0
	for ctx.Err() == nil {
		e.setStatus(func(s *Status) { s.State = StateConnecting })

		snap, err := e.up.Snapshot(ctx)
		if err != nil {
			if e.enterWaiting(ctx, &attempt, StateOffline, err.Error()) {
				return
			}
			continue
		}
		e.apply(snap, true)

		watchCtx, cancel := context.WithCancel(ctx)
		e.setActiveCancel(cancel)

		ch, err := e.up.Watch(watchCtx)
		if err != nil {
			cancel()
			e.setActiveCancel(nil)
			if e.enterWaiting(ctx, &attempt, StateOffline, err.Error()) {
				return
			}
			continue
		}

		e.setStatus(func(s *Status) {
			s.State = StateLive
			s.LastError = ""
		})

		progressed := false
		for ev := range ch {
			e.apply(ev, false)
			progressed = true
		}
		cancel()
		e.setActiveCancel(nil)

		if ctx.Err() != nil {
			return
		}
		// A connection only resets the backoff schedule if it made real
		// progress (delivered at least one event). A watch that opens and
		// instantly closes is a flapping upstream — keep escalating.
		if progressed {
			attempt = 0
		}
		// Clean end after a live connection: Backoff (no error), held for
		// the whole retry wait, then loop to resnapshot and reconnect.
		if e.enterWaiting(ctx, &attempt, StateBackoff, "") {
			return
		}
	}
}

// restore seeds the in-memory mirror from disk so a cold start resumes from
// the last reconciled state: dedup works across restarts and the version
// counter stays monotonic (a fresh "1" could otherwise collide with a
// pre-restart version). An empty/absent file leaves seeded=false so the
// first snapshot always publishes.
func (e *Engine) restore() {
	env, err := e.store.Load()
	if err != nil || env.Version == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastEnv = env
	e.seeded = true
	if n, perr := strconv.ParseUint(env.Version, 10, 64); perr == nil {
		e.ver = n
	}
	e.status.LastSyncedAt = time.UnixMilli(env.LastSyncedAt)
}

// apply records v as the reconciled value and fans it out to the Hub.
// snapshot==true stamps LastSyncedAt; events stamp LastEventAt. If the
// payload is unchanged it skips the disk write and Hub publish entirely —
// every reconnect/wake re-snapshots, so an unchanged value must not rewrite
// disk or wake subscribers; only the in-memory freshness is refreshed.
func (e *Engine) apply(v prefs.Settings, snapshot bool) {
	now := e.opt.Now()

	e.mu.Lock()
	if e.seeded && v == e.lastEnv.Data {
		if snapshot {
			e.lastEnv.LastSyncedAt = now.UnixMilli()
			e.status.LastSyncedAt = now
		}
		e.mu.Unlock()
		return
	}
	e.ver++
	env := syncstore.Envelope[prefs.Settings]{
		Data:         v,
		Version:      strconv.FormatUint(e.ver, 10),
		LastSyncedAt: e.lastEnv.LastSyncedAt,
		LastEventAt:  e.lastEnv.LastEventAt,
	}
	if snapshot {
		env.LastSyncedAt = now.UnixMilli()
	} else {
		env.LastEventAt = now.UnixMilli()
	}
	e.lastEnv = env
	e.seeded = true
	if snapshot {
		e.status.LastSyncedAt = now
	}
	e.mu.Unlock()

	_ = e.store.Save(env)
	e.hub.Publish(v)
}

// backoffDelay is the exponential-with-jitter delay for a given attempt,
// clamped to MaxBackoff. The clamp also backstops the `<<` overflowing to a
// non-positive value at extreme attempts; enterWaiting bounds attempt so it
// never gets that far in practice.
func (e *Engine) backoffDelay(attempt int) time.Duration {
	d := e.opt.BaseBackoff << attempt
	if d <= 0 || d > e.opt.MaxBackoff {
		d = e.opt.MaxBackoff
	}
	return e.opt.Jitter(d)
}

// enterWaiting records the retry state (Offline with errMsg after a connect
// failure, or Backoff with "" after a clean post-live end) and sleeps the
// backoff delay. attempt stops incrementing once the delay reaches the cap,
// keeping it bounded. Returns true if ctx ended during the wait.
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
	return e.opt.Sleep(ctx, d) != nil
}

// setActiveCancel publishes (or clears, with nil) the in-flight watch's
// cancel func for the wake detector.
func (e *Engine) setActiveCancel(c context.CancelFunc) {
	e.mu.Lock()
	e.activeCancel = c
	e.mu.Unlock()
}

// wakeLoop is the resume detector. The ticker can't fire while the process
// is frozen, so on the first tick after a sleep the wall clock has jumped
// far past the heartbeat interval — that gap forces the active watch
// closed, which makes Run take a fresh snapshot.
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
				e.mu.Lock()
				cancel := e.activeCancel
				e.mu.Unlock()
				if cancel != nil {
					cancel()
				}
			}
			lastSeen = now
		}
	}
}
