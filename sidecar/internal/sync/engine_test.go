package sync_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
)

// fakeUpstream is a programmable stand-in for the cloud client. Snapshot
// and Watch behavior are supplied per-test via funcs; call counts are
// recorded so tests can assert "one upstream connection regardless of
// subscribers" and "resync happened after wake".
type fakeUpstream struct {
	snapshots atomic.Int64
	watches   atomic.Int64

	snapshot func(ctx context.Context, n int64) (prefs.Settings, error)
	watch    func(ctx context.Context, n int64) (<-chan prefs.Settings, error)
}

func (f *fakeUpstream) Snapshot(ctx context.Context) (prefs.Settings, error) {
	n := f.snapshots.Add(1)
	if f.snapshot != nil {
		return f.snapshot(ctx, n)
	}
	return prefs.Settings{Placeholder: "snap"}, nil
}

func (f *fakeUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, error) {
	n := f.watches.Add(1)
	if f.watch != nil {
		return f.watch(ctx, n)
	}
	ch := make(chan prefs.Settings)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}

func newStore(t *testing.T) *syncstore.Store[prefs.Settings] {
	t.Helper()
	return syncstore.NewStore[prefs.Settings](filepath.Join(t.TempDir(), "settings.json"))
}

// (a) After an upstream watch ends, reconnect delays follow an exponential
// schedule (no jitter, injected here) capped at MaxBackoff, and the
// schedule resets to base once a connection goes Live again.
func TestBackoffScheduleExponentialAndCapped(t *testing.T) {
	var mu sync.Mutex
	var slept []time.Duration

	up := &fakeUpstream{
		// Watch returns an already-closed channel: upstream "ends"
		// immediately, forcing the backoff path every loop.
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			ch := make(chan prefs.Settings)
			close(ch)
			return ch, nil
		},
	}

	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  40 * time.Millisecond,
		Jitter:      func(d time.Duration) time.Duration { return d }, // identity
		Sleep: func(ctx context.Context, d time.Duration) error {
			mu.Lock()
			slept = append(slept, d)
			mu.Unlock()
			return ctx.Err()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { eng.Run(ctx); close(done) }()

	// Let several reconnect cycles accumulate, then stop.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(slept) < 4 {
		t.Fatalf("want >=4 backoff sleeps, got %v", slept)
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		if slept[i] != w {
			t.Fatalf("backoff[%d]=%v, want %v (full=%v)", i, slept[i], w, slept)
		}
	}
}

// (b) On (re)connect the engine takes a snapshot, persists it with bumped
// Version + LastSyncedAt, then applies stream events with LastEventAt.
func TestSnapshotThenStreamPersistedWithMetadata(t *testing.T) {
	events := make(chan prefs.Settings, 1)
	up := &fakeUpstream{
		snapshot: func(context.Context, int64) (prefs.Settings, error) {
			return prefs.Settings{Placeholder: "snap"}, nil
		},
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			out := make(chan prefs.Settings)
			go func() {
				defer close(out)
				for {
					select {
					case <-ctx.Done():
						return
					case v := <-events:
						out <- v
					}
				}
			}()
			return out, nil
		},
	}
	store := newStore(t)
	hub := prefs.NewHub()
	sub, unsub := hub.Subscribe()
	defer unsub()

	now := atomic.Int64{}
	now.Store(1_000)
	eng := syncpkg.New(up, store, hub, syncpkg.Options{
		Now: func() time.Time { return time.UnixMilli(now.Load()) },
	})
	go eng.Run(t.Context())

	// Snapshot delivered to Hub + persisted with sync metadata.
	if got := <-sub; got.Placeholder != "snap" {
		t.Fatalf("hub snapshot: %+v", got)
	}
	env := waitEnvelope(t, store, func(e syncstore.Envelope[prefs.Settings]) bool {
		return e.Data.Placeholder == "snap" && e.LastSyncedAt == 1_000
	})
	if env.Version == "" {
		t.Fatalf("snapshot version not set: %+v", env)
	}
	snapVer := env.Version

	now.Store(2_000)
	events <- prefs.Settings{Placeholder: "ev"}
	if got := <-sub; got.Placeholder != "ev" {
		t.Fatalf("hub event: %+v", got)
	}
	env = waitEnvelope(t, store, func(e syncstore.Envelope[prefs.Settings]) bool {
		return e.Data.Placeholder == "ev" && e.LastEventAt == 2_000
	})
	if env.Version == snapVer {
		t.Fatalf("version not bumped on event: %q", env.Version)
	}
}

// (c) State machine: Snapshot error -> Offline (carries the error);
// recovery -> Live; clean upstream end after live -> Backoff (no error).
// Transitions are gated by the fake so each state is durably observable
// rather than racing on microsecond-lived states.
func TestStateMachineTransitions(t *testing.T) {
	failFirst := atomic.Bool{}
	failFirst.Store(true)
	endLive := make(chan struct{}) // test signals the live connection to end
	liveOnce := atomic.Bool{}

	up := &fakeUpstream{
		snapshot: func(_ context.Context, _ int64) (prefs.Settings, error) {
			if failFirst.Swap(false) {
				return prefs.Settings{}, errors.New("boom")
			}
			return prefs.Settings{Placeholder: "ok"}, nil
		},
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			ch := make(chan prefs.Settings)
			if liveOnce.CompareAndSwap(false, true) {
				// First (live) connection: stays open until the test
				// signals, then ends cleanly -> Backoff.
				go func() {
					select {
					case <-endLive:
					case <-ctx.Done():
					}
					close(ch)
				}()
			} else {
				go func() { <-ctx.Done(); close(ch) }()
			}
			return ch, nil
		},
	}
	// Backoff long enough that Offline is comfortably observable by the
	// 1ms poller during the post-error wait.
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
		Jitter:      func(d time.Duration) time.Duration { return d },
	})
	go eng.Run(t.Context())

	waitState(t, eng, syncpkg.StateOffline, "boom")
	waitState(t, eng, syncpkg.StateLive, "")
	close(endLive)
	waitState(t, eng, syncpkg.StateBackoff, "")
}

// (d) Wake detector: a wall-clock gap far larger than the heartbeat tick
// forces an immediate resync (a fresh Snapshot) even though the active
// watch never ended.
func TestWakeGapForcesResync(t *testing.T) {
	now := atomic.Int64{}
	now.Store(time.Now().UnixMilli())

	up := &fakeUpstream{
		// Watch blocks until ctx cancel — only a forced reconnect can
		// cause a second Snapshot.
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			ch := make(chan prefs.Settings)
			go func() { <-ctx.Done(); close(ch) }()
			return ch, nil
		},
	}
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		Tick:      20 * time.Millisecond,
		GapFactor: 2,
		Now:       func() time.Time { return time.UnixMilli(now.Load()) },
	})
	go eng.Run(t.Context())

	waitCount(t, &up.snapshots, 1)
	// Simulate the process being frozen for 1s (>> Tick*GapFactor).
	now.Add(1_000)
	waitCount(t, &up.snapshots, 2)
}

// (e) One upstream Watch is opened regardless of how many Hub subscribers
// exist; every subscriber still sees the event.
func TestSingleUpstreamFansOutToAllSubscribers(t *testing.T) {
	emit := make(chan prefs.Settings, 1)
	up := &fakeUpstream{
		// Stays open for the whole test so the connection never ends and
		// reconnects (which would bump the Watch count past 1).
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			out := make(chan prefs.Settings)
			go func() {
				defer close(out)
				for {
					select {
					case <-ctx.Done():
						return
					case v := <-emit:
						out <- v
					}
				}
			}()
			return out, nil
		},
	}
	hub := prefs.NewHub()
	a, ua := hub.Subscribe()
	b, ub := hub.Subscribe()
	defer ua()
	defer ub()

	eng := syncpkg.New(up, newStore(t), hub, syncpkg.Options{})
	go eng.Run(t.Context())

	// Drain the snapshot publish that precedes streaming.
	<-a
	<-b
	emit <- prefs.Settings{Placeholder: "fanned"}

	for _, ch := range []<-chan prefs.Settings{a, b} {
		select {
		case got := <-ch:
			if got.Placeholder != "fanned" {
				t.Fatalf("subscriber got %+v", got)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("subscriber did not receive event")
		}
	}
	if n := up.watches.Load(); n != 1 {
		t.Fatalf("want exactly 1 upstream Watch, got %d", n)
	}
}

// --- helpers ---

func waitEnvelope(t *testing.T, s *syncstore.Store[prefs.Settings], ok func(syncstore.Envelope[prefs.Settings]) bool) syncstore.Envelope[prefs.Settings] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e, err := s.Load()
		if err == nil && ok(e) {
			return e
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("store did not reach expected state in time")
	return syncstore.Envelope[prefs.Settings]{}
}

func waitState(t *testing.T, eng *syncpkg.Engine, want syncpkg.State, wantErrSubstr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last syncpkg.Status
	for time.Now().Before(deadline) {
		last = eng.Status()
		if last.State == want && strings.Contains(last.LastError, wantErrSubstr) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state never reached %v/%q (last=%+v)", want, wantErrSubstr, last)
}

func waitCount(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("counter never reached %d (got %d)", want, c.Load())
}

// WatchStatus streams transitions to subscribers (push-based, like
// prefs.Hub — the syncStatusWatch resolver relies on this instead of
// polling Status()).
func TestWatchStatusStreamsTransitions(t *testing.T) {
	up := &fakeUpstream{
		snapshot: func(context.Context, int64) (prefs.Settings, error) {
			return prefs.Settings{}, errors.New("boom")
		},
	}
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Jitter:      func(d time.Duration) time.Duration { return d },
	})
	sub, unsub := eng.WatchStatus()
	defer unsub()
	go eng.Run(t.Context())

	deadline := time.After(3 * time.Second)
	for {
		select {
		case s := <-sub:
			if s.State == syncpkg.StateOffline && s.LastError == "boom" {
				return
			}
		case <-deadline:
			t.Fatal("never observed Offline on the status stream")
		}
	}
}

// OnConnected fires when the engine reaches Live (the offline-queue
// drainer hooks here). It runs concurrently with streaming and is
// cancelled when the connection drops.
func TestOnConnectedFiresOnLive(t *testing.T) {
	up := &fakeUpstream{
		watch: func(ctx context.Context, _ int64) (<-chan prefs.Settings, error) {
			ch := make(chan prefs.Settings)
			go func() { <-ctx.Done(); close(ch) }()
			return ch, nil
		},
	}
	connected := make(chan struct{}, 1)
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		OnConnected: func(context.Context) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	})
	go eng.Run(t.Context())

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnected never fired on reaching Live")
	}
}

// Poke interrupts an in-flight backoff so Run retries immediately,
// instead of waiting out the (here, very long) delay.
func TestPokeInterruptsBackoff(t *testing.T) {
	up := &fakeUpstream{
		snapshot: func(context.Context, int64) (prefs.Settings, error) {
			return prefs.Settings{}, errors.New("down")
		},
	}
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{
		BaseBackoff: 30 * time.Second, // ≫ test deadline; only a poke can shortcut it
		MaxBackoff:  30 * time.Second,
	})
	go eng.Run(t.Context())

	waitCount(t, &up.snapshots, 1) // first attempt failed → now in backoff
	eng.Poke()
	waitCount(t, &up.snapshots, 2) // retried well before 30s would elapse
}

// Poke cancels the active watch so a healthy-but-stale connection is
// dropped and Run resnapshots (the OS-resume path; same effect the
// wall-clock backstop produces).
func TestPokeCancelsActiveWatch(t *testing.T) {
	// Default fakeUpstream.Watch already blocks until ctx is cancelled.
	up := &fakeUpstream{}
	eng := syncpkg.New(up, newStore(t), prefs.NewHub(), syncpkg.Options{})
	go eng.Run(t.Context())

	waitCount(t, &up.snapshots, 1) // Live on the first watch
	eng.Poke()
	waitCount(t, &up.snapshots, 2) // watch cancelled → resnapshot
}
