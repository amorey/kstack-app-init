package cloud

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
)

// fakeAuth is a hand-written auth.Service for the poke-inversion test. cloud
// depends on the auth.Service interface (Subscribe + a latest-value session
// State stream), so the test fakes that interface rather than building the real
// service. It starts signed in and, on Logout, publishes the signed-out State
// the cloud engine reacts to.
type fakeAuth struct {
	mu       sync.Mutex
	signedIn bool
	subs     map[int]chan auth.State
	nextID   int
}

func signedInFakeAuth() *fakeAuth {
	return &fakeAuth{signedIn: true, subs: map[int]chan auth.State{}}
}

func (f *fakeAuth) Current(context.Context) (auth.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return auth.State{Authenticated: f.signedIn}, nil
}

func (f *fakeAuth) StartLogin(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signedIn = true
	f.publishLocked()
	return nil
}

func (f *fakeAuth) Logout(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signedIn = false
	f.publishLocked()
	return nil
}

// TokenSource is unused here — the test injects a fake Upstream, so cloud.New
// never builds the api client from a token source.
func (f *fakeAuth) TokenSource(context.Context) oauth2.TokenSource { return nil }

// Subscribe mirrors the real latest-value stream: it delivers the current State
// on subscribe (so the consumer seeds its baseline without poking), then each
// subsequent change.
func (f *fakeAuth) Subscribe() (<-chan auth.State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan auth.State, 8)
	ch <- auth.State{Authenticated: f.signedIn} // current-on-subscribe
	f.subs[id] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if c, ok := f.subs[id]; ok {
			delete(f.subs, id)
			close(c)
		}
	}
}

// publishLocked publishes the current State to subscribers (non-blocking).
// Caller holds f.mu.
func (f *fakeAuth) publishLocked() {
	st := auth.State{Authenticated: f.signedIn}
	for _, ch := range f.subs {
		select {
		case ch <- st:
		default:
		}
	}
}

// signalUpstream is a fake prefsync.Upstream whose Snapshot signals that the
// engine has started running.
type signalUpstream struct {
	started chan struct{}
}

func (s *signalUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	return prefs.Settings{}, nil
}

func (s *signalUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil, nil
}

func (s *signalUpstream) Update(_ context.Context, p prefs.Settings) (prefs.Settings, error) {
	return p, nil
}

// cancelWatchUpstream signals when each opened Watch's context is cancelled,
// so a test can observe the live stream being torn down.
type cancelWatchUpstream struct {
	watchOpened chan struct{}
	cancelled   chan struct{}
}

func (u *cancelWatchUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

func (u *cancelWatchUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	select {
	case u.watchOpened <- struct{}{}:
	default:
	}
	go func() {
		<-ctx.Done()
		select {
		case u.cancelled <- struct{}{}:
		default:
		}
		close(ch)
	}()
	return ch, nil, nil
}

func (u *cancelWatchUpstream) Update(_ context.Context, p prefs.Settings) (prefs.Settings, error) {
	return p, nil
}

// Poke inversion: the settings-sync engine no longer learns of sign-out from
// auth reaching into it; instead cloud observes the auth session and pokes its
// own engine. Signing out via the auth service must therefore tear down the
// already-open, previously-authenticated watch (otherwise it could keep
// delivering cloud events that mutate local prefs after sign-out).
func TestWatchTornDownOnAuthSignOut(t *testing.T) {
	authSvc := signedInFakeAuth()

	up := &cancelWatchUpstream{
		watchOpened: make(chan struct{}, 8),
		cancelled:   make(chan struct{}, 8),
	}
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up))
	if err != nil {
		t.Fatalf("cloud.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Start(ctx)

	select {
	case <-up.watchOpened:
	case <-time.After(2 * time.Second):
		t.Fatal("engine never opened a watch")
	}

	// Sign out via auth → session change → cloud pokes its engine → the live,
	// in-flight authenticated watch is cancelled.
	if err := authSvc.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	select {
	case <-up.cancelled:
		// good: the live watch was cancelled in response to the auth sign-out
	case <-time.After(2 * time.Second):
		t.Fatal("live watch was not torn down on auth sign-out")
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// Start is idempotent: a second call must not launch (and leak) a second engine
// goroutine, and Close still unwinds cleanly.
func TestStartIsIdempotent(t *testing.T) {
	authSvc, _ := auth.New(auth.Config{})
	up := &signalUpstream{started: make(chan struct{}, 1)}
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Start(ctx)
	svc.Start(ctx) // second start must be a no-op

	select {
	case <-up.started:
	case <-time.After(2 * time.Second):
		t.Fatal("engine did not start")
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// A Service built with no cloud config (and no auth) is safe — Prefs is absent
// and the lifecycle is a no-op.
func TestDegradedConstruction(t *testing.T) {
	svc, err := New("", "", nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.Prefs() != nil {
		t.Fatal("degraded service should have no Prefs store")
	}
}

// Start/Close on a degraded Service are no-ops and don't panic.
func TestDegradedLifecycleNoop(t *testing.T) {
	svc, err := New("", "", nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	svc.Start(context.Background())
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// A fully-configured Service starts the engine on Start and stops it on Close.
func TestConfiguredStartStop(t *testing.T) {
	authSvc, _ := auth.New(auth.Config{})
	up := &signalUpstream{started: make(chan struct{}, 1)}
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up)) // test seam: inject fake instead of the api client
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.Prefs() == nil {
		t.Fatal("configured service should have a Prefs store")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Start(ctx)

	select {
	case <-up.started:
		// engine reached Snapshot → it's running
	case <-time.After(2 * time.Second):
		t.Fatal("engine did not start")
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
