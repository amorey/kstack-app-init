package cloud

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/api"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
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
	return f.stateLocked(), nil
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

// Subscribe delivers the current State on subscribe (so the consumer seeds its
// baseline without poking), then every subsequent change.
//
// Deliberately a lossless buffered fan-out rather than a gochan/watch hub, which
// is what the real service publishes through: the consumer under test detects the
// signed-in → signed-out EDGE, and a latest-value hub is free to coalesce the seed
// with the change into one delivery, leaving the consumer with a single value it
// reads as its baseline. The edge is the thing being tested, so the fake has to
// deliver both sides of it.
func (f *fakeAuth) Subscribe() (<-chan auth.State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan auth.State, 8)
	ch <- f.stateLocked() // current-on-subscribe
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

func (f *fakeAuth) stateLocked() auth.State {
	return auth.State{Authenticated: f.signedIn}
}

// publishLocked publishes the current State to subscribers (non-blocking).
// Caller holds f.mu.
func (f *fakeAuth) publishLocked() {
	st := f.stateLocked()
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
	started *testutil.Signal
}

func newSignalUpstream() *signalUpstream {
	return &signalUpstream{started: testutil.NewSignal()}
}

func (s *signalUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	s.started.Fire()
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
	watchOpened *testutil.Probe[struct{}]
	cancelled   *testutil.Probe[struct{}]
}

func newCancelWatchUpstream() *cancelWatchUpstream {
	return &cancelWatchUpstream{
		watchOpened: testutil.NewProbe[struct{}](8),
		cancelled:   testutil.NewProbe[struct{}](8),
	}
}

func (u *cancelWatchUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

func (u *cancelWatchUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	u.watchOpened.Fire(struct{}{})
	go func() {
		<-ctx.Done()
		u.cancelled.Fire(struct{}{})
		close(ch)
	}()
	return ch, nil, nil
}

func (u *cancelWatchUpstream) Update(_ context.Context, p prefs.Settings) (prefs.Settings, error) {
	return p, nil
}

// cloud observes the auth session and pokes its own engine (auth never reaches
// into the settings-sync engine), so signing out via the auth service must tear
// down the already-open, previously-authenticated watch — otherwise it could keep
// delivering cloud events that mutate local prefs after sign-out.
func TestWatchTornDownOnAuthSignOut(t *testing.T) {
	authSvc := signedInFakeAuth()

	up := newCancelWatchUpstream()
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up))
	if err != nil {
		t.Fatalf("cloud.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	up.watchOpened.Await(t, "the engine to open a watch")

	// Sign out via auth → session change → cloud pokes its engine → the live,
	// in-flight authenticated watch is cancelled.
	if err := authSvc.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	// the live watch must be cancelled in response to the auth sign-out
	up.cancelled.Await(t, "the live watch to be torn down on auth sign-out")

	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// Start is idempotent: a second call must not launch (and leak) a second engine
// goroutine, and stop still unwinds cleanly.
func TestStartIsIdempotent(t *testing.T) {
	authSvc, _ := auth.New(auth.Config{})
	up := newSignalUpstream()
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := svc.Start(ctx); err != nil { // second start must be a no-op
		t.Fatalf("second Start: %v", err)
	}

	up.started.Wait(t, "the engine to start")
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
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

// Start (and its returned stop) on a degraded Service are no-ops and don't panic.
func TestDegradedLifecycleNoop(t *testing.T) {
	svc, err := New("", "", nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, err := svc.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// A fully-configured Service starts the engine on Start and stops it on Close.
func TestConfiguredStartStop(t *testing.T) {
	authSvc, _ := auth.New(auth.Config{})
	up := newSignalUpstream()
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", authSvc, nil, withUpstream(up)) // test seam: inject fake instead of the api client
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.Prefs() == nil {
		t.Fatal("configured service should have a Prefs store")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// the engine reached Snapshot → it's running
	up.started.Wait(t, "the engine to start")

	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// A data dir that cannot hold the settings file is a construction failure, not
// a degrade: degrading is for "no cloud configured", and silently dropping the
// user's prefs on the floor is a different thing entirely.
func TestNewFailsOnAnUnusableDataDir(t *testing.T) {
	// A directory where the settings file goes, not a file where the dir goes:
	// Windows reports a path under a regular file as "not found", which is what
	// a settings file nobody has written yet looks like.
	dataDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dataDir, "settings.json"), 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := newWithOptions(dataDir, "https://api.example.test", nil, nil, withUpstream(newSignalUpstream())); err == nil {
		t.Fatal("New with an unusable data dir returned no error")
	}
}

// Without an auth service there is no session to watch, so Start runs the
// engine alone and stop still unwinds cleanly.
func TestStartWithoutAuthStopsCleanly(t *testing.T) {
	up := newSignalUpstream()
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", nil, nil, withUpstream(up))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stop, err := svc.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	up.started.Wait(t, "the engine to start")
	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// stop is bounded by its own context: a caller that has run out of shutdown
// budget gets the deadline error rather than blocking on a goroutine that is
// taking too long.
func TestStopIsBoundedByItsContext(t *testing.T) {
	up := newSignalUpstream()
	svc, err := newWithOptions(t.TempDir(), "https://api.example.test", signedInFakeAuth(), nil, withUpstream(up))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, err := svc.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	up.started.Wait(t, "the engine to start")

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stop(expired); err == nil {
		t.Fatal("stop with an expired context returned nil")
	}
	// The goroutines still unwind; give them an unbounded stop so the test leaves
	// nothing running.
	_ = stop(context.Background())
}

// The api-backed upstream is a pass-through to the client; the adapter is what
// the engine actually calls, so both directions are exercised against a server.
func TestAPIUpstreamPassesThroughToTheClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"updateSettings":{"theme":"dark"}}}`)
	}))
	defer ts.Close()

	up := apiUpstream{c: api.New(ts.URL, func(context.Context) oauth2.TokenSource {
		return staticSource{}
	})}

	got, err := up.Update(context.Background(), prefs.Settings{})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got.Theme == nil || *got.Theme != "dark" {
		t.Errorf("Update returned %+v, want the server's settings", got)
	}
	ch, _, err := up.Watch(context.Background())
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// The body is one JSON document, not an event stream: no frame is published and
	// the stream ends — the adapter forwarding the client's channel is the point.
	testutil.RecvClosed(t, ch, "the watch stream")
}

// staticSource is a token source for the api-client pass-through test.
type staticSource struct{}

func (staticSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "tok"}, nil
}
