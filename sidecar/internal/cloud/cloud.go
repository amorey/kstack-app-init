// Package cloud is the composition + lifecycle owner for the cloud-synced
// settings feature (prefs, mutationqueue, api, prefsync). It mirrors
// internal/kube's Service shape — New wires the pieces, Start launches the
// background sync, Close tears it down — and degrades gracefully when the host
// hasn't configured a cloud URL / data dir (then it's a no-op with no settings
// store).
//
// It depends on the local-first internal/auth subsystem (not the reverse): it
// authenticates the api client from auth's oauth2.TokenSource, and wakes its
// sync engine by observing auth's session-change Events. auth knows nothing
// about settings sync.
package cloud

import (
	"context"
	"path/filepath"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/api"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// option is an unexported build seam for New. Because the type is unexported,
// only in-package (test) code can pass one — production callers pass dataDir +
// cloudURL alone and cannot inject a fake upstream. This mirrors the auth
// subsystem's white-box option pattern.
type option func(*buildOpts)

// buildOpts collects the seam overrides applied before New resolves the upstream.
type buildOpts struct {
	// upstream overrides the api-backed sync upstream with a fake; when set it
	// also bypasses the CloudURL + token-source requirement.
	upstream prefsync.Upstream
}

// withUpstream injects a fake prefsync.Upstream instead of the api client, so a
// white-box test can drive the engine without a real cloud endpoint or auth.
func withUpstream(up prefsync.Upstream) option {
	return func(o *buildOpts) { o.upstream = up }
}

// Service owns the cloud-synced settings subsystem.
type Service struct {
	auth auth.Service // the auth dependency; may be nil/degraded

	prefs  *prefs.Store     // nil when degraded
	engine *prefsync.Engine // nil when degraded

	cancel   context.CancelFunc
	done     chan struct{} // closed when the engine goroutine exits
	pokeDone chan struct{} // closed when the session→poke goroutine exits (nil if no auth)
	started  bool
}

// New builds the cloud settings service over the given auth subsystem. dataDir is
// the per-machine app data dir for the settings file + write queue; cloudURL is
// the kstack cloud API base URL (e.g. https://api.kstack.sh). It degrades (no
// engine, no prefs store) unless dataDir is set and an upstream is available —
// the api client needs cloudURL plus an oauth2.TokenSource from a configured auth
// service.
// New builds the cloud settings service. pokeSvc is the shared resync
// broadcaster owned by the app; nil disables external wake signals (degraded
// deployments, tests).
func New(dataDir, cloudURL string, authSvc auth.Service, pokeSvc *poke.Service) (*Service, error) {
	return newWithOptions(dataDir, cloudURL, authSvc, pokeSvc)
}

// newWithOptions is the build entry point that also accepts the unexported test
// seams. New is the production wrapper (no options); in-package white-box tests
// call this directly to inject a fake upstream.
func newWithOptions(dataDir, cloudURL string, authSvc auth.Service, pokeSvc *poke.Service, opts ...option) (*Service, error) {
	var o buildOpts
	for _, opt := range opts {
		opt(&o)
	}

	s := &Service{auth: authSvc}

	if dataDir == "" {
		return s, nil
	}

	up := o.upstream
	if up == nil {
		// A degraded auth service (no credentials store) exposes a nil token
		// source; without one there's nothing to authenticate the api client with,
		// so the settings subsystem stays degraded.
		if cloudURL == "" || authSvc == nil || authSvc.TokenSource(context.Background()) == nil {
			return s, nil // no upstream available ⇒ degraded
		}
		up = apiUpstream{c: api.New(cloudURL, authSvc.TokenSource)}
	}

	prefsStore, err := prefs.NewStore(filepath.Join(dataDir, "settings.json"))
	if err != nil {
		return nil, err
	}
	queue, err := mutationqueue.New(filepath.Join(dataDir, "settings-queue.json"))
	if err != nil {
		return nil, err
	}

	s.prefs = prefsStore
	s.engine = prefsync.New(up, prefsStore, queue, pokeSvc)
	return s, nil
}

// Prefs returns the settings store, or nil when the service is degraded.
func (s *Service) Prefs() *prefs.Store { return s.prefs }

// Start launches the settings-sync engine bound to a context derived from ctx,
// plus a goroutine that wakes the engine on every auth session change. No-op
// when degraded or already started — a second call must not leak a second engine
// goroutine that Close wouldn't wait on.
func (s *Service) Start(ctx context.Context) {
	if s.engine == nil || s.started {
		return
	}
	s.started = true
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})

	// Wake the engine on every auth sign-in / sign-out. This is the inverse of the
	// old auth→engine poke: cloud (the dependent) observes auth's session state
	// stream, and auth knows nothing of the engine. A sign-in pokes the idle
	// engine so it authenticates and syncs now instead of waiting out its backoff;
	// a sign-out pokes it to cancel the in-flight authenticated watch (which then
	// reconnects as signed-out and idles).
	//
	// The stream is latest-value session State (current-on-subscribe). We track
	// only the Authenticated bit and poke when it flips: the first receive seeds
	// the baseline without poking (the engine authenticates from the token source
	// on its own startup), and a routine token refresh leaves Authenticated
	// unchanged, so it doesn't wake the engine. Coalescing is harmless here — what
	// matters is the resulting sign-in state, not the path taken to it.
	if s.auth != nil {
		states, cancelSub := s.auth.Subscribe()
		s.pokeDone = make(chan struct{})
		go func() {
			defer close(s.pokeDone)
			defer cancelSub()
			authed, seeded := false, false
			for {
				select {
				case <-runCtx.Done():
					return
				case st, ok := <-states:
					if !ok {
						return
					}
					if !seeded {
						authed, seeded = st.Authenticated, true
						continue
					}
					if st.Authenticated != authed {
						authed = st.Authenticated
						s.engine.Poke()
					}
				}
			}
		}()
	}

	go func() {
		defer close(s.done)
		s.engine.Run(runCtx)
	}()
}

// Close stops the sync engine (and the session-poke goroutine) and waits for
// them to unwind. Safe to call without Start and on a degraded service.
func (s *Service) Close() error {
	if s.cancel != nil {
		s.cancel()
		<-s.done
		if s.pokeDone != nil {
			<-s.pokeDone
		}
	}
	return nil
}

// apiUpstream adapts the cloud api client to the prefsync.Upstream interface.
type apiUpstream struct {
	c *api.Client
}

func (a apiUpstream) Snapshot(ctx context.Context) (prefs.Settings, error) {
	return a.c.GetSettings(ctx)
}

func (a apiUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	return a.c.WatchSettings(ctx)
}

func (a apiUpstream) Update(ctx context.Context, patch prefs.Settings) (prefs.Settings, error) {
	return a.c.UpdateSettings(ctx, patch)
}
