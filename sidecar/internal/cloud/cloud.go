// Package cloud is the composition + lifecycle owner for cloud-synced settings (prefs,
// mutationqueue, api, prefsync): New wires the pieces, Start launches the background
// sync. Degrades to a no-op without a cloud URL / data dir.
//
// **cloud depends on auth, never the reverse**: it authenticates the api client from
// auth's TokenSource and wakes its engine off auth's session stream.
// See docs/adr/2026-08-09-local-first-auth-settings.md.
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

// option is an unexported build seam for New, so only in-package tests can pass one —
// production callers can't inject a fake upstream. Same pattern as internal/auth.
type option func(*buildOpts)

// buildOpts collects the seam overrides applied before New resolves the upstream.
type buildOpts struct {
	// upstream replaces the api-backed upstream, also bypassing the cloudURL +
	// token-source requirement.
	upstream prefsync.Upstream
}

// withUpstream drives the engine without a real cloud endpoint or auth.
func withUpstream(up prefsync.Upstream) option {
	return func(o *buildOpts) { o.upstream = up }
}

// Service owns the cloud-synced settings subsystem.
type Service struct {
	auth auth.Service // the auth dependency; may be nil/degraded

	prefs  *prefs.Store     // nil when degraded
	engine *prefsync.Engine // nil when degraded

	started bool
}

// New builds the cloud settings service. dataDir holds the settings file + write queue;
// cloudURL is the API base; pokeSvc is the shared resync broadcaster (nil disables
// external wakes). Degrades unless dataDir is set and an upstream is available.
func New(dataDir, cloudURL string, authSvc auth.Service, pokeSvc *poke.Service) (*Service, error) {
	return newWithOptions(dataDir, cloudURL, authSvc, pokeSvc)
}

// newWithOptions is New plus the unexported test seams.
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
		// A degraded auth service exposes a nil token source, leaving nothing to
		// authenticate the api client with.
		if cloudURL == "" || authSvc == nil || authSvc.TokenSource(context.Background()) == nil {
			return s, nil
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

// Start launches the sync engine plus a goroutine waking it on auth session changes. The
// returned stop cancels both and blocks until they unwind, bounded by its own context.
// Returns a no-op stop when degraded or already started, so a second call can't leak a
// second engine goroutine.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if s.engine == nil || s.started {
		return noop, nil
	}
	s.started = true
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	// Wake the engine on sign-in/sign-out: cloud observes auth's stream, never the
	// reverse. Only the Authenticated BIT is tracked — the first receive seeds the
	// baseline without poking, and a routine token refresh is a non-event.
	var pokeDone chan struct{}
	if s.auth != nil {
		states, cancelSub := s.auth.Subscribe()
		pokeDone = make(chan struct{})
		go func() {
			defer close(pokeDone)
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
		defer close(done)
		s.engine.Run(runCtx)
	}()

	stop := func(ctx context.Context) error {
		cancel()
		for _, ch := range []chan struct{}{done, pokeDone} {
			if ch == nil {
				continue
			}
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	return stop, nil
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
