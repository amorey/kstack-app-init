// Package auth is the local-first identity subsystem: signed-in session state,
// locally-persisted credentials, and the sidecar-owned browser sign-in/out flow. A user
// stays signed in offline (a present refresh token marks "signed in"). **internal/cloud
// depends on this package, never the reverse.**
//
// One flat package by file: this one (Service, State/TokenSet, Config, composition
// owner), grant.go (the credential state machine + oauth2.TokenSource adapter), login.go
// (the interactive flow + loopback listener), keyring.go (OS-keyring persistence). The
// one sub-package is auth/oauth, the OAuth2/OIDC protocol layer, whose Token/Identity are
// re-exported below. Degrades to permanently signed-out with no credentials store.
// See docs/adr/2026-08-09-local-first-auth-settings.md.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth/oauth"
)

// loginTimeout bounds the async tail of a browser sign-in (the post-browser
// wait/exchange/verify) so an abandoned flow can't keep the loopback listener alive. The
// synchronous setup uses the caller's context.
const loginTimeout = 5 * time.Minute

// State is the public snapshot of the auth session. Authenticated is DERIVED from the
// presence of a refresh token, so credential state and published State can't drift.
// Identity and Tokens are nil while signed out.
type State struct {
	Authenticated bool
	Identity      *Identity
	Tokens        *TokenSet
}

// Token (stored credential) and Identity (verified ID-token claims) are TRUE aliases of
// auth/oauth's types — the leaf owns them, and aliasing avoids the import cycle while
// letting consumers say auth.Token / auth.Identity.
type (
	Token    = oauth.Token
	Identity = oauth.Identity
)

// TokenSet is the stored Token minus the (display-only, internal) id token, surfaced on
// State for callers that authenticate from it.
type TokenSet struct {
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

// Service is the auth subsystem's public surface, passed as an interface to downstream
// consumers. Subscribe is latest-value, current-on-subscribe.
type Service interface {
	Current(ctx context.Context) (State, error)
	StartLogin(ctx context.Context) error
	Logout(ctx context.Context) error
	TokenSource(ctx context.Context) oauth2.TokenSource
	Subscribe() (<-chan State, func())
}

// revokeTimeout bounds Logout's fire-and-forget revocation goroutine.
const revokeTimeout = 10 * time.Second

// Config carries production knobs ONLY — nothing here bypasses the real OAuth flow. Test
// seams are unexported options (see option), reachable only from in-package tests.
type Config struct {
	// IssuerURL is the Hydra issuer base; every endpoint and the expected ID-token
	// "iss" derive from it via newHydraOAuthConfig.
	IssuerURL string
	// ClientID is the public (PKCE/loopback) OAuth client id.
	ClientID string
	// Scopes optionally overrides defaultScopes.
	Scopes []string
	// KeychainService is the OS-keyring service name credentials persist under. Empty
	// (and no withCredentialsStore) ⇒ degraded.
	KeychainService string
}

// option is an unexported build seam for New, so only in-package tests can pass one.
type option func(*buildOpts)

// buildOpts collects the seam overrides applied before New resolves the store and oauth
// config.
type buildOpts struct {
	store       CredentialsStore
	oauthConfig *oauth.Config // nil ⇒ derive from Config via newHydraOAuthConfig
	start       loginStarter  // overrides the whole login step (flow seams tested against the flow itself)
}

// withCredentialsStore injects an in-memory store instead of the OS keyring, taking
// precedence over Config.KeychainService.
func withCredentialsStore(s CredentialsStore) option {
	return func(o *buildOpts) { o.store = s }
}

// withOAuthConfig points the endpoint URLs at httptest servers.
func withOAuthConfig(c oauth.Config) option {
	return func(o *buildOpts) { o.oauthConfig = &c }
}

// withLoginStarter replaces the login step, so a service-level test exercises
// setup → async persist → sign-in without the loginFlow's own seams (see login_test.go).
func withLoginStarter(fn loginStarter) option {
	return func(o *buildOpts) { o.start = fn }
}

// service is the auth composition owner: it holds the grant aggregate and the OAuth
// client and runs login/logout over them. No long-lived goroutines (restore happens in
// newGrant, revocation is per-Logout fire-and-forget), so no Start/Close.
type service struct {
	grant *grant        // always non-nil (signed-out when degraded)
	oauth *oauth.Client // nil when degraded

	// configured is false when no credentials store was wired: the grant can't persist,
	// so TokenSource returns nil and Login errors.
	configured bool

	// start is the browser login step, nil when degraded so Login errors instead of
	// half-running. Production: (*loginFlow).start.
	start loginStarter
}

// New builds the auth service from production configuration; an unconfigured Config
// yields a degraded Service (signed out, no token source, login errors, logout a no-op).
// newGrant restores signed-in state from persisted credentials, so a restart isn't
// signed-out while a consumer authenticates from the stored token.
func New(cfg Config) (Service, error) {
	return newWithOptions(cfg)
}

// newWithOptions is New plus the unexported test seams, which resolve here because they
// determine the store and oauth client the grant is built over.
func newWithOptions(cfg Config, opts ...option) (Service, error) {
	var o buildOpts
	for _, opt := range opts {
		opt(&o)
	}

	// An injected store wins, else a keyringStore; neither ⇒ degraded.
	store := o.store
	if store == nil && cfg.KeychainService != "" {
		store = newKeyringStore(cfg.KeychainService)
	}
	if store == nil {
		// A grant with no store is permanently signed-out.
		return &service{grant: newGrant(nil, nil)}, nil
	}

	oauthCfg := newHydraOAuthConfig(cfg.IssuerURL, cfg.ClientID, cfg.Scopes...)
	if o.oauthConfig != nil {
		oauthCfg = *o.oauthConfig
	}

	oc := oauth.NewClient(oauthCfg)

	start := (&loginFlow{oauth: oc, newLoopback: defaultNewLoopback, openBrowser: defaultOpenBrowser}).start
	if o.start != nil {
		start = o.start
	}

	return &service{
		grant:      newGrant(store, oc.Refresh),
		oauth:      oc,
		configured: true,
		start:      start,
	}, nil
}

// defaultScopes: openid (ID token), offline_access (refresh token), email + profile
// (identity claims).
var defaultScopes = []string{"openid", "offline_access", "email", "profile"}

// newHydraOAuthConfig derives Hydra's endpoint layout from the issuer base (auth/oauth
// itself stays Hydra-agnostic). The trailing slash is trimmed before paths are appended
// but KEPT on the expected "iss": Hydra's ID tokens carry it, so dropping it fails
// verification with a spurious provider mismatch.
func newHydraOAuthConfig(issuer, clientID string, scopes ...string) oauth.Config {
	base := strings.TrimRight(issuer, "/")
	if len(scopes) == 0 {
		scopes = defaultScopes
	}
	return oauth.Config{
		ClientID:      clientID,
		AuthURL:       base + "/oauth2/auth",
		TokenURL:      base + "/oauth2/token",
		JWKSURL:       base + "/.well-known/jwks.json",
		RevocationURL: base + "/oauth2/revoke",
		Issuer:        base + "/",
		Scopes:        scopes,
	}
}

// Current returns the session State without forcing a refresh.
func (s *service) Current(ctx context.Context) (State, error) {
	return s.grant.state(ctx)
}

// Subscribe streams session State (current-on-subscribe, then changes) plus a cancel
// func. Usable even when degraded, where it delivers the permanent signed-out State.
func (s *service) Subscribe() (<-chan State, func()) {
	return s.grant.subscribe()
}

// TokenSource returns the oauth2.TokenSource consumers authenticate from, capturing ctx
// for the lazy refresh exchange. Nil when degraded.
func (s *service) TokenSource(ctx context.Context) oauth2.TokenSource {
	if !s.configured {
		return nil
	}
	return grantTokenSource{ctx: ctx, grant: s.grant}
}

// StartLogin runs the desktop OAuth flow in two phases: the synchronous setup (loopback
// listener + browser open) under the caller's ctx, returning its failures to the
// mutation; then the slow round-trip (redirect, exchange, verify, persist) in a bounded
// detached goroutine, observed via Subscribe. A tail failure logs and stays signed-out.
// Errors rather than half-running when degraded.
func (s *service) StartLogin(ctx context.Context) error {
	if !s.configured || s.start == nil {
		return fmt.Errorf("auth: login unavailable (service not configured)")
	}

	finish, err := s.start(ctx)
	if err != nil {
		return err
	}
	gen := s.grant.supersede()

	// The grant persists BEFORE broadcasting signed-in, so a keychain write failure
	// leaves us signed-out rather than reporting a credential that never landed.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
		defer cancel()

		tok, id, err := finish(ctx)
		if err != nil {
			// A timeout means an abandoned browser flow — expected, not a fault.
			if errors.Is(err, context.DeadlineExceeded) {
				slog.Warn("cloud sign-in timed out", "err", err)
			} else {
				slog.Error("cloud sign-in failed", "err", err)
			}
			return
		}
		ok, err := s.grant.setIfGen(gen, tok, id)
		if err != nil {
			slog.Error("cloud sign-in: persist failed", "err", err)
			return
		}
		if !ok {
			// The credential is live at the issuer even though nothing here will use it,
			// so hand it back rather than leave it standing until it expires.
			slog.Info("cloud sign-in completed against a session that has since ended; discarding it")
			s.revokeAsync(tok.RefreshToken)
		}
	}()

	return nil
}

// Logout clears the local session first, then revokes server-side best-effort: a user
// signing out must not wait on an unreachable server while old credentials are live. The
// revoke gets its own bounded context, so a returning mutation can't cancel it.
//
// A failed keychain write returns the error and stays signed IN — reporting signed-out
// while a durable refresh token survives to re-authenticate on restart would be
// inconsistent. internal/cloud observes the session change itself; auth never calls it.
func (s *service) Logout(ctx context.Context) error {
	// Capture the refresh token BEFORE clear erases it.
	var refreshToken string
	if tok, err := s.grant.current(); err == nil {
		refreshToken = tok.RefreshToken
	}
	if err := s.grant.clear(); err != nil {
		return err
	}
	s.revokeAsync(refreshToken)
	return nil
}

// revokeAsync revokes the refresh token off the Logout path, best-effort and bounded by
// its own timeout; a no-op when there's nothing to revoke.
func (s *service) revokeAsync(refreshToken string) {
	if s.oauth == nil || refreshToken == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), revokeTimeout)
		defer cancel()
		_ = s.oauth.Revoke(ctx, refreshToken)
	}()
}
