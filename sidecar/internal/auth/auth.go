// Package auth is the local-first identity subsystem: it owns the signed-in
// session state, the locally-persisted credentials (token store + refresh), and
// the sidecar-owned browser sign-in/out flow. It is deliberately independent of
// the cloud-synced settings feature — a user stays signed in offline (identity
// is read back from a locally-stored ID token, and a present refresh token is
// what marks "signed in"), and the cloud settings-sync subsystem depends on
// this package (for an oauth2.TokenSource and the session change stream), not the
// other way around.
//
// The subsystem is mostly one flat package organized by domain into four files:
// the service facade (this file — the Service interface, the State/Identity/
// TokenSet value types, Config + the composition owner), the OAuth grant
// aggregate (grant.go — the credential state machine, its CredentialsStore/
// RefreshFunc ports, and the oauth2.TokenSource adapter), the interactive login
// flow (login.go — the loginFlow orchestration, its seams, and the 127.0.0.1
// loopback redirect listener), and the OS-keyring credential persistence
// (keyring.go). The one piece carved into its own sub-package is the OAuth2 /
// OIDC protocol layer (auth/oauth) — a deep module (PKCE, JWKS verification,
// revocation, token exchange/refresh) that auth builds a Client from and drives.
// auth re-exports oauth's value types (Token, Identity) via the aliases below so
// callers keep naming them auth.Token / auth.Identity. Like internal/cluster and
// internal/cloud, it is fronted by one composition owner (Service) and degrades
// gracefully when the host hasn't wired a credentials store (then it's signed-out,
// login errors, logout is a no-op, and TokenSource is nil).
package auth

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth/oauth"
)

// loginTimeout bounds the async tail of a browser sign-in so an abandoned flow
// (the user closes the browser without finishing) can't leave the loopback
// listener and tail goroutine alive indefinitely. The synchronous setup phase
// uses the caller's context; only the post-browser wait/exchange/verify runs
// under this bound.
const loginTimeout = 5 * time.Minute

// State is the public snapshot of the auth session. Authenticated is derived
// from the presence of a refresh token (never stored alongside), so the
// credential state and the published State cannot drift. Identity and Tokens are
// nil while signed out (so a consumer can't read stale claims / tokens).
type State struct {
	Authenticated bool
	Identity      *Identity
	Tokens        *TokenSet
}

// Token and Identity are re-exported (true aliases — the identical types, not
// wrappers) from the auth/oauth sub-package, which owns them because the OAuth /
// OIDC protocol layer produces them. The aliases let the rest of auth, and
// external consumers, keep naming them auth.Token / auth.Identity without
// importing the sub-package or any conversion. Token is the user's stored cloud
// credential (access/refresh/id tokens + expiry); Identity is the subset of the
// verified ID-token claims the app cares about.
type (
	Token    = oauth.Token
	Identity = oauth.Identity
)

// TokenSet is the tokenless-of-id-token public projection of the stored Token —
// the access/refresh pair plus expiry, surfaced on State for downstream callers
// that authenticate from it. (The id token stays internal; it's display-only.)
type TokenSet struct {
	AccessToken  string
	RefreshToken string
	Expiry       time.Time
}

// Service is the auth subsystem's public surface, passed (as an interface) to
// downstream consumers like the cloud client. Current reads the session state;
// StartLogin/Logout drive the sidecar-owned browser flow; TokenSource vends an
// oauth2.TokenSource for authenticated cloud calls; Subscribe streams the
// current session State (latest-value, current-on-subscribe).
type Service interface {
	Current(ctx context.Context) (State, error)
	StartLogin(ctx context.Context) error
	Logout(ctx context.Context) error
	TokenSource(ctx context.Context) oauth2.TokenSource
	Subscribe() (<-chan State, func())
}

// revokeTimeout bounds the fire-and-forget token revocation kicked off by
// Logout, so its goroutine can't outlive a stalled revocation endpoint.
const revokeTimeout = 10 * time.Second

// Config is what the composition root (internal/app) hands the auth service. It
// carries only production knobs — a production caller supplies IssuerURL +
// ClientID (+ optional Scopes) and a KeychainService name, and nothing here lets
// it bypass the real OAuth flow. The test seams (a fake oauth flow, an in-memory
// store, httptest endpoints, etc.) are unexported options instead (see option
// below), so they're reachable only from in-package tests and can't leak onto
// this public surface.
type Config struct {
	// IssuerURL is the Hydra OAuth issuer base URL. The auth service derives every
	// endpoint (authorize/token/jwks/revocation) and the expected ID-token "iss"
	// from it via newHydraOAuthConfig, baking in Hydra's standard path layout — so a
	// production caller supplies only IssuerURL + ClientID (+ optional Scopes).
	IssuerURL string
	// ClientID is the public (PKCE/loopback) OAuth client id.
	ClientID string
	// Scopes optionally overrides the default desktop scopes (openid,
	// offline_access, email, profile).
	Scopes []string
	// KeychainService is the OS-keyring service name the auth token is persisted
	// under. When set, the service builds its own keyringStore over it — the caller
	// hands a name, not a store. Empty (and no withCredentialsStore option) ⇒
	// degraded (signed-out, no persistence).
	KeychainService string
}

// option is an unexported build seam for New. Because the type is unexported,
// only in-package (test) code can pass one — production callers configure auth
// through Config alone and cannot inject a fake flow/store/endpoint. The seams
// mirror what the now-white-box tests need: a fake oauth flow, an in-memory
// credentials store, httptest endpoints, a canned loopback, a no-op browser.
type option func(*buildOpts)

// buildOpts collects the seam overrides applied by the options before New
// resolves the store and oauth config.
type buildOpts struct {
	store       CredentialsStore
	oauthConfig *oauth.Config // nil ⇒ derive from Config via newHydraOAuthConfig
	start       loginStarter  // overrides the whole login step (the loginFlow seams are tested against the flow itself)
}

// withCredentialsStore injects a credentials store (in-memory, in tests) instead
// of the OS keyring. Takes precedence over Config.KeychainService.
func withCredentialsStore(s CredentialsStore) option {
	return func(o *buildOpts) { o.store = s }
}

// withOAuthConfig overrides the derived oauth.Config wholesale, pointing the
// endpoint URLs at httptest servers instead of a real Hydra.
func withOAuthConfig(c oauth.Config) option {
	return func(o *buildOpts) { o.oauthConfig = &c }
}

// withLoginStarter overrides the login step wholesale (a fake whose setup phase
// returns a canned pendingLogin tail, or a synchronous setup error), so a
// service-level test exercises Login → setup → async persist → sign-in without
// the loginFlow's loopback/browser/oauth seams — those are tested directly
// against the flow in login_test.go.
func withLoginStarter(fn loginStarter) option {
	return func(o *buildOpts) { o.start = fn }
}

// service is the composition owner for the auth subsystem and the concrete
// Service impl: it owns the OAuth grant aggregate (token set + identity) and the
// OAuth client, and runs the login/logout flow over them. It owns no long-lived
// goroutines (restore happens in newGrant; revocation is a per-Logout
// fire-and-forget), so it needs no Start/Close.
type service struct {
	grant *grant        // always non-nil (signed-out + degraded when unconfigured)
	oauth *oauth.Client // nil when degraded

	// configured is false when no credentials store was wired (degraded): the
	// grant can't persist, so TokenSource returns nil and Login errors.
	configured bool

	// start is the (browser) login step; nil when degraded so Login errors
	// instead of half-running. In production it's (*loginFlow).start — it runs
	// the synchronous setup (loopback bind + browser open) and returns the tail.
	start loginStarter
}

// New builds the auth service from production configuration. With an
// unconfigured Config (no keychain service) it returns a degraded Service:
// signed-out session, no token source, login errors, logout is a no-op.
//
// The grant aggregate restores signed-in state from persisted credentials in
// newGrant (so a restart isn't signed-out while a downstream consumer
// authenticates from the stored token).
func New(cfg Config) (Service, error) {
	return newWithOptions(cfg)
}

// newWithOptions is the build entry point that also accepts the unexported test
// seams. New is the production wrapper (no options); in-package white-box tests
// call this directly to inject a fake store/oauth/flow. The seams resolve here,
// at construction, because they determine the store and oauth client the grant
// is built over — they can't be applied to an already-built service.
func newWithOptions(cfg Config, opts ...option) (Service, error) {
	var o buildOpts
	for _, opt := range opts {
		opt(&o)
	}

	// Resolve the credentials store: an injected one (test seam) wins, else a
	// keyringStore over the keychain service name. Neither ⇒ degraded.
	store := o.store
	if store == nil && cfg.KeychainService != "" {
		store = newKeyringStore(cfg.KeychainService)
	}
	if store == nil {
		// Degraded: a grant with no store is permanently signed-out.
		return &service{grant: newGrant(nil, nil)}, nil
	}

	// Resolve the OAuth config: an injected one (httptest endpoints) wins, else
	// derive Hydra's standard endpoint layout from IssuerURL + ClientID.
	oauthCfg := newHydraOAuthConfig(cfg.IssuerURL, cfg.ClientID, cfg.Scopes...)
	if o.oauthConfig != nil {
		oauthCfg = *o.oauthConfig
	}

	oc := oauth.NewClient(oauthCfg)

	// Login step: the real browser loginFlow (oc drives the protocol) by default,
	// overridable wholesale for service-level tests.
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

// defaultScopes are the scopes the desktop flow needs: openid (ID token),
// offline_access (refresh token), email + profile (identity claims).
var defaultScopes = []string{"openid", "offline_access", "email", "profile"}

// newHydraOAuthConfig builds an OAuthConfig for an Ory Hydra issuer, deriving
// Hydra's standard endpoint paths from the issuer base so the composition root
// supplies only the issuer and client id (and, optionally, overriding scopes).
// It's a caller-side helper for New — oauth.go stays Hydra-agnostic, taking the
// resolved endpoint URLs. The base has any trailing slash trimmed before the
// paths are appended; the expected "iss", however, is the base WITH a trailing
// slash — Hydra issues ID tokens whose iss claim carries it, so verification
// would otherwise fail with a spurious "issued by a different provider" mismatch.
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

// Current returns the current session State (no forced refresh). Identity and
// Tokens are nil while signed out.
func (s *service) Current(ctx context.Context) (State, error) {
	return s.grant.state(ctx)
}

// Subscribe returns a latest-value stream of session State (current-on-subscribe,
// then each change) plus a cancel func. Always usable, including on a degraded
// service (where it just delivers the permanent signed-out State).
func (s *service) Subscribe() (<-chan State, func()) {
	return s.grant.subscribe()
}

// TokenSource returns an oauth2.TokenSource downstream consumers (the cloud api
// client) authenticate from, capturing ctx for the lazy refresh exchange. Nil
// when the service is degraded.
func (s *service) TokenSource(ctx context.Context) oauth2.TokenSource {
	if !s.configured {
		return nil
	}
	return grantTokenSource{ctx: ctx, grant: s.grant}
}

// StartLogin runs the desktop OAuth flow in two phases. The synchronous setup —
// spin up the loopback redirect listener, open the system browser at the PKCE
// authorize URL — runs under the caller's ctx and its failures (loopback bind,
// browser launch) are returned, so the GraphQL login mutation can surface them
// to the UI. The slow browser round-trip — wait for the redirect, exchange +
// verify the code, persist, flip the session to signed-in — runs in a bounded,
// detached goroutine; its completion (or failure) is observed via Subscribe /
// authStateWatch, never blocking the caller. A tail failure is logged and leaves
// the session signed-out (a known v1 limitation). The tail is fire-and-forget,
// like Logout's revoke — not tied to shutdown (a sidecar shutdown is a process
// exit, which reaps it).
//
// On a degraded service (no credentials store / oauth config) it returns an error
// rather than half-running.
func (s *service) StartLogin(ctx context.Context) error {
	if !s.configured || s.start == nil {
		return fmt.Errorf("auth: login unavailable (service not configured)")
	}

	// Synchronous setup: bind loopback + open browser. A failure here is the
	// caller's error.
	finish, err := s.start(ctx)
	if err != nil {
		return err
	}

	// Async tail: wait for the redirect, exchange + verify, then persist. The
	// grant aggregate persists before broadcasting signed-in: a keychain write
	// failure leaves us signed-out rather than reporting signed-in over a
	// credential that never landed.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), loginTimeout)
		defer cancel()

		tok, id, err := finish(ctx)
		if err != nil {
			slog.Error("cloud sign-in failed", "err", err)
			return
		}
		if err := s.grant.set(tok, id); err != nil {
			slog.Error("cloud sign-in: persist failed", "err", err)
		}
	}()

	return nil
}

// Logout tears down the local session first — erase the persisted credentials,
// broadcast signed-out — and only then revokes the token server-side,
// asynchronously and best-effort.
//
// Local teardown comes first on purpose: revocation is a network round-trip, and
// a user signing out must not wait on (nor be blocked by) an unreachable server
// while their old credentials are still live. The refresh token captured before
// clear is handed to a fire-and-forget revoke; revoking it also invalidates the
// grant's access tokens, and a failure is harmless (the token is already gone
// locally and expires server-side on its own). ctx is not threaded into the
// revoke (which runs on its own bounded background context) so a returning
// mutation can't cancel it.
//
// Order within the local teardown matters: if the keychain write fails we return
// the error while staying signed in (no signed-out broadcast) — reporting
// signed-out while the durable refresh token survives (so a restart
// re-authenticates) would be inconsistent. The settings-sync engine observes the
// session change (see internal/cloud) and tears down any in-flight authenticated
// watch itself; auth does not reach into it.
func (s *service) Logout(ctx context.Context) error {
	// Capture the refresh token BEFORE clear erases it, for the revoke below.
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

// revokeAsync asks the cloud to revoke the given refresh token off the Logout
// path, so a slow/unreachable revocation endpoint never blocks sign-out. It is
// best-effort (failures are ignored) and bounded by its own timeout so the
// goroutine can't leak; a no-op when there's nothing to revoke.
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
