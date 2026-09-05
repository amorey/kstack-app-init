package auth

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth/oauth"
)

// ErrSignedOut means there's no usable credential — nothing to refresh, so callers fail
// locally rather than retrying.
var ErrSignedOut = errors.New("auth: signed out")

// CredentialsStore persists the Token (keyringStore in production, a fake in tests). Load
// on an empty store returns the zero Token, not an error.
type CredentialsStore interface {
	Load() (Token, error)
	Save(Token) error
}

// RefreshFunc exchanges a (static) refresh token for a fresh Token — the oauth client's
// Refresh in production.
type RefreshFunc func(ctx context.Context, refreshToken string) (Token, error)

// grantOption configures a grant.
type grantOption func(*grant)

// withNow injects the clock (test seam).
func withNow(now func() time.Time) grantOption {
	return func(s *grant) { s.now = now }
}

// grant is the OAuth grant aggregate: it owns the stored token set plus cached identity,
// vends a valid access token (refreshing lazily), and publishes session State to a
// latest-value hub. Authenticated and Identity are DERIVED from the token set, never
// stored alongside, so credential state and published State can't drift.
type grant struct {
	store   CredentialsStore
	refresh RefreshFunc
	now     func() time.Time

	hub *watch.Hub[State]
	tx  *watch.Sender[State]

	mu       sync.Mutex
	tok      Token
	identity Identity
	loaded   bool
}

// newGrant builds the aggregate and RESTORES persisted state: a stored refresh token
// starts the grant signed in, identity decoded (unverified) from the stored ID token. A
// nil store yields a degraded grant (signed-out, validToken errors, clear a no-op).
func newGrant(store CredentialsStore, refresh RefreshFunc, opts ...grantOption) *grant {
	s := &grant{store: store, refresh: refresh, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	// ensureLoaded degrades an unreadable store (a headless box with no keyring) to
	// signed-out rather than failing every later state read.
	s.ensureLoaded()
	// Seed the hub so current-on-subscribe hands the live session, not a zero value.
	s.hub = watch.New(s.stateLocked())
	s.tx = s.hub.Sender()
	return s
}

// subscribe streams session State (current-on-subscribe, then changes) plus a cancel
// func. Latest-value: a slow consumer catches up to the newest State, which is all
// consumers care about here.
func (s *grant) subscribe() (<-chan State, func()) {
	rx := s.hub.Receiver()
	return rx.Chan(), rx.Close
}

// state returns the current State without forcing a refresh, loading the store on first
// use.
func (s *grant) state(context.Context) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoaded()
	return s.stateLocked(), nil
}

// stateLocked computes the current State (s.mu held); Identity and Tokens stay nil while
// signed out, so no consumer reads stale material.
func (s *grant) stateLocked() State {
	st := State{Authenticated: s.tok.RefreshToken != ""}
	if st.Authenticated {
		id := s.identity
		st.Identity = &id
		st.Tokens = &TokenSet{
			AccessToken:  s.tok.AccessToken,
			RefreshToken: s.tok.RefreshToken,
			Expiry:       s.tok.Expiry,
		}
	}
	return st
}

// validToken returns a valid token, lazily refreshing an expired one. The mutex
// serializes callers, so a burst against an expired token triggers exactly one refresh.
func (s *grant) validToken(ctx context.Context) (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureLoaded()
	if s.tok.Valid(s.now()) {
		return s.tok, nil
	}
	// No refresh token means signed out — fail locally rather than hammer the endpoint.
	if s.tok.RefreshToken == "" {
		return Token{}, ErrSignedOut
	}

	tok, err := s.refresh(ctx, s.tok.RefreshToken)
	if err != nil {
		return Token{}, err
	}
	// Persist before caching: a write failure must not leave a fresh-but-unpersisted
	// token in memory that later calls serve as valid.
	if err := s.store.Save(tok); err != nil {
		return Token{}, err
	}
	s.tok = tok
	s.loaded = true
	// A refresh changes only the Tokens projection, so consumers keying off the sign-in
	// bit (the settings-sync poke) see no transition.
	s.publishLocked()
	return s.tok, nil
}

// current returns the cached token as-is, for callers needing the stored token itself
// (e.g. revoking on sign-out).
func (s *grant) current() (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoaded()
	return s.tok, nil
}

// set records a freshly-minted token + verified identity, persisting BEFORE caching so a
// store failure leaves the grant signed-out rather than reporting a credential that never
// landed.
func (s *grant) set(tok Token, id Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store == nil {
		return ErrSignedOut
	}
	if err := s.store.Save(tok); err != nil {
		return err
	}
	s.tok = tok
	s.identity = id
	s.loaded = true
	s.publishLocked()
	return nil
}

// clear erases durable storage BEFORE dropping the cache, so a failed write leaves the
// process still signed in and agreeing with what a restart would find.
func (s *grant) clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store == nil {
		return nil
	}
	if err := s.store.Save(Token{}); err != nil {
		return err
	}
	s.tok = Token{}
	s.identity = Identity{}
	s.loaded = true
	s.publishLocked()
	return nil
}

// publishLocked publishes the current State (s.mu held); Send never blocks.
func (s *grant) publishLocked() {
	s.tx.Send(s.stateLocked()) //nolint:errcheck // Send never blocks; closed hub is a no-op
}

// ensureLoaded loads the token into the cache on first use (s.mu held). It cannot fail:
// an unreadable store degrades to signed-out, so no caller has a load error to handle.
func (s *grant) ensureLoaded() {
	if s.loaded {
		return
	}
	if s.store == nil {
		s.loaded = true
		return
	}
	tok, err := s.store.Load()
	if err != nil {
		// An unreadable keyring degrades to signed-out (the user can sign in again)
		// rather than failing every state read. Mark loaded so we don't re-hit it.
		slog.Warn("auth: could not read stored credentials; treating as signed out", "err", err)
		s.loaded = true
		return
	}
	s.applyToken(tok)
}

// applyToken caches a loaded token and decodes its identity. DisplayOnly is the one place an
// unchecked claim becomes an Identity: s.identity is only ever published on State, which no
// caller authorizes from.
func (s *grant) applyToken(tok Token) {
	s.tok = tok
	s.loaded = true
	if tok.RefreshToken != "" {
		claims, _ := oauth.ParseIdentityUnverified(tok.IDToken)
		s.identity = claims.DisplayOnly()
	}
}

// grantTokenSource adapts the grant to oauth2.TokenSource. ctx is captured at
// construction, matching x/oauth2's model; the api client builds one per request, so the
// per-request timeout still governs a refresh.
type grantTokenSource struct {
	ctx   context.Context
	grant *grant
}

// Token returns a valid oauth2 token, lazily refreshing. The refresh token rides along
// (Hydra's is static); bearer-only callers read AccessToken.
func (t grantTokenSource) Token() (*oauth2.Token, error) {
	tok, err := t.grant.validToken(t.ctx)
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		Expiry:       tok.Expiry,
	}, nil
}
