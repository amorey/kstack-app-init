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

// ErrSignedOut means there is no usable credential to authenticate with — the
// store is empty or has been cleared, so there's nothing to refresh. Callers
// should treat this as "signed out" and fail locally rather than retrying.
var ErrSignedOut = errors.New("auth: signed out")

// CredentialsStore persists the Token. Implemented by keyringStore over the OS
// keyring in production; an in-memory fake in tests. Load on an empty store
// returns the zero Token (signed-out), not an error.
type CredentialsStore interface {
	Load() (Token, error)
	Save(Token) error
}

// RefreshFunc exchanges a (static) refresh token for a fresh Token. In
// production this is the oauth client's Refresh; tests inject a stub.
type RefreshFunc func(ctx context.Context, refreshToken string) (Token, error)

// grantOption configures a grant.
type grantOption func(*grant)

// withNow injects the clock (test seam). Defaults to time.Now.
func withNow(now func() time.Time) grantOption {
	return func(s *grant) { s.now = now }
}

// grant is the auth subsystem's OAuth grant aggregate. It owns the stored token
// set (the source of truth for "am I signed in") plus the cached identity, vends
// a valid access token (refreshing lazily), and publishes the current session
// State to subscribers via a latest-value watch hub.
//
// Authenticated and Identity are DERIVED from the token set, never stored
// alongside it, so the credential state and the published State cannot drift.
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

// newGrant builds the grant aggregate over a CredentialsStore + refresh func
// and RESTORES persisted state: if the store holds a token with a refresh token,
// the grant starts signed in, with the identity decoded (unverified) from the
// stored ID token — so a restart isn't signed-out while a consumer
// re-authenticates from the stored token.
//
// A nil store yields a degraded grant: signed-out, validToken returns
// ErrSignedOut, set errors, clear is a no-op.
func newGrant(store CredentialsStore, refresh RefreshFunc, opts ...grantOption) *grant {
	s := &grant{store: store, refresh: refresh, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	// Restore persisted state. ensureLoaded degrades a store read failure (e.g.
	// an unavailable OS keyring on a headless Linux box) to signed-out, so a
	// missing keyring backend doesn't break every later state read.
	_ = s.ensureLoaded()
	// Seed the hub with the restored State so a current-on-subscribe delivery
	// hands a new subscriber the live session, not a zero value.
	s.hub = watch.New(s.stateLocked())
	s.tx = s.hub.Sender()
	return s
}

// subscribe returns a latest-value stream of session State plus a cancel func
// that tears the subscription down. The first receive yields the CURRENT State
// (current-on-subscribe), then each subsequent change — so a consumer needs no
// separate state() read to seed itself. Because it is latest-value, a consumer
// that falls behind catches up to the newest State rather than seeing every
// intermediate one (fine here: the only transitions are sign-in/out and the
// occasional token refresh, and consumers care about the resulting state, not
// the path taken).
func (s *grant) subscribe() (<-chan State, func()) {
	rx := s.hub.Receiver()
	return rx.Chan(), rx.Close
}

// state returns the current public State (no forced refresh). It loads the store
// on first use.
func (s *grant) state(context.Context) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return State{}, err
	}
	return s.stateLocked(), nil
}

// stateLocked computes the current State. Caller must hold s.mu. Identity and
// Tokens are nil while signed out so a consumer can't read stale material.
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

// validToken returns a valid token, refreshing if the cached one has expired.
// The mutex serializes concurrent callers, so a burst against an expired token
// triggers exactly one refresh. A successful refresh republishes the State.
func (s *grant) validToken(ctx context.Context) (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureLoaded(); err != nil {
		return Token{}, err
	}
	if s.tok.Valid(s.now()) {
		return s.tok, nil
	}
	// No refresh token (empty/cleared store) means we're signed out — fail
	// locally instead of hammering the refresh endpoint with an empty token.
	if s.tok.RefreshToken == "" {
		return Token{}, ErrSignedOut
	}

	tok, err := s.refresh(ctx, s.tok.RefreshToken)
	if err != nil {
		return Token{}, err
	}
	// Persist before caching: a store write failure must not leave a
	// fresh-but-unpersisted token in memory that later calls serve as valid.
	if err := s.store.Save(tok); err != nil {
		return Token{}, err
	}
	s.tok = tok
	s.loaded = true
	// Identity is unchanged by a refresh, so the published State keeps the same
	// Authenticated bit + Identity; only the Tokens projection changes. Consumers
	// that key off the sign-in bit (the settings-sync poke) see no transition.
	s.publishLocked()
	return s.tok, nil
}

// current returns the cached token as-is (no refresh) — for callers that need
// the stored token itself, e.g. revoking the refresh token on sign-out.
func (s *grant) current() (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return Token{}, err
	}
	return s.tok, nil
}

// set records a freshly-minted token + its verified identity (sign-in) and
// persists it. It persists before caching, so a store failure leaves the grant
// signed-out rather than reporting signed-in over a credential that never
// landed. On success it publishes the new signed-in State.
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

// clear erases the persisted token and publishes the signed-out State. It erases
// durable storage BEFORE dropping the cache: if the store write fails, the cache
// is left intact (still signed in) so the process and a restart agree, instead
// of the process appearing signed out while a restart would re-authenticate from
// the leftover token.
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

// publishLocked publishes the current State to subscribers. Caller must hold
// s.mu. watch.Send never blocks; a slow receiver just catches up to the latest
// State on its next read.
func (s *grant) publishLocked() {
	s.tx.Send(s.stateLocked()) //nolint:errcheck // Send never blocks; closed hub is a no-op
}

// ensureLoaded loads the token from the store into the cache on first use.
// Caller must hold s.mu.
func (s *grant) ensureLoaded() error {
	if s.loaded {
		return nil
	}
	if s.store == nil {
		s.loaded = true
		return nil
	}
	tok, err := s.store.Load()
	if err != nil {
		// A keyring that can't be read (locked, or no Secret Service backend at
		// all) degrades to signed-out rather than failing every state read — the
		// user can sign in again. Mark loaded so we don't re-hit the store on
		// each call. This mirrors the local-first "degrade, never error" contract.
		slog.Warn("auth: could not read stored credentials; treating as signed out", "err", err)
		s.loaded = true
		return nil
	}
	s.applyToken(tok)
	return nil
}

// applyToken caches a token loaded from the store and decodes its display
// identity. Identity is display-only — the cloud re-verifies the access token
// per request, so an unverified decode of our own stored ID token is safe here.
func (s *grant) applyToken(tok Token) {
	s.tok = tok
	s.loaded = true
	if tok.RefreshToken != "" {
		s.identity, _ = oauth.ParseIdentityUnverified(tok.IDToken)
	}
}

// grantTokenSource adapts the grant aggregate to oauth2.TokenSource. The ctx
// is captured at construction (Service.TokenSource(ctx)) — matching x/oauth2's
// model, where the refresh exchange runs under the construction context. The
// cloud api client constructs one per request from the request context, so the
// bounded per-request timeout still governs a refresh.
type grantTokenSource struct {
	ctx   context.Context
	grant *grant
}

// Token returns a valid oauth2 token, lazily refreshing the cached one if it has
// expired. The refresh token rides along (Hydra's is static) so a caller that
// wants the whole grant has it; callers that only need the bearer read
// AccessToken.
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
