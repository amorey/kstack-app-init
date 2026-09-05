package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// memCredStore is an in-memory CredentialsStore fake standing in for the host
// keychain.
type memCredStore struct {
	mu      sync.Mutex
	tok     Token
	loadErr error // when set, Load fails (e.g. an unreachable OS keyring backend)
	saveErr error // when set, Save fails without mutating tok
}

func (m *memCredStore) Load() (Token, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadErr != nil {
		return Token{}, m.loadErr
	}
	return m.tok, nil
}

func (m *memCredStore) Save(t Token) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.tok = t
	return nil
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// idToken builds a (signature-less) JWT carrying the given claims, for
// exercising the unverified restore-identity decode.
func idToken(t *testing.T, claims map[string]string) string {
	t.Helper()
	b, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	return "header." + base64.RawURLEncoding.EncodeToString(b) + ".sig"
}

func noRefresh(context.Context, string) (Token, error) {
	return Token{}, errors.New("refresh should not be called")
}

// recvState blocks for the next published State, failing on timeout.
func recvState(t *testing.T, ch <-chan State) State {
	t.Helper()
	return testutil.Recv(t, ch, "a published State")
}

// --- token / refresh behavior ---

// validToken returns the cached access token while it is still valid, without
// calling refresh.
func TestValidTokenReturnsCachedWhenValid(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "cached",
		RefreshToken: "refresh",
		Expiry:       epoch.Add(time.Hour),
	}}
	var refreshes int32
	m := newGrant(store, func(context.Context, string) (Token, error) {
		atomic.AddInt32(&refreshes, 1)
		return Token{}, nil
	}, withNow(func() time.Time { return epoch }))

	got, err := m.validToken(context.Background())
	if err != nil {
		t.Fatalf("validToken: %v", err)
	}
	if got.AccessToken != "cached" {
		t.Fatalf("want cached, got %q", got.AccessToken)
	}
	if n := atomic.LoadInt32(&refreshes); n != 0 {
		t.Fatalf("want 0 refreshes, got %d", n)
	}
}

// When the cached token is expired, validToken refreshes and returns the fresh
// access token.
func TestValidTokenRefreshesWhenExpired(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "stale",
		RefreshToken: "refresh",
		Expiry:       epoch.Add(-time.Minute), // already expired
	}}
	m := newGrant(store, func(_ context.Context, rt string) (Token, error) {
		if rt != "refresh" {
			t.Errorf("refresh called with %q, want refresh", rt)
		}
		return Token{AccessToken: "fresh", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	got, err := m.validToken(context.Background())
	if err != nil {
		t.Fatalf("validToken: %v", err)
	}
	if got.AccessToken != "fresh" {
		t.Fatalf("want fresh, got %q", got.AccessToken)
	}
}

// After a refresh, the new Token is persisted via the store so it survives a
// restart.
func TestValidTokenPersistsAfterRefresh(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "stale",
		RefreshToken: "refresh",
		Expiry:       epoch.Add(-time.Minute),
	}}
	m := newGrant(store, func(context.Context, string) (Token, error) {
		return Token{AccessToken: "fresh", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	if _, err := m.validToken(context.Background()); err != nil {
		t.Fatalf("validToken: %v", err)
	}
	saved, _ := store.Load()
	if saved.AccessToken != "fresh" {
		t.Fatalf("store not updated: %+v", saved)
	}
}

// An empty/cleared store has no refresh token, so validToken fails locally as
// ErrSignedOut instead of calling refresh with an empty token.
func TestValidTokenSignedOutWhenNoRefreshToken(t *testing.T) {
	store := &memCredStore{} // zero token: no access token, no refresh token
	var refreshes int32
	m := newGrant(store, func(context.Context, string) (Token, error) {
		atomic.AddInt32(&refreshes, 1)
		return Token{}, nil
	}, withNow(func() time.Time { return epoch }))

	_, err := m.validToken(context.Background())
	if err != ErrSignedOut {
		t.Fatalf("want ErrSignedOut, got %v", err)
	}
	if n := atomic.LoadInt32(&refreshes); n != 0 {
		t.Fatalf("want 0 refreshes, got %d", n)
	}
}

// An unreadable credentials store (e.g. a headless Linux box with no Secret
// Service backend, where keyring.Get fails with "org.freedesktop.secrets was
// not provided") degrades to signed-out: state() answers signed-out without
// error and validToken returns ErrSignedOut, rather than propagating the
// keyring error out through Current/authState. Regression test for the CI
// failure where a store read error broke the whole authState query.
func TestStoreLoadErrorDegradesSignedOut(t *testing.T) {
	store := &memCredStore{loadErr: errors.New("org.freedesktop.secrets was not provided by any .service files")}
	m := newGrant(store, noRefresh, withNow(func() time.Time { return epoch }))

	st, err := m.state(context.Background())
	if err != nil {
		t.Fatalf("state should degrade to signed-out, got err %v", err)
	}
	if st.Authenticated || st.Identity != nil {
		t.Fatalf("want signed-out state, got %+v", st)
	}
	if _, err := m.validToken(context.Background()); err != ErrSignedOut {
		t.Fatalf("want ErrSignedOut, got %v", err)
	}
}

// When the post-refresh Save fails, validToken reports the error and does not
// cache the unpersisted token — a later call must not serve it as valid.
func TestValidTokenNotCachedWhenSaveFails(t *testing.T) {
	store := &memCredStore{
		tok:     Token{AccessToken: "stale", RefreshToken: "refresh", Expiry: epoch.Add(-time.Minute)},
		saveErr: errors.New("keychain unavailable"),
	}
	var refreshes int32
	m := newGrant(store, func(context.Context, string) (Token, error) {
		atomic.AddInt32(&refreshes, 1)
		return Token{AccessToken: "fresh", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	if _, err := m.validToken(context.Background()); err == nil {
		t.Fatal("want error when Save fails, got nil")
	}
	// A second call must still fail (and refresh again) rather than serving the
	// never-persisted "fresh" token from cache.
	if got, err := m.validToken(context.Background()); err == nil {
		t.Fatalf("want error on second call, got token %q", got.AccessToken)
	}
	if n := atomic.LoadInt32(&refreshes); n != 2 {
		t.Fatalf("want 2 refresh attempts (token never cached), got %d", n)
	}
}

// When set's Save fails, the token must not be cached: a later validToken call
// must not serve the unpersisted credential (which would look signed-in).
func TestSetNotCachedWhenSaveFails(t *testing.T) {
	store := &memCredStore{saveErr: errors.New("keychain unavailable")}
	m := newGrant(store, func(context.Context, string) (Token, error) {
		return Token{}, errors.New("refresh should not be called")
	}, withNow(func() time.Time { return epoch }))

	tok := Token{AccessToken: "fresh", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour)}
	if err := m.set(tok, Identity{Email: "a@x.com"}); err == nil {
		t.Fatal("want error when Save fails, got nil")
	}
	// The store is empty (Save failed), so validToken loads nothing and reports
	// signed-out rather than serving the never-persisted token from memory.
	if got, err := m.validToken(context.Background()); err != ErrSignedOut {
		t.Fatalf("want ErrSignedOut (token not cached), got token %q err %v", got.AccessToken, err)
	}
	// And the State must remain signed-out.
	if st, _ := m.state(context.Background()); st.Authenticated {
		t.Fatal("State signed-in after a failed set")
	}
}

// clear erases the persisted token and leaves the grant signed out: a later
// validToken call returns ErrSignedOut, the store is empty, and the State is
// signed-out.
func TestClearSignsOut(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken: "cached", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour),
	}}
	m := newGrant(store, func(context.Context, string) (Token, error) {
		return Token{}, errors.New("should not refresh after clear")
	}, withNow(func() time.Time { return epoch }))

	if err := m.clear(); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if saved, _ := store.Load(); saved.RefreshToken != "" || saved.AccessToken != "" {
		t.Fatalf("store not cleared: %+v", saved)
	}
	if _, err := m.validToken(context.Background()); err != ErrSignedOut {
		t.Fatalf("want ErrSignedOut after clear, got %v", err)
	}
	if st, _ := m.state(context.Background()); st.Authenticated {
		t.Fatal("State signed-in after clear")
	}
}

// N concurrent validToken calls on an expired token trigger exactly one refresh
// (single-flight via the mutex).
func TestConcurrentValidTokenSingleRefresh(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "stale",
		RefreshToken: "refresh",
		Expiry:       epoch.Add(-time.Minute),
	}}
	var refreshes int32
	m := newGrant(store, func(context.Context, string) (Token, error) {
		atomic.AddInt32(&refreshes, 1)
		return Token{AccessToken: "fresh", RefreshToken: "refresh", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.validToken(context.Background()); err != nil {
				t.Errorf("validToken: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&refreshes); got != 1 {
		t.Fatalf("want exactly 1 refresh, got %d", got)
	}
}

// --- state + event behavior ---

// set publishes the signed-in State; clear publishes the signed-out State. The
// stream is current-on-subscribe, so the first receive is the initial signed-out
// snapshot, then each transition's resulting State.
func TestSetAndClearPublishState(t *testing.T) {
	m := newGrant(&memCredStore{}, noRefresh)
	ch, cancel := m.subscribe()
	defer cancel()

	// Current-on-subscribe: the initial snapshot is signed-out.
	if st := recvState(t, ch); st.Authenticated {
		t.Fatalf("initial snapshot should be signed out: %+v", st)
	}

	tok := Token{AccessToken: "a", RefreshToken: "r", Expiry: epoch.Add(time.Hour)}
	if err := m.set(tok, Identity{UserID: "user-1", Email: "a@example.com", Name: "Ada"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	st := recvState(t, ch)
	if !st.Authenticated || st.Identity == nil || st.Identity.Email != "a@example.com" {
		t.Fatalf("after set: %+v", st)
	}
	if cur, _ := m.state(context.Background()); !cur.Authenticated || cur.Identity == nil || cur.Identity.UserID != "user-1" {
		t.Fatalf("state after set: %+v", cur)
	}

	if err := m.clear(); err != nil {
		t.Fatalf("clear: %v", err)
	}
	st = recvState(t, ch)
	if st.Authenticated || st.Identity != nil {
		t.Fatalf("after clear: %+v", st)
	}
}

// On construction, a persisted token restores a signed-in State with the
// identity decoded from the stored ID token (no network).
func TestNewRestoresSignedInState(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "a",
		RefreshToken: "r",
		IDToken:      idToken(t, map[string]string{"sub": "u1", "email": "a@x.com", "name": "Ada"}),
		Expiry:       epoch.Add(time.Hour),
	}}
	m := newGrant(store, noRefresh, withNow(func() time.Time { return epoch }))

	st, _ := m.state(context.Background())
	if !st.Authenticated {
		t.Fatal("expected restored State to be signed in")
	}
	if st.Identity == nil || st.Identity.UserID != "u1" || st.Identity.Email != "a@x.com" {
		t.Fatalf("restored identity: %+v", st.Identity)
	}
}

// A routine refresh keeps the session signed in (identity unchanged) but
// republishes State carrying the renewed token. Consumers that key off the
// Authenticated bit (the settings-sync poke) see no transition, since it stays
// true across the refresh.
func TestRefreshRepublishesState(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "stale",
		RefreshToken: "r",
		IDToken:      idToken(t, map[string]string{"sub": "u1", "email": "a@x.com"}),
		Expiry:       epoch.Add(-time.Minute), // expired ⇒ next validToken refreshes
	}}
	m := newGrant(store, func(context.Context, string) (Token, error) {
		// Fresh access token, same refresh; identity is unchanged.
		return Token{AccessToken: "fresh", RefreshToken: "r", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	ch, cancel := m.subscribe()
	defer cancel()

	// Current-on-subscribe: initial snapshot is the signed-in, pre-refresh state.
	if st := recvState(t, ch); !st.Authenticated || st.Tokens == nil || st.Tokens.AccessToken != "stale" {
		t.Fatalf("initial snapshot: %+v", st)
	}

	if _, err := m.validToken(context.Background()); err != nil {
		t.Fatalf("validToken: %v", err)
	}

	st := recvState(t, ch)
	if !st.Authenticated || st.Identity == nil || st.Identity.UserID != "u1" {
		t.Fatalf("refresh state: %+v", st)
	}
	if st.Tokens == nil || st.Tokens.AccessToken != "fresh" {
		t.Fatalf("refresh should republish the renewed token: %+v", st.Tokens)
	}
}

// The oauth2.TokenSource adapter vends the cached access token (with the refresh
// token + expiry riding along) and refreshes lazily when the cached one expires.
func TestGrantTokenSource(t *testing.T) {
	store := &memCredStore{tok: Token{
		AccessToken:  "stale",
		RefreshToken: "r",
		Expiry:       epoch.Add(-time.Minute), // expired ⇒ Token() refreshes
	}}
	g := newGrant(store, func(context.Context, string) (Token, error) {
		return Token{AccessToken: "fresh", RefreshToken: "r", Expiry: epoch.Add(time.Hour)}, nil
	}, withNow(func() time.Time { return epoch }))

	ts := grantTokenSource{ctx: context.Background(), grant: g}
	tok, err := ts.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok.AccessToken != "fresh" {
		t.Fatalf("access token = %q, want fresh", tok.AccessToken)
	}
	if tok.RefreshToken != "r" {
		t.Fatalf("refresh token = %q, want r", tok.RefreshToken)
	}
	if !tok.Expiry.Equal(epoch.Add(time.Hour)) {
		t.Fatalf("expiry = %v", tok.Expiry)
	}
}

// A signed-out grant yields ErrSignedOut from the adapter rather than minting
// a token from an empty refresh token.
func TestGrantTokenSourceSignedOut(t *testing.T) {
	g := newGrant(&memCredStore{}, noRefresh, withNow(func() time.Time { return epoch }))
	ts := grantTokenSource{ctx: context.Background(), grant: g}
	if _, err := ts.Token(); err != ErrSignedOut {
		t.Fatalf("want ErrSignedOut, got %v", err)
	}
}

// A refresh that fails leaves the cached token alone: validToken reports the failure
// rather than serving the expired token it was asked to replace.
func TestGrantValidTokenSurfacesARefreshFailure(t *testing.T) {
	boom := errors.New("refresh rejected")
	g := newGrant(
		&memStore{tok: Token{RefreshToken: "r", Expiry: time.Now().Add(-time.Hour)}},
		func(context.Context, string) (Token, error) { return Token{}, boom },
	)

	if _, err := g.validToken(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("validToken err = %v, want %v", err, boom)
	}
}

// A grant built without a store is permanently signed out, so set has nowhere to persist
// and must say so rather than caching a credential a restart would not find.
func TestGrantSetRefusesWithoutAStore(t *testing.T) {
	g := newGrant(nil, nil)

	if err := g.set(Token{RefreshToken: "r"}, Identity{}); !errors.Is(err, ErrSignedOut) {
		t.Fatalf("set err = %v, want ErrSignedOut", err)
	}
	st, _ := g.state(context.Background())
	if st.Authenticated {
		t.Fatal("a storeless grant must stay signed out")
	}
}

// The sentinel for this subsystem: a credential-store failure is logged, and an OS keyring
// error commonly quotes the request that failed. What reaches the record must carry no part
// of it that could be a credential — the host forwards every sidecar line into its own log
// sink, so the rendering has to happen here.
func TestStoreLoadErrorIsLoggedWithoutTheCredential(t *testing.T) {
	logs := testutil.CaptureLogs(t)
	store := &memCredStore{loadErr: errors.New(
		`keyring: Get "https://vault.example/v1/creds?token=SEKRIT": 403 Forbidden`)}
	m := newGrant(store, noRefresh, withNow(func() time.Time { return epoch }))

	if _, err := m.state(context.Background()); err != nil {
		t.Fatalf("state should degrade to signed-out, got err %v", err)
	}

	if strings.Contains(logs.String(), "SEKRIT") {
		t.Fatalf("the credential reached the log: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "403 Forbidden") {
		t.Fatalf("the diagnostic did not survive: %s", logs.String())
	}
}
