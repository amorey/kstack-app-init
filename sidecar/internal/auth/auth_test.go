package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth/oauth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// memStore is an in-memory CredentialsStore (host keychain stand-in).
type memStore struct {
	tok     Token
	saveErr error // when set, Save fails without mutating tok
}

func (m *memStore) Load() (Token, error) { return m.tok, nil }
func (m *memStore) Save(t Token) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.tok = t
	return nil
}

// Login's synchronous setup returns promptly, and the async tail persists the
// minted credentials and flips the session to signed-in — observed via Subscribe.
// The login step is faked to a canned tail so the test focuses on the service's
// setup-then-async-persist + sign-in responsibility (the browser/loopback/oauth
// mechanics are the loginFlow's own concern, see login_test.go).
func TestLoginPersistsAndSignsIn(t *testing.T) {
	store := &memStore{}
	svc, err := newWithOptions(Config{},
		withCredentialsStore(store),
		withLoginStarter(func(context.Context) (pendingLogin, error) {
			return func(context.Context) (Token, Identity, error) {
				return Token{AccessToken: "a", RefreshToken: "r", IDToken: "id", Expiry: time.Now().Add(time.Hour)},
					Identity{UserID: "u1", Email: "a@x.com", Name: "Ada"}, nil
			}, nil
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Subscribe before Login so we don't miss the sign-in publish (latest-value
	// stream: current-on-subscribe, then each change).
	states, unsub := svc.Subscribe()
	defer unsub()

	if err := svc.StartLogin(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}

	// The async tail flips the session to signed-in with the verified identity.
	waitForAuthenticated(t, states)

	// Credentials were persisted.
	if saved, _ := store.Load(); saved.RefreshToken != "r" {
		t.Fatalf("credentials not persisted on sign-in: %+v", saved)
	}
	cur, err := svc.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if !cur.Authenticated || cur.Identity == nil || cur.Identity.UserID != "u1" || cur.Identity.Email != "a@x.com" {
		t.Fatalf("session after sign-in: %+v", cur)
	}
}

// When the synchronous setup fails (loopback bind / browser launch), Login
// surfaces the error to the caller and stays signed out — nothing persisted, no
// signed-in broadcast.
func TestLoginSetupErrorStaysSignedOut(t *testing.T) {
	store := &memStore{}
	svc, err := newWithOptions(Config{},
		withCredentialsStore(store),
		withLoginStarter(func(context.Context) (pendingLogin, error) {
			return nil, errors.New("setup failed")
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.StartLogin(context.Background()); err == nil {
		t.Fatal("want Login to surface the synchronous setup error")
	}
	if saved, _ := store.Load(); saved.RefreshToken != "" {
		t.Fatalf("nothing should be persisted on a failed login: %+v", saved)
	}
	if cur, _ := svc.Current(context.Background()); cur.Authenticated {
		t.Fatal("session must stay signed out after a failed login")
	}
}

// waitForAuthenticated drains the latest-value session stream until it observes
// an Authenticated state, failing on a deadline rather than blocking forever.
func waitForAuthenticated(t *testing.T, states <-chan State) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case st := <-states:
			if st.Authenticated {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the async sign-in to publish an authenticated state")
		}
	}
}

// Login on a degraded (unconfigured) service errors clearly instead of
// panicking — there's no credentials store or oauth client to drive.
func TestLoginDegradedErrors(t *testing.T) {
	svc, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.StartLogin(context.Background()); err == nil {
		t.Fatal("want an error signing in on a degraded service, got nil")
	}
}

// unsignedIDToken builds a (signature-less) JWT carrying the given claims, for
// exercising the unverified startup identity decode.
func unsignedIDToken(t *testing.T, claims map[string]string) string {
	t.Helper()
	b, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(b)
	return "header." + payload + ".sig"
}

// On construction, a persisted token restores signed-in session state (so a
// restart doesn't show signed-out while sync authenticates), with the identity
// decoded from the stored ID token.
func TestNewRestoresSessionFromCredentials(t *testing.T) {
	store := &memStore{tok: Token{
		AccessToken:  "a",
		RefreshToken: "r",
		IDToken:      unsignedIDToken(t, map[string]string{"sub": "u1", "email": "a@x.com", "name": "Ada"}),
		Expiry:       time.Now().Add(time.Hour),
	}}
	svc, err := newWithOptions(Config{}, withCredentialsStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cur, err := svc.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if !cur.Authenticated {
		t.Fatal("expected restored session to be signed in")
	}
	if cur.Identity == nil || cur.Identity.UserID != "u1" || cur.Identity.Email != "a@x.com" {
		t.Fatalf("restored identity: %+v", cur.Identity)
	}
}

// With no persisted token, construction stays signed out.
func TestNewStaysSignedOutWithoutCredentials(t *testing.T) {
	svc, err := newWithOptions(Config{}, withCredentialsStore(&memStore{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cur, err := svc.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if cur.Authenticated {
		t.Fatal("expected signed-out with an empty credentials store")
	}
}

// Sign-out revokes the refresh token server-side (RFC 7009) — fire-and-forget,
// after the local teardown — so a leaked token can't be used after the user
// signs out.
func TestLogoutRevokesToken(t *testing.T) {
	revoked := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		revoked <- r.Form.Get("token")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	store := &memStore{tok: Token{
		AccessToken: "a", RefreshToken: "r", Expiry: time.Now().Add(time.Hour),
	}}
	svc, err := newWithOptions(Config{},
		withCredentialsStore(store),
		withOAuthConfig(oauth.Config{ClientID: "kstack-app", RevocationURL: ts.URL}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if tok := testutil.Recv(t, revoked, "sign-out to revoke the token"); tok != "r" {
		t.Fatalf("revoked token = %q, want the refresh token \"r\"", tok)
	}
	// And the local credentials were still cleared.
	if saved, _ := store.Load(); saved.RefreshToken != "" {
		t.Fatalf("credentials not cleared after sign-out: %+v", saved)
	}
}

// Sign-out must not block on server-side revocation: a slow/unreachable
// revocation endpoint can't trap the user signed-in. Revocation runs
// fire-and-forget after the local teardown, so Logout returns and clears the
// credentials even while the revoke endpoint hangs.
func TestLogoutDoesNotBlockOnRevoke(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release // a synchronous revoke inside Logout would hang here forever
		w.WriteHeader(http.StatusOK)
	}))
	// Defer order matters: close(release) must run BEFORE ts.Close(), or
	// ts.Close() deadlocks waiting on the still-blocked handler.
	defer ts.Close()
	defer close(release)

	store := &memStore{tok: Token{
		AccessToken: "a", RefreshToken: "r", Expiry: time.Now().Add(time.Hour),
	}}
	svc, err := newWithOptions(Config{},
		withCredentialsStore(store),
		withOAuthConfig(oauth.Config{ClientID: "kstack-app", RevocationURL: ts.URL}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- svc.Logout(context.Background()) }()

	if err := testutil.Recv(t, done, "Logout to return without blocking on token revocation"); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	// Local teardown happened even though revocation is still blocked.
	if saved, _ := store.Load(); saved.RefreshToken != "" {
		t.Fatalf("credentials not cleared on sign-out: %+v", saved)
	}
}

// If clearing the durable credentials fails during sign-out, Logout returns
// the error and stays signed in (session not broadcast signed-out, token still
// servable) — so the process and a restart agree rather than the process
// reporting signed-out while the leftover refresh token would re-authenticate.
func TestLogoutKeepsSessionWhenCredentialClearFails(t *testing.T) {
	store := &memStore{
		tok:     Token{AccessToken: "a", RefreshToken: "r", Expiry: time.Now().Add(time.Hour)},
		saveErr: errors.New("keychain unavailable"),
	}
	// The preloaded token (with a refresh token) restores a signed-in session.
	svc, err := newWithOptions(Config{}, withCredentialsStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cur, _ := svc.Current(context.Background()); !cur.Authenticated {
		t.Fatal("precondition: expected restored session to be signed in")
	}

	if err := svc.Logout(context.Background()); err == nil {
		t.Fatal("want error when credential clearing fails, got nil")
	}
	if cur, _ := svc.Current(context.Background()); !cur.Authenticated {
		t.Fatal("session must stay signed in when credential clearing fails")
	}
	// The durable token survives (clear was rejected), keeping process + restart
	// consistent.
	if saved, _ := store.Load(); saved.RefreshToken != "r" {
		t.Fatalf("durable token should be intact, got %+v", saved)
	}
}

// Logout on a configured service clears the persisted credentials (not just
// the in-memory session), so the user can't keep authenticating after restart.
func TestLogoutClearsCredentials(t *testing.T) {
	store := &memStore{tok: Token{
		AccessToken:  "access",
		RefreshToken: "refresh",
		Expiry:       time.Now().Add(time.Hour),
	}}
	svc, err := newWithOptions(Config{}, withCredentialsStore(store))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if saved, _ := store.Load(); saved.RefreshToken != "" || saved.AccessToken != "" {
		t.Fatalf("credentials not cleared on sign-out: %+v", saved)
	}
	if cur, _ := svc.Current(context.Background()); cur.Authenticated {
		t.Fatal("session still signed in after Logout")
	}
}

// Logout on a degraded (unconfigured) service is a no-op and doesn't panic.
func TestLogoutDegradedNoop(t *testing.T) {
	svc, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
}

// A Service built with no credentials store is safe — Current reports
// signed-out, TokenSource is absent, and accessors don't panic.
func TestDegradedConstruction(t *testing.T) {
	svc, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cur, err := svc.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if cur.Authenticated {
		t.Fatal("degraded service should be signed-out")
	}
	if svc.TokenSource(context.Background()) != nil {
		t.Fatal("degraded service should expose no TokenSource")
	}
}

// A configured Service exposes an oauth2.TokenSource for downstream cloud calls.
func TestTokenSourceExposedWhenConfigured(t *testing.T) {
	svc, err := newWithOptions(Config{}, withCredentialsStore(&memStore{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.TokenSource(context.Background()) == nil {
		t.Fatal("configured service should expose a TokenSource")
	}
}

// newHydraOAuthConfig derives Hydra's standard endpoint paths from the issuer
// base, uses the default scopes when none are given, and (crucially) keeps the
// expected "iss" with a trailing slash even when the supplied issuer has none.
func TestNewHydraOAuthConfig(t *testing.T) {
	cfg := newHydraOAuthConfig("https://oauth.example", "client-1")
	if cfg.ClientID != "client-1" {
		t.Errorf("ClientID = %q", cfg.ClientID)
	}
	if cfg.AuthURL != "https://oauth.example/oauth2/auth" {
		t.Errorf("AuthURL = %q", cfg.AuthURL)
	}
	if cfg.TokenURL != "https://oauth.example/oauth2/token" {
		t.Errorf("TokenURL = %q", cfg.TokenURL)
	}
	if cfg.JWKSURL != "https://oauth.example/.well-known/jwks.json" {
		t.Errorf("JWKSURL = %q", cfg.JWKSURL)
	}
	if cfg.RevocationURL != "https://oauth.example/oauth2/revoke" {
		t.Errorf("RevocationURL = %q", cfg.RevocationURL)
	}
	if cfg.Issuer != "https://oauth.example/" {
		t.Errorf("Issuer = %q, want trailing slash", cfg.Issuer)
	}
	if len(cfg.Scopes) == 0 {
		t.Error("want default scopes when none supplied")
	}

	// A trailing slash on the issuer must not double up the paths nor the iss.
	withSlash := newHydraOAuthConfig("https://oauth.example/", "c", "openid")
	if withSlash.AuthURL != "https://oauth.example/oauth2/auth" {
		t.Errorf("AuthURL (trailing slash) = %q", withSlash.AuthURL)
	}
	if withSlash.Issuer != "https://oauth.example/" {
		t.Errorf("Issuer (trailing slash) = %q", withSlash.Issuer)
	}
	if len(withSlash.Scopes) != 1 || withSlash.Scopes[0] != "openid" {
		t.Errorf("Scopes override = %v", withSlash.Scopes)
	}
}
