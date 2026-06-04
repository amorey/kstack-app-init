package oauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// testVerifier is a sample PKCE code verifier. The protocol layer treats the
// verifier as caller-supplied (the parent auth package generates the real one),
// so these tests just need any non-empty value.
const testVerifier = "test-verifier-1234567890"

// s256Challenge is the test oracle for the PKCE code challenge of a verifier —
// base64url(SHA256(verifier)), no padding (RFC 7636). Production builds the
// challenge via oauth2.S256ChallengeOption inside AuthCodeURL; this independent
// computation lets TestAuthCodeURL assert that wiring produced the right value.
func s256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// AuthCodeURL carries client_id, the per-flow redirect_uri (the loopback
// listener's ephemeral 127.0.0.1 address — exactly one), code_challenge (S256),
// code_challenge_method, and state.
func TestAuthCodeURL(t *testing.T) {
	c := NewClient(Config{
		ClientID: "kstack-app",
		AuthURL:  "https://oauth.kstack.sh/oauth2/auth",
		TokenURL: "https://oauth.kstack.sh/oauth2/token",
		Scopes:   []string{"openid", "offline_access"},
	})
	redirect := "http://127.0.0.1:49217/oauth/callback"
	raw := c.AuthCodeURL("xyz-state", testVerifier, redirect)

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q := u.Query()
	if q.Get("client_id") != "kstack-app" {
		t.Errorf("client_id = %q", q.Get("client_id"))
	}
	if got := q["redirect_uri"]; len(got) != 1 || got[0] != redirect {
		t.Errorf("redirect_uri = %v, want exactly [%q]", got, redirect)
	}
	if q.Get("state") != "xyz-state" {
		t.Errorf("state = %q", q.Get("state"))
	}
	if q.Get("code_challenge_method") != "S256" {
		t.Errorf("code_challenge_method = %q", q.Get("code_challenge_method"))
	}
	if q.Get("code_challenge") != s256Challenge(testVerifier) {
		t.Errorf("code_challenge mismatch")
	}
}

// stubTokenServer serves an OAuth token endpoint returning a fixed token bundle.
func stubTokenServer(t *testing.T, idTok string, wantGrant string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != wantGrant {
			t.Errorf("grant_type = %q, want %q", got, wantGrant)
		}
		if wantGrant == "authorization_code" && r.Form.Get("code_verifier") == "" {
			t.Errorf("missing code_verifier on auth-code exchange")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-123",
			"refresh_token": "refresh-123",
			"id_token":      idTok,
			"token_type":    "bearer",
			"expires_in":    3600,
		})
	}))
}

// Exchange posts code + verifier and parses the token bundle.
func TestExchange(t *testing.T) {
	ts := stubTokenServer(t, "id-tok", "authorization_code")
	defer ts.Close()

	c := NewClient(Config{
		ClientID: "kstack-app",
		TokenURL: ts.URL,
	})
	tok, err := c.Exchange(context.Background(), "the-code", testVerifier, "")
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if tok.AccessToken != "access-123" || tok.RefreshToken != "refresh-123" || tok.IDToken != "id-tok" {
		t.Fatalf("token: %+v", tok)
	}
	if tok.Expiry.IsZero() {
		t.Fatalf("expiry not set")
	}
}

// Refresh exchanges the static refresh token for a fresh bundle.
func TestRefresh(t *testing.T) {
	ts := stubTokenServer(t, "id-tok-2", "refresh_token")
	defer ts.Close()

	c := NewClient(Config{
		ClientID: "kstack-app",
		TokenURL: ts.URL,
	})
	tok, err := c.Refresh(context.Background(), "refresh-123")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if tok.AccessToken != "access-123" {
		t.Fatalf("access token: %q", tok.AccessToken)
	}
}

// Revoke POSTs the token + client_id (public client, no secret) to the
// revocation endpoint and treats a 2xx as success.
func TestRevoke(t *testing.T) {
	var gotToken, gotClient, gotCT string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		gotToken = r.Form.Get("token")
		gotClient = r.Form.Get("client_id")
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := NewClient(Config{
		ClientID:      "kstack-app",
		RevocationURL: ts.URL,
	})
	if err := c.Revoke(context.Background(), "refresh-123"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if gotToken != "refresh-123" || gotClient != "kstack-app" {
		t.Fatalf("posted token=%q client_id=%q", gotToken, gotClient)
	}
	if gotCT != "application/x-www-form-urlencoded" {
		t.Fatalf("content-type = %q", gotCT)
	}
}

// A non-2xx revocation response surfaces an error.
func TestRevokeServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewClient(Config{ClientID: "x", RevocationURL: ts.URL})
	if err := c.Revoke(context.Background(), "t"); err == nil {
		t.Fatal("want error on 500, got nil")
	}
}

// Revoke without a configured endpoint errors rather than silently no-op'ing.
func TestRevokeNoEndpoint(t *testing.T) {
	c := NewClient(Config{ClientID: "x"})
	if err := c.Revoke(context.Background(), "t"); err == nil {
		t.Fatal("want error when no revocation endpoint is configured")
	}
}

// ParseIdentityUnverified decodes claims from an ID token's payload without
// checking the signature (used to restore display identity at startup).
func TestParseIdentityUnverified(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"u1","email":"a@x.com","name":"Ada"}`))
	raw := "header." + payload + ".sig"
	id, err := ParseIdentityUnverified(raw)
	if err != nil {
		t.Fatalf("ParseIdentityUnverified: %v", err)
	}
	if id.UserID != "u1" || id.Email != "a@x.com" || id.Name != "Ada" {
		t.Fatalf("identity: %+v", id)
	}
}

// A token that isn't a three-segment JWT is rejected.
func TestParseIdentityUnverifiedMalformed(t *testing.T) {
	if _, err := ParseIdentityUnverified("not-a-jwt"); err == nil {
		t.Fatal("want error for a malformed ID token")
	}
}

// --- ID-token verification helpers ---

type idTokenServer struct {
	srv      *httptest.Server
	signKey  *rsa.PrivateKey
	otherKey *rsa.PrivateKey
	issuer   string
}

func newIDTokenServer(t *testing.T) *idTokenServer {
	t.Helper()
	signKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	otherKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key: signKey.Public(), KeyID: "test-key", Algorithm: "RS256", Use: "sig",
	}}}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	})
	its := &idTokenServer{signKey: signKey, otherKey: otherKey}
	its.srv = httptest.NewServer(mux)
	its.issuer = its.srv.URL
	return its
}

func (its *idTokenServer) sign(t *testing.T, key *rsa.PrivateKey, aud string, extra map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test-key"),
	)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	claims := jwt.Claims{
		Issuer:   its.issuer,
		Subject:  "user-1",
		Audience: jwt.Audience{aud},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		IssuedAt: jwt.NewNumericDate(time.Now()),
	}
	raw, err := jwt.Signed(signer).Claims(claims).Claims(extra).Serialize()
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return raw
}

// Verify decodes sub/email/name from a properly signed token.
func TestVerifyValidToken(t *testing.T) {
	its := newIDTokenServer(t)
	defer its.srv.Close()

	c := NewClient(Config{
		ClientID: "kstack-app",
		Issuer:   its.issuer,
		JWKSURL:  its.srv.URL + "/jwks",
	})
	raw := its.sign(t, its.signKey, "kstack-app", map[string]any{
		"email": "a@example.com", "name": "Ada",
	})
	id, err := c.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.UserID != "user-1" || id.Email != "a@example.com" || id.Name != "Ada" {
		t.Fatalf("identity: %+v", id)
	}
}

// Token.Valid requires a present access token AND a now strictly before expiry
// minus the leeway window — so a token handed out stays usable through the
// request that consumes it.
func TestTokenValid(t *testing.T) {
	// epoch is a fixed reference time for the deterministic cases below.
	epoch := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name string
		tok  Token
		want bool
	}{
		{
			name: "valid well before expiry",
			tok:  Token{AccessToken: "a", Expiry: epoch.Add(time.Hour)},
			want: true,
		},
		{
			name: "no access token is never valid",
			tok:  Token{Expiry: epoch.Add(time.Hour)},
			want: false,
		},
		{
			name: "already expired",
			tok:  Token{AccessToken: "a", Expiry: epoch.Add(-time.Minute)},
			want: false,
		},
		{
			name: "inside the leeway window counts as expired",
			tok:  Token{AccessToken: "a", Expiry: epoch.Add(expiryLeeway - time.Second)},
			want: false,
		},
		{
			name: "exactly at the leeway boundary is not valid",
			tok:  Token{AccessToken: "a", Expiry: epoch.Add(expiryLeeway)},
			want: false,
		},
		{
			name: "just outside the leeway window is valid",
			tok:  Token{AccessToken: "a", Expiry: epoch.Add(expiryLeeway + time.Second)},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tok.Valid(epoch); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Verify rejects a token signed by a key absent from the JWKS.
func TestVerifyBadSignature(t *testing.T) {
	its := newIDTokenServer(t)
	defer its.srv.Close()

	c := NewClient(Config{
		ClientID: "kstack-app",
		Issuer:   its.issuer,
		JWKSURL:  its.srv.URL + "/jwks",
	})
	raw := its.sign(t, its.otherKey, "kstack-app", nil) // wrong key
	if _, err := c.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected verification to fail for a bad signature")
	}
}
