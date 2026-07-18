// Package oauth is the OAuth2 / OIDC protocol layer for the auth subsystem's
// sign-in flow against kstack-cloud's Hydra server: PKCE (S256 challenge), the
// code→token exchange, refresh-token renewal, RFC 7009 revocation, and ID-token
// verification. It is a leaf with no dependency on the parent auth package — auth
// builds a Client from resolved endpoint URLs and drives it — so it owns the
// value types the protocol produces (Token, Identity), which the parent
// re-exports via type aliases.
//
// The Client takes resolved endpoint URLs (it is Hydra-agnostic; the parent
// derives Hydra's path layout). The loopback redirect listener that catches the
// callback, and the verifier/state generation that feed AuthCodeURL, live in the
// parent auth package — they aren't OAuth-protocol concerns.
package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// expiryLeeway treats a token as expired slightly before its real expiry so a
// token handed out is still valid through the request that uses it.
const expiryLeeway = 30 * time.Second

// Token is the user's stored cloud credential. The refresh token is static
// (Hydra does not rotate it); the access and id tokens are short-lived and
// renewed via Refresh. It keeps the id token (needed to restore display
// identity at startup) and so is distinct from the tokenless public TokenSet.
type Token struct {
	AccessToken  string    `json:"accessToken"`
	RefreshToken string    `json:"refreshToken"`
	IDToken      string    `json:"idToken"`
	Expiry       time.Time `json:"expiry"`
}

// Valid reports whether the access token is present and not within the leeway
// window of expiry.
func (t Token) Valid(now time.Time) bool {
	return t.AccessToken != "" && now.Before(t.Expiry.Add(-expiryLeeway))
}

// Identity is the subset of the verified ID-token claims the app cares about.
// The JSON tags decode straight from the OIDC claims (`sub` → UserID), so Verify
// and the unverified startup decode unmarshal into this type directly.
type Identity struct {
	UserID string `json:"sub"`
	Email  string `json:"email"`
	Name   string `json:"name"`
}

// Config is the static configuration for the OAuth client. Endpoint URLs are
// explicit (rather than discovered) so the flow is deterministic and testable —
// tests point them at httptest servers. In production the parent auth package
// builds it from just an issuer + client id via newHydraOAuthConfig.
type Config struct {
	ClientID      string
	AuthURL       string
	TokenURL      string
	JWKSURL       string
	RevocationURL string // Hydra's RFC 7009 token-revocation endpoint
	Issuer        string // expected "iss" of the ID token
	Scopes        []string
}

// Client performs the OAuth flow and verifies ID tokens.
type Client struct {
	oc            *oauth2.Config
	verifier      *oidc.IDTokenVerifier
	revocationURL string
	httpc         *http.Client
}

// NewClient builds a Client. The ID-token verifier fetches Hydra's JWKS from
// cfg.JWKSURL lazily (on first Verify), so construction does no network I/O. That
// fetch reuses the client's own 15s-timeout HTTP client (via oidc.ClientContext),
// so a slow/hung JWKS endpoint can't block Verify unbounded — go-oidc ignores
// context cancellation for the key set, so the client timeout, not a context,
// bounds the fetch.
func NewClient(cfg Config) *Client {
	oc := &oauth2.Config{
		ClientID: cfg.ClientID,
		Scopes:   cfg.Scopes,
		Endpoint: oauth2.Endpoint{
			AuthURL:  cfg.AuthURL,
			TokenURL: cfg.TokenURL,
		},
	}
	c := &Client{
		oc:            oc,
		revocationURL: cfg.RevocationURL,
		httpc:         &http.Client{Timeout: 15 * time.Second},
	}
	if cfg.JWKSURL != "" {
		// oidc.ClientContext carries our timeout-bounded client to the (lazy) JWKS
		// fetch; the base context is irrelevant since go-oidc ignores its cancel.
		keySet := oidc.NewRemoteKeySet(oidc.ClientContext(context.Background(), c.httpc), cfg.JWKSURL)
		c.verifier = oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{ClientID: cfg.ClientID})
	}
	return c
}

// Revoke asks Hydra to revoke a token (RFC 7009). For the desktop public client
// this needs only client_id (no secret). Revoking the refresh token also
// invalidates the access tokens issued from the same grant, so sign-out passes
// the refresh token here. The server reports success even for an already-invalid
// token, so a nil error means "the token is not usable anymore".
func (c *Client) Revoke(ctx context.Context, token string) error {
	// maxErrBody caps how much of a non-2xx body we read for the error message.
	const maxErrBody = 4 << 10
	if c.revocationURL == "" {
		return fmt.Errorf("oauth: no revocation endpoint configured")
	}
	form := url.Values{
		"token":     {token},
		"client_id": {c.oc.ClientID},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.revocationURL, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
		return fmt.Errorf("oauth: revoke failed %d: %s", resp.StatusCode, body)
	}
	return nil
}

// AuthCodeURL builds the authorize URL with PKCE (S256). The sidecar opens this
// in the system browser. redirect is the per-flow redirect_uri — the loopback
// listener's ephemeral 127.0.0.1 address — and MUST match the redirect passed
// to Exchange.
func (c *Client) AuthCodeURL(state, verifier, redirect string) string {
	return c.oc.AuthCodeURL(state,
		oauth2.S256ChallengeOption(verifier),
		oauth2.SetAuthURLParam("redirect_uri", redirect),
	)
}

// Exchange trades an authorization code (+ PKCE verifier) for a Token. redirect
// must be the same redirect_uri sent to AuthCodeURL (OAuth requires the token
// request to echo it).
func (c *Client) Exchange(ctx context.Context, code, verifier, redirect string) (Token, error) {
	tok, err := c.oc.Exchange(ctx, code,
		oauth2.VerifierOption(verifier),
		oauth2.SetAuthURLParam("redirect_uri", redirect),
	)
	if err != nil {
		return Token{}, err
	}
	return toToken(tok), nil
}

// Refresh renews the access/id tokens from the (static) refresh token.
func (c *Client) Refresh(ctx context.Context, refreshToken string) (Token, error) {
	src := c.oc.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken})
	tok, err := src.Token()
	if err != nil {
		return Token{}, err
	}
	t := toToken(tok)
	// oauth2 carries the prior refresh token forward when the server omits one;
	// guard anyway so a static refresh token is never lost.
	if t.RefreshToken == "" {
		t.RefreshToken = refreshToken
	}
	return t, nil
}

// Verify validates an ID token against Hydra's JWKS (signature, issuer,
// audience, expiry) and returns the identity claims.
func (c *Client) Verify(ctx context.Context, rawIDToken string) (Identity, error) {
	if c.verifier == nil {
		return Identity{}, fmt.Errorf("oauth: no verifier configured (missing JWKSURL)")
	}
	idt, err := c.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return Identity{}, err
	}
	var id Identity
	if err := idt.Claims(&id); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// ParseIdentityUnverified decodes the identity claims from an ID token WITHOUT
// verifying its signature. It exists only to restore display identity from a
// token already held in our own trusted storage (the keychain) at startup. It
// grants no access: the cloud re-verifies the access token on every request, so
// a tampered local token would simply fail those calls. Never use this to make
// an authorization decision — use Verify for that.
func ParseIdentityUnverified(rawIDToken string) (Identity, error) {
	parts := strings.Split(rawIDToken, ".")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("oauth: malformed ID token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, fmt.Errorf("oauth: decode ID token payload: %w", err)
	}
	var id Identity
	if err := json.Unmarshal(payload, &id); err != nil {
		return Identity{}, fmt.Errorf("oauth: unmarshal ID token claims: %w", err)
	}
	return id, nil
}

// toToken maps an oauth2.Token to our Token, lifting the OIDC id_token out of
// the token's extra fields.
func toToken(t *oauth2.Token) Token {
	idToken, _ := t.Extra("id_token").(string)
	return Token{
		AccessToken:  t.AccessToken,
		RefreshToken: t.RefreshToken,
		IDToken:      idToken,
		Expiry:       t.Expiry,
	}
}
