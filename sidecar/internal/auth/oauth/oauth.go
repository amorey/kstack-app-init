// Package oauth is the OAuth2/OIDC protocol layer for the sign-in flow: PKCE (S256), the
// code→token exchange, refresh, RFC 7009 revocation, ID-token verification. A LEAF —
// it must not import the parent auth package, so it owns the value types the protocol
// produces (Token, Identity), which the parent re-exports as aliases.
//
// The Client takes resolved endpoint URLs and is Hydra-agnostic. The loopback listener
// and the verifier/state generation live in the parent — not protocol concerns.
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

// expiryLeeway expires a token early, so one handed out stays valid through the request
// that uses it.
const expiryLeeway = 30 * time.Second

// Token is the stored cloud credential. The refresh token is static (Hydra doesn't rotate
// it); access/id tokens are short-lived and renewed via Refresh. It keeps the id token —
// needed to restore display identity at startup — unlike the public TokenSet.
type Token struct {
	AccessToken  string    `json:"accessToken"`
	RefreshToken string    `json:"refreshToken"`
	IDToken      string    `json:"idToken"`
	Expiry       time.Time `json:"expiry"`
}

// Valid reports an access token present and outside the leeway window of expiry.
func (t Token) Valid(now time.Time) bool {
	return t.AccessToken != "" && now.Before(t.Expiry.Add(-expiryLeeway))
}

// Identity is the ID-token claims the app cares about; the JSON tags decode straight from
// the OIDC claims, so both Verify and the startup decode unmarshal into it.
type Identity struct {
	UserID string `json:"sub"`
	Email  string `json:"email"`
	Name   string `json:"name"`
}

// Config is the OAuth client's static configuration. Endpoints are explicit rather than
// discovered, so the flow is deterministic and tests can point them at httptest servers.
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

// NewClient does no network I/O — the JWKS fetch is lazy, on first Verify. That fetch
// reuses the 15s-timeout HTTP client, because go-oidc ignores context cancellation for
// the key set, so only the client timeout bounds it.
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
		// The base context is irrelevant — go-oidc ignores its cancel; ClientContext
		// is only how the timeout-bounded client reaches the lazy fetch.
		keySet := oidc.NewRemoteKeySet(oidc.ClientContext(context.Background(), c.httpc), cfg.JWKSURL)
		c.verifier = oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{ClientID: cfg.ClientID})
	}
	return c
}

// Revoke revokes a token (RFC 7009); the desktop public client needs only client_id.
// Revoking the REFRESH token also invalidates its grant's access tokens, which is what
// sign-out passes. A nil error means "no longer usable" — the server also reports success
// for an already-invalid token.
func (c *Client) Revoke(ctx context.Context, token string) error {
	// Cap of the non-2xx body read for the error message.
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

// AuthCodeURL builds the PKCE (S256) authorize URL. redirect is the loopback listener's
// ephemeral address and MUST match the one passed to Exchange.
func (c *Client) AuthCodeURL(state, verifier, redirect string) string {
	return c.oc.AuthCodeURL(state,
		oauth2.S256ChallengeOption(verifier),
		oauth2.SetAuthURLParam("redirect_uri", redirect),
	)
}

// Exchange trades an authorization code + PKCE verifier for a Token; redirect must echo
// the one sent to AuthCodeURL, as OAuth requires.
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
	// oauth2 already carries the prior refresh token forward; belt-and-braces so a
	// static refresh token is never lost.
	if t.RefreshToken == "" {
		t.RefreshToken = refreshToken
	}
	return t, nil
}

// Verify validates an ID token against the JWKS (signature, issuer, audience, expiry) and
// returns its identity claims.
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

// ParseIdentityUnverified decodes an ID token's claims WITHOUT verifying its signature —
// only to restore display identity at startup from our own keychain. Never use it for an
// authorization decision (use Verify); it grants no access, since the cloud re-verifies
// the access token per request.
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

// toToken maps an oauth2.Token to ours, lifting id_token out of the extra fields.
func toToken(t *oauth2.Token) Token {
	idToken, _ := t.Extra("id_token").(string)
	return Token{
		AccessToken:  t.AccessToken,
		RefreshToken: t.RefreshToken,
		IDToken:      idToken,
		Expiry:       t.Expiry,
	}
}
