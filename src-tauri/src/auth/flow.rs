//! Login state machine. Wraps `openidconnect::CoreClient`, which owns the
//! protocol — discovery, PKCE, the token endpoint, and ID token signature
//! + claim verification. This module's job is the desktop-shaped glue:
//!
//! 1. `Auth::login` binds an ephemeral loopback listener (RFC 8252 §7.3),
//!    builds the auth URL with `http://127.0.0.1:<port>/oauth/callback`
//!    as the redirect, and opens the system browser.
//! 2. The browser follows Hydra's redirect to our listener, which parses
//!    `code` + `state`, replies with a "you can close this tab" page,
//!    and hands the code back.
//! 3. `Auth::login` exchanges the code, verifies the ID token, persists
//!    the refresh token, and returns identity claims.
//!
//! Why loopback (and not a custom URL scheme): RFC 8252 recommends it for
//! native apps because it works identically across operating systems,
//! across signed and unsigned binaries, and across `tauri dev` and
//! production. Custom schemes have ambiguous namespace ownership on
//! desktop OSes (any app can register `kstack://`) and break in dev when
//! the binary isn't a properly bundled `.app`. The `kstack://` scheme is
//! still registered with the OS (see `crate::deep_link`) for non-auth
//! deep links — e.g. opening the app to a particular cluster from a URL
//! posted in chat — but the OAuth flow doesn't ride on it.

use openidconnect::core::{
    CoreAuthenticationFlow, CoreClient, CoreIdTokenClaims, CoreProviderMetadata,
};
use openidconnect::reqwest;
use openidconnect::{
    AuthorizationCode, ClientId, CsrfToken, EndpointMaybeSet, EndpointNotSet, EndpointSet,
    IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge, RedirectUrl, RefreshToken, Scope,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::loopback;
use super::tokens::{load_persisted, now, save_persisted, Persisted, Tokens};
use super::{CLIENT_ID, ISSUER, SCOPES};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
#[cfg(test)]
mod logout_tests;

/// Concrete `CoreClient` after we've supplied issuer + endpoints from
/// discovery and pinned the redirect URI. The phantom-typestates record
/// which endpoints are known; we end up with auth+token+userinfo set, the
/// rest "maybe set" (depends on the IdP).
type OidcClient = CoreClient<
    EndpointSet,      // HasAuthUrl
    EndpointNotSet,   // HasDeviceAuthUrl
    EndpointNotSet,   // HasIntrospectionUrl
    EndpointNotSet,   // HasRevocationUrl
    EndpointMaybeSet, // HasTokenUrl
    EndpointMaybeSet, // HasUserInfoUrl
>;

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub authenticated: bool,
    pub email: Option<String>,
    pub name: Option<String>,
    pub sub: Option<String>,
}

impl From<Option<&Tokens>> for Status {
    fn from(tokens: Option<&Tokens>) -> Self {
        match tokens {
            Some(t) => Status {
                // A live AT *or* a stored RT counts as authenticated;
                // `access_token()` will refresh transparently.
                authenticated: !t.is_expired() || t.refresh_token.is_some(),
                email: t.email.clone(),
                name: t.name.clone(),
                sub: t.sub.clone(),
            },
            None => Status {
                authenticated: false,
                email: None,
                name: None,
                sub: None,
            },
        }
    }
}

pub struct Auth {
    tokens: RwLock<Option<Tokens>>,
    /// Cached OIDC client. Lazily built by `client()` because discovery
    /// is a network call we don't want to make at process start.
    oidc: RwLock<Option<OidcClient>>,
    /// Single-flight guard around discovery so concurrent callers don't
    /// each fetch the metadata.
    oidc_init: Mutex<()>,
    /// Serializes refresh-token grants so concurrent callers don't burn a
    /// just-rotated refresh token.
    refresh_lock: Mutex<()>,
    http: reqwest::Client,
    /// OIDC issuer base URL (trailing slash). Injectable so tests can
    /// point at a local mock IdP without monkey-patching the const.
    /// Production code uses [`Auth::new`], which pins it to [`ISSUER`].
    issuer: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self::new()
    }
}

impl Auth {
    pub fn new() -> Self {
        Self::with_issuer(ISSUER.to_string())
    }

    /// Constructor for tests that need to redirect discovery and the
    /// revocation endpoint at a stub server. The `issuer` should end in
    /// `/` to match what we concatenate `.well-known/...` onto.
    pub fn with_issuer(issuer: String) -> Self {
        // `redirect(none)` is the openidconnect-recommended setting:
        // following the auth-server's 30x is a known SSRF vector.
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client");
        Self {
            tokens: RwLock::new(None),
            oidc: RwLock::new(None),
            oidc_init: Mutex::new(()),
            refresh_lock: Mutex::new(()),
            http,
            issuer,
        }
    }

    async fn client(&self) -> Result<OidcClient, String> {
        if let Some(c) = self.oidc.read().await.clone() {
            return Ok(c);
        }
        let _g = self.oidc_init.lock().await;
        if let Some(c) = self.oidc.read().await.clone() {
            return Ok(c);
        }
        let issuer = IssuerUrl::new(self.issuer.clone()).map_err(|e| format!("issuer: {e}"))?;
        let metadata = CoreProviderMetadata::discover_async(issuer, &self.http)
            .await
            .map_err(|e| format!("discovery: {e}"))?;
        // The redirect URI is set per-login (each binds a fresh ephemeral
        // port), so we don't pin one on the cached client.
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(CLIENT_ID.to_string()),
            None,
        );
        *self.oidc.write().await = Some(client.clone());
        Ok(client)
    }

    /// On startup, try to silently restore a session from the stored RT.
    /// Errors clear the persisted session so we don't keep retrying a
    /// token the IdP has already revoked. We also seed identity claims
    /// from the persisted ID token (unverified; see
    /// [`decode_id_claims_unverified`]) so the menu can render before
    /// `do_refresh` resolves.
    pub async fn try_restore(&self) -> Result<bool, String> {
        let Some(persisted) = load_persisted()? else {
            return Ok(false);
        };
        if let Some(idt) = persisted.id_token.as_deref() {
            let (email, name, sub) = decode_id_claims_unverified(idt);
            *self.tokens.write().await = Some(Tokens {
                access_token: String::new(),
                refresh_token: Some(persisted.refresh_token.clone()),
                id_token: Some(idt.to_string()),
                expires_at: 0,
                email,
                name,
                sub,
            });
        }
        let _g = self.refresh_lock.lock().await;
        match self.do_refresh(&persisted.refresh_token).await {
            Ok(_) => Ok(true),
            Err(e) => {
                log::warn!("auth: refresh on startup failed: {e}");
                *self.tokens.write().await = None;
                let _ = save_persisted(None);
                Ok(false)
            }
        }
    }

    pub async fn status(&self) -> Status {
        Status::from(self.tokens.read().await.as_ref())
    }

    #[cfg(test)]
    async fn set_tokens_for_test(&self, t: Tokens) {
        *self.tokens.write().await = Some(t);
    }

    #[cfg(test)]
    async fn has_tokens(&self) -> bool {
        self.tokens.read().await.is_some()
    }

    pub async fn access_token(&self) -> Result<String, String> {
        // Snapshot just the fields we need so we don't hold the read lock
        // (or clone the whole `Tokens` with its secrets) across the await
        // for the refresh path.
        let snapshot = self.tokens.read().await.as_ref().map(|t| {
            (
                t.access_token.clone(),
                t.is_expired(),
                t.refresh_token.is_some(),
            )
        });
        match snapshot {
            None => Err("not authenticated".into()),
            Some((at, false, _)) => Ok(at),
            Some((_, true, false)) => Err("not authenticated".into()),
            Some((_, true, true)) => self.refresh_now().await,
        }
    }

    /// Refresh path that owns the lock + RT lookup. Both `access_token`
    /// (stale-AT path) and `try_restore` (cold start) share this so the
    /// token endpoint sees at most one in-flight refresh and always uses
    /// the *current* RT — Hydra rotates refresh tokens. Returns the new
    /// access token directly, avoiding a second read on `self.tokens`.
    async fn refresh_now(&self) -> Result<String, String> {
        let _g = self.refresh_lock.lock().await;
        // Snapshot expiry + RT under one read lock; another caller may
        // have just refreshed, in which case we hand back their AT.
        let snapshot = self.tokens.read().await.as_ref().map(|t| {
            (
                t.access_token.clone(),
                t.is_expired(),
                t.refresh_token.clone(),
            )
        });
        match snapshot {
            Some((at, false, _)) => Ok(at),
            Some((_, true, Some(rt))) => self.do_refresh(&rt).await,
            _ => Err("no refresh token".into()),
        }
    }

    /// `open_browser` is injected so tests can observe the URL without
    /// shelling out to `xdg-open`/`open`.
    pub async fn login<F>(&self, open_browser: F) -> Result<Status, String>
    where
        F: FnOnce(&str) -> Result<(), String>,
    {
        let (listener, redirect_uri) = loopback::bind()?;
        // Override the cached client's redirect URI per-login. Hydra
        // accepts loopback redirects with any port at runtime per RFC
        // 8252 §7.3, as long as the host + path match what's registered.
        let client = self.client().await?.set_redirect_uri(
            RedirectUrl::new(redirect_uri).map_err(|e| format!("redirect: {e}"))?,
        );
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (auth_url, csrf, nonce) = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scopes(SCOPES.iter().map(|s| Scope::new((*s).to_string())))
            .set_pkce_challenge(challenge)
            .url();

        open_browser(auth_url.as_str())?;
        let result = loopback::accept_callback_once(listener, csrf.secret()).await?;
        if let Some(err) = result.error {
            return Err(format!("idp returned error: {err}"));
        }
        let code = result
            .code
            .ok_or_else(|| "callback missing code".to_string())?;
        self.complete(&client, AuthorizationCode::new(code), verifier, &nonce)
            .await?;
        Ok(self.status().await)
    }

    async fn complete(
        &self,
        client: &OidcClient,
        code: AuthorizationCode,
        verifier: openidconnect::PkceCodeVerifier,
        nonce: &Nonce,
    ) -> Result<(), String> {
        let response = client
            .exchange_code(code)
            .map_err(|e| format!("exchange_code: {e}"))?
            .set_pkce_verifier(verifier)
            .request_async(&self.http)
            .await
            .map_err(|e| format!("token exchange: {e}"))?;

        // Verify the ID token: signature against discovery's JWKS,
        // `iss`/`aud`/`exp`, and the `nonce` we minted above.
        let id_token = response
            .extra_fields()
            .id_token()
            .ok_or_else(|| "token response missing id_token".to_string())?;
        let claims = id_token
            .claims(&client.id_token_verifier(), nonce)
            .map_err(|e| format!("id_token verify: {e}"))?;

        let expires_at = now() + response.expires_in().map(|d| d.as_secs()).unwrap_or(3600);

        let (email, name, sub) = identity_from_claims(claims);
        let tokens = Tokens {
            access_token: response.access_token().secret().clone(),
            refresh_token: response.refresh_token().map(|r| r.secret().clone()),
            id_token: Some(id_token.to_string()),
            expires_at,
            email,
            name,
            sub,
        };
        persist_session(&tokens)?;
        *self.tokens.write().await = Some(tokens);
        Ok(())
    }

    /// Refresh-token grant. Hydra rotates refresh tokens, so we replace
    /// the stored value with whatever comes back. When the IdP reissues
    /// an ID token, we verify it skipping the nonce check — refresh
    /// responses don't echo the original nonce, and signature + iss/aud/
    /// exp are what actually matter here. Otherwise we carry identity
    /// claims forward from in-memory state.
    async fn do_refresh(&self, refresh_token: &str) -> Result<String, String> {
        let client = self.client().await?;
        let response = client
            .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
            .map_err(|e| format!("exchange_refresh_token: {e}"))?
            .request_async(&self.http)
            .await
            .map_err(|e| format!("refresh: {e}"))?;

        let expires_at = now() + response.expires_in().map(|d| d.as_secs()).unwrap_or(3600);

        let (id_token, email, name, sub) = match response.extra_fields().id_token() {
            Some(fresh) => {
                let claims = fresh
                    .claims(&client.id_token_verifier(), |_: Option<&Nonce>| Ok(()))
                    .map_err(|e| format!("id_token verify on refresh: {e}"))?;
                let (email, name, sub) = identity_from_claims(claims);
                (Some(fresh.to_string()), email, name, sub)
            }
            None => {
                let prev = self.tokens.read().await;
                match prev.as_ref() {
                    Some(p) => (
                        p.id_token.clone(),
                        p.email.clone(),
                        p.name.clone(),
                        p.sub.clone(),
                    ),
                    None => (None, None, None, None),
                }
            }
        };
        let new = Tokens {
            access_token: response.access_token().secret().clone(),
            refresh_token: response.refresh_token().map(|r| r.secret().clone()),
            id_token,
            expires_at,
            email,
            name,
            sub,
        };
        persist_session(&new)?;
        let access_token = new.access_token.clone();
        *self.tokens.write().await = Some(new);
        Ok(access_token)
    }

    /// Clears local session, then revokes the OAuth tokens at the IdP per
    /// RFC 7009. We intentionally do *not* hit Hydra's RP-initiated logout
    /// (`end_session_endpoint`): that flow ends the *browser session* with
    /// the IdP, which is a global, user-visible side effect — it would log
    /// the user out of anything else federated against the same Hydra in
    /// the same browser. From a desktop client's perspective "log out"
    /// should mean "this app forgets me," which is exactly token
    /// revocation: invalidate the refresh token (and the access token, so
    /// any in-flight bearer use stops working immediately) without
    /// touching the IdP's SSO cookie.
    ///
    /// Local state is cleared *before* the network call so a logout still
    /// looks instant offline; revocation is best-effort. If it fails the
    /// tokens remain valid server-side until they expire on their own,
    /// which is acceptable — the alternative (leaving local state intact
    /// when the network is down) is worse UX.
    pub async fn logout(&self) -> Result<(), String> {
        let (access_token, refresh_token) = {
            let mut guard = self.tokens.write().await;
            match guard.take() {
                Some(t) => (Some(t.access_token), t.refresh_token),
                None => (None, None),
            }
        };
        save_persisted(None)?;

        if access_token.is_none() && refresh_token.is_none() {
            return Ok(());
        }

        // `openidconnect`'s `CoreProviderMetadata` doesn't surface
        // `revocation_endpoint` without an `AdditionalProviderMetadata`
        // impl, so fetch the discovery doc directly for just this field.
        #[derive(Deserialize)]
        struct RevocationMetadata {
            revocation_endpoint: Option<String>,
        }
        // `issuer` ends with `/`; concatenate without an extra separator
        // to avoid a double-slash some servers reject.
        let url = format!("{}.well-known/openid-configuration", self.issuer);
        let meta: RevocationMetadata = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("revoke discovery: {e}"))?
            .error_for_status()
            .map_err(|e| format!("revoke discovery status: {e}"))?
            .json()
            .await
            .map_err(|e| format!("revoke discovery parse: {e}"))?;
        let Some(endpoint) = meta.revocation_endpoint else {
            // No revocation endpoint advertised — nothing more we can do.
            // Tokens will expire naturally; local state is already gone.
            return Ok(());
        };

        // On Hydra, revoking the refresh token also invalidates access
        // tokens minted from it, so the AT call is usually a no-op — but
        // we send it anyway for IdPs that don't cascade. The two calls
        // are independent (`revoke_one` swallows errors), so fire them
        // concurrently to halve the network leg.
        let rt_fut = async {
            if let Some(rt) = refresh_token.as_deref() {
                self.revoke_one(&endpoint, rt, "refresh_token").await;
            }
        };
        let at_fut = async {
            if let Some(at) = access_token.as_deref() {
                self.revoke_one(&endpoint, at, "access_token").await;
            }
        };
        tokio::join!(rt_fut, at_fut);
        Ok(())
    }

    /// Single RFC 7009 revocation POST. Errors are logged, not returned:
    /// per RFC 7009 §2.2 the server SHOULD return 200 even for an unknown
    /// token, so the only thing a failure here means is "we couldn't
    /// reach the IdP" — which doesn't change what the caller does next.
    /// `client_id` goes in the body because we're a public client
    /// (`token_endpoint_auth_method=none`); there's no secret to send.
    async fn revoke_one(&self, endpoint: &str, token: &str, hint: &'static str) {
        let res = self
            .http
            .post(endpoint)
            .form(&[
                ("token", token),
                ("token_type_hint", hint),
                ("client_id", CLIENT_ID),
            ])
            .send()
            .await;
        match res {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => log::warn!("auth: revoke {hint} returned {}", r.status()),
            Err(e) => log::warn!("auth: revoke {hint} failed: {e}"),
        }
    }
}

fn persist_session(t: &Tokens) -> Result<(), String> {
    match &t.refresh_token {
        Some(rt) => save_persisted(Some(&Persisted {
            refresh_token: rt.clone(),
            id_token: t.id_token.clone(),
        })),
        // No RT means nothing usable to restore from — drop the slot
        // entirely rather than leaving a stale ID token behind.
        None => save_persisted(None),
    }
}

fn identity_from_claims(
    claims: &CoreIdTokenClaims,
) -> (Option<String>, Option<String>, Option<String>) {
    (
        claims.email().map(|e| e.as_str().to_string()),
        claims
            .name()
            .and_then(|n| n.get(None).map(|s| s.as_str().to_string())),
        Some(claims.subject().as_str().to_string()),
    )
}

/// Unverified decode of `email`/`name`/`sub` from a JWT. Used only to
/// seed UI identity from our own persisted ID token at cold start; the
/// upcoming refresh either replaces these with verified claims or carries
/// them forward. Never trust the result for authorization decisions.
fn decode_id_claims_unverified(id_token: &str) -> (Option<String>, Option<String>, Option<String>) {
    let none = (None, None, None);
    let mut parts = id_token.split('.');
    let (_header, payload) = match (parts.next(), parts.next()) {
        (Some(h), Some(p)) => (h, p),
        _ => return none,
    };
    let Ok(bytes) = URL_SAFE_NO_PAD.decode(payload) else {
        return none;
    };
    #[derive(serde::Deserialize)]
    struct Claims {
        email: Option<String>,
        name: Option<String>,
        sub: Option<String>,
    }
    match serde_json::from_slice::<Claims>(&bytes) {
        Ok(c) => (c.email, c.name, c.sub),
        Err(_) => none,
    }
}
