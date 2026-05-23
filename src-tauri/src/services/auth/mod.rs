// Copyright 2026 The Kubetail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The OAuth authentication service.
//!
//! [`AuthService`] is the host's single source of truth for who is signed in.
//! It is held by `AppState` (parallel to `SidecarService`), owns its
//! [`AuthConfig`] and the in-memory session, and exposes the sign-in lifecycle
//! as methods. The renderer never holds tokens — it mirrors the [`Session`]
//! and triggers these methods through the `auth_*` commands.
//!
//! Auth is the **Authorization Code + PKCE** flow for a public native client:
//! a loopback `127.0.0.1` listener catches the browser redirect, the code is
//! exchanged for tokens, and the bundle is persisted to the OS keychain.

mod config;
mod oauth;

pub use config::AuthConfig;

use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager};
use tauri_plugin_opener::OpenerExt;
use zeroize::Zeroize;

use crate::error::{AppError, Result};
use crate::state::AppState;

/// Tauri event broadcast to **all** windows on every post-startup auth change
/// (login / logout). Mirrored by `SESSION_CHANGED_EVENT` in `src/lib/auth.tsx`.
pub const SESSION_CHANGED_EVENT: &str = "auth:session-changed";

/// Tauri event emitted exactly once at startup, after [`AuthService::try_restore`]
/// resolves. Mirrored by `SESSION_RESOLVED_EVENT` in `src/lib/auth.tsx`.
pub const SESSION_RESOLVED_EVENT: &str = "auth:session-resolved";

/// Keychain service name — the app's bundle identifier.
const KEYRING_SERVICE: &str = "sh.kstack.app";

/// Keychain account name. Fixed: the app holds one signed-in identity.
const KEYRING_ACCOUNT: &str = "oauth-tokens";

/// How long to wait for the browser redirect before treating a sign-in as
/// abandoned.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(180);

/// Error message for a sign-in that a newer login click cancelled. The
/// renderer matches on the `superseded` substring to swallow it silently
/// rather than surfacing it as a failure.
const SUPERSEDED_MSG: &str = "sign-in superseded by a newer attempt";

/// The page the browser lands on after a successful redirect to our loopback
/// listener. `tauri_plugin_oauth` injects its own `<script>` right after the
/// `<head>` tag (it posts the callback URL back to the plugin), so an explicit
/// `<head>` must be present or the plugin falls back to prepending one and logs
/// a warning.
const CALLBACK_HTML: &str = "<!doctype html>\
<html lang=en>\
<head>\
<meta charset=utf-8>\
<title>kstack — signed in</title>\
<style>body{font:16px system-ui,sans-serif;text-align:center;padding:4em;color:#222}h1{font-weight:600;margin:0 0 .25em}p{color:#666}</style>\
</head>\
<body>\
<h1>Signed in</h1>\
<p>You can close this tab and return to the app.</p>\
</body>\
</html>";

/// The session snapshot handed to the renderer.
///
/// This is the wire type for `auth_login` / `auth_status` and the payload of
/// the [`SESSION_CHANGED_EVENT`] / [`SESSION_RESOLVED_EVENT`] events. It deliberately
/// carries no tokens.
#[derive(Debug, Clone, Serialize)]
pub struct Session {
    /// Whether a user is currently signed in.
    pub authenticated: bool,
    /// The signed-in user's email, if known.
    pub email: Option<String>,
    /// The signed-in user's display name, if known.
    pub name: Option<String>,
    /// The signed-in user's stable subject identifier, if known.
    pub sub: Option<String>,
}

impl Session {
    /// The signed-out session.
    pub fn anonymous() -> Self {
        Self {
            authenticated: false,
            email: None,
            name: None,
            sub: None,
        }
    }
}

impl From<&TokenBundle> for Session {
    fn from(bundle: &TokenBundle) -> Self {
        Self {
            authenticated: true,
            email: bundle.email.clone(),
            name: bundle.name.clone(),
            sub: bundle.sub.clone(),
        }
    }
}

/// The full set of OAuth tokens plus cached identity, persisted to the
/// keychain as a single JSON blob.
#[derive(Clone, Serialize, Deserialize)]
pub struct TokenBundle {
    /// The bearer access token.
    pub access_token: String,
    /// The refresh token, if the provider issued one.
    pub refresh_token: Option<String>,
    /// Absolute UTC instant the access token expires (survives restarts).
    pub access_expires_at: Option<DateTime<Utc>>,
    /// The raw OIDC ID-token JWT.
    pub id_token: Option<String>,
    /// Cached email claim, so a session restored without re-decoding the JWT
    /// still has an identity.
    pub email: Option<String>,
    /// Cached display-name claim.
    pub name: Option<String>,
    /// Cached subject claim.
    pub sub: Option<String>,
}

/// Redacts the three bearer-token fields so a stray `{:?}` — in a log line, a
/// panic message, an error chain — can never spill credentials. Identity fields
/// stay visible: they are not secret (the renderer already holds them via
/// [`Session`]) and keep the output useful for diagnostics.
impl std::fmt::Debug for TokenBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenBundle")
            .field("access_token", &"<redacted>")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("access_expires_at", &self.access_expires_at)
            .field("id_token", &self.id_token.as_ref().map(|_| "<redacted>"))
            .field("email", &self.email)
            .field("name", &self.name)
            .field("sub", &self.sub)
            .finish()
    }
}

impl Drop for TokenBundle {
    fn drop(&mut self) {
        self.access_token.zeroize();
        self.refresh_token.zeroize();
        self.id_token.zeroize();
    }
}

/// The host's OAuth authentication service.
pub struct AuthService {
    /// OAuth client configuration (client id, issuer, scopes).
    config: AuthConfig,
    /// The current session's tokens, or `None` when signed out.
    bundle: Mutex<Option<TokenBundle>>,
    /// Canceller for the in-flight sign-in, if any. A new `login` installs its
    /// own sender here and signals the one it displaces, so the latest click
    /// supersedes any earlier attempt instead of being rejected.
    cancel_login: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// The discovered OIDC provider (client + endpoints + JWKS), built and
    /// cached on first use.
    provider: tokio::sync::OnceCell<oauth::Provider>,
}

impl AuthService {
    /// Creates the service with the given OAuth configuration, signed out.
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            bundle: Mutex::new(None),
            cancel_login: Mutex::new(None),
            provider: tokio::sync::OnceCell::new(),
        }
    }

    /// Returns the current session snapshot.
    pub fn current_session(&self) -> Session {
        match &*self.lock_bundle() {
            Some(bundle) => Session::from(bundle),
            None => Session::anonymous(),
        }
    }

    /// Runs an interactive sign-in: opens the provider's login page in the
    /// system browser, exchanges the returned code for tokens, persists them,
    /// and broadcasts the new session to every window.
    pub async fn login(&self, app: &AppHandle) -> Result<Session> {
        // Supersede any in-flight sign-in: install our canceller and signal
        // the one we displace. Its `complete_login` aborts with `SUPERSEDED_MSG`
        // — there can only ever be one live loopback flow, the most recent.
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        if let Some(previous) = self.lock_cancel().replace(cancel_tx) {
            // `Err` just means that flow already finished — nothing to cancel.
            let _ = previous.send(());
        }

        let bundle = self.run_login(app, cancel_rx).await?;
        let session = Session::from(&bundle);

        if let Err(e) = persist_bundle(bundle.clone()).await {
            // Non-fatal: the session is live for this run, it just will not
            // survive a restart.
            tracing::warn!(error = %e, "failed to persist OAuth tokens to the keychain");
        }

        self.set_bundle(Some(bundle));
        emit_session(app, SESSION_CHANGED_EVENT, &session);
        Ok(session)
    }

    /// Signs the user out: revokes the tokens, clears local + persisted state,
    /// and broadcasts the anonymous session to every window.
    pub async fn logout(&self, app: &AppHandle) -> Result<()> {
        let Some(previous) = self.lock_bundle().take() else {
            // Already signed out — nothing to revoke, clear, or broadcast.
            return Ok(());
        };

        // Best-effort revocation — discovery or the revocation call can fail
        // (offline, no revocation endpoint); none of that must block logout.
        if let Some(refresh_token) = previous.refresh_token.as_deref() {
            if let Err(e) = self.revoke(refresh_token).await {
                tracing::warn!(error = %e, "token revocation failed; continuing logout");
            }
        }

        if let Err(e) = delete_bundle().await {
            tracing::warn!(error = %e, "failed to delete stored OAuth tokens");
        }

        emit_session(app, SESSION_CHANGED_EVENT, &Session::anonymous());
        Ok(())
    }

    /// Spawns the startup session restore as a background task.
    ///
    /// Call once after the owning [`AppState`] has been managed. A detached
    /// `'static` task cannot borrow the service (it now lives inside
    /// `AppState`), so the task resolves it back out through the handle.
    pub fn spawn_restore(app: &AppHandle) {
        let app = app.clone();
        tauri::async_runtime::spawn(async move {
            let state = app.state::<AppState>();
            state.auth.try_restore(&app).await;
        });
    }

    /// Restores a persisted session at startup, then emits the one-shot
    /// [`SESSION_RESOLVED_EVENT`] the renderer waits for.
    async fn try_restore(&self, app: &AppHandle) {
        let session = match load_bundle().await {
            Ok(Some(bundle)) => {
                let session = Session::from(&bundle);
                self.set_bundle(Some(bundle));
                session
            }
            Ok(None) => Session::anonymous(),
            Err(e) => {
                tracing::warn!(error = %e, "failed to restore the OAuth session from the keychain");
                Session::anonymous()
            }
        };
        emit_session(app, SESSION_RESOLVED_EVENT, &session);
    }

    /// Returns the discovered OIDC provider, running discovery against the
    /// issuer's `.well-known` endpoint on first use and caching it thereafter.
    async fn provider(&self) -> Result<&oauth::Provider> {
        self.provider
            .get_or_try_init(|| oauth::Provider::discover(&self.config))
            .await
    }

    /// Revokes a refresh token at the provider's revocation endpoint.
    async fn revoke(&self, refresh_token: &str) -> Result<()> {
        self.provider()
            .await?
            .revoke_refresh_token(refresh_token)
            .await
    }

    /// Drives the loopback listener for the duration of a sign-in, cancelling
    /// it however the flow ends. `cancel_rx` fires when a newer `login`
    /// supersedes this one.
    async fn run_login(
        &self,
        app: &AppHandle,
        cancel_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<TokenBundle> {
        // The redirect arrives on a listener thread; bridge it to this async
        // flow with a oneshot channel.
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let mut tx = Some(tx);
        let port = tauri_plugin_oauth::start_with_config(
            tauri_plugin_oauth::OauthConfig {
                ports: None,
                response: Some(CALLBACK_HTML.into()),
            },
            move |url| {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(url);
                }
            },
        )
        .map_err(|e| AppError::Auth(format!("failed to start the loopback listener: {e}")))?;

        let outcome = self.complete_login(app, port, rx, cancel_rx).await;
        let _ = tauri_plugin_oauth::cancel(port);
        outcome
    }

    /// The sign-in steps that run once the loopback `port` is bound.
    async fn complete_login(
        &self,
        app: &AppHandle,
        port: u16,
        rx: tokio::sync::oneshot::Receiver<String>,
        cancel_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<TokenBundle> {
        let redirect = format!("http://127.0.0.1:{port}/oauth/callback");
        let provider = self.provider().await?;

        // `openidconnect` mints the PKCE pair, CSRF `state`, and `nonce`.
        let request = provider.authorize(&redirect, &self.config.scopes)?;
        app.opener()
            .open_url(request.url.as_str(), None::<&str>)
            .map_err(|e| AppError::Auth(format!("failed to open the browser: {e}")))?;

        // Race the browser redirect against a supersede signal from a newer
        // `login`. `biased` checks the cancel first so a fresh click wins even
        // if the callback lands in the same poll.
        let mut callback_url = tokio::select! {
            biased;
            _ = cancel_rx => return Err(AppError::Auth(SUPERSEDED_MSG.into())),
            result = tokio::time::timeout(LOGIN_TIMEOUT, rx) => match result {
                Err(_elapsed) => {
                    return Err(AppError::Auth(
                        "sign-in timed out waiting for the browser redirect".into(),
                    ))
                }
                Ok(Err(_closed)) => return Err(AppError::Auth("sign-in was cancelled".into())),
                Ok(Ok(url)) => url,
            },
        };

        let parsed_callback = oauth::parse_callback(&callback_url, &redirect);
        callback_url.zeroize();
        let (code, state) = parsed_callback?;
        if state != *request.csrf.secret() {
            return Err(AppError::Auth(
                "OAuth state mismatch — possible CSRF, sign-in aborted".into(),
            ));
        }

        // The exchange has `openidconnect` verify the ID token end to end —
        // signature against the discovered JWKS, `iss`/`aud`/`exp`/`azp`, and
        // that the `nonce` matches the one minted above.
        let login = provider
            .exchange_code(&redirect, code, request.pkce_verifier, &request.nonce)
            .await?;

        Ok(build_bundle(login))
    }

    /// Locks the in-memory bundle, recovering from a poisoned lock.
    fn lock_bundle(&self) -> std::sync::MutexGuard<'_, Option<TokenBundle>> {
        self.bundle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Locks the in-flight sign-in canceller, recovering from a poisoned lock.
    fn lock_cancel(&self) -> std::sync::MutexGuard<'_, Option<tokio::sync::oneshot::Sender<()>>> {
        self.cancel_login
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Replaces the in-memory bundle.
    fn set_bundle(&self, value: Option<TokenBundle>) {
        *self.lock_bundle() = value;
    }
}

/// Assembles a [`TokenBundle`] from a completed, verified sign-in, converting
/// the relative access-token expiry to an absolute instant.
fn build_bundle(login: oauth::VerifiedLogin) -> TokenBundle {
    let access_expires_at = login
        .expires_in
        .and_then(|d| chrono::Duration::from_std(d).ok())
        .map(|d| Utc::now() + d);

    TokenBundle {
        access_token: login.access_token,
        refresh_token: login.refresh_token,
        access_expires_at,
        id_token: Some(login.id_token),
        email: login.identity.email,
        name: login.identity.name,
        sub: Some(login.identity.sub),
    }
}

/// Broadcasts a session event to every window.
fn emit_session(app: &AppHandle, event: &str, session: &Session) {
    if let Err(e) = app.emit(event, session) {
        tracing::error!(error = %e, event, "failed to emit an auth session event");
    }
}

/// Runs a blocking keychain operation on the blocking thread pool.
///
/// The `keyring` API is synchronous and an OS keychain call can be slow (and on
/// macOS may surface an auth prompt), so it must not run on an async runtime
/// worker.
async fn on_keychain<T, F>(operation: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tauri::async_runtime::spawn_blocking(operation).await?
}

/// Opens the single keychain entry that stores the token bundle.
fn keyring_entry() -> Result<keyring_core::Entry> {
    keyring_core::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT).map_err(AppError::from)
}

/// Writes the token bundle to the keychain.
async fn persist_bundle(bundle: TokenBundle) -> Result<()> {
    on_keychain(move || {
        let mut json = serde_json::to_string(&bundle)?;
        // Zeroize the cleartext blob on every path — including a failed
        // keychain write — so it never lingers in freed heap.
        let result =
            keyring_entry().and_then(|entry| entry.set_password(&json).map_err(AppError::from));
        json.zeroize();
        result
    })
    .await
}

/// Reads the token bundle from the keychain.
///
/// A missing entry yields `Ok(None)`; a corrupt entry is discarded (also
/// `Ok(None)`) so a bad blob never wedges startup.
async fn load_bundle() -> Result<Option<TokenBundle>> {
    on_keychain(|| {
        let entry = keyring_entry()?;
        match entry.get_password() {
            Ok(mut json) => {
                let parsed = serde_json::from_str::<TokenBundle>(&json);
                json.zeroize();
                match parsed {
                    Ok(bundle) => Ok(Some(bundle)),
                    Err(e) => {
                        tracing::warn!(error = %e, "stored OAuth tokens are corrupt; discarding them");
                        let _ = entry.delete_credential();
                        Ok(None)
                    }
                }
            }
            Err(keyring_core::Error::NoEntry) => Ok(None),
            Err(e) => Err(AppError::from(e)),
        }
    })
    .await
}

/// Deletes the keychain entry; a missing entry counts as success.
async fn delete_bundle() -> Result<()> {
    on_keychain(|| match keyring_entry()?.delete_credential() {
        Ok(()) | Err(keyring_core::Error::NoEntry) => Ok(()),
        Err(e) => Err(AppError::from(e)),
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bundle() -> TokenBundle {
        TokenBundle {
            access_token: "access".into(),
            refresh_token: Some("refresh".into()),
            access_expires_at: Some(Utc::now()),
            id_token: Some("id-jwt".into()),
            email: Some("ada@example.com".into()),
            name: Some("Ada Lovelace".into()),
            sub: Some("user-1".into()),
        }
    }

    #[test]
    fn token_bundle_round_trips_through_json() {
        let bundle = sample_bundle();
        let json = serde_json::to_string(&bundle).expect("should serialize");
        let restored: TokenBundle = serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(restored.access_token, "access");
        assert_eq!(restored.refresh_token.as_deref(), Some("refresh"));
        assert_eq!(restored.sub.as_deref(), Some("user-1"));
        assert_eq!(restored.access_expires_at, bundle.access_expires_at);
    }

    #[test]
    fn session_from_bundle_is_authenticated() {
        let session = Session::from(&sample_bundle());
        assert!(session.authenticated);
        assert_eq!(session.email.as_deref(), Some("ada@example.com"));
        assert_eq!(session.name.as_deref(), Some("Ada Lovelace"));
    }

    #[test]
    fn anonymous_session_serializes_with_nulls() {
        let session = Session::anonymous();
        assert!(!session.authenticated);

        let json = serde_json::to_value(&session).expect("should serialize");
        assert_eq!(json["authenticated"], serde_json::json!(false));
        assert_eq!(json["email"], serde_json::Value::Null);
        assert_eq!(json["sub"], serde_json::Value::Null);
    }

    #[test]
    fn debug_never_prints_token_material() {
        let bundle = TokenBundle {
            access_token: "ACCESS-SECRET".into(),
            refresh_token: Some("REFRESH-SECRET".into()),
            access_expires_at: None,
            id_token: Some("IDTOKEN-SECRET".into()),
            email: Some("ada@example.com".into()),
            name: None,
            sub: Some("user-1".into()),
        };
        let printed = format!("{bundle:?}");
        assert!(
            !printed.contains("SECRET"),
            "token material leaked: {printed}"
        );
        assert!(printed.contains("<redacted>"));
        // Identity stays visible — it is not secret and aids diagnostics.
        assert!(printed.contains("ada@example.com"));
        assert!(printed.contains("user-1"));
    }
}
