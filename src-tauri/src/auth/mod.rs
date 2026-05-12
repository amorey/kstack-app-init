//! OAuth 2.1 / OIDC authentication against Hydra at `oauth.kstack.sh`.
//!
//! The desktop app is a **public client** with `token_endpoint_auth_method
//! =none`, so every flow uses PKCE and no client secret is ever embedded.
//! The redirect URI is a per-login loopback URL (`http://127.0.0.1:<port>
//! /oauth/callback`) per RFC 8252 §7.3 — see [`flow::Auth::login`] and
//! [`loopback`] for why we don't ride on the `kstack://` custom scheme
//! for OAuth.
//!
//! # Module layout
//! - [`tokens`] — in-memory token set + RT persistence (keychain in
//!   release builds, mode-0600 file in debug builds).
//! - [`loopback`] — ephemeral 127.0.0.1 listener that catches Hydra's
//!   redirect for the duration of one login.
//! - [`flow`] — login state machine; wraps `openidconnect::CoreClient`,
//!   which owns the protocol bits (PKCE, discovery, token endpoint, ID
//!   token verification).
//! - [`commands`] — Tauri `#[command]`s exposed to the frontend.
//!
//! # Threat model & design choices
//! - **Tokens never enter the renderer.** `auth_access_token` is the only
//!   bearer-revealing surface and exists for transitional callers.
//! - **Refresh token persisted; access token in memory.** Access tokens
//!   are short-lived; persisting them only widens the blast radius of a
//!   leak.
//! - **System browser, not embedded webview.** Honors the user's IdP
//!   session and avoids credential phishing surface.
//! - **PKCE + state verification.** `openidconnect` validates the ID
//!   token (RS256 signature against the JWKS, plus `iss`/`aud`/`exp`/
//!   `nonce`); the loopback handler validates `state` to prevent CSRF.

pub mod commands;
pub mod flow;
pub mod loopback;
pub mod tokens;

use once_cell::sync::Lazy;

/// Hydra issuer. The trailing slash matches what Hydra returns in its
/// `iss` claim and discovery doc — `openidconnect` is strict about
/// equality between the URL we pass in and the one the server publishes.
/// All other endpoints come from `{ISSUER}.well-known/openid-configuration`.
pub const ISSUER: &str = "https://oauth.kstack.sh/";

/// Public client ID registered via `hydra create oauth2-client`.
pub const CLIENT_ID: &str = "kstack-desktop";

/// Tauri event emitted when the silent-restore path on startup finishes
/// (success *or* failure). The renderer listens to flip its idle splash
/// off without polling.
pub const RESTORE_EVENT: &str = "auth:restore-complete";

/// Scopes requested at every login. `offline_access` is what makes Hydra
/// issue a refresh token; without it, sessions die when the access token
/// expires.
pub const SCOPES: &[&str] = &["openid", "offline_access", "email", "profile"];

/// Process-wide auth singleton. `Lazy` instead of `app.manage` so the
/// deep-link callback (which doesn't have an `AppHandle` in scope) can
/// reach it without threading state.
pub static AUTH: Lazy<flow::Auth> = Lazy::new(flow::Auth::new);
