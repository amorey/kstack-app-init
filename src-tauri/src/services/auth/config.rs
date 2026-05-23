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

//! OAuth/OIDC client configuration for [`AuthService`](super::AuthService).
//!
//! Only the client identifier and the provider's issuer URL are configured
//! here — the actual endpoints (authorization, token, revocation) are fetched
//! at runtime from the issuer's OIDC discovery document. The values are not
//! secret (this is a public, PKCE-protected native client) and can be
//! overridden per-process with `KSTACK_OAUTH_*` environment variables.

// TODO: confirm the client_id registered with the Kstack Cloud OAuth server.
const DEFAULT_CLIENT_ID: &str = "kstack-desktop";

/// Issuer URL of the Kstack Cloud OAuth provider (Ory Hydra). OIDC discovery
/// resolves the concrete endpoints from `{issuer}/.well-known/openid-configuration`.
const DEFAULT_ISSUER_URL: &str = "https://oauth.kstack.sh/";

/// The OAuth scopes requested at sign-in.
///
/// `openid` makes the token endpoint return an ID token (for identity);
/// `offline_access` requests a refresh token.
const SCOPES: [&str; 4] = ["openid", "email", "profile", "offline_access"];

/// OAuth/OIDC client configuration.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// The public OAuth client identifier.
    pub client_id: String,
    /// The OIDC issuer URL; endpoints are discovered from it at runtime.
    pub issuer_url: String,
    /// The OAuth scopes to request.
    pub scopes: Vec<String>,
}

impl AuthConfig {
    /// Builds the config from compile-time defaults, applying any
    /// `KSTACK_OAUTH_*` environment overrides.
    pub fn from_env() -> Self {
        Self {
            client_id: env_or("KSTACK_OAUTH_CLIENT_ID", DEFAULT_CLIENT_ID),
            issuer_url: env_or("KSTACK_OAUTH_ISSUER_URL", DEFAULT_ISSUER_URL),
            scopes: SCOPES.iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// Reads an environment variable, falling back to `default` when it is unset.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
