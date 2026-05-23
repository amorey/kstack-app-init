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

//! The OIDC client, built on the `openidconnect` crate.
//!
//! `openidconnect` owns the security-critical protocol: discovery (with
//! issuer validation), JWKS retrieval, PKCE, and full ID-token verification —
//! signature against the discovered JWKS, plus `iss` / `aud` / `exp` / `azp`
//! and the `nonce`. This module is the thin glue around it: a cached
//! [`Provider`], the loopback-callback parser the crate does not cover, the
//! HTTP URL policy, and the provider metadata extension for the RFC 7009
//! revocation endpoint.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use openidconnect::core::{
    CoreAuthDisplay, CoreAuthenticationFlow, CoreClaimName, CoreClaimType, CoreClient,
    CoreClientAuthMethod, CoreGrantType, CoreIdTokenClaims, CoreJsonWebKey,
    CoreJweContentEncryptionAlgorithm, CoreJweKeyManagementAlgorithm, CoreResponseMode,
    CoreResponseType, CoreRevocableToken, CoreSubjectIdentifierType,
};
use openidconnect::{
    http, AdditionalProviderMetadata, AsyncHttpClient, AuthorizationCode, ClientId, CsrfToken,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, HttpClientError, HttpRequest, HttpResponse,
    IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, ProviderMetadata,
    RedirectUrl, RefreshToken, RevocationUrl, Scope,
};
use serde::{Deserialize, Serialize};

use super::config::AuthConfig;
use crate::error::{AppError, Result};

/// The `openidconnect` client once discovery has run: authorization and token
/// endpoints known, the rest unset — the typestate `from_provider_metadata`
/// produces. (`openidconnect` proves at compile time that an endpoint is set
/// before a request needs it; these six markers carry that proof.)
type OidcClient = CoreClient<
    EndpointSet,      // authorization
    EndpointNotSet,   // device authorization
    EndpointNotSet,   // introspection
    EndpointNotSet,   // revocation
    EndpointMaybeSet, // token
    EndpointMaybeSet, // userinfo
>;

/// Core OIDC metadata plus the RFC 7009 revocation endpoint. `openidconnect`
/// deliberately supports provider-specific fields through this extension
/// point, which lets discovery fetch and validate the document once.
type OidcProviderMetadata = ProviderMetadata<
    ProviderExtraMetadata,
    CoreAuthDisplay,
    CoreClientAuthMethod,
    CoreClaimName,
    CoreClaimType,
    CoreGrantType,
    CoreJweContentEncryptionAlgorithm,
    CoreJweKeyManagementAlgorithm,
    CoreJsonWebKey,
    CoreResponseMode,
    CoreResponseType,
    CoreSubjectIdentifierType,
>;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ProviderExtraMetadata {
    revocation_endpoint: Option<RevocationUrl>,
}

impl AdditionalProviderMetadata for ProviderExtraMetadata {}

/// A discovered OIDC provider — the configured `openidconnect` client plus the
/// shared no-redirect HTTP client. Built once per process by
/// [`Provider::discover`] and cached by the parent [`AuthService`](super::AuthService).
pub struct Provider {
    client: OidcClient,
    http: SecureHttpClient,
    /// The RFC 7009 revocation endpoint, captured at discovery time because
    /// the Core metadata alias does not include extension fields. `None` when
    /// the provider advertises no revocation support.
    revocation_endpoint: Option<RevocationUrl>,
}

/// The browser URL for one sign-in, plus the per-login secrets the caller must
/// hold to verify the callback and finish the exchange.
pub struct AuthorizeRequest {
    /// The provider's authorize URL, to open in the system browser.
    pub url: url::Url,
    /// CSRF token; the callback's `state` must equal its secret.
    pub csrf: CsrfToken,
    /// OIDC nonce; bound into the ID token and re-checked at code exchange.
    pub nonce: Nonce,
    /// PKCE verifier; sent with the code exchange.
    pub pkce_verifier: PkceCodeVerifier,
}

/// The outcome of a completed, fully verified sign-in.
pub struct VerifiedLogin {
    /// The bearer access token.
    pub access_token: String,
    /// The refresh token, if the provider issued one.
    pub refresh_token: Option<String>,
    /// How long the access token is valid for, from now.
    pub expires_in: Option<Duration>,
    /// The raw ID-token JWT, kept for the persisted bundle.
    pub id_token: String,
    /// Identity lifted from the verified ID token.
    pub identity: Identity,
}

/// Displayable identity, lifted from a verified ID token's claims.
pub struct Identity {
    /// The stable subject identifier — always present in a verified token.
    pub sub: String,
    /// The user's email, if the token carries one.
    pub email: Option<String>,
    /// The user's best display name, if the token carries one.
    pub name: Option<String>,
}

impl Provider {
    /// Runs OIDC discovery against the configured issuer and builds the client.
    ///
    /// `openidconnect` fetches `{issuer}/.well-known/openid-configuration` plus
    /// the JWKS, and validates that the document's `issuer` equals the URL we
    /// passed — the OIDC Discovery §4.3 integrity check.
    pub async fn discover(config: &AuthConfig) -> Result<Self> {
        let policy = validate_issuer_url(&config.issuer_url)?;
        let http = http_client(policy)?;
        let issuer = IssuerUrl::new(config.issuer_url.clone())
            .map_err(|e| AppError::Auth(format!("invalid issuer URL: {e}")))?;
        let metadata = OidcProviderMetadata::discover_async(issuer, &http)
            .await
            .map_err(|e| AppError::Auth(format!("OIDC discovery failed: {e}")))?;
        let revocation_endpoint = metadata.additional_metadata().revocation_endpoint.clone();
        if let Some(endpoint) = &revocation_endpoint {
            require_secure_parsed_url("revocation_endpoint", endpoint.url(), policy)?;
        }
        let client = CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(config.client_id.clone()),
            None,
        );
        Ok(Self {
            client,
            http,
            revocation_endpoint,
        })
    }

    /// Builds the browser authorize URL for an Authorization Code + PKCE
    /// sign-in. `openidconnect` mints the PKCE pair, CSRF `state`, and `nonce`;
    /// the caller holds the returned secrets to finish the flow.
    pub fn authorize(&self, redirect: &str, scopes: &[String]) -> Result<AuthorizeRequest> {
        let client = self.client_with_redirect(redirect)?;
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, csrf, nonce) = client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .add_scopes(scopes.iter().map(|s| Scope::new(s.clone())))
            .set_pkce_challenge(pkce_challenge)
            .url();
        require_secure_parsed_url("authorization URL", &url, self.http.policy())?;
        Ok(AuthorizeRequest {
            url,
            csrf,
            nonce,
            pkce_verifier,
        })
    }

    /// Exchanges the authorization `code` for tokens and has `openidconnect`
    /// fully verify the returned ID token — signature against the discovered
    /// JWKS, `iss` / `aud` / `exp` / `azp`, and that its `nonce` equals the one
    /// minted at [`authorize`](Self::authorize).
    pub async fn exchange_code(
        &self,
        redirect: &str,
        code: String,
        pkce_verifier: PkceCodeVerifier,
        nonce: &Nonce,
    ) -> Result<VerifiedLogin> {
        let client = self.client_with_redirect(redirect)?;
        let response = client
            .exchange_code(AuthorizationCode::new(code))
            .map_err(|e| AppError::Auth(format!("token endpoint is unavailable: {e}")))?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&self.http)
            .await
            .map_err(|e| AppError::Auth(format!("token exchange failed: {e}")))?;

        let id_token = response
            .extra_fields()
            .id_token()
            .ok_or_else(|| AppError::Auth("token response did not include an id_token".into()))?;
        let claims = id_token
            .claims(&self.client.id_token_verifier(), nonce)
            .map_err(|e| AppError::Auth(format!("id_token verification failed: {e}")))?;
        let identity = identity_from_claims(claims);

        Ok(VerifiedLogin {
            access_token: response.access_token().secret().clone(),
            refresh_token: response.refresh_token().map(|t| t.secret().clone()),
            expires_in: response.expires_in(),
            id_token: id_token.to_string(),
            identity,
        })
    }

    /// Best-effort RFC 7009 revocation of a refresh token, using the endpoint
    /// captured at discovery time. A provider that advertises no revocation
    /// endpoint makes this a no-op success.
    pub async fn revoke_refresh_token(&self, refresh_token: &str) -> Result<()> {
        let Some(endpoint) = &self.revocation_endpoint else {
            return Ok(());
        };
        let client = self.client.clone().set_revocation_url(endpoint.clone());
        client
            .revoke_token(CoreRevocableToken::RefreshToken(RefreshToken::new(
                refresh_token.to_string(),
            )))
            .map_err(|e| AppError::Auth(format!("token revocation is unavailable: {e}")))?
            .request_async(&self.http)
            .await
            .map_err(|e| AppError::Auth(format!("token revocation failed: {e}")))?;
        Ok(())
    }

    /// Clones the cached client with the per-login redirect URI pinned on it.
    /// `openidconnect`'s builders consume the client, so each request needs its
    /// own clone.
    fn client_with_redirect(&self, redirect: &str) -> Result<OidcClient> {
        Ok(self
            .client
            .clone()
            .set_redirect_uri(redirect_url(redirect)?))
    }
}

/// Extracts the `code` and `state` query parameters from a loopback callback
/// URL, surfacing any `error` the authorization server reported instead.
///
/// A duplicated `code`, `state`, or `error` parameter is rejected outright:
/// RFC 6749 §3.1 says request parameters MUST NOT appear more than once, and a
/// duplicate is a parameter-pollution / code-injection signal. An empty `code`
/// or `state` value is rejected for the same reason.
pub fn parse_callback(callback_url: &str, expected_redirect: &str) -> Result<(String, String)> {
    let parsed = url::Url::parse(callback_url)
        .map_err(|e| AppError::Auth(format!("invalid callback URL: {e}")))?;
    let expected = url::Url::parse(expected_redirect)
        .map_err(|e| AppError::Auth(format!("invalid expected redirect URL: {e}")))?;
    validate_callback_url(&parsed, &expected)?;

    let mut code = None;
    let mut state = None;
    let mut oauth_error = None;
    for (key, value) in parsed.query_pairs() {
        let slot = match key.as_ref() {
            "code" => &mut code,
            "state" => &mut state,
            "error" => &mut oauth_error,
            _ => continue,
        };
        if slot.is_some() {
            return Err(AppError::Auth(format!(
                "callback has a duplicated '{key}' parameter — possible injection"
            )));
        }
        *slot = Some(value.into_owned());
    }

    if let Some(err) = oauth_error {
        return Err(AppError::Auth(format!(
            "authorization server returned an error: {err}"
        )));
    }

    let code = require_non_empty(code, "code")?;
    let state = require_non_empty(state, "state")?;
    Ok((code, state))
}

/// Rejects a callback unless it landed on the exact loopback redirect URI this
/// process registered for the current sign-in.
fn validate_callback_url(callback: &url::Url, expected: &url::Url) -> Result<()> {
    if !callback.username().is_empty() || callback.password().is_some() {
        return Err(AppError::Auth(
            "callback URL must not contain credentials".into(),
        ));
    }
    if callback.fragment().is_some() {
        return Err(AppError::Auth(
            "callback URL must not contain a fragment".into(),
        ));
    }
    if callback.scheme() != expected.scheme()
        || callback.host_str() != expected.host_str()
        || callback.port() != expected.port()
        || callback.path() != expected.path()
    {
        return Err(AppError::Auth(
            "callback URL did not match the expected redirect URI".into(),
        ));
    }
    Ok(())
}

/// Unwraps a callback parameter, rejecting both a missing and an empty value.
fn require_non_empty(value: Option<String>, name: &str) -> Result<String> {
    match value {
        Some(v) if !v.is_empty() => Ok(v),
        Some(_) => Err(AppError::Auth(format!("callback has an empty '{name}'"))),
        None => Err(AppError::Auth(format!("callback is missing '{name}'"))),
    }
}

/// Lifts the displayable identity from verified ID-token claims, preferring the
/// full `name`, then `given_name`, then `preferred_username`.
fn identity_from_claims(claims: &CoreIdTokenClaims) -> Identity {
    let name = claims
        .name()
        .and_then(|n| n.get(None))
        .map(|n| n.as_str().to_string())
        .or_else(|| {
            claims
                .given_name()
                .and_then(|n| n.get(None))
                .map(|n| n.as_str().to_string())
        })
        .or_else(|| claims.preferred_username().map(|u| u.as_str().to_string()));
    Identity {
        sub: claims.subject().as_str().to_string(),
        email: claims.email().map(|e| e.as_str().to_string()),
        name,
    }
}

/// The HTTP client `openidconnect` drives discovery and token requests through.
/// It is built to **not** follow redirects — a 30x on a token or discovery
/// response could otherwise carry the code or token to an unintended host.
fn http_client(policy: SecureHttpPolicy) -> Result<SecureHttpClient> {
    let inner = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(AppError::from)?;
    Ok(SecureHttpClient { inner, policy })
}

/// Per-issuer policy for HTTP requests. Remote providers must use HTTPS for
/// every OIDC/OAuth endpoint; loopback HTTP is kept only for local development
/// issuers.
#[derive(Clone, Copy, Debug)]
struct SecureHttpPolicy {
    allow_loopback_http: bool,
}

impl SecureHttpPolicy {
    fn allows_http_for(self, url: &url::Url) -> bool {
        self.allow_loopback_http && is_loopback_http(url)
    }
}

/// `openidconnect` accepts any implementation of `AsyncHttpClient`; this one
/// wraps `reqwest` with the URL policy this native-client flow requires.
#[derive(Clone)]
struct SecureHttpClient {
    inner: reqwest::Client,
    policy: SecureHttpPolicy,
}

impl SecureHttpClient {
    fn policy(&self) -> SecureHttpPolicy {
        self.policy
    }
}

#[derive(Debug, thiserror::Error)]
enum SecureHttpError {
    #[error("{0}")]
    UrlPolicy(String),
    #[error(transparent)]
    Http(#[from] HttpClientError<reqwest::Error>),
}

impl<'c> AsyncHttpClient<'c> for SecureHttpClient {
    type Error = SecureHttpError;
    type Future = Pin<
        Box<dyn Future<Output = std::result::Result<HttpResponse, Self::Error>> + Send + Sync + 'c>,
    >;

    fn call(&'c self, request: HttpRequest) -> Self::Future {
        Box::pin(async move {
            let request_url = url::Url::parse(&request.uri().to_string()).map_err(|e| {
                SecureHttpError::UrlPolicy(format!("invalid OAuth HTTP request URL: {e}"))
            })?;
            check_secure_url("OAuth HTTP request", &request_url, self.policy)
                .map_err(SecureHttpError::UrlPolicy)?;

            let request: reqwest::Request = request
                .try_into()
                .map_err(Box::new)
                .map_err(HttpClientError::<reqwest::Error>::from)?;
            let response = self
                .inner
                .execute(request)
                .await
                .map_err(Box::new)
                .map_err(HttpClientError::<reqwest::Error>::from)?;

            let mut builder = http::Response::builder()
                .status(response.status())
                .version(response.version());

            for (name, value) in response.headers().iter() {
                builder = builder.header(name, value);
            }

            let body = response
                .bytes()
                .await
                .map_err(Box::new)
                .map_err(HttpClientError::<reqwest::Error>::from)?
                .to_vec();
            builder
                .body(body)
                .map_err(HttpClientError::Http)
                .map_err(SecureHttpError::from)
        })
    }
}

/// Builds an `openidconnect` redirect URL, mapping a parse failure to [`AppError`].
fn redirect_url(redirect: &str) -> Result<RedirectUrl> {
    RedirectUrl::new(redirect.to_string())
        .map_err(|e| AppError::Auth(format!("invalid redirect URL: {e}")))
}

/// Validates the configured issuer and returns the HTTP policy it implies.
fn validate_issuer_url(raw: &str) -> Result<SecureHttpPolicy> {
    let parsed =
        url::Url::parse(raw).map_err(|e| AppError::Auth(format!("invalid issuer URL: {e}")))?;
    if parsed.query().is_some() {
        return Err(AppError::Auth("issuer URL must not contain a query".into()));
    }
    let bootstrap_policy = SecureHttpPolicy {
        allow_loopback_http: true,
    };
    require_secure_parsed_url("issuer", &parsed, bootstrap_policy)?;
    Ok(SecureHttpPolicy {
        allow_loopback_http: is_loopback_http(&parsed),
    })
}

fn require_secure_parsed_url(
    label: &str,
    parsed: &url::Url,
    policy: SecureHttpPolicy,
) -> Result<()> {
    check_secure_url(label, parsed, policy).map_err(AppError::Auth)
}

/// Rejects non-HTTPS OAuth URLs, except for HTTP loopback URLs when the issuer
/// itself is loopback HTTP. OAuth endpoint URLs may carry a query, but never
/// credentials or fragments.
fn check_secure_url(
    label: &str,
    parsed: &url::Url,
    policy: SecureHttpPolicy,
) -> std::result::Result<(), String> {
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!("{label} must not contain credentials"));
    }
    if parsed.fragment().is_some() {
        return Err(format!("{label} must not contain a fragment"));
    }
    if parsed.scheme() == "https" || (parsed.scheme() == "http" && policy.allows_http_for(parsed)) {
        return Ok(());
    }
    Err(format!("{label} must use https, got {:?}", parsed.scheme()))
}

fn is_loopback_http(parsed: &url::Url) -> bool {
    parsed.scheme() == "http" && is_loopback_host(parsed)
}

fn is_loopback_host(parsed: &url::Url) -> bool {
    match parsed.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain("localhost")) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_REDIRECT: &str = "http://127.0.0.1:9999/oauth/callback";

    fn callback(query: &str) -> String {
        format!("{EXPECTED_REDIRECT}?{query}")
    }

    #[test]
    fn parse_callback_extracts_code_and_state() {
        let (code, state) = parse_callback(&callback("code=abc123&state=xyz"), EXPECTED_REDIRECT)
            .expect("should parse");
        assert_eq!(code, "abc123");
        assert_eq!(state, "xyz");
    }

    #[test]
    fn parse_callback_surfaces_an_oauth_error() {
        let err = parse_callback(&callback("error=access_denied"), EXPECTED_REDIRECT)
            .expect_err("should be an error");
        assert!(err.to_string().contains("access_denied"));
    }

    #[test]
    fn parse_callback_rejects_a_missing_code() {
        assert!(parse_callback(&callback("state=xyz"), EXPECTED_REDIRECT).is_err());
    }

    #[test]
    fn parse_callback_rejects_a_duplicated_code() {
        // Parameter pollution: a second `code` must not be silently dropped.
        let err = parse_callback(
            &callback("code=attacker&code=real&state=xyz"),
            EXPECTED_REDIRECT,
        )
        .expect_err("a duplicated code must be rejected");
        assert!(err.to_string().contains("duplicated"));
    }

    #[test]
    fn parse_callback_rejects_a_duplicated_state() {
        assert!(
            parse_callback(&callback("code=abc&state=one&state=two"), EXPECTED_REDIRECT).is_err()
        );
    }

    #[test]
    fn parse_callback_rejects_an_empty_code() {
        let err = parse_callback(&callback("code=&state=xyz"), EXPECTED_REDIRECT)
            .expect_err("an empty code must be rejected");
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn parse_callback_rejects_an_empty_state() {
        assert!(parse_callback(&callback("code=abc&state="), EXPECTED_REDIRECT).is_err());
    }

    #[test]
    fn parse_callback_rejects_a_malformed_url() {
        assert!(parse_callback("not a url", EXPECTED_REDIRECT).is_err());
    }

    #[test]
    fn parse_callback_rejects_a_wrong_path() {
        let err = parse_callback(
            "http://127.0.0.1:9999/other?code=abc&state=xyz",
            EXPECTED_REDIRECT,
        )
        .expect_err("the path must match the registered redirect URI");
        assert!(err.to_string().contains("expected redirect"));
    }

    #[test]
    fn parse_callback_rejects_a_wrong_host() {
        assert!(parse_callback(
            "http://localhost:9999/oauth/callback?code=abc&state=xyz",
            EXPECTED_REDIRECT,
        )
        .is_err());
    }

    #[test]
    fn parse_callback_rejects_a_wrong_port() {
        assert!(parse_callback(
            "http://127.0.0.1:10000/oauth/callback?code=abc&state=xyz",
            EXPECTED_REDIRECT,
        )
        .is_err());
    }

    #[test]
    fn parse_callback_rejects_credentials() {
        assert!(parse_callback(
            "http://user@127.0.0.1:9999/oauth/callback?code=abc&state=xyz",
            EXPECTED_REDIRECT,
        )
        .is_err());
    }

    #[test]
    fn parse_callback_rejects_a_fragment() {
        assert!(parse_callback(
            "http://127.0.0.1:9999/oauth/callback?code=abc&state=xyz#frag",
            EXPECTED_REDIRECT,
        )
        .is_err());
    }

    #[test]
    fn parse_callback_ignores_unrelated_parameters() {
        // Extra params a browser or the IdP may append must not derail parsing.
        let (code, state) = parse_callback(
            "http://127.0.0.1:9999/oauth/callback?code=abc&state=xyz&scope=openid&iss=x",
            EXPECTED_REDIRECT,
        )
        .expect("should parse");
        assert_eq!(code, "abc");
        assert_eq!(state, "xyz");
    }

    #[test]
    fn validate_issuer_url_accepts_https() {
        let policy = validate_issuer_url("https://oauth.example.test/").expect("https is allowed");
        assert!(!policy.allow_loopback_http);
    }

    #[test]
    fn validate_issuer_url_allows_loopback_http() {
        // A locally hosted dev IdP over http on the loopback interface.
        let localhost = validate_issuer_url("http://localhost:4444/").expect("localhost ok");
        let loopback_ip = validate_issuer_url("http://127.0.0.1:4444/").expect("127.0.0.1 ok");
        assert!(localhost.allow_loopback_http);
        assert!(loopback_ip.allow_loopback_http);
    }

    #[test]
    fn validate_issuer_url_rejects_plaintext_remote() {
        let err = validate_issuer_url("http://oauth.example.test/")
            .expect_err("a remote http URL must be rejected");
        assert!(err.to_string().contains("https"));
    }

    #[test]
    fn validate_issuer_url_rejects_query_and_fragment() {
        assert!(validate_issuer_url("https://oauth.example.test/?x=1").is_err());
        assert!(validate_issuer_url("https://oauth.example.test/#frag").is_err());
    }

    #[test]
    fn secure_url_policy_rejects_http_when_issuer_is_https() {
        let endpoint = url::Url::parse("http://127.0.0.1:4444/token").expect("valid URL");
        let policy = SecureHttpPolicy {
            allow_loopback_http: false,
        };
        let err = require_secure_parsed_url("token_endpoint", &endpoint, policy)
            .expect_err("https issuers must not downgrade to http endpoints");
        assert!(err.to_string().contains("https"));
    }
}
