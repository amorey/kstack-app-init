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

//! Tests for `Auth::logout`. We stand up a `wiremock` server, point
//! `Auth` at it via `with_issuer`, and assert the RFC 7009 revocation
//! shape on the wire.
//!
//! The contract worth protecting is the ordering and fan-out — local
//! state cleared *first*, both tokens revoked, and a network failure on
//! revocation still leaves the user signed out locally.
//!
//! Caveat: `Auth::logout` calls `save_refresh_token(None)` which, in
//! debug builds (i.e. `cargo test`), targets the real per-user data
//! dir at `dirs::data_dir().join("sh.kstack.app/oauth-refresh.dev")`.
//! Removing a non-existent file is a no-op, so these tests don't
//! clobber anything — but if a developer has an active dev session it
//! will be signed out. Same trade-off the existing `tokens.rs` test
//! already accepts.

use super::*;
use crate::auth::tokens::Tokens;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// Minimum OIDC discovery doc that satisfies `Auth::logout` (which only
/// reads `revocation_endpoint`). Other endpoints are stubs — none of
/// these tests route through `client()`.
fn discovery_body(issuer: &str, revocation_endpoint: Option<&str>) -> String {
    let extra = match revocation_endpoint {
        Some(url) => format!(r#","revocation_endpoint":"{url}""#),
        None => String::new(),
    };
    format!(
        r#"{{
          "issuer":"{issuer}",
          "authorization_endpoint":"{issuer}oauth2/auth",
          "token_endpoint":"{issuer}oauth2/token",
          "jwks_uri":"{issuer}.well-known/jwks.json",
          "response_types_supported":["code"],
          "subject_types_supported":["public"],
          "id_token_signing_alg_values_supported":["RS256"]
          {extra}
        }}"#
    )
}

fn make_tokens(access: &str, refresh: Option<&str>) -> Tokens {
    Tokens {
        access_token: access.into(),
        refresh_token: refresh.map(String::from),
        id_token: None,
        issued_at: 0,
        // Far future so nothing about "is expired" affects the test.
        expires_at: u64::MAX,
        email: None,
        name: None,
        sub: None,
    }
}

/// `wiremock` exposes the server at a URL without a trailing slash; the
/// issuer string we feed `Auth` does need one (we concatenate
/// `.well-known/openid-configuration` straight onto it).
fn issuer_for(server: &MockServer) -> String {
    format!("{}/", server.uri())
}

/// Mount the discovery endpoint with the given (optional) revocation URL.
/// Returns the URL so callers can mount matching revoke mocks against it.
async fn mount_discovery(server: &MockServer, with_revocation: bool) -> Option<String> {
    let revoke = with_revocation.then(|| format!("{}/oauth2/revoke", server.uri()));
    Mock::given(method("GET"))
        .and(path("/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            discovery_body(&issuer_for(server), revoke.as_deref()),
            "application/json",
        ))
        .mount(server)
        .await;
    revoke
}

/// Build an `Auth` pointed at `server` and seed it with a token pair so
/// `logout()` has something to revoke.
async fn auth_with_session(server: &MockServer, access: &str, refresh: Option<&str>) -> Auth {
    let auth = Auth::with_issuer(issuer_for(server));
    auth.set_tokens_for_test(make_tokens(access, refresh)).await;
    auth
}

#[tokio::test]
async fn revokes_both_tokens_and_clears_local_state() {
    let server = MockServer::start().await;
    mount_discovery(&server, true).await;

    // One mock per token kind: matching on the form body lets us assert
    // both `token=` and `token_type_hint=` in one matcher, and gives a
    // distinct call count per token so we can't get a false positive
    // from the AT request masquerading as the RT one.
    Mock::given(method("POST"))
        .and(path("/oauth2/revoke"))
        .and(body_string_contains("token=rt-secret"))
        .and(body_string_contains("token_type_hint=refresh_token"))
        .and(body_string_contains(format!("client_id={CLIENT_ID}")))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/oauth2/revoke"))
        .and(body_string_contains("token=at-secret"))
        .and(body_string_contains("token_type_hint=access_token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let auth = auth_with_session(&server, "at-secret", Some("rt-secret")).await;
    auth.logout().await.expect("logout");

    assert!(!auth.has_tokens().await, "local tokens must be cleared");
    // Explicit verify so a count mismatch points here rather than into
    // wiremock's Drop impl.
    server.verify().await;
}

#[tokio::test]
async fn local_state_cleared_even_when_revocation_endpoint_500s() {
    let server = MockServer::start().await;
    mount_discovery(&server, true).await;
    Mock::given(method("POST"))
        .and(path("/oauth2/revoke"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let auth = auth_with_session(&server, "at", Some("rt")).await;

    // Failing revocation must not surface as an error: the user already
    // sees "signed out," and the only thing a 500 means we can't do is
    // tell the IdP — which it'll figure out when the tokens expire.
    auth.logout()
        .await
        .expect("logout must succeed despite IdP 5xx");
    assert!(!auth.has_tokens().await);
}

#[tokio::test]
async fn discovery_without_revocation_endpoint_is_not_an_error() {
    let server = MockServer::start().await;
    mount_discovery(&server, false).await;
    // `expect(0)` on a catch-all so a regression that starts POSTing
    // revoke (despite no endpoint advertised) shows up here, not as a
    // silent 404 absorbed by `revoke_one`'s error swallowing.
    Mock::given(method("POST"))
        .and(path("/oauth2/revoke"))
        .respond_with(ResponseTemplate::new(404))
        .expect(0)
        .mount(&server)
        .await;

    let auth = auth_with_session(&server, "at", Some("rt")).await;
    auth.logout().await.expect("logout");
    assert!(!auth.has_tokens().await);
    server.verify().await;
}

#[tokio::test]
async fn logout_with_no_tokens_makes_no_http_calls() {
    let server = MockServer::start().await;

    // Any HTTP call here is a bug — we short-circuit before discovery
    // when there's nothing to revoke.
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let auth = Auth::with_issuer(issuer_for(&server));
    auth.logout().await.expect("logout on empty session");
    server.verify().await;
}

/// Covered implicitly by `revokes_both_tokens_and_clears_local_state`
/// via `body_string_contains`, but inspecting the raw recorded request
/// catches a future refactor that drops `client_id` or stops form-
/// encoding entirely (which `body_string_contains` would tolerate).
#[tokio::test]
async fn revocation_post_is_form_encoded_with_client_id() {
    let server = MockServer::start().await;
    mount_discovery(&server, true).await;
    Mock::given(method("POST"))
        .and(path("/oauth2/revoke"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let auth = auth_with_session(&server, "at", Some("rt")).await;
    auth.logout().await.expect("logout");

    let received = server.received_requests().await.expect("recorded requests");
    let revoke_requests: Vec<&Request> = received
        .iter()
        .filter(|r| r.url.path() == "/oauth2/revoke")
        .collect();
    assert_eq!(revoke_requests.len(), 2, "RT + AT");
    for req in revoke_requests {
        let ct = req
            .headers
            .get("content-type")
            .map(|v| v.to_str().unwrap_or(""))
            .unwrap_or("");
        assert!(
            ct.starts_with("application/x-www-form-urlencoded"),
            "RFC 7009 requires form encoding, got {ct:?}"
        );
        let body = std::str::from_utf8(&req.body).expect("utf-8 body");
        assert!(
            body.contains(&format!("client_id={CLIENT_ID}")),
            "body: {body}"
        );
        assert!(body.contains("token="), "body: {body}");
        assert!(body.contains("token_type_hint="), "body: {body}");
    }
}
