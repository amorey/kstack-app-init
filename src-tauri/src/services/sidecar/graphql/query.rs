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

//! HTTP client the host uses to forward GraphQL queries to the sidecar.
//!
//! One fresh HTTP/1 connection per call (`Connection: close` model). The
//! sidecar listens on a UDS / named pipe, so connect cost is in the
//! sub-millisecond range — caching the connection (with reconnect-on-stale
//! logic and a mutex held across the request/response round-trip) was
//! premature optimization that bought negligible latency for meaningful
//! complexity. The hand-rolled-HTTP precedent (~/scratch/kstack-app-init)
//! used the same per-call model for the same reason; we keep hyper for the
//! parser but drop the caching.

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use thiserror::Error;

use super::super::ipc::{self, Endpoint};
use crate::error::{AppError, Result};

/// What went wrong on a single host↔sidecar HTTP call.
///
/// Distinguishes failure phases that all map to the same outward
/// `AppError::Io` at the command boundary, but matter for diagnostics
/// (and for any future routing — e.g. fast-fail vs. retry policy):
///
///   * `Connect` — couldn't even dial the endpoint. Almost always means
///     the sidecar isn't running or hasn't bound yet.
///   * `Io` — connected, then the stream broke mid-request or
///     mid-response (peer close, write error, etc.).
///   * `Protocol` — connected and read bytes, but they didn't parse as
///     valid HTTP/UTF-8/etc.
#[derive(Debug, Error)]
enum TransportError {
    #[error("connect: {0}")]
    Connect(std::io::Error),
    #[error("io: {0}")]
    Io(std::io::Error),
    #[error("protocol: {0}")]
    Protocol(String),
}

impl From<TransportError> for AppError {
    /// All transport-layer failures cross the Tauri boundary as
    /// `AppError::Io`. The frontend's `invokeFetch` adapter throws on Err,
    /// which urql turns into a `networkError` — exactly the retryable
    /// class. The variant-level distinction is preserved in the error
    /// `Display` (and in tracing fields, below) for debugging.
    fn from(err: TransportError) -> Self {
        match err {
            TransportError::Connect(e) | TransportError::Io(e) => AppError::Io(e),
            TransportError::Protocol(msg) => {
                AppError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            }
        }
    }
}

/// Defensive cap on the size of a response body the host will collect.
/// Set well above any realistic GraphQL payload — we'd rather error out
/// than let a runaway sidecar (bug, OOM upstream, or anything malicious)
/// drag the host's memory along with it. Mirrors the precedent from the
/// previous host impl (~/scratch/kstack-app-init/src-tauri/src/sidecar/
/// transport.rs:46), kept at the same 64 MiB.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Per-request wall-clock budget covering connect + write + read. Bounds
/// the case where the sidecar accepts the UDS connection but stalls its
/// handler (deadlocked goroutine, blocked downstream call) — without it,
/// `send_request` and the body collect would park indefinitely and the
/// Tauri command would never resolve, leaving urql with no `networkError`
/// to retry. Set generously: any GraphQL call exceeding it is almost
/// certainly stuck rather than slow.
const REQUEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// What [`QueryClient::query`] hands back. The status is surfaced
/// separately from the body so the frontend's `invokeFetch` adapter can
/// construct a `Response` with the *real* HTTP status — `Err` is reserved
/// strictly for transport failures (which urql's retryExchange retries),
/// while 4xx/5xx responses with valid GraphQL error bodies travel through
/// as `Ok` and reach urql as non-retryable server errors.
#[derive(Debug, Serialize)]
pub struct GraphqlResponse {
    pub status: u16,
    pub body: String,
}

/// Forwards GraphQL HTTP requests from the host to the sidecar.
///
/// Stateless: holds only the endpoint address. Each call opens, uses, and
/// drops its own HTTP/1 connection — no caching, no shared mutex, no
/// reconnect-on-stale dance.
pub struct QueryClient {
    path: Endpoint,
}

impl QueryClient {
    pub fn new(path: Endpoint) -> Self {
        Self { path }
    }

    /// Forwards a GraphQL query/mutation body to the sidecar's `/graphql`
    /// endpoint and returns the response body alongside its HTTP status.
    ///
    /// One fresh UDS / named-pipe connection per call. Transport errors
    /// map to [`AppError::Io`] so the frontend's `invokeFetch` adapter
    /// surfaces them as `networkError` (retryable by urql's
    /// retryExchange); HTTP-level non-2xx is *not* a transport error —
    /// it travels through as `Ok` with `status` set so the frontend can
    /// treat it as a server error.
    pub async fn query(&self, body: String) -> Result<GraphqlResponse> {
        match tokio::time::timeout(REQUEST_BUDGET, self.try_query(body)).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(err)) => {
                // Log the variant explicitly so failure mode is visible
                // in logs even though we collapse to `AppError::Io` at
                // the Tauri boundary.
                tracing::warn!(target: "sidecar", %err, "query failed");
                Err(err.into())
            }
            Err(_elapsed) => {
                tracing::warn!(target: "sidecar", "query exceeded request budget");
                Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "query exceeded request budget",
                )))
            }
        }
    }

    async fn try_query(
        &self,
        body: String,
    ) -> std::result::Result<GraphqlResponse, TransportError> {
        // Open and own the connection for exactly this request. Hyper's
        // connection-driver task is spawned and ends when `sender` drops
        // (after the response body has been collected below).
        let stream = ipc::connect(&self.path).await.map_err(|e| match e {
            AppError::Io(io) => TransportError::Connect(io),
            other => TransportError::Protocol(other.to_string()),
        })?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e)))?;
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                tracing::debug!(target: "sidecar", %err, "http1 connection ended");
            }
        });

        let req = Request::builder()
            .method("POST")
            // Sidecar exposes GraphQL at `/graphql` (server.go:66; mirrors
            // its own test at server_test.go:199). The mux happens to
            // wildcard unknown paths to the same handler, but `/graphql`
            // is the contract — `/control/*` is reserved for host↔sidecar
            // signals.
            .uri("/graphql")
            // hyper requires a Host header on HTTP/1.1 requests, even when
            // the underlying transport (UDS) makes it semantically empty.
            .header("host", "sidecar.local")
            .header("content-type", "application/json")
            // We don't reuse the connection across calls; declare it.
            .header("connection", "close")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| TransportError::Protocol(format!("build request: {e}")))?;

        let response = sender
            .send_request(req)
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e)))?;
        let status = response.status();
        // `Limited` short-circuits as soon as the running byte count
        // would exceed the cap, regardless of what Content-Length claimed.
        let bytes = Limited::new(response.into_body(), MAX_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e)))?
            .to_bytes();
        let body = String::from_utf8(bytes.to_vec())
            .map_err(|e| TransportError::Protocol(format!("non-utf8 body: {e}")))?;

        Ok(GraphqlResponse {
            status: status.as_u16(),
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interprocess::local_socket::{
        traits::tokio::Listener as _, GenericFilePath, ListenerOptions, ToFsName,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Spawns a tiny HTTP/1 server on `path` that reads one request, sends
    /// `response_body` back with `status_line`, and closes. Returns once
    /// the listener is bound so callers can connect without racing.
    ///
    /// Uses `interprocess`'s cross-platform listener so the same fixture
    /// drives Unix UDS tests and Windows named-pipe tests.
    async fn spawn_oneshot_server_with_status(
        path: String,
        status_line: &'static str,
        response_body: &'static str,
    ) {
        let name = path
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .expect("to_fs_name");
        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind listener");
        tokio::spawn(async move {
            if let Ok(mut stream) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = Vec::new();
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    total.extend_from_slice(&buf[..n]);
                    if total.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
    }

    async fn spawn_oneshot_server(path: String, response_body: &'static str) {
        spawn_oneshot_server_with_status(path, "200 OK", response_body).await;
    }

    /// Happy path: against a server that replies 200 with a JSON body, the
    /// client returns that body verbatim alongside the status. This is the
    /// frontend's `invokeFetch` contract — `Response.text()` round-trips.
    #[tokio::test]
    async fn query_returns_response_body() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_oneshot_server(arg.clone(), r#"{"data":{"ok":true}}"#).await;
        // Give the server a beat to be ready (UnixListener::bind is
        // synchronous so this is just paranoia on slow CI).
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = QueryClient::new(path);
        let resp = client
            .query("{\"query\":\"{ ping }\"}".to_string())
            .await
            .expect("query should succeed");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, r#"{"data":{"ok":true}}"#);
        let _ = std::fs::remove_file(arg);
    }

    /// `TransportError` distinguishes the failure phase. We can't observe
    /// the variant from outside the client (it collapses to `AppError::Io`
    /// at the boundary), but we *can* assert it directly against the
    /// internal `try_query` path so the categorization is locked
    /// in. The Connect arm is the one users hit most often (sidecar not
    /// running yet); make sure it's tagged correctly.
    #[tokio::test]
    async fn try_query_categorizes_connect_failure() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let client = QueryClient::new(path);
        // No listener anywhere — the dial will exhaust its budget and
        // return a Connect-variant error, not Io or Protocol.
        let err = client
            .try_query("{}".to_string())
            .await
            .expect_err("no listener: expected Err");
        assert!(
            matches!(err, TransportError::Connect(_)),
            "expected Connect variant, got {err:?}"
        );
    }

    /// Defensive cap: a runaway or malicious sidecar must not be able to
    /// pump unbounded bytes into the host process. The collected body is
    /// limited to MAX_RESPONSE_BYTES; anything larger surfaces as a
    /// transport error rather than oom'ing.
    #[tokio::test]
    async fn query_rejects_response_exceeding_max_bytes() {
        // Spin up a hand-rolled server that claims a Content-Length larger
        // than our cap. We don't need to actually send that many bytes —
        // the cap is enforced against the announced body length.
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        let arg_for_server = arg.clone();
        tokio::spawn(async move {
            let name = arg_for_server
                .as_str()
                .to_fs_name::<GenericFilePath>()
                .expect("to_fs_name");
            let listener = ListenerOptions::new()
                .name(name)
                .create_tokio()
                .expect("bind");
            if let Ok(mut stream) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = Vec::new();
                while let Ok(n) = stream.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    total.extend_from_slice(&buf[..n]);
                    if total.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                // Announce a body 1 byte over the cap. We send a partial
                // body and close — http_body_util::Limited fails as soon
                // as the announced length exceeds the cap or as bytes
                // accumulate past it.
                let oversized = MAX_RESPONSE_BYTES + 1;
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {oversized}\r\nContent-Type: application/json\r\n\r\n"
                );
                let _ = stream.write_all(header.as_bytes()).await;
                // Write a chunk past the cap so Limited trips even if it
                // doesn't pre-check Content-Length.
                let chunk = vec![b'x'; MAX_RESPONSE_BYTES + 16];
                let _ = stream.write_all(&chunk).await;
                let _ = stream.shutdown().await;
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = QueryClient::new(path);
        let err = client
            .query("{}".to_string())
            .await
            .expect_err("oversized body must error");
        assert!(matches!(err, AppError::Io(_)), "got: {err}");
        let _ = std::fs::remove_file(arg);
    }

    /// Non-2xx responses (auth failures, malformed-request errors) must
    /// reach the frontend as a successful command result carrying the real
    /// status — NOT as a thrown error. urql's retryExchange only retries
    /// `networkError`s; if we mapped a 4xx to a thrown error it would loop
    /// forever instead of surfacing the failure to the UI.
    #[tokio::test]
    async fn query_returns_4xx_as_response_not_error() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_oneshot_server_with_status(
            arg.clone(),
            "401 Unauthorized",
            r#"{"errors":[{"message":"no auth"}]}"#,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = QueryClient::new(path);
        let resp = client
            .query("{}".to_string())
            .await
            .expect("4xx must NOT be a transport error");
        assert_eq!(resp.status, 401);
        assert_eq!(resp.body, r#"{"errors":[{"message":"no auth"}]}"#);
        let _ = std::fs::remove_file(arg);
    }

    /// With no listener at the path at all, the query must fail with a
    /// transport error (mapped to `AppError::Io`) within the connect
    /// budget — and crucially, not hang. The frontend's retryExchange
    /// depends on seeing a `networkError`, not a stuck promise.
    #[tokio::test]
    async fn query_returns_transport_error_when_socket_unreachable() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        // Override the Endpoint's connect budget would require plumbing,
        // so just rely on default budget here being capped (5s) and
        // assert the error type.
        let client = QueryClient::new(path);

        // Wrap the call in a timeout so a failure to fail won't hang CI.
        let result = tokio::time::timeout(Duration::from_secs(10), client.query("{}".to_string()))
            .await
            .expect("must not hang past test timeout");

        let err = result.expect_err("no listener: expected Err");
        assert!(matches!(err, AppError::Io(_)), "got: {err}");
    }

    /// Per-call connect model: each query opens its own connection, so
    /// the second call against a freshly-bound listener succeeds even
    /// though the first server closed. Models the sidecar crashing and
    /// being restarted between calls.
    ///
    /// Unix-only: this exercises rebinding a UDS path after removing the
    /// inode. Windows named pipes live in a kernel namespace (no file to
    /// remove) and rebinding the same name while the prior instance is
    /// still tearing down returns `ACCESS_DENIED`.
    #[cfg(unix)]
    #[tokio::test]
    async fn query_succeeds_against_fresh_listener_on_each_call() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();

        spawn_oneshot_server(arg.clone(), r#"{"data":1}"#).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let client = QueryClient::new(path);

        let first = client.query("{}".to_string()).await.expect("first");
        assert_eq!(first.body, r#"{"data":1}"#);

        // Old server is gone (oneshot). Stand up a new one on the same path.
        let _ = std::fs::remove_file(&arg);
        spawn_oneshot_server(arg.clone(), r#"{"data":2}"#).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let second = client
            .query("{}".to_string())
            .await
            .expect("second should reconnect");
        assert_eq!(second.body, r#"{"data":2}"#);
        let _ = std::fs::remove_file(arg);
    }
}
