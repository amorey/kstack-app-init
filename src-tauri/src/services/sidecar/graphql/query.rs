// Copyright 2026 The Kstack Authors
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

//! Forwards GraphQL queries to the sidecar. One fresh HTTP/1 connection per
//! call (`Connection: close`) — UDS/pipe connect is sub-ms, so pooling isn't
//! worth the reconnect-on-stale logic. hyper is kept only for the parser.

use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use thiserror::Error;

use super::super::ipc::{self, Endpoint};
use crate::error::{AppError, Result};

/// Failure phase of one host↔sidecar HTTP call (`Connect` = couldn't dial,
/// `Io` = stream broke mid-flight, `Protocol` = unparseable bytes). All
/// collapse to `AppError::Io` at the command boundary; the phase survives in
/// the `Display` text.
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
    /// `AppError::Io` → the frontend's `invokeFetch` throws → urql retries it
    /// as a `networkError`.
    fn from(err: TransportError) -> Self {
        match err {
            TransportError::Connect(e) | TransportError::Io(e) => AppError::Io(e),
            TransportError::Protocol(msg) => {
                AppError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
            }
        }
    }
}

/// Defensive response-body cap so a runaway sidecar can't drag the host's
/// memory down.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Wall-clock budget for connect + write + read. Without it a stalled handler
/// parks the Tauri command forever, leaving urql no `networkError` to retry.
/// Generous: anything exceeding it is stuck, not slow.
const REQUEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Status is separate from body so `invokeFetch` can build a `Response` with
/// the real HTTP status: `Err` is reserved for transport failures (urql
/// retries those); 4xx/5xx travel as `Ok` — non-retryable server errors.
#[derive(Debug, Serialize)]
pub struct GraphqlResponse {
    pub status: u16,
    pub body: String,
}

/// Stateless GraphQL forwarder: one owned HTTP/1 connection per call.
pub struct QueryClient {
    path: Endpoint,
}

impl QueryClient {
    pub fn new(path: Endpoint) -> Self {
        Self { path }
    }

    /// Forwards a query/mutation to `/graphql`. Transport errors →
    /// [`AppError::Io`]; HTTP non-2xx is NOT an error — it travels as `Ok`
    /// with the real status.
    pub async fn query(&self, body: String) -> Result<GraphqlResponse> {
        match tokio::time::timeout(REQUEST_BUDGET, self.try_query(body)).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(err)) => {
                // Log the phase, which the boundary's `AppError::Io` loses.
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
        // Hyper's connection-driver task ends when `sender` drops (after the
        // body collect).
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
            // `/graphql` is the contract (sidecar server.go).
            .uri("/graphql")
            // hyper requires a Host header on HTTP/1.1, even over UDS.
            .header("host", "sidecar.local")
            .header("content-type", "application/json")
            .header("connection", "close")
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| TransportError::Protocol(format!("build request: {e}")))?;

        let response = sender
            .send_request(req)
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e)))?;
        let status = response.status();
        // `Limited` short-circuits once the running count exceeds the cap,
        // regardless of Content-Length.
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

        let client = QueryClient::new(path);
        let resp = client
            .query("{\"query\":\"{ ping }\"}".to_string())
            .await
            .expect("query should succeed");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, r#"{"data":{"ok":true}}"#);
        let _ = std::fs::remove_file(arg);
    }

    /// Asserts the failure phase against `try_query` directly (it collapses to
    /// `AppError::Io` at the boundary). Connect is the arm users hit most (sidecar
    /// not running yet), so make sure it's tagged correctly.
    #[tokio::test]
    async fn try_query_categorizes_connect_failure() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let client = QueryClient::new(path);
        // No listener: the dial exhausts its budget and returns Connect.
        let err = client
            .try_query("{}".to_string())
            .await
            .expect_err("no listener: expected Err");
        assert!(
            matches!(err, TransportError::Connect(_)),
            "expected Connect variant, got {err:?}"
        );
    }

    /// A body over MAX_RESPONSE_BYTES surfaces as a transport error rather than
    /// letting a runaway or malicious sidecar OOM the host.
    #[tokio::test]
    async fn query_rejects_response_exceeding_max_bytes() {
        // Server that announces a Content-Length larger than the cap.
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        // Bind synchronously before spawning the accept loop, so the socket exists the
        // instant this returns — the client connects into the listen backlog with no
        // wait-for-bind race, and no sleep.
        let name = arg
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .expect("to_fs_name");
        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind");
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
                // Announce a body 1 byte over the cap; `Limited` fails once the
                // announced length exceeds it or bytes accumulate past it.
                let oversized = MAX_RESPONSE_BYTES + 1;
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {oversized}\r\nContent-Type: application/json\r\n\r\n"
                );
                let _ = stream.write_all(header.as_bytes()).await;
                // Write a chunk past the cap so Limited trips even without a
                // Content-Length pre-check.
                let chunk = vec![b'x'; MAX_RESPONSE_BYTES + 16];
                let _ = stream.write_all(&chunk).await;
                let _ = stream.shutdown().await;
            }
        });

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
        // Rely on the default (capped) connect budget and assert the error type.
        let client = QueryClient::new(path);

        // Timeout so a failure to fail won't hang CI.
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
        let client = QueryClient::new(path);

        let first = client.query("{}".to_string()).await.expect("first");
        assert_eq!(first.body, r#"{"data":1}"#);

        // Old server is gone (oneshot). Stand up a new one on the same path.
        let _ = std::fs::remove_file(&arg);
        spawn_oneshot_server(arg.clone(), r#"{"data":2}"#).await;

        let second = client
            .query("{}".to_string())
            .await
            .expect("second should reconnect");
        assert_eq!(second.body, r#"{"data":2}"#);
        let _ = std::fs::remove_file(arg);
    }
}
