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

//! Wire transport for the sidecar.
//!
//! AF_UNIX socket on all targets (UDS on macOS/Linux, AF_UNIX on Windows 10
//! 17063+). One HTTP/1.1 POST per call, `Content-Length`-framed, no
//! keep-alive. Small and explicit by design — we control both ends, so we
//! don't need a full HTTP client.

use std::path::Path;

use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericFilePath, ToFsName,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Sidecar prints this followed by the socket path on its first stdout line.
/// Exported so the Tauri host and integration tests stay in sync with the
/// Go side's `fmt.Printf("READY unix:%s\n", ...)` in sidecar/main.go.
pub const READY_PREFIX: &str = "READY unix:";

/// Host header value for HTTP/WS requests over the UDS. Meaningless on the
/// wire (no DNS) but required by HTTP/1.1.
pub(super) const HOST: &str = "sidecar.local";

/// Connect to the sidecar's Unix domain socket.
pub(super) async fn connect_uds(socket: &Path) -> Result<Stream, std::io::Error> {
    let name = socket.to_fs_name::<GenericFilePath>()?;
    Stream::connect(name).await
}

/// Cap response body size. Defends against a misbehaving sidecar buffering
/// host memory; well above any realistic GraphQL response.
const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum SidecarError {
    #[error("connect: {0}")]
    Connect(std::io::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed http response: {0}")]
    Protocol(&'static str),
    #[error("sidecar returned http {0}")]
    Status(u16),
}

/// POST `body` to `path` over the UDS at `socket`; returns the raw
/// response body bytes. One `Content-Length`-framed HTTP/1.1 request, no
/// keep-alive. `bearer`, when `Some`, is attached as `Authorization:
/// Bearer <token>`. Both callers below share this so the wire framing
/// lives in exactly one place.
async fn post_uds(
    socket: &Path,
    path: &str,
    bearer: Option<&str>,
    body: &[u8],
) -> Result<Vec<u8>, SidecarError> {
    let conn = connect_uds(socket).await.map_err(SidecarError::Connect)?;

    let mut req = Vec::with_capacity(200 + body.len());
    req.extend_from_slice(b"POST ");
    req.extend_from_slice(path.as_bytes());
    req.extend_from_slice(b" HTTP/1.1\r\n");
    req.extend_from_slice(b"Host: ");
    req.extend_from_slice(HOST.as_bytes());
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(b"Content-Type: application/json\r\n");
    req.extend_from_slice(b"Connection: close\r\n");
    if let Some(token) = bearer {
        req.extend_from_slice(b"Authorization: Bearer ");
        req.extend_from_slice(token.as_bytes());
        req.extend_from_slice(b"\r\n");
    }
    use std::io::Write as _;
    write!(&mut req, "Content-Length: {}\r\n\r\n", body.len())
        .expect("write to Vec<u8> is infallible");
    req.extend_from_slice(body);

    let (rx, mut tx) = conn.split();
    tx.write_all(&req).await?;
    tx.flush().await?;

    let mut raw = Vec::with_capacity(8 * 1024);
    rx.take(MAX_RESPONSE_BYTES).read_to_end(&mut raw).await?;

    parse_response(raw)
}

/// POST `body` (typically a GraphQL JSON envelope) to /graphql over the UDS
/// at `socket`. Returns the raw response body bytes — the caller decodes
/// JSON.
///
/// `bearer` is the OAuth access token. When `Some`, attached as
/// `Authorization: Bearer <token>` so the sidecar can forward credentials to
/// the cloud API for any operation that needs them. When `None`, no header
/// is sent — useful for purely-local operations (e.g. `ping`).
pub async fn query_uds(
    socket: &Path,
    body: &[u8],
    bearer: Option<&str>,
) -> Result<Vec<u8>, SidecarError> {
    post_uds(socket, "/graphql", bearer, body).await
}

/// POST the host's bearer token to the sidecar's host-only
/// `/control/credentials` endpoint so the always-on engine can authenticate
/// without an inbound request. Deliberately not on the GraphQL path (see
/// `sidecar/server/server.go`); the sidecar replies 204 (empty body) and
/// any non-2xx is surfaced as an error so the caller can retry.
pub async fn push_credentials(socket: &Path, token: &str) -> Result<(), SidecarError> {
    let body = serde_json::json!({ "token": token }).to_string();
    post_uds(socket, "/control/credentials", None, body.as_bytes())
        .await
        .map(|_| ())
}

/// POST the host-only `/control/wake` trigger on every host OS wake
/// (power-resume / network-change). Generic name on purpose: today the
/// sidecar responds by resyncing, but the wire stays stable if future
/// wake-responses are added. Empty body; sidecar replies 204 and any
/// non-2xx is surfaced so the caller can retry.
pub async fn post_wake(socket: &Path) -> Result<(), SidecarError> {
    post_uds(socket, "/control/wake", None, &[])
        .await
        .map(|_| ())
}

fn parse_response(mut raw: Vec<u8>) -> Result<Vec<u8>, SidecarError> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(SidecarError::Protocol("missing header terminator"))?;

    let first_line_end = raw[..split]
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(split);
    let status_line = std::str::from_utf8(&raw[..first_line_end])
        .map_err(|_| SidecarError::Protocol("non-utf8 status line"))?;
    let code = status_line
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or(SidecarError::Protocol("status code"))?;
    if !(200..300).contains(&code) {
        return Err(SidecarError::Status(code));
    }

    raw.drain(..split + 4);
    Ok(raw)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn http(status_line: &str, body: &str) -> Vec<u8> {
        format!(
            "{status_line}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[test]
    fn ok_returns_body() {
        let raw = http("HTTP/1.1 200 OK", r#"{"data":{"ping":"pong"}}"#);
        let body = parse_response(raw).unwrap();
        assert_eq!(body, br#"{"data":{"ping":"pong"}}"#);
    }

    #[test]
    fn ok_with_empty_body() {
        let raw = http("HTTP/1.1 200 OK", "");
        let body = parse_response(raw).unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn ok_preserves_body_with_embedded_crlf() {
        // Body containing what looks like a header terminator must not confuse
        // the splitter — `windows().position` finds the *first* match, which
        // is the real header/body boundary.
        let raw = http("HTTP/1.1 200 OK", "line1\r\n\r\nline2");
        let body = parse_response(raw).unwrap();
        assert_eq!(body, b"line1\r\n\r\nline2");
    }

    #[test]
    fn non_2xx_status_is_error() {
        for code in [400u16, 404, 500, 503] {
            let raw = http(&format!("HTTP/1.1 {code} Whatever"), "{}");
            match parse_response(raw) {
                Err(SidecarError::Status(c)) => assert_eq!(c, code),
                other => panic!("expected Status({code}), got {other:?}"),
            }
        }
    }

    #[test]
    fn missing_header_terminator_is_protocol_error() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n".to_vec();
        match parse_response(raw) {
            Err(SidecarError::Protocol(msg)) => assert!(msg.contains("terminator")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[test]
    fn unparseable_status_code_is_protocol_error() {
        let raw = b"HTTP/1.1 NOTANUMBER Bad\r\n\r\n".to_vec();
        match parse_response(raw) {
            Err(SidecarError::Protocol(msg)) => assert!(msg.contains("status")),
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    use interprocess::local_socket::{tokio::Listener, GenericFilePath, ListenerOptions, ToFsName};
    use std::path::PathBuf;

    fn temp_socket() -> PathBuf {
        // Windows requires a named-pipe path (`\\.\pipe\...`); a temp-dir
        // file path is rejected by `to_fs_name::<GenericFilePath>`.
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        #[cfg(windows)]
        {
            PathBuf::from(format!(r"\\.\pipe\ks-tx-{}-{n}", std::process::id()))
        }
        #[cfg(not(windows))]
        {
            let p = std::env::temp_dir().join(format!("ks-tx-{}-{n}.sock", std::process::id()));
            let _ = std::fs::remove_file(&p);
            p
        }
    }

    fn bind(socket: &Path) -> Listener {
        let name = socket.to_fs_name::<GenericFilePath>().unwrap();
        ListenerOptions::new().name(name).create_tokio().unwrap()
    }

    /// Read one full HTTP request (headers + declared Content-Length body)
    /// off `rx`. Shared by the fake-server helpers below.
    async fn read_http_request<R: tokio::io::AsyncReadExt + Unpin>(rx: &mut R) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2048);
        let mut chunk = [0u8; 1024];
        loop {
            let n = rx.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&buf[..p]);
                let len: usize = header
                    .lines()
                    .find_map(|l| l.strip_prefix("Content-Length: "))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                if buf.len() >= p + 4 + len {
                    break;
                }
            }
        }
        buf
    }

    /// Reads one HTTP request off conn, echoes back a fixed 200 with the
    /// observed Authorization header as the body. Lets the test assert on
    /// what actually crossed the wire.
    async fn echo_auth(conn: interprocess::local_socket::tokio::Stream) {
        let (mut rx, mut tx) = conn.split();
        let buf = read_http_request(&mut rx).await;
        let header = String::from_utf8_lossy(&buf);
        let auth = header
            .lines()
            .find_map(|l| l.strip_prefix("Authorization: "))
            .unwrap_or("<none>")
            .to_owned();
        let body = format!("{{\"auth\":{}}}", serde_json::Value::String(auth));
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        tx.write_all(resp.as_bytes()).await.unwrap();
        tx.flush().await.unwrap();
    }

    #[tokio::test]
    async fn bearer_header_attached_when_provided() {
        let socket = temp_socket();
        let listener = bind(&socket);
        tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            echo_auth(conn).await;
        });

        let body = query_uds(&socket, b"{}", Some("tok-xyz")).await.unwrap();
        let got = String::from_utf8(body).unwrap();
        assert!(
            got.contains(r#""auth":"Bearer tok-xyz""#),
            "auth not echoed: {got}"
        );
    }

    #[tokio::test]
    async fn no_bearer_header_when_none() {
        let socket = temp_socket();
        let listener = bind(&socket);
        tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            echo_auth(conn).await;
        });

        let body = query_uds(&socket, b"{}", None).await.unwrap();
        let got = String::from_utf8(body).unwrap();
        assert!(got.contains(r#""auth":"<none>""#), "got {got}");
    }

    /// Reads one HTTP request, sends back `status_line` with an empty body,
    /// and returns the bytes it observed so the test can assert on the
    /// request line + JSON body that crossed the wire.
    async fn capture_request(
        conn: interprocess::local_socket::tokio::Stream,
        status_line: &str,
    ) -> String {
        let (mut rx, mut tx) = conn.split();
        let buf = read_http_request(&mut rx).await;
        let resp = format!("{status_line}\r\nContent-Length: 0\r\n\r\n");
        tx.write_all(resp.as_bytes()).await.unwrap();
        tx.flush().await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn push_credentials_posts_token_and_succeeds() {
        let socket = temp_socket();
        let listener = bind(&socket);
        let seen = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            capture_request(conn, "HTTP/1.1 204 No Content").await
        });

        push_credentials(&socket, "tok-xyz").await.unwrap();

        let req = seen.await.unwrap();
        assert!(
            req.starts_with("POST /control/credentials HTTP/1.1"),
            "request line: {req}"
        );
        assert!(req.contains(r#""token":"tok-xyz""#), "body: {req}");
    }

    #[tokio::test]
    async fn post_wake_posts_and_succeeds() {
        let socket = temp_socket();
        let listener = bind(&socket);
        let seen = tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            capture_request(conn, "HTTP/1.1 204 No Content").await
        });

        post_wake(&socket).await.unwrap();

        let req = seen.await.unwrap();
        assert!(
            req.starts_with("POST /control/wake HTTP/1.1"),
            "request line: {req}"
        );
    }

    #[tokio::test]
    async fn push_credentials_non_2xx_is_error() {
        let socket = temp_socket();
        let listener = bind(&socket);
        tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            capture_request(conn, "HTTP/1.1 400 Bad Request").await;
        });

        match push_credentials(&socket, "tok").await {
            Err(SidecarError::Status(400)) => {}
            other => panic!("expected Status(400), got {other:?}"),
        }
    }
}
