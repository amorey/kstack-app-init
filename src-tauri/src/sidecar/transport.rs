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

/// POST `body` (typically a GraphQL JSON envelope) to /graphql over the UDS at
/// `socket`. Returns the raw response body bytes — the caller decodes JSON.
pub async fn query_uds(socket: &Path, body: &[u8]) -> Result<Vec<u8>, SidecarError> {
    let name = socket
        .to_fs_name::<GenericFilePath>()
        .map_err(SidecarError::Connect)?;
    let conn = Stream::connect(name).await.map_err(SidecarError::Connect)?;

    let mut req = Vec::with_capacity(160 + body.len());
    req.extend_from_slice(b"POST /graphql HTTP/1.1\r\n");
    // Host header is meaningless for UDS but required by HTTP/1.1.
    req.extend_from_slice(b"Host: sidecar.local\r\n");
    req.extend_from_slice(b"Content-Type: application/json\r\n");
    req.extend_from_slice(b"Connection: close\r\n");
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
}
