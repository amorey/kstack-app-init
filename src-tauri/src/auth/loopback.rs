//! Loopback redirect catcher (RFC 8252 §7.3).
//!
//! Auth lives on `http://127.0.0.1:<ephemeral>/oauth/callback` rather than
//! a custom URL scheme: it works the same in dev and prod, on every OS,
//! for bundled and unbundled binaries, and avoids the namespace-ownership
//! issues custom schemes have on desktop.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use url::Url;

const CALLBACK_PATH: &str = "/oauth/callback";
const ACCEPT_DEADLINE: Duration = Duration::from_secs(300);
const READ_BUF_LIMIT: usize = 8 * 1024;

pub struct CallbackResult {
    pub code: Option<String>,
    pub error: Option<String>,
}

const CLOSE_TAB_HTML: &str = "<!doctype html>\
<meta charset=utf-8>\
<title>kstack — signed in</title>\
<style>body{font:16px system-ui,sans-serif;text-align:center;padding:4em;color:#222}h1{font-weight:600}</style>\
<h1>Signed in</h1>\
<p>You can close this tab and return to the app.</p>";

const NOT_FOUND_RESPONSE: &str =
    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// Bind on `127.0.0.1:0` and return (listener, redirect_uri). The caller
/// passes the URI to the IdP and then awaits [`accept_callback_once`].
pub fn bind() -> Result<(TcpListener, String), String> {
    // Sync-bound via std so we can grab `local_addr()` before any await —
    // the redirect URI has to be in hand before we open the browser.
    let std_listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| format!("loopback bind: {e}"))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| format!("loopback nonblock: {e}"))?;
    let port = std_listener
        .local_addr()
        .map_err(|e| format!("loopback addr: {e}"))?
        .port();
    let listener =
        TcpListener::from_std(std_listener).map_err(|e| format!("loopback from_std: {e}"))?;
    let redirect_uri = format!("http://127.0.0.1:{port}{CALLBACK_PATH}");
    Ok((listener, redirect_uri))
}

/// Accept connections until one targets `CALLBACK_PATH`, parse query
/// params from its request line, reply with [`CLOSE_TAB_HTML`], and
/// return the parsed values. Verifies `state` matches what we minted —
/// mismatch is treated as a CSRF signal and refused.
///
/// Loops because browser extensions / OS-level prefetchers occasionally
/// open a stray probe to `127.0.0.1:<port>/`; we 404 those (so the bad
/// actor doesn't get to consume our single accept) and keep waiting for
/// the real redirect. The 5-minute deadline applies to the whole loop.
pub async fn accept_callback_once(
    listener: TcpListener,
    expected_state: &str,
) -> Result<CallbackResult, String> {
    let deadline = Instant::now() + ACCEPT_DEADLINE;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "loopback accept timed out".to_string())?;
        let (stream, _peer) = tokio::time::timeout(remaining, listener.accept())
            .await
            .map_err(|_| "loopback accept timed out".to_string())?
            .map_err(|e| format!("loopback accept: {e}"))?;
        match handle_connection(stream, expected_state).await? {
            Some(result) => return Ok(result),
            None => continue,
        }
    }
}

/// Handles one connection. Returns:
/// - `Ok(Some(_))` for a successful `/oauth/callback` request,
/// - `Ok(None)` for any other path (404'd; caller keeps waiting),
/// - `Err(_)` for protocol / parse / state-mismatch failures we treat as fatal.
async fn handle_connection(
    mut stream: TcpStream,
    expected_state: &str,
) -> Result<Option<CallbackResult>, String> {
    // Loop reads until `\r\n` shows up or we hit the limit. TCP can
    // fragment the request; one `read()` may give us `GET /oauth/cal`.
    let mut buf = Vec::with_capacity(1024);
    let line_end = loop {
        if let Some(idx) = find_subsequence(&buf, b"\r\n") {
            break idx;
        }
        if buf.len() >= READ_BUF_LIMIT {
            return Err("loopback request line too long".into());
        }
        let mut chunk = [0u8; 1024];
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("loopback read: {e}"))?;
        if n == 0 {
            return Err("loopback connection closed before request line".into());
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let line =
        std::str::from_utf8(&buf[..line_end]).map_err(|_| "loopback non-utf8".to_string())?;
    let path = line
        .split_whitespace()
        .nth(1)
        .ok_or("loopback malformed request")?;

    // Parse path even for non-callback requests so we can match cleanly.
    // `Url::parse` won't accept a bare path; prefix a dummy origin.
    let url =
        Url::parse(&format!("http://127.0.0.1{path}")).map_err(|e| format!("loopback url: {e}"))?;

    if url.path() != CALLBACK_PATH {
        let _ = stream.write_all(NOT_FOUND_RESPONSE.as_bytes()).await;
        let _ = stream.shutdown().await;
        return Ok(None);
    }

    let mut code = None;
    let mut state = None;
    let mut error = None;
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => error = Some(v.into_owned()),
            _ => {}
        }
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        CLOSE_TAB_HTML.len(),
        CLOSE_TAB_HTML
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;

    // CSRF defense: openidconnect's exchange_code doesn't auto-verify
    // state, so we do it here. Constant-time isn't worth it — both sides
    // are random tokens of equal length.
    if state.as_deref() != Some(expected_state) {
        return Err("loopback state mismatch".into());
    }
    Ok(Some(CallbackResult { code, error }))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
