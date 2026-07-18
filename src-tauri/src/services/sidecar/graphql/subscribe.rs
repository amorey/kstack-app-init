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

//! Per-subscription Server-Sent Events (SSE) reader.
//!
//! Each GraphQL subscription the webview opens gets its own short-lived HTTP/1
//! connection to the sidecar (UDS / named pipe) — exactly like a query
//! (`query.rs`), except we POST with `Accept: text/event-stream` and stream the
//! sidecar's gqlgen SSE frames instead of collecting a single response body.
//!
//! gqlgen's SSE wire format (distinct-connection mode):
//! ```text
//!   :\n\n                                       initial comment (ignored)
//!   : ping\n\n                                  keep-alive comment (ignored)
//!   event: next\ndata: <graphql.Response>\n\n   one per emitted value
//!   event: complete\n\n                         terminal, sent once
//! ```
//!
//! We translate that into the webview channel envelope the frontend already
//! speaks (`subscribe-exchange.ts`):
//! ```text
//!   open     → {"type":"open"}    once, on 200, before any `next`
//!   next     → {"type":"next","payload":<graphql.Response>}
//!   complete → {"type":"complete"}   graceful end (clean end-of-body)
//!   closed   → {"type":"closed"}     abnormal end (mid-stream read failure)
//!   error    → {"type":"error","payload":<message>}  (transport-level only)
//! ```
//! `complete` vs `closed` is the "was this the server's decision?" split: both
//! make the frontend reconnect (a long-lived watch completed by the server —
//! e.g. sidecar shutdown — must come back), but only `closed` is an *abnormal*
//! end worth reporting to the user; a graceful `complete` (also the normal end
//! of a finite stream like chat) reconnects silently. See
//! `subscribe-exchange.ts`.
//!
//! The split is detected at the *body* level, not from the `complete` SSE
//! event: gqlgen's `event: complete` carries no `data:` line, and the SSE spec
//! discards data-less events at dispatch, so the parser never yields it.
//! What distinguishes the endings is chunked transfer framing — a graceful
//! completion ends with the server's chunked terminator (a clean end-of-body),
//! while a crash/sleep/network drop truncates the chunked body and surfaces as
//! a read error.
//! `open` is the "connection established" signal: it fires only after the dial
//! succeeds (200 received), so the frontend can reset its per-subscription
//! accumulators on a reconnect without also clearing them on a failed dial
//! (which emits `error` instead). See `subscribe-exchange.ts`.
//!
//! There's no shared session, no `connection_init`/`ack`, and no demultiplexing:
//! a dropped connection ends exactly one subscription, and the frontend's
//! capped-backoff reconnect (subscribe-exchange.ts) re-subscribes — which just
//! opens a fresh connection here. The only state we keep is a table of cancel
//! handles so `unsubscribe` can drop the right connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::sync::{oneshot, Mutex};

use super::super::ipc::{self, Endpoint, DEFAULT_CONNECT_BUDGET};
use crate::error::{AppError, Result};

/// Where the host sends each subscription envelope. Production wraps a Tauri
/// `Channel<String>`; tests use an mpsc adapter so they don't have to stand up
/// a Tauri runtime.
pub trait FrameSink: Send + Sync {
    /// Forwards one `SubMessage`-shaped JSON envelope to the consumer.
    /// Best-effort: a closed sink simply drops the message — the consumer has
    /// already torn down.
    fn send_frame(&self, frame: String);
}

/// The graceful-end envelope: the server finished the stream on its own terms
/// (clean end-of-body — how gqlgen's data-less `event: complete` actually
/// surfaces, see the module docs). The frontend still reconnects (a
/// server-completed watch — e.g. sidecar shutdown — must come back) but does
/// not report it as an error.
const COMPLETE_FRAME: &str = r#"{"type":"complete"}"#;

/// The abnormal-end envelope: the connection died mid-stream (truncated
/// chunked body / read failure — sidecar crash, sleep, network loss). The
/// frontend's reconnect path fires and, unlike `complete`, reports the drop.
const CLOSED_FRAME: &str = r#"{"type":"closed"}"#;

/// The "connection established" envelope. Sent once, before any `next`, after
/// the dial succeeds (200 received) — never on a failed dial (that emits
/// `error`). The frontend resets its per-subscription accumulators on this
/// frame so a reconnect's snapshot replaces rather than merges. See
/// `subscribe-exchange.ts`.
const OPEN_FRAME: &str = r#"{"type":"open"}"#;

/// Opens one SSE connection per subscription and tracks a cancel handle for
/// each so [`SubscriptionClient::unsubscribe`] can tear it down.
pub struct SubscriptionClient {
    path: Endpoint,
    /// Cancel handles keyed by host-side op id (the `u64` returned to Tauri).
    /// Dropping/firing the sender ends the matching streaming task.
    subs: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>>,
    /// Host-side op id counter. Monotonic for the process lifetime so the
    /// frontend can rely on distinct ids across reconnects.
    next_id: AtomicU64,
    /// How long the initial UDS dial may take before the subscription gives up
    /// and emits an `error` frame. Tests shrink this; production uses
    /// [`DEFAULT_CONNECT_BUDGET`].
    connect_budget: Duration,
}

impl SubscriptionClient {
    pub fn new(path: Endpoint) -> Self {
        Self::new_with_budget(path, DEFAULT_CONNECT_BUDGET)
    }

    /// Same as [`SubscriptionClient::new`] but with a caller-supplied connect
    /// budget. Not part of the public surface — only `new` and tests (which
    /// pass a short budget so a dead-socket case fails fast) construct with it.
    fn new_with_budget(path: Endpoint, connect_budget: Duration) -> Self {
        Self {
            path,
            subs: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            connect_budget,
        }
    }

    /// Registers a subscription: opens its own SSE connection to the sidecar
    /// and streams frames to `sink` until the server completes, the connection
    /// drops, or [`Self::unsubscribe`] is called. Returns the host-side op id.
    ///
    /// Returns `Ok` as soon as the streaming task is spawned — connection and
    /// stream failures surface on the sink (as an `error` or `closed`
    /// envelope), which is exactly the frontend's reconnect path, so there's
    /// no transport error to bubble up synchronously.
    pub async fn subscribe(
        &self,
        query: String,
        variables: serde_json::Value,
        sink: Arc<dyn FrameSink>,
    ) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = serde_json::json!({ "query": query, "variables": variables }).to_string();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        // Register before spawning. The frontend only learns `id` once this
        // returns, so no `unsubscribe(id)` can race ahead of the insert.
        self.subs.lock().await.insert(id, cancel_tx);

        let path = self.path.clone();
        let budget = self.connect_budget;
        let subs = Arc::clone(&self.subs);
        tokio::spawn(run_subscription(
            path, budget, body, sink, cancel_rx, subs, id,
        ));
        Ok(id)
    }

    /// Cancels a subscription by host-side op id, dropping its connection.
    ///
    /// Tolerant of unknown ids — urql's subscribe-exchange can race teardown
    /// with the `graphql_subscribe` resolve (subscribe-exchange.ts) and call
    /// this with an id that already completed on its own.
    pub async fn unsubscribe(&self, id: u64) {
        // Removing drops the cancel sender, which resolves the streaming task's
        // cancel future → it stops and drops the connection. If the task
        // already finished, the entry is gone and this is a no-op.
        let _ = self.subs.lock().await.remove(&id);
    }
}

/// Drives one subscription end-to-end: connect, stream, clean up. Always
/// removes `id` from `subs` on the way out (a no-op if `unsubscribe` got there
/// first), so the table never leaks completed subscriptions.
async fn run_subscription(
    path: Endpoint,
    budget: Duration,
    body: String,
    sink: Arc<dyn FrameSink>,
    mut cancel_rx: oneshot::Receiver<()>,
    subs: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>>,
    id: u64,
) {
    // Honor an unsubscribe that lands while we're still dialing.
    let opened = tokio::select! {
        biased;
        _ = &mut cancel_rx => { subs.lock().await.remove(&id); return; }
        r = open_stream(&path, budget, body) => r,
    };

    match opened {
        Ok(resp) => {
            // Signal the live connection before streaming its snapshot, so the
            // frontend resets accumulators here (and only here — a failed dial
            // takes the `Err` arm and emits no `open`).
            sink.send_frame(OPEN_FRAME.to_string());
            stream_to_sink(resp, &sink, cancel_rx).await
        }
        Err(err) => {
            // Connect / handshake / non-200: report it as a transport error.
            // The frontend treats `error` the same as a drop — it reconnects.
            sink.send_frame(error_frame(&err.to_string()));
        }
    }

    subs.lock().await.remove(&id);
}

/// Dials the sidecar and issues the SSE `POST /graphql`, returning the live
/// streaming response. Mirrors `query.rs`'s per-call connect, but asks for
/// `text/event-stream` and keeps the body for incremental reads.
async fn open_stream(
    path: &Endpoint,
    budget: Duration,
    body: String,
) -> Result<Response<Incoming>> {
    let stream = ipc::connect_with_budget(path, budget).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| AppError::Io(std::io::Error::other(e)))?;
    // Drive the connection in the background. It must keep running for the
    // streaming body to receive bytes; it ends when the body is dropped.
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::debug!(target: "sidecar", %err, "sse connection ended");
        }
    });

    let req = Request::builder()
        .method("POST")
        .uri("/graphql")
        // hyper requires a Host header on HTTP/1.1 even over UDS.
        .header("host", "sidecar.local")
        .header("content-type", "application/json")
        // The Accept header is what selects gqlgen's SSE transport over POST.
        .header("accept", "text/event-stream")
        .body(Full::new(Bytes::from(body)))
        .map_err(|e| AppError::Io(std::io::Error::other(format!("build request: {e}"))))?;

    let resp = sender
        .send_request(req)
        .await
        .map_err(|e| AppError::Io(std::io::Error::other(e)))?;
    if resp.status() != hyper::StatusCode::OK {
        return Err(AppError::Io(std::io::Error::other(format!(
            "sidecar returned {} for subscription",
            resp.status()
        ))));
    }
    Ok(resp)
}

/// Reads the SSE body, forwarding decoded events to `sink`, until the server
/// completes, the stream ends, or `cancel_rx` fires (consumer unsubscribed).
async fn stream_to_sink(
    resp: Response<Incoming>,
    sink: &Arc<dyn FrameSink>,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    // Adapt hyper's streaming body into a `Bytes` stream and let
    // `eventsource-stream` own the SSE wire parsing: line framing, the
    // single-leading-space strip, multi-line `data:`, comment / keep-alive
    // lines, and buffering events split across chunk boundaries.
    let mut events = resp.into_body().into_data_stream().eventsource();
    loop {
        tokio::select! {
            biased;
            // Consumer torn down: drop the stream (closes the connection, which
            // cancels the sidecar resolver) and emit nothing — they're gone.
            _ = &mut cancel_rx => return,
            next = events.next() => {
                match next {
                    Some(Ok(event)) => {
                        if event.event == "complete" {
                            // Only reachable if a `complete` ever carries data
                            // (gqlgen's is data-less, so the SSE parser discards
                            // it and the graceful end surfaces as the clean EOF
                            // below) — but if one does arrive, it means the same
                            // thing.
                            sink.send_frame(COMPLETE_FRAME.to_string());
                            return;
                        } else if !event.data.is_empty() {
                            // gqlgen tags value events `next`; any non-empty
                            // data frame carries a `graphql.Response`. Empty
                            // dispatches (e.g. the initial `:` comment) are
                            // dropped by the parser and never reach here.
                            sink.send_frame(next_frame(&event.data));
                        }
                    }
                    // Clean end-of-body: the server wrote its chunked
                    // terminator and finished — the graceful completion
                    // (gqlgen's `event: complete` lands here, see module docs).
                    // The frontend reconnects silently.
                    None => {
                        sink.send_frame(COMPLETE_FRAME.to_string());
                        return;
                    }
                    // Mid-stream read/parse failure (truncated chunked body —
                    // sidecar crash, sleep, network loss): emit `closed` so the
                    // frontend reconnects (rather than hanging) *and* reports
                    // the drop.
                    Some(Err(_)) => {
                        sink.send_frame(CLOSED_FRAME.to_string());
                        return;
                    }
                }
            }
        }
    }
}

/// Wraps a gqlgen `graphql.Response` (already JSON) in the `next` envelope.
/// The data is valid JSON straight off the wire, so embedding it verbatim keeps
/// the envelope valid without a parse round-trip.
fn next_frame(data: &str) -> String {
    format!(r#"{{"type":"next","payload":{data}}}"#)
}

/// Builds the `error` envelope, escaping `message` via serde. Keeps `type`
/// first to match the `next`/`complete` envelopes (the frontend parses by key,
/// so order is cosmetic, but consistency keeps logs readable).
fn error_frame(message: &str) -> String {
    let payload = serde_json::to_string(message).unwrap_or_else(|_| "\"\"".to_string());
    format!(r#"{{"type":"error","payload":{payload}}}"#)
}

#[cfg(test)]
mod tests {
    use super::*;
    use interprocess::local_socket::{
        traits::tokio::Listener as _, GenericFilePath, ListenerOptions, ToFsName,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    /// Test sink that forwards into an mpsc — the easy way to assert what the
    /// reader actually delivers without spinning up a Tauri runtime.
    struct MpscSink(mpsc::UnboundedSender<String>);
    impl FrameSink for MpscSink {
        fn send_frame(&self, frame: String) {
            let _ = self.0.send(frame);
        }
    }

    /// Stands up a one-shot HTTP/1 server on `path` that reads the request
    /// headers, writes a `200 text/event-stream` head followed by `events` as
    /// one chunk of a chunked body (matching gqlgen, whose streaming responses
    /// are chunked — that framing is what lets the client tell the endings
    /// apart), then runs `after` (e.g. return immediately, or wait to keep the
    /// stream open). `clean_end` decides the ending: `true` writes the chunked
    /// terminator before closing (a graceful completion), `false` just closes
    /// (a truncated body — the abnormal drop). Uses `interprocess` so the same
    /// fixture drives Unix UDS and Windows named-pipe tests.
    async fn spawn_sse_server<Fut>(
        path: String,
        events: &'static str,
        clean_end: bool,
        after: impl FnOnce() -> Fut + Send + 'static,
    ) where
        Fut: std::future::Future<Output = ()> + Send,
    {
        let name = path
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .expect("to_fs_name");
        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind listener");
        tokio::spawn(async move {
            let Ok(mut stream) = listener.accept().await else {
                return;
            };
            // Drain the request headers.
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
            let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                        Cache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n";
            let _ = stream.write_all(head.as_bytes()).await;
            if !events.is_empty() {
                let chunk = format!("{:x}\r\n{events}\r\n", events.len());
                let _ = stream.write_all(chunk.as_bytes()).await;
            }
            let _ = stream.flush().await;
            after().await;
            if clean_end {
                let _ = stream.write_all(b"0\r\n\r\n").await;
                let _ = stream.flush().await;
            }
            let _ = stream.shutdown().await;
        });
        // UnixListener::bind is synchronous so the socket exists, but the
        // accept loop hasn't started polling yet.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    fn sink_pair() -> (Arc<dyn FrameSink>, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Arc::new(MpscSink(tx)), rx)
    }

    async fn recv(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("frame within 2s")
            .expect("sink open")
    }

    /// Happy path: two `next` events then `complete` arrive as the matching
    /// envelopes, with the raw `data` JSON passed through as `payload`.
    #[tokio::test]
    async fn forwards_next_then_complete() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_sse_server(
            arg.clone(),
            "event: next\ndata: {\"data\":{\"tick\":1}}\n\n\
             event: next\ndata: {\"data\":{\"tick\":2}}\n\n\
             event: complete\n\n",
            true, // graceful: gqlgen's data-less `complete` + chunked terminator
            || async {},
        )
        .await;

        let client = SubscriptionClient::new(path);
        let (sink, mut rx) = sink_pair();
        let _id = client
            .subscribe(
                "subscription { tick }".to_string(),
                serde_json::json!({}),
                sink,
            )
            .await
            .expect("subscribe");

        assert_eq!(recv(&mut rx).await, OPEN_FRAME);
        assert_eq!(
            recv(&mut rx).await,
            r#"{"type":"next","payload":{"data":{"tick":1}}}"#
        );
        assert_eq!(
            recv(&mut rx).await,
            r#"{"type":"next","payload":{"data":{"tick":2}}}"#
        );
        assert_eq!(recv(&mut rx).await, COMPLETE_FRAME);
        let _ = std::fs::remove_file(arg);
    }

    /// A connection that dies mid-stream (sidecar crash, sleep, network loss —
    /// modelled as a chunked body truncated without its terminator) yields
    /// `closed` — distinct from the graceful `complete` — so the frontend
    /// reconnects *and* reports it.
    #[tokio::test]
    async fn truncated_stream_synthesizes_closed() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_sse_server(
            arg.clone(),
            "event: next\ndata: {\"data\":{\"tick\":1}}\n\n",
            false, // abrupt: close without the chunked terminator
            || async {},
        )
        .await;

        let client = SubscriptionClient::new(path);
        let (sink, mut rx) = sink_pair();
        client
            .subscribe(
                "subscription { tick }".to_string(),
                serde_json::json!({}),
                sink,
            )
            .await
            .expect("subscribe");

        assert_eq!(recv(&mut rx).await, OPEN_FRAME);
        assert_eq!(
            recv(&mut rx).await,
            r#"{"type":"next","payload":{"data":{"tick":1}}}"#
        );
        assert_eq!(recv(&mut rx).await, CLOSED_FRAME);
        let _ = std::fs::remove_file(arg);
    }

    /// The leading `:` comment and `: ping` keep-alives are dropped; only the
    /// real events reach the sink.
    #[tokio::test]
    async fn ignores_comments_and_keepalives() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_sse_server(
            arg.clone(),
            ":\n\n: ping\n\nevent: next\ndata: {\"data\":1}\n\nevent: complete\n\n",
            true,
            || async {},
        )
        .await;

        let client = SubscriptionClient::new(path);
        let (sink, mut rx) = sink_pair();
        client
            .subscribe(
                "subscription { tick }".to_string(),
                serde_json::json!({}),
                sink,
            )
            .await
            .expect("subscribe");

        assert_eq!(recv(&mut rx).await, OPEN_FRAME);
        assert_eq!(
            recv(&mut rx).await,
            r#"{"type":"next","payload":{"data":1}}"#
        );
        assert_eq!(recv(&mut rx).await, COMPLETE_FRAME);
        let _ = std::fs::remove_file(arg);
    }

    /// Unsubscribe drops the connection without delivering a `complete` — the
    /// consumer is gone, so nothing more should land on the sink.
    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        spawn_sse_server(
            arg.clone(),
            "event: next\ndata: {\"data\":1}\n\n",
            false,
            // Hold the stream open until released so the client must actively
            // tear it down rather than seeing a natural EOF.
            || async move {
                let _ = release_rx.await;
            },
        )
        .await;

        let client = SubscriptionClient::new(path);
        let (sink, mut rx) = sink_pair();
        let id = client
            .subscribe(
                "subscription { tick }".to_string(),
                serde_json::json!({}),
                sink,
            )
            .await
            .expect("subscribe");

        assert_eq!(recv(&mut rx).await, OPEN_FRAME);
        assert_eq!(
            recv(&mut rx).await,
            r#"{"type":"next","payload":{"data":1}}"#
        );
        client.unsubscribe(id).await;

        // No `complete` (or any other frame) should arrive — the consumer
        // unsubscribed. The sink simply closes when the task tears down, which
        // surfaces as a timeout (nothing pending) or a clean channel close.
        match tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
            Err(_) => {}   // timed out with nothing pending — good
            Ok(None) => {} // sink closed without a frame — good
            Ok(Some(f)) => panic!("unexpected frame after unsubscribe: {f}"),
        }

        // The entry is gone from the table.
        assert!(client.subs.lock().await.is_empty());
        let _ = release_tx.send(());
        let _ = std::fs::remove_file(arg);
    }

    /// A dead socket surfaces as an `error` frame (not a hang) within the
    /// connect budget — the frontend's reconnect path. The `error` is the
    /// *first* frame: no `open` precedes it, so the frontend won't reset
    /// accumulators on a failed dial.
    #[tokio::test]
    async fn connect_failure_emits_error_frame() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let client = SubscriptionClient::new_with_budget(path, Duration::from_millis(150));
        let (sink, mut rx) = sink_pair();
        client
            .subscribe(
                "subscription { tick }".to_string(),
                serde_json::json!({}),
                sink,
            )
            .await
            .expect("subscribe returns Ok even when the socket is dead");

        let frame = recv(&mut rx).await;
        assert!(frame.starts_with(r#"{"type":"error"#), "got {frame}");
    }
}
