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

//! Multiplexed `graphql-transport-ws` registry.
//!
//! One shared WebSocket carries every GraphQL subscription the webview opens.
//! That matches what the protocol is designed for (the per-frame `id` exists
//! exactly to demultiplex) and avoids paying a `connection_init`/`ack` per
//! subscription. Over UDS the only realistic way the transport drops is a
//! sidecar restart, which kills *all* subscriptions on both ends anyway —
//! so per-subscription connection isolation buys nothing real.
//!
//! On a WS drop the reader task fans a synthetic `{"type":"complete"}`
//! envelope out to every live sink. Each frontend subscription then runs
//! its own capped-backoff reconnect (subscribe-exchange.ts:88–119), which
//! triggers a fresh `subscribe` here — that lazily re-handshakes the WS.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::Request as WsRequest;
use tokio_tungstenite::tungstenite::Message;

use std::time::Duration;

use super::super::ipc::{self, Endpoint, DEFAULT_CONNECT_BUDGET};
use crate::error::{AppError, Result};

/// Where the host sends each demultiplexed envelope. Production wraps a
/// Tauri `Channel<String>`; tests use an mpsc adapter so they don't have
/// to stand up a Tauri runtime.
pub trait FrameSink: Send + Sync {
    /// Forwards one `SubMessage`-shaped JSON envelope to the consumer.
    /// Best-effort: a closed sink simply drops the message — the consumer
    /// has already torn down.
    fn send_frame(&self, frame: String);
}

/// The shape we expect on every inbound graphql-transport-ws frame. We
/// only care about the type and (sometimes) the routing id; `payload` is
/// passed through to the sink unparsed.
#[derive(Deserialize)]
struct InboundHead {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    id: Option<String>,
}

/// One live subscription's bookkeeping.
struct SubEntry {
    /// Wire-level id (what the sidecar tags frames with).
    wire_id: String,
    /// Where to forward inbound frames for this subscription.
    sink: Arc<dyn FrameSink>,
}

/// Mutable state of the shared WS connection. `None` between sessions:
/// the next `subscribe` re-establishes it.
struct WsState {
    /// Outgoing frames the writer task drains. Writes from `subscribe`,
    /// `unsubscribe`, and the reader's Pong-on-Ping path push here so
    /// they don't have to serialize against each other on a shared sink
    /// lock.
    writer_tx: mpsc::UnboundedSender<Message>,
    /// Live subscriptions, keyed by host-side op id (the u64 returned to
    /// Tauri). Shared with the reader task so it can route inbound frames
    /// and fan-out `complete` on disconnect.
    subs: Arc<Mutex<HashMap<u64, SubEntry>>>,
    /// Set true by the reader task before draining `subs`. Lets a racing
    /// `subscribe` (one that cloned this Arc just before the WS closed)
    /// bail out rather than inserting an entry into a session that will
    /// never deliver another frame.
    dead: Arc<AtomicBool>,
}

/// Owns the host's single graphql-transport-ws connection to the sidecar
/// and the table of subscriptions multiplexed over it.
///
/// Concurrency: two locks split distinct responsibilities so unrelated
/// operations don't serialize.
///   * `state` — protects the active-session slot only. Held briefly to
///     read (clone the `Arc<WsState>` out) or to install a freshly-built
///     session. Never held across an `.await` that touches the network.
///   * `connecting` — single-flight gate. Only one task at a time runs
///     `open_session`; concurrent first-time subscribers behind it wait
///     on the gate, but `unsubscribe` and the per-subscription send path
///     never touch this lock. Sized to keep dial/ack stalls from
///     blocking the teardown path.
pub struct SubscriptionClient {
    path: Endpoint,
    /// `None` until the first subscribe initializes the WS. Behind an
    /// `Arc` so the reader task can also clear it when the session
    /// terminates, without holding a `&self` reference.
    state: Arc<Mutex<Option<Arc<WsState>>>>,
    /// Serializes session opens. Only the slow path (`ensure_state`)
    /// acquires it; `unsubscribe` and the per-frame send path don't.
    connecting: Mutex<()>,
    /// Host-side op id counter. Monotonic for the process lifetime so
    /// the frontend can rely on distinct ids across reconnects.
    next_id: AtomicU64,
    /// How long open_session may spend in any single phase (UDS dial,
    /// connection_ack read) before surfacing a transport error. Tests
    /// shrink this; production uses [`DEFAULT_CONNECT_BUDGET`].
    connect_budget: Duration,
}

impl SubscriptionClient {
    pub fn new(path: Endpoint) -> Self {
        Self::new_with_budget(path, DEFAULT_CONNECT_BUDGET)
    }

    /// Same as [`SubscriptionClient::new`] but with a caller-supplied
    /// budget — used by tests to bound how long the no-ack-from-server
    /// scenario takes to surface a timeout error.
    pub fn new_with_budget(path: Endpoint, connect_budget: Duration) -> Self {
        Self {
            path,
            state: Arc::new(Mutex::new(None)),
            connecting: Mutex::new(()),
            next_id: AtomicU64::new(1),
            connect_budget,
        }
    }

    /// Returns a live `WsState`, opening one if necessary.
    ///
    /// Fast path: a session is already live — clone the `Arc` out under
    /// the `state` lock and release it immediately.
    ///
    /// Slow path: acquire `connecting` so only one task does the
    /// handshake; re-check `state` after winning the gate (another task
    /// may have raced ahead); call `open_session` *without* holding
    /// `state`; install the result under `state`. While `open_session`
    /// is running (up to `connect_budget`), other tasks calling
    /// `unsubscribe` or holding `Arc<WsState>` from a prior session can
    /// proceed undisturbed.
    async fn ensure_state(&self) -> Result<Arc<WsState>> {
        if let Some(s) = self.state.lock().await.as_ref() {
            return Ok(Arc::clone(s));
        }
        let _gate = self.connecting.lock().await;
        if let Some(s) = self.state.lock().await.as_ref() {
            return Ok(Arc::clone(s));
        }
        let new_state = Arc::new(self.open_session().await?);
        *self.state.lock().await = Some(Arc::clone(&new_state));
        Ok(new_state)
    }

    /// Registers a new subscription on the shared WS.
    ///
    /// Returns the host-side op id urql can later pass to
    /// [`SubscriptionClient::unsubscribe`]. Lazily (re)connects the WS
    /// if the prior session ended.
    pub async fn subscribe(
        &self,
        query: String,
        variables: serde_json::Value,
        sink: Arc<dyn FrameSink>,
    ) -> Result<u64> {
        let state = self.ensure_state().await?;
        let host_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Wire ids are strings on the protocol (per graphql-transport-ws).
        // Reuse the host id as the wire id — they don't have to match,
        // but keeping them aligned simplifies log correlation.
        let wire_id = host_id.to_string();

        let frame = json!({
            "id": wire_id,
            "type": "subscribe",
            "payload": { "query": query, "variables": variables },
        })
        .to_string();

        // Insert AND send under the same `subs` lock. The reader task
        // pairs `dead.store(true)` with draining `subs` under this same
        // lock, so atomicity here gives us three clean outcomes:
        //
        //   * `dead` already set → bail before inserting (session is
        //      doomed; next subscribe rebuilds).
        //   * insert + send both succeed → return Ok. The entry rides
        //      whatever happens next: live frames, terminal `complete`/
        //     `error` from the server, or the reader's drain delivering
        //      a synthetic `complete` to the sink if the WS later dies.
        //   * insert succeeds but `writer_tx.send` fails (writer task
        //      already gone) → remove the entry under the SAME lock so
        //      a racing drain finds an empty slot and does NOT fan a
        //      synthetic `complete` to the caller's sink. The caller
        //      then sees a single, unambiguous Err — no duplicate
        //      "Err from the command and a complete on the channel."
        //
        // Holding the lock through `writer_tx.send` is safe: it's a
        // sync unbounded mpsc send that never yields, so no `.await`
        // happens inside the critical section.
        let send_failed = {
            let mut subs = state.subs.lock().await;
            if state.dead.load(Ordering::Acquire) {
                return Err(AppError::Io(std::io::Error::other(
                    "subscription transport closed before subscribe",
                )));
            }
            subs.insert(
                host_id,
                SubEntry {
                    wire_id: wire_id.clone(),
                    sink,
                },
            );
            if state.writer_tx.send(Message::Text(frame)).is_err() {
                subs.remove(&host_id);
                true
            } else {
                false
            }
        };
        if send_failed {
            // Evict the dead session so the next subscribe rebuilds.
            // Only clear if `state` still points at the same Arc — a
            // parallel reconnect may have already replaced it.
            let mut guard = self.state.lock().await;
            if guard.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, &state)) {
                *guard = None;
            }
            return Err(AppError::Io(std::io::Error::other(
                "subscription transport closed before subscribe frame was sent",
            )));
        }
        Ok(host_id)
    }

    /// Cancels a subscription by host-side op id.
    ///
    /// Tolerant of unknown ids — urql's subscribe-exchange can race
    /// teardown with the `graphql_subscribe` resolve (subscribe-exchange.ts:129)
    /// and will call this with an id that was never registered.
    ///
    /// Crucially, this path does NOT take the `connecting` gate and only
    /// holds `state` long enough to clone an `Arc`. A subscribe that is
    /// currently parked in `open_session` does not block teardown.
    pub async fn unsubscribe(&self, id: u64) {
        let state = match self.state.lock().await.as_ref() {
            Some(s) => Arc::clone(s),
            None => return,
        };
        let mut subs = state.subs.lock().await;
        let Some(entry) = subs.remove(&id) else {
            return;
        };

        // Best-effort `complete` to the sidecar. If the writer task is
        // gone the session is already done; nothing to send.
        let frame = json!({ "id": entry.wire_id, "type": "complete" }).to_string();
        let _ = state.writer_tx.send(Message::Text(frame));
    }

    /// Dials the sidecar, performs the `connection_init`/`ack` handshake,
    /// and spawns the reader + writer tasks. Returns the live session
    /// handle. Errors here keep `state` at `None`, so the next subscribe
    /// retries from scratch.
    async fn open_session(&self) -> Result<WsState> {
        // Use the registry's own budget for the dial too (not just the
        // ack), so the no-listener case fails inside the configured
        // bound — important for tests, and for callers who care about
        // bounding total session-open latency.
        let stream = ipc::connect_with_budget(&self.path, self.connect_budget).await?;

        // `client_async` with a hand-built `Request<()>` does NOT auto-fill
        // the required ws handshake headers; we must supply Upgrade/
        // Connection/Sec-WebSocket-{Key,Version} ourselves. Host is
        // semantically meaningless over UDS but HTTP/1.1 requires it.
        // Subprotocol matches the sidecar (server.go:167; server_test.go:201).
        let req = WsRequest::builder()
            .uri("ws://sidecar.local/graphql")
            .header("Host", "sidecar.local")
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Protocol", "graphql-transport-ws")
            .body(())
            .map_err(|e| AppError::Io(std::io::Error::other(e)))?;

        let (ws, _resp) = tokio_tungstenite::client_async(req, stream)
            .await
            .map_err(|e| AppError::Io(std::io::Error::other(e)))?;
        let (mut write, mut read) = ws.split();

        // Drive the connection_init first; everything else waits on the ack.
        write
            .send(Message::Text(r#"{"type":"connection_init"}"#.into()))
            .await
            .map_err(|e| AppError::Io(std::io::Error::other(e)))?;

        // Wait for the ack inline (don't spawn the reader task until the
        // session is live — a transport error here should map to a plain
        // `Err` from subscribe, not a half-set-up state). The timeout
        // bounds a sidecar that accepts the upgrade but stalls before
        // sending `connection_ack`; without it `read.next().await` would
        // park forever, freezing every subsequent subscribe/unsubscribe.
        //
        // Loop because a server keepalive Ping (or non-text control frame)
        // may legitimately arrive before the GraphQL handshake's ack;
        // only Text frames carry app-level messages. Pings get an
        // immediate Pong reply via the still-owned write half so we don't
        // have to relay through the (not-yet-spawned) writer task.
        let ack_text = loop {
            match tokio::time::timeout(self.connect_budget, read.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => break t,
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    let _ = write.send(Message::Pong(payload)).await;
                    continue;
                }
                Ok(Some(Ok(Message::Pong(_))))
                | Ok(Some(Ok(Message::Binary(_))))
                | Ok(Some(Ok(Message::Frame(_)))) => continue,
                Ok(Some(Ok(Message::Close(_)))) => {
                    return Err(AppError::Io(std::io::Error::other(
                        "ws closed before connection_ack",
                    )))
                }
                Ok(Some(Err(e))) => return Err(AppError::Io(std::io::Error::other(e))),
                Ok(None) => {
                    return Err(AppError::Io(std::io::Error::other(
                        "ws closed before connection_ack",
                    )))
                }
                Err(_elapsed) => {
                    return Err(AppError::Io(std::io::Error::other(
                        "timed out waiting for connection_ack",
                    )))
                }
            }
        };
        let head: InboundHead = serde_json::from_str(&ack_text)?;
        if head.kind != "connection_ack" {
            return Err(AppError::Io(std::io::Error::other(format!(
                "expected connection_ack, got {}",
                head.kind
            ))));
        }

        let subs: Arc<Mutex<HashMap<u64, SubEntry>>> = Arc::new(Mutex::new(HashMap::new()));
        let dead = Arc::new(AtomicBool::new(false));
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<Message>();

        // Writer task: serializes outgoing frames (Text from subscribe/
        // unsubscribe, Pong from the reader's keepalive handler) so they
        // never contend for the WS sink.
        tokio::spawn(async move {
            while let Some(msg) = writer_rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
            // Best-effort close so the sidecar sees a clean 1000, not a reset.
            let _ = write.send(Message::Close(None)).await;
        });

        // Reader task: routes inbound frames to subscription sinks, echoes
        // server-issued Pings back as Pongs (tokio-tungstenite no longer
        // auto-ponds once the stream is split), and fans out a synthetic
        // `complete` to every live sink when the session ends. On exit it
        // sets `dead` BEFORE draining `subs` so any racing `subscribe`
        // that already cloned this Arc<WsState> bails instead of leaving
        // an orphan entry behind. Then it evicts the session slot if it
        // still points at this session — the `same_channel` check
        // prevents clobbering a parallel reconnect that already installed
        // fresh state.
        let subs_for_reader = Arc::clone(&subs);
        let state_slot = Arc::clone(&self.state);
        let writer_tx_for_reader = writer_tx.clone();
        let dead_for_reader = Arc::clone(&dead);
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                let text = match msg {
                    Ok(Message::Text(t)) => t,
                    Ok(Message::Ping(payload)) => {
                        // Route the Pong through the writer task so we
                        // don't need a second handle to the WS sink.
                        let _ = writer_tx_for_reader.send(Message::Pong(payload));
                        continue;
                    }
                    Ok(Message::Pong(_)) | Ok(Message::Binary(_)) => continue,
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Frame(_)) => continue,
                };
                forward_frame(&subs_for_reader, &text).await;
            }
            // Order matters: set `dead` under the subs lock (paired with
            // the check in `subscribe`) before draining, so a racing
            // subscribe either lands an entry the drain will see, or
            // returns Err before inserting.
            {
                let mut map = subs_for_reader.lock().await;
                dead_for_reader.store(true, Ordering::Release);
                let envelope = r#"{"type":"complete"}"#.to_string();
                for (_, entry) in map.drain() {
                    entry.sink.send_frame(envelope.clone());
                }
            }
            let mut guard = state_slot.lock().await;
            if guard
                .as_ref()
                .is_some_and(|s| s.writer_tx.same_channel(&writer_tx_for_reader))
            {
                *guard = None;
            }
        });

        Ok(WsState {
            writer_tx,
            subs,
            dead,
        })
    }
}

/// Routes one inbound text frame to the right sink.
///
/// Frames carry an `id` field on graphql-transport-ws; we parse it back
/// to the host-side `u64` for an O(1) HashMap lookup (the wire id is
/// always `host_id.to_string()` — set in [`SubscriptionClient::subscribe`]).
/// The forwarded envelope has `id` stripped, since the frontend's
/// `SubMessage` shape doesn't include it — it's pinned to the
/// per-subscription channel already. If the frame is *terminal*
/// (`complete` or `error`), the entry is removed afterward — leaving it
/// would leak SubEntry rows across the frontend's reconnect cycle
/// (subscribe-exchange.ts:88 spawns a fresh op without dropping the old).
///
/// `send_frame` is called *outside* the `subs` lock: it's synchronous,
/// and a slow sink would otherwise serialize every other subscription's
/// dispatch and every concurrent subscribe/unsubscribe behind itself.
async fn forward_frame(subs: &Mutex<HashMap<u64, SubEntry>>, raw: &str) {
    let head: InboundHead = match serde_json::from_str(raw) {
        Ok(h) => h,
        Err(_) => return,
    };
    let Some(wire_id) = head.id.as_deref() else {
        // Server keepalive `ping`/`pong` envelopes (graphql-transport-ws
        // spec, type "ping"/"pong") and connection-level frames have no
        // id and don't go to any sink.
        return;
    };
    let Ok(host_id) = wire_id.parse::<u64>() else {
        // A wire id we never minted — silently drop. The sidecar would
        // have to be misbehaving for this to happen.
        return;
    };
    let terminal = head.kind == "complete" || head.kind == "error";
    let envelope = strip_id_field(raw);

    let sink = {
        let mut map = subs.lock().await;
        if terminal {
            match map.remove(&host_id) {
                Some(entry) => entry.sink,
                None => return,
            }
        } else {
            match map.get(&host_id) {
                Some(entry) => Arc::clone(&entry.sink),
                None => return,
            }
        }
    };
    sink.send_frame(envelope);
}

/// Best-effort: remove the `"id":"..."` field from the raw JSON object so
/// the envelope matches the frontend's `SubMessage` shape (which has no
/// id). Falling back to a re-encoded `{type,payload}` would force a full
/// parse on the hot path — this string-edit keeps it cheap.
fn strip_id_field(raw: &str) -> String {
    // Parse-and-re-encode is simpler and still cheap; readability wins.
    let parsed: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return raw.to_string(),
    };
    let mut obj = match parsed {
        serde_json::Value::Object(m) => m,
        _ => return raw.to_string(),
    };
    obj.remove("id");
    serde_json::Value::Object(obj).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use interprocess::local_socket::{
        tokio::Stream as IpcStream, traits::tokio::Listener as _, GenericFilePath, ListenerOptions,
        ToFsName,
    };
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as ServerRequest, Response as ServerResponse,
    };
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    /// Test sink that forwards into an mpsc — the easy way to assert what
    /// the registry actually delivers without spinning up a Tauri runtime.
    struct MpscSink(mpsc::UnboundedSender<String>);
    impl FrameSink for MpscSink {
        fn send_frame(&self, frame: String) {
            let _ = self.0.send(frame);
        }
    }

    /// Stands up a tokio-tungstenite WS server on `path` that runs a small
    /// scripted state machine: ack the connection_init, then for every
    /// inbound text frame call `on_msg(text, &mut sender)` so the test
    /// can drive responses. Returns once `bind` succeeds.
    ///
    /// Uses `interprocess`'s cross-platform listener so the same fixture
    /// drives Unix UDS and Windows named-pipe tests.
    async fn spawn_ws_server<F>(path: String, on_msg: F)
    where
        F: FnMut(
                String,
                &mut futures_util::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<IpcStream>,
                    Message,
                >,
            ) -> futures_util::future::BoxFuture<'_, ()>
            + Send
            + 'static,
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
            let mut on_msg = on_msg;
            let stream = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            // Echo the client's requested subprotocol back so tungstenite's
            // client-side check accepts the upgrade. The real sidecar's
            // gqlgen `transport.Websocket` does the same thing.
            let ws =
                match accept_hdr_async(stream, |req: &ServerRequest, mut resp: ServerResponse| {
                    if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol").cloned() {
                        resp.headers_mut()
                            .insert("Sec-WebSocket-Protocol", HeaderValue::from(proto));
                    }
                    Ok(resp)
                })
                .await
                {
                    Ok(w) => w,
                    Err(_) => return,
                };
            let (mut write, mut read) = ws.split();

            // Handshake: read connection_init, reply with connection_ack.
            if let Some(Ok(Message::Text(_init))) = read.next().await {
                let _ = write
                    .send(Message::Text(r#"{"type":"connection_ack"}"#.into()))
                    .await;
            }

            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(t) = msg {
                    on_msg(t.to_string(), &mut write).await;
                }
            }
        });
        // Brief settling pause — UnixListener::bind is synchronous so the
        // socket exists, but the accept loop hasn't started polling yet.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    /// One subscription, one `next` from the server: the envelope reaches
    /// the sink with `id` stripped and `type`/`payload` intact.
    #[tokio::test]
    async fn subscribe_forwards_next_frames_to_sink() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_ws_server(arg.clone(), |text, write| {
            Box::pin(async move {
                // Inbound is the subscribe frame; echo a `next` tagged
                // with the same id.
                let head: InboundHead = serde_json::from_str(&text).unwrap();
                let id = head.id.expect("subscribe must carry id");
                let resp = json!({
                    "id": id,
                    "type": "next",
                    "payload": { "data": { "tick": 1 } },
                })
                .to_string();
                let _ = write.send(Message::Text(resp.into())).await;
            })
        })
        .await;

        let reg = SubscriptionClient::new(path);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn FrameSink> = Arc::new(MpscSink(tx));
        let _id = reg
            .subscribe("subscription { tick }".to_string(), json!({}), sink)
            .await
            .expect("subscribe");

        let frame = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("frame should arrive")
            .expect("channel still open");
        let env: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(env["type"], "next");
        assert_eq!(env["payload"]["data"]["tick"], 1);
        assert!(env.get("id").is_none(), "id must be stripped from envelope");

        let _ = std::fs::remove_file(arg);
    }

    /// Two concurrent subscriptions share one WS: each sink sees only its
    /// own server-tagged frames. Locks the multiplex routing contract.
    #[tokio::test]
    async fn subscribe_demultiplexes_by_wire_id() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_ws_server(arg.clone(), |text, write| {
            Box::pin(async move {
                let head: InboundHead = serde_json::from_str(&text).unwrap();
                let id = head.id.expect("subscribe id");
                // For each subscribe, emit one next frame tagged with the
                // same id and a payload that names the id so the test can
                // verify routing was correct.
                let resp = json!({
                    "id": id,
                    "type": "next",
                    "payload": { "data": { "from": id } },
                })
                .to_string();
                let _ = write.send(Message::Text(resp.into())).await;
            })
        })
        .await;

        let reg = SubscriptionClient::new(path);

        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        let sink_a: Arc<dyn FrameSink> = Arc::new(MpscSink(tx_a));
        let sink_b: Arc<dyn FrameSink> = Arc::new(MpscSink(tx_b));

        let id_a = reg
            .subscribe("subscription { a }".into(), json!({}), sink_a)
            .await
            .expect("subscribe a");
        let id_b = reg
            .subscribe("subscription { b }".into(), json!({}), sink_b)
            .await
            .expect("subscribe b");
        assert_ne!(id_a, id_b);
        assert!(id_b > id_a, "ids should be monotonic");

        let frame_a = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
            .await
            .unwrap()
            .unwrap();
        let frame_b = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .unwrap()
            .unwrap();

        let ea: serde_json::Value = serde_json::from_str(&frame_a).unwrap();
        let eb: serde_json::Value = serde_json::from_str(&frame_b).unwrap();
        assert_eq!(ea["payload"]["data"]["from"], id_a.to_string());
        assert_eq!(eb["payload"]["data"]["from"], id_b.to_string());

        let _ = std::fs::remove_file(arg);
    }

    /// `unsubscribe` sends a `complete` frame tagged with the wire id so
    /// the sidecar tears the sub down on its side too. Server captures
    /// the frame in a oneshot for the test to assert against.
    #[tokio::test]
    async fn unsubscribe_sends_complete_frame_with_wire_id() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();

        let (seen_complete_tx, seen_complete_rx) = oneshot::channel::<String>();
        let seen_complete_tx = Arc::new(Mutex::new(Some(seen_complete_tx)));
        spawn_ws_server(arg.clone(), move |text, _write| {
            let seen_complete_tx = Arc::clone(&seen_complete_tx);
            Box::pin(async move {
                let head: InboundHead = match serde_json::from_str(&text) {
                    Ok(h) => h,
                    Err(_) => return,
                };
                if head.kind == "complete" {
                    if let Some(tx) = seen_complete_tx.lock().await.take() {
                        let _ = tx.send(head.id.unwrap_or_default());
                    }
                }
            })
        })
        .await;

        let reg = SubscriptionClient::new(path);
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn FrameSink> = Arc::new(MpscSink(tx));
        let id = reg
            .subscribe("subscription { x }".into(), json!({}), sink)
            .await
            .expect("subscribe");

        reg.unsubscribe(id).await;

        let wire_id = tokio::time::timeout(Duration::from_secs(2), seen_complete_rx)
            .await
            .expect("server should see complete frame")
            .expect("oneshot delivered");
        assert_eq!(wire_id, id.to_string());

        let _ = std::fs::remove_file(arg);
    }

    /// On reconnect (or first-time connect) `subscribe` may park for the
    /// full connect budget waiting on `open_session`. Other operations
    /// MUST NOT be serialized behind that wait — in particular,
    /// `unsubscribe` is invoked by the frontend on teardown and would
    /// freeze the webview if it had to wait for an unrelated subscribe
    /// to finish reconnecting.
    #[tokio::test]
    async fn unsubscribe_does_not_block_on_in_flight_handshake() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        // No server yet — the subscribe below will sit retrying the UDS
        // dial until the budget elapses (or we cancel its handle).
        let reg = Arc::new(SubscriptionClient::new_with_budget(
            path,
            Duration::from_millis(800),
        ));

        let reg_clone = Arc::clone(&reg);
        let stuck_subscribe = tokio::spawn(async move {
            let (tx, _rx) = mpsc::unbounded_channel();
            let sink: Arc<dyn FrameSink> = Arc::new(MpscSink(tx));
            // Will Err after the budget elapses since no listener appears.
            let _ = reg_clone
                .subscribe("subscription { x }".into(), json!({}), sink)
                .await;
        });

        // Let the spawned subscribe enter open_session and start polling
        // the dial. 50ms is plenty: Endpoint::connect's first retry
        // happens at 50ms, and the spawned task definitely entered the
        // session-setup path by then.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let started = std::time::Instant::now();
        reg.unsubscribe(99).await;
        let elapsed = started.elapsed();
        // Generous slack: anything under 100ms proves the unsubscribe is
        // NOT serialized behind the ~800ms dial budget.
        assert!(
            elapsed < Duration::from_millis(100),
            "unsubscribe must not block on in-flight handshake; took {elapsed:?}"
        );

        let _ = stuck_subscribe.await;
        let _ = std::fs::remove_file(arg);
    }

    /// Unsubscribing an id the registry never handed out must be a no-op,
    /// not a panic. The frontend races teardown against the
    /// `graphql_subscribe` resolve and will hit this path.
    #[tokio::test]
    async fn unsubscribe_unknown_id_is_no_op() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let reg = SubscriptionClient::new(path);
        // No WS was ever opened — unsubscribe must still complete.
        reg.unsubscribe(99).await;
    }

    /// A sidecar that completes the WS upgrade but never sends
    /// `connection_ack` must not freeze subscribe forever. The
    /// per-registry budget bounds the wait.
    #[tokio::test]
    async fn subscribe_fails_fast_when_ack_never_arrives() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        // Server that accepts the upgrade but never sends ack.
        let name = arg
            .as_str()
            .to_fs_name::<GenericFilePath>()
            .expect("to_fs_name");
        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind listener");
        tokio::spawn(async move {
            if let Ok(stream) = listener.accept().await {
                let _ws =
                    accept_hdr_async(stream, |req: &ServerRequest, mut resp: ServerResponse| {
                        if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol").cloned() {
                            resp.headers_mut()
                                .insert("Sec-WebSocket-Protocol", HeaderValue::from(proto));
                        }
                        Ok(resp)
                    })
                    .await
                    .ok();
                // Park forever — never write connection_ack.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let reg = SubscriptionClient::new_with_budget(path, Duration::from_millis(300));
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn FrameSink> = Arc::new(MpscSink(tx));

        let started = std::time::Instant::now();
        let err = reg
            .subscribe("subscription { x }".into(), json!({}), sink)
            .await
            .expect_err("must surface a timeout error, not hang");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "subscribe should fail near the budget, took {elapsed:?}"
        );
        assert!(matches!(err, AppError::Io(_)), "got: {err}");
        let _ = std::fs::remove_file(arg);
    }

    /// A server-sent `complete` (or `error`) frame ends the subscription.
    /// The registry must remove the SubEntry — otherwise the frontend's
    /// reconnect (subscribe-exchange.ts:88) creates a fresh op without
    /// dropOp-ing the old, and orphan entries accumulate on the shared
    /// WS until it drops.
    #[tokio::test]
    async fn forward_frame_removes_entry_on_terminal_frame() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_ws_server(arg.clone(), |text, write| {
            Box::pin(async move {
                let head: InboundHead = match serde_json::from_str(&text) {
                    Ok(h) => h,
                    Err(_) => return,
                };
                // Only respond to the subscribe — ignore the client's own
                // `complete` (which `unsubscribe` would send, but the
                // registry shouldn't send one when the server has already
                // closed the op from its side).
                if head.kind != "subscribe" {
                    return;
                }
                let id = head.id.expect("subscribe id");
                // Push one `next`, then immediately `complete` to end the op.
                let next = json!({
                    "id": id,
                    "type": "next",
                    "payload": { "data": { "ok": true } },
                })
                .to_string();
                let _ = write.send(Message::Text(next.into())).await;
                let done = json!({ "id": id, "type": "complete" }).to_string();
                let _ = write.send(Message::Text(done.into())).await;
            })
        })
        .await;

        let reg = SubscriptionClient::new(path);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = reg
            .subscribe(
                "subscription { x }".into(),
                json!({}),
                Arc::new(MpscSink(tx)),
            )
            .await
            .expect("subscribe");

        // Drain the next + complete envelopes.
        let _next = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap();
        let _done = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap();

        // The entry must be gone now: subsequent unsubscribe must hit the
        // unknown-id path (no panic, no stale `complete` frame to the
        // server). We assert by peeking at the registry's internal table.
        let state_guard = reg.state.lock().await;
        let state = state_guard.as_ref().expect("session was opened");
        let subs = state.subs.lock().await;
        assert!(
            !subs.contains_key(&id),
            "entry for {id} should be removed after terminal frame; map: {:?}",
            subs.keys().collect::<Vec<_>>()
        );
        let _ = std::fs::remove_file(arg);
    }

    /// When the server closes the WS, every live sink gets a synthetic
    /// `{"type":"complete"}` so each frontend exchange independently
    /// triggers its own backoff/reconnect.
    #[tokio::test]
    async fn ws_close_fans_out_complete_to_every_sink() {
        let path = Endpoint::pick(&std::env::temp_dir()).expect("pick");
        let arg = path.as_arg().to_owned();
        spawn_ws_server(arg.clone(), |_text, write| {
            Box::pin(async move {
                // Drop the connection immediately on first inbound (after
                // ack) by sending a close frame.
                let _ = write.send(Message::Close(None)).await;
            })
        })
        .await;

        let reg = SubscriptionClient::new(path);
        let (tx_a, mut rx_a) = mpsc::unbounded_channel();
        let (tx_b, mut rx_b) = mpsc::unbounded_channel();
        let _id_a = reg
            .subscribe("sub a".into(), json!({}), Arc::new(MpscSink(tx_a)))
            .await
            .expect("sub a");
        let _id_b = reg
            .subscribe("sub b".into(), json!({}), Arc::new(MpscSink(tx_b)))
            .await
            .expect("sub b");

        // After server close, both sinks must see a synthetic complete.
        let frame_a = tokio::time::timeout(Duration::from_secs(2), rx_a.recv())
            .await
            .expect("a complete should arrive")
            .unwrap();
        let frame_b = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .expect("b complete should arrive")
            .unwrap();
        let ea: serde_json::Value = serde_json::from_str(&frame_a).unwrap();
        let eb: serde_json::Value = serde_json::from_str(&frame_b).unwrap();
        assert_eq!(ea["type"], "complete");
        assert_eq!(eb["type"], "complete");

        let _ = std::fs::remove_file(arg);
    }
}
