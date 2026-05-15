//! GraphQL subscription bridge: speaks graphql-transport-ws to the sidecar
//! over UDS and forwards each frame to a `MsgSink`.
//!
//! The graphql-transport-ws protocol itself (connection_init/ack, subscribe,
//! next/error/complete) is handled by `graphql-ws-client`. This module owns
//! the UDS connect + WS HTTP-Upgrade handshake and the translation from the
//! crate's typed stream into the JSON envelope our webview consumes.
//!
//! One WebSocket per subscription. Multiplexing exists to dodge browser
//! per-origin connection caps; the Rust host has no such cap and UDS
//! connections are essentially free.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::StreamExt;
use graphql_ws_client::{graphql::GraphqlOperation, Client};
use http::Request;
use serde::Serialize;
use serde_json::value::RawValue;
use thiserror::Error;
use tokio_tungstenite::client_async;

use super::transport::{connect_uds, HOST};

pub const SUBPROTOCOL: &str = "graphql-transport-ws";

const FRAME_COMPLETE_OUT: &str = r#"{"type":"complete"}"#;

/// Sink for protocol frames delivered to the consumer (webview) in a stable
/// JSON shape: `{"type":"next","payload":{...}}` / `{"type":"error",...}` /
/// `{"type":"complete"}`. Trait-bounded so tests don't depend on Tauri.
pub trait MsgSink: Send + Sync + 'static {
    fn send(&self, msg: String);
}

#[derive(Debug, Error)]
pub enum SubError {
    #[error("connect uds: {0}")]
    Connect(std::io::Error),
    #[error("ws handshake: {0}")]
    Handshake(String),
    #[error("ws transport: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("graphql-ws: {0}")]
    Client(#[from] graphql_ws_client::Error),
}

/// Run one subscription end-to-end. The sink always observes a terminating
/// `complete` or `error` frame before this function returns. `bearer`, when
/// `Some`, is shipped in the `connection_init` payload so the sidecar's
/// graphql-transport-ws InitFunc can pull it into the resolver context —
/// the upgrade headers can't carry it reliably across all platforms.
pub async fn run_subscription(
    socket: PathBuf,
    query: String,
    variables: serde_json::Value,
    bearer: Option<String>,
    sink: Arc<dyn MsgSink>,
) -> Result<(), SubError> {
    let result = run_inner(&socket, query, variables, bearer, sink.clone()).await;
    if let Err(ref e) = result {
        sink.send(format!(
            r#"{{"type":"error","payload":{}}}"#,
            json_string(&e.to_string())
        ));
    }
    result
}

/// Operation type handed to graphql-ws-client. Serializes as the `payload`
/// of the `subscribe` message (`{query, variables}`) and forwards the
/// response payload through as raw JSON bytes (no re-parse).
#[derive(Serialize)]
struct RawOp {
    query: String,
    variables: serde_json::Value,
}

impl GraphqlOperation for RawOp {
    type Response = Box<RawValue>;
    type Error = serde_json::Error;
    fn decode(&self, data: serde_json::Value) -> Result<Self::Response, Self::Error> {
        serde_json::value::to_raw_value(&data)
    }
}

async fn run_inner(
    socket: &Path,
    query: String,
    variables: serde_json::Value,
    bearer: Option<String>,
    sink: Arc<dyn MsgSink>,
) -> Result<(), SubError> {
    let stream = connect_uds(socket).await.map_err(SubError::Connect)?;

    // graphql-ws-client speaks WebSocket frames, not HTTP — we still own
    // the upgrade handshake (sets the subprotocol header).
    let request = Request::builder()
        .uri(format!("ws://{HOST}/graphql"))
        .header("Host", HOST)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Protocol", SUBPROTOCOL)
        .body(())
        .map_err(|e| SubError::Handshake(e.to_string()))?;

    let (ws, _resp) = client_async(request, stream).await?;

    let mut builder = Client::build(ws);
    if let Some(token) = bearer {
        // graphql-transport-ws ships an opaque `payload` map with
        // `connection_init`; the sidecar's InitFunc pulls `Authorization`
        // out of it. Keys are case-sensitive on the wire.
        let payload = serde_json::json!({ "Authorization": format!("Bearer {token}") });
        builder = builder.payload(payload)?;
    }
    let mut sub = builder.subscribe(RawOp { query, variables }).await?;

    while let Some(item) = sub.next().await {
        match item {
            Ok(payload) => {
                sink.send(format!(r#"{{"type":"next","payload":{}}}"#, payload.get()));
            }
            Err(e) => {
                sink.send(format!(
                    r#"{{"type":"error","payload":{}}}"#,
                    json_string(&e.to_string())
                ));
                return Ok(());
            }
        }
    }

    sink.send(FRAME_COMPLETE_OUT.to_string());
    Ok(())
}

/// JSON-encode a string as a quoted scalar.
fn json_string(s: &str) -> String {
    serde_json::Value::String(s.to_owned()).to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use futures_util::SinkExt as _;
    use interprocess::local_socket::{
        tokio::{prelude::*, Listener},
        GenericFilePath, ListenerOptions, ToFsName,
    };
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    fn temp_socket() -> PathBuf {
        // Counter + pid keeps names unique across concurrent test binaries.
        // macOS limits UDS paths to ~104 bytes — keep the prefix short.
        // Windows requires a named-pipe path (`\\.\pipe\...`); a temp-dir
        // file path is rejected by `to_fs_name::<GenericFilePath>`.
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        #[cfg(windows)]
        {
            PathBuf::from(format!(r"\\.\pipe\ks-{}-{n}", std::process::id()))
        }
        #[cfg(not(windows))]
        {
            let path = std::env::temp_dir().join(format!("ks-{}-{n}.sock", std::process::id()));
            let _ = std::fs::remove_file(&path);
            path
        }
    }

    fn bind(socket: &Path) -> Listener {
        let name = socket.to_fs_name::<GenericFilePath>().unwrap();
        ListenerOptions::new().name(name).create_tokio().unwrap()
    }

    struct TestSink(mpsc::UnboundedSender<String>);
    impl MsgSink for TestSink {
        fn send(&self, msg: String) {
            let _ = self.0.send(msg);
        }
    }
    fn test_sink() -> (Arc<dyn MsgSink>, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Arc::new(TestSink(tx)), rx)
    }

    async fn drain(rx: &mut mpsc::UnboundedReceiver<String>) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(s)) => out.push(s),
                _ => return out,
            }
        }
    }

    /// Server-side handshake that echoes the subprotocol back, mirroring
    /// gqlgen. Without it tungstenite's client errors with `NoSubProtocol`.
    async fn accept_ws(
        conn: interprocess::local_socket::tokio::Stream,
    ) -> tokio_tungstenite::WebSocketStream<interprocess::local_socket::tokio::Stream> {
        use tokio_tungstenite::tungstenite::handshake::server::{
            Request as SReq, Response as SResp,
        };
        tokio_tungstenite::accept_hdr_async(conn, |req: &SReq, mut resp: SResp| {
            if let Some(p) = req.headers().get("Sec-WebSocket-Protocol") {
                resp.headers_mut()
                    .insert("Sec-WebSocket-Protocol", p.clone());
            }
            Ok(resp)
        })
        .await
        .unwrap()
    }

    /// Accept one WS, ack, then emit `count` `next` frames + complete.
    async fn fake_server_emits_ticks(listener: Listener, count: u32) {
        let conn = listener.accept().await.unwrap();
        let mut ws = accept_ws(conn).await;
        let init = ws.next().await.unwrap().unwrap();
        assert!(init.into_text().unwrap().contains("connection_init"));
        ws.send(Message::Text(r#"{"type":"connection_ack"}"#.into()))
            .await
            .unwrap();
        let sub = ws.next().await.unwrap().unwrap();
        assert!(sub.into_text().unwrap().contains("subscribe"));
        for n in 1..=count {
            let frame =
                format!(r#"{{"id":"1","type":"next","payload":{{"data":{{"tick":{n}}}}}}}"#);
            ws.send(Message::Text(frame)).await.unwrap();
        }
        ws.send(Message::Text(r#"{"id":"1","type":"complete"}"#.into()))
            .await
            .unwrap();
        let _ = ws.close(None).await;
    }

    #[tokio::test]
    async fn forwards_two_next_then_complete() {
        let socket = temp_socket();
        let listener = bind(&socket);
        let server = tokio::spawn(fake_server_emits_ticks(listener, 2));

        let (sink, mut rx) = test_sink();
        run_subscription(
            socket,
            "subscription { tick }".into(),
            serde_json::json!({}),
            None,
            sink,
        )
        .await
        .unwrap();
        server.await.unwrap();

        let got = drain(&mut rx).await;
        assert_eq!(got.len(), 3, "got: {got:?}");
        assert!(got[0].contains(r#""tick":1"#), "{}", got[0]);
        assert!(got[1].contains(r#""tick":2"#), "{}", got[1]);
        assert!(got[2].contains("complete"), "{}", got[2]);
    }

    #[tokio::test]
    async fn server_drop_after_ack_completes_the_subscription() {
        // graphql-ws-client treats any end-of-stream (clean `complete`,
        // unilateral close, or transport drop) the same way: the
        // `Subscription` stream returns `None`. We can't distinguish abrupt
        // drop from natural end at this layer, so both surface as
        // `{"type":"complete"}` to the webview.
        let socket = temp_socket();
        let listener = bind(&socket);
        tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            let mut ws = accept_ws(conn).await;
            let _init = ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(r#"{"type":"connection_ack"}"#.into()))
                .await
                .unwrap();
            let _sub = ws.next().await.unwrap().unwrap();
            // drop without complete
        });

        let (sink, mut rx) = test_sink();
        run_subscription(
            socket,
            "subscription { tick }".into(),
            serde_json::json!({}),
            None,
            sink,
        )
        .await
        .unwrap();

        let got = drain(&mut rx).await;
        assert!(
            got.iter().any(|m| m.contains("complete")),
            "expected complete frame, got {got:?}",
        );
    }

    #[tokio::test]
    async fn refused_handshake_before_ack_returns_error() {
        // Pre-ack failures still surface as errors (graphql-ws-client's
        // build() returns Err if connection_ack never arrives).
        let socket = temp_socket();
        let listener = bind(&socket);
        tokio::spawn(async move {
            let conn = listener.accept().await.unwrap();
            let mut ws = accept_ws(conn).await;
            let _init = ws.next().await.unwrap().unwrap();
            // drop before ack
            let _ = ws.close(None).await;
        });

        let (sink, mut rx) = test_sink();
        let res = run_subscription(
            socket,
            "subscription { tick }".into(),
            serde_json::json!({}),
            None,
            sink,
        )
        .await;
        assert!(res.is_err(), "expected error, got {res:?}");

        let got = drain(&mut rx).await;
        assert!(
            got.iter().any(|m| m.contains(r#""type":"error""#)),
            "expected error frame, got {got:?}",
        );
    }

    #[tokio::test]
    async fn two_subscriptions_open_two_independent_connections() {
        let socket = temp_socket();
        let listener = bind(&socket);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let conn = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut ws = accept_ws(conn).await;
                    let _ = ws.next().await;
                    ws.send(Message::Text(r#"{"type":"connection_ack"}"#.into()))
                        .await
                        .unwrap();
                    let _ = ws.next().await;
                    ws.send(Message::Text(
                        r#"{"id":"1","type":"next","payload":{"data":{"tick":7}}}"#.into(),
                    ))
                    .await
                    .unwrap();
                    ws.send(Message::Text(r#"{"id":"1","type":"complete"}"#.into()))
                        .await
                        .unwrap();
                    let _ = ws.close(None).await;
                });
            }
        });

        let (sink_a, mut rx_a) = test_sink();
        let (sink_b, mut rx_b) = test_sink();
        let socket2 = socket.clone();
        let (a, b) = tokio::join!(
            run_subscription(
                socket,
                "subscription { tick }".into(),
                serde_json::json!({}),
                None,
                sink_a
            ),
            run_subscription(
                socket2,
                "subscription { tick }".into(),
                serde_json::json!({}),
                None,
                sink_b
            ),
        );
        a.unwrap();
        b.unwrap();
        server.await.unwrap();

        for rx in [&mut rx_a, &mut rx_b] {
            let got = drain(rx).await;
            assert!(got.iter().any(|m| m.contains(r#""tick":7"#)), "{got:?}");
            assert!(got.iter().any(|m| m.contains("complete")), "{got:?}");
        }
    }
}
