//! Tauri commands bridging the webview to the sidecar over UDS.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tauri::ipc::Channel;
use tauri::State;
use tokio::task::AbortHandle;

use super::lifecycle::{wait_for_socket, SidecarState};
use super::subscribe::{run_subscription, MsgSink};
use super::transport;

#[tauri::command]
pub(crate) async fn graphql_query(
    state: State<'_, SidecarState>,
    body: String,
) -> Result<String, String> {
    let socket = wait_for_socket(state.socket_rx()).await?;
    let bytes = transport::query_uds(&socket, body.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

/// Live subscription registry. Installed via `app.manage(...)` in `lib.rs`.
/// Wrapped in an `Arc` internally so the spawned task can scrub its own
/// entry when the subscription terminates naturally.
#[derive(Default)]
pub struct Operations {
    next_id: AtomicU32,
    inner: Arc<Mutex<HashMap<u32, AbortHandle>>>,
}

struct ChannelSink(Channel<String>);
impl MsgSink for ChannelSink {
    fn send(&self, msg: String) {
        // Channel send only fails after the webview has dropped the receiver,
        // at which point the spawned task is about to be aborted anyway.
        let _ = self.0.send(msg);
    }
}

#[tauri::command]
pub(crate) async fn graphql_subscribe(
    state: State<'_, SidecarState>,
    ops: State<'_, Operations>,
    query: String,
    variables: Value,
    channel: Channel<String>,
) -> Result<u32, String> {
    let socket = wait_for_socket(state.socket_rx()).await?;
    let sink: Arc<dyn MsgSink> = Arc::new(ChannelSink(channel));

    let id = ops.next_id.fetch_add(1, Ordering::Relaxed);
    let registry = ops.inner.clone();
    let handle = tokio::spawn(async move {
        let _ = run_subscription(socket, query, variables, sink).await;
        // Self-scrub on natural completion so the registry doesn't grow
        // unbounded across the app's lifetime.
        if let Ok(mut g) = registry.lock() {
            g.remove(&id);
        }
    });

    ops.inner
        .lock()
        .map_err(|e| e.to_string())?
        .insert(id, handle.abort_handle());
    Ok(id)
}

#[tauri::command]
pub(crate) async fn graphql_unsubscribe(ops: State<'_, Operations>, id: u32) -> Result<(), String> {
    if let Some(handle) = ops.inner.lock().map_err(|e| e.to_string())?.remove(&id) {
        handle.abort();
    }
    Ok(())
}
