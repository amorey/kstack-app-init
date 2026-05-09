//! Tauri command bridging the webview to the sidecar over UDS.

use tauri::State;

use super::lifecycle::{wait_for_socket, SidecarState};
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
