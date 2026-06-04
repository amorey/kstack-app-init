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

use tauri::ipc::Channel;
use tauri::{AppHandle, State};

use crate::error::Result;
use crate::services::sidecar::GraphqlResponse;
use crate::state::AppState;

/// Probes the sidecar's IPC listener. Returns once the listener accepts a
/// connection, or `Err` (as `networkError` on the frontend) once the
/// connect budget elapses. Frontend uses it as a startup gate so the
/// first real GraphQL call doesn't absorb the bind-wait latency silently.
#[tauri::command]
pub async fn ready(state: State<'_, AppState>) -> Result<()> {
    state.sidecar.ready().await
}

/// Opens a new application window. Backs the webview `MenuRibbon`'s
/// "New Window" item and `Ctrl+N` shortcut on Linux/Windows; macOS drives the
/// same action through its native menu (`app_menu.rs`). Delegates to the
/// shared `WindowManager` so labeling/focus stay consistent.
#[tauri::command]
pub fn new_window(app: AppHandle, state: State<'_, AppState>) -> Result<()> {
    state.window_manager.new_window(&app)?;
    Ok(())
}

/// Quits the app. Routes through [`AppHandle::exit`] — the same
/// graceful-shutdown path as the tray and macOS-menu "Quit" — so the
/// `RunEvent::ExitRequested` teardown runs. Backs the `MenuRibbon` "Quit" item
/// and `Ctrl+Q` on Linux/Windows.
#[tauri::command]
pub fn quit(app: AppHandle) {
    app.exit(0);
}

/// Forwards a GraphQL query/mutation body to the sidecar over its UDS HTTP
/// endpoint. Returns the raw response body alongside the HTTP status so the
/// frontend can construct a `Response` with the real status — the only
/// case that surfaces as `Err` (and hence as `networkError`/retry on the
/// frontend) is a transport failure. The body argument shape matches
/// `src/lib/graphql/invoke-fetch.ts`.
#[tauri::command]
pub async fn graphql_query(state: State<'_, AppState>, body: String) -> Result<GraphqlResponse> {
    state.sidecar.query(body).await
}

/// Registers a new GraphQL subscription, which the host streams over its own
/// SSE connection to the sidecar. Returns the op id urql will pass to
/// [`graphql_unsubscribe`] on teardown. Argument shape matches
/// `src/lib/graphql/subscribe-exchange.ts`.
#[tauri::command]
pub async fn graphql_subscribe(
    state: State<'_, AppState>,
    query: String,
    variables: serde_json::Value,
    channel: Channel<String>,
) -> Result<u64> {
    state.sidecar.subscribe(query, variables, channel).await
}

/// Cancels a previously registered subscription. Tolerant of unknown ids:
/// the frontend can race teardown with the subscribe resolve
/// (subscribe-exchange.ts:129) and pass an id the registry never minted.
#[tauri::command]
pub async fn graphql_unsubscribe(state: State<'_, AppState>, id: u64) -> Result<()> {
    state.sidecar.unsubscribe(id).await;
    Ok(())
}
