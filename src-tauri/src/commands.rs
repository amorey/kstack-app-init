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
use tauri::{AppHandle, Emitter, Manager, State};

use crate::error::Result;
use crate::host_file::{self, HostFile, HostFilePatch};
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

/// Opens a new application window. Backs the webview `AppMenu`'s
/// "New Window" item and `Ctrl+N` shortcut on Linux/Windows; macOS drives the
/// same action through its native menu (`app_menu.rs`). Delegates to the
/// shared `WindowManager` so labeling/focus stay consistent.
///
/// **Deliberately `async`** so Tauri runs it on the async runtime rather than
/// the main thread. Building a `WebviewWindow` blocks the calling thread until
/// the webview is created, and on Windows the WebView2 controller is created
/// asynchronously by the main-thread event loop — so building on the main
/// thread deadlocks (the loop can't pump while it's blocked in `build()`),
/// leaving a blank window and freezing the whole app. Off the main thread the
/// loop stays free to pump and the window materializes. (macOS/WKWebView needs
/// no such pump, but async is harmless there.)
///
/// **And deliberately on the blocking pool**, for the same reason stated the
/// other way round: because `build()` parks its thread until the window exists,
/// running it on an async worker would hold one of the threads that serve
/// [`graphql_query`] for the whole build. `spawn_blocking` is the pool meant to
/// absorb that. The `AppState` is resolved inside the closure rather than taken
/// as a `State<'_, _>` parameter, since a borrow of `app` cannot cross into a
/// `'static` task.
#[tauri::command]
pub async fn new_window(app: AppHandle) -> Result<()> {
    tauri::async_runtime::spawn_blocking(move || {
        app.state::<AppState>().window_manager.new_window(&app)?;
        Ok(())
    })
    .await
    .map_err(|err| std::io::Error::other(format!("window build task failed: {err}")))?
}

/// Applies a partial update to `host.json`, the host's persisted settings
/// file and the source of truth for what it holds (see the `host_file`
/// module). Deliberately general — the webview sends just the fields it wants
/// to change (e.g. `{ colorSchemePreference: "dark" }`); adding a
/// host-persisted setting extends `HostFilePatch` rather than adding a
/// command. On success the merged file is broadcast to every window as a
/// `host_file::UPDATED_EVENT` — that push is how all open windows (including
/// the caller's) track the file live — and returned to the caller directly.
#[tauri::command]
pub fn update_host_file(app: AppHandle, patch: HostFilePatch) -> Result<HostFile> {
    let file = host_file::update(&host_file::path(&app)?, patch)?;
    app.emit(host_file::UPDATED_EVENT, &file)?;
    Ok(file)
}

/// Quits the app. Routes through [`AppHandle::exit`] — the same
/// graceful-shutdown path as the tray and macOS-menu "Quit" — so the
/// `RunEvent::ExitRequested` teardown runs. Backs the `AppMenu` "Quit" item
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
