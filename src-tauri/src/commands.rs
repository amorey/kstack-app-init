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
use crate::services::auth::Session;
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

#[tauri::command]
pub async fn auth_login(app: AppHandle, state: State<'_, AppState>) -> Result<Session> {
    state.auth.login(&app).await
}

#[tauri::command]
pub async fn auth_logout(app: AppHandle, state: State<'_, AppState>) -> Result<()> {
    state.auth.logout(&app).await
}

#[tauri::command]
pub async fn auth_status(state: State<'_, AppState>) -> Result<Session> {
    Ok(state.auth.current_session())
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

/// Registers a new GraphQL subscription on the host's shared
/// graphql-transport-ws connection. Returns the op id urql will pass to
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
