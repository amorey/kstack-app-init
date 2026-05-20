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

//! Tauri commands exposed to the renderer.

use tauri::{AppHandle, Runtime};
use tauri_plugin_opener::OpenerExt;

use super::{flow::Status, AUTH};

/// Always uses the system browser — embedded webviews break OAuth's
/// trust model (credential phishing, no shared IdP session).
fn open_browser<R: Runtime>(app: &AppHandle<R>, url: &str) -> Result<(), String> {
    app.opener()
        .open_url(url, None::<&str>)
        .map_err(|e| format!("open_url: {e}"))
}

#[tauri::command]
pub async fn auth_login<R: Runtime>(app: AppHandle<R>) -> Result<Status, String> {
    AUTH.login(|url| open_browser(&app, url)).await
}

#[tauri::command]
pub async fn auth_logout() -> Result<(), String> {
    AUTH.logout().await
}

#[tauri::command]
pub async fn auth_status() -> Status {
    AUTH.status().await
}

#[tauri::command]
pub async fn auth_access_token() -> Result<String, String> {
    AUTH.access_token().await
}
