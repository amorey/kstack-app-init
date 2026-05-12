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
