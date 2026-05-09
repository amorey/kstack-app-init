// Surface accidental `unwrap()` in runtime code as a lint warning. Tests opt
// out individually via `#[allow(clippy::unwrap_used)]` on the `mod tests`.
#![warn(clippy::unwrap_used)]

use tauri::{Manager, RunEvent};
use tauri_plugin_log::{Target, TargetKind};

pub mod sidecar;

/// Single env var controls verbosity for both the Rust host and the Go
/// sidecar (mirrored in `sidecar/internal/logging.ParseLevel`).
pub(crate) const KSTACK_LOG_ENV: &str = "KSTACK_LOG";

fn host_log_level() -> log::LevelFilter {
    match std::env::var(KSTACK_LOG_ENV)
        .unwrap_or_default()
        .to_lowercase()
        .as_str()
    {
        "debug" => log::LevelFilter::Debug,
        "warn" | "warning" => log::LevelFilter::Warn,
        "error" => log::LevelFilter::Error,
        _ => log::LevelFilter::Info,
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(
            tauri_plugin_log::Builder::new()
                .targets([
                    Target::new(TargetKind::LogDir {
                        file_name: Some("kstack".into()),
                    }),
                    Target::new(TargetKind::Stdout),
                ])
                .level(host_log_level())
                .build(),
        )
        .invoke_handler(tauri::generate_handler![
            sidecar::command::graphql_query,
            sidecar::command::graphql_subscribe,
            sidecar::command::graphql_unsubscribe,
        ])
        .setup(|app| {
            app.manage(sidecar::command::Operations::default());
            sidecar::spawn(app)?;
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            if let RunEvent::Exit = event {
                sidecar::shutdown(app);
            }
        });
}
