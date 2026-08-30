# src-tauri — Tauri/Rust native host

The desktop app's native host (Tauri 2). It owns windows, the tray and menus, the Go sidecar's lifecycle, and bridges GraphQL from the webview to the sidecar over a Unix socket (named pipe on Windows).

## Distribution & updates

**Not distributed through the Mac App Store** — notarized direct downloads (`.dmg`/`.app`) plus Windows/Linux bundles; no MAS build target. In-app updates use the official `tauri-plugin-updater` (signed bundles + hosted manifest). The app and its bundled sidecar (`externalBin`) are versioned and updated together — a release bump covers either side changing.

## Layout

- `main.rs` — thin; calls `lib::run()`.
- `lib.rs` — **entry point** (`run()`): builds the app, registers plugins + commands, runs `setup` (spawn sidecar, menu/tray, background tasks), owns the shutdown event loop.
- `commands.rs` — `#[tauri::command]` handlers (`graphql_query/subscribe/unsubscribe`, `ready`, `new_window`, `update_host_file`, `quit`). Keep thin — delegate to services. `quit` routes through `AppHandle::exit`.
- `host_file.rs` — **`host.json`**, the persisted-settings source of truth (color-scheme preference today). Versioned JSON, all-`Option` partial patches (`HostFilePatch`) through the one `update_host_file` command; defensive reads, atomic writes (temp + rename). Reaches the webview via the `window.__KSTACK_HOST__` initialization script and the `host-file-updated` broadcast, both fed from one read in `build_window`. Persistence only — how a setting is *used* lives with its consumer. Webview counterpart: `src/lib/host-file.ts`. → [ADR: host.json settings](../docs/adr/2026-08-09-host-json-settings.md).
- `state.rs` — `AppState` (sidecar, window_manager, tray, shutdown) via `app.manage`. `shutdown` is a `CancellationToken` cancelled once on Quit (see Graceful shutdown).
- `app_menu.rs` — **macOS-only** global menu bar; `build_app_menu` is a no-op on Linux/Windows, where the webview's `AppMenu` drives the same actions via `new_window`/`quit`.
- `tray/` — tray icon/menu + the gRPC auth-state watch supervisor. `tray/mod.rs` is Tauri wiring (`spawn_authstate_subscription`, `rebuild_tray_menu` — reads the snapshot in `AppState::tray`, rebuilds on main thread); `tray/account_menu.rs` is the pure, unit-tested account-section logic. Signed out → "Login / Create Account"; signed in → name-titled submenu (Account Settings → `https://app.kstack.sh/account`, Sign out). The tray does not surface kube-context (webview-only GraphQL feature).
- `window_manager.rs`, `dock_menu.rs` (macOS), `error.rs` — see Windows below.
- `os_theme.rs` — `prefers_dark()`, resolving the `system` preference before any window exists (macOS: `AppleInterfaceStyle` via `NSUserDefaults`; Windows: `AppsUseLightTheme` registry). Not compiled on Linux. Every failure path degrades to light.
- `services/sidecar/` — sidecar process + IPC (below).
- `wake/` — fires the sidecar's `Poke` on OS wake-from-sleep and network-return. `wake/core.rs` is the pure, unit-tested heart (`classify` rising-edge + `run_coalescer` trailing 3s debounce; injectable clock/sink); `wake/supervisor.rs` owns the channel and `#[cfg]`-spawns the platform sources (`macos.rs`/`windows.rs`/`linux.rs` — thin FFI glue; the "which integer means online" mappings stay in `core.rs` so they test everywhere). Poke is best-effort — failures logged and swallowed. → [ADR: poke resync fan-out](../docs/adr/2026-08-09-poke-resync-fanout.md).

## Windows

All chrome decisions live in `build_window` (`window_manager.rs`); `tauri.conf.json` declares **no windows** (a unit test pins this). Per-platform: macOS native decorations with Overlay title bar + repositioned traffic lights; Windows frameless-opaque; Linux frameless-transparent (the webview's `WindowFrame` paints border/shadow). → [ADR: per-platform window chrome](../docs/adr/2026-08-09-per-platform-window-chrome.md).

Traps that bite:

- **Building a window is a blocking call — use the blocking pool.** `WebviewWindowBuilder::build()` parks its thread until the webview exists; on the main thread that deadlocks Windows (WebView2's controller is created *by* the main-thread event loop). Every off-main-thread entry point (`commands::new_window`, the single-instance handler, the tray's window items) uses `tauri::async_runtime::spawn_blocking`, never `spawn` (which would park a worker serving `graphql_query`). Such commands take `AppHandle` alone and resolve state inside the closure. The macOS-only `app_menu.rs`/`dock_menu.rs` call straight from the main thread — fine there.
- **Windows are visible from creation, painted with the resolved scheme at t0** (`background_color_for`). There is **no reveal step** — no `visible(false)`, no `show_window`, no page-load handler, no fallback timer. Do not reintroduce one. On macOS this depends on **`tauri/macos-private-api`** (mirrored by `app.macOSPrivateApi` in `tauri.conf.json`); `background_color` must always be passed on opaque platforms, and the document must stay opaque. → [ADR: first-paint theming](../docs/adr/2026-08-09-first-paint-theming.md).
- **New windows cascade** (`cascade_position`): 28px down-right of the anchor (focused window, else last built), restarting from center when the step wouldn't fit the work area. Positioned explicitly on **all three platforms** (AppKit auto-cascades unpositioned windows; Linux/Windows pile up).
- The pure helpers (`traffic_light_position`, `background_color_for`, `cascade_position`) are free functions compiled on every platform so CI covers them. Traffic-light pixel constants stay in sync with `src/components/widgets/app-sidebar.tsx`. `LIGHT_BACKGROUND`/`DARK_BACKGROUND` track `@kubetail/ui`'s `--background` token by eye — the webview paints the real token over them on first frame.

## Sidecar IPC (`services/sidecar/`)

The sidecar is a bundled child process (`service.rs::spawn`) on a per-instance socket picked pre-spawn (`ipc.rs`). Readiness: it prints `READY ` on stdout; `SidecarService::ready()` gates the first call. Shutdown: dropping the child closes stdin → EOF → sidecar drains. Closing the last window does **not** exit the app; only Quit (or a Unix signal, `lib.rs::spawn_signal_handler`) does.

- **Queries/mutations**: one fresh HTTP/1 connection per call (`graphql/query.rs`). No pooling — UDS connect is sub-ms.
- **Subscriptions**: one SSE connection per subscription (`graphql/subscribe.rs`), parsed via `eventsource-stream` into `{type,payload}` channel envelopes. Graceful completion → `{"type":"complete"}` (silent reconnect); abnormal drop → `{"type":"closed"}` (reconnect + report) — the split is read off body framing, since gqlgen's `event: complete` is data-less and discarded by SSE dispatch. On a successful dial the host emits `{"type":"open"}` before the snapshot — **only after** the 200, never on a failed dial. The frontend keys its accumulator resets on it; don't move that signal to the ack. → [ADR: transport-status generation](../docs/adr/2026-08-09-transport-status-generation.md). The `FrameSink` trait is the delivery seam (production: `TauriChannelSink`). **The host tears a webview's subscriptions down itself** (`lib.rs::cancel_webview_subscriptions`, on `PageLoadEvent::Started` and `WindowEvent::Destroyed`): a reload or a window close runs no JS teardown, so every handle is tagged with its webview label — otherwise each orphaned subscription keeps streaming into a callback id the new page never registered ("Couldn't find callback id N", once per frame) and leaks a sidecar connection and a cluster watch.
- **gRPC** (`grpc.rs`): auth (watch/login/logout) + resync poke, over the same socket via h2c — tonic dials the `interprocess` stream with HTTP/2 prior knowledge, one cached `Channel`, re-dialed on drop. Bindings generated by `build.rs` from `proto/auth.proto`/`proto/poke.proto`. Entry points: `SidecarService::{watch_auth_state, start_login, logout, poke}`. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

## Graceful shutdown (`AppState::shutdown`)

Quit cancels the app-wide `CancellationToken` before `SidecarService::graceful_shutdown`. Every app-lifetime background task holds a clone and `tokio::select!`s on `shutdown.cancelled()` in each loop/await (tray supervisor, wake supervisor, signal handler). **New background tasks must follow the same pattern.** Per-subscription teardown is separate — the `oneshot` cancel table in `graphql/subscribe.rs`, keyed by op id and tagged by webview.

## Conventions

- **No `unwrap`/`expect`** — `#![warn(clippy::unwrap_used)]`. Poisoned mutexes: `.lock().unwrap_or_else(|p| p.into_inner())`.
- **Errors** (`error.rs::AppError`): transport failures cross the command boundary as `AppError::Io` so urql sees a `networkError` and retries. **HTTP non-2xx is NOT an error** — return `Ok(GraphqlResponse{status, body})`; don't map 4xx/5xx to `Err`.
- Menu/tray callbacks are sync and can't return errors — log failures; for async work `tauri::async_runtime::spawn`.
- Off-main-thread menu mutation: queue with `app.run_on_main_thread(...)` (muda requires the main thread).

## Tests & checks

- Inline `#[cfg(test)] mod tests` with `#[tokio::test]`; UDS/pipe fixtures via `interprocess`. Factor pure logic into free functions to unit-test it.
- **No magic sleeps** (repo-wide — see the root `CLAUDE.md` for the rule and its two carve-outs). Synchronize on the actual signal (`watch`/`mpsc`, `wait_for`, polling a condition), or run the test on a paused clock — `#[tokio::test(start_paused = true)]` auto-advances virtual time between parked timers, so a `tokio::time::sleep` inside it costs nothing real (`ipc.rs`'s `connect_retries_until_endpoint_appears` is the reference). Never a real `sleep` to let work settle.
- `make test-rust` — **builds the sidecar first** (an integration test spawns the real binary).
- `make lint-rust` = `cargo fmt --check`; `make vet-rust` = `cargo clippy --all-targets -- -D warnings` (enforced in CI).

When you change the host's architecture, conventions, or IPC contract, update this `CLAUDE.md` in the same change.
