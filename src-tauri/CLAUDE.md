# src-tauri — Tauri/Rust native host

The desktop app's native host (Tauri 2). It owns windows, the tray and menus, the Go sidecar's lifecycle, and bridges GraphQL from the webview to the sidecar over a Unix socket (named pipe on Windows).

## Distribution & updates

**Not distributed through the Mac App Store** — direct downloads, no MAS build target. **There are no in-app updates** — nothing checks for a newer build, and updating means downloading one by hand. `release.yml` signs what it ships: a notarized `.dmg` with the sidecar signed inside it, an `.msi` signed through SignPath, unsigned `.deb`/`.rpm`/`.AppImage`, and `SHA256SUMS` with a detached GPG signature beside all of them. The app and its bundled sidecar (`externalBin`) are versioned and released together — a release bump covers either side changing.

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

The sidecar is a bundled child process (`service.rs::spawn`) on a per-instance socket picked pre-spawn (`ipc.rs`), inside an owner-only runtime directory (`runtime_dir`: `$XDG_RUNTIME_DIR/kstack` on Linux, a `0700` per-uid temp subdirectory otherwise, reused only if this user owns it; Windows pipes have no directory). Readiness: it prints `READY ` on stdout; `SidecarService::ready()` gates the first call. Shutdown: dropping the child closes stdin → EOF → sidecar drains. Closing the last window does **not** exit the app; only Quit (or a Unix signal, `lib.rs::spawn_signal_handler`) does. Its stdout/stderr are re-emitted as host `tracing` events (`logs.rs`) at the line's own severity; the sidecar's structured fields ride as one JSON value under `sidecar.fields`, never as top-level host fields.

- **Queries/mutations**: one fresh HTTP/1 connection per call (`graphql/query.rs`). No pooling — UDS connect is sub-ms.
- **Subscriptions**: one SSE connection per subscription (`graphql/subscribe.rs`), parsed via `eventsource-stream` into `{type,payload}` channel envelopes. Graceful completion → `{"type":"complete"}` (silent reconnect); abnormal drop → `{"type":"closed"}` (reconnect + report) — the split is read off body framing, since gqlgen's `event: complete` is data-less and discarded by SSE dispatch. On a successful dial the host emits `{"type":"open"}` before the snapshot — **only after** the 200, never on a failed dial. The frontend keys its accumulator resets on it; don't move that signal to the ack. → [ADR: transport-status generation](../docs/adr/2026-08-09-transport-status-generation.md). The `FrameSink` trait is the delivery seam (production: `TauriChannelSink`). **The host tears a webview's subscriptions down itself** (`lib.rs::cancel_webview_subscriptions`, on `PageLoadEvent::Started` and `WindowEvent::Destroyed`): a reload or a window close runs no JS teardown, so every handle is tagged with its webview label — otherwise each orphaned subscription keeps streaming into a callback id the new page never registered ("Couldn't find callback id N", once per frame) and leaks a sidecar connection and a cluster watch.
- **gRPC** (`grpc.rs`): auth (watch/login/logout) + resync poke, over the same socket via h2c — tonic dials the `interprocess` stream with HTTP/2 prior knowledge, one cached `Channel`, re-dialed on drop. Bindings generated by `build.rs` from `proto/auth.proto`/`proto/poke.proto`. Entry points: `SidecarService::{watch_auth_state, start_login, logout, poke}`. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

## Graceful shutdown (`AppState::shutdown`)

Quit cancels the app-wide `CancellationToken` before `SidecarService::graceful_shutdown`. Every app-lifetime background task holds a clone and `tokio::select!`s on `shutdown.cancelled()` in each loop/await (tray supervisor, wake supervisor, signal handler). **New background tasks must follow the same pattern.** Per-subscription teardown is separate — the `oneshot` cancel table in `graphql/subscribe.rs`, keyed by op id and tagged by webview.

## Security invariants

Full picture: [`docs/security-model.md`](../docs/security-model.md). What the host owns:

- **The webview never navigates off the app origin.** `build_window` installs two default-deny
  callbacks: `on_navigation(is_app_origin)` — an allowlist ending in `false`, admitting the bundle's
  own origin, `about:blank`, and (behind `cfg(debug_assertions)`) the dev server — and
  `on_new_window`, which always answers `Deny`. Client-side routing is `pushState` and never reaches
  either. A feature that seems to need an external URL needs a host command instead: the opener
  plugin, as the tray's account item does. Which engine fires which callback when is verified by
  hand, not by a test.
- **The webview is trusted by custom host commands.** `capabilities/default.json` grants core defaults and window controls, without shell or opener grants; custom commands also expose GraphQL, host preferences and app lifecycle, pinned by `default_capability_grants_only_window_chrome`; the opener and shell plugins are registered for Rust-side use and grant the page nothing. Add a permission only when a call site needs it, scoped as narrowly as the plugin allows — a grant with no consumer is standing authority for injected script.
- **Every dial checks who answered.** An address and the pid that must be serving it travel together as one `ipc::Target`, so there is no way to dial without checking — which matters because gRPC re-dials on loss and every subscription reconnects. The pid is read from the kernel (`peer.rs`), published on a `watch` channel by `spawn`, and cleared when the child terminates; `None` refuses, since a pid the OS has reassigned is exactly what must not pass. An unreadable peer is refused too. macOS reads `LOCAL_PEERPID` itself — its `xucred` carries no pid, so `interprocess` reports `None` there. This stops another process of the same user impersonating the sidecar; it is no defence against code already inside the host process. Pinned by `connect_refuses_a_peer_that_is_not_the_expected_process` and `query_refuses_a_server_that_is_not_the_sidecar`.
- **The sidecar inherits this process's environment, so nothing it needs may come from there.** Every endpoint — cloud URL, OAuth issuer, client id, keychain service — is passed by `cmd_args` from constants in `service.rs`, never read from our own environment (that would move the redirection risk, not close it). New configuration goes the same way. The sidecar honours `KSTACK_*` overrides only when built with `-tags debug` (`make sidecar-dev`), which is a standalone dev seam, not a dev-run one.
- **A webview's subscriptions are torn down by the host** on `PageLoadEvent::Started` and `WindowEvent::Destroyed` — a reload runs no JS teardown, and an orphaned subscription keeps a sidecar connection and a cluster watch alive. Pinned by `cancel_webview_drops_only_that_webviews_subscriptions`.
- **The macOS entitlements are exceptions, not defaults.** `disable-library-validation` permits loading libraries without Apple's or the app's Team ID signature; it does not govern spawning a separate sidecar. Its necessity, and that of `allow-unsigned-executable-memory`, remain unverified pending signed macOS testing (H-3). Don't add an entitlement without saying in `entitlements.plist` what needs it.

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
- **The compiler is pinned** in `rust-toolchain.toml` (channel + `rustfmt`/`clippy`), so clippy's verdict is the same locally and in CI. Upgrading is a deliberate one-line PR; CI reads the channel out of that file, so nothing else needs bumping with it. rustup resolves it by walking up from the working directory — a `rustup`/`cargo` call from the repo root needs `working-directory: ./src-tauri` (as `release.yml`'s `rustup target add` steps do) or it silently uses rustup's default toolchain.

When you change the host's architecture, conventions, or IPC contract, update this `CLAUDE.md` in the same change.
