# src-tauri — Tauri/Rust native host

The desktop app's native host (Tauri 2). It owns windows, the tray and menus, the Go sidecar's lifecycle, and bridges GraphQL from the webview to the sidecar over a Unix socket (named pipe on Windows).

## Distribution & updates

**This app is NOT distributed through the Mac App Store** — we ship notarized direct downloads (`.dmg`/`.app`) on macOS plus Windows/Linux bundles. There is no MAS build target.

Because we control distribution end-to-end, in-app updates use the **official `tauri-plugin-updater`** (signed bundles + a hosted release manifest), not Apple's update mechanism. The MAS self-update prohibition does not apply to us. Note the app and its bundled Go sidecar (`externalBin`) are versioned and updated together — the whole bundle is replaced — so a release bump covers either side changing.

## Layout

- `main.rs` — thin; just calls `lib::run()`.
- `lib.rs` — **entry point** (`run()`): builds the Tauri app, registers plugins + webview commands, runs the `setup` hook (spawn sidecar, build menu/tray, start background tasks), and owns the shutdown event loop.
- `commands.rs` — `#[tauri::command]` handlers the webview invokes (`graphql_query/subscribe/unsubscribe`, `ready`, `new_window`, `quit`). Keep thin — delegate to services. (`new_window`/`quit` back the Linux/Windows `MenuRibbon`; `quit` routes through `AppHandle::exit` for graceful shutdown.)
- `state.rs` — `AppState` (sidecar, window_manager, tray, shutdown), shared via `app.manage` / `app.state::<AppState>()`. `tray: Arc<Mutex<TraySnapshots>>` holds the latest kube-context + auth snapshots so either watch supervisor's rebuild sees both. `shutdown` is a `tokio_util::sync::CancellationToken` cancelled once on Quit; background tasks select on it for clean shutdown — see below.
- `app_menu.rs` — application menu bar construction. **macOS only** — it builds the global menu bar there; on Linux/Windows `build_app_menu` is a no-op (the native menu would render *inside* each window) and the webview's `MenuRibbon` replaces it, driving the same actions via the `new_window` / `quit` commands.
- `tray/` — system tray icon, menu, and the host-internal **gRPC** watch supervisors. `tray/mod.rs` is the Tauri wiring: it builds the tray, handles menu events, runs `spawn_kubeconfig_subscription` (kube-context) and `spawn_authstate_subscription` (auth-state), and `rebuild_tray_menu` (reads both latest snapshots from `AppState::tray`, rebuilds the full menu on main thread). `tray/default_context_picker.rs` is the pure, unit-tested "Default Context" logic; `tray/account_menu.rs` is the pure, unit-tested account-section logic (menu-id constants, `AccountSnapshot`, `AccountMenuDescriptor`, `build_account_descriptor`, `account_snapshot_from`). When signed out the tray shows "Login / Create Account"; when signed in it shows a submenu titled with the user's name containing "Account Settings" (opens `https://app.kstack.sh/account` in the browser) and "Sign out".
- `window_manager.rs`, `dock_menu.rs` (macOS), `error.rs`.
- `services/sidecar/` — sidecar process + IPC. The gRPC client now also exposes `start_login`, `logout`, and `watch_auth_state` (alongside `set_current_context` and `watch_kube_context`).

## Sidecar IPC (`services/sidecar/`)

The sidecar is a **bundled child process** (`service.rs::spawn`) addressed over a per-instance socket picked pre-spawn (`ipc.rs`). Two channels share that socket:

- **Queries/mutations**: one fresh HTTP/1 connection per call (`graphql/query.rs`). No pooling — UDS connect is sub-ms.
- **Subscriptions**: one SSE (`text/event-stream`) HTTP connection per subscription (`graphql/subscribe.rs`). The host POSTs `/graphql` with `Accept: text/event-stream`, parses gqlgen's `event: next` / `event: complete` frames (SSE wire parsing is delegated to the `eventsource-stream` crate, not hand-rolled), and forwards them as the same `{type,payload}` channel envelopes the renderer already consumes. No shared session/handshake; a dropped connection ends one subscription and the frontend reconnects.

The **`FrameSink` trait is the seam** for delivering GraphQL subscription frames: production wraps a Tauri `Channel` (`TauriChannelSink`) for the webview.

- **gRPC** (`services/sidecar/grpc.rs`): the host-internal control channel for kube-context (watch + set) and auth (watch + login + logout). gRPC rides HTTP/2; it shares the socket with GraphQL via **h2c**. tonic dials the same `interprocess` `ipc::Stream` through a custom connector, speaking HTTP/2 with prior knowledge — no TLS. Bindings are generated from `proto/kubecontext.proto`, `proto/auth.proto` and `proto/poke.proto` by `build.rs` (`tonic-prost-build`, vendored `protoc`); they live in the `kubecontext`, `auth` and `poke` proto modules. A cached tonic `Channel` multiplexes all RPCs and re-dials if the connection drops. Reach the RPCs via `SidecarService::{watch_kube_context, set_current_context, watch_auth_state, start_login, logout}`. (The `poke` binding is compiled but not yet driven — wiring a host trigger that calls `PokeService.Poke` on OS resume / network-on is a follow-up; the sidecar broadcasts the resulting `SourceHost` resync to its cluster-sync and settings-sync engines.) Both tray supervisors (`spawn_kubeconfig_subscription` / `spawn_authstate_subscription`) keep the capped-backoff + `AppState::shutdown` biased-select shape.

Readiness: the sidecar prints `READY ` on stdout once bound; `SidecarService::ready()` gates the first call. Shutdown: dropping the child closes its stdin → EOF → sidecar drains. Closing the **last window does not exit** the app; only Quit (or a Unix signal, see `lib.rs::spawn_signal_handler`) does.

## Graceful shutdown (`AppState::shutdown`)

Quit cancels the app-wide `CancellationToken` (`RunEvent::ExitRequested`) *before* `SidecarService::graceful_shutdown`. The three long-lived tasks spawned at setup each hold a clone and `tokio::select!` on `shutdown.cancelled()`: the tray kube-context supervisor (`tray::spawn_kubeconfig_subscription`), the tray auth-state supervisor (`tray::spawn_authstate_subscription`), and the Unix signal handler (`lib::spawn_signal_handler`). **New app-lifetime background tasks should follow the same pattern** — clone the token from `app.state::<AppState>().shutdown` and select on it in every loop/await so Quit tears them down cleanly instead of leaving them churning against a sidecar that's going away. (Per-subscription teardown is separate — that's the `oneshot` cancel table in `graphql/subscribe.rs`.)

## Conventions

- **No `unwrap`/`expect`** — `#![warn(clippy::unwrap_used)]`. Use `?` / `match` / `if let`. Poisoned mutexes: `.lock().unwrap_or_else(|p| p.into_inner())`.
- **Errors** (`error.rs::AppError`): transport failures cross the command boundary as `AppError::Io` so urql sees a `networkError` and retries. **HTTP non-2xx is NOT an error** — it travels back as `Ok(GraphqlResponse{status, body})` so the frontend treats it as a (non-retryable) server error. Don't map 4xx/5xx to `Err`.
- **Menu/tray callbacks are sync and can't return errors** — log failures; for async work (e.g. firing a mutation on a tray click) `tauri::async_runtime::spawn`.
- Off-main-thread menu mutation: queue with `app.run_on_main_thread(...)` (muda requires the main thread).

## Tests & checks

- Tests are inline `#[cfg(test)] mod tests` with `#[tokio::test]`; UDS/pipe fixtures via `interprocess`. Factor pure logic into free functions to unit-test it (see `tray/default_context_picker.rs` helpers, `service.rs::cmd_args`).
- **Avoid magic sleeps.** Don't `tokio::time::sleep` to wait for async work to settle — synchronize on the actual signal (`watch`/`mpsc` channels, `wait_for`, polling a condition) or drive time with `tokio::time::pause()` + `advance`. Fixed delays are flaky on slow CI and slow the suite.
- `make test-rust` — **builds the sidecar first** (an integration test spawns the real binary).
- `make lint-rust` = `cargo fmt --check` (run `cargo fmt` before committing).
- `make vet-rust` = **`cargo clippy --all-targets -- -D warnings`** — clippy is enforced (warnings fail CI via `make vet`). Keep new code clippy-clean.

When you change the host's architecture, conventions, or IPC contract, update this `CLAUDE.md` in the same change.
