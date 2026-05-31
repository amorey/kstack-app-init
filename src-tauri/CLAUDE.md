# src-tauri — Tauri/Rust native host

The desktop app's native host (Tauri 2). It owns windows, the tray and menus, the Go sidecar's lifecycle, and bridges GraphQL from the webview to the sidecar over a Unix socket (named pipe on Windows).

## Layout

- `main.rs` — thin; just calls `lib::run()`.
- `lib.rs` — **entry point** (`run()`): builds the Tauri app, registers plugins + webview commands, runs the `setup` hook (spawn sidecar, build menu/tray, start background tasks), and owns the shutdown event loop.
- `commands.rs` — `#[tauri::command]` handlers the webview invokes (`graphql_query/subscribe/unsubscribe`, `auth_*`, `ready`). Keep thin — delegate to services.
- `state.rs` — `AppState` (sidecar, window_manager, auth, shutdown), shared via `app.manage` / `app.state::<AppState>()`. `shutdown` is a `tokio_util::sync::CancellationToken` cancelled once on Quit (`lib.rs` `RunEvent::ExitRequested`, before the sidecar teardown); the app-lifetime background tasks select on it for clean shutdown — see below.
- `app_menu.rs` — application menu bar construction.
- `tray/` — system tray icon, menu, and the host-internal **gRPC** kube-context watch that keeps the live context list current. `tray/mod.rs` is the Tauri wiring (and the watch supervisor); `tray/default_context_picker.rs` is the pure, unit-tested "Default Context" submenu logic (menu-id formatting, descriptor building).
- `window_manager.rs`, `dock_menu.rs` (macOS), `error.rs`.
- `services/sidecar/` — sidecar process + IPC. `services/auth/` — OAuth.

## Sidecar IPC (`services/sidecar/`)

The sidecar is a **bundled child process** (`service.rs::spawn`) addressed over a per-instance socket picked pre-spawn (`ipc.rs`). Two channels share that socket:

- **Queries/mutations**: one fresh HTTP/1 connection per call (`graphql/query.rs`). No pooling — UDS connect is sub-ms.
- **Subscriptions**: one SSE (`text/event-stream`) HTTP connection per subscription (`graphql/subscribe.rs`). The host POSTs `/graphql` with `Accept: text/event-stream`, parses gqlgen's `event: next` / `event: complete` frames (SSE wire parsing is delegated to the `eventsource-stream` crate, not hand-rolled), and forwards them as the same `{type,payload}` channel envelopes the renderer already consumes. No shared session/handshake; a dropped connection ends one subscription and the frontend reconnects.

The **`FrameSink` trait is the seam** for delivering GraphQL subscription frames: production wraps a Tauri `Channel` (`TauriChannelSink`) for the webview.

- **gRPC** (`services/sidecar/grpc.rs`): the host-internal control channel (today: the tray's kube-context watch + set). gRPC rides HTTP/2; it shares the socket with GraphQL via **h2c** (the sidecar routes by content-type). tonic dials the same `interprocess` `ipc::Stream` (not TCP) through a custom `tower::service_fn` connector, speaking HTTP/2 with prior knowledge — no TLS. The bindings are generated from the repo-root `proto/` by `build.rs` (`tonic-prost-build`, using a **vendored `protoc`** so `cargo build` stays hermetic — no system protoc, no CI setup step); reach the RPCs via `SidecarService::watch_kube_context` / `set_current_context`. A cached tonic `Channel` multiplexes RPCs and re-dials if the connection drops. The tray's watch supervisor (`tray::spawn_tray_subscription`) consumes the typed `KubeContextState` stream directly (no `FrameSink`/JSON round-trip) and keeps the capped-backoff + `AppState::shutdown` select shape.

Readiness: the sidecar prints `READY ` on stdout once bound; `SidecarService::ready()` gates the first call. Shutdown: dropping the child closes its stdin → EOF → sidecar drains. Closing the **last window does not exit** the app; only Quit (or a Unix signal, see `lib.rs::spawn_signal_handler`) does.

## Graceful shutdown (`AppState::shutdown`)

Quit cancels the app-wide `CancellationToken` (`RunEvent::ExitRequested`) *before* `SidecarService::graceful_shutdown`. The two long-lived tasks spawned at setup each hold a clone and `tokio::select!` on `shutdown.cancelled()`: the tray kube-context supervisor (`tray::spawn_tray_subscription`) so it stops reconnecting and abandons any backoff sleep, and the Unix signal handler (`lib::spawn_signal_handler`) so it exits if Quit came through another path. **New app-lifetime background tasks should follow the same pattern** — clone the token from `app.state::<AppState>().shutdown` and select on it in every loop/await so Quit tears them down cleanly instead of leaving them churning against a sidecar that's going away. (Per-subscription teardown is separate — that's the `oneshot` cancel table in `graphql/subscribe.rs`.)

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
