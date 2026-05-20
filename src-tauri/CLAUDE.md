# src-tauri/ — CLAUDE.md

The Rust Tauri 2 host. Owns windows, tray, menus, OAuth, and the Go sidecar's
lifecycle. It does **not** talk to the cloud — every cloud-bound operation is
bridged to the sidecar over AF_UNIX (see the request path in the root
`CLAUDE.md`). This file is the per-module map.

## Entry point

- **`main.rs`** — thin; calls `lib.rs::run`.
- **`lib.rs`** — the Tauri builder. **Plugin registration order is
  load-bearing**: `tauri_plugin_single_instance` must stay first (a second
  launch exits inside its init and forwards into the original process before
  any other plugin binds shared resources). `setup` spawns the sidecar, inits
  deep links, starts the auth session broadcaster, runs the off-critical-path
  keychain restore, then spawns the wake signal + credential pusher. Closing
  the last window does **not** exit (`ExitRequested` with no code →
  `prevent_exit`); exit is the tray "Quit" / Cmd+Q calling `app.exit`.
  `host_log_level` mirrors the sidecar's `KSTACK_LOG_LEVEL` mapping — keep in sync.

## `auth/` — OAuth 2.1 / OIDC (public client, PKCE, no secret)

Loopback redirect flow (`http://127.0.0.1:<ephemeral>/oauth/callback`, RFC 8252
§7.3) against Hydra — chosen over the `kstack://` scheme because it works
identically in dev/prod, signed/unsigned, every OS.

- **`mod.rs`** — module docs + threat model; the `AUTH` global; event name
  constants (`SESSION_RESOLVED_EVENT`).
- **`flow.rs`** — login state machine wrapping `openidconnect::CoreClient`
  (which owns discovery/PKCE/token endpoint/ID-token verification).
- **`refresher.rs`** — the proactive refresh driver. Owns refresh *timing*:
  `select!`s on a TTL-derived timer (~75% of lifetime, `MIN_MARGIN` floor,
  `MAX_REARM` cap) + the host wake signal; on either it calls
  `access_token_with_expiry`, which runs the expiry-gated refresh path. A
  successful refresh bumps `creds_gen`, which the sidecar credential pusher
  observes.
- **`loopback.rs`** — ephemeral 127.0.0.1 listener that catches the redirect
  for one login.
- **`tokens.rs`** — token storage. Refresh token (only durable secret) → OS
  keychain in **release**, mode-0600 file in **debug** (`cfg(debug_assertions)`;
  avoids a keychain prompt on every dev rebuild's changed signing identity).
  Access/ID tokens stay in memory (short-lived).
- **`commands.rs`** — the `auth_login/_logout/_status/_access_token` Tauri
  commands; always opens the system browser (never an embedded webview).
- **`broadcast.rs`** — fans post-startup credential changes out to every window
  via an app-wide event, so a login/logout in one window updates the others.
  Event-driven off `Auth`'s `creds_gen` watch.

## `sidecar/` — bridge, transport, lifecycle

- **`mod.rs`** — intentionally small public surface (`spawn`/`shutdown`,
  `graphql_query`, the bits integration tests need). `command` is `pub` so
  `generate_handler!` can resolve the macro path.
- **`command.rs`** — the `graphql_query/_subscribe/_unsubscribe` invoke
  handlers; fetches the access token from `AUTH` per request (`None` = logged
  out → resolver auth fails, which is correct).
- **`transport.rs`** — the wire transport. One `Content-Length`-framed HTTP/1.1
  POST per call, no keep-alive; deliberately minimal (both ends controlled).
  `post_uds` is the shared request builder.
- **`lifecycle.rs`** — spawns the `externalBin`, watches stdout for the `READY`
  line, graceful shutdown on app exit.
- **`subscribe.rs`** — graphql-transport-ws over UDS, one WebSocket per
  subscription (no browser per-origin cap to dodge here).
- **`credentials.rs`** — the **credential pusher**: pure transport that
  re-publishes the access token to the sidecar's `/control/credentials`.
  `select!`s only on `Auth::watch_credentials()` (bumped by the auth
  refresher on login / refresh / logout); pushes when the token actually
  changed (dedup on `last`). No timer, no wake here — see
  `auth/refresher.rs`.
- **`wake_poster.rs`** — on every host wake, `POST`s the sidecar's
  `/control/wake` so the engine reacts immediately (today: an upstream
  resync) instead of waiting on its wall-clock backstop. Best-effort:
  failures are logged at debug and the next wake retries. Subscribes to
  the top-level [`wake`](#wake--host-level-os-wake-signal) module.

## `wake/` — host-level OS wake signal

- **`mod.rs`** — `Waker` (a cheaply-cloneable `watch<u64>` publisher) +
  `spawn_wake()` which starts the per-OS listeners and returns the
  `Waker`. The caller (`lib.rs`) is responsible for keeping the `Waker`
  alive (handed to `app.manage`); if every clone drops, the channel
  closes and every subscriber's `changed()` errors. `changed()` is the
  shared helper that names the `.changed().await.is_ok()` pattern used
  across `select!` arms.
- **`{macos,linux,win}.rs`** — per-OS sources (NSWorkspace notifications,
  zbus over logind/NetworkManager, Win32 power/network broadcasts).
  Unwired platforms compile to a no-op; the engine wall-clock backstop
  and the refresher's `MAX_REARM` cover suspend/resume.

  Top-level (not under `sidecar/`) because the signal is a host-level
  OS event with subscribers in both `auth` and `sidecar`, and `auth`
  shouldn't depend on `sidecar`.

## Other modules

- **`deep_link.rs`** — `kstack://` scheme registration (declared in
  `tauri.conf.json`). **Not** used for OAuth; kept for future "open to a
  cluster from a URL" features. Three delivery paths converge on `on_open_url`.
- **`windows.rs`** — multi-window spawning, the app menu, and the
  close-to-tray policy (last visible window hides, process stays alive so the
  sidecar/engine keep running).

## Conventions & build

- `#![warn(clippy::unwrap_used)]` is on for runtime code; tests opt out
  per-module with `#[allow(clippy::unwrap_used)]`. `make vet-rust` runs clippy
  with `-D warnings`.
- **Build ordering**: `tests/sidecar_smoke.rs` spawns the *real* sidecar
  binary, and `externalBin` embeds it, so the Go sidecar must be built before
  anything builds/tests Rust. `make test-rust` and `tauri.conf.json`'s
  before-commands encode this — preserve it for any new build path.
  `build.rs` is the Tauri codegen build script; `capabilities/default.json`
  is the Tauri 2 capability set; `entitlements.plist` is macOS signing.
- Single test: `cd src-tauri && cargo test name`.
- If you add a new `Auth` token mutation, bump the `creds_gen` watch
  (`bump_creds`) or the pusher and the session broadcaster won't see it.
