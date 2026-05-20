# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Tauri 2 desktop app for kstack. Three languages, one process tree:

- **`src/`** — React 19 + TypeScript frontend (Vite, urql, TanStack Router). Runs in the Tauri webview. **Detailed below** — this root file covers the frontend and the cross-process contracts.
- **`src-tauri/`** — Rust Tauri host. Owns windows, tray, menus, auth, and the sidecar lifecycle. **See `src-tauri/CLAUDE.md`** for the per-module map.
- **`sidecar/`** — Go binary. The **only** component that talks to the kstack cloud. Serves a local GraphQL API (gqlgen) and runs the always-on sync engine. **See `sidecar/CLAUDE.md`** for the per-package map.

## Commands

The `Makefile` is the polyglot entry point (Go/Rust/JS commands meet here); use it rather than running per-language commands ad hoc.

- `make test` — all suites. Individual: `make test-go`, `make test-rust`, `make test-js`.
- `make lint` / `make vet` — formatting checks / static analysis, per language (`-go`, `-rust`, `-js` suffixes).
- `make sidecar` — build the Go sidecar into `src-tauri/binaries/` (see "Build ordering" below).
- Single tests: `cd sidecar && go test ./internal/prefs/ -run TestName`; `cd src-tauri && cargo test name`; `pnpm test -- invoke-fetch` (or `pnpm test --run` for one-shot).
- Run the app: `pnpm tauri dev` (its `beforeDevCommand` is `make sidecar && pnpm dev`, so the sidecar is rebuilt automatically). `pnpm dev` alone runs only the frontend with no sidecar.

## Build ordering (important)

The Rust host embeds the Go sidecar via Tauri's `externalBin`. `scripts/build-sidecar.sh` compiles it to `src-tauri/binaries/kstack-sidecar-<rust-host-triple>` — note the filename uses the **Rust** host triple (from `rustc -vV`), not Go's. The Rust integration tests spawn the *real* sidecar binary, so anything that builds or tests Rust must build the sidecar first. `make test-rust` and `tauri.conf.json`'s before-commands already encode this dependency; preserve it if you add new build paths.

For a **universal macOS build** (`tauri build --target universal-apple-darwin`), `externalBin` looks for a single fat binary named `kstack-sidecar-universal-apple-darwin`. Tauri does *not* lipo the per-arch sidecars together for you — the `build-bundle-macos` job in `.github/workflows/release.yml` downloads both per-arch sidecars and `lipo -create`s them into the universal-named binary before `tauri build`.

## Architecture: the request path

The webview cannot dial the sidecar directly (it's on an AF_UNIX socket, not a TCP port). Every GraphQL operation flows through the host:

```
urql  →  invokeFetch (src/lib/graphql/invoke-fetch.ts)
      →  Tauri invoke: graphql_query / graphql_subscribe / graphql_unsubscribe
      →  Rust host (src-tauri/src/sidecar/command.rs, transport.rs)
      →  AF_UNIX socket  →  Go sidecar (sidecar/server)  →  kstack cloud
```

`invokeFetch` adapts urql's stock `fetchExchange` onto the Tauri `invoke` bridge instead of `window.fetch`. The Rust transport speaks one `Content-Length`-framed HTTP/1.1 POST per call, no keep-alive — both ends are controlled, so it's deliberately minimal (`transport::post_uds` is the shared request builder; `query_uds` and `push_credentials` both delegate). The sidecar is started by `sidecar::spawn` (`src-tauri/src/sidecar/lifecycle.rs`), announces `READY unix:<path>` on stdout for the host to parse, and shuts down on SIGINT/SIGTERM **or stdin EOF** (the host closing its end is the cross-platform "parent gone" signal). IPC is AF_UNIX everywhere — UDS on macOS/Linux, named pipe / AF_UNIX on Windows (requires Windows 10 build 17063+); see `sidecar/listen_unix.go` vs `listen_windows.go`.

The UDS is not GraphQL-only and the host does more than bridge requests: the same socket also serves **host-only `POST /control/credentials`** and **`POST /control/wake`** endpoints (deliberately off the GraphQL surface — the webview has no business setting process credentials or poking the engine; the UDS is already user-0600), and the host runs background tasks (the credential pusher and wake poster, see Auth). The sidecar is now an always-on stateful daemon (see "always-on sidecar" below).

## Architecture: the frontend (`src/`)

The webview app. Entry is `src/main.tsx` → TanStack Router (`routeTree.ts`,
`routes/`). The tree is file-light today (`__root.tsx` + `index.tsx`); `__root`
mounts the app-wide providers in order: `UrqlProvider` → `SessionProvider` →
`SyncStatusProvider`, plus `ErrorBoundary`, `ConnectionStatus`, `ProfileMenu`,
`SyncHealthBadge`. UI primitives come from `@kubetail/ui`.

Key modules under `src/lib/`:

- **`graphql/`** — the urql client and its custom exchanges.
  `client.ts` assembles the client (`cacheExchange` → `errorReportExchange` →
  `networkRetryExchange` → `tauriSubscriptionExchange` → `fetchExchange`). The
  URL is a dummy — **`invoke-fetch.ts`** swaps urql's `window.fetch` for the
  Tauri `invoke` bridge (see the request path above). `subscribe-exchange.ts`
  routes subscription operations over the host's WS bridge with its own
  reconnect (no jitter — one long-lived client). `networkRetryExchange` retries
  **only** `networkError`s (sidecar restart blips), never GraphQL errors,
  bounded, with jitter to de-correlate a route's parallel queries.
- **`auth.tsx`** — `SessionProvider` / `useSession`. The renderer **never holds
  tokens**: it reads session state from the Rust host and triggers login/logout
  via Tauri commands; the access token is fetched on demand by `invoke-fetch`.
  Consumes the one-shot `auth:session-resolved` startup event (mirror of
  `auth::SESSION_RESOLVED_EVENT`) so no follow-up `auth_status` round-trip is
  needed.
- **`sync-status.tsx`** — `SyncStatusProvider`: subscribes `syncStatusWatch`
  (snapshot then live transitions) and adapts the engine's health into context;
  `sync-health-badge.tsx` renders it.
- **`error-bus.ts`** — tiny `EventTarget`-backed pub/sub. Producers (graphql /
  subscription / network / render / auth failures) publish; `connection-status.tsx`
  renders the latest as an auto-dismissing banner. `error-boundary.tsx` feeds
  render-time exceptions in. Decoupled so a future Sentry tap needs no producer
  changes.

Frontend dev/test: `pnpm dev` (frontend only, **no sidecar** — cloud ops fail),
`pnpm tauri dev` (full stack), `pnpm test -- <name>` / `pnpm test --run`,
`pnpm codegen` after a schema change (see dual-codegen below).

## Architecture: always-on sidecar (landed)

The sidecar is now an always-on engine that keeps cloud-synced state locally,
not just a request proxy. It is constructed and started in `sidecar/main.go`
(`engine.Run` in a goroutine); the read path (`settings`, `settingsWatch`,
`syncStatusWatch`) is served from the engine-maintained local store + hub, with
no synchronous cloud call. The frontend contract: query `settings`/`syncStatus`
and subscribe `settingsWatch`/`syncStatusWatch`; `updateSettings` is a
write-through (cloud, then echoed back and persisted; queued offline). Engine
internals — `syncstore`, `sync` (engine + cloud upstream), `mutationqueue`,
`authcreds`, `hub` — are mapped in **`sidecar/CLAUDE.md`**.

## Architecture: the GraphQL schema is single-source, dual-generated

`sidecar/server/graph/schema.graphqls` is authoritative. **Two** codegen pipelines consume it and must both be regenerated after any schema change:

- **Go**: gqlgen (`sidecar/gqlgen.yml`) → `sidecar/server/graph/generated.go` + `models_gen.go`. Resolvers are hand-written in `*.resolvers.go`.
- **TypeScript**: graphql-codegen (`codegen.ts`) → `src/gql/` (typed document nodes for urql). Run `pnpm codegen` (or `pnpm codegen:watch`).

A schema edit that updates only one side will compile but drift.

## Auth

OAuth loopback flow lives in `src-tauri/src/auth/` (Rust) — per-module detail in
**`src-tauri/CLAUDE.md`**. On startup the host does a silent keychain restore
off the critical path and emits the one-shot `auth:session-resolved` event
(`auth::SESSION_RESOLVED_EVENT`) carrying the resolved status, so the renderer
needs no follow-up round-trip. The frontend consumes it in `src/lib/auth.tsx`.

There are **two auth paths**, don't conflate them:

- **Request-scoped** (every webview op): bearer travels per-request through `graphql_query` → resolver context. Stateless; the sidecar never stores it.
- **Always-on** (the engine has no inbound request): two cooperating tasks.
  - The **auth refresher** (`src-tauri/src/auth/refresher.rs`) owns the
    *timing* of refresh: it `select!`s on a TTL-derived timer (~75% of
    lifetime, `MIN_MARGIN` floor, `MAX_REARM` cap) and the OS power/network
    host wake (`src-tauri/src/wake/`); on either it calls
    `Auth::access_token_with_expiry`, which runs the expiry-gated refresh
    path. A successful refresh bumps `creds_gen`.
  - The **credential pusher** (`src-tauri/src/sidecar/credentials.rs`) is
    pure transport: it `select!`s only on `Auth::watch_credentials()` and
    pushes the current access token to the sidecar's `/control/credentials`
    whenever the token actually changed (dedup on `last`). No timer, no
    wake — the refresher's `creds_gen` bump is the only signal it needs.

  A separate **wake poster** (`src-tauri/src/sidecar/wake_poster.rs`) also
  subscribes to the wake signal and `POST`s the sidecar's `/control/wake`
  to poke the engine into an immediate reconnect. `MAX_REARM` is a
  defense-in-depth backstop for a missed wake or an unwired platform. If you add new `Auth` token mutations, bump the
  `creds_gen` watch (`bump_creds`) or neither task will see them.

## Conventions

- **Env vars**: `KSTACK_LOG_LEVEL` sets verbosity for *both* the Rust host and the Go sidecar (kept in sync between `lib.rs::host_log_level` and `sidecar/internal/logging.ParseLevel`). `KSTACK_CLOUD_URL` is the cloud base URL — forwarded to the sidecar, which appends `/graphql` itself; unset means the compiled-in production default.
- **Rust**: `#![warn(clippy::unwrap_used)]` is on for runtime code; tests opt out per-module. `make vet-rust` runs clippy with `-D warnings`.
- **Window/process lifecycle**: closing the last window does **not** exit — the process stays alive (tray "Quit" or Cmd+Q calls `app.exit`). `tauri_plugin_single_instance` must stay registered first; second launches forward into the original process. Don't reorder plugin registration in `lib.rs` casually.
- **Cross-platform builds**: Tauri can't cross-compile to Windows from macOS; the CI matrix in `.github/workflows/ci.yml` covers Linux/macOS/Windows × amd64/arm64 for Go and Rust. `ci.yml` has `workflow_dispatch` enabled (`gh workflow run ci.yml --ref <branch>`).

## Dev sandbox

The README documents a Docker/`sbx` sandbox flow (`Dockerfile.sbx`, `scripts/expose-dev.sh`) for running the dev server in an isolated container with port forwarding.
