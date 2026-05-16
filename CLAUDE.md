# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Tauri 2 desktop app for kstack. Three languages, one process tree:

- **`src/`** — React 19 + TypeScript frontend (Vite, urql, TanStack Router). Runs in the Tauri webview.
- **`src-tauri/`** — Rust Tauri host. Owns windows, tray, menus, auth, and the sidecar lifecycle.
- **`sidecar/`** — Go binary. The **only** component that talks to the kstack cloud. Serves a local GraphQL API (gqlgen).

## Commands

The `Makefile` is the polyglot entry point (Go/Rust/JS commands meet here); use it rather than running per-language commands ad hoc.

- `make test` — all suites. Individual: `make test-go`, `make test-rust`, `make test-js`.
- `make lint` / `make vet` — formatting checks / static analysis, per language (`-go`, `-rust`, `-js` suffixes).
- `make sidecar` — build the Go sidecar into `src-tauri/binaries/` (see "Build ordering" below).
- Single tests: `cd sidecar && go test ./internal/prefs/ -run TestName`; `cd src-tauri && cargo test name`; `pnpm test -- invoke-fetch` (or `pnpm test --run` for one-shot).
- Run the app: `pnpm tauri dev` (its `beforeDevCommand` is `make sidecar && pnpm dev`, so the sidecar is rebuilt automatically). `pnpm dev` alone runs only the frontend with no sidecar.

## Build ordering (important)

The Rust host embeds the Go sidecar via Tauri's `externalBin`. `scripts/build-sidecar.sh` compiles it to `src-tauri/binaries/kstack-sidecar-<rust-host-triple>` — note the filename uses the **Rust** host triple (from `rustc -vV`), not Go's. The Rust integration tests spawn the *real* sidecar binary, so anything that builds or tests Rust must build the sidecar first. `make test-rust` and `tauri.conf.json`'s before-commands already encode this dependency; preserve it if you add new build paths.

## Architecture: the request path

The webview cannot dial the sidecar directly (it's on an AF_UNIX socket, not a TCP port). Every GraphQL operation flows through the host:

```
urql  →  invokeFetch (src/lib/graphql/invoke-fetch.ts)
      →  Tauri invoke: graphql_query / graphql_subscribe / graphql_unsubscribe
      →  Rust host (src-tauri/src/sidecar/command.rs, transport.rs)
      →  AF_UNIX socket  →  Go sidecar (sidecar/server)  →  kstack cloud
```

`invokeFetch` adapts urql's stock `fetchExchange` onto the Tauri `invoke` bridge instead of `window.fetch`. The Rust transport speaks one `Content-Length`-framed HTTP/1.1 POST per call, no keep-alive — both ends are controlled, so it's deliberately minimal. The sidecar is started by `sidecar::spawn` (`src-tauri/src/sidecar/lifecycle.rs`), announces `READY unix:<path>` on stdout for the host to parse, and shuts down on SIGINT/SIGTERM **or stdin EOF** (the host closing its end is the cross-platform "parent gone" signal). IPC is AF_UNIX everywhere — UDS on macOS/Linux, named pipe / AF_UNIX on Windows (requires Windows 10 build 17063+); see `sidecar/listen_unix.go` vs `listen_windows.go`.

## Architecture: the GraphQL schema is single-source, dual-generated

`sidecar/server/graph/schema.graphqls` is authoritative. **Two** codegen pipelines consume it and must both be regenerated after any schema change:

- **Go**: gqlgen (`sidecar/gqlgen.yml`) → `sidecar/server/graph/generated.go` + `models_gen.go`. Resolvers are hand-written in `*.resolvers.go`.
- **TypeScript**: graphql-codegen (`codegen.ts`) → `src/gql/` (typed document nodes for urql). Run `pnpm codegen` (or `pnpm codegen:watch`).

A schema edit that updates only one side will compile but drift.

## Auth

OAuth loopback flow lives in `src-tauri/src/auth/` (Rust): `auth_login`/`auth_logout`/`auth_status`/`auth_access_token` invoke handlers, tokens in the OS keychain. On startup the host attempts a silent keychain restore off the critical path and emits `RESTORE_EVENT` with the resolved status so the renderer needs no follow-up round-trip. The frontend consumes this via `src/lib/auth/auth-context.tsx`.

## Conventions

- **Env vars**: `KSTACK_LOG` sets verbosity for *both* the Rust host and the Go sidecar (kept in sync between `lib.rs::host_log_level` and `sidecar/internal/logging.ParseLevel`). `KSTACK_CLOUD_URL` is the cloud base URL — forwarded to the sidecar, which appends `/graphql` itself; unset means the compiled-in production default.
- **Rust**: `#![warn(clippy::unwrap_used)]` is on for runtime code; tests opt out per-module. `make vet-rust` runs clippy with `-D warnings`.
- **Window/process lifecycle**: closing the last window does **not** exit — the process stays alive (tray "Quit" or Cmd+Q calls `app.exit`). `tauri_plugin_single_instance` must stay registered first; second launches forward into the original process. Don't reorder plugin registration in `lib.rs` casually.
- **Cross-platform builds**: Tauri can't cross-compile to Windows from macOS; the CI matrix in `.github/workflows/ci.yml` covers Linux/macOS/Windows × amd64/arm64 for Go and Rust. `ci.yml` has `workflow_dispatch` enabled (`gh workflow run ci.yml --ref <branch>`).

## Dev sandbox

The README documents a Docker/`sbx` sandbox flow (`Dockerfile.sbx`, `scripts/expose-dev.sh`) for running the dev server in an isolated container with port forwarding.
