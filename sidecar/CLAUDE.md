# sidecar/ — CLAUDE.md

The Go binary. The **only** component that talks to the kstack cloud. Serves a
local GraphQL API (gqlgen) over an AF_UNIX socket to the Rust host, and runs the
always-on reconciliation engine. See the root `CLAUDE.md` for the cross-process
request path and build ordering; this file is the per-package map.

## Entry point & lifecycle

- **`main.go`** — the composition root. Builds the shared instances
  (`syncStore`, `prefs.Hub`, `authcreds.Holder`, `cloud.Client`,
  `mutationqueue.Queue`, `sync.Engine`) and threads them both into
  `server.NewHandler` and into `engine.Run`. Owns shutdown ordering: HTTP
  `Shutdown` → wait for hijacked WS conns → bounded join on the engine. Parses
  flags (`--socket`, `--cloud-url`, `--prefs-path`); each has a
  production default an env var can override (`KSTACK_CLOUD_URL`).
- **`listen_unix.go` / `listen_windows.go`** — per-OS socket bind, both
  restricted to the current user (UDS chmod 0600 via a tightened umask;
  named-pipe SDDL `D:P(A;;GA;;;OW)`). Build-tagged.
- The sidecar prints `READY unix:<path>` on stdout (the host parses it) and
  shuts down on SIGINT/SIGTERM **or stdin EOF** (parent-gone signal). stdout is
  reserved for that one line — all logs go to stderr.

## `server/` — the HTTP/GraphQL surface

- **`server.go`** — wires the gqlgen executable schema into an
  `http.Handler`. Also mounts two **host-only** control endpoints off the
  GraphQL surface: `POST /control/credentials` (writes the `authcreds.Holder`)
  and `POST /control/wake` (calls the engine's `Poke` — the OS-wake hook).
  `Config{}` with no engine wired falls back to a no-op status source so tests
  don't need the engine.
- **`paths.go`** — single definition of the on-disk layout
  (`SyncPath` → `<prefs-dir>/sync/<file>`) so Store/Queue defaults and `main()`
  can't drift.
- **`shutdown.go`** — `AttachGracefulShutdown`: delivers a clean WS Close frame
  to hijacked subscriptions on shutdown (gqlgen's WS transport cancels on the
  request context) and returns a wait fn the caller must invoke after
  `Shutdown` (net/http doesn't drain hijacked conns).
- **`graph/`** — `schema.graphqls` is authoritative (see "dual codegen" in root
  `CLAUDE.md`); `generated.go`/`models_gen.go` are gqlgen output; resolvers are
  hand-written in `*.resolvers.go`. `resolver.go` is the DI struct.
  `stream.go`'s `streamWithSnapshot` is the shared snapshot-then-stream body for
  the `settingsWatch` and `syncStatusWatch` subscriptions. **Post-cutover the
  read path (`settings`, `settingsWatch`) is served from the
  engine-maintained syncstore + shared Hub, not a synchronous cloud call;**
  `Cloud` is retained only for the `updateSettings` write-through.

## `internal/` — the always-on engine and its state

This is wired and running (constructed in `main.go`, `engine.Run` in a
goroutine) — not dormant scaffolding.

- **`syncstore`** — generic `Store[T]` over an `Envelope` (payload + version
  cursor + last-synced / last-event timestamps); the engine's resume state.
  Resource-agnostic (Settings today, cluster state later).
- **`sync`** — the reconciliation engine. `engine.go`: supervised
  single-upstream loop (snapshot → persist → stream into `prefs.Hub`), state
  machine (`CONNECTING/LIVE/BACKOFF/OFFLINE`), exponential backoff, wall-clock
  wake detector (backstop for missed OS wakes), `Poke` for forced resync.
  Talks to an `Upstream` interface, not `*cloud.Client`, so other resources can
  reuse it. `cloud_upstream.go`: the `CloudUpstream` adapter binding
  `cloud.Client` + `authcreds.Holder` (pulls the token per call; no creds ⇒
  treated as Offline, never an unauthenticated cloud call).
- **`cloud`** — minimal GraphQL-over-HTTP client for the cloud's Settings
  surface (`GetSettings`/`UpdateSettings`/`WatchSettings` SSE). Stateless re:
  auth — every call takes a bearer token; never persists it.
- **`authcreds`** — `Holder` for the always-on token, written by
  `/control/credentials`, read by `CloudUpstream`. Empty until the host pushes.
- **`mutationqueue`** — durable offline-write buffer for `updateSettings`.
  Coalesces to latest (Settings is one deep-merged field, last-write-wins).
  Drained by the engine's `OnConnected` on every healthy (re)connect; in-memory
  atomics (`pending`/`seq`/`draining`) keep the dominant "nothing queued" path
  off disk and make concurrent drains safe.
- **`hub`** — generic in-process fan-out; slow consumer drops, never stalls the
  publisher. `prefs.Hub` (Settings events) and the engine's status hub are both
  this. **`prefs`** — local Settings cache + the `Hub` alias; `Settings` JSON
  tags must match the GraphQL field names (same bytes on the wire).
- **`atomicjson`** — crash-safe single-document JSON I/O (tmp + rename); a
  missing file reads as the zero value. Not internally synchronized — `prefs`
  and `syncstore` Stores each serialize writers behind a mutex.
- **`logging`** — slog JSON to stderr; `ParseLevel` mirrors the Rust host's
  `KSTACK_LOG_LEVEL` mapping (keep the two in sync).

## Working here

- Schema edits: regenerate **both** pipelines (`cd sidecar && go generate ./...`
  or per `gqlgen.yml`, plus `pnpm codegen` from the repo root) — a one-sided
  edit compiles but drifts.
- Single test: `cd sidecar && go test ./internal/prefs/ -run TestName`.
- The engine is resource-agnostic by construction; when adding a new synced
  resource prefer a new `Upstream` impl + `Store[T]` over branching the engine.
