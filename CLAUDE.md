# kstack-app

A Kubernetes desktop app (Kubetail). Three parts that talk to each other:

- **`src/`** — React webview (this file's focus).
- **`src-tauri/`** — Tauri/Rust native host (windows, tray, sidecar lifecycle). See `src-tauri/CLAUDE.md`.
- **`sidecar/`** — Go sidecar (GraphQL API, Kubernetes + cloud logic). See `sidecar/CLAUDE.md`.
- **`proto/`** — shared gRPC contract (host↔sidecar). Single source of truth, compiled to Go (committed, via `protoc`) and Rust (via `src-tauri/build.rs`). Run `make proto` after editing.

The **`Makefile` is the polyglot entry point** — Go/Rust/JS commands meet there (`make test`, `make lint`, `make vet`, `make proto`). The Go sidecar deliberately lives outside `package.json`.

## Host↔sidecar transport: GraphQL **and** gRPC over one socket (h2c)

The host and sidecar share a single Unix socket / named pipe. **GraphQL** (webview↔sidecar) rides HTTP/1.1 (POST + SSE). **gRPC** (host↔sidecar control channel — today the tray's kube-context watch/set) rides HTTP/2. Both are multiplexed by **h2c**: the sidecar's composition root (`internal/app`) wraps its handler in `h2c.NewHandler` with a dispatcher (keyed on `grpcserver.IsGRPCRequest`) that routes HTTP/2 `application/grpc` requests to the gRPC server and everything else to GraphQL. No TLS (the socket is user-restricted). gRPC and GraphQL kube-context surfaces share the one `KubeConfigWatcher`, so a change via either is seen by both.

> **Keep these docs current.** There's a `CLAUDE.md` per area (root/frontend, `src-tauri/`, `sidecar/`). When you change architecture, conventions, commands, or add a pattern worth knowing, update the relevant `CLAUDE.md` in the same change.

## Frontend (`src/`)

React 19 + TanStack Router + [urql] (GraphQL) + Vite + Tailwind v4. UI primitives come from `@kubetail/ui`. Package manager is **pnpm**.

- Entry: `src/main.tsx` → `RouterProvider`. Routes in `src/routes/` (`__root.tsx` is the layout shell that mounts all providers; `index.tsx` etc. are pages). `src/routeTree.ts` wires them.
- Path alias: **`@/*` → `src/*`**.
- App-wide concerns live in `src/lib/` (auth, error bus/boundary, sync-status, kube-config, ready-gate); reusable UI in `src/components/widgets/`.

### GraphQL — never talks HTTP directly

The webview has **no network access**. urql is configured (`src/lib/graphql/`) to route every operation through Tauri IPC:

- Queries/mutations → `invoke('graphql_query')` (`invoke-fetch.ts`).
- Subscriptions → `invoke('graphql_subscribe' / 'graphql_unsubscribe')` over a Tauri `Channel` (`subscribe-exchange.ts`, with its own capped-backoff reconnect).

The Rust host forwards these to the sidecar over a Unix socket — queries as plain HTTP POSTs, and each subscription as its own SSE (`text/event-stream`) stream that the host parses into the `Channel` frames. When touching transport/retry logic, read `src/lib/graphql/client.ts` and its tests first.

### Codegen — generated types, don't hand-write

`pnpm codegen` (`codegen.ts`) reads the **sidecar's** schema (`sidecar/graph/schema.graphqls` — single source of truth) and emits `src/gql/`.

- Write operations with the **`graphql()` tagged template from `@/gql`**; the typed document drives urql.
- **`src/gql/` is generated — never edit it.** After changing a query or the sidecar schema, run `pnpm codegen` (or `pnpm codegen:watch`).

### Tests

Vitest + `@testing-library/react` (jsdom). Tests are co-located (`*.test.ts[x]`). Mock the Tauri bridge with **`mockTauriCore()` from `@/test-utils`** (and `vi.mock('@tauri-apps/api/core', …)` + dynamic import of the module under test). For GraphQL-driven components, push frames via `liveChannel().onmessage!(...)` — see `src/components/widgets/sync-health-badge.test.tsx`.

- **No magic sleeps.** Don't wait on wall-clock delays to let async work settle — use `vi.useFakeTimers()` + `advanceTimersByTimeAsync`, `await waitFor(...)`, or a `flush()`/`act` helper that drains microtasks. Real `setTimeout` waits are flaky and slow.
- Run: `pnpm test --run` (or `make test-js`). Watch: `pnpm test`.

### Lint / build

- `pnpm lint` (ESLint: airbnb-extended + prettier) → `make lint-js`. Run before committing.
- `pnpm build` = `tsc -b && vite build`. Dev: `pnpm dev` (webview) or `pnpm tauri dev` (full app).

[urql]: https://commerce.nearform.com/open-source/urql/
