# kstack-app

A Kubernetes desktop app (Kubetail). Three parts that talk to each other:

- **`src/`** — React webview (this file's focus).
- **`src-tauri/`** — Tauri/Rust native host (windows, tray, sidecar lifecycle). See `src-tauri/CLAUDE.md`.
- **`sidecar/`** — Go sidecar (GraphQL API, Kubernetes + cloud logic). See `sidecar/CLAUDE.md`.
- **`proto/`** — shared gRPC contract (host↔sidecar). Single source of truth, compiled to Go (committed, via `protoc`) and Rust (via `src-tauri/build.rs`). Run `make proto` after editing.

The **`Makefile` is the polyglot entry point** — Go/Rust/JS commands meet there (`make test`, `make lint`, `make vet`, `make proto`). The Go sidecar deliberately lives outside `package.json`.

## Host↔sidecar transport: GraphQL **and** gRPC over one socket (h2c)

The host and sidecar share a single Unix socket / named pipe. **GraphQL** (webview↔sidecar) rides HTTP/1.1 (POST + SSE). **gRPC** (host↔sidecar control channel — today the tray's auth-state watch + login/logout, and the resync poke) rides HTTP/2. Both are multiplexed by **h2c**: the sidecar's composition root (`internal/app`) wraps its handler in `h2c.NewHandler` with a dispatcher (keyed on `grpcserver.IsGRPCRequest`) that routes HTTP/2 `application/grpc` requests to the gRPC server and everything else to GraphQL. No TLS (the socket is user-restricted). The cluster surface is **GraphQL-only** (`clusters`/`clustersWatch` + the parallel `clusterCachesWatch` + cluster mutations), consumed by the webview; there is no gRPC kube/cluster channel. `clustersWatch`/`clusterCachesWatch` are **Kubernetes-style delta watches** (an `Added` snapshot then per-object `Added`/`Modified`/`Deleted`), streaming the Cluster and ClusterCache kinds independently — the webview keys each into a map and joins caches onto clusters by `clusterID`. (The webview's kube-context picker derives its context list from `clustersWatch` — see `src/lib/kube-config.tsx`.)

> **Keep these docs current.** There's a `CLAUDE.md` per area (root/frontend, `src-tauri/`, `sidecar/`). When you change architecture, conventions, commands, or add a pattern worth knowing, update the relevant `CLAUDE.md` in the same change.
>
> **Keep comments current — describe the present, not the past.** Code comments (and these docs) must describe only the *current* state of the code. When you change something, update the comments around it instead of layering on history: don't leave notes about how the code *used to* work, what a field was *renamed from*, what machinery was *removed*, or which past refactor got you here. A reader should never have to reason about a prior version. State current design and rationale directly; drop "used to", "no longer", "formerly", "superseded", and similar backward-looking framing. (Contrasts with *external* systems — e.g. "unlike apimachinery's default" — are fine; they describe present behavior.)

## Frontend (`src/`)

React 19 + TanStack Router + [urql] (GraphQL) + Vite + Tailwind v4. UI primitives come from `@kubetail/ui`. Package manager is **pnpm**.

- Entry: `src/main.tsx` → `RouterProvider`. Routes in `src/routes/` (code-based, wired in `src/routeTree.ts`): `__root.tsx` mounts the provider stack only and renders an `Outlet`; `_app.tsx` is a **pathless layout route** whose component is `AppLayout` (the sidebar shell), and the pages — `chat.tsx` (`/chat`) and `dashboard.tsx` (`/dashboard`) — nest under it so they share that shell. Chat and Dashboard are **peer named routes**; `index.tsx` (`/`) owns no view — it redirects to `DEFAULT_ROUTE` (the one place the default lives), so New Window / secondary windows can open at `/`. The Chat/Dashboard switch is the sidebar's `ModeNav` (router `Link`s, not local state), so each mode is a real, deep-linkable route.
- **Layouts** live in `src/layouts/` — presentational shells (each renders an `Outlet`) that layout routes mount. `AppLayout` is the main window (floating sidebar + inset). Secondary windows (log tail, container exec) will get their own sidebar-less layout here, nested under a separate pathless layout route; a Tauri window just opens at that route's path (see `src-tauri/window_manager.rs`).
- Path alias: **`@/*` → `src/*`**.
- App-wide concerns live in `src/lib/` (auth, error bus/boundary, sync-status, kube-config, ready-gate, `platform.ts` for sync `isMacOS()` UA detection); reusable UI in `src/components/widgets/`.
- **Window frame**: off macOS the window is not only frameless but **transparent** (`src-tauri`), so the OS draws no border or drop shadow. `components/widgets/window-frame.tsx` (`WindowFrame`, wrapping the whole app at the root — see `routes/__root.tsx`, so loading/error states get it too) supplies them: it insets the app a few px from the window edge, clips to rounded corners, and paints a thin border + soft outer shadow into the transparent gutter. It uses `contain: paint` so the app's `position: fixed` chrome (title bar, sidebar) re-anchors to the inset frame instead of the window edge. `main.tsx` tags `<html class="frameless">` off macOS so the document background goes transparent (revealing the gutter). On macOS this is a passthrough — the native decorations already give it corners + shadow.
- **Title bar / menu**: macOS keeps its native traffic lights and native global menu bar (`src-tauri/app_menu.rs`); the sidebar header doubles as the title bar. On Linux/Windows the window is frameless with a full-width custom title bar across the top (`app-sidebar.tsx`): a hamburger **app menu** (`components/widgets/app-menu.tsx`, `AppMenu`) at the left, a draggable strip in the middle, and custom **window controls** (`components/widgets/window-controls.tsx`, `WindowControls` — minimize/maximize/close, styled to read as native since there are no OS caption buttons) at the right; the floating sidebar starts just below the bar. Off macOS there's no native menu bar, so `AppMenu` stands in for it as the single hamburger holding the app-wide actions (New Window and Quit); it renders only off-macOS and owns the `Ctrl/Cmd+N` / `Ctrl/Cmd+Q` shortcuts, invoking the `new_window` / `quit` Tauri commands.

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
