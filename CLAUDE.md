# kstack-app

A Kubernetes desktop app (Kubetail). Three parts that talk to each other:

- **`src/`** — React webview (this file's focus).
- **`src-tauri/`** — Tauri/Rust native host (windows, tray, sidecar lifecycle). See `src-tauri/CLAUDE.md`.
- **`sidecar/`** — Go sidecar (GraphQL API, Kubernetes + cloud logic). See `sidecar/CLAUDE.md`.
- **`proto/`** — shared gRPC contract (host↔sidecar). Single source of truth, compiled to Go (committed, via `protoc`) and Rust (via `src-tauri/build.rs`). Run `make proto` after editing.

The **`Makefile` is the polyglot entry point** — Go/Rust/JS commands meet there (`make test`, `make lint`, `make vet`, `make proto`). The Go sidecar deliberately lives outside `package.json`.

**Building inside a Linux sandbox (macOS host).** `scripts/sandbox-dev-setup.sh` **must run before any build, test, or install command** — a `SessionStart` hook in `.claude/settings.json` runs it automatically, and it's idempotent, so just run it yourself if you're unsure. It isolates the Linux build outputs under `.sandbox-linux/` so the host's macOS `node_modules` etc. are untouched; without it, a `pnpm install` here overwrites the host's native binaries and vice versa. It no-ops outside the sandbox. Details: → [ADR: sandbox build-output isolation](docs/adr/2026-08-09-sandbox-build-separation.md). See README for the one-time toolchain install.

## Host↔sidecar transport

The host and sidecar share a single Unix socket / named pipe. **GraphQL** (webview↔sidecar) rides HTTP/1.1 (POST + SSE); **gRPC** (host↔sidecar control: tray auth watch, login/logout, resync poke) rides HTTP/2. Both are multiplexed by **h2c** in the sidecar's `internal/app`; no TLS (the socket is user-restricted). The cluster surface is **GraphQL-only** — no gRPC kube channel. → [ADR: single-socket h2c](docs/adr/2026-08-09-single-socket-h2c.md).

**The sidecar's cluster backend has been stripped to a shell pending a rebuild** — everything below describes the wire contract, which is unchanged, but **every cluster field and subscription panics in the sidecar today**, `clustersWatch` included. The schema, the resolvers, and the whole webview are untouched and still compile; don't "fix" the frontend against the missing data. → `sidecar/CLAUDE.md`.

Cluster data streams as **Kubernetes-style delta watches** (an `Added` snapshot, one `Bookmark` closing it, then per-object `Added`/`Modified`/`Deleted`): `clustersWatch`, `clusterCachesWatch`, `clusterCacheGVRDiscoveriesWatch` (all unscoped), and the cache-scoped `clusterCacheGVRSyncsWatch` (opened only by whoever is looking at a cache). The sync verdict rides `clusterCacheSyncHealthWatch`, which is **a gauge, not a delta watch** — current-on-subscribe, so no `Bookmark` closes it and none of the Bookmark discipline below applies. Each kind is its own stream; the webview keys each into a map and **joins client-side**: caches onto clusters by `clusterID`, verdict + discovery onto their cache by `cacheID`. The `Bookmark` carries no entity (every change wrapper's entity field is nullable for it alone) and marks the snapshot complete — **never render an empty state before it lands**, or a populated table reads as empty for as long as the server takes to list it. **Detect it as `type === 'Bookmark'`, never as a missing entity**: a nested non-null field erroring nulls its parent, so a null entity also arrives on ordinary changes, and reading one as the snapshot boundary declares a still-loading collection complete. A change with no entity is dropped, not folded. → [ADR: delta-watch protocol](docs/adr/2026-08-09-delta-watch-protocol.md). The kube-context picker derives its list from `clustersWatch` (`src/lib/kube-config.tsx`).

> **Keep these docs current.** There's a `CLAUDE.md` per area (root/frontend, `src-tauri/`, `sidecar/`). When you change architecture, conventions, commands, or add a pattern worth knowing, update the relevant `CLAUDE.md` in the same change.
>
> **Rationale lives in ADRs, not here.** These files state *what is true now* — invariants, conventions, commands, traps. *Why* a design was chosen, and what we rejected, goes in `docs/adr/` (see [`docs/adr/README.md`](docs/adr/README.md)) and is linked from here. When a decision changes, write a new ADR, flip the old one's status, and repoint every link in the same commit — a `CLAUDE.md` must never link to a superseded ADR.
>
> **Keep comments current — describe the present, not the past.** Code comments (and these docs) must describe only the *current* state of the code. **`docs/adr/` is the sole exemption** — ADRs are an append-only historical record and are meant to say what we rejected, replaced, and believed at the time. When you change something, update the comments around it instead of layering on history: don't leave notes about how the code *used to* work, what a field was *renamed from*, what machinery was *removed*, or which past refactor got you here. A reader should never have to reason about a prior version. State current design and rationale directly; drop "used to", "no longer", "formerly", "superseded", and similar backward-looking framing. (Contrasts with *external* systems — e.g. "unlike apimachinery's default" — are fine; they describe present behavior.)

## Writing standards

Applies to every hand-written file in the repo — Go, Rust, TypeScript, Markdown. Not to generated output (`src/gql/`, `sidecar/graph/generated*`, lockfiles), which is nobody's to hand-tune.

1. **Code — simple, idiomatic, easy for a human to follow.** Prefer the boring construction. Match the idiom of the file you are in over the one you would pick on a blank page. Cleverness that needs a comment to survive review is usually the wrong trade.
2. **Comments — terse, necessary, easy for a human to read.** Say what the code cannot: the *why*, the invariant, the trap that made this shape necessary. Never restate what the line already says. A comment justifying a choice the code no longer contains is dead weight — state the current design, don't argue against the alternatives you rejected (that is what `docs/adr/` is for).
3. **Documentation — simple, concise, easy for a human to read.** Lead with what is true now. One idea per sentence.

The failure mode to watch for is a comment addressed to a *reviewer* — someone who just watched the reasoning — rather than to a reader opening the file cold. The tell is a comment that spends its length on the option **not** taken.

A `UserPromptSubmit` hook (`scripts/writing-standards-hook.sh`, wired in `.claude/settings.json`) restates this at the start of every turn, since the rule has to be in mind *before* anything is composed. Keep the script's short form in sync with this section.

## Testing conventions (all three languages)

These apply to `src/`, `sidecar/`, and `src-tauri/` alike; each area's `CLAUDE.md` adds its own tooling on top.

**Every test file sits beside the file it covers.** `foo_test.go` / `foo.test.ts` tests `foo.go` / `foo.ts`. A test file with no counterpart means the unit under test is buried in a larger file — split the *implementation* out to match the test's name rather than renaming the test to match the monolith. The one exception is a file that exists solely to hold shared fixtures (`testutil_test.go`), which tests nothing.

**No magic sleeps — never block on the wall clock to let something happen.** A real delay is flaky under load and slow in aggregate, and it fails at the *end* of the wait rather than the moment the invariant breaks. Per language:

- **TypeScript** — `vi.useFakeTimers()` + `advanceTimersByTimeAsync`, `await waitFor(...)`, or a `flush()`/`act` helper. Never a bare `setTimeout` wait.
- **Go** — wait on a channel, not a duration: `testutil.Probe`/`Signal`, `testutil.Recv`/`Wait`, or `require.Eventually`. Never `time.Sleep`.
- **Rust** — `#[tokio::test(start_paused = true)]`, which auto-advances virtual time between parked timers. `src-tauri/src/services/sidecar/ipc.rs` (`connect_retries_until_endpoint_appears`) is the reference.

**Pace a timing-dependent unit by parameter, not by constant.** A production constant a test must outwait (a retry cadence, a poll interval) becomes an argument, and production passes the constant — see `prefsync`'s `withBackoff`. Shrinking it in a test is then free, and the test never encodes the production number.

Two shapes are *not* magic sleeps, and both must say so in a comment:

- **A negative assertion** ("must NOT happen") has no event to wait for, so it needs a bounded window. Write it as a `select` on the tripwire channel versus `time.After` — it fails the instant the thing happens rather than at the end — and size the window as a multiple of the (shrunk) cadence, never a bare guess. Sample the baseline off the same channel, not by polling a counter, or the baseline read races the bug and swallows it.
- **Latency injected into the code under test** — a fake that takes a moment on purpose so a join-vs-no-join race has a determinate answer. The test's own assertion path stays immediate.

## Frontend (`src/`)

React 19 + TanStack Router + [urql] (GraphQL) + Vite + Tailwind v4. UI primitives from `@kubetail/ui`. Package manager is **pnpm**. Path alias: **`@/*` → `src/*`**.

### Routing & layout

- Entry: `src/main.tsx` → `RouterProvider`. Routes in `src/routes/` (code-based, wired in `src/routeTree.ts`): `__root.tsx` mounts the provider stack; `_app.tsx` is a pathless layout route rendering `AppLayout` (the sidebar shell); `chat.tsx` and `dashboard.tsx` nest under it as peer routes. `index.tsx` (`/`) just redirects to `DEFAULT_ROUTE`. The Chat/Dashboard switch is the sidebar's `ModeNav` (router `Link`s), so each mode is a real, deep-linkable route.
- **Layouts** live in `src/layouts/` — presentational shells rendering an `Outlet`. Secondary windows (log tail, exec) will get their own sidebar-less layout under a separate pathless route; a Tauri window opens at that route's path.
- App-wide concerns live in `src/lib/` (auth, error bus, sync-status, kube-config, ready-gate, `platform.ts`, `host-file.ts`, `theme.tsx`); reusable UI in `src/components/widgets/`.

### Window-scoped state = URL search params

`kubeContext` (on `_app`, kept across mode switches via `retainSearchParams`) and `resource` (on `/dashboard`) are search params — per-window, deep-linkable, history-pushing. → [ADR: URL params as window state](docs/adr/2026-08-09-url-params-as-window-state.md). Accessors: `useActiveKubeContext` (`src/lib/active-kube-context.tsx`; falls back to the kubeconfig current context; `setContext` writes the param — a frontend view-scope only, never rewrites the kubeconfig) and `resolveDashboardResource`. Links writing one param must spread the previous search (`(prev) => ({ ...prev, resource })`).

The **context bar** (`components/widgets/kube-context-bar.tsx`, mounted in `AppLayout` above the `Outlet`) holds the picker (`kube-context-picker.tsx`), the context's cluster/user metadata, and the back/forward **`HistoryNav`** (`history-nav.tsx`), which walks router history via `__TSR_index` read through `useSyncExternalStore`.

### Dashboard resource nav

The curated base tree is `DASHBOARD_NAV` (`src/lib/dashboard-resources.ts`); `buildDashboardNav(serverKinds)` merges in the active cache's discovered kinds (bucketed by API group; unmapped groups — including core — deferred to Custom Resources work). Disclosure style is derived from shape (curated `children` vs discovered `moreChildren`); counts join by API group + plural via `CURATED_LEAF_API_GROUP`. `DashboardResource` is an **open string**, validated leniently. → [ADR: dashboard nav merge](docs/adr/2026-08-09-dashboard-nav-merge.md).

Live data: `useClusterDataKinds` (`src/lib/cluster-data-kinds.tsx`) consumes the `clusterDataKindsWatch(id, cacheID)` subscription; `useDashboardNav` builds the tree over it. All three cluster-data hooks resolve the active cluster/cache through the shared `useActiveCluster()` (`src/lib/active-cluster.tsx`) and reduce through the shared **`useCacheDeltaWatch`** (`src/lib/graphql/use-cache-delta-watch.ts`), which owns the id-keyed fold, the **cache/kind provenance guard** (rejects retained rows and stragglers from a superseded cache — don't bypass it; → delta-watch ADR), and `watchPhase`. `AppLayout` mounts `DashboardResourceNav` (`components/widgets/dashboard-resource-nav.tsx`) only while `/dashboard` matches (`useMatchRoute`); `routes/dashboard.tsx` reads `resource` back and picks a panel three ways: `events` → `EventsTable` (`events-table.tsx`, fed by `useClusterDataEvents`), a resolved discovered kind → the generic `ObjectsTable` (`objects-table.tsx`, fed by `useClusterDataObjects(kind)` — keyed by cache **plus** `apiVersion`+`resource`), else a placeholder. `ClusterDataObject.rawJSON` is the full native body (typed `unknown`; kind-specific columns are a client-side registry — the remaining step, see `TODO.md`). Both tables gate on the same `active`/`phase` split (no active cache → "not synced", `connecting` → spinner, then `live` with no rows → "No …").

### Theming & window frame

Two axes in `src/lib/theme.tsx`: **color scheme** (`system`/`light`/`dark` → `.dark` class on `<html>`; Tailwind `dark:` variant) and **theme/skin** (reserved, not built). The preference persists in the host's `host.json` — **no localStorage**; `src/lib/host-file.ts` owns the read/update/subscribe protocol. Boot is synchronous via `window.__KSTACK_HOST__` + the inline script in `index.html` (hand-mirrors `resolveColorScheme` — keep in sync); changes write through and broadcast to all windows. → [ADR: host.json settings](docs/adr/2026-08-09-host-json-settings.md), [ADR: first-paint theming](docs/adr/2026-08-09-first-paint-theming.md). The Settings dialog hosts the picker (`settings-dialog.tsx`, via `openDialog('settings')`).

Window frame is per-platform (macOS native / Windows frameless-opaque / Linux frameless-transparent): `WindowFrame` (`window-frame.tsx`, wraps the app at `__root.tsx`) paints Linux's border/shadow and is a passthrough elsewhere; `WindowResizeHandles` is its **sibling**, never a child (the frame's `contain: paint` would clip it). `main.tsx` tags `<html class="frameless">` on Linux only. **Full-height screens use `min-h-[var(--app-min-h)]`, never `min-h-svh`** — the frame insets the app on Linux (`app-sidebar.tsx` overrides the library's hardcoded value via `cn`). Maximized Linux collapses to full-bleed (`useWindowMaximized`). → [ADR: per-platform window chrome](docs/adr/2026-08-09-per-platform-window-chrome.md).

Off macOS there's no native menu bar: `AppMenu` (`app-menu.tsx`) is the hamburger in the custom title bar (`app-sidebar.tsx`), owns `Ctrl/Cmd+N`/`Ctrl/Cmd+Q`, and invokes the `new_window`/`quit` commands; `WindowControls` (`window-controls.tsx`) supplies caption buttons.

### GraphQL — never talks HTTP directly

The webview has **no network access**. urql routes everything through Tauri IPC (`src/lib/graphql/`): queries/mutations via `invoke('graphql_query')` (`invoke-fetch.ts`), subscriptions via `invoke('graphql_subscribe'/'graphql_unsubscribe')` over a Tauri `Channel` (`subscribe-exchange.ts`, own capped-backoff reconnect). → [ADR: GraphQL over Tauri IPC](docs/adr/2026-08-09-graphql-over-tauri-ipc.md). Read `src/lib/graphql/client.ts` and its tests before touching transport/retry logic.

A `next` frame marked **`extensions.watchFailed`** is the sidecar naming the reason a watch died (its `WatchFailureExtension`). `subscribe-exchange` routes it down the ordinary drop path — report, hold last-known data, reconnect — and never to the urql sink: urql merges each frame into the previous result, so an errors-only frame pushed to the sink would re-deliver the last frame's data and fold it twice. **Key on the marker, never on shape** — a non-null field erroring nulls its parent, so a live frame with a field error looks identical, and tearing the subscription down for one would turn a bad field into a reconnect loop. → [ADR: watch-failure reporting](docs/adr/2026-08-14-watch-failure-reporting.md).

Connection lifecycle is published out of band on the per-operation **transport-status registry** (`transport-status.ts`): `connected` + a process-wide monotonic `generation`, bumped on the host's **`open` frame** — deliberately *not* on the `graphql_subscribe` ack (the host acks before it dials; an ack-keyed reset would clear last-known data on every failed retry during an outage). Consumers use **`useWatchSubscription`** (`use-watch-subscription.ts`), never raw `useSubscription`; it generation-gates its accumulator so state from a previous connection never survives a reconnect. `watchPhase(synced, connected)` turns the delta watches' `Bookmark` into the three states a view renders — `connecting` (no complete snapshot yet, whatever the transport is doing), `live`, `reconnecting`. → [ADR: transport-status generation](docs/adr/2026-08-09-transport-status-generation.md).

### Codegen — generated types, don't hand-write

`pnpm codegen` (`codegen.ts`) reads the **sidecar's** schema (`sidecar/graph/schema.graphqls` — single source of truth) and emits `src/gql/`.

- Write operations with the **`graphql()` tagged template from `@/gql`**.
- **`src/gql/` is generated — never edit it.** After changing a query or the sidecar schema, run `pnpm codegen` (or `pnpm codegen:watch`).

### Tests

Vitest + `@testing-library/react` (jsdom), co-located (`*.test.ts[x]`). Mock the Tauri bridge with **`mockTauriCore()` from `@/test-utils`** (+ `vi.mock('@tauri-apps/api/core', …)` and dynamic import of the module under test). For GraphQL-driven components, push frames via `liveChannel().onmessage!(...)` — see `src/components/widgets/sync-health-badge.test.tsx`.

- The repo-wide rules above (co-located test files, no magic sleeps) apply here — `vi.useFakeTimers()` + `advanceTimersByTimeAsync`, `await waitFor(...)`, or a `flush()`/`act` helper, never a bare `setTimeout` wait.
- Run: `pnpm test --run` (or `make test-js`). Watch: `pnpm test`.

### Lint / build

- `pnpm lint` (ESLint: airbnb-extended + prettier) → `make lint-js`. Run before committing.
- `pnpm build` = `tsc -b && vite build`. Dev: `pnpm dev` (webview) or `pnpm tauri dev` (full app).

[urql]: https://commerce.nearform.com/open-source/urql/
