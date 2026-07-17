# TODO

Pending work across the three parts of the app. Grouped by area; detailed items keep their acceptance notes inline.

## Auth

- **OAuth access-token refresh — background/proactive half.** On-demand refresh is done (`sidecar/internal/auth/grant.go` refreshes a lazily-expired token using the stored refresh token). What remains: a proactive/background refresh before expiry rather than only refreshing when a consumer hits an already-expired token.
- **SSO failure didn't retry.** The async login tail (wait-for-redirect → exchange → verify → persist) is fire-and-forget; a tail failure is only logged and leaves the session signed-out (a known v1 limitation), with no retry. The user must manually re-initiate login.
- **Check RBAC permissions?** The `ClusterPermissions`/`ResourceRule`/`NonResourceRule` types and schema exist, but the `Permissions` resolver is a stub that returns `not implemented: permissions`. Implement it via a `SelfSubjectRulesReview`. (Not to be confused with the `SelfSubjectReview` *authentication* probe in `core_controller.go`, which is already implemented.)

## Sidecar (Go)

- **Detect new GVRs** — discovery is one-shot per engine run (`internal/cluster/cache/engine`): a CRD/GVR installed after the engine reaches `Watching` is only picked up when the engine restarts (poke resync, credential rotation, or a run exiting on error). Add continuous/dynamic GVR detection while a run is live.

- **Migrate `poke` and `cloud` to `Start(ctx) (stop func(context.Context) error, err error)`.** `cluster` is already migrated (`internal/cluster/service.go` returns a stop func); `poke` (`internal/poke/poke.go`) and `cloud` (`internal/cloud/cloud.go`) still use a `Start(ctx)/Close()` pair. Migrate them to return a stop func: `ctx` scopes to initialization only; the returned `stop` accepts a drain-deadline context and blocks until in-flight work is done. Cleaner than `Start/Stop`: startup vs. shutdown contexts are unambiguous, the shape doesn't imply restartability, and the drain timeout lives naturally on `stop`.

- **Cluster-schema namespace scheme — the deferred hoist.** The naming scheme is otherwise complete: type prefixes announce **where the data lives** — `Cluster*` (bare) = the beehive **Cluster** kind (registry record + status, including connection/probe observations); `ClusterCache*` = the beehive **ClusterCache** kind plus that cache's on-disk stats; `ClusterData*` = the per-cluster **data cache file** contents; unprefixed = a type that spans more than one kind (`Condition`, `Event`, `Schedule`, `ChangeType`, RBAC `ResourceRule`/`NonResourceRule`). The renames are **done** (`ClusterKind` → `ClusterDataKind`, `ClusterCondition` → `Condition`, `CachedResource` → `ClusterCacheResourceStats`). **What remains (deliberately deferred):** `Condition`/`Event`/`Schedule` are still bound to `internal/cluster` Go types because `cluster` is their only consumer today. **Trigger to hoist:** the first non-cluster kind/subsystem that needs conditions/events/schedules — at that point move all three into a shared leaf package (e.g. `internal/apimeta`/`internal/k8smeta`), leaving `cluster` its `ConditionType` constants (`Connected`/`Healthy`/`Synced`). Hoisting earlier would be a one-importer abstraction.

- **`ClusterData*` instances** (deferred to the Custom Resources / dashboard-objects work): a generic `ClusterDataObject` envelope (GVK + metadata + extracted `labels`/`ownerRefs` columns + full `object: JSON` behind a resolver), keyed per-cache like `clusterDataKinds`, with `clusterDataObjects`/`clusterDataObjectsWatch` entrypoints — **not** per-kind `Pod`/`Deployment` types (CRDs are unbounded; the cache is universal). "Resource" stays reserved for the actual k8s resource *collection*, never a stats-row or an instance — hence `ClusterDataObject`, not `ClusterDataResource`.

## Frontend (webview)

- **React compiler — adopt the ESLint plugin.** The Vite/babel transform is already configured (`vite.config.ts`, `babel-plugin-react-compiler`). What remains: add `eslint-plugin-react-compiler` to the lint config.

- **Startup URLs don't reference the active kube context.** A fresh window lands on a bare `/chat` (`index.tsx` redirects `/` → `DEFAULT_ROUTE` with no search); `useActiveKubeContext` resolves the context by *falling back* to `kubeConfig.currentContext` but only *writes* `?kubeContext=` on an explicit pick. Consequence: the landing URL isn't self-describing or deep-linkable until the user interacts, and two windows on different default-resolved contexts look identical in the URL. Fix: seed `kubeContext` from the resolved current-context at boot (e.g. `index.tsx` redirect or an `_app` `beforeLoad`). Catch: at `beforeLoad` the clusters stream may not have delivered its first frame, so current-context isn't known synchronously — either accept a sometimes-omitted param or resolve+write once after the first frame lands.

- **Subscriptions leak stale state across a transport reconnect** (own PR — fix once, uniformly). `subscribe-exchange.ts` reconnects a dropped subscription stream *internally* — it never emits `complete` to urql, so urql keeps the same operation/source and `useSubscription`'s accumulator (`prev`) is never reset across a reconnect. On reconnect the sidecar replays a fresh full snapshot as an `Added` burst, but any object **deleted during the outage** is never replayed and nothing removes it from `prev`, so it lingers indefinitely. Affects every delta watch sharing `applyChange` — `clusterDataKindsWatch` (`src/lib/dashboard-nav.tsx`, whose only reset is on a `cacheID` mismatch, not a reconnect) and both `clustersWatch`/`clusterCachesWatch` (`src/lib/clusters.tsx`). Acceptance criteria:
  - Reset each delta accumulator at every transport reconnection; merely opening another SSE operation is insufficient because urql preserves the existing reducer state.
  - Reset correctly when the replayed snapshot is empty and therefore produces no `Added` frame; the reset cannot depend on seeing the snapshot's first object.
  - Apply the mechanism uniformly to `clusterDataKindsWatch`, `clustersWatch`, and `clusterCachesWatch` rather than adding per-screen workarounds.
  - Add coverage that removes a kind, cluster, and cache while disconnected, reconnects, and verifies that each absent object is removed from accumulated UI state (including the fully-empty snapshot case).

- **`src/lib/dashboard-nav.tsx` has type errors** (confirmed live via `pnpm exec tsc -b`) against the generated `ClusterDataKindsWatchSubscription` type: the reducer at `dashboard-nav.tsx:111` returns `Catalog | undefined` where a `SubscriptionHandler` requires `Catalog`; because that handler doesn't type-match, `data` falls back to the raw subscription type, so line 130 reads `.cacheID`/`.kinds` off the top level when the generated shape exposes those under `clusterDataKindsWatch`. Fix the reducer's return type (non-`undefined` seed) and the accessors to match the generated shape.

## Tests

- **Extract `channelFor`/`pushClusters` cluster-delta test helpers into `src/test-utils.tsx`** — copied verbatim across 6 test files (`lib/clusters.test.tsx`, `components/widgets/cluster-sync-panel.test.tsx`, `lib/kube-config.test.tsx`, `lib/active-kube-context.test.tsx`, `components/widgets/kube-context-picker.test.tsx`, `components/widgets/kube-context-bar.test.tsx`). `channelFor` closes over `invokeMock`/`channels`, so expose it from `mockTauriCore()`'s return.

## Done

- ~~OAuth sign-in/logout~~ (token storage in OS keyring, multi-window sync, startup restore); on-demand access-token refresh.
- ~~Sidecar lifecycle~~ — host spawns, monitors (READY gate), and gracefully shuts down the sidecar (`src-tauri/src/services/sidecar/`).
- ~~Rust → sidecar GraphQL bridge~~ — `graphql_query`/`graphql_subscribe`/`graphql_unsubscribe` commands forward query/mutation as HTTP POST and each subscription as its own SSE stream.
- ~~Waker + Rust → sidecar waker~~ — OS wake/network-return supervisor (`src-tauri/src/wake/`) pokes the sidecar's `poke` service over gRPC to trigger a resync.
- ~~Rust → sidecar credentials push~~ — obsolete: auth moved entirely into the sidecar (refresh token in the OS keyring, login/logout over gRPC), so there's no host→sidecar credentials channel and no `/control/credentials` endpoint.
- ~~Cluster-schema renames~~ — `ClusterKind` → `ClusterDataKind`; `ClusterCondition` → `Condition`; `CachedResource` → `ClusterCacheResourceStats`.
- ~~`cluster` service lifecycle migration~~ — `internal/cluster/service.go` `Start` returns a stop func.
- ~~React compiler build transform~~ — wired into Vite via `babel-plugin-react-compiler` (ESLint plugin still pending, above).
