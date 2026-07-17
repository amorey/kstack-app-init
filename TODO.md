# TODO

- Sidecar lifecycle
- Rust->Sidecar graphql bridge
- ~~Oauth~~ (sign-in/logout, token storage, multi-window sync, startup restore)
- Rust->Sidecar credentials push (Oauth access token -> /control/credentials)
- Oauth access-token refresh (background + on-demand; refresh token already stored)
- Waker
- Rust->Sidecar waker

- React compiler

- Check RBAC permissions?
- SSO failure didn't retry
- detect new GVRs

- Extract `channelFor`/`pushClusters` cluster-delta test helpers into `src/test-utils.tsx` — copied verbatim across 5 test files (`clusters.test.tsx`, `cluster-sync-panel.test.tsx`, `kube-config.test.tsx`, `active-context.test.tsx`, `kube-context-picker.test.tsx`). `channelFor` closes over `invokeMock`/`channels`, so expose it from `mockTauriCore()`'s return.

- Subscriptions leak stale state across a transport reconnect. `subscribe-exchange.ts` reconnects a dropped subscription stream *internally* — it never emits `complete` to urql, so urql keeps the same operation/source and `useSubscription`'s accumulator (`prev`) is never reset across a reconnect. On reconnect the sidecar replays a fresh full snapshot as an `Added` burst, but any object **deleted during the outage** is never replayed and nothing removes it from `prev`, so it lingers indefinitely. Affects every delta watch sharing `applyChange` — `clusterDataKindsWatch` (`src/lib/dashboard-nav.tsx`) and both `clustersWatch`/`clusterCachesWatch` (`src/lib/clusters.tsx`) — where a reconnect can leave a deleted kind/cluster/cache stuck in the UI. Should be fixed once, uniformly, for all subscriptions (its own PR). Acceptance criteria:
  - Reset each delta accumulator at every transport reconnection; merely opening another SSE operation is insufficient because urql preserves the existing reducer state.
  - Reset correctly when the replayed snapshot is empty and therefore produces no `Added` frame; the reset cannot depend on seeing the snapshot's first object.
  - Apply the mechanism uniformly to `clusterDataKindsWatch`, `clustersWatch`, and `clusterCachesWatch` rather than adding per-screen workarounds.
  - Add coverage that removes a kind, cluster, and cache while disconnected, reconnects, and verifies that each absent object is removed from accumulated UI state (including the fully-empty snapshot case).

- Startup URLs don't reference the active kube context. A fresh window lands on a bare `/chat` (`index.tsx` redirects `/` → `DEFAULT_ROUTE` with no search); `useActiveContext` resolves the context by *falling back* to `kubeConfig.currentContext` but only *writes* `?kubeContext=` on an explicit pick. Consequence: the landing URL isn't self-describing or deep-linkable until the user interacts, and two windows on different default-resolved contexts look identical in the URL. Fix: seed `kubeContext` from the resolved current-context at boot (e.g. `index.tsx` redirect or an `_app` `beforeLoad`). Catch: at `beforeLoad` the clusters stream may not have delivered its first frame, so current-context isn't known synchronously — either accept a sometimes-omitted param or resolve+write once after the first frame lands.
