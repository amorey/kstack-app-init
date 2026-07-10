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

- Startup URLs don't reference the active kube context. A fresh window lands on a bare `/chat` (`index.tsx` redirects `/` → `DEFAULT_ROUTE` with no search); `useActiveContext` resolves the context by *falling back* to `kubeConfig.currentContext` but only *writes* `?kubeContext=` on an explicit pick. Consequence: the landing URL isn't self-describing or deep-linkable until the user interacts, and two windows on different default-resolved contexts look identical in the URL. Fix: seed `kubeContext` from the resolved current-context at boot (e.g. `index.tsx` redirect or an `_app` `beforeLoad`). Catch: at `beforeLoad` the clusters stream may not have delivered its first frame, so current-context isn't known synchronously — either accept a sometimes-omitted param or resolve+write once after the first frame lands.
