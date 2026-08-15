# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (parent gone).

The data dir (`--data-dir` / `KSTACK_DATA_DIR`) is **required** — `app.New` errors when empty; tests supply `t.TempDir()`. `<data-dir>/app.db` is owned by `internal/appdb` (one migration sequence; add app-level tables as numbered migrations in `appdb/migrations/`, never a second embed against the same file).

## Layout

Mirrors the kubetail layout: `main.go` is lifecycle only, `internal/app` is the composition root + routing, GraphQL lives in `graph/`. There is no `server` package.

- `main.go` — parse flags, bind socket, build `*app.App`, serve, drive graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close`).
- `internal/app/` — **composition root**: builds `poke.Service`, `clustersvc.New(...)`, `auth.Service`, `cloud.Service`; wires `graph.NewServer` + `grpcserver.NewServer`; multiplexes both onto one h2c handler (dispatcher keyed on `grpcserver.IsGRPCRequest`). `App.Start(ctx)` returns a drain-func; the stop chain is `clusterSvcStop → cloudSvcStop → pokeSvcStop` — poke's hub closes **last**, after its subscribers drain (the left-to-right arg evaluation in the `errors.Join` enforces it).
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go` (gqlgen handler, bearer-token plumbing, SSE shutdown lifecycle). Resolver deps must be non-nil — tests wire fakes; degraded behavior lives inside the services, not behind nil-guards.
- `grpc/` — gRPC surface: `AuthService` (`StartLogin`/`Logout` unary; `AuthStateWatch` server-streaming, joins the drain WaitGroup) and `PokeService` (unary `Poke` → `poke.Poke(SourceHost)`). Committed protoc output in `grpc/authpb/`, `grpc/pokepb/`; regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here — it *is* the definition of a gRPC request.
- `internal/` — `ipc` (per-OS user-only endpoint), `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `drain`, `testutil` (test-only helpers, imported by no production code), plus the subsystems below.

## gRPC + GraphQL over one socket (h2c)

`internal/app` owns the topology (that two surfaces share one socket); `grpc/` owns the predicate. HTTP/1.1 GraphQL POST + SSE are untouched. An idle `AuthStateWatch` survives the 60s `IdleTimeout` via gRPC keepalive pings. The cluster surface is **GraphQL-only**. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

**Shutdown order** (from `main.go`): `app.NotifyShutdown()` (gRPC streams end on their serving context; each SSE request's context is cancelled per-request) → `srv.Shutdown` → `app.DrainWithContext` (waits both sub-servers' WaitGroups — essential for hijacked h2c gRPC streams `srv.Shutdown` can't see) → `stop(ctx)` → `app.Close()` (`grpcServer.Stop()`, `clusterSvc.Close()`). Traps: grpc-go's `GracefulStop` **panics** on the h2c path — `Stop` only runs after the drain; never cancel via `http.Server.BaseContext` — it would tear down the shared h2c connection carrying gRPC mid-stream.

## Cluster subsystem (`internal/clustersvc`)

**Stripped to a shell, pending a rebuild.** The package is three files — `service.go` (the
`Service` interface, its five family sub-interfaces, the accessors, and the stateless unexported
`service` whose every method but `Start`/`Close` panics), `stream.go` (`Stream[T]`, which appears in
those signatures), and
`types/` (the record structs the GraphQL schema binds by name). Nothing else survives: there is no
beehive control plane, no controllers, no kubeconfig watcher, no connection manager, and no on-disk
cache.

**The point of the shell is that the surfaces above it did not move.** `graph/`, `grpc/`, and
`internal/app` compile and are wired exactly as before; `clustersvc.New(dataDir, kubeconfigPath,
pokeSvc)`, `Start`, and `Close` keep their signatures so reconnecting a real implementation is not a
change to the composition root. `New` returns an empty `service` as a `Service`, `Start` returns a
no-op stop func, and every API call panics. **The sidecar therefore serves no cluster data at all** — the app starts,
the socket comes up, auth and cloud settings work, and any cluster query or subscription panics.

The five families are `Clusters()`, `Caches()`, `CachedCatalogs()`, `CachedResources()`, and
`CachedData()`. **The `Cached*` prefix marks the cache subtree** — what a `ClusterCache` catalogs,
the per-kind records under that catalog, and the mirrored content itself — so the grouping is visible
in the accessor list rather than something you have to know. Keep it when adding a family there.

Rebuilding a family means writing its implementation in its own file (`clusters.go`, `caches.go`,
`cachedcatalogs.go`, `cachedresources.go`, `cacheddata.go` — one per family, each with a test beside
it) and deleting its stub from `service.go`. Keep the method naming rule when you do: **VerbNoun with the noun elided when
it equals the family's subject**, so `Caches().Watch()` watches caches and `Caches().GetStats()` reads
one cache's stats. **A family owns a read only when the read differs per record type.**
`RetryConnection`/`GetConnection` stay top-level (they answer about a connection, not a record), and
so do `ListEvents`/`WatchEvents`: an event carries no kind, every id is the same `ObjectID`, and only
three of the five families have a timeline at all — scoping them would be three copies of one method
plus an unanswerable question about the other two. Every family is asserted separately
(`var _ Caches = cachesAPI{}`), in the resolver tests' fake too: satisfying `Service` only proves the
accessors exist.
→ [ADR: record-family sub-APIs](../docs/adr/2026-08-10-cluster-service-sub-apis.md).

**A watch whose source can die returns `*Stream[T]`** (`Frames` + `Err()`), not a bare channel:
`Frames` closes on every exit, so `Err` is the only thing separating a failure from an ordinary
teardown, and the reason is set *before* `Frames` closes — which is what makes "Frames closed" a safe
cue to read `Err`. `NewStream` is exported so a fake implementing these interfaces can build one. The
rule is the source, not the shape: anything reading a fallible upstream returns a `Stream`, gauges
included; a watch that cannot fail terminally may stay a plain channel.

### Types

`types` is a leaf and the one part of the subsystem that is fully intact — the four kinds' spec/
status/record structs, identity, conditions, change types, and the cached-data records, split one
file per noun to match the schema's sections. **The schema binds these by name, which is why they
survive a teardown that removed everything that produced them.** Placement rule when adding
something: types the schema binds and the identity/condition vocabulary go to `types`; within it, a
type follows the schema section it binds to, and anything kind-agnostic goes to `shared.go`.
Unexported helpers live with their only consumer (a callee follows its caller — `LiveCondition` needs
`TruncateMessage`, so both are in `types/shared.go`).

- `ClusterID` **is** the beehive ObjectID — opaque, source-agnostic, stable for the record's life;
  never the remote UID or the context name. One shared GraphQL `ObjectID` scalar (decimal string)
  carries every kind's id; frontend codegen maps it to `string`.
- `ClusterSpec` is user/API-owned (`Name`, `Enabled`, `SyncEnabled`, `Source` — a discriminated
  union, kubeconfig today); the matching *observation* belongs in `ClusterStatus.Source`, never spec.
  The spec carries **no trigger/counter fields** — retries and resyncs ride out-of-band buses.
- `ClusterCache.Spec.ServerUID` names the physical identity a cache mirrors. **Active-ness is
  deliberately not a field** — it is the live join against the cluster's `status.server.uid`
  (`types.CacheIsActive`). → [ADR: delta watches](../docs/adr/2026-08-09-delta-watch-protocol.md).
- Every condition is a **liveness** condition (`types.LiveCondition` is the only constructor);
  beehive serves a previous process's write as `Unknown`+`Unconfirmed` until re-confirmed.
  **`Unconfirmed` is load-bearing on the wire**: the surviving reason/stamps describe *last-known*
  state, and a consumer must not render a pre-restart reason as current.
  → [ADR: liveness conditions](../docs/adr/2026-08-09-liveness-conditions.md).
- `types.Condition` aliases `beehive.Condition`, so `types` still depends on beehive even though
  nothing here runs it. That is the seam a rebuild on a different control plane would cut.

### GraphQL surface (cluster)

The schema **is** the Go shape — every GraphQL type binds 1:1 by name to its `internal/clustersvc/types` type in `gqlgen.yml`; no projection layer. Resolvers are one-liners delegating to a family on `r.ClusterSvc` (e.g. `r.ClusterSvc.CachedData().WatchObjects`; the field is named `ClusterSvc` to avoid shadowing the generated `Clusters` method). The whole surface below is intact in the schema and in the resolvers — but **every field reaches the stubbed boundary and panics**, so this section is the contract the rebuild must satisfy, not a description of what answers today. Key entry points:

- Delta watches: `clustersWatch`/`clusterCachesWatch` (independent; joined client-side), `clusterCachedCatalogsWatch` (unscoped, one per cache), `clusterCachedResourcesWatch(cacheID)` (cache-scoped — ~100 records; the always-mounted registry must not carry it), `clusterCacheSyncHealthWatch` (the fold — a latest-value gauge, **not** a delta watch, so no `Bookmark` rides it).
- **Every delta watch closes its snapshot with one `FrameBookmark`**, carrying a nil entity — which is why the seven `*WatchFrame` types hold their entity by pointer and the schema types it nullable. Both are named for the frame, not the change: a frame is a change **or** the bookmark, so `ClusterChange`/`ChangeType` would each have been a lie for one value of the enum. A record watch sends it between the snapshot and the first live change, and carries a failure reason out through `Stream.Err()`. A per-cache watch must send it after the first successful read *or* the first bind that finds no open cache (an unopened cache is definitively empty, not pending), and anything that holds frames back must queue the bookmark behind them — it must not claim a snapshot is complete over frames still undecided. → [ADR: delta-watch protocol](../docs/adr/2026-08-09-delta-watch-protocol.md).
- Cache-data watches (all keyed by cluster id + cache id; frames carry `cacheID` provenance — objects additionally `apiVersion`/`resource` — so the client rejects stale frames after a swap): `clusterCachedDataKindsWatch` (kind catalog + counts; subscribes to **both** brokers via `catalogSubscribe`, since Event counts come from event triggers), `clusterCachedDataEventsWatch` (newest window, `Deleted` when aging out), `clusterCachedDataObjectsWatch` (per-kind rows incl. `rawJSON`; resource-keyed broker subscription). Unopened cache → the `Bookmark` alone.
- Point reads hang off the record that owns them, resolved on selection: every event timeline is an `events(category, limit)` field (`Cluster.events`, `ClusterCache.events`, `ClusterCachedResource.events`), the discovered kind catalog is `ClusterCache.kinds` (no arguments — both ids it reads with come off the record), and `Cluster.caches` / `ClusterCache.cachedResources` walk the owner chain down (`Caches().List`, `CachedResources().List`). So there are no root `cluster*Events` or `clusterCachedDataKinds` fields. The lookups `clusterCache(id)` and `clusterCachedResource(id)` (over `Caches().Get`/`CachedResources().Get`) address a record by **its own** id, which a caller holding one from a watch frame uses directly.
- **Every noun has the same pair at root: `<noun>(id)` and `<nouns>(<parent>ID)`** — `cluster`/`clusters`, `clusterCache`/`clusterCaches(clusterID)`, `clusterCachedResource`/`clusterCachedResources(cacheID)`. The plural's scope argument is **optional**: omitted it reads the whole fleet, passed it returns exactly what the nested field serves (`Cluster.caches`, `ClusterCache.cachedResources`). One boundary method backs both — `Caches().List`/`CachedResources().List` take a nilable scope, nil meaning fleet-wide. Keep that shape when adding a noun.
- **The query path skips `ClusterCachedCatalog`.** `ClusterCache.cachedResources` is keyed by the cache and resolves the catalog itself (`CachedResources().List`, like `CachedResources().Watch`): exactly one catalog exists per cache and its name is derived from the cache id (`types.ClusterCachedCatalogName`), so it is an implementation detail, not a branch to navigate. The catalog's own state still streams on `clusterCachedCatalogsWatch`.
- **`Cluster.caches` is the set, never "the" cache.** Activeness is the live join against the parent's `status.server.uid` (`types.CacheIsActive`), and a probe rewrites that UID with no cache event — so a consumer that must follow it over time reads `clustersWatch` + `clusterCachesWatch` and joins them, rather than reading the query field. → [ADR: delta watches](../docs/adr/2026-08-09-delta-watch-protocol.md). The live counterparts `eventsWatch` and `clusterScheduleWatch` (countdown + `probing`) stay flat at root: only the point reads nest.
- Mutations: `clusterEnabledSet`, `clusterSyncEnabledSet`, `clusterConnectionRetry` (returns immediately; outcome lands on conditions), `clusterCacheClear` (delete files then **bounce that cache's workers** — nothing else would rebuild them; they cold-sync, the cookie died with the file), `clusterDelete` (GC cascades; a still-present context is re-imported under a fresh id).
- `ClusterCachedDataEvent.type` is a plain `String!` (k8s doesn't constrain it) and timestamps are nullable `Time` via `nilIfZeroTime` (`graph/util.go`) — the record keeps value `time.Time` for comparability.
- **A watch that dies reports why** (`graph/watch_failure.go`). gqlgen builds each subscription frame as data alone and stops the instant the resolver's channel closes, so a failed watch is otherwise byte-identical to a graceful end and the webview reconnects forever with nothing shown. `WatchFailureExtension` bridges that in two halves — the resolver and the frame that would carry the reason never share a response context: `InterceptOperation` hangs a slot on the operation ctx (gqlgen threads it into the resolvers *and* every later frame), `watchStream` files `Stream.Err()` into it as the frames run out, and `InterceptResponse` claims it once the stream is spent, emitting one errors-only `graphql.Response` before the transport completes. Claimed once, so the next poll ends the subscription instead of looping. The reason goes through `AddError`, so the server's error presenter logs it. The client treats that frame as a drop with a reason — reported, last-known data held, reconnect — see the root `CLAUDE.md`. → [ADR: watch-failure reporting](../docs/adr/2026-08-14-watch-failure-reporting.md).

Frontend join: `src/lib/clusters.tsx` (`ClustersProvider`/`useClusters`) reduces the three unscoped streams and joins `activeCache` + `syncHealth` client-side; `cluster-sync-panel.tsx` renders it (per-row detail streams subscribe only while a row is expanded; the sync column reads the rollup's reason, never the cache's coarse `Synced`).

## Auth / identity (`internal/auth`)

Local-first accounts against kstack-cloud's Hydra (`https://oauth.kstack.sh/`). The sidecar owns the whole flow: system browser (auth-code + PKCE, loopback redirect), exchange + verify (go-oidc vs JWKS; identity from the verified ID token), refresh token in the OS keyring (`keyringStore` over `zalando/go-keyring`). Signed-in ⇔ refresh token present; works offline. No gRPC credentials channel. Degrades to signed-out when unconfigured. → [ADR: local-first auth & settings](../docs/adr/2026-08-09-local-first-auth-settings.md).

- Root `package auth` is deliberately flat, organized by file: `auth.go` (the `Service` interface + `New(Config)`, `State`/`TokenSet`, `Token`/`Identity` aliases), `grant.go` (the grant aggregate — token set as source of truth, `Authenticated`/`Identity` **derived**, lazy refresh with burst-dedup, persist-before-cache, latest-value `State` hub), `login.go` (`loginFlow`: synchronous setup — loopback bind + browser open — returning its error to the mutation; the slow tail runs in a bounded detached goroutine, observed via `authStateWatch`), `keyring.go`.
- The one carved-out sub-package is `auth/oauth` — the OAuth2/OIDC protocol layer, a **leaf** (must not import `auth`). It owns `Token`/`Identity` (root re-exports as true aliases to avoid the cycle).
- `Config` carries only production knobs; **test seams are unexported functional options** on an unexported `newWithOptions` builder (white-box tests only). External consumers fake the `Service` interface. No `Start`/`Close` — no long-lived goroutines; `Logout` clears locally first, revokes fire-and-forget (keychain-write failure → error, stays signed in).
- `TokenSource(ctx)` returns an `oauth2.TokenSource` (nil when degraded); it exposes the refresh token — consumers read `AccessToken` only. The GraphQL projection drops tokens (`AuthState { authenticated, identity }`).

## Cloud settings sync (`internal/cloud`) — depends on `auth`

Local-first settings: an edit applies to a local JSON file immediately and queues durably for the cloud. **`cloud` depends on `auth`, never the reverse** — it authenticates from `authSvc.TokenSource` and wakes on `authSvc.Subscribe()`, tracking only the `Authenticated` bit (a token refresh is a non-event). Degrades without a data dir or cloud URL. `Start` is idempotent; its `stop` replaces `Close`.

Sub-packages (leaf-first): `syncstore` (generic `Envelope[T]` + `Store[T]` over `atomicjson`), `prefs` (`Settings` — pointer fields + omitempty so absent ≠ cleared; `Merge`; store deep-copies at boundaries), `mutationqueue` (durable FIFO, survives restart), `api` (GraphQL-over-HTTP client, per-request `TokenSource`), `prefsync` (the reconcile `Engine`: supervised connection with backoff + poke; `Watch` returns data + a buffered terminal-error channel so an errored close keeps escalating backoff; on Live drains the queue, and incoming snapshots get pending patches re-layered via `prefs.Merge`). Test seams: unexported functional options, same pattern as `auth`.

## Resync broadcaster (`internal/poke`)

A cross-subsystem **leaf**: a wall-clock gap detector (15s tick, 2× factor — catches sleep/SIGSTOP/VM pause, works headless) plus a `gochan/broadcast` fan-out hub. `New()` takes no arguments; `Start(ctx)` returns a stop func; `Poke(src)` never blocks. Consumers subscribe directly: the core controller re-probes all clusters, the GVR-sync controller bounces workers in place (cheap cookie resume), `prefsync` reconnects. **A poke is a fan-out, not a cascade** — never routed through spec counters or conditions (a clean resume produces no condition transition, so a cascade would silently skip the stale watches). → [ADR: poke resync fan-out](../docs/adr/2026-08-09-poke-resync-fanout.md).

## GraphQL via gqlgen — the schema is the source of truth

`graph/schema.graphqls` is authoritative — also consumed by the frontend's codegen (`codegen.ts`). One file for the whole surface, sectioned by noun (shared vocabulary, cluster, cluster_cache, cluster_data, chat, cloud account) with `type Query`/`Mutation`/`Subscription` collected at the end. Resolver layout is `follow-schema`, so the one schema file generates the one `schema.resolvers.go`.

After editing:

```sh
cd sidecar && go run github.com/99designs/gqlgen generate
```

This rewrites `graph/generated.go` + `graph/model/models_gen.go` and appends panicking resolver stubs to `graph/schema.resolvers.go` — implement those. **Never hand-edit `generated.go`/`models_gen.go`.** `tools.go` pins gqlgen. Also re-run the frontend `pnpm codegen`. `graph/model/models.go` is a permanent stub keeping the package non-empty across regen.

**Renaming, splitting, or merging a schema file is a two-pass regen**: the first pass copies each resolver body into its new file but leaves the old `*.resolvers.go` in place, so the package has duplicate declarations until you delete it and regenerate. Verify no body came through as `panic("not implemented")` before committing.

## Patterns

- **Resolver deps are always non-nil** — the composition root wires every field; tests use fakes.
- **Pub/sub**: two modules, split on whether delivery is **keyed**. Unkeyed → `github.com/amorey/gochan`: `watch` for latest-value current-state streams (current snapshot on subscribe: auth `State`), `broadcast` for fan-out where subscribers supply their own snapshot (poke). Keyed → `github.com/amorey/gobus`: `watch` for a keyed latest-value bus. Note the two `watch` packages differ on registration — gochan's hub holds a seed and delivers it, gobus's takes the caller's already-read value as a baseline and never delivers it back. Never hand-roll a subscriber map.
- **Subscription resolvers** return a channel emitting the current snapshot first, then deltas (`mapStream` in `graph/util.go`). Honor `ctx.Done()`. A resolver over a `*clustersvc.Stream` goes through **`watchStream`** (`graph/watch_failure.go`), never `ptrStream` — see below.
- **Unexported functional options** for test seams (`auth`/`cloud`/`prefsync`/`poke`): exported `New` takes production knobs only; `newWithOptions(cfg, opts...)` is reachable only from white-box tests.

## Tests & checks

- testify + `httptest`. Resolver-level tests stand up `graph.NewServer(&graph.Resolver{...})` + `POST /graphql`; h2c/lifecycle tests stand up `app.New(...)`. Filesystem via `t.TempDir()`.
- **White-box tests by default** (`package foo`, not `foo_test`) — boundaries are kept by discipline, not the compiler. Escape hatch: external `package foo_test` only when pinning the public contract is the test's purpose — then say so in a comment.
- **No magic sleeps** (repo-wide — see the root `CLAUDE.md` for the rule and its two carve-outs). Block on the actual event, never a fixed `time.Sleep`. A cadence a test would otherwise have to outwait becomes a **parameter** whose production value is the constant — `prefsync`'s `withBackoff` takes `base`/`max` for exactly this — so a test picks its own timescale and never encodes the production number.
- **Waiting on a channel goes through `internal/testutil`**, which owns the one failsafe bound (`testutil.Timeout`): `Wait` (a done/ready channel), `Recv[T]` (the next value), `RecvClosed[T]` (the next receive must be a close), `WaitClosed[T]` (drain until close). Don't hand-roll a `select` with a `time.After` deadline. The exception is a **negative** assertion — "no frame arrived" — which needs its own short window, not the failsafe.
- **A fake that notifies the test uses `testutil.Signal` or `testutil.Probe[T]`**, never a hand-rolled channel. `Signal` (a `gochan/oneshot` pair) is single-shot: `Fire` is idempotent by contract, so a callback that runs many times needs no `sync.Once` and no `select`/`default` guard, and `Fire`'s bool tells the first call from the rest. `Probe[T]` is the repeating case: `Fire` never blocks (a fake that blocks stalls the code under test) and drops the **oldest** on overrun, because the event a test waits for is the newest — which is exactly what a `select`/`default` send throws away. `Await`/`TryAwait`/`Drain`/`Chan` are the read side.
  - The exception is a consumer that does **edge detection**. `internal/cloud`'s auth subscriber swallows its first value as a baseline and acts on the next *change*, so its fake must deliver every state losslessly: a latest-value hub (`gochan/watch`, which is what the real `auth` service publishes through) or a drop-oldest `Probe` can both coalesce the seed with the change and hide the edge. Its `fakeAuth` keeps a plain buffered fan-out, and says so.
- `make test-go` (`cd sidecar && go test ./...`); `make lint-go` (gofmt); `make vet-go` (`go vet`). Run `gofmt -w` before committing.

When you change the sidecar's schema workflow, wiring, or conventions, update this `CLAUDE.md` in the same change.
