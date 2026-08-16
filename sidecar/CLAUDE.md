# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (parent gone).

The data dir (`--data-dir` / `KSTACK_DATA_DIR`) is **required** — `app.New` errors when empty; tests supply `t.TempDir()`. `<data-dir>/app.db` is owned by `internal/appdb` (one migration sequence; add app-level tables as numbered migrations in `appdb/migrations/`, never a second embed against the same file).

## Layout

Mirrors the kubetail layout: `main.go` is lifecycle only, `internal/app` is the composition root + routing, GraphQL lives in `graph/`. There is no `server` package.

- `main.go` — parse flags, bind socket, build `*app.App`, serve, drive graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close`).
- `internal/app/` — **composition root**: builds `poke.Service`, `kubeconfig.Service`, `clustersvc.New(...)`, `auth.Service`, `cloud.Service`; wires `graph.NewServer` + `grpcserver.NewServer`; multiplexes both onto one h2c handler (dispatcher keyed on `grpcserver.IsGRPCRequest`). `App.Start`/`App.Close` compose `App.parts` through `lifecycle.StartAll`/`CloseAll`: the slice is start order (poke → kubeconfig → cluster → cloud), and stop and close reverse it, so poke's hub closes **last**, after its subscribers drain. Poke and cloud enter the slice as `lifecycle.StartFunc`. The two transports stay out of the slice — they shut down through `NotifyShutdown`/`DrainWithContext`, and `grpcServer.Stop()` runs first in `Close`.
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go` (gqlgen handler, bearer-token plumbing, SSE shutdown lifecycle). Resolver deps must be non-nil — tests wire fakes; degraded behavior lives inside the services, not behind nil-guards.
- `grpc/` — gRPC surface: `AuthService` (`StartLogin`/`Logout` unary; `AuthStateWatch` server-streaming, joins the drain WaitGroup) and `PokeService` (unary `Poke` → `poke.Poke(SourceHost)`). Committed protoc output in `grpc/authpb/`, `grpc/pokepb/`; regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here — it *is* the definition of a gRPC request.
- `internal/` — `ipc` (per-OS user-only endpoint), `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `kubeconfig` (the one reader of the user's kubeconfig), `drain`, `lifecycle` (the start/stop/close shape every level wears), `testutil` (test-only helpers, imported by no production code), plus the subsystems below.

## gRPC + GraphQL over one socket (h2c)

`internal/app` owns the topology (that two surfaces share one socket); `grpc/` owns the predicate. HTTP/1.1 GraphQL POST + SSE are untouched. An idle `AuthStateWatch` survives the 60s `IdleTimeout` via gRPC keepalive pings. The cluster surface is **GraphQL-only**. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

**Shutdown order** (from `main.go`): `app.NotifyShutdown()` (gRPC streams end on their serving context; each SSE request's context is cancelled per-request) → `srv.Shutdown` → `app.DrainWithContext` (waits both sub-servers' WaitGroups — essential for hijacked h2c gRPC streams `srv.Shutdown` can't see) → `stop(ctx)` → `app.Close()` (`grpcServer.Stop()`, then `lifecycle.CloseAll(a.parts)`). Traps: grpc-go's `GracefulStop` **panics** on the h2c path — `Stop` only runs after the drain; never cancel via `http.Server.BaseContext` — it would tear down the shared h2c connection carrying gRPC mid-stream.

## Cluster subsystem (`internal/clustersvc`)

**Mid-rebuild.** The layout:

```
internal/clustersvc/
  service.go          the whole API — Service + the five family interfaces — plus the
                      accessors, beehive bootstrap, and registerControllers
  clusters.go         ┐ one per family, implementing its interface and holding
  caches.go           │ everything else about that kind: its beehive shapes, the
  cachedcatalogs.go   │ record GraphQL binds, its *WatchFrame, its controller, and
  cachedresources.go  │ the machinery that controller owns
  cacheddata.go       ┘ (no controller — the one family that isn't a beehive kind)
  shared.go           vocabulary every family reuses, and the two GraphQL scalars
  stream.go           Stream[T]
```

**The interfaces are specified together; the kinds are implemented apart.** The naming and scoping
rules below are rules *across* the five, checked by eye — a violation shows when they read side by
side and hides when they don't. Everything else slices by kind, so one file teaches you one kind.
`registerControllers` stays whole in `service.go` for the same reason the interfaces do: its options
are the subsystem's concurrency and retry budget, which only reads as a budget in one place.

`New` opens the beehive store under `dataDir` and registers all four controllers; `Start` runs
beehive, then each controller's background work. **The three cache controllers reconcile to a no-op,
and most family methods still panic** — `TestUnimplementedBoundaryPanics` is the inventory of what
is left, and an entry must be deleted as its method lands, since the test fails when a stub stops
panicking.

Built so far, produced: the importer that creates `Cluster` records,
`clusterController.Reconcile` observing what the kubeconfig says about each one
(`status.source.kubeconfig`), and that same pass creating the `ClusterCache` for the identity a
probe recorded, and `clusterCacheController.Reconcile` creating the `ClusterCachedCatalog` beneath
each cache, carrying the pause switch (`cacheSyncEnabled`: the cluster's toggles, and whether the
cache is still the active identity). Served: the whole `Clusters()` family except `WatchSchedule`,
plus `Caches()`' point reads (`Get`/`List`/`ListByCluster`) and its unscoped watches
(`Watch`/`WatchList`). That is enough for the kube-context picker, which reads
`clustersWatch` alone. **No cache exists at runtime yet**: creation keys off `status.server.uid` and
nothing writes it until the connection probe lands, so those reads answer empty — and no catalog
either, since one is only written for a cache. There is no connection manager, discovery pass, sync
worker, or on-disk cache; `CachedCatalogs()` serves nothing yet, and the two families below it have
no producer at all.

**A read reports the store as it is, and never filters.** A record awaiting deletion is served like
any other, carrying the tombstone (`deletionRequestedAt`) the consumer decides on — rendering it
"Deleting…" is as valid as hiding it, and only the consumer knows which. So `Deleted` means what
beehive means by it, the row is gone, and the soft-delete mark is an ordinary `Modified`: the row is
still there, wearing a tombstone. The frontend drops those rows once, in `ClustersProvider`'s fold.

Filtering in the boundary is what this replaced, and the reason is worth keeping: "invisible to a
reader" was an invariant every read, every watch, and every mutation had to maintain *in agreement*,
it needed per-subscription state to suppress the duplicate departure it created, and four
consecutive reviews found a different place that had forgotten it.

**Every send goes through `sendFrame`** (`stream.go`), which is how a pump keeps the promise
`NewStream` states: a bare channel send blocks forever once the consumer stops draining, leaking the
goroutine and the beehive watch behind it.

**A controller owns its kind's machinery**, and `service` holds the controllers only to drive their
lifecycle — read `clusterController.machinery()` for what the Cluster kind owns, and the leaves each
other controller grows land the same way. Otherwise the composition root accumulates every kind's
detail.
`registerControllers` builds and registers all four, returning them in registration order plus the
cluster's on its own, which is the one `New` keeps a reference to. All four register with
`startupPass` (`WithStartupFullPass(true)`): each owns state a restart invalidates and the store
reads as settled, since the generation was observed by a process that is gone. **No periodic full
pass** — controllers re-arm with `RequeueAfter` and the out-of-band buses cover the rest.
→ [ADR: beehive control plane](../docs/adr/2026-08-09-beehive-control-plane.md).

**Shared dependencies travel in `deps`** — one beehive client per kind plus the process-wide services
(`poke` today), built once by `newDeps(bh, pokeSvc)` and **embedded** by `service` and by each
controller, so a family reads `a.s.cacheClient` and a controller reads `c.cacheClient`. The `Client`
suffix is load-bearing: the fields are promoted into both, and `a.s.cacheClient` must not read like
the `Caches` family it is reached through. **A new kind or a new
shared service is a field, never another constructor parameter** — the alternative threads each one
through the constructors that don't use it, which is what the parameter list was doing at two kinds.
What stays an argument is a single owner's own *configuration*, which nothing has today —
every controller takes `deps` alone. Tests build the same struct (`newTestDeps` /
`newRunningDeps` in `testutil_test.go`) rather than assembling clients of their own: the owner edges
need every kind in one store, which beehive enforces.

**One lifecycle shape at every level** — `lifecycle.StartCloser`. Beehive included: it is wrapped
as one and sits at the head of `service.parts`, so `Start`/`Close` are one
`lifecycle.StartAll`/`CloseAll` call. **Add a participant by putting it in the slice as a named
`lifecycle.Part`, never by writing another stop closure** — every phase reports failures under
that name, so a participant must not wrap its own. Slice order is start order; stop and close
reverse it. `ctx` bounds
startup alone — background work ends via the stop func, which must be idempotent and must wait with
`drain.WithContext`. A kind with no machinery embeds `lifecycle.None`; something whose stop func
already releases everything enters as a `lifecycle.StartFunc`. → [ADR: lifecycle
composition](../docs/adr/2026-08-16-lifecycle-composition.md).

**A parent controller creates the child kinds it owns.** A cache's identity is discovered by the
cluster's probe, and a controller only ever reconciles an object that already exists — so
`clusterController.Reconcile` creates the `ClusterCache` (via `ensureClusterCache`) and
`clusterCacheController.Reconcile` the `ClusterCachedCatalog` beneath it (via
`ensureClusterCachedCatalog`), and the same shape carries on down the chain. Distinct from an
importer, which decides which objects exist *including when there are none*. **The writes live in the
child kind's file**, not the parent's: the name, spec and owner edge are that kind's vocabulary, and
the parent supplies only the policy — when, and with which switch. A teardown stops the chain: a
pass whose object, or whose owner, is deletion-pending or already collected writes nothing, since the
cascade is coming for the subtree either way.

**A relayed value needs a `depends_on` edge; the owner edge is not one.** The catalog's `Enabled` is
the cluster's toggles resolved once above (`cacheSyncEnabled`, which also folds in whether the cache
is still the active identity), so a flip on the cluster has to reach the cache — and owning a child
wakes nothing. `clusterCacheController.Reconcile` therefore declares `AddDependency(cache, cluster)`;
re-asserting an existing edge records nothing, so every later pass is free. A relay written without
one sits stale until something unrelated wakes the child.

**The rest of the chain needs no edge, because the relay lands in the child's own spec.** A parent
writes `Enabled` onto the catalog, and the catalog onto each resource — a spec write bumps the
generation, which is already a wake. The cache is the exception precisely because
`ClusterCacheSpec` is identity-only (`serverUID`): its switch is never written to it, so it has to
read the cluster, and reading another object is what an edge pays for. Adding a `depends_on` where a
spec write already carries the value buys nothing and doubles the wakes.

**Importers create Cluster records; controllers reconcile them.** An importer runs *outside* beehive
because it decides which objects exist — which means running when there are none, and a controller
reconciles one object that already does. (It hangs off the controller for lifecycle ownership only;
`Reconcile` never touches it.) Each importer covers one `ClusterSpecSource` variant and is
**creation-only**: it creates a record for every context nothing yet references and never updates,
orphans, or deletes. A departed context is orphaned by `clusterController` observing it absent
(`IsPresent=false`), which keeps set membership and per-object observation from fighting. It is also
why status is unreachable from an importer at all: `UpdateStatus` lives on beehive's
`ControllerClient`, which only a registered controller is handed.

**One pass creates and wakes.** The observation reads the watcher rather than the object, so beehive
cannot know it went stale; the same pass that creates the missing records therefore `Requeue`s the
ones that already existed. They share a trigger and a `List`, and a requeue is a wake rather than a
write, so creation-only still holds. A departure is absent from the snapshot the create loop walks —
the wake is the only thing that reaches it. Creates and wakes share one retry ladder — a doubling
delay capped at `importRetryMax` — because a lost wake is the failure nothing else re-levels: an
unchanged kubeconfig republishes nothing, so the record keeps a stale observation until the file next
changes. The one wake error that is not retried is a record collected since the `List`, which no
later pass would find.

**`Reconcile` defers until the kubeconfig has been read.** beehive sits at the head of
`service.parts`, so its first owed pass can reach a record left unsettled by a previous process
before the app's kubeconfig service has read the file. `Service.Get` reports the read alongside the config
precisely because the two states are the same value — an empty config — and observing the pre-read
one would mark every present context absent and wake the kind's watches for a flap. Unread is a
`RequeueAfter`, not a write.

Scope an importer by the source discriminant (`Spec.Source.Kubeconfig != nil`), not by the name prefix.
Manual creation will have no importer at all, so *"every Cluster has an importer behind it"* is not
an invariant to lean on.

**Watches are pull-first** — correctness comes from the poll, and push only makes it prompt.
`kubeconfig.Service` is the worked example (its godoc has the reasoning): a 30-minute backstop tick
under fsnotify wakes and a poke subscription, both optional and allowed to fail. Applies to every
watch. **Keep the tick under a new push layer rather than replacing it** — it is what covers what
events cannot see, including the resume the poke subscription is there for.
`kubeconfigImporter` has no tick and no poke of its own: its triggers are a kubeconfig snapshot and
`Resync`, and a failed pass re-levels on the backoff retry. Whatever changes the record set out of
band has to call `Resync` — `Clusters().Delete` does, since deleting a `Cluster` whose context is
still in the file frees that context without changing anything the kubeconfig service would
republish. That pass fails while the drained record still holds the name, and the retry ladder
covers the tail.

The service watches **directories, and follows symlinks**: a save replaces the inode (so a
file-level watch goes deaf), and a dotfiles-managed kubeconfig is a link whose target lives in a
directory nobody would otherwise watch. The resolved set is recomputed per reload, so a re-pointed
link follows to its new directory. Reach for `resolvePaths` before adding anything here.

`clustersvc.New(dataDir, kubeconfigSvc, pokeSvc)`, `Start`, and `Close` keep their signatures, so
filling the shell in is never a change to the composition root.

**The leaves this package drives speak native vocabulary** — GVRs, a `rest.Config`, cache rows —
never the records above; the controllers translate. A leaf reaching for one of this package's types
gets an import cycle, which is what enforces the direction. Put a mechanism in a leaf, never in a
controller: **if `go test ./internal/clustersvc` stops being fast, one has leaked back in.**

**A process-wide service is the app's, and this package only reads it.** `kubeconfig.Service`
arrives through `deps` behind the narrow `kubeconfigService`, so `clusterController` neither starts
nor closes it — its `Close` ends every subscription in the process, including other packages'.
Only the importer is the controller's own machinery.

The five families are `Clusters()`, `Caches()`, `CachedCatalogs()`, `CachedResources()`, and
`CachedData()`. **The `Cached*` prefix marks the cache subtree** — what a `ClusterCache` catalogs,
the per-kind records under that catalog, and the mirrored content itself — so the grouping is visible
in the accessor list rather than something you have to know. Keep it when adding a family there.

Rebuilding a family means replacing the panics in that family's file. Keep the method naming rule
when you do: **VerbNoun with the noun elided when it equals the family's subject**, so
`Caches().WatchList()` watches caches and `Caches().WatchStats()` streams one cache's stats.
**A family owns a read only when the read differs per record type.**
`RetryConnection`/`GetConnection` stay top-level (they answer about a connection, not a record), and
so do `ListEvents`/`WatchEvents`: an event carries no kind, every id is the same `ObjectID`, and only
three of the five families have a timeline at all — scoping them would be three copies of one method
plus an unanswerable question about the other two. Every family is asserted separately
(`var _ Caches = cachesAPI{}`), in the resolver tests' fake too: satisfying `Service` only proves the
accessors exist.
→ [ADR: record-family sub-APIs](../docs/adr/2026-08-10-cluster-service-sub-apis.md).

**The scope is the entry point, never an argument to a general one** — the rule beehive states in
`objectswatch.go` ("a caller cannot ask for a scope the entry point did not choose"), and the reason
this boundary reads like the library under it. Each axis is its own method: `Get(id)`/`Watch(id)` for
one record, `List()`/`WatchList()` for the whole collection, `ListBy*(id)`/`WatchBy*(id)` for a
scope. Every id is the same `ObjectID`, so a shared `List(id)` could not say whether the id was the
record's or its parent's; the method name is what disambiguates, and folding these back into one
method with a selector argument would undo it.

**`By*` names the scope the caller passes, not the owner edge.** They coincide for `Caches`
(`ByCluster`) and `CachedCatalogs` (`ByCache`), but `CachedResources().ListByCache` crosses the
catalog that actually owns those records — the service resolves that anchor precisely so callers,
who only ever hold a cache id, never have to. The schema keeps the catalog out of the path for the
same reason.

The interface is designed complete rather than caller-driven: the backend is a shell, so the
methods are the specification and a missing frontend caller is not an argument against one. Fill
the matrix for a new family.

**A watch whose source can die returns `*Stream[T]`** (`Frames` + `Err()`), not a bare channel:
`Frames` closes on every exit, so `Err` is the only thing separating a failure from an ordinary
teardown, and the reason is set *before* `Frames` closes — which is what makes "Frames closed" a safe
cue to read `Err`. `NewStream` is exported so a fake implementing these interfaces can build one. The
rule is the source, not the shape: anything reading a fallible upstream returns a `Stream`, gauges
included; a watch that cannot fail terminally may stay a plain channel.

### Types

The four kinds' spec/status/record structs, identity, conditions, frame types, and the cached-data
records are fully intact — **the schema binds them by name, which is why they survived a teardown
that removed everything that produced them.** Each lives in its family's file, beside the methods
that serve it; anything kind-agnostic goes to `shared.go`. Unexported helpers live with their only
consumer (a callee follows its caller — `LiveCondition` needs `TruncateMessage`, so both are in
`shared.go`).

- `ClusterID` **is** the beehive ObjectID — opaque, source-agnostic, stable for the record's life;
  never the remote UID or the context name. One shared GraphQL `ObjectID` scalar (decimal string)
  carries every kind's id; frontend codegen maps it to `string`.
- `ClusterSpec` is user/API-owned (`Name`, `Enabled`, `SyncEnabled`, `Source` — a discriminated
  union, kubeconfig today); the matching *observation* belongs in `ClusterStatus.Source`, never spec.
  The spec carries **no trigger/counter fields** — retries and resyncs ride out-of-band buses.
- `ClusterCache.Spec.ServerUID` names the physical identity a cache mirrors. **Active-ness is
  deliberately not a field** — it is the live join against the cluster's `status.server.uid`
  (`CacheIsActive`). → [ADR: delta watches](../docs/adr/2026-08-09-delta-watch-protocol.md).
- Every condition is a **liveness** condition (`LiveCondition` is the only constructor);
  beehive serves a previous process's write as `Unknown`+`Unconfirmed` until re-confirmed.
  **`Unconfirmed` is load-bearing on the wire**: the surviving reason/stamps describe *last-known*
  state, and a consumer must not render a pre-restart reason as current.
  → [ADR: liveness conditions](../docs/adr/2026-08-09-liveness-conditions.md).
- `Condition` aliases `beehive.Condition`, so the record vocabulary depends on beehive directly.
  That is the seam a rebuild on a different control plane would cut.

### GraphQL surface (cluster)

The schema **is** the Go shape — every GraphQL type binds 1:1 by name to its `internal/clustersvc` type in `gqlgen.yml`; no projection layer. Resolvers are one-liners delegating to a family on `r.ClusterSvc` (e.g. `r.ClusterSvc.CachedData().WatchObjects`; the field is named `ClusterSvc` to avoid shadowing the generated `Clusters` method). The whole surface below is intact in the schema and in the resolvers, but **only the `Cluster` surface and the `ClusterCache` reads answer** — `cluster`, `clusters`, `clustersWatch`, the enable/sync/delete mutations, and `clusterCache`/`clusterCaches`/`clusterCachesWatch` (with `Cluster.caches` alongside them). `Cluster.events` does not: it reaches `ListEvents`, which still panics, so a query selecting it panics with the rest. Neither do the cache gauges, which are unbuilt. This section is the contract the rebuild must satisfy rather than a description of what answers today. Key entry points:

- Delta watches: `clustersWatch`/`clusterCachesWatch` (independent; joined client-side), `clusterCachedCatalogsWatch` (unscoped, one per cache), `clusterCachedResourcesWatch(cacheID)` (cache-scoped — ~100 records; the always-mounted registry must not carry it), `clusterCacheHealthWatch` (the fold — a gauge, **not** a delta watch, so no `Bookmark` rides it; see the gauge bullet below).
- **Every delta watch closes its snapshot with one `FrameBookmark`**, carrying a nil entity — which is why the seven `*WatchFrame` types hold their entity by pointer and the schema types it nullable. Both are named for the frame, not the change: a frame is a change **or** the bookmark, so `ClusterChange`/`ChangeType` would each have been a lie for one value of the enum. A record watch sends it between the snapshot and the first live change, and carries a failure reason out through `Stream.Err()`. A per-cache watch must send it after the first successful read *or* the first bind that finds no open cache (an unopened cache is definitively empty, not pending), and anything that holds frames back must queue the bookmark behind them — it must not claim a snapshot is complete over frames still undecided. → [ADR: delta-watch protocol](../docs/adr/2026-08-09-delta-watch-protocol.md).
- **Gauges are their own subscriptions, never a field on the record they describe** — `clusterCacheStatsWatch(id, cacheID)`, `clusterCacheHealthWatch`, `clusterScheduleWatch(id)`. A field would only be re-read when the record's own watch fires a frame, and each of these keeps moving after its record settles: a cache's object counts, a countdown. So a field freezes at whatever the last frame happened to carry. Re-emitting the record to refresh one is the other half of the trap — these numbers sit outside `status` precisely so a measurement never wakes the record's dependents. Current-on-subscribe, so no `Bookmark` rides them, and nothing is emitted at all before the first measurement (which is what a consumer renders "not observed yet" from). Keep that shape when adding one: **the per-kind sync stamps and the discovery pass's gauges are deliberately unserved** until the views that need them settle, rather than parked on a record where they would freeze.
- Cache-data watches (all keyed by cluster id + cache id; frames carry `cacheID` provenance — objects additionally `apiVersion`/`resource` — so the client rejects stale frames after a swap): `clusterCachedDataKindsWatch` (kind catalog + counts; subscribes to **both** brokers via `catalogSubscribe`, since Event counts come from event triggers), `clusterCachedDataEventsWatch` (newest window, `Deleted` when aging out), `clusterCachedDataObjectsWatch` (per-kind rows incl. `rawJSON`; resource-keyed broker subscription). Unopened cache → the `Bookmark` alone.
- Point reads hang off the record that owns them, resolved on selection: every event timeline is an `events(category, limit)` field (`Cluster.events`, `ClusterCache.events`, `ClusterCachedResource.events`), the discovered kind catalog is `ClusterCache.kinds` (no arguments — both ids it reads with come off the record), and `Cluster.caches` / `ClusterCache.cachedResources` walk the owner chain down (`Caches().List`, `CachedResources().List`). So there are no root `cluster*Events` or `clusterCachedDataKinds` fields. The lookups `clusterCache(id)` and `clusterCachedResource(id)` (over `Caches().Get`/`CachedResources().Get`) address a record by **its own** id, which a caller holding one from a watch frame uses directly.
- **Every noun has the same pair at root: `<noun>(id)` and `<nouns>(<parent>ID)`** — `cluster`/`clusters`, `clusterCache`/`clusterCaches(clusterID)`, `clusterCachedResource`/`clusterCachedResources(cacheID)`. The plural's scope argument is **optional**: omitted it reads the whole fleet, passed it returns exactly what the nested field serves (`Cluster.caches`, `ClusterCache.cachedResources`). The resolver picks the boundary method the argument implies — `Caches().List` when nil, `Caches().ListByCluster` when set. Keep that shape when adding a noun.
- **The query path skips `ClusterCachedCatalog`.** `ClusterCache.cachedResources` is keyed by the cache and resolves the catalog itself (`CachedResources().List`, like `CachedResources().Watch`): exactly one catalog exists per cache and its name is derived from the cache id (`ClusterCachedCatalogName`), so it is an implementation detail, not a branch to navigate. The catalog's own state still streams on `clusterCachedCatalogsWatch`.
- **`Cluster.caches` is the set, never "the" cache.** Activeness is the live join against the parent's `status.server.uid` (`CacheIsActive`), and a probe rewrites that UID with no cache event — so a consumer that must follow it over time reads `clustersWatch` + `clusterCachesWatch` and joins them, rather than reading the query field. → [ADR: delta watches](../docs/adr/2026-08-09-delta-watch-protocol.md). The live counterparts `eventsWatch` and `clusterScheduleWatch` (countdown + `probing`) stay flat at root: only the point reads nest.
- Mutations: `clusterEnabledSet`, `clusterSyncEnabledSet`, `clusterConnectionRetry` (returns immediately; outcome lands on conditions), `clusterCacheClear` (delete files then **bounce that cache's workers** — nothing else would rebuild them; they cold-sync, the cookie died with the file), `clusterDelete` (GC cascades; a still-present context is re-imported under a fresh id).
- `ClusterCachedDataEvent.type` is a plain `String!` (k8s doesn't constrain it) and timestamps are nullable `Time` via `nilIfZeroTime` (`graph/util.go`) — the record keeps value `time.Time` for comparability.
- **A watch that dies reports why** (`graph/watch_failure.go`). gqlgen builds each subscription frame as data alone and stops the instant the resolver's channel closes, so a failed watch is otherwise byte-identical to a graceful end and the webview reconnects forever with nothing shown. `WatchFailureExtension` bridges that in two halves — the resolver and the frame that would carry the reason never share a response context: `InterceptOperation` hangs a slot on the operation ctx (gqlgen threads it into the resolvers *and* every later frame), `watchStream` files `Stream.Err()` into it as the frames run out, and `InterceptResponse` claims it once the stream is spent, emitting one errors-only `graphql.Response` before the transport completes. Claimed once, so the next poll ends the subscription instead of looping. The reason goes through `AddError`, so the server's error presenter logs it. The client treats that frame as a drop with a reason — reported, last-known data held, reconnect — see the root `CLAUDE.md`. → [ADR: watch-failure reporting](../docs/adr/2026-08-14-watch-failure-reporting.md).

Frontend join: `src/lib/clusters.tsx` (`ClustersProvider`/`useClusters`) reduces the three unscoped streams and joins `activeCache` + `health` client-side; `cluster-sync-panel.tsx` renders it (per-row detail streams subscribe only while a row is expanded; the sync column reads the rollup's reason, never the cache's coarse `Synced`).

## Auth / identity (`internal/auth`)

Local-first accounts against kstack-cloud's Hydra (`https://oauth.kstack.sh/`). The sidecar owns the whole flow: system browser (auth-code + PKCE, loopback redirect), exchange + verify (go-oidc vs JWKS; identity from the verified ID token), refresh token in the OS keyring (`keyringStore` over `zalando/go-keyring`). Signed-in ⇔ refresh token present; works offline. No gRPC credentials channel. Degrades to signed-out when unconfigured. → [ADR: local-first auth & settings](../docs/adr/2026-08-09-local-first-auth-settings.md).

- Root `package auth` is deliberately flat, organized by file: `auth.go` (the `Service` interface + `New(Config)`, `State`/`TokenSet`, `Token`/`Identity` aliases), `grant.go` (the grant aggregate — token set as source of truth, `Authenticated`/`Identity` **derived**, lazy refresh with burst-dedup, persist-before-cache, latest-value `State` hub), `login.go` (`loginFlow`: synchronous setup — loopback bind + browser open — returning its error to the mutation; the slow tail runs in a bounded detached goroutine, observed via `authStateWatch`), `keyring.go`.
- The one carved-out sub-package is `auth/oauth` — the OAuth2/OIDC protocol layer, a **leaf** (must not import `auth`). It owns `Token`/`Identity` (root re-exports as true aliases to avoid the cycle).
- `Config` carries only production knobs; **test seams are unexported functional options** on an unexported `newWithOptions` builder (white-box tests only). External consumers fake the `Service` interface. No `Start`/`Close` — no long-lived goroutines; `Logout` clears locally first, revokes fire-and-forget (keychain-write failure → error, stays signed in).
- `TokenSource(ctx)` returns an `oauth2.TokenSource` (nil when degraded); it exposes the refresh token — consumers read `AccessToken` only. The GraphQL projection drops tokens (`AuthState { authenticated, identity }`).

## Cloud settings sync (`internal/cloud`) — depends on `auth`

Local-first settings: an edit applies to a local JSON file immediately and queues durably for the cloud. **`cloud` depends on `auth`, never the reverse** — it authenticates from `authSvc.TokenSource` and wakes on `authSvc.Subscribe()`, tracking only the `Authenticated` bit (a token refresh is a non-event). Degrades without a data dir or cloud URL. `Start` is idempotent; its `stop` replaces `Close`.

Sub-packages (leaf-first): `syncstore` (generic `Envelope[T]` + `Store[T]` over `atomicjson`), `prefs` (`Settings` — pointer fields + omitempty so absent ≠ cleared; `Merge`; store deep-copies at boundaries), `mutationqueue` (durable FIFO, survives restart), `api` (GraphQL-over-HTTP client, per-request `TokenSource`), `prefsync` (the reconcile `Engine`: supervised connection with backoff + poke; `Watch` returns data + a buffered terminal-error channel so an errored close keeps escalating backoff; on Live drains the queue, and incoming snapshots get pending patches re-layered via `prefs.Merge`). Test seams: unexported functional options, same pattern as `auth`.

## Kubeconfig (`internal/kubeconfig`)

**The one reader of the user's kubeconfig.** App-owned, in `App.parts` between poke and the cluster
service. Nothing else in the sidecar watches the file, calls `clientcmd`, or builds a `rest.Config`
— a package that wants to know about a context reads the cluster records. `Get()` returns the last
snapshot plus whether a read has happened; `Subscribe()` is current-on-subscribe; `Close()` ends
every subscription in the process, which is why only the app calls it.

`RESTConfig(contextName)` resolves one context to credentials **and** the key a connection pool
caches them under (`restconfig.go`). Three things it holds to:

- **One snapshot per call.** The credentials and the proxy URL come from the same `Get`; two reads
  would let a reload key one snapshot's proxy onto another's credentials, and the key is the pool's
  identity.
- **Only a config the loading rules produced.** Loading is what resolves `certificate-authority:
  ca.crt` against the kubeconfig's own directory. A hand-built `api.Config` silently yields CA and
  client-cert paths that cannot be opened.
- **The key excludes the context name**, so two contexts aimed at one cluster share a connection. It
  covers the *static* exec/auth-provider config — minting a token is the transport's job, but
  editing how one is obtained must redial, including what the plugin is *handed*
  (`ProvideClusterInfo`, and the cluster's own exec extension, which is how one user entry serves
  several clusters under different audiences) — and carries `proxy-url` alongside the `rest.Config`,
  which compiles it into an unhashable func. **Every value is length-prefixed and every list and
  optional block carries its length**: hashed as a bare run of values, an auth provider and an exec
  plugin collide, and the pool serves one context a transport built for another's credentials.

Two sentinels, both acted on rather than logged: `ErrContextNotFound` (the record is orphaned —
also what an empty context name gets, since `clientcmd` would otherwise fall back to the current
context) and `ErrNotRead` (nothing read yet, which looks identical to "every context absent").

Resolution is **not memoized**: each call re-copies the config and rebuilds TLS and auth material.
Add a per-context memo invalidated on publish when a caller's cadence makes it show.

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
