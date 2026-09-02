# sidecar — Go backend

A standalone Go binary started by the Tauri host. It serves the app's GraphQL API and a gRPC control channel and owns all Kubernetes logic. **No TCP**: it listens on a Unix socket (named pipe on Windows), prints `READY unix:<path>` to stdout, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF**.

`--data-dir` / `KSTACK_DATA_DIR` is **required**; `app.New` errors when empty and tests pass `t.TempDir()`. `<data-dir>/app.db` is `internal/appdb`'s: one migration sequence, numbered files in `appdb/migrations/`, never a second embed against the same file.

This file states what is true now. Why it is that way lives in `docs/adr/`; every section links its ADRs. Rationale goes there, not here.

## Layout

`main.go` is lifecycle only; `internal/app` is the composition root and routing; GraphQL lives in `graph/`. No `server` package.

- `internal/app/` builds `poke`, `kubeconfig`, `clustersvc`, `auth`, `cloud`, wires `graph.NewServer` + `grpcserver.NewServer`, and multiplexes both onto one h2c handler. `App.parts` is start order (poke → kubeconfig → cluster → cloud); stop and close reverse it. **kubeconfig before cluster is load-bearing** (`app_test.go` pins it). The transports stay out of the slice; `grpcServer.Stop()` runs first in `Close`.
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go`. Resolver deps are non-nil; tests wire fakes.
- `grpc/` — `AuthService`, `PokeService`, committed protoc output in `authpb/`, `pokepb/`. Regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here.
- `internal/` — `ipc`, `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `kubeconfig`, `drain`, `lifecycle`, `workqueue`, `supervisor`, `testutil` (test-only, imported by no production code), plus the subsystems below.

## gRPC + GraphQL over one socket (h2c)

`internal/app` owns the topology; `grpc/` owns the predicate. The cluster surface is GraphQL-only. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

Shutdown order from `main.go`: `app.NotifyShutdown()` → `srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close()`. Two traps: grpc-go's `GracefulStop` **panics** on the h2c path, so `Stop` runs only after the drain; and never cancel via `http.Server.BaseContext`, which would tear down the shared connection mid-stream.

## Security invariants

Full picture: [`docs/security-model.md`](../docs/security-model.md). The sidecar holds every credential in the system, so these four are load-bearing:

- **Only the host process may connect.** `ipc.Authenticated` checks each accepted connection's peer pid against `--host-pid` (the kernel stamps it, so a client cannot claim another's) and closes anything else without ending the accept loop; zero, the standalone-run default, falls back to the uid alone. The file mode carries the rest: `ipc.Listen` tightens the umask *before* `net.Listen` so the socket is never briefly world-accessible, then chmods 0600; Windows binds the pipe owner-only (`D:P(A;;GA;;;OW)`). Both are pinned by tests. Never widen either, never move the endpoint out of a 0700 directory, and never add a TCP listener — the GET transport is registered alongside POST and SSE and is only harmless because the transport is local.
- **Redaction happens at write time, keyed off the body's own group and kind,** so it cannot be bypassed by how an object was addressed (`kubestore/objects.go`). A new read path serves the stored body; it does not get to re-derive what to hide. The table is deliberately incomplete, so treat a cache file as holding cluster data in the clear — that is what makes its file mode and its lifetime security properties. → [ADR: secret redaction](../docs/adr/2026-08-30-secret-redaction-at-write-time.md).
- **Reading a kubeconfig can execute code.** `clientcmd` honours `exec` credential plugins, and the connection probe dials every declared context on startup and on every file change. Anything that widens what gets probed widens what runs.
- **Credentials never reach a log or the wire.** The error presenter logs the operation but never `variables`; the GraphQL projection carries the sign-in bit and identity, never tokens; `oauth.ParseIdentityUnverified` skips signature verification and is display-only — never an authorization input. The loopback callback compares `state` before consuming a code or an `error`, so a stray local request cannot abort the one-shot flow (`TestLoopbackRejectsInvalidCallbackWithoutConsuming`).

## Cluster subsystem (`internal/clustersvc`)

```
internal/clustersvc/
  service.go           Service + the four family interfaces, accessors, beehive bootstrap,
                       registerControllers
  clusters.go          ┐ one per family: its beehive shapes, GraphQL binds, *WatchFrame,
  caches.go            │ controller, and the machinery that controller owns
  cachedkinds.go       │
  cacheddata.go        ┘ (no controller — the one family that isn't a beehive kind)
  clustersources.go    the ClusterSource kind: one discovery anchor per source variant
  triggers.go          feed→wake bridge onto beehive names
  shared.go            shared vocabulary, the app's services as this package sees them, scalars
  stream.go            Stream[T], deltaWatch, sendFrame
  events.go            the one events read path
  internal/kubeconn/   connections and the five probes (leases, probe.go, service.go)
  internal/kubesync/   what fills a cache: arming seam, one session per cache, discovery.go,
                       kinds.go
  internal/kubestore/  one SQLite file per cache behind a refcounted manager; statements.go,
                       a file per table, the change ping bus
```

**Direction.** The leaves speak native vocabulary (GVRs, `rest.Config`, cache rows), never records; the controllers translate. A leaf importing a record type is an import cycle. Put a mechanism in a leaf, never in a controller: if `go test ./internal/clustersvc` stops being fast, one has leaked back in. → [ADR: package split](../docs/adr/2026-08-10-cluster-package-split.md).

**The chain.** The `ClusterSource` anchor's pass creates `Cluster` records; the cluster pass holds a `kubeconn` claim and folds what its probe found; the same pass creates the `ClusterCache` for the identity the probe recorded; the cache pass arms discovery and mirrors `kind_catalog` into `ClusterCachedKind` records; each kind record's pass arms that kind's sync. **kubesync decides what exists; the records decide what is mirrored.** → [ADR: beehive control plane](../docs/adr/2026-08-09-beehive-control-plane.md), [ADR: discovery as a beehive kind](../docs/adr/2026-08-18-discovery-as-a-beehive-kind.md), [ADR: arming is policy](../docs/adr/2026-08-28-arming-is-policy-never-interest.md).

### Controllers and passes

- A pass returns a verdict, never an error: `Settled()`, `Unsettled()`, or `Fail(err)`. `.RequeueAfter(d)` is for a wait this pass knows the length of; a cadence a kind depends on goes at registration. A no-op pass still settles.
- A status write is unconditional; beehive suppresses equal bytes. Don't add a guard in the pass.
- Only the values, never the timing. No timestamp or counter goes on a record's status; the steady state must be silent. → [ADR: two conditions, no timing](../docs/adr/2026-09-02-cluster-conditions-two-subjects.md).
- A parent controller creates the child kinds it owns, in the child kind's file, as one write with no read in front (`CreateOrUpdate(name, spec, WithOwner(parent))`, or `GetOrCreate` when the spec is the identity). Both refuse a deletion-pending row. A pass whose object or owner is deletion-pending writes nothing.
- A relayed value needs a `depends_on` edge; the owner edge is not one. `clusterCacheController` declares `AddDependency(cluster)`; the rest of the chain relays into the child's spec, which is already a wake.
- All four controllers register `startupPass`. `ClusterSource` and `Cluster` also take a resync interval: each observes something the store cannot see move (a file, a remote server). A fold whose answer the store cannot see move owes a resync.
- Both reconciles defer with `Unsettled` until the kubeconfig has been read. **Keep those guards if you reorder startup**; the pre-read config is indistinguishable from a file with no contexts.
- Scope a discovery pass by `Spec.Source.Kubeconfig != nil`, not by name prefix. The create pass is creation-only and runs ahead of the fingerprint gate. `Clusters().Delete` refuses a record its source still declares (`ErrDeclaredBySource`), reading the kubeconfig, not the record's observation.
- Neither cache controller writes a condition; the verdict is the gauge's. `Paused` is the user's field; the catalog owns the other four. Pause keeps the rows. → [ADR: kind records mirror the catalog](../docs/adr/2026-09-02-kind-records-mirror-the-catalog.md).
- Shared dependencies travel in `deps`, embedded by `service` and every controller. A new kind or service is a field, never a constructor parameter. Tests build the same struct via `newTestDeps` / `newRunningDeps` / `newRunningRegisteredDeps` (`testutil_test.go`).
- One lifecycle shape at every level: `lifecycle.StartCloser`, composed through `StartAll`/`CloseAll`. Add a participant as a named `lifecycle.Part` in the slice, never a stop closure. → [ADR: lifecycle composition](../docs/adr/2026-08-16-lifecycle-composition.md).
- `clustersvc.New(dataDir, kubeconfigSvc, pokeSvc)` grows a parameter only for a new process-wide service. The package only reads `kubeconfig.Service`; only the app closes it.

### Identity

A context is not an identity. `Connection` carries a set-once `serverUID`; `Lease.ConnFor(ctx, serverUID)` answers from it. **Never correlate a connection with `State.ServerUID`**: it is a separate probe's observable and lags a rebuilt connection by a round-trip. A second, different UID over one connection makes it vouch for nobody, and the conflict rebuilds the connection. `ConnFor` never waits; a run that cannot get a connection records `NoConnection`/`IdentityMismatch` and `Suspend`s. → [ADR: connection-carried identity](../docs/adr/2026-08-25-connection-carried-identity.md), [ADR: identity-driven retirement](../docs/adr/2026-08-27-identity-driven-retirement.md).

### Events

Three timelines, each `(ObjectID, category)`: `Cluster`/`connection`, `ClusterCache`/`discovery`, `ClusterCachedKind`/`sync`. Every pass writes unconditionally. One read path (`events.go`) by id alone. A nil `category` adds no option (the empty string is the default timeline, which answers nothing). `terminalErr` drops `ErrNotFound` and forwards the rest. `Event.id` is unique within one timeline; never hand one where an object id is expected. → [ADR: event timelines](../docs/adr/2026-09-02-event-timelines.md).

### Streams and watches

- Every send goes through `sendFrame` (`stream.go`). A bare channel send leaks the goroutine and the beehive watch once the consumer stops draining.
- One pump serves every record watch: `deltaWatch[Spec, Status, Frame]`. A kind supplies a `frame` projection, a `departed` builder, and a `bookmark` value. Never a fourth pump. The pump's rules are tested once in `stream_test.go`.
- A watch whose source can die returns `*Stream[T]` (`Frames` + `Err()`); `Err` is set before `Frames` closes. Gauges included. `NewStream` drops whatever a pump returns once ctx is done.
- A read reports the store as it is and never filters; a tombstoned record is an ordinary `Modified`. beehive folds a run of writes to one object into its last op, so a record collected before the tailer reads its mark arrives as the `Deleted` alone — the mark is never promised, the departure always is. The frontend drops those rows once. → root `CLAUDE.md`.
- The gauges (`WatchStats`, `WatchHealth`, `WatchSyncStatus`, `WatchSchedule`) are read-side folds on a cadence, current-on-subscribe, no `Bookmark`, nothing emitted before the first measurement. Paused kinds resolve from the record ahead of every `GetKindState`; an unanswered kind is not an offender; `LastLiveAt` is the oldest proof. → [ADR: cache health fold](../docs/adr/2026-09-02-cache-health-fold.md).
- `clusterScheduleWatch` reads the pool's cadence, never beehive's: the connection probe's next run alone.

### Families and the GraphQL surface

The four families are `Clusters()`, `Caches()`, `CachedKinds()`, `CachedData()`. The `Cached*` prefix marks the cache subtree; the `Data` infix marks content the store serves, as against control-plane records (`CachedKinds()` vs `CachedData().*Kinds`, `clusterCachedKindsWatch` vs `clusterCachedDataKindsWatch`). Method names are VerbNoun with the noun elided when it equals the family's subject. A family owns a read only when it differs per record type; `RetryConnection`/`AcquireConnection`/`ListEvents`/`WatchEvents` stay top-level. The scope is the entry point, never an argument: `Get(id)`/`Watch(id)`, `List()`/`WatchList()`, `ListBy*(id)`/`WatchBy*(id)`. Every family is asserted separately (`var _ Caches = cachesAPI{}`), in the resolver tests' fake too. → [ADR: record-family sub-APIs](../docs/adr/2026-08-10-cluster-service-sub-apis.md).

The schema **is** the Go shape: every GraphQL type binds 1:1 by name in `gqlgen.yml`; resolvers are one-liners on `r.ClusterSvc`.

- Every delta watch closes its snapshot with one `FrameBookmark` carrying a nil entity, which is why every `*WatchFrame` holds its entity by pointer. A per-cache watch sends it after the first read or the first bind that finds no cache; an unopened cache is empty, not pending. → [ADR: delta-watch protocol](../docs/adr/2026-08-09-delta-watch-protocol.md).
- Gauges are their own subscriptions, never a field on the record they describe. `clusterCacheSyncStatusWatch` is the only wire field carrying a per-kind verdict; its fold answers `Paused`, then `StoreFailed`, before the per-kind loop.
- Cache-data watches are keyed by cluster id + cache id and carry `cacheID` provenance (objects also `apiVersion`/`resource`).
- Point reads hang off the record that owns them (`Cluster.events`, `ClusterCache.kinds`, `Cluster.caches`, `ClusterCache.cachedKinds`). Every noun has the same root pair, `<noun>(id)` and `<nouns>(<parent>ID)`, the scope argument optional. Keep the shape when adding a noun.
- `Cluster.caches` is the set, never "the" cache; activeness is the live join on `status.server.uid` (`CacheIsActive`).
- Mutations: `clusterConnectionRetry` is held open for the probe's round trip; `clusterCacheClear` takes the cache's own id, stops its workers, deletes the file, then requeues its kinds; `clusterCachedKindSyncEnabledSet` pauses one kind and keeps the rows; `clusterDelete` is refused with `ErrDeclaredBySource`.
- Timestamps are nullable `Time` autobound to value `time.Time`; the delta-watch diff compares frames with `==`.
- A watch that dies reports why through `WatchFailureExtension` (`graph/watch_failure.go`). A resolver over a `*clustersvc.Stream` goes through `watchStream`, never `ptrStream`. → [ADR: watch-failure reporting](../docs/adr/2026-08-14-watch-failure-reporting.md).

Types: `ClusterID` **is** the beehive ObjectID; one `ObjectID` scalar carries every kind's id. `RecordMeta` (`shared.go`) is the metadata half of every record, embedded and autobound. `ClusterSpec` carries no trigger or counter fields. `ClusterCache.Spec.ServerUID` is the identity a cache mirrors; active-ness is not a field. Every condition is a liveness condition (`LiveCondition` is the only constructor); `Unconfirmed` is load-bearing on the wire. `Condition` aliases `beehive.Condition`. → [ADR: liveness conditions](../docs/adr/2026-08-09-liveness-conditions.md).

### The connection pool (`internal/kubeconn`)

A cluster is the only way to address a connection; the pool sits behind `clustersvc`. → [ADR: addressed by ClusterID](../docs/adr/2026-08-22-connections-addressed-by-cluster-id.md), [ADR: one connection per context](../docs/adr/2026-08-23-one-connection-per-context.md).

- `Acquire(contextName)` never fails and never waits. `Lease` is `Conn` / `ConnFor` / `State` / `WatchState` / `Departed` / `Release`. `Conn` never dials; a connection whose last probe failed is still handed out.
- `RetryAndWait` wakes all five probes and returns once the connection probe it asked for has finished (`LastRunAt` at or after the ask). Nothing cancels the run. → [ADR: retry resolves with its probe](../docs/adr/2026-08-30-retry-resolves-with-its-probe.md).
- A `Connection` carries `Dynamic`, `HTTPClient`, `Discovery` over one pool. `Discovery` gets its own `http.Client` with a timeout because client-go's discovery calls take no context.
- Every non-watch request carries an idle-read bound (`idletimeout.go`): progress, never a deadline; watches exempt; a cancel reports `ErrIdleTimeout`. → [ADR: idle-read bound](../docs/adr/2026-09-02-idle-read-bound.md).
- The boundary gate (`AcquireConnection`/`RetryConnection`/`WatchSchedule`) answers `ErrNotFound` or `ErrNotConnectable` off the record's own state, never off whether the server answers.
- A claim outlives what it is a claim on; an unread kubeconfig is not a departure. `stateHub` publishes before `signalHub`.
- The pool publishes per claim on `WatchState` and per context on `Subscribe` (a `gobus/conflate` bus; `conflate`, not `WatchAcross`, which collapses a burst). `WatchState` delivers nothing on attach; pair it with `State()`. Every value is a level, never an edge.
- `State.Identity()` is what the probes last read; `Connection.ServerUID()` is what one connection vouches for. Both exist and answer different questions.
- `State.Phase()` and `State.Identity()` are the pool's readings; condition types, reasons, and `Inactive` are the record's vocabulary.
- `configureHTTP2Keepalive` is called from `New`, not the composition root.
- Five independent `Observation[T]`s: `Connection`, `Readiness`, `ServerUID`, `ServerVersion`, `Principal`. An `Observation` keeps its value through a failure; `LastSeen` dates the value, not the verdict. `Attempt` is one run at any stage; a zero `NextAttempt` means suspended, and why is `LastAttempt.Reason`. `Reason` is our own vocabulary styled as a condition reason, assigned when the attempt ends, spanning layers on purpose; free text goes in `Message`. Two traps: `NotFound` and `Unsupported` both arrive as a 404, and `Dynamic` returns `*apierrors.StatusError` while the raw endpoints leave only a status code. A `State` copy is shallow. → [ADR: connection probing](../docs/adr/2026-08-09-connection-probing.md).
- Reaching the server is one `GET /api`; empty `versions` is `ReasonMalformed`. The probe builds a connection; the pool retires one, on a changed fingerprint *or* no connection *or* a conflict. → [ADR: the connection probe dials /api](../docs/adr/2026-08-25-connection-probe-dial.md).
- Publishing is `OnPass`: `stateHub` carries every pass, `signalHub` only when the news changed. A conflict's rebuild wake is edge-gated on the news moving.

### The supervisor (`internal/supervisor`)

Kubernetes-free scheduling: a work queue, a level-triggered pass, a schedule derived from what the last run recorded. → [ADR: probe engine](../docs/adr/2026-08-24-probe-engine.md), [ADR: supervisor vocabulary](../docs/adr/2026-08-28-supervisor-vocabulary.md), [ADR: jobs and workers](../docs/adr/2026-08-28-jobs-and-workers.md).

- Two kinds of thing. A **job** runs, returns, and is quiet until due; a **worker** blocks until stopped or dead and reports while it runs. Every probe and discovery read is a job; the kind sync is the one worker.
- The registration name is the whole public identity. A `supervisor.Key[T]` states a name↔type pairing once; read through `keyConnection.From(snap)`.
- A `Result` is its schedule: `Succeeded` (interval), `Fail` (ladder), `Suspend` and `Skip` (wait for a `Wake`). `Succeeded().RequeueAfter(d)` can only bring a run forward.
- `WithStartConcurrency(n)` bounds what is *starting*: a job for its whole run, a worker until `Ready`.
- `JobPass.Commit` is buffered and applied on return; `WorkerPass.Commit` is applied at once, so a worker's `T` is what a reader reacts to, never what arrives. Commit only on a change; the supervisor never compares.
- `Known()` is has-ever-answered; use it for a probe whose zero `T` is an answer.
- The supervisor hands back every value it stops holding, through `Discard(T)`, outside its lock. A commit's replaced value is not handed back.
- A worker: `Ready` means started, not proven; a worker that never calls it is recorded `NeverReady`; a stop records nothing; the ladder paces failures and the floor (`WithInterval`) paces clean restarts.
- `Wake` never tears a live worker down; `Restart` does. Neither waits. `Remove` and `Close` wait and must not be called from inside a `Run`. A watch edge onto a worker is a `Restart`.
- A body that panics or returns the zero `Result` is recorded `Internal`; the supervisor logs it through `slog` and nothing else logs.
- `Suspended()` is the narrower read (nothing due, nothing running, a suspension last); gate a revival wake on it, not on `Scheduled()`.

### The sync engine (`internal/kubesync`)

Speaks cache ids, contexts, server UIDs, GVRs; never records. Its dependencies are `Acquire(contextName)` and `OpenOrCreate(cacheID)`. A `lifecycle.Part` between `kubestore` and beehive. `Start` refuses a second start. It subscribes to `poke` for `RestartAll`. `withKindSync` substitutes the kind worker in arming tests.

- A clear runs inside kubesync: `RunWithCacheSyncStopped` / `RunWithKindSyncStopped` take `armMu`, stop and join the workers, run `fn` once, re-arm. The store work stays with the caller. `fn` must not call back into the Service.
- Two levels of arming that AND: `TrackDiscovery` (and it supplies the session) and `TrackKind`. A kind's registration outlives its cache being forgotten. `ForgetDiscovery` is a pause; `ForgetCache` is a teardown. Forgetting is synchronous.
- Nothing syncs into a cache whose connection does not vouch for its `ServerUID`; the session's connection bridge brings both levels back.
- A cache whose file will not open reports `StoreFailed` via `Service.storeFailures`, which holds exactly the caches whose most recent arm failed.
- A verdict is a gauge, never a stored condition. `false` from `GetDiscoveryState`/`GetKindState` means nothing observed yet, not empty.
- News is not data: two feeds, one per level; the reader answers by re-reading. A kind is keyed by `(APIVersion, Resource)`; the singular is data.
- Every walk over `s.tracked` that ends in a `Remove` snapshots under `s.mu` and acts outside it.

**The sweep** (`discovery.go`): three jobs per cache. Writes only on a changed fingerprint. Four filters, none optional; `notMirrored` is the explicit drop list. `Partial` blocks the prune. `IsCRD` by (group, plural); printer columns by (group, version, plural), kept as a string on `KindRow`. Two wake loops: connection change and catalog change (whole sweep). → [ADR: discovery sweep rules](../docs/adr/2026-09-02-discovery-sweep-rules.md).

**The kind sync** (`kinds.go`): the run is the stream. `Ready` when the watch is open, never on a frame. Clean exit at the floor is a rotation; a close with nothing proved is a failure. The cookie decides cold versus resume; an expired position relists as `Resyncing`. A resume commits only when its reason moved, except a run's first report. `KindState` is assembled at read; a run in flight speaks for itself and a last exit describes a kind only while it is down. A run lasts as long as its connection, and a retirement is read off the connection rather than off the context: `Retire` closes `Done` at once while the cancel it triggers waits on a goroutine, so a run can read an error over a dead connection with its own context still live — reported as the kind's own failure, that would put a kind the pool just moved onto the ladder. Every duration is a `pacing` field. → [ADR: kind sync verdicts](../docs/adr/2026-09-02-kind-sync-verdicts.md).

### The store (`internal/kubestore`)

One SQLite file per cache behind a refcounted `Manager`; a `Store` is a claim. → [ADR: one store per cache](../docs/adr/2026-08-26-cache-store-per-cache.md).

- Nothing on the read side creates a file. `OpenOrCreate` (writers), `OpenExisting` (the door to a cache's contents), `Subscribe` (borrow a feed, no claim), `Clear`, `Remove`, `Stats` (no claim, read-only open), `WatchOpen`. A claim is bound to the file it opened; a `Clear` swaps it and holders answer `ErrClosed`.
- `ForgetCache` before `Manager.Remove`; `ForgetKind` before `Store.ClearKind`.
- Every write takes a strictly increasing stamp; a relist reconciles by mark and sweep on `updated_at`, never `generation`.
- Every row carries `write_seq`; the stamp moves only when `resource_version` does, and unchanged means unchanged in full. Every delete logs to `deletes` first, in the same transaction; a row leaving a kind logs one too. A reader applies deletes before writes. → [ADR: write positions and the deletes log](../docs/adr/2026-08-30-write-positions-and-the-deletes-log.md).
- Core `v1` events go to the `events` table, routed by api version and plural. Nothing ages them out.
- Bodies are sanitized on the way in; redaction is the `redactions` table keyed by (api group, Kind), looked up on the body's own apiVersion and kind. Nothing derived from a secret is ever stored. → [ADR: secret redaction](../docs/adr/2026-08-30-secret-redaction-at-write-time.md).
- Every statement is named in `statements.go`; pools hold their halves by `stmtWrites`; collections bind as one JSON argument; the prune uses `RETURNING` and drains it. Reads ride their own `query_only` pool. `sqlitemigrate` owns the open contract. → [ADR: SQL discipline](../docs/adr/2026-09-02-kubestore-sql-discipline.md).
- One janitor per open file: freelist-gated bounded vacuum, per-kind deletes trim marks, nothing waits under `m.mu`. Zero `Interval` runs none. → [ADR: janitor](../docs/adr/2026-09-02-kubestore-janitor.md).
- All-key tables are `WITHOUT ROWID`. Nothing has shipped, so a form change edits `0001_init.sql`. → [ADR: schema edit, not migration](../docs/adr/2026-08-29-schema-edit-not-migration.md).
- The change signal is a coalesced ping per kind or on the events bus, never a row delta. Closing the store closes the bus. → [ADR: ping bus](../docs/adr/2026-08-26-store-change-ping-bus.md).

**Cached-data watches** (`cacheddatawatch.go`): subscribe, snapshot, `Bookmark`, then one read per debounced burst. Objects and events read past a `Cursor` (a position **and** the Kind it was read under); the kinds watch re-reads and diffs. A cursor below the trim mark or under a different Kind goes back to the full read. A failed re-read retries in place. A cache that goes away ends the watch cleanly (`Err()` nil); any other open failure is a fault, never an empty cache. No read loads a body; `Store.ObjectBody` is fetched per `Added`/`Modified` row and a body that will not load is a null field. → [ADR: cached-data reads](../docs/adr/2026-08-26-cached-data-read-loop.md), [ADR: objects read split](../docs/adr/2026-08-29-object-read-split.md).

## Auth / identity (`internal/auth`)

Local-first accounts against kstack-cloud's Hydra: system browser (auth-code + PKCE, loopback redirect), verification via go-oidc, refresh token in the OS keyring. Signed-in ⇔ refresh token present; works offline; degrades to signed-out when unconfigured. → [ADR: local-first auth & settings](../docs/adr/2026-08-09-local-first-auth-settings.md).

- Flat root package by file: `auth.go`, `grant.go` (token set as source of truth, `Authenticated`/`Identity` derived, lazy refresh with burst-dedup, persist-before-cache), `login.go` (synchronous setup, bounded detached tail), `keyring.go`. `auth/oauth` is a leaf that must not import `auth`.
- `Config` carries production knobs only; test seams are unexported functional options on `newWithOptions`. No `Start`/`Close`. `Logout` clears locally first, revokes fire-and-forget.
- `TokenSource(ctx)` is nil when degraded; consumers read `AccessToken` only. The GraphQL projection drops tokens.

## Cloud settings sync (`internal/cloud`)

An edit applies to a local JSON file immediately and queues durably for the cloud. **`cloud` depends on `auth`, never the reverse**, tracking only the `Authenticated` bit. Degrades without a data dir or cloud URL. `Start` is idempotent. Sub-packages leaf-first: `syncstore`, `prefs` (pointer fields + omitempty so absent ≠ cleared), `mutationqueue`, `api`, `prefsync` (the reconcile `Engine`; `Watch` returns data plus a buffered terminal-error channel). Test seams as in `auth`.

## Kubeconfig (`internal/kubeconfig`)

**The one reader of the user's kubeconfig.** Nothing else watches the file, calls `clientcmd`, or builds a `rest.Config`. `Get()` returns the last snapshot plus whether a read has happened; `Subscribe()` is current-on-subscribe; `Close()` ends every subscription in the process, so only the app calls it.

`RESTConfig(contextName)` resolves one context to credentials and the pool's cache key. Three rules: one snapshot per call; only a config the loading rules produced (a hand-built `api.Config` yields CA paths that cannot open); the key excludes the context name, covers the static exec/auth-provider config and `proxy-url`, and length-prefixes every value. Two sentinels acted on, not logged: `ErrContextNotFound` (also for an empty name) and `ErrNotRead`. Resolution is not memoized.

Watches are pull-first: a 30-minute backstop tick under fsnotify and a poke subscription. **Keep the tick under any new push layer.** The service watches directories and follows symlinks; use `resolvePaths` and keep every path in one namespace (no `filepath.EvalSymlinks`, which rewrites every component and matches nothing on macOS).

## Resync broadcaster (`internal/poke`)

A leaf: wall-clock gap detector (15s tick, 2× factor) plus a `gochan/broadcast` hub. `Poke(src)` never blocks. A poke is a fan-out, never a cascade through spec counters or conditions. → [ADR: poke resync fan-out](../docs/adr/2026-08-09-poke-resync-fanout.md).

## GraphQL via gqlgen

`graph/schema.graphqls` is authoritative and also feeds the frontend's codegen. One file, sectioned by noun, `Query`/`Mutation`/`Subscription` at the end. After editing:

```sh
cd sidecar && go run github.com/99designs/gqlgen generate
```

Implement the panicking stubs it appends to `schema.resolvers.go`. **Never hand-edit `generated.go`/`models_gen.go`.** Re-run the frontend `pnpm codegen`. Renaming a schema file is a two-pass regen: delete the old `*.resolvers.go` between passes and check no body came through as `panic("not implemented")`.

## Patterns

- A type's methods live in the type's file; a helper belongs to whoever calls it, and only what more than one needs goes on the service.
- Pub/sub: unkeyed → `gochan` (`watch` for latest-value with a seed, `broadcast` for fan-out); keyed → `gobus` (`watch` delivers nothing until the next send; `conflate` for bursts). Never hand-roll a subscriber map.
- Work to do is a queue, not a bus: `internal/workqueue`, one `Queue` per job. `Done` is owed for every key taken.
- Subscription resolvers emit the current snapshot first, then deltas (`mapStream`), and honor `ctx.Done()`.
- Unexported functional options for test seams; exported `New` takes production knobs only.

## Tests & checks

- testify + `httptest`. Resolver tests stand up `graph.NewServer(&graph.Resolver{...})` + `POST /graphql`; lifecycle tests stand up `app.New(...)`. Filesystem via `t.TempDir()`.
- A fixture that needs a stored status writes it with `beehive.NewAdminClient`, never by registering a controller. A controller's own writes are asserted by calling `Reconcile` against a stubbed `ControllerClient`.
- White-box tests by default (`package foo`). External `package foo_test` only to pin a public contract, and say so.
- No magic sleeps (root `CLAUDE.md`). A cadence becomes a parameter whose production value is the constant.
- Wait on channels through `internal/testutil` (`Wait`, `Recv`, `RecvClosed`, `WaitClosed`); the one failsafe is `testutil.Timeout`. A negative assertion gets its own short window.
- A fake that notifies the test uses `testutil.Signal` (single-shot, idempotent `Fire`) or `testutil.Probe[T]` (repeating, non-blocking, drops oldest). Exception: a consumer doing edge detection needs a lossless fan-out (`internal/cloud`'s `fakeAuth`).
- `make test-go`, `make lint-go` (gofmt), `make vet-go`. Run `gofmt -w` before committing.

When you change the sidecar's wiring or conventions, update this file in the same change. When you change *why*, write an ADR.
