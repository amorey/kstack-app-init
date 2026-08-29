# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (parent gone).

The data dir (`--data-dir` / `KSTACK_DATA_DIR`) is **required** — `app.New` errors when empty; tests supply `t.TempDir()`. `<data-dir>/app.db` is owned by `internal/appdb` (one migration sequence; add app-level tables as numbered migrations in `appdb/migrations/`, never a second embed against the same file).

## Layout

Mirrors the kubetail layout: `main.go` is lifecycle only, `internal/app` is the composition root + routing, GraphQL lives in `graph/`. There is no `server` package.

- `main.go` — parse flags, bind socket, build `*app.App`, serve, drive graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close`).
- `internal/app/` — **composition root**: builds `poke.Service`, `kubeconfig.Service`, `clustersvc.New(...)`, `auth.Service`, `cloud.Service`; wires `graph.NewServer` + `grpcserver.NewServer`; multiplexes both onto one h2c handler (dispatcher keyed on `grpcserver.IsGRPCRequest`). `App.Start`/`App.Close` compose `App.parts` through `lifecycle.StartAll`/`CloseAll`: the slice is start order (poke → kubeconfig → cluster → cloud), and stop and close reverse it, so poke's hub closes **last**, after its subscribers drain. **kubeconfig before cluster is load-bearing** — `kubeconfig.Service.Start` reads synchronously, so every cluster reconcile observes a read config, and `app_test.go` pins it. Poke and cloud enter the slice as `lifecycle.StartFunc`. The two transports stay out of the slice — they shut down through `NotifyShutdown`/`DrainWithContext`, and `grpcServer.Stop()` runs first in `Close`.
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go` (gqlgen handler, bearer-token plumbing, SSE shutdown lifecycle). Resolver deps must be non-nil — tests wire fakes; degraded behavior lives inside the services, not behind nil-guards.
- `grpc/` — gRPC surface: `AuthService` (`StartLogin`/`Logout` unary; `AuthStateWatch` server-streaming, joins the drain WaitGroup) and `PokeService` (unary `Poke` → `poke.Poke(SourceHost)`). Committed protoc output in `grpc/authpb/`, `grpc/pokepb/`; regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here — it *is* the definition of a gRPC request.
- `internal/` — `ipc` (per-OS user-only endpoint), `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `kubeconfig` (the one reader of the user's kubeconfig), `drain`, `lifecycle` (the start/stop/close shape every level wears), `workqueue` (keyed work, delivered to one worker), `supervisor` (subjects holding a value, reconciled on a schedule derived from what the last run recorded; `clustersvc`'s `kubeconn` runs on it), `testutil` (test-only helpers, imported by no production code), plus the subsystems below.

## gRPC + GraphQL over one socket (h2c)

`internal/app` owns the topology (that two surfaces share one socket); `grpc/` owns the predicate. HTTP/1.1 GraphQL POST + SSE are untouched. An idle `AuthStateWatch` survives the 60s `IdleTimeout` via gRPC keepalive pings. The cluster surface is **GraphQL-only**. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

**Shutdown order** (from `main.go`): `app.NotifyShutdown()` (gRPC streams end on their serving context; each SSE request's context is cancelled per-request) → `srv.Shutdown` → `app.DrainWithContext` (waits both sub-servers' WaitGroups — essential for hijacked h2c gRPC streams `srv.Shutdown` can't see) → `stop(ctx)` → `app.Close()` (`grpcServer.Stop()`, then `lifecycle.CloseAll(a.parts)`). Traps: grpc-go's `GracefulStop` **panics** on the h2c path — `Stop` only runs after the drain; never cancel via `http.Server.BaseContext` — it would tear down the shared h2c connection carrying gRPC mid-stream.

## Cluster subsystem (`internal/clustersvc`)

The layout — five beehive kinds, three of which are GraphQL families:

```
internal/clustersvc/
  service.go          the whole API — Service + the four family interfaces — plus the
                      accessors, beehive bootstrap, and registerControllers
  clusters.go         ┐ one per family, implementing its interface and holding
  caches.go           │ everything else about that kind: its beehive shapes, the
  cachedkinds.go      │ record GraphQL binds, its *WatchFrame, its controller, and
                      │ the machinery that controller owns
  cacheddata.go       ┘ (no controller — the one family that isn't a beehive kind)
  clustersources.go   the ClusterSource kind: one discovery anchor per source variant.
                      A beehive kind with no GraphQL type behind it — internal
  triggers.go         the feed→wake bridge: maps a source's vocabulary onto the beehive
                      names the trigger declared at registration requeues
  shared.go           vocabulary every family reuses, the app's services as this
                      package sees them, and the two GraphQL scalars
  stream.go           Stream[T]
  internal/kubeconn/  the connections a cluster is talked to over, and what probing them
                      found — leases, the five probes, and the connection itself. A leaf
                      under internal/, so the compiler keeps it this package's own
  internal/kubesync/  what fills a cache: the arming seam (service.go) over kubeconn and
                      kubestore, one session per armed cache (session.go), the discovery
                      sweep (discovery.go) and one kind's sync (kinds.go). Same leaf rule
  internal/kubestore/  the on-disk cache: one SQLite file per ClusterCache behind a
                      refcounted manager (manager.go), the claim and the write path
                      (store.go), a file per table beside it (catalog.go is
                      kind_catalog; objects/events/status/rawcodec project the rows),
                      and the change ping bus every read re-reads on. Same leaf rule
```

**The interfaces are specified together; the kinds are implemented apart.** The naming and scoping
rules below are rules *across* the four, checked by eye — a violation shows when they read side by
side and hides when they don't. Everything else slices by kind, so one file teaches you one kind.
`registerControllers` stays whole in `service.go` for the same reason the interfaces do: its options
are the subsystem's concurrency and retry budget, which only reads as a budget in one place.

`New` opens the beehive store under `dataDir` and registers all four controllers; `Start` runs
beehive, then each controller's background work. **The boundary has no stubs left** — every method
on `Service` and its four families answers.

Produced: the `ClusterSource` anchor whose pass creates `Cluster` records,
`clusterController.Reconcile` observing what the kubeconfig says about each one
(`status.source.kubeconfig`), and that same pass creating the `ClusterCache` for the identity a
probe recorded. Served: the whole `Clusters()` family, the whole `Caches()` family, the whole
`CachedKinds()` family, and the kstack event log (`events.go`).

**The caches fill.** `clusterCacheController` arms `internal/kubesync` off the switch it already
computes and mirrors the cluster's `kind_catalog` into the `ClusterCachedKind` records it owns;
each record's own pass arms that kind's sync. So **kubesync decides what EXISTS and the records
decide what is MIRRORED** — the same shape as the rest of the chain, where the pool finds a
serverUID, the cluster pass creates the cache, and the cache pass arms the sweep.

- **The desired set comes off DISK, never off the seam.** `OpenExisting` → `KindsWithFingerprint`
  → `Release`, rows and fingerprint in one read transaction. Three properties carry the pass:
  `OpenExisting` never creates a file, so a pass before any sweep prunes nothing; **the
  fingerprint's absence is the "never swept" bit**, which is not the same as a cluster that serves
  nothing, and only the first of those may delete records; and reading both together is what stops
  a stale fingerprint passing its check beside a clear's empty table.
- **A row with no record is a `CreateOrUpdate`**, not the `GetOrCreate` that creates a cache: a
  kind's spec carries data outside its name (the singular, the scope), so a renamed or re-scoped
  kind converges in place. **A record with no row is a `Delete`** — marked, not collected, since
  the record's own pass clears its rows first. **A record whose catalog fields all match is
  written NOT AT ALL** (`sameCatalogFields`), which is both what keeps a sweep off a
  hundred-kind cache's write path every pass and what makes the ownership split below hold.
- **The catalog owns four spec fields; the user owns `Paused`.** Nothing that writes on a
  schedule may write it: the desired spec is built from the catalog row alone, so it carries
  `Paused`'s zero value, and a pass that wrote it whole would un-pause every kind within one
  discovery interval — silently, since nothing else moves. The one write that does touch a
  stored record (a catalog change converging in place) carries `Paused` forward under
  `deps.kindSpecMu`, which `SetSyncEnabled` takes too — and it **rereads the record** for it
  rather than trusting the pass's own list, which ran before the lock was taken.
- **The field is `Paused`, and the inversion is load-bearing.** beehive stores a spec as JSON
  and decodes it back, so a key absent from every existing record decodes to `false` — a
  positive `Enabled` would read as disabled fleet-wide on the upgrade that shipped it, and the
  no-op rule above means nothing would ever rewrite the record to repair it. The wire keeps the
  positive form (`ClusterCachedKindSpec.syncEnabled`, one negation in its resolver).
- **Pause stops the sync and KEEPS the rows.** `clusterCachedKindController` calls `ForgetKind`
  and no `clearKindRows` — that call is what makes a deletion a deletion. Level-triggered, so
  it fires every pass while the kind is paused; the whole chain is idempotent. The cached
  objects stay listed and readable throughout, and a resume reconciles into them (off the
  cookie if the server still serves that resourceVersion, otherwise by relisting).
- **Neither controller writes a condition.** The verdict is the gauge's; a stored one would serve
  a dead process's answer until the passes caught up.
- **`ForgetCache` before `Manager.Remove`, and `ForgetKind` before `Store.ClearKind`** — both
  return only once nothing can still write through what the next line deletes. A kind whose cache
  is already gone skips both: its registration went with the cache.

**Two triggers carry the news**, one per registration, because a trigger wakes a record for every
value its feed carries and one feed carrying both would wake a cache for each of its hundreds of
kinds. The cache's is a `WithTriggerByID` over cache ids — the seam speaks the number the store
assigned. The kind's is a `WithTriggerByName` mapping a `KindKey` onto
`ClusterCachedKindName(cacheID, apiVersion, resource)`, by name because that record's id is the
store's to assign where its name is derivable — which is what keeps a record id out of kubesync.
`trigger[T, W]` is generic over the address for exactly this: each source holds a name or an id and
not the other, and converting between them is a store read a translation must not need.

**Three event timelines, each an (ObjectID, category) pair** — the axis beehive already bounds
retention on (`maxEventRuns`): `Cluster`/`connection` for reachability and identity,
`ClusterCache`/`discovery` for sweep verdicts, and `ClusterCachedKind`/`sync` for one kind's
transitions. Every pass writes unconditionally, because repeating a run's
`(Category, Type, Reason)` extends that run rather than appending — so a flapping kind costs one
row per transition and a settled one costs nothing. **A session suspended for `NoConnection` writes
no discovery event**: that fact is the cluster's, already on its own timeline, and logging it per
cache is the same news twice.

**The read side is one path for all three kinds** (`events.go`): `ListEvents` and `WatchEvents`
take an `ObjectID` and hand it to `clusterClient` — beehive reads a timeline by id alone, so the
client's kind picks only which registration is checked, never which rows are served. Three rules
the code turns on:

- **A nil `category` adds no option.** `beehive.WithEventCategory("")` selects the *default*
  timeline, which is a timeline of its own; every write here carries `connection`, `discovery` or
  `sync`, so the empty string would answer nothing rather than everything. A nil `limit` is
  deliberately unbounded — `maxEventRuns` retention already caps each `(object, category)` pair.
- **`WatchEvents` is `EventFrameRun` frames, one `EventFrameBookmark`, then the tail**, snapshot
  forwarded newest-first because the client upserts by `Event.ID`. The bookmark lands even for an
  empty timeline — it is what tells "no events" from "still arriving". The `beehive.WatchEvents`
  call is synchronous, ahead of `NewStream`, so a refused subscribe is an error the resolver
  answers with rather than a terminal frame on a stream the client already holds.
- **`terminalErr` drops `beehive.ErrNotFound` and forwards the rest.** A record collected under a
  live watch takes its log with it, so beehive ends the stream `ErrNotFound` — but the deletion is
  the answer, and forwarding it would raise `watchFailed` once per open kind timeline when a user
  clears a cache. `ErrWatchTooOld` stays reported: runs were lost, and a resubscribe is what makes
  the client correct. The asymmetry to know before testing it: an id that NEVER held a row does not
  fail — it bookmarks an empty snapshot and waits — so a bogus id proves nothing about this path.

`Event.id` reuses the `ObjectID` scalar for its wire form only. Event runs come from beehive's own
`EventID` sequence, so the scalar's uniqueness sentence is scoped to objects and an event id is
unique within one timeline; never hand one where an object id is expected.

**The store is one SQLite file per cache behind a refcounted `Manager`** (`internal/kubestore`),
cleared by deleting the file and removed with the record it is named for. A `Store` is a *claim*
on one cache: `Manager.OpenOrCreate` takes one, `Store.Release` gives it back, and the file is
resolved per call — so a `Clear` swaps it under live holders, and a `Remove` leaves them answering
`ErrClosed` rather than writing into an unlinked inode.
→ [ADR: one store per cache](../docs/adr/2026-08-26-cache-store-per-cache.md). Three traps live in
its write path:

- **A relist reconciles by mark and sweep**, on `updated_at` — never on the `generation` column,
  which is the object's own `metadata.generation`. Every write takes a **strictly increasing**
  stamp (`Store.stamp`): the clock has millisecond resolution, so a relist running in the same
  tick as the rows it supersedes would otherwise keep every one of them.
- **Core `v1` events are written to the `events` table**, routed by api version and plural rather
  than by the Kind name — any group may serve a Kind called `Event`, and a CRD's rows are ordinary
  objects. **Nothing ages them out and nothing bounds the read**: a cache holds what the server
  holds, for events as for every other kind, and `Store.Events` serves all of it newest-first. A
  row leaves only when the server says it did.
- **Object bodies are sanitized on the way in** — `managedFields` and the kubectl last-applied
  annotation stripped, Secret values redacted (by the *body's* own kind, so how a collection was
  addressed cannot bypass it) — which is what lets a read serve `raw_json` verbatim.

**One janitor per open file**, started in `openFile` and stopped in `(*file).close` — so its
lifetime is the file's, and a `Clear`'s fresh file gets one like any other (`openFile` has two
call sites, and the reopen mid-clear is the one a start at the call site misses). It trims
`status_history` past `Retention.StatusHistoryTTL`, then hands free pages back with
`PRAGMA incremental_vacuum`. Three rules hold it together:

- **Gate on `PRAGMA freelist_count`, never on what the sweep itself deleted.** The writers that
  actually free pages — a relist's prune, `ClearKind`, a `Remove` — do not vacuum, so a
  rows-deleted gate would strand the file at its high-water mark and `Stats.Bytes` would report
  the worst the cache has ever been.
- **The vacuum is bounded** (`vacuumPagesPerSweep`), because a cache has one writer and the
  freelist is biggest right after a relist, when blocking it hurts most. A backlog drains over
  the following sweeps. The `status_history` delete is not bounded: one statement over a table
  that is small by construction.
- **Nothing waits under `m.mu`.** The stop is a cancel, and the sweep runs on the janitor's own
  context, so it aborts mid-statement — all three exits (`Clear`, `Remove`, `Manager.Close`) hold
  the lock across the close, and a wait there would stall `Stats` behind a vacuum. For the same
  reason the first sweep runs inside the goroutine rather than inline in `openFile`.

`NewManager(dir, Retention{...})` is the whole plumbing; production passes `DefaultRetention` and
a zero `Interval` runs no janitor, which is what a test about anything else opens with.

**The store owns the change signal, and it is a coalesced ping, not a row delta.** Writers notify
after commit, keyed per kind (`objects/<apiVersion>/<resource>`) or on the events bus; a reader
subscribes first, snapshots, and re-reads and diffs by UID on each ping. Closing the store closes
the bus, which is what ends a live watch when a cache is cleared.
→ [ADR: the store's ping bus](../docs/adr/2026-08-26-store-change-ping-bus.md).

**Nothing on the read side creates a file.** `Manager` keeps only what is about *which* file and
its life — `OpenOrCreate` (writers), `OpenExisting` (**the door to a cache's contents**: a read, or
a per-kind clear; claims the file, answers `ok=false` when there is none), `Subscribe` (borrow the
change feed of a file someone else holds open, no claim), `Clear`, `Remove`, `Stats`. **Every
`Store` is a claim** — the two claimless paths hand back a feed and a measurement, not a store. Everything about a cache's *contents* is on the `Store` you
opened. A read that created would resurrect the file as an orphan nothing can name again, for a
reader that reconnected between a cache being marked for deletion and its teardown pass.
`Manager.Stats` measures without a claim at all — file size plus the counts, read through the open
file or a **read-only** open, so a paused cache still reports what it holds. `WatchOpen` is the
other half of the bind: a watch that found no file waits on it rather than polling, since nothing
else would say the cache came up.

**Reads ride their own pool.** Each `file` holds a reader pool (`readerPoolSize`) beside the
one-connection writer, so a watch re-reading on every ping never queues behind a write. Distinct
from the manager's read-only open, which measures a **closed** cache's file per call; the pool
serves an **open** one for the file's life.

**A cached-data watch pings, re-reads, and diffs — it never carries a row delta.** One loop
(`cacheddatawatch.go`) serves all three: subscribe first, snapshot as `Added` frames, one `Bookmark`,
then per debounced burst of pings re-read and diff by id against the previous snapshot. A re-read
is always full current state, so an early or late ping costs one idempotent read rather than a
wrong frame — which is why the bus carries no payload and needs no coupling to the store's
transactions. Four rules carry it:

- **The debounce is load-bearing.** `conflate` merges only what a reader has not yet taken, so a
  loop that drains promptly gets one wake per *write*. Three constants, since the streams do not
  carry the same load: kinds and objects 250ms, events 500ms — the highest-volume stream and the
  one that storms.
- **A failed re-read retries in place** (`dataRetryInterval`) rather than ending the stream. The
  bus is keyed by what was written, so a kind nobody writes to may not ping for hours and one
  transient error would leave the client's table empty until something else moved.
- **A cache that goes away ends the watch CLEANLY** — `Stream.Err()` nil. A clear is a user
  pressing a button; a non-nil `Err` is filed as a watch failure and reaches the client as an error
  per open watch, plus a suppressed backoff reset. The reconnect re-snapshots, so silence costs
  nothing. `Err` is for a read that is actually broken.
- **The diff takes a `changed` func, not a `comparable` constraint.** `ObjectRow` holds the body as
  stored (`CompressedJSON`) and cannot be compared with `==`. Objects diff on
  `(uid, resourceVersion)` — the server moves it on every write — so only a row that becomes a
  frame is ever decompressed; kinds and events compare their whole row.

**A read claims the file and never creates one.** `OpenExisting` first, `WatchOpen` if the cache
has no file at all, and the `Bookmark` goes out either way: a cache with no file is empty, not
pending, and rows arriving later diff in as ordinary `Added` frames. **Claiming, not borrowing, is
load-bearing** — nothing holds an idle cache open (the workers release on pause and on shutdown),
so a read bound only to an already-open file would show an empty nav over a paused cache's rows,
and over a full cache on every restart until the first worker arms. The claim is **bound to the
file it opened**, so a `Clear`'s swap answers `ErrClosed` rather than silently re-reading the fresh
empty one — which would emit a `Deleted` for every row the client holds — and the watch gives back
both the claim and its subscription whichever way it ends.

**A file that will not open is a fault, not an empty cache.** Only `ErrRemoved`/`ErrClosed` — the
cache went away — degrade to empty for a read and to a clean end for a watch. Every other open
failure (corrupt file, permission, a migration that would not run) is reported: `ListKinds` returns
it, and the watch sets `Stream.Err()`. Reading one as "no file yet" is the trap, because `WatchOpen`
fires when a file is **created** — a cache whose file is already there would park on a signal that
never comes, leaving a silently empty table.
→ [ADR: cached-data reads](../docs/adr/2026-08-26-cached-data-read-loop.md).

**The gauges are read-side folds**, all three on the cadence, because what they carry — a file's
size, a row count, a freshness stamp — moves while every record under them sits still.
`Caches().WatchStats` measures the file and the trigger-maintained counts, re-emitting on the
store's ping as well. `Caches().WatchHealth` folds each live cache: `Paused` off the switch the
records carry (`cacheSyncEnabled`), otherwise every `ClusterCachedKind` record's own
`GetKindState`. `Caches().WatchSyncStatus(clusterID, cacheID)` expands ONE cache instead — the
discovery verdict and a row per mirrored kind, with each kind's own reason and its row count off
the store. A cache being collected is skipped. None of the three emits before its first
measurement, and none carries a `Bookmark`.

**A paused kind's verdict comes from its record, never from kubesync.** A paused kind is
forgotten, so the seam has nothing to say about it — and it deliberately does not know why it was
not asked to sync something. So `Paused` is resolved from `Spec.Paused` **ahead of** every
`GetKindState` call: in `kindVerdict` (the timeline), in `readSyncStatus` (the panel's row), and at
the top of `readCacheHealth`'s loop. That last one is a skip, not a filter applied after the fold —
reading a paused kind's state would report it unanswered, and one unanswered kind pins the whole
cache at `Connecting` forever. A paused kind still counts in `TotalKinds` (a wire field with three
consumers) and is tallied apart in `PausedKinds`, which `sameHealth` must compare or pausing a kind
on an otherwise-idle healthy cache publishes nothing.

**A kind that has not answered is not an offender.** `GetKindState` reporting false is "nothing
observed yet" — a cache still starting, a clear in progress — so the health fold counts it neither
as unhealthy nor as proof, and the cache reads `Connecting` until every kind has spoken. That rule
is also what keeps a clear in progress from reading as a cache that stopped syncing, which is why
the clear needs no flag of its own. `LastLiveAt` is the OLDEST proof across the kinds and **absent
while any kind has none**: a cache is only as verified as its least proven watch.

**A verdict comes from the records, not from silence.** The health gauge is latest-value with no
departure frame, so a cache the pass skipped would read as its last verdict — or, for a subscriber
that arrived after it went quiet, as no verdict ever. Enumerating the records each pass is what
keeps that honest.

**`Caches().Clear` is `Manager.Clear`; `CachedKinds().Clear` is `Store.ClearKind`** over the
kind's own `Kind`, read off the record rather than out of `kind_catalog`. Both run inside
kubesync's `RunWithCacheSyncStopped`/`RunWithKindSyncStopped`, so the workers writing through the
file are down for the whole swap. A **teardown** is `Manager.Remove`, which tombstones the id so a later claim is
refused with `ErrRemoved`.

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

**One pump serves every record watch** — `deltaWatch[Spec, Status, Frame]` (`stream.go`), whose
`streamOne`/`streamList` cover the single-object and list shapes. A kind supplies only what is its own:
a `frame` projection, a `departed` builder, and its `bookmark` value (`clusterWatch`, `cacheWatch`,
`kindWatch`). Add a kind by writing those three, never a fourth pump — the bookmark discipline is
a protocol rule, and a per-kind copy is a place for it to be got wrong. The pump's own rules are
tested once, in `stream_test.go` over a stand-in kind; a kind's tests pin its projection and its
departure.

**A controller owns its kind's machinery**, and `service` holds the controllers only to drive their
lifecycle. None has any yet — all four embed `lifecycle.None` — but the leaves a controller grows
land there rather than on `service`, or the composition root accumulates every kind's detail.
`registerControllers` builds and registers all four, returning them in registration order. All register with
`startupPass` (`WithStartupFullPass(true)`): each owns state a restart invalidates and the store
reads as settled, since the generation was observed by a process that is gone. **`ClusterSource`
also registers `sourceResync`** (`WithIndividualPassInterval(clusterSourceResyncInterval)`),
the poll its correctness rests on: it reads a file the store cannot see, so a lost trigger poke is
a change nothing else would report. **`Cluster` takes `clusterResync`** for the same reason
— what its probe reports is a remote server's, so nothing in the store moves when the answer does.
The other kinds are woken by a spec write or a dependency edge. **A resync is owed by a fold whose
answer the store cannot see move**, so the new seam's records will likely take one back.
→ [ADR: beehive control plane](../docs/adr/2026-08-09-beehive-control-plane.md).

**A context is not an identity, and identity lives on the connection.** Re-point a context at
another cluster and the pool hands out whatever now answers, while the only thing that disarms a
superseded cache's work is a pause flip several reconciles downstream — and the pool wakes every
subject over a context whose identity moved, so that stale work is the *first* thing to run
against the new server. `Connection` therefore carries a **set-once `serverUID`, stamped by
`serverUIDProbe` when it reads one over that connection**, and `Lease.ConnFor(ctx, serverUID)`
answers from it.

**Never correlate a connection with `State.ServerUID`** — that is the trap this shape exists to
close, and reading both from one snapshot does not close it. `serverUID` is its own probe,
*queued* by a committed connection rather than applied by it, so the supervisor legitimately holds
`{conn: B, serverUID: "uid-A"}` for a dispatch plus a round-trip. Asking the connection who it
reached has one writer and nothing to pair. A connection nobody has identified answers
`("", false)` and is refused; the connection is resolved first, so a cluster nothing reached still
reports the outage. The stamp is unconditional while the commit stays change-gated — the commit
says the *context's* identity moved, the stamp says *this connection* has been identified, and
gating the stamp would leave every rebuilt connection to an unchanged cluster unstamped.

**A second, different UID over one connection makes it vouch for nobody.** That is a server
replaced behind an endpoint and credentials that never moved, so no connection is rebuilt and the
probe reads a new uid over the old stamp. The stamp is never overwritten — keeping it would go on
authorizing the old cluster's subjects against the replacement, and adopting the new one would let
a connection that already answered as something else vouch for what answers now — so the conflict
is recorded and `ConnFor` refuses everyone. **The conflict then rebuilds the connection**, so the
stall is a window rather than permanent: `connectionProbe.Run` rebuilds on a conflicted connection
as well as on a changed fingerprint, and the pass that records the conflict wakes it.
→ [ADR: connection-carried identity](../docs/adr/2026-08-25-connection-carried-identity.md),
[ADR: identity-driven retirement](../docs/adr/2026-08-27-identity-driven-retirement.md).

**A status write is unconditional.** Beehive compares what a pass writes against the status it handed
that pass and reaches the store only for a difference, so an observation that moved nothing costs a
marshal rather than a transaction — a guard in the pass would only duplicate it, and would drift from
what the pass actually writes.

**A pass returns a verdict, never an error**: `beehive.Settled()` (the pass observed the object's
current generation, which beehive records), `beehive.Unsettled()` (a real pass that is not caught
up — the deferred kubeconfig read), or `beehive.Fail(err)` (the backoff ladder). `.RequeueAfter(d)`
on the first two schedules the next pass, for a wait this pass knows the length of — the startup
window's 1s retry. **A cadence a kind depends on belongs at registration instead**, where no return
path can forget it. **A no-op pass still settles**: unsettled, every object of the kind comes back
on the owed pass's cadence, forever.

**Shared dependencies travel in `deps`** — one beehive client per kind, the process-wide services
(`kubeconfig`, `kubeconn`, `kubestore`, `kubesync`, `poke`), built once by
`newDeps(bh, kubeconfigSvc, kubeconnSvc, kubestoreMgr, kubesyncSvc, pokeSvc)` and **embedded** by `service` and by each
controller, so a family reads `a.s.cacheClient` and a controller reads `c.cacheClient`. The `Client`
suffix is load-bearing: the fields are promoted into both, and `a.s.cacheClient` must not read like
the `Caches` family it is reached through. **A new kind or a new
shared service is a field, never another constructor parameter** — the alternative threads each one
through the constructors that don't use it, which is what the parameter list was doing at two kinds.
What stays an argument is a single owner's own *configuration*, which nothing has today —
every controller takes `deps` alone. Tests build the same struct (`newTestDeps` /
`newRunningDeps` / `newRunningRegisteredDeps` in `testutil_test.go`) rather than assembling clients
of their own: the owner edges need every kind in one store, which beehive enforces. The three differ
in what beehive is doing behind them — registered but stopped, running with no controller, or both.
**Both is what an event watch needs** (`WatchEvents` refuses an unregistered kind, and only a
running beehive collects), and it is the one where the reconcilers write runs of their own, so an
assertion over it scopes itself to a category no controller writes.

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
`clusterController.Reconcile` creates the `ClusterCache` (via `ensureClusterCache`), and the same
shape carries on down the chain. Distinct from a
discovery pass, which decides which objects exist *including when there are none*, and so needs an
anchor object of its own to run against. **The writes live in the
child kind's file**, not the parent's: the name, spec and owner edge are that kind's vocabulary, and
the parent supplies only the policy — when, and with which switch. A teardown stops the chain: a
pass whose object, or whose owner, is deletion-pending or already collected writes nothing, since the
cascade is coming for the subtree either way.

**Each of those writes is one call, with no read in front of it.** A relay is
`CreateOrUpdate(name, spec, WithOwner(parent))` — resolve and write in one transaction; a
create-only child whose spec *is* its identity (`ClusterCache`) is `GetOrCreate`. Both refuse a
deletion-pending row rather than rewriting it: `GetOrCreate` returns it as-is, `CreateOrUpdate`
returns `ErrDeletionPending`, which the caller treats as "nothing to relay and nothing to depend on"
rather than as a failed pass. **Don't put a `GetByName` probe in front of one to keep the converged
case off the write path** — beehive measured it, and the transaction it saves costs more than it
saves below roughly four converged writes per changed one. What the pass must still hold to is a
spec that marshals identically when nothing moved, since beehive's no-op suppression is what keeps a
converged relay from waking every dependent.

**The probe rides the `Cluster` pass.** `clusterController.Reconcile` observes the kubeconfig, reads
the cluster's `kubeconn` claim for what connecting with that context's credentials revealed, and
folds both into one grouped write (`Within`) so a watcher never sees the status without the
condition explaining it. **No pass dials**: a claim reports what its last probe found, and the
dialing stays off every reconcile goroutine. `clusterResync` re-runs each record's pass on its own
timer, and is the only thing that does: the `Cluster` kind declares no trigger, so nothing yet makes
a landed probe prompt.

**The claim is the pass's other job.** `ensureLease`/`dropLease` hold one `kubeconn.Lease` per
cluster in `clusterLeases`, keyed by `ClusterID`. A record's context is fixed in its name, so a
held claim stays the right one for the record's life; credentials moving under that context is the
pool's to notice, since it is what resolves. Holding is what arms the probe,
so a disabled, tombstoned, or non-kubeconfig record is dropped and costs no dial. These are the
controller's own claims: a boundary caller takes its own, since the pool refcounts and a log tail
ending must not stop this cluster being probed.

**A probe landing wakes the context's cluster.** `newKubeconnTrigger` is the Cluster kind's
`WithTriggerByName` feed, the same three-line `trigger[T]` shape as `newKubeconfigTrigger`: it
reads `kubeconn.Service.Subscribe()` — a `gobus/conflate` bus keyed by **context name** — and maps
each key through `KubeconfigName`. `conflate` and not `watch.WatchAcross`, which collapses a burst
to whichever key landed last and would silently drop every other cluster's wake. The controller
holds nothing and knows nothing about waking: it takes claims, and the trigger is registered beside
the kubeconfig one.

The pool publishes the same send two ways — per claim on `WatchState`, per context on `Subscribe`.
The fleet feed is for a reader whose reaction to any change is the same ("re-read it"); a holder
that cares about one claim watches that claim.

**The pass reconciles the claim, then observes.** `reconcileConnection` is the one place that
touches the pool — the claim is taken while the record asks to be connected and dropped otherwise
— and it returns a `connectionFinding`: `observed`, the claim's `*kubeconn.State`, plus
`Connected`'s reason. `observed` is **nil when there is no claim**, which is the three findings this
package makes before the pool is involved: the record is switched off, its context left the file,
or its credentials will not resolve. The server exists in all three; what is missing is our
observation of it. `inactive` marks the first two and takes precedence, since the pool cannot see a
choice the user made. How far a probe got is never copied out — the verdicts read `State.Phase()`,
so every lease holder answers pending-versus-failed the same way. The verdicts are then pure
functions of that finding, so the claim's lifetime happens once while each condition reads the same
value. A record from a source with no credentials to resolve gets **no conditions at all**, rather
than verdicts no probe produced.

**Two conditions with two subjects**: `observeConnected` (did we reach it) and `observeIdentified`
(could these credentials name it, from the `kube-system` UID). Each maps that finding to its own
answer top to bottom — deliberately two switches rather than one shared verdict, because the
aspects fail independently and a helper forcing them to agree is one someone splits later under
pressure. Reaching a server needs no authorization and naming it does, so a namespace-scoped user
gets `Connected=True` with `Identified=False/UIDUnreadable`, which is the **only** thing that
explains a healthy-looking cluster that never gets a cache (`ensureCache` skips a record with no
UID).

**The bar for a condition is a distinct remedy.** `Connected` points at the network, the kubeconfig,
or the credentials; `Identified` points at an RBAC grant. The server's own readiness is not one:
nothing here gates on it, no user action follows from it, and a lease holder that wants it reads
`State.Readiness` directly.

`Connected` carries the finding's own reason: `Inactive` when the cluster is switched off,
`Connecting` until a probe lands, and `ProbeFailed` for everything the probe found short of
reaching the server — carrying that attempt's message, so a context that left the kubeconfig, a
file that will not resolve, and a server that would not answer are one reason and three messages.
A broken file is reported on the record rather than failing the pass, since beehive's backoff
cannot fix a file. `Inactive` is the pass's own finding, made before the pool is involved; the
other two read the claim's `State` — `Acquire` itself never refuses. The other two derive their own: `NoConnection` where a probe never got to the server, since
neither readiness nor identity is a fact about a server nothing reached.

`foldState` copies what the pool knows into `status.server` (`uid`, `version`, `endpoint`) and
`status.principal` (`username`, `groups` — sorted, so a re-ordered read is not a change). It
decides no retention of its own — an `Observation` already keeps its last answer through a failure
— so a probe that has never answered (`Known()` is false) leaves its field alone, which is what
stops a first pass from clearing the UID a live cache is named for. **The record's copy is the
durable one**: a restart empties the pool's.

**Only the values, never the timing.** `Reason`, `Latency`, `Failures`, and `NextAttempt` stay off
the record: they move every cycle, and a status that moves re-emits to every watcher. A reader that
wants them takes a lease. This is the same trap as the paragraph below — the record has no
timestamp field at all, deliberately.

**Its steady state must be silent.** A cluster record is what every watcher streams, so the pass
reports only what it observed and lets beehive's no-op suppression (equal status bytes, unchanged
conditions) do the rest. A timestamp in that status — or in a condition the pass writes
unconditionally — would re-emit the record on every probe, which is the same trap
`ClusterSourceStatus` carries.

### The connection pool (`internal/clustersvc/internal/kubeconn`)

**A cluster is the only way to address a connection**, so the pool sits behind this boundary and
nothing outside it can import one. → [ADR: connections are addressed by
ClusterID](../docs/adr/2026-08-22-connections-addressed-by-cluster-id.md).

**It hands out leases and reports what probing the server behind one found.**
`Acquire(contextName)` never fails and never waits — a context the file does not name yet is
claimable, because it may name it later and the claim is how the holder finds out. `Lease` is
`Conn` / `ConnFor` / `State` / `WatchState` / `Departed` / `Release`. **`Conn` never dials**: it hands out what
the connection probe built, or `ErrNoConnection` for a context that resolves to nothing — a
connection whose last probe *failed* is still handed out, since only the holder can tell a revoked
credential from a control plane mid-restart. `Retry(contextName)` wakes **all five** probes on a
claimed context: a connection that is already up commits nothing, so waking it alone would leave a
probe that failed on its own — a forbidden `kube-system` read — sitting on the answer the user just
fixed. A context nobody claims is untracked, so it does nothing.

**A `Connection` carries the clients built over one set of credentials** — `Dynamic`, `HTTPClient`,
and `Discovery` — sharing one pool, which under HTTP/2 is one TCP connection to that API server.
`Discovery` is the exception that proves the rule: client-go's discovery calls take **no context**,
so it gets its own `http.Client` carrying a timeout instead. The pool is still the shared one, since
client-go caches transports by TLS config — but the timeout must not ride the shared client, where
every other caller (which bounds itself with a context) would inherit it.

**Every non-watch request carries an idle-read bound** (`idletimeout.go`), installed on the
connection's config beside the QPS/burst tuning. HTTP/2's `READ_IDLE_TIMEOUT` is connection-level
keepalive — it detects a dead peer, not a live one that has stopped sending, which is what a
wedged LIST is. It matters because a kind sync holds its start slot until its watch is open, past
the cold list, so one hung LIST costs a permanent fraction of the fleet's start capacity.

- **Progress, never a deadline.** Headers and every body chunk count, so a slow but streaming LIST
  of a large collection always completes. Detection is coarse — the watchdog ticks once per window,
  so idle-to-cancel lands in `[timeout, 2*timeout]` — and the timer re-arms only from inside its
  own callback, never from the read path, so a read landing as the timer fires cannot race the
  cancel.
- **Watches are exempt**, matched by `watch=true` as a substring of `RawQuery`. A healthy watch is
  legitimately silent between bookmarks, so a bound would kill it; `RetryWatcher` and the HTTP/2
  keepalive govern one instead.
- **A cancelled request reports `ErrIdleTimeout`**, not the transport's bare `context canceled` —
  that string is what a stalled cold list ends its run with, as the `SyncFailed` message a user
  reads. The caller's own cancel still reports itself.

**The boundary in front of it is `AcquireConnection`/`RetryConnection`/`Clusters().WatchSchedule`**, all resolving the
`ClusterID` to its context through one gate: `ErrNotFound` for an id naming nothing, and
`ErrNotConnectable` for a record that is disabled, awaiting deletion, or from a source carrying no
credentials. The gate is the record's own state, never the cluster's — whether the server answers
is the probe's to report, so an unreachable cluster is claimed and retried like any other. The
claim handed back is the caller's own, refcounted alongside the one `clusterController` holds, so
releasing it never stops the cluster being probed.

**`clusterScheduleWatch` reads the pool's cadence, never beehive's.** A cluster reconcile is never
requeued to retry a connection — the probes carry their own backoff and a pass only folds what they
found — so the record's beehive schedule is empty and a countdown read off it would never move.
`Clusters().WatchSchedule` claims the context for the life of the stream and projects
`Lease.State()` + `WatchState()`: `nextRequeueAt` is the **connection probe's** next run (null while
it is suspended), `probing` is that run in flight. The connection alone, of the five — it is what
"when do we next try to reach this cluster" means and the only one `clusterConnectionRetry` acts
on; the other four run on their own clocks (readiness 30s, the rest 5-10m), so folding them in
would count down to whichever happened to be due next. It emits nothing until the
first pass lands, since a fresh claim's zero state is not "nothing is scheduled". `probing` is
asserted from the run, never inferred from a countdown that has run out — but the supervisor publishes
only on a pass, so the in-flight window is not observable yet; see `TODO.md`.

**A claim outlives what it is a claim on.** The file can stop naming a context while a holder
still holds it, and the entry stays — only releasing drops one. An **unread** kubeconfig names
nothing and is deliberately not a departure: saying so would report every context gone for as long
as the first read takes. `stateHub` is published before `signalHub`, so a reader the signal wakes
finds the value already there.

#### The probes (`probe.go`) over the supervisor (`internal/supervisor`)

**The scheduling machinery is `sidecar/internal/supervisor`** — a reusable supervisor (a work queue, a
level-triggered pass, a schedule derived from recorded state) that knows nothing about
kube-contexts.

**It runs two kinds of thing.** A **job** runs, returns, and is quiet until it is due again; a
**worker** starts, blocks until it is stopped or it dies, and reports while it runs. The rule of
thumb: work with a natural end is a job, work that would need a goroutine outliving the call is a
worker. Every probe and every discovery read is a job; `kubesync`'s kind sync is the one worker.
→ [ADR: jobs and workers](../docs/adr/2026-08-28-jobs-and-workers.md).

A probe is a job whose value is an observation, registered with
`supervisor.RegisterJob(e, name, p, opts...)` — the same shape as a beehive controller, with `T` inferred
from the instance — and `T` is its observable's value type. **The registration name is the
probe's whole public identity**: the edge options, `Wake`, `Restart`, and every read take one, and
`RegisterJob` returns nothing. `kubeconn`
keeps what is asked and what the answers mean: `probe.go` is `registerProbes` — five
registrations kept side by side on purpose, since the set's rules are checked by eye — plus the
probe structs; `service.go` is leases and publishing. → [ADR: the supervisor's
extraction](../docs/adr/2026-08-24-probe-engine.md).

**A run's `Result` is its schedule** — `Succeeded` waits out the interval, `Fail` climbs the
backoff ladder, `Suspend` and `Skip` wait for a `Wake` — so no domain rule lives in the scheduler.
What the supervisor DOES with each verdict differs by kind, which is the whole of the split.
**`WithStartConcurrency(n)` bounds what is STARTING**, across every subject: a job is starting for
its whole run, a worker only until `Ready`, so eight is eight cold lists however many streams are
already up.
`Succeeded().RequeueAfter(d)` asks for the next run sooner, for a wait the run knows the length
of — beehive's spelling, for the same ask. **Unlike beehive's it can only bring a run forward**,
since the supervisor takes it when it is positive and shorter than the registered interval: a probe's
registration bounds requests against someone else's cluster, so forget the ask and a subject is
slower, never wrong. A zero is no ask, not "immediately". Read on a succeeded result and nowhere
else — `Fail` owns the ladder and `Suspend` schedules nothing.

**The supervisor hands back every value it stops holding.** A committed value can own something —
a connection, a file, a goroutine a run started — and one the supervisor is no longer holding is
one nothing else can reach to release: a commit refused because the subject was removed mid-run, a
run that concluded `Skip`, one that returned the zero `Result`, one that panicked, and the standing
value of a subject dropped by `Remove` or `Close`. **A commit is the exception**: the value it
replaces is not handed back, since a commit often carries the last one's holdings forward — a
struct value with one field moved keeps the connection inside it — so a run drops what it is
really dropping itself. A reconciler implementing `Discard(T)` is handed
it (`kubeconn`'s connection probe retires the connection); one that does not is unaffected.
**`Discard` runs outside the supervisor's lock**, because one that joins a goroutine can wait on an
exit that calls `Wake`. **A worker gets none of this** — its commits are live, so a refused one is
simply dropped, and its value must own nothing.

**A worker's own rules**, all three of them the supervisor's:

- **`Ready` is the worker saying its STARTING phase is over** — the expensive part is done and it
  is now doing the thing. For a stream that is the watch being open, never its first frame: a body
  that waits for its source to say something holds a start slot for as long as that source stays
  quiet, and bookmarks are advisory, so the first few kinds would keep the rest of a cache from
  ever listing. It releases the slot, stops the startup timer, stamps the attempt and opens a
  healthy stretch, so dependents are scheduled. **It does not clear the failure streak** — a run
  finishing cleanly is what does, since starting is not proof and a source that accepts every
  start and drops it calls `Ready` on each one.
- **A worker that never calls it is recorded `NeverReady`** — a **failure**, paced by the ladder —
  in the two cases where what it returned would otherwise leave it unpaced: it says it finished
  cleanly, or the startup timer ended it. A `Fail` keeps its own reason up the same ladder, and a
  `Suspend` or a `Skip` the worker chose for itself is it parking at a gate, never a failure.
- **A stop records nothing.** `Restart`, `Remove` and `Close` ask for the end, so it is not the
  body's doing: the last record and the failure streak stand. A resume poke restarts every kind on
  a cache at once, and one that reset the streak would have the whole cache retry a struggling
  server at the base delay. **The startup timeout is not a stop** — it cancels the same context, but
  the run is recorded, and recorded `NeverReady` whatever the body then returns: a worker reading
  its cancelled context reports the cancel rather than a verdict it chose, so a `Skip` taken at
  its word would park it on a wake nobody owes it.
- **Two paces.** The ladder paces failures; the **floor** — the worker's `WithInterval`, defaulting
  to the backoff base — paces clean restarts, which is what keeps a watch rotation from being free.

**`Wake` and `Restart` are the two ways to ask for a run.** A `Wake` never tears a live worker
down — it means "when you next stop, start again at once", which is what lets `kubesync`'s
connection bridge wake every kind on a cache per state frame. A `Restart` cancels the run first and
marks it stopped. Neither waits; `Remove` and `Close` do, and so must not be called from inside a
`Run`. A **watch edge onto a worker is a `Restart`** where it is a `Wake` for a job: a worker's
input moving means the one it is running on is stale.

**A body may not take the supervisor down with it.** One that panics, or that hands back the
zero `Result`, is recorded as an `Internal` failure and gives its key back — the supervisor logs it
through `slog`, the only place it logs at all. Nothing else reports a bug in a body, and leaving
one unrecorded wedges the probe twice over: in flight forever, with its key held in the queue.

**Each of `State`'s five observations has one probe behind it**, registered with its own interval
(a cluster's UID never moves; its readiness moves constantly). The supervisor owns the observables —
one value beside one `Attempts` per probe, the value written by that probe's `Run` alone — and
`Read`/`OnPass` hand them back as a `supervisor.Snapshot`, frozen at the moment it was taken.
Anything reads one out of it by registration name
(`supervisor.GetJobObservation[connInfo](snap, nameConnection)`, the `name*` constants), which is
how a `Run` reads a sibling and how `stateOf` assembles `State` at publish time. **A `supervisor.Key[T]` states that name↔type pairing once** rather than at every
read site — `keyConnection.From(snap)`. It is a freestanding declaration: registration never
hears about it, and the pairing is checked where the read checks it, when a value lands. The
connection is the only observable another probe reads, so it is the only one keyed. Its value
(`connInfo`) bundles `departed` and the connection with the endpoint; `stateOf` projects only the
endpoint into `State.Connection`, and `newsOf` walks `probeNames` for the untyped per-probe read.

**A `Run` takes a pass and returns only its `Result`.** Both passes carry the run's inputs —
`Subject()`, `Prev()`, `Known()`, `Snapshot()` — and `pass.Commit(v)` records what the run found,
wherever in the body it learns it. They split over what happens next, and the compiler is what
enforces it: `Ready` is not a method on a `JobPass`, and a worker handed to `RegisterJob` does not
build.

- **`JobPass.Commit` is buffered** and applied when `Run` returns, in the same critical section as
  the attempt: nothing is published mid-run, the last call wins, and a run that then concludes
  `Skip` or panics commits nothing.
- **`WorkerPass.Commit` is applied at once** — a worker has no end of the run to wait for, and
  reporting while it runs is what it is for. Every commit takes the supervisor's lock and fires a
  pass, so **a worker's `T` is what a reader reacts to, never what arrives**: one committing per
  frame would publish per object.

**A job's observation and a worker's are different types**, read through `GetJobObservation` and
`GetWorkerObservation`, each panicking on a name registered as the other kind. A job confirms its
value by running again (`LastSeenAt`); a worker confirms it by still running, so its stamp is
`ChangedAt` and its freshness is `Live()` — a worker's `Watching` is false the moment it exits,
where a job's `identified, as of 10:00` still holds.

**`Known()` is what a probe whose zero `T` is an answer needs.** `Prev()` cannot tell "nothing has
landed" from "the last answer was the zero value", and the supervisor dates an observation by its
*value* — so readiness (healthy is the empty `ComponentStatus`) would never commit, and a cluster
that has never had a failing component would read as never observed. Its guard is
`!pass.Known() || the set moved`.

**Commit only on a change.** A committed value is what tells the supervisor the value moved, and so
what re-runs every probe watching it — commit unconditionally and the four behind the connection
re-run every cycle, which is the intervals they are registered with undone. The supervisor never
compares (it holds values as `any`, and a probe's value may be uncomparable or carry funcs), so
the guard is the body's: `connInfo` is comparable, so it is `if next != pass.Prev()`.

**A probe's result is its schedule** — `Succeeded` (due again after the interval), `Fail` (due up
the backoff ladder), `Suspend` (nothing due until a `Wake`), `Skip` (record nothing; wait for a
`Wake`). The four behind reachability declare both edges on it — `supervisor.WithDependencies` (they
cannot run without a connection) and `supervisor.WithWatches` (they read the one it commits). The
supervisor records them as `DependencyFailed` rather than dialing while the connection has not
succeeded — one timeout per cycle, not one per probe — a recovery makes them due again by
derivation, and a connection whose value moves re-runs them at once.

**The connection probe owns the context's lifecycle**, because resolving the kubeconfig is the
first step of reaching a server. Its classifications: `ReasonContextNotFound` suspends with
`departed` committed true (the file is the whole truth about presence, and the watch reports it
moving — a departure is also not a failure streak, being the user's own edit);
`ReasonResolveFailed` fails up the ladder for both a file that will not resolve and a build that
will not materialize clients from it (nothing was dialed either way, and the file can be fixed in a
way `kubeconfig.Service` cannot see, such as a CA path that now opens); an unread file is a `Skip`
(an unread kubeconfig names nothing, and is deliberately not a departure).

**Reaching the server is one `GET /api`**: the cheapest
request that proves DNS → TCP → TLS → authentication, the only endpoint of the five probes' that
can answer 401 or 403, and the one whose body tells a Kubernetes API server from a captive portal
answering 200 to everything — so empty `versions` is `ReasonMalformed`. **The probe builds a
connection; the pool retires one**, and a rebuild happens on a changed fingerprint *or* no
connection, never the fingerprint alone.
→ [ADR: the connection probe dials /api](../docs/adr/2026-08-25-connection-probe-dial.md).

**Wiring**: `Acquire`'s first holder is `supervisor.Add`; the last `Release` is `supervisor.Remove`,
under `Service.mu` so a stale release cannot remove the subject a fresh claim just added; the
kubeconfig watch is `supervisor.WakeAll(nameConnection)` on every change — every claimed context
rather than the ones that moved, because finding which moved is what the probe does anyway. `New`
calls `configureHTTP2Keepalive` (10s/5s, only where unset): the vars are read when a transport is
built and this package builds them, so a call the composition root has to remember is one that
goes missing.

**Retiring is the pool's because a run cannot do it**: `Pass.Commit` is buffered and applied after
the run returns, so a probe closing `Done` first would leave holders reconnecting against a `Conn`
still handing out the dead one. `publish` files what a pass concluded (`record`, one critical section, since a release landing
between the entry check and the `published` write would announce a claim that is gone and leave a
baseline the next claim's first pass compares equal to) and retires the connection nothing holds
any more — including the connection a pass carries for a context
that was released between the commit and the pass, which is the one a release could not reach.
`Release` and `Close` retire what the entry holds, or a released context leaves its sockets
behind.

**Publishing is the supervisor's `OnPass`** — after every pass, outside the supervisor's lock,
serialized per context. Two publish rules, because the two feeds answer different questions:
`stateHub` carries every pass (the timing is what a claim watcher subscribed for, and the
countdown to the next run is visible nowhere else); `signalHub` fires only when the **news**
changed — `departed`, `Phase()`, `Identity()`, each probe's `OK()`, never a timestamp — measured
against `Service.published`, what the fleet was last told. State first, so a reader the signal
wakes finds the value already there. A claim reads through `supervisor.Read`, with the entry-identity
check *after* the read so a name released and re-claimed mid-read is never answered on behalf of
a stale lease.

**The leaf's exported types are the boundary's**, aliased rather than copied: `clustersvc.Lease`,
`Connection`, `ConnIdentity`, `ConnState`, `ConnStateSubscription`. Aliases because an
`internal/` type cannot be *named* outside, which would leave `Service` unimplementable by the
resolver tests' fake. The layering exception is in `service.go`'s package doc.

**`State.Identity()` is what the probes last read; `Connection.ServerUID()` is what one connection
vouches for.** Both exist and they answer different questions. `Identity` is the fleet-facing
value — comparable, carrying no errors, since why a field is missing belongs on the `Observation`
that could not read it — and it is what `news` signals on. The connection's own stamp is what an
identity-scoped caller must use, through `ConnFor`; **never compare a connection against
`State.ServerUID`**, which is a separate probe's observable and lags a rebuilt connection by a
round-trip. → [ADR: connection-carried identity](../docs/adr/2026-08-25-connection-carried-identity.md).

**A conflict rebuilds the connection.** `connectionProbe.Run`'s rebuild arm asks the standing
connection whether it is `conflicted()` — never comparing it against `State.Identity()`, which is
the stale pairing — and `publish` wakes the connection probe so the rebuild does not wait out the
30s interval. **The wake is gated on the news having moved**, which is an edge: a `Wake` is a queue
add rather than a schedule, and a run that returns before the rebuild arm (a kubeconfig that stops
resolving) leaves the conflict standing, so a level-read condition would hot-loop past the backoff
ladder. Recording the conflict empties `news.vouchedFor`, so the edge lands on exactly the pass
that records it, and the interval is the backstop.
→ [ADR: identity-driven retirement](../docs/adr/2026-08-27-identity-driven-retirement.md).
Note what a username change does
**not** cover: ordinary RBAC edits leave it identical, so permissions need the
`SelfSubjectRulesReview` behind `ClusterPermissions`.

**`State` is what the last probe read about the server, not the connection's own life** —
whether one is built or retiring surfaces on `Connection.Done()`. **Five probes that fail and go
stale independently.** A cluster is rebuilt, upgraded, re-issues a token, or revokes a namespace
read, and none implies the others — so `Connection`, `Readiness`, `ServerUID`,
`ServerVersion`, and `Principal` are each an `Observation[T]`. Only reachability is a prerequisite; the rest are peers.

**An `Observation` keeps its value through a failure** — a read that stops being permitted does not
mean the fact changed — and `LastSeen` is what makes the survivor readable: *identified, as of
10:00* is usable where *ready, as of 10:00* is not. **`LastSeen` dates the value, not the verdict**:
it moves whenever a value is committed, whatever the run concluded, and on a success that
re-confirms the standing one. A failing run can still have *read* something — which components are
down — so dating that by the last success would leave it undated, and would date a replaced answer
by a read of what it replaced. Beside the value it holds two `Attempt`s and a
failure run: `Failures` with `FailingSince`, because the ladder widens and a count does not give
elapsed time. `Known()` is has-ever-answered, `OK()` is answered-last-time, `InFlight()` is
running-now.

**`Attempt` is one run at any stage of its life** — `ScheduledAt`, then `StartedAt`, then
`FinishedAt` and the outcome. One type, filled in order, which is why an unfinished run needs no
second one: `LastAttempt` is the run that finished, `NextAttempt` the one that has not, and a run
moves between them as it completes. `ScheduledAt` is separate from `StartedAt` because a saturated
prober lets a scheduled time slip into the past, which a single stamp compared against the clock
would read as running.

**A probe that has never run is the zero `Observation`** — a zero `LastAttempt` is not `Done`, so
every accessor answers correctly with no sentinel.

**A zero `NextAttempt` means the probe is suspended**: nothing is due and the last answer stands
(`Scheduled()` is the accessor). The four probes behind the connection suspend while it is down —
a server nothing reached cannot answer them — and re-arm when it recovers; a probe that came back
`Unsupported` stays suspended for the connection's life, since the endpoint is absent rather than
failing. `DependencyFailed` marks the one cycle where a probe went from running to suspended, and
the cycles after it schedule nothing, which is what makes a dead cluster cost one timeout per cycle
instead of one per probe. **Why a probe is suspended is `LastAttempt.Reason`** — no field beside
`NextAttempt`, since a probe suspends over what its last attempt found. That is why suspending must
write an attempt instead of going quiet. So *ready, as of 10:00, nothing due* is a state to render, not a stall.

**A `Skip` parks with nothing due as well**, so `Scheduled()` alone does not say what a registration
is waiting for. `Suspended()` is the narrower read — nothing due, nothing running, and a suspension
is the last thing that happened — and it is what a caller waking whatever a returning connection
revives must gate on. Waking a skipped registration on a hunch re-dispatches whatever shares that
wake ahead of its backoff ladder.

A **disabled** cluster never gets here: the controller drops the claim and the pool stops probing
credentials nobody holds. `kubeconn` does not learn what disabled means.

**`NextAttempt.ScheduledAt` is the backoff ladder made visible**, and it costs nothing to publish:
the prober schedules the next run as it finishes the last, so the countdown rides a send it was
already making. Successive values show the interval widening — otherwise invisible outside the
prober.

**`Reason` is assigned when the attempt ends**, in our own vocabulary styled as a Kubernetes
condition reason (`Unreachable`, `Forbidden`, `Unsupported`, `ServiceUnavailable`, …). It has to be:
`Err` arrives wrapped and does not survive the copy a watcher holds, so a caller sniffing it later
cannot tell a 403 from a timeout. **It spans layers on purpose** — transport, API response, and
rules of ours — because a caller asks why a probe failed once, not three times. Names shared with
`metav1.StatusReason` are the same word for the same thing; the set is not that set.

Two prober traps live here. `NotFound` and `Unsupported` **both arrive as a 404** — the object was
missing versus the endpoint is not served — and only the probe knows which it asked for, so
classifying on the code alone permanently suspends a probe that should keep running. And `Dynamic`
returns `*apierrors.StatusError` carrying the API's own reason, while only the raw endpoints
(`/readyz`, `/version`) leave a status code as the sole evidence; one switch over codes for both
discards what the typed half knows. `Canceled` says nothing about the cluster and counts toward neither failure field;
`DependencyFailed` is a probe recorded rather than attempted, which is what keeps a dead cluster
costing one timeout per cycle instead of one per probe. Free-form text goes in `Message`, never
`Reason`.

A `State` is a value copy, but a **shallow** one: the slices inside belong to the prober and every
watcher shares the backing array.

The pool owns the reading, so every holder agrees: `State.Phase()` is `Pending`/`Unreached`/`Probed`
off `Connection` (the trap it exists for — no attempt yet is not an attempt that failed), and
`State.Identity()` projects the three comparable scalars out of the rich observations. The verdicts
stay above: condition types, reasons, and `Inactive` are the record's vocabulary, not the pool's.

**Everything a holder learns comes through its `Lease`** — `Conn`, `State()`, `WatchState()`,
`Departed()` — so the pool publishes per context and never asks a holder to know the credentials
behind one. `WatchState` is a `gobus/watch` receiver keyed by that context. **It delivers nothing
on attach** — gobus's baseline is a comparison value, not a delivery — so a watcher pairs it with
`State()` for what is known now. Reading and registering under one lock (`Hub.WithBaseline`, which
needs an `Accept` to mean anything) is what closes the gap between the two, and is worth having
once a probe can land at all. **Every value is a level, never an edge** — the hub keeps the latest,
so a reader that falls behind skips what came between, and transitions come from the record's
conditions and event timeline.

**Asking for an identity-scoped connection is `ConnFor`, and nothing waits.** Neither `Done()` nor
a state frame is the signal such a holder needs: retirement puts the replacement in the observable
*before* `Done()` fires, but that replacement is unstamped for a round trip after, so `ConnFor`
refuses through the window — and `State.Identity()` reaches the new UID as soon as a probe reads it
over the OLD connection, which says nothing about the replacement being stamped. Asking the
connection rather than pairing those two is the whole point of the method.

**A refusal is a verdict, never a wait.** A run holds a supervisor worker, so one that cannot get a
connection records `NoConnection`/`IdentityMismatch` and returns `Suspend`; what brings it back is a
wake — the fleet bus for a probe, the session's connection bridge for `kubesync`. There is no
blocking form to reach for, which is what keeps a worker from being spent on a cluster that is
down. → [ADR: identity-driven retirement](../docs/adr/2026-08-27-identity-driven-retirement.md).

**One context, one entry.** `Service.claimed` is a single map keyed by context name — also the key
both hubs publish under — holding the holder count, whether the file still names the context, and
what a probe read. Contexts resolving alike are **not** merged. → [ADR: one connection per
context](../docs/adr/2026-08-23-one-connection-per-context.md).

### The sync engine (`internal/clustersvc/internal/kubesync`)

**What fills a cache.** It speaks cache ids, kube-contexts, server UIDs and GVRs — never records;
`clustersvc` translates, and a record type reaching it is an import cycle. Its two dependencies are
the narrow `Acquire(contextName) kubeconn.Lease` and `OpenOrCreate(cacheID) (*kubestore.Store,
error)`. → [ADR: arming is policy](../docs/adr/2026-08-28-arming-is-policy-never-interest.md).

It is a `lifecycle.Part` between `kubestore` and `beehive`, so stopping runs beehive → kubesync →
kubestore → kubeconn: no pass can arm a session that is stopping, and no worker outlives the file
it writes into. `Start` **refuses a second start** — a second pair of supervisor loops on the same
wait group would leave the first stop draining loops only the second can end. It subscribes to
`poke` for `RestartAll`. `withKindSync` substitutes the kind worker in tests that are about arming.

**A clear runs INSIDE kubesync.** `RunWithCacheSyncStopped(cacheID, fn)` and
`RunWithKindSyncStopped(cacheID, k, fn)` take `armMu`, stop the workers (and the sweep, for the
cache-wide one) and JOIN them, run `fn` once, and arm everything again — so a `Manager.Clear` swapping the file cannot land under a relist page or a
`SyncKinds`. The store work stays with the CALLER, which is what rules out moving the clear down
here: `Caches().Clear` has to work on a paused cache, which has no session at all. `fn` runs under
`armMu`, so it must not call back into the Service.

**Two levels of arming, and they AND rather than nest.** `TrackDiscovery` says whether a cache
syncs at all — and *supplies* it, since the session it arms is what takes both claims — while
`TrackKind` says which kinds. A kind's registration **outlives its cache being forgotten**: pausing is one call and
resuming is one call, with no record written and none requeued, where gating through the records
would mean relaying the switch onto hundreds of them.

- **Arming is policy, never interest.** A kind syncs because a record's pass armed it, never
  because something read it.
- **A session takes its own claims and gives them back**, in `start` and `close`. The lease is
  taken only once the file is open, so a store that will not open leaves nothing to unwind — the
  cache arms on a later pass instead, and is logged because nothing else would report it.
- **Nothing syncs into a cache whose connection does not vouch for its `ServerUID`.** The gate is
  the session's, and both levels pass it the same way: a run holds a supervisor worker, so it
  records why and `Suspend`s rather than waiting. The session's connection bridge is what brings
  both back — one guard per session, since the pool's answer is one fact for every kind under it.
- **Forgetting is synchronous.** `ForgetDiscovery` returns only when nothing can still write
  through that cache's store, and `ForgetKind` only when that kind cannot. **`ForgetDiscovery` is a
  pause; `ForgetCache` is a teardown** — a pause keeps every kind registered so a resume is one
  call, where a deleted cache leaves nothing holding them. `Supervisor.Remove`
  stops a subject being scheduled and hands back the value it stood on, but it does not reach a
  run already dispatched — so each level supplies the rest:
  - **A sweep** is registered wrapped in `sessionScoped`: the run is counted so the teardown waits
    for it, and its context ends with the session's. Wrapped at registration, because a body that
    forgot would break the promise silently.
  - **A kind sync** is a worker, so `Supervisor.Remove` IS the whole answer: it cancels the run
    and waits for it, and past that nothing can write through the kind. The cancel is what bounds
    the wait — what remains is a page request unwinding, not the cold list it was in the middle
    of — which matters because `armMu` is the Service's, so a join of any length under it stalls
    arming on every cache.
- **A cache whose file will not open says so.** `arm` carries the failure out instead of only
  logging it, as the discovery verdict `StoreFailed` with the driver's message — a failing read,
  ranked above "has yet to answer". It lives in `Service.storeFailures` rather than the session,
  because a failed start leaves none, and it is `mu`'s: the map holds **exactly** the caches whose
  most recent arm failed. Cleared on the way into `arm` (a retry does not pass through `tearDown`,
  since a failed arm left no session to tear down) and in `tearDown`'s first critical section,
  above the no-session guard — a cache whose arm failed is precisely a cache with no session. The
  message is capped where it is recorded: it is the first discovery message this package does not
  write itself, and it leaves by two paths that bound nothing.
- **A verdict is a gauge, never a stored condition** (`GetDiscoveryState`/`GetKindState`), and
  **no answer is not an empty answer**: `false` means nothing has been observed yet, and a caller
  folding it into "serves no kinds" deletes a record set that was only waiting.
- **News is not data.** Two feeds, one per level, because their consumers are two beehive
  triggers and one feed carrying both would wake a cache for each of its hundreds of kinds. The
  key is the whole message and the reader answers it by re-reading. A resume is not news — only a
  reason that settled somewhere new — with one exception a verdict cannot carry: a sweep that
  committed a catalog (the session's `announce`).
- **A kind is keyed by `(APIVersion, Resource)`, and the singular is data.** Every map inside drops
  it; `KindKey` carries it, where a rename costs a duplicate wake and never a missed one.
- **Every walk over `s.tracked` that ends in a `Remove` snapshots under `s.mu` and acts outside
  it**, as `arm` does. `Discard` joins a goroutine whose exit commits through `commitKind`, which
  takes `s.mu` — it is the lock that deadlocks first.

#### The sweep (`discovery.go`)

**Discovery runs on `internal/supervisor`** — three probes over a per-cache subject (the cache id),
`kubeconn`'s shape, since both are periodic pulls whose answers are values. `apiVersions` reads
`/api`, `apiGroups` reads `/apis` (one preferred group-version per group), and `resources` fans out
over both on a **data edge**, so a document that has not answered leaves the fan-out `Skip`ped
rather than failing it. `DiscoveryState` is projected from the snapshot, so the seam stays this
package's vocabulary rather than the supervisor's.

- **A sweep is a probe whose collection cannot be watched.** Plain GETs, no resourceVersion, no
  watch verb — so it is a cold list with no watch phase, re-listing on the supervisor's cadence.
  `SyncKinds` reconciles by fingerprint and prune, as a relist does by mark and sweep.
- **The answer goes to disk and nowhere else.** The sweep starts no kind and stops none — what is
  synced is the records' to say. It publishes news; the kind records' passes do the rest.
- **A sweep skips the write when the stored fingerprint matches.** `SyncKinds` is a delete plus an
  upsert per row against the single writer every kind's deltas queue behind. The fingerprint is read
  off the table rather than remembered, so a restart and a cleared cache each write once. **The
  prune flag is part of it**: a partial answer and a complete one over identical rows are different
  writes.
- **Four filters, none optional** — preferred version only, `list` and `watch` in the verbs, no `/`
  in the plural, and not the `events.k8s.io` spelling of Event.
- **A group that will not answer is `Partial`, and blocks the prune.** Its kinds report their
  own verdicts, so a broken aggregated API shows up twice and correctly. `Partial` is the one
  verdict a `supervisor.Result` cannot carry (`Succeeded` takes no reason, and both neighbours misprice
  the backoff ladder), so it rides two fields on the session.
- **`IsCRD` comes from a CRD list, matched by (group, plural)** with no version. **Best-effort and
  outside the verdict**: a refusal leaves every kind reading as built-in.
- **Two loops wake a sweep the supervisor cannot schedule.** `wakeDiscoverySweepOnConnectionChange`
  carries both directions: a suspended run schedules nothing, so nothing but a wake brings it back
  once a connection vouches for the cache; and a settled run is *scheduled*, so a connection that
  stopped dialing would read `Discovered` until the interval came round. Level-triggered against
  the facts, since `WatchState` is latest-value and an edge between two frames is one a reader can
  skip. **The invalid direction wakes only what the verdict does not already say** — that feed
  publishes every pass, not only the ones that changed something, and re-waking on each frame is
  the poll suspending exists to avoid. `connectionReason` is the single mapping, shared with the
  run, so the loop and the sweep cannot disagree about what the pool said.
  `wakeDiscoverySweepOnCatalogChange` is the other: subscribed on the CRD and APIService object
  keys, which the cache already mirrors. **It wakes the whole sweep, not the fan-out alone** — a
  CRD for a group the cluster did not serve adds that group to `/apis`, so the list the fan-out
  reads has moved too.
- **A sweep prunes against the last group list it read, and that is safe.** The rows on disk were
  written from a list no newer than that one, so a stale list cannot orphan them; and a
  group-version on it that stopped serving fails its own document read, which makes the sweep
  partial and prunes nothing.

#### The kind sync (`kinds.go`)

One kind's rows, held current by a standing stream — and **the run IS the stream**, from the cold
list to the last delta. It is the supervisor's one worker (§the probes): `kindSync.Run` blocks for
the stream's whole life, `pass.Ready()` on the first frame says it is up, and returning is the
stream having ended. Two types carry it — `kindSync` is the body, `kindSyncer` is one run's state.
One subject per kind, `"<cacheID>/<apiVersion>/<resource>"`, on the `kindSupervisor`.
→ [ADR: jobs and workers](../docs/adr/2026-08-28-jobs-and-workers.md).

- **The start cap is the cold-list gate.** A worker holds a start slot until `pass.Ready()`, which
  the sync calls when the WATCH IS OPEN — the gate, the list and the open being what cost. So
  `pacing.kindStartConcurrency` bounds the relists in flight across every cache however many kinds
  are already streaming, which is what arming one with hundreds of kinds needs. **Never wait for a
  frame to release it**: bookmarks are advisory and a quiet collection may send nothing for hours,
  so the first few kinds would hold every slot and the rest of the cache would never list a row.
- **The subject names a kind but does not carry it.** `enterKindRun` hands the run the whole
  `kubestore.Kind` out of `s.tracked` — the singular included, which the rows are keyed by and no
  body can learn from a collection that lists empty. One critical section, which also counts the
  run against the session so a teardown waits for it.
- **How a run ends is the whole schedule.** `nil` from `applyDeltas` with the context still live is
  the apiserver rotating the watch: a **clean exit**, paced by the floor, and the verdict never
  leaves `Watching` — the rows stayed current across it. A context that ended is a `Skip`: the
  session went, the supervisor stopped it, or the connection was retired, and none is this kind's
  failure. Anything else is a `Fail` up the ladder.
- **A watch that closed having proved nothing is a failure, not a rotation.** Proof is a frame, or
  simply staying open past `staleAfter` — a quiet collection rather than a wedged one. Without
  that split, a server which accepts every watch and drops it would reopen at the floor forever
  while reporting `Watching`; the ladder is the honest pacing for a stream we cannot say works,
  and it climbs because `Ready` does not clear the streak.
- **A retirement asks for its own next run.** The pool publishes the replacement *before* the
  stream can notice the connection under it died, so the session's bridge has already fired and no
  other wake is owed. The run `Wake`s its own subject on the way out, which the queue redelivers
  when it ends.
- **The cookie decides which start this is** — not whether the cache holds rows. One on disk means
  a completed LIST landed, so the watch resumes from it; without one the collection is cold-listed
  through `BeginReplace`/`WritePage`/`Commit` first. A relist that wrote a page and then died leaves rows
  but no cookie, and reads as cold — which is right, since those rows still need the reconcile.
- **An expired position relists instead of resuming, and says `Resyncing` while it does.** The
  flag that forces it is cleared only once the relist has landed: the cookie survives a LIST that
  failed before its first page, so dropping it earlier would send the next attempt back to a
  position the server has already refused.
  `Expired`/`Gone` off the watch — at open or as an `Error` frame — is the one failure a resume
  cannot retry its way out of, because the cookie is what it would retry with. The watch error is
  wrapped rather than flattened so the loop can ask. The rows stay served throughout, which is why
  this is not the `Syncing` a cold start reports.
- **A resume holds its reason.** Each run seeds its verdict from what the run before it committed
  (`pass.Prev()`) and commits only when that MOVED — otherwise `RestartAll` walks every kind
  through `Watching`→`Syncing`→`Watching`, and a resume poke on a 300-kind cache becomes six
  hundred reconciles. The one exception is a run's FIRST report, which always commits even when
  nothing moved: until this run has said something, a reader has only the last exit to describe the
  kind by, so a stream that came back after a suspension would read `IdentityMismatch` for as long
  as it streamed. It is not news either way, since the reason has not moved. Only a resume
  that outlasts `staleAfter` says `Resuming` — announced by `openWatch` from the establishing run
  itself, never a timer callback: **one stream's state has one writer**, and `Timer.Stop` does not
  wait for a callback already running, so one firing as the stream settles would leave `Resuming`
  standing over the `Watching` it raced. **The wait for that open stays on `ctx`
  throughout**, before and after the announcement: forgetting a kind cancels and joins its run, and
  whether an open ever unwinds is the server's business. What lands after the run has gone is
  collected by `abandon`, since a watch nobody waits for still holds a connection.
  **A cold start is different and reports `Syncing`**:
  there the kind genuinely has nothing. What must survive a run — the verdict and the stamps —
  is read back off the session, since each establishment builds a fresh `kindSyncer`.
- **A bookmark is proof of life, not data.** It moves `LastLiveAt` and the cookie; only a delta
  moves `LastUpdateAt`. `staleAfter` without either is what `Stale` reads off — the rows are still
  served, they have simply stopped being known to be current.
- **`KindState` is assembled at read and stored nowhere** (`kindStateOf`), from the three that own
  its parts: the reason the worker committed, the supervisor's `Attempts`, and the session's
  per-frame stamps. A stored copy would be a stale duplicate of all three. **The worker's value is
  the reason alone** — it commits exactly when that moves, so `WorkerObservation.ChangedAt` is
  when it last moved, and anything else a value carried would be a second copy of what `Attempts`
  already holds. `Restarts` is
  `Attempts.Restarts` — how many times the stream came back inside the current healthy stretch,
  the flapping question a retry streak cannot answer — and the streak itself is readable as a
  non-zero `NextRetryAt`.
- **`Live` is the one supervisor reading carried through rather than folded away**: the watch is
  open right now. A consumer cannot redo it — reconstructing it means enumerating which reasons a
  running stream reports, and that set is this package's to change. `Stale` is live, `Syncing` is
  not. The rest of `Attempts` stays behind the seam, since a worker's last exit and next schedule
  are the two facts a reader gets wrong (below).
- **A run in flight speaks for itself; a worker's last exit describes it only while it is DOWN.**
  Two ways it has spoken: it is `Live()` (running and ready), or it has committed something since
  that exit ended. Otherwise the exit outranks the verdict — `NoConnection`/`IdentityMismatch` from
  a suspension, `SyncFailed` from any failure. Without the rule a kind relisting after a `410`
  would read `SyncFailed` throughout the relist, and one whose connection came back would read
  `IdentityMismatch` for as long as it streamed. The same gate is on `NextRetryAt`, so a live
  stream never serves the countdown of the failure it recovered from. **A kind with nothing
  committed and no exit that outranks it answers nothing at all** — the seam promises the getter
  says so, and inventing an empty answer would wake its record for a verdict nothing reached.
- **`publishKind` wakes a record only when that answer MOVED.** `OnPass` fires on every pass,
  schedule-only ones included, so the baseline is the session's `published` map; without it every
  pass would wake every record on the cache.
- **A run lasts as long as its connection.** It ends when the pool retires the connection it was
  handed (`Connection.Done`), and the next run goes back through the gate for the replacement. A
  stream blocked in a watch read cannot see the retirement itself, and retrying over a retired
  connection would climb the ladder until a resume poke. **Every cancelled run leaves through one
  exit** (`kindSync.stopped`), which records nothing and asks for its own next run when the
  connection was what went: the bridge's wake has already been and found this run in flight, and
  the cold list and the watch open are cancelled as readily as the stream.
- **A kind at the gate says why**: `NoConnection` or `IdentityMismatch`, from the `Suspend` the run
  records, re-reported as the pool's answer moves. `NextRetryAt` is zero and so is `Restarts` —
  nothing is retrying at the gate, and a suspension ends a healthy stretch rather than a streak.
  The stamps stand, so they survive the wait. What brings it back is the session's connection
  bridge, whose `Wake` starts a parked kind and leaves a live one alone.
- **Events are synced like any other kind** — listed, watched, and never trimmed here. The cache
  mirrors the collection the server serves, so how many rows it holds is the server's answer.
- **Every duration is a `pacing` field**, and production passes `defaultPacing()`. No test outwaits
  a production number.

**A relayed value needs a `depends_on` edge; the owner edge is not one.** The catalog's `Enabled` is
the cluster's toggles resolved once above (`cacheSyncEnabled`, which also folds in whether the cache
is still the active identity), so a flip on the cluster has to reach the cache — and owning a child
wakes nothing. `clusterCacheController.Reconcile` therefore declares `AddDependency(cluster)`, the
edge running from the cache its pass was handed (a client only ever declares its own edges);
re-asserting an existing edge records nothing, so every later pass is free. A relay written without
one sits stale until something unrelated wakes the child.

**The rest of the chain needs no edge, because the relay lands in the child's own spec.** A parent
writes `Enabled` onto the catalog, and the catalog onto each resource — a spec write bumps the
generation, which is already a wake. The cache is the exception precisely because
`ClusterCacheSpec` is identity-only (`serverUID`): its switch is never written to it, so it has to
read the cluster, and reading another object is what an edge pays for. Adding a `depends_on` where a
spec write already carries the value buys nothing and doubles the wakes.

**Discovery is a beehive kind, not a loop beside one.** `ClusterSource` is one anchor object per
`ClusterSpecSource` variant (today `clustersource/kubeconfig`), and its controller runs the pass that
keeps the record set in step with that source. It is a kind precisely so the pass gets what a loop
would have to hand-roll: beehive's backoff ladder on a failed pass, `startupPass` for the boot
import, `Requeue` as the out-of-band kick, an observed generation, and an events timeline. The one
piece outside beehive is a `trigger` (`triggers.go`), which subscribes to the source's own change
feed and `Requeue`s that source's anchor — a source of truth is not an object, so nothing else could
span that gap. It is generic over the feed's element type (`feed[T]`, satisfied by any
`Chan()`/`Close()` pair) because **the value is dropped**: a poke asks for a pass, and the pass reads
current state. **Beehive owns the receive loop** — a feed is declared at registration with
`WithTriggerByName`, which resolves each name within the kind and requeues it, along with its rate
against the store and its place in the shutdown order. What is left here is translation, which
beehive cannot do: only this package knows that the kube-context "prod" is the record
`kubeconfig/prod`. A second feed is `newTrigger(subscribe, name)` plus the option, and nothing else.
It carries **no retry**: a lost poke costs latency, since the kind's own cadence runs the pass
anyway.

The pass is **creation-only** (`ensureKubeconfigClusters`, which lives in `clusters.go` because the
name and spec are the Cluster kind's vocabulary): it creates a record for every context nothing yet
references and never updates, orphans, or deletes. A departed context is orphaned by
`clusterController` observing it absent (`IsPresent=false`), which keeps set membership and
per-object observation from fighting, and lets a returning context reuse its record **with the
user's toggles intact**. It is also why status is unreachable from the pass: beehive's
`ControllerClient` is bound to the object the pass was handed, so `UpdateStatus` writes the anchor
and nothing else, even though the pass creates the records.

**The anchor's status is a wake signal, not a report.** It carries one field — a fingerprint of what
the pass observed — and every `Cluster` declares `AddDependency(anchor)` from its own
reconcile, so one status write there wakes all of them through beehive's dependency waker, with the
stale-dependents pass as the guarantee behind it. The observation reads `kubeconfigSvc.Get()` rather
than the object, so beehive cannot know it went stale; the edge is the only thing that reaches it,
and a departed context — absent from every snapshot the create pass walks — is reachable *no other
way*. **A stamp that moved every pass would wake every record every pass** — that is the trap the
fingerprint exists to avoid, and any new field on `ClusterSourceStatus` inherits it.

`kubeconfigFingerprint` is **a hash of the whole snapshot**, deliberately coarser than what any
record observes. A digest built from the folds instead would wake nobody the day one of them starts
reading another field, and keeping the two in step is a coupling nothing enforces — so this covers
everything and pays in false positives: an edit no record cares about wakes them all, each to
compare, find nothing moved, and settle. A kubeconfig save is a human-paced event and a pass that
observes nothing is a map lookup.

The wake is deliberately broad rather than targeted: to know which records a change affects you must
compare each one's stored observation against the snapshot, which is the per-object work the Cluster
controller already does. An unaffected pass is a map lookup, a struct compare, and a no-op settle.
Narrowing it means assuming stored state matches the last event, which is the assumption
level-triggered reconciliation exists not to make. Revisit only when a pass becomes expensive — at
which point the fix is to gate the expense inside `Reconcile`, or to give discovery its own
per-context kind whose spec the anchor writes (the shape `ClusterCachedKind` already uses).
→ [ADR: discovery as a beehive kind](../docs/adr/2026-08-18-discovery-as-a-beehive-kind.md).

**Both reconciles defer until the kubeconfig has been read**, though neither reaches that branch
today: the app starts `kubeconfig.Service` ahead of the cluster service and its first read is
synchronous, so `Get` reports read before beehive dispatches anything. The guards stay because the
pre-read config is empty and indistinguishable from a file with no contexts — `Service.Get` reports
the read alongside the config precisely because the two states are the same value. Observing the
pre-read one would mark every present context absent and wake the kind's watches for a flap, and for
the anchor the stake is higher: it would fingerprint an empty set. Unread is an `Unsettled` requeue,
not a write. **Keep them if you reorder startup** — that is the only thing standing between a
reordering and a silent mass-orphaning.

Scope a discovery pass by the source discriminant (`Spec.Source.Kubeconfig != nil`), not by the name
prefix. Manual creation will have no source at all, so *"every Cluster has an anchor behind it"* is
not an invariant to lean on — the dependency edge is declared only for records that have one.

**Watches are pull-first** — correctness comes from the poll, and push only makes it prompt.
`kubeconfig.Service` is the worked example (its godoc has the reasoning): a 30-minute backstop tick
under fsnotify wakes and a poke subscription, both optional and allowed to fail. Applies to every
watch. **Keep the tick under a new push layer rather than replacing it** — it is what covers what
events cannot see, including the resume the poke subscription is there for.
The `ClusterSource` anchor follows it: `clusterSourceResyncInterval` is the poll, and the trigger
only makes it prompt. **Nothing changes the record set out of band**, which is what lets the anchor
need nothing else to wake it: `Clusters().Delete` refuses a record its source still declares, so a delete
can never free a natural key the source would want back. **The create pass still runs ahead of the
fingerprint gate** — a failed create is retried against the snapshot that failed, so a pass that
returned early there would skip the retry.

The service watches **directories, and follows symlinks**: a save replaces the inode (so a
file-level watch goes deaf), and a dotfiles-managed kubeconfig is a link whose target lives in a
directory nobody would otherwise watch. The resolved set is recomputed per reload, so a re-pointed
link follows to its new directory. Reach for `resolvePaths` before adding anything here, and keep
every path it yields in **one namespace** — the watch list and each event's name are matched against
that set by string, so resolving a path further than the link itself (`filepath.EvalSymlinks` rewrites
every component, and macOS answers `/var` with `/private/var`) leaves a path that quietly matches
nothing.

`clustersvc.New(dataDir, kubeconfigSvc, pokeSvc)`, `Start`, and `Close` are what the
composition root calls. `New` grows a parameter only for a new process-wide service; filling in a
family or a controller never touches it.

**The leaves this package drives speak native vocabulary** — GVRs, a `rest.Config`, cache rows —
never the records above; the controllers translate. A leaf reaching for one of this package's types
gets an import cycle, which is what enforces the direction. Put a mechanism in a leaf, never in a
controller: **if `go test ./internal/clustersvc` stops being fast, one has leaked back in.**

**A process-wide service is the app's, and this package only reads it.** `kubeconfig.Service`
arrives through `deps` behind the narrow `kubeconfigService` (`shared.go`), so nothing in this
package starts or closes it — the kubeconfig's
`Close` ends every subscription in the process, including other packages'. The trigger subscribes
to it and releases only its own subscription.

The four families are `Clusters()`, `Caches()`, `CachedKinds()`, and `CachedData()`. **The
`Cached*` prefix marks the cache subtree** — the per-kind records under a `ClusterCache` and the
mirrored content itself — so the grouping is visible in the accessor list rather than something you
have to know. Keep it when adding a family there.

**`CachedKinds()` and `CachedData().*Kinds` are two different things and read almost alike.** The
first is the control plane — one beehive record per kind a cache mirrors, `ClusterCachedKind`,
carrying that kind's verdict. The second is content — the `kind_catalog` rows in the cache's own
file, `ClusterCachedDataKind`, carrying counts. On the wire that is `ClusterCache.cachedKinds`
against `ClusterCache.kinds`, and `clusterCachedKindsWatch` against `clusterCachedDataKindsWatch`.
The `Data` infix is the whole distinction; keep it on everything the store serves.

Rebuilding a family means replacing the panics in that family's file. Keep the method naming rule
when you do: **VerbNoun with the noun elided when it equals the family's subject**, so
`Caches().WatchList()` watches caches and `Caches().WatchStats()` streams one cache's stats.
**A family owns a read only when the read differs per record type.**
`RetryConnection`/`AcquireConnection` stay top-level (they answer about a connection, not a
record), and so do `ListEvents`/`WatchEvents`: an event carries no kind, every id is the same `ObjectID`, and only
three of the four families have a timeline at all — scoping them would be three copies of one method
plus an unanswerable question about the fourth. Every family is asserted separately
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

**`By*` names the scope the caller passes**, which is the owner edge for both of them: `Caches`
(`ByCluster`) and `CachedKinds` (`ByCache`) each enumerate what the id they were handed owns.
**A cache nothing has discovered kinds for reads empty, never an error**, and its watch bookmarks
that empty snapshot rather than holding it back — an unsynced cache is definitively empty, not
pending, and the kinds arrive above the snapshot as ordinary `Added` frames.

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

The three kinds' spec/status/record structs, identity, conditions, frame types, and the cached-data
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

The schema **is** the Go shape — every GraphQL type binds 1:1 by name to its `internal/clustersvc` type in `gqlgen.yml`; no projection layer. Resolvers are one-liners delegating to a family on `r.ClusterSvc` (e.g. `r.ClusterSvc.CachedData().WatchObjects`; the field is named `ClusterSvc` to avoid shadowing the generated `Clusters` method). Everything below answers. Key entry points:

- Delta watches: `clustersWatch`/`clusterCachesWatch` (independent; joined client-side), `clusterCachedKindsWatch(cacheID)` (cache-scoped — ~100 records; the always-mounted registry must not carry it), `clusterCacheHealthWatch` (the fold — a gauge, **not** a delta watch, so no `Bookmark` rides it; see the gauge bullet below).
- **Every delta watch closes its snapshot with one `FrameBookmark`**, carrying a nil entity — which is why the seven `*WatchFrame` types hold their entity by pointer and the schema types it nullable. Both are named for the frame, not the change: a frame is a change **or** the bookmark, so `ClusterChange`/`ChangeType` would each have been a lie for one value of the enum. A record watch sends it between the snapshot and the first live change, and carries a failure reason out through `Stream.Err()`. A per-cache watch must send it after the first successful read *or* the first bind that finds no open cache (an unopened cache is definitively empty, not pending), and anything that holds frames back must queue the bookmark behind them — it must not claim a snapshot is complete over frames still undecided. → [ADR: delta-watch protocol](../docs/adr/2026-08-09-delta-watch-protocol.md).
- **Gauges are their own subscriptions, never a field on the record they describe** — `clusterCacheStatsWatch(id, cacheID)`, `clusterCacheHealthWatch`, `clusterScheduleWatch(id)`. A field would only be re-read when the record's own watch fires a frame, and each of these keeps moving after its record settles: a cache's object counts, a countdown. So a field freezes at whatever the last frame happened to carry. Re-emitting the record to refresh one is the other half of the trap — these numbers sit outside `status` precisely so a measurement never wakes the record's dependents. Current-on-subscribe, so no `Bookmark` rides them, and nothing is emitted at all before the first measurement (which is what a consumer renders "not observed yet" from). **`clusterCacheSyncStatusWatch(id, cacheID)`** is the newest: the discovery verdict plus a row per mirrored kind, each with its own reason and row count. It is the only field on the wire carrying a per-kind verdict — `clusterCacheHealthWatch` folds a cache into one, and neither record stores one — and it re-reads on the cadence alone, since its counts and stamps move while every record under it sits still. **The fold answers a cache-level verdict first**: `Paused` when every kind this cache mirrors is paused (paused kinds are skipped, so a fully paused cache has no offenders and would otherwise read `Watching`), then `StoreFailed`, because a cache whose file will not open arms nothing, so every kind reads as unanswered and the loop's default would report a permanently broken cache as still connecting.
- Cache-data watches (all keyed by cluster id + cache id; frames carry `cacheID` provenance — objects additionally `apiVersion`/`resource` — so the client rejects stale frames after a swap): `clusterCachedDataKindsWatch` (kind catalog + counts; subscribes to **both** brokers via `catalogSubscribe`, since Event counts come from event triggers), `clusterCachedDataEventsWatch` (every cached event, newest first; `Deleted` when the server drops one), `clusterCachedDataObjectsWatch` (per-kind rows incl. `rawJSON`; resource-keyed broker subscription). Unopened cache → the `Bookmark` alone.
- Point reads hang off the record that owns them, resolved on selection: every event timeline is an `events(category, limit)` field (`Cluster.events`, `ClusterCache.events`, `ClusterCachedKind.events`), the discovered kind catalog is `ClusterCache.kinds` (no arguments — both ids it reads with come off the record), and `Cluster.caches` / `ClusterCache.cachedKinds` walk the owner chain down (`Caches().List`, `CachedKinds().List`). So there are no root `cluster*Events` or `clusterCachedDataKinds` fields. The lookups `clusterCache(id)` and `clusterCachedKind(id)` (over `Caches().Get`/`CachedKinds().Get`) address a record by **its own** id, which a caller holding one from a watch frame uses directly.
- **Every noun has the same pair at root: `<noun>(id)` and `<nouns>(<parent>ID)`** — `cluster`/`clusters`, `clusterCache`/`clusterCaches(clusterID)`, `clusterCachedKind`/`clusterCachedKinds(cacheID)`. The plural's scope argument is **optional**: omitted it reads the whole fleet, passed it returns exactly what the nested field serves (`Cluster.caches`, `ClusterCache.cachedKinds`). The resolver picks the boundary method the argument implies — `Caches().List` when nil, `Caches().ListByCluster` when set. Keep that shape when adding a noun.
- **`Cluster.caches` is the set, never "the" cache.** Activeness is the live join against the parent's `status.server.uid` (`CacheIsActive`), and a probe rewrites that UID with no cache event — so a consumer that must follow it over time reads `clustersWatch` + `clusterCachesWatch` and joins them, rather than reading the query field. → [ADR: delta watches](../docs/adr/2026-08-09-delta-watch-protocol.md). The live counterparts `eventsWatch` and `clusterScheduleWatch` (countdown + `probing`) stay flat at root: only the point reads nest.
- Mutations: `clusterEnabledSet`, `clusterSyncEnabledSet`, `clusterConnectionRetry` (returns immediately; outcome lands on conditions), `clusterCacheClear` (takes the **cache's own id**, since a UID migration leaves a cluster owning more than one: stop that cache's workers, delete the files, then **requeue its kinds** — their passes re-arm the workers, which cold-sync, the cookie having died with the file), `clusterCachedKindSyncEnabledSet` (**one kind's** switch, taking that record's own id — pausing stops the watch and keeps the rows, where `clusterCacheClear` throws them away), `clusterDelete` (GC cascades to the cache; **refused with `ErrDeclaredBySource` for a record its source still declares**, since the discovery pass would re-import it under a fresh id and the new record would carry defaults rather than the user's toggles . **The guard reads the kubeconfig, not the record's observation**, which is only a cached view of it: status is nil for exactly as long as a just-imported record has not reconciled, and the webview renders such a record as orphaned (`isPresent ?? false`) — so its Remove button is live in precisely the window a status-only check would wave through. Status is the fallback while the file is unread, and a record with neither is refused, since refusing is recoverable and allowing is not).
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

A cross-subsystem **leaf**: a wall-clock gap detector (15s tick, 2× factor — catches sleep/SIGSTOP/VM pause, works headless) plus a `gochan/broadcast` fan-out hub. `New()` takes no arguments; `Start(ctx)` returns a stop func; `Poke(src)` never blocks. Consumers subscribe directly: the core controller re-probes all clusters, the GVR-sync controller restarts workers in place (cheap cookie resume), `prefsync` reconnects. **A poke is a fan-out, not a cascade** — never routed through spec counters or conditions (a clean resume produces no condition transition, so a cascade would silently skip the stale watches). → [ADR: poke resync fan-out](../docs/adr/2026-08-09-poke-resync-fanout.md).

## GraphQL via gqlgen — the schema is the source of truth

`graph/schema.graphqls` is authoritative — also consumed by the frontend's codegen (`codegen.ts`). One file for the whole surface, sectioned by noun (shared vocabulary, cluster, cluster_cache, cluster_data, chat, cloud account) with `type Query`/`Mutation`/`Subscription` collected at the end. Resolver layout is `follow-schema`, so the one schema file generates the one `schema.resolvers.go`.

After editing:

```sh
cd sidecar && go run github.com/99designs/gqlgen generate
```

This rewrites `graph/generated.go` + `graph/model/models_gen.go` and appends panicking resolver stubs to `graph/schema.resolvers.go` — implement those. **Never hand-edit `generated.go`/`models_gen.go`.** `tools.go` pins gqlgen. Also re-run the frontend `pnpm codegen`. `graph/model/models.go` is a permanent stub keeping the package non-empty across regen.

**Renaming, splitting, or merging a schema file is a two-pass regen**: the first pass copies each resolver body into its new file but leaves the old `*.resolvers.go` in place, so the package has duplicate declarations until you delete it and regenerate. Verify no body came through as `panic("not implemented")` before committing.

## Patterns

- **A type's methods live in the type's file.** Splitting them across files means a reader has to
  find the pieces before they can see what a type does. In `clustersvc` that puts every
  `func (s *service)` in `service.go`, helpers included. **A helper belongs to whoever calls it**:
  one family's goes on that family's `*API` type in its kind's file (`cachesAPI.measureCache`),
  a controller's on the controller (`resourceOwnersOf`), and only what more than one of them needs
  is on the service (`cacheBelongsTo`). So a file that earns its place owns a
  type or a body of free functions — not a slice of some other file's type's behavior. In
  `kubeconn` that puts the pool (`Service`'s methods) in `service.go`, the probes in `probe.go`,
  the reason vocabulary with `State`'s accessors in `state.go`, and everything that happens over a
  `Connection` — building it, its raw-path request, classifying a failure, the transport
  keepalive — in `connection.go`.
- **Resolver deps are always non-nil** — the composition root wires every field; tests use fakes.
- **Pub/sub**: two modules, split on whether delivery is **keyed**. Unkeyed → `github.com/amorey/gochan`: `watch` for latest-value current-state streams (current snapshot on subscribe: auth `State`), `broadcast` for fan-out where subscribers supply their own snapshot (poke). Keyed → `github.com/amorey/gobus`: `watch` for a keyed latest-value bus. Note the two `watch` packages differ on registration — gochan's hub holds a seed and delivers it, gobus's delivers nothing until the next send (a subscriber that has already read the current value can pass it as a baseline, which is measured against and never delivered back). Never hand-roll a subscriber map.
- **Work to do is a queue, not a bus** — `internal/workqueue`, one `Queue` per job: producers call
  `Add`, each worker goroutine loops on `Next`. Reach for it when a key names a pass someone must
  run rather than news everyone should hear: a key goes to **one** worker, queued work survives
  having no worker running, a key waits once however many times it is added, and one added while a
  worker holds it is queued afresh on `Done` rather than folded into a pass that could not have
  seen it. A bus gets all four wrong for this job — which is what the `kubeconn` presence queue was
  built out of, and where two of them were found. `Done` is owed for every key taken, or that key
  never comes back.
- **Subscription resolvers** return a channel emitting the current snapshot first, then deltas (`mapStream` in `graph/util.go`). Honor `ctx.Done()`. A resolver over a `*clustersvc.Stream` goes through **`watchStream`** (`graph/watch_failure.go`), never `ptrStream` — see below.
- **Unexported functional options** for test seams (`auth`/`cloud`/`prefsync`/`poke`): exported `New` takes production knobs only; `newWithOptions(cfg, opts...)` is reachable only from white-box tests.

## Tests & checks

- testify + `httptest`. Resolver-level tests stand up `graph.NewServer(&graph.Resolver{...})` + `POST /graphql`; h2c/lifecycle tests stand up `app.New(...)`. Filesystem via `t.TempDir()`.
- **A fixture that needs a stored status writes it with `beehive.NewAdminClient`**, never by registering a controller to do it: `clustersvc`'s `newClusterStatusDeps` stands in for the connection probe that way, and beehive stays stopped, so nothing reconciles behind the test. A controller's *own* status writes are asserted by calling `Reconcile` against a stubbed `ControllerClient` instead.
- **White-box tests by default** (`package foo`, not `foo_test`) — boundaries are kept by discipline, not the compiler. Escape hatch: external `package foo_test` only when pinning the public contract is the test's purpose — then say so in a comment.
- **No magic sleeps** (repo-wide — see the root `CLAUDE.md` for the rule and its two carve-outs). Block on the actual event, never a fixed `time.Sleep`. A cadence a test would otherwise have to outwait becomes a **parameter** whose production value is the constant — `prefsync`'s `withBackoff` takes `base`/`max` for exactly this — so a test picks its own timescale and never encodes the production number.
- **Waiting on a channel goes through `internal/testutil`**, which owns the one failsafe bound (`testutil.Timeout`): `Wait` (a done/ready channel), `Recv[T]` (the next value), `RecvClosed[T]` (the next receive must be a close), `WaitClosed[T]` (drain until close). Don't hand-roll a `select` with a `time.After` deadline. The exception is a **negative** assertion — "no frame arrived" — which needs its own short window, not the failsafe.
- **A fake that notifies the test uses `testutil.Signal` or `testutil.Probe[T]`**, never a hand-rolled channel. `Signal` (a `gochan/oneshot` pair) is single-shot: `Fire` is idempotent by contract, so a callback that runs many times needs no `sync.Once` and no `select`/`default` guard, and `Fire`'s bool tells the first call from the rest. `Probe[T]` is the repeating case: `Fire` never blocks (a fake that blocks stalls the code under test) and drops the **oldest** on overrun, because the event a test waits for is the newest — which is exactly what a `select`/`default` send throws away. `Await`/`TryAwait`/`Drain`/`Chan` are the read side.
  - The exception is a consumer that does **edge detection**. `internal/cloud`'s auth subscriber swallows its first value as a baseline and acts on the next *change*, so its fake must deliver every state losslessly: a latest-value hub (`gochan/watch`, which is what the real `auth` service publishes through) or a drop-oldest `Probe` can both coalesce the seed with the change and hide the edge. Its `fakeAuth` keeps a plain buffered fan-out, and says so.
- `make test-go` (`cd sidecar && go test ./...`); `make lint-go` (gofmt); `make vet-go` (`go vet`). Run `gofmt -w` before committing.

When you change the sidecar's schema workflow, wiring, or conventions, update this `CLAUDE.md` in the same change.
