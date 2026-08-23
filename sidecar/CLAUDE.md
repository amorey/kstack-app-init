# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (parent gone).

The data dir (`--data-dir` / `KSTACK_DATA_DIR`) is **required** — `app.New` errors when empty; tests supply `t.TempDir()`. `<data-dir>/app.db` is owned by `internal/appdb` (one migration sequence; add app-level tables as numbered migrations in `appdb/migrations/`, never a second embed against the same file).

## Layout

Mirrors the kubetail layout: `main.go` is lifecycle only, `internal/app` is the composition root + routing, GraphQL lives in `graph/`. There is no `server` package.

- `main.go` — parse flags, bind socket, build `*app.App`, serve, drive graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close`).
- `internal/app/` — **composition root**: builds `poke.Service`, `kubeconfig.Service`, `clustersvc.New(...)`, `auth.Service`, `cloud.Service`; wires `graph.NewServer` + `grpcserver.NewServer`; multiplexes both onto one h2c handler (dispatcher keyed on `grpcserver.IsGRPCRequest`). `App.Start`/`App.Close` compose `App.parts` through `lifecycle.StartAll`/`CloseAll`: the slice is start order (poke → kubeconfig → cluster → cloud), and stop and close reverse it, so poke's hub closes **last**, after its subscribers drain. **kubeconfig before cluster is load-bearing** — `kubeconfig.Service.Start` reads synchronously, so every cluster reconcile observes a read config, and `app_test.go` pins it. Poke and cloud enter the slice as `lifecycle.StartFunc`. The two transports stay out of the slice — they shut down through `NotifyShutdown`/`DrainWithContext`, and `grpcServer.Stop()` runs first in `Close`.
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go` (gqlgen handler, bearer-token plumbing, SSE shutdown lifecycle). Resolver deps must be non-nil — tests wire fakes; degraded behavior lives inside the services, not behind nil-guards.
- `grpc/` — gRPC surface: `AuthService` (`StartLogin`/`Logout` unary; `AuthStateWatch` server-streaming, joins the drain WaitGroup) and `PokeService` (unary `Poke` → `poke.Poke(SourceHost)`). Committed protoc output in `grpc/authpb/`, `grpc/pokepb/`; regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here — it *is* the definition of a gRPC request.
- `internal/` — `ipc` (per-OS user-only endpoint), `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `kubeconfig` (the one reader of the user's kubeconfig), `kubeconn` (a pool nothing builds — see below), `drain`, `lifecycle` (the start/stop/close shape every level wears), `testutil` (test-only helpers, imported by no production code), plus the subsystems below.

## gRPC + GraphQL over one socket (h2c)

`internal/app` owns the topology (that two surfaces share one socket); `grpc/` owns the predicate. HTTP/1.1 GraphQL POST + SSE are untouched. An idle `AuthStateWatch` survives the 60s `IdleTimeout` via gRPC keepalive pings. The cluster surface is **GraphQL-only**. → [ADR: single-socket h2c](../docs/adr/2026-08-09-single-socket-h2c.md).

**Shutdown order** (from `main.go`): `app.NotifyShutdown()` (gRPC streams end on their serving context; each SSE request's context is cancelled per-request) → `srv.Shutdown` → `app.DrainWithContext` (waits both sub-servers' WaitGroups — essential for hijacked h2c gRPC streams `srv.Shutdown` can't see) → `stop(ctx)` → `app.Close()` (`grpcServer.Stop()`, then `lifecycle.CloseAll(a.parts)`). Traps: grpc-go's `GracefulStop` **panics** on the h2c path — `Stop` only runs after the drain; never cancel via `http.Server.BaseContext` — it would tear down the shared h2c connection carrying gRPC mid-stream.

## Cluster subsystem (`internal/clustersvc`)

**Mid-rebuild.** The layout — six beehive kinds, four of which are GraphQL families:

```
internal/clustersvc/
  service.go          the whole API — Service + the five family interfaces — plus the
                      accessors, beehive bootstrap, and registerControllers
  clusters.go         ┐ one per family, implementing its interface and holding
  caches.go           │ everything else about that kind: its beehive shapes, the
  cachedcatalogs.go   │ record GraphQL binds, its *WatchFrame, its controller, and
  cachedresources.go  │ the machinery that controller owns
  cacheddata.go       ┘ (no controller — the one family that isn't a beehive kind)
  clustersources.go   the ClusterSource kind: one discovery anchor per source variant.
                      A beehive kind with no GraphQL type behind it — internal
  triggers.go         the feed→wake bridge: maps a source's vocabulary onto the beehive
                      names the trigger declared at registration requeues
  shared.go           vocabulary every family reuses, the app's services as this
                      package sees them, and the two GraphQL scalars
  stream.go           Stream[T]
  internal/kubeconn/  the connections a cluster is talked to over, and what probing them
                      found. A leaf under internal/, so the compiler keeps it this
                      package's own
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

Built so far, produced: the `ClusterSource` anchor whose pass creates `Cluster` records,
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
lifecycle. No kind has any yet — all five embed `lifecycle.None` — but the leaves a controller grows
land there rather than on `service`, or the composition root accumulates every kind's detail.
`registerControllers` builds and registers all five, returning them in registration order. All register with
`startupPass` (`WithStartupFullPass(true)`): each owns state a restart invalidates and the store
reads as settled, since the generation was observed by a process that is gone. **`ClusterSource`
also registers `sourceResync`** (`WithIndividualPassInterval(clusterSourceResyncInterval)`),
the poll its correctness rests on: it reads a file the store cannot see, so a lost trigger poke is
a change nothing else would report. **`Cluster` takes `clusterResync`** for the same reason
— what its probe reports is a remote server's, so nothing in the store moves when the answer does.
The other kinds are woken by a spec write or a dependency edge.
→ [ADR: beehive control plane](../docs/adr/2026-08-09-beehive-control-plane.md).

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
(`kubeconfig`, `kubeconn`, `poke`), built once by
`newDeps(bh, kubeconfigSvc, kubeconnSvc, pokeSvc)` and **embedded** by `service` and by each
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
`ensureClusterCachedCatalog`), and the same shape carries on down the chain. Distinct from a
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

`Connected` carries the finding's own reason: `Inactive` when the cluster is switched off **or its
context left the kubeconfig** (both states the user chose), `ResolveFailed` when the context is
there and its entries will not resolve (a broken file, reported on the record rather than failing
the pass, since beehive's backoff cannot fix a file), `ProbeFailed` when the server would not
answer, and `Connecting` until a probe lands — which is every context that resolves, today.
`Inactive` and `ResolveFailed` come from `Acquire` refusing the claim, the rest from the claim's
`State`. The other two derive their own: `NoConnection` where a probe never got to the server, since
neither readiness nor identity is a fact about a server nothing reached.

`foldState` copies what the pool knows into `status.server` (`uid`, `version`, `endpoint`) and
`status.principal` (`username`, `groups` — sorted, so a re-ordered read is not a change). It
decides no retention of its own — an `Observation` already keeps its last answer through a failure
— so a check that has never answered (`Known()` is false) leaves its field alone, which is what
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

**Nothing dials yet.** `clustersvc.New` builds it and carries it as a `lifecycle.Part` ahead of
beehive — closing drops sockets, so a connection has to outlive every pass that could still be
dialing on it. `Acquire` panics; the pool and the probe behind it land next, drawn from the
worked-out `internal/kubeconn`.

**The leaf's exported types are the boundary's**, aliased rather than copied: `clustersvc.Lease`,
`Connection`, `ConnIdentity`, `ConnState`, `ConnStateSubscription`. Aliases because an
`internal/` type cannot be *named* outside, which would leave `Service` unimplementable by the
resolver tests' fake. The layering exception is in `service.go`'s package doc.

**A connection is scoped to one `Identity`**, so any of its three fields moving — server, version,
user — retires it and builds another, and the field is stable for the connection's life. Comparing
is the holder's (`Identity` is comparable); the pool's key is credentials, which do not move when a
cluster is rebuilt behind them. `Identity` carries no errors, which is what keeps it comparable —
why a field is missing belongs on the `Observation` that could not read it. Note what a username change does
**not** cover: ordinary RBAC edits leave it identical, so permissions need the
`SelfSubjectRulesReview` behind `ClusterPermissions`.

**`State` is what the last probe read about the server, not the connection's own life** —
whether one is built or retiring surfaces on `Connection.Done()`. **Five checks that fail and go
stale independently.** A cluster is rebuilt, upgraded, re-issues a token, or revokes a namespace
read, and none implies the others — so `Connection`, `Readiness`, `ServerUID`,
`ServerVersion`, and `Principal` are each an `Observation[T]`. Only reachability is a prerequisite; the rest are peers.

**An `Observation` keeps its value through a failure** — a read that stops being permitted does not
mean the fact changed — and `LastSeen` is what makes the survivor readable: *identified, as of
10:00* is usable where *ready, as of 10:00* is not. Beside the value it holds two `Attempt`s and a
failure run: `Failures` with `FailingSince`, because the ladder widens and a count does not give
elapsed time. `Known()` is has-ever-answered, `OK()` is answered-last-time, `InFlight()` is
running-now.

**`Attempt` is one run at any stage of its life** — `ScheduledAt`, then `StartedAt`, then
`FinishedAt` and the outcome. One type, filled in order, which is why an unfinished run needs no
second one: `LastAttempt` is the run that finished, `NextAttempt` the one that has not, and a run
moves between them as it completes. `ScheduledAt` is separate from `StartedAt` because a saturated
prober lets a scheduled time slip into the past, which a single stamp compared against the clock
would read as running.

**A check that has never run is the zero `Observation`** — a zero `LastAttempt` is not `Done`, so
every accessor answers correctly with no sentinel.

**A zero `NextAttempt` means the check is suspended**: nothing is due and the last answer stands
(`Scheduled()` is the accessor). The four checks behind the connection suspend while it is down —
a server nothing reached cannot answer them — and re-arm when it recovers; a check that came back
`Unsupported` stays suspended for the connection's life, since the endpoint is absent rather than
failing. `DependencyFailed` marks the one cycle where a check went from running to suspended, and
the cycles after it schedule nothing, which is what makes a dead cluster cost one timeout per cycle
instead of one per check. **Why a check is suspended is `LastAttempt.Reason`** — no field beside
`NextAttempt`, since a check suspends over what its last attempt found. That is why suspending must
write an attempt instead of going quiet. So *ready, as of 10:00, nothing due* is a state to render, not a stall.

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
rules of ours — because a caller asks why a check failed once, not three times. Names shared with
`metav1.StatusReason` are the same word for the same thing; the set is not that set.

Two prober traps live here. `NotFound` and `Unsupported` **both arrive as a 404** — the object was
missing versus the endpoint is not served — and only the check knows which it asked for, so
classifying on the code alone permanently suspends a check that should keep running. And `Dynamic`
returns `*apierrors.StatusError` carrying the API's own reason, while only the raw endpoints
(`/readyz`, `/version`) leave a status code as the sole evidence; one switch over codes for both
discards what the typed half knows. `Canceled` says nothing about the cluster and counts toward neither failure field;
`DependencyFailed` is a check recorded rather than attempted, which is what keeps a dead cluster
costing one timeout per cycle instead of one per check. Free-form text goes in `Message`, never
`Reason`.

A `State` is a value copy, but a **shallow** one: the slices inside belong to the prober and every
watcher shares the backing array.

The pool owns the reading, so every holder agrees: `State.Phase()` is `Pending`/`Unreached`/`Probed`
off `Connection` (the trap it exists for — no attempt yet is not an attempt that failed), and
`State.Identity()` projects the three comparable scalars out of the rich observations. The verdicts
stay above: condition types, reasons, and `Inactive` are the record's vocabulary, not the pool's.

**Everything a holder learns comes through its `Lease`** — `Conn`, `State()`, `WatchState()` — so
the pool needs no index from a credential key back out to the contexts sharing it. `WatchState` is
a `gobus/watch` receiver current on attach, over the hub `Conn` parks on: one mechanism, and no
attach-before-read ordering left to each watcher. **Every value is a level, never an edge** — the
hub keeps the latest, so a reader that falls behind skips what came between, and transitions come
from the record's conditions and event timeline. A long-lived reader cannot see a field it is not
re-reading, so a retired connection closes `Connection.Done()` — retirement, not the replacement.

**What is pooled is keyed by credentials, not by clusters** — two kube-contexts aimed at one
server as one user are one socket and one probe. Nothing in the vocabulary of a cluster record
says so, which is why the package doc leads with it: do not read the location as one entry per
cluster. The resolve belongs here rather than at the caller, so what a connection was built from
and what it is stored under cannot disagree.

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
per-context kind whose spec the anchor writes (the shape `ClusterCachedResource` already uses).
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

The five families are `Clusters()`, `Caches()`, `CachedCatalogs()`, `CachedResources()`, and
`CachedData()`. **The `Cached*` prefix marks the cache subtree** — what a `ClusterCache` catalogs,
the per-kind records under that catalog, and the mirrored content itself — so the grouping is visible
in the accessor list rather than something you have to know. Keep it when adding a family there.

Rebuilding a family means replacing the panics in that family's file. Keep the method naming rule
when you do: **VerbNoun with the noun elided when it equals the family's subject**, so
`Caches().WatchList()` watches caches and `Caches().WatchStats()` streams one cache's stats.
**A family owns a read only when the read differs per record type.**
`RetryConnection`/`AcquireConnection` stay top-level (they answer about a connection, not a
record), and so do `ListEvents`/`WatchEvents`: an event carries no kind, every id is the same `ObjectID`, and only
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
- Mutations: `clusterEnabledSet`, `clusterSyncEnabledSet`, `clusterConnectionRetry` (returns immediately; outcome lands on conditions), `clusterCacheClear` (takes the **cache's own id**, since a UID migration leaves a cluster owning more than one: delete files then **bounce that cache's workers** — nothing else would rebuild them; they cold-sync, the cookie died with the file), `clusterDelete` (GC cascades to the cache; **refused with `ErrDeclaredBySource` for a record its source still declares**, since the discovery pass would re-import it under a fresh id and the new record would carry defaults rather than the user's toggles . **The guard reads the kubeconfig, not the record's observation**, which is only a cached view of it: status is nil for exactly as long as a just-imported record has not reconciled, and the webview renders such a record as orphaned (`isPresent ?? false`) — so its Remove button is live in precisely the window a status-only check would wave through. Status is the fallback while the file is unread, and a record with neither is refused, since refusing is recoverable and allowing is not).
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

## Kube connections (`internal/kubeconn`)

**Nothing builds this package.** No composition root constructs it and no code path acquires a
lease; it is the worked-out pool, kept here to be drawn from as
`internal/clustersvc/internal/kubeconn` fills in. A cluster is the only useful way to address a
connection, so the pool a caller reaches belongs behind that boundary — see [ADR: connections are
addressed by ClusterID](../docs/adr/2026-08-22-connections-addressed-by-cluster-id.md).

**Credentials are the unit, not clusters.** One entry per credential *key* holds the connection
and the probe that keeps it validated, so two kube-contexts aimed at one server as one user are
one socket and **one probe** — the identity they would each learn is the same answer. The caller
computes the key; this package never reads a kubeconfig and never learns what a cluster is.

A connection is built on first use for a key and shared by every later caller under it; `Close`
drops them all, and only idle sockets close, so a stream in flight is never cut. **There is no way
to obtain one without a `Lease`** — that is what lets the pool know when nothing needs a connection
any more, which reclaiming idle ones will depend on. **The build runs outside the pool's lock** (one `sync.Once` per key): it reads TLS material from
disk, and holding the lock across that would queue every other key — and every cache hit — behind
it, flattening the probe fan-out.

- **The caller supplies credentials; the pool supplies the tuning.** The build stamps `QPS`/`Burst`/
  `UserAgent` onto its own copy, because the key fingerprints credentials only — two callers under
  one key with different tuning would otherwise share whichever built first. `WrapTransport` is a
  chain (add with `cfg.Wrap`, never assign) and `ContentType` stays JSON, or the dynamic client
  cannot decode.
- **`QPS`/`Burst` reach the dynamic client alone.** The bucket lives in `rest.RESTClient`, not the
  transport, so every raw path on `Connection.HTTPClient` — the probe included — is unthrottled.
- **`BaseURL` comes from `rest.DefaultServerUrlFor`**, which derives the scheme from whether the
  config carries CA/client-cert data. `DefaultServerURL` with a hardcoded `defaultTLS` turns a
  plain-HTTP port-forward into `https://` and fails at the handshake.
- **`New(budget)` tightens the HTTP/2 keepalive** (`keepalive.go`, 10s/5s, only where unset). Here
  rather than in the composition root because the vars are read when a transport is built, and this
  builds them — a call the root has to remember is one that goes missing.

`Probe(ctx, conn)` reads a cluster's identity with three **concurrent** requests: `/version`, the
`kube-system` UID, and a `SelfSubjectReview`. **A refused request leaves its field empty rather than
failing the probe** — it reached the API server, which is the thing being probed — so a
namespace-scoped user's missing RBAC (403 on `kube-system`) does not read as a cluster that is down;
`Identity.UIDErr` carries why, and makes `Identity` uncomparable, so compare its fields rather than
the struct. **Two things do fail it**: a transport failure, and *every* request being refused, which
is credentials that no longer work (401 across the board) rather than a user who may not read much —
an identity with nothing in it is what a caller cannot tell from a healthy cluster.

### Demand keeps a connection validated (`lease.go`, `loop.go`)

Nothing dials or probes until something asks. `lease.go` is what a caller asks and reads;
`loop.go` is the engine behind it. Both asks name credentials:

- **`Acquire(cfg, key)`** returns a `Lease` — "I am using these credentials." A held claim arms the
  cadence; `Lease.Conn` is the connection and `Release` gives the claim back. Releasing the last one
  ends the loop after at most the probe it is on. The connection stays pooled; `Close` drops those.
- **`ProbeNow(cfg, key)`** asks for one probe and claims nothing, so unheld credentials are probed
  once and the loop ends again.
- **`State(key)`** is what is known: the last `Result`, and whether a probe is asked-for-and-unanswered.
- **`Subscribe(key)`** is a `gobus/conflate` receiver of one key's news, for a reader holding no
  claim; `Close` is the unsubscribe. The bus keeps a slot per key and coalesces, so a fleet probing
  at once neither blocks a probe loop nor loses a key behind a busier one. A probe publishes when it
  **starts and nothing is known yet** — the one stretch a reader cannot infer — and otherwise only
  when the answer **changed**, so nobody is woken every cadence to conclude nothing moved. The value
  is empty: it says `State` is worth re-reading, not what `State` now says, so a reader acts on what
  is current rather than a snapshot from when the probe landed.

- **The loop belongs to demand, not to pooling** (`loop.go`). Building costs a map entry; `demand` starts the
  goroutine and the loop ends itself once no claim and no queued ask remain. The exit decision and
  the flag that records it are taken under the same mutex `demand` takes, so a claim arriving as a
  loop winds down either keeps it or starts a fresh one — never neither, which would leave a caller
  parked in `Conn` with nothing left to answer it. The shutdown exits take `endLoop` instead, which
  is safe only there: `stopped` is set before the loops are cancelled, so no demand can start
  another.
- **A `Result` names no credentials of its own.** It is stored under the key it ran against, so a
  rotation cannot be told the wrong cluster's news — new credentials are a different key with their
  own entry and their own result. `Conn` hands back the entry's connection for the same reason.
- **`Probing` is asked-for, not running-now.** Set when the request is made, so a caller that asks
  and then reads `State` is not told nothing is happening while its request sits behind the
  semaphore (`Budget.Concurrency`, which stops a first install running a credential helper per
  cluster in the same second).
- **`Conn` reads and registers in one critical section.** Results ride a **`gobus/watch` hub keyed
  by credential key**, so a probe landing between the read and the registration is delivered
  rather than missed. `watch.Watch` calls no
  caller code, which is what makes registering under the service's own mutex legal. The **wake**
  (`entry.wake`) stays a 1-buffered channel: a single-consumer, coalescing "run again", not state to
  distribute.
- **`Conn` waits out a failure rather than returning it from the store**, taking *that* probe's
  result rather than a successful one. Returning a stored failure would leave credentials that were
  down at boot unreachable for the life of the process. It does not kick: the claim already has the
  loop probing, on the backoff ladder, and kicking per call would flatten it.

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
- **Pub/sub**: two modules, split on whether delivery is **keyed**. Unkeyed → `github.com/amorey/gochan`: `watch` for latest-value current-state streams (current snapshot on subscribe: auth `State`), `broadcast` for fan-out where subscribers supply their own snapshot (poke). Keyed → `github.com/amorey/gobus`: `watch` for a keyed latest-value bus. Note the two `watch` packages differ on registration — gochan's hub holds a seed and delivers it, gobus's delivers nothing until the next send (a subscriber that has already read the current value can pass it as a baseline, which is measured against and never delivered back). Never hand-roll a subscriber map.
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
