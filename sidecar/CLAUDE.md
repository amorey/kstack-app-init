# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (parent gone).

The data dir (`--data-dir` / `KSTACK_DATA_DIR`) is **required** — `app.New` errors when empty; tests supply `t.TempDir()`. `<data-dir>/app.db` is owned by `internal/appdb` (one migration sequence; add app-level tables as numbered migrations in `appdb/migrations/`, never a second embed against the same file).

## Layout

Mirrors the kubetail layout: `main.go` is lifecycle only, `internal/app` is the composition root + routing, GraphQL lives in `graph/`. There is no `server` package.

- `main.go` — parse flags, bind socket, build `*app.App`, serve, drive graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `stop(ctx)` → `app.Close`).
- `internal/app/` — **composition root**: builds `poke.Service`, `kubeconfig.Service`, `clustersvc.New(...)`, `auth.Service`, `cloud.Service`; wires `graph.NewServer` + `grpcserver.NewServer`; multiplexes both onto one h2c handler (dispatcher keyed on `grpcserver.IsGRPCRequest`). `App.Start`/`App.Close` compose `App.parts` through `lifecycle.StartAll`/`CloseAll`: the slice is start order (poke → kubeconfig → cluster → cloud), and stop and close reverse it, so poke's hub closes **last**, after its subscribers drain. **kubeconfig before cluster is load-bearing** — `kubeconfig.Service.Start` reads synchronously, so every cluster reconcile observes a read config, and `app_test.go` pins it. Poke and cloud enter the slice as `lifecycle.StartFunc`. The two transports stay out of the slice — they shut down through `NotifyShutdown`/`DrainWithContext`, and `grpcServer.Stop()` runs first in `Close`.
- `graph/` — `schema.graphqls`, generated code, resolvers, `server.go` (gqlgen handler, bearer-token plumbing, SSE shutdown lifecycle). Resolver deps must be non-nil — tests wire fakes; degraded behavior lives inside the services, not behind nil-guards.
- `grpc/` — gRPC surface: `AuthService` (`StartLogin`/`Logout` unary; `AuthStateWatch` server-streaming, joins the drain WaitGroup) and `PokeService` (unary `Poke` → `poke.Poke(SourceHost)`). Committed protoc output in `grpc/authpb/`, `grpc/pokepb/`; regenerate with `make proto`; **never hand-edit `*.pb.go`**. `IsGRPCRequest` lives here — it *is* the definition of a gRPC request.
- `internal/` — `ipc` (per-OS user-only endpoint), `atomicjson`, `logging`, `sqlitemigrate`, `appdb`, `poke`, `kubeconfig` (the one reader of the user's kubeconfig), `drain`, `lifecycle` (the start/stop/close shape every level wears), `workqueue` (keyed work, delivered to one worker), `probe` (the probe engine — periodic observations over subjects, scheduled by derivation; `clustersvc`'s `kubeconn` runs on it), `testutil` (test-only helpers, imported by no production code), plus the subsystems below.

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
                      found — leases, the five probes, and the connection itself. A leaf
                      under internal/, so the compiler keeps it this package's own
  internal/kubecatalog/  the discovery sweeper: a second probe engine over per-catalog
                      subjects, the sweep, and the change signal that re-runs each
                      catalog's fold. Borrows kubeconn's leases; same leaf rule
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
cache is still the active identity). Served: the whole `Clusters()` family, the whole
`CachedCatalogs()` family, and everything on `Caches()` but its two gauges and `Clear`.
That is enough for the kube-context picker, which reads
`clustersWatch` alone. **A cache now exists at runtime**: the serverUID probe writes `status.server.uid`, which is what
`ensureClusterCache` keys off, so a reachable cluster whose credentials can read `kube-system` gets
one — and a `ClusterCachedCatalog` beneath it. **The catalog's pass discovers kinds**, so a
`ClusterCachedResource` exists per kind the cluster serves, and its six reads answer.
Nothing below that: no sync worker and no on-disk cache, so `CachedResources().Clear` and the whole
`CachedData()` family still serve nothing, and every `ClusterCachedResource` carries a `Synced`
condition nothing writes.

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
`catalogWatch`). Add a kind by writing those three, never a fourth pump — the bookmark discipline is
a protocol rule, and a per-kind copy is a place for it to be got wrong. The pump's own rules are
tested once, in `stream_test.go` over a stand-in kind; a kind's tests pin its projection and its
departure.

**A controller owns its kind's machinery**, and `service` holds the controllers only to drive their
lifecycle. None has any yet — all five embed `lifecycle.None` — but the leaves a controller grows
land there rather than on `service`, or the composition root accumulates every kind's detail.
`registerControllers` builds and registers all five, returning them in registration order. All register with
`startupPass` (`WithStartupFullPass(true)`): each owns state a restart invalidates and the store
reads as settled, since the generation was observed by a process that is gone. **`ClusterSource`
also registers `sourceResync`** (`WithIndividualPassInterval(clusterSourceResyncInterval)`),
the poll its correctness rests on: it reads a file the store cannot see, so a lost trigger poke is
a change nothing else would report. **`Cluster` takes `clusterResync`** for the same reason
— what its probe reports is a remote server's, so nothing in the store moves when the answer does —
and **`ClusterCachedCatalog` takes `catalogResync`**, the third: what its fold reads is the
sweeper's in-memory answer, which the store cannot see move — the kubecatalog trigger makes the
fold prompt, and the resync covers a signal that went missing. The other kinds are woken by a spec
write or a dependency edge.
→ [ADR: beehive control plane](../docs/adr/2026-08-09-beehive-control-plane.md).

**Discovery is a sweep on its own engine (`internal/kubecatalog`); the catalog pass only folds
it.** A second `probe.Engine` with one probe, subjects keyed by the catalog record's beehive name
and armed from its reconcile — `Track` while the record wants discovery, `Forget` on pause and
teardown. **Arming is policy, not interest**, which is why this is not a refcounted lease pool
like kubeconn: a reader must never re-arm a sweep the user paused. The sweep borrows the context's
connection through the service's own `kubeconn` lease (refcounted beside every other holder's),
commits only on a change, suspends while the context resolves to nothing (the kubeconn bridge
wakes it the moment the pool reaches the server), and `Subscribe` signals the ids whose news moved
— `newKubecatalogTrigger` maps that signal straight onto the record, the id being the name. The
subject is bound to **a server, not just a context**: `Track(id, contextName, serverUID)`, and it
asks the pool for that server's connection (`Lease.ConnFor`), suspending with `IdentityMismatch`
when it is not. The
sweep's 10m interval is the correctness bound — a poll, since nothing here sees a CRD land.

**Promptness is a watch that only wakes.** Every run that resolves a connection stands a watcher
up over it, before sweeping: one stream each on `customresourcedefinitions` and `apiservices`,
the two things that change what a cluster serves. A change wakes the sweep and the event is
dropped — the sweep reads current state. **The watch's precondition is the connection, not the
answer**, so it is reconciled on every pass; deferring it to a clean sweep would leave a replaced
connection's watcher standing for as long as discovery keeps failing. Four rules keep it cheap
and safe:

- **A stream never opens without a version.** A watch given none *replays* — the server streams a
  synthetic `Added` for every object that already exists — and each one reads here as a change, so
  every establishment would wake the sweep that is about to read the same state. `openCollectionWatch`
  therefore reads the collection's current version first (`List` with `Limit: 1`, the objects
  dropped) and starts there. **Two requests per fresh stream, against a discovery pass of one per
  group-version**, which is what that buys.
- **A clean end resumes; it does not sweep.** Each stream remembers the resourceVersion of the
  last event it saw and reopens from it, and `AllowWatchBookmarks` is what keeps a quiet stream
  resumable. Without this every server-side timeout (~5m, two streams, own clocks) would be
  treated as a gap and swept — 2-4x the pull-only baseline, to learn nothing. **A `Bookmark` never
  wakes**, or the sweep would run on the bookmark cadence and cost more than not watching at all.
  Reopening is paced by `reopenDelay`, since nothing else paces it and a proxy can hang up as fast
  as it accepts; the resume means that wait costs latency, never events.
- **Only an end that proves a gap wakes**, which is the server refusing our version for being too
  old (`IsResourceExpired`/`IsGone`, in `errorEnd`). A re-list is the only answer to a gap of
  unknown size, and only the sweep can do it. Every other end is silent and waits for the next
  sweep: a stop, an end before any version was known, a refused open, **and any other watch error**.
  **Waking on those loops** — the sweep's answer to a dead watch is to stand another one up, and
  they repeat (RBAC on a cluster-scoped collection repeats exactly; so does a server erroring for
  its own reasons), so each buys a full discovery pass per turn with no committed answer to make
  the spin visible. The gap wake cannot loop for that reason inverted: its replacement starts from
  the collection's version as it is now, which has to age before it can be refused.
- **A resourceVersion never outlives its connection** — it is one cluster's etcd revision, and the
  next connection may be another cluster. It lives in the stream and dies with it.

**Watchers live on the `Service` beside `tracked`, under the same mutex**, and a watcher exists
only for a tracked id. Both halves hold that under one critical section: establishment checks
`tracked` and stores in the same one, and `Forget` drops the subject and the watcher in the same
one, stopping it after the unlock. A run is on a worker, so `Forget` can land under it and the
finishing sweep would otherwise leave a goroutine and two streams standing for an id nothing
tracks — the engine's commit refusal does not cover it, because establishment is not a commit.
**Splitting the teardown is the easy way to lose this**, since stopping a watcher waits for its
streams: drop it before the subject and the window is as long as the API server takes to hang up.
Every refusal stops the watcher too: `conn.Done()` misses the conflicted case, which never
retires.

**A standing watcher is kept only while it is live and over the sweep's own connection** —
`ensureWatcher` compares both, and replaces otherwise. Nothing else re-establishes one, so
either state read as "a watch already stands" costs that cluster its promptness permanently: a
watcher whose stream hit a gap marks itself **spent before it wakes** (the wake runs the sweep
that must replace it), and a watcher over a connection since replaced holds an HTTP watch on
retired credentials that the streams never notice.

**A context is not an identity, and identity lives on the connection.** Re-point a context at
another cluster and the pool hands out whatever now answers, while the only thing that disarms the
superseded cache's subject is a pause flip three reconciles downstream — and the pool wakes every
subject over a context whose identity moved, so that stale sweep is the *first* thing to run
against the new server. `Connection` therefore carries a **set-once `serverUID`, stamped by
`serverUIDProbe` when it reads one over that connection**, and `Lease.ConnFor(ctx, serverUID)`
answers from it.

**Never correlate a connection with `State.ServerUID`** — that is the trap this shape exists to
close, and reading both from one snapshot does not close it. `serverUID` is its own probe,
*queued* by a committed connection rather than applied by it, so the engine legitimately holds
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

`clusterCachedCatalogController.Reconcile` walks its two owner edges (cache, then cluster), arms
or disarms the sweeper, and rewrites one `ClusterCachedResource` per kind the standing answer
names. **No pass dials, and none needs extra workers** — the sweep's concurrency is the
kubecatalog engine's worker bound. The rules, each carried by a reason in the `Discovered`
vocabulary:

- **`DiscoveryPartial` adds without pruning.** client-go returns partial results *and* an
  `ErrGroupDiscoveryFailed` when an aggregated API server is down. A group that went quiet has not
  stopped being served, and deleting its children would stop live workers over a transient outage.
  The probe commits the partial list and fails its run, so its ladder retries sooner than the
  interval.
- **`DiscoveryFailed` leaves the children alone** and **settles**: the standing answer keeps
  converging, the condition carries the sweep's failure message, and retrying the sweep is the
  probe's own ladder, not beehive's.
- **`DiscoveryDraining`** is a served kind whose name is still held by an earlier prune's tombstone
  (`ErrDeletionPending`) — a state to come back to, not a failure. The one path that still
  requeues (`catalogRetryInterval`), since a tombstone releasing its name is not an event anything
  reports.
- **`Paused` disarms the sweep** and only relays the switch onto the children already there. The
  anchor lives as long as the cache, so its subtree survives a pause rather than being rebuilt.
- **`NoConnection`** settles with no requeue: the sweep is suspended on its claim, the bridge
  re-runs it on recovery, and its signal re-runs this fold. The outage itself is the cluster
  pass's to report.

A kind is mirrorable when it is not a subresource and carries both `list` and `watch`; the
`events.k8s.io` spelling is dropped so one event store is not cached twice. The filter lives with
the sweep, in kubecatalog.

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
asserted from the run, never inferred from a countdown that has run out — but the engine publishes
only on a pass, so the in-flight window is not observable yet; see `TODO.md`.

**A claim outlives what it is a claim on.** The file can stop naming a context while a holder
still holds it, and the entry stays — only releasing drops one. An **unread** kubeconfig names
nothing and is deliberately not a departure: saying so would report every context gone for as long
as the first read takes. `stateHub` is published before `signalHub`, so a reader the signal wakes
finds the value already there.

#### The probes (`probe.go`) over the engine (`internal/probe`)

**The scheduling machinery is `sidecar/internal/probe`** — a reusable engine (a work queue, a
level-triggered pass, a schedule derived from recorded state) that knows nothing about
kube-contexts. A probe is a struct implementing `probe.Probe[T]`, registered with
`probe.Register(e, name, p, opts...)` — the same shape as a beehive controller, with `T` inferred
from the instance — and `T` is its observable's value type. **The registration name is the
probe's whole public identity**: the edge options, `Wake`, and every read take one, and
`Register` returns nothing. `kubeconn`
keeps what is asked and what the answers mean: `probe.go` is `registerProbes` — five
registrations kept side by side on purpose, since the set's rules are checked by eye — plus the
probe structs; `service.go` is leases and publishing. → [ADR: probe
engine](../docs/adr/2026-08-24-probe-engine.md).

**A value the engine drops goes back to the probe.** A committed value can own something — a
connection, a file — and one the engine never applies is one nothing else can reach to release: a
commit refused because the subject was removed mid-run, a run that concluded `Skip`, one that
returned the zero `Result`, one that panicked. A probe implementing `Discard(T)` is handed it
(`kubeconn`'s connection probe retires the connection); one that does not is unaffected.

**A `Run` body may not take the engine down with it.** One that panics, or that hands back the
zero `Result`, is recorded as an `Internal` failure and gives its key back — the engine logs it
through `slog`, the only place it logs at all. Nothing else reports a bug in a body, and leaving
one unrecorded wedges the probe twice over: in flight forever, with its key held in the queue.

**Each of `State`'s five observations has one probe behind it**, registered with its own interval
(a cluster's UID never moves; its readiness moves constantly). The engine owns the observables —
one value beside one `Attempts` per probe, the value written by that probe's `Run` alone — and
`Read`/`OnPass` hand them back as a `probe.Snapshot`, frozen at the moment it was taken.
Anything reads one out of it by registration name (`probe.Get[connInfo](snap, nameConnection)`,
the `name*` constants), which is how a `Run` reads a sibling and how `stateOf` assembles `State`
at publish time. **A `probe.Key[T]` states that name↔type pairing once** rather than at every
read site — `keyConnection.From(snap)`. It is a freestanding declaration: registration never
hears about it, and the pairing is checked where `Get` checks it, when a value lands. The
connection is the only observable another probe reads, so it is the only one keyed. Its value
(`connInfo`) bundles `departed` and the connection with the endpoint; `stateOf` projects only the
endpoint into `State.Connection`, and `newsOf` walks `probeNames` for the untyped per-probe read.

**A `Run` takes a `probe.Pass[T]` and returns only its `Result`.** The pass carries the run's
inputs — `Subject()`, `Prev()`, `Known()`, `Snapshot()` — and `pass.Commit(v)` records what the run found,
wherever in the body it learns it. The engine buffers that and applies it when the run returns,
in the same critical section as the attempt: nothing is published mid-run, the last call wins,
and a run that then concludes `Skip` or panics commits nothing.

**`Known()` is what a probe whose zero `T` is an answer needs.** `Prev()` cannot tell "nothing has
landed" from "the last answer was the zero value", and the engine dates an observation by its
*value* — so readiness (healthy is the empty `ComponentStatus`) would never commit, and a cluster
that has never had a failing component would read as never observed. Its guard is
`!pass.Known() || the set moved`.

**Commit only on a change.** A committed value is what tells the engine the value moved, and so
what re-runs every probe watching it — commit unconditionally and the four behind the connection
re-run every cycle, which is the intervals they are registered with undone. The engine never
compares (it holds values as `any`, and a probe's value may be uncomparable or carry funcs), so
the guard is the body's: `connInfo` is comparable, so it is `if next != pass.Prev()`.

**A probe's result is its schedule** — `Succeeded` (due again after the interval), `Fail` (due up
the backoff ladder), `Suspend` (nothing due until a `Wake`), `Skip` (record nothing; wait for a
`Wake`). The four behind reachability declare both edges on it — `probe.WithDependencies` (they
cannot run without a connection) and `probe.WithWatches` (they read the one it commits). The
engine records them as `DependencyFailed` rather than dialing while the connection has not
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

**Wiring**: `Acquire`'s first holder is `engine.Add`; the last `Release` is `engine.Remove`,
under `Service.mu` so a stale release cannot remove the subject a fresh claim just added; the
kubeconfig watch is `engine.WakeAll(nameConnection)` on every change — every claimed context
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

**Publishing is the engine's `OnPass`** — after every pass, outside the engine's lock,
serialized per context. Two publish rules, because the two feeds answer different questions:
`stateHub` carries every pass (the timing is what a claim watcher subscribed for, and the
countdown to the next run is visible nowhere else); `signalHub` fires only when the **news**
changed — `departed`, `Phase()`, `Identity()`, each probe's `OK()`, never a timestamp — measured
against `Service.published`, what the fleet was last told. State first, so a reader the signal
wakes finds the value already there. A claim reads through `engine.Read`, with the entry-identity
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

**Waiting for a usable connection is `ReadyFor`/`AwaitConnFor`**, not a hand-rolled loop. Neither
`Done()` nor a state frame is the signal an identity-scoped holder needs: retirement puts the
replacement in the observable *before* `Done()` fires, but that replacement is unstamped for a
round trip after, so `ConnFor` refuses through the window. `ReadyFor` returns a channel closed
when a connection vouching for the uid exists — already closed when one does, so the steady state
costs no goroutine — and `AwaitConnFor` is that plus the re-check, since the close is an edge and
the connection can move again. **Free functions over `Lease`, never methods**, so no fake can get
the attach-before-check ordering wrong; a waiter lives until it fires or ctx ends, so bound it
with the work's context. **Neither may be called from a probe `Run`** — blocking holds an engine
worker, which is why `kubecatalog` refuses-and-suspends instead, woken by the fleet bus.

**One context, one entry.** `Service.claimed` is a single map keyed by context name — also the key
both hubs publish under — holding the holder count, whether the file still names the context, and
what a probe read. Contexts resolving alike are **not** merged. → [ADR: one connection per
context](../docs/adr/2026-08-23-one-connection-per-context.md).

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
catalog that actually owns those records — `service.catalogIDFor` resolves that anchor from the
cache id (a point read on the derived name), precisely so callers, who only ever hold a cache id,
never have to. The schema keeps the catalog out of the path for the same reason. **A cache with no
anchor yet reads empty, never an error** — nothing can hang off an anchor that does not exist — and
the scoped watch answers it with `deltaWatch.streamEmpty`, the bookmark alone, since an unopened
collection is definitively empty rather than pending.

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

The schema **is** the Go shape — every GraphQL type binds 1:1 by name to its `internal/clustersvc` type in `gqlgen.yml`; no projection layer. Resolvers are one-liners delegating to a family on `r.ClusterSvc` (e.g. `r.ClusterSvc.CachedData().WatchObjects`; the field is named `ClusterSvc` to avoid shadowing the generated `Clusters` method). The whole surface below is intact in the schema and in the resolvers, but **only the `Cluster` surface and the `ClusterCache` reads answer** — `cluster`, `clusters`, `clustersWatch`, `clusterScheduleWatch`, the enable/sync/delete/`clusterConnectionRetry` mutations, `clusterCache`/`clusterCaches`/`clusterCachesWatch` (with `Cluster.caches` alongside them), and the `ClusterCachedCatalog` reads. `Cluster.events` does not: it reaches `ListEvents`, which still panics, so a query selecting it panics with the rest. Neither do the cache gauges, which are unbuilt. This section is the contract the rebuild must satisfy rather than a description of what answers today. Key entry points:

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

- **A type's methods live in the type's file.** Splitting them across files means a reader has to
  find the pieces before they can see what a type does. So a file that earns its place owns a
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
