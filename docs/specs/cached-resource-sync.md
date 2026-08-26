---
title: Cached resource sync
scope: sidecar
status: Planned
---

# Cached resource sync

## Goal

Make each `ClusterCachedResource` real: a live mirror of one Kubernetes kind into the local
cache, with the `Synced` verdict, the on-disk store behind `ClusterCacheStats`, the whole
`CachedData()` family, the health fold, and the two `Clear`s. The wire contract is already
specified — the schema, the condition vocabulary (`Syncing`/`Watching`/`Stale`/`SyncFailed`/
`Paused`/`NoConnection`/`IdentityMismatch`), the sync-event reasons, and the `CachedData` frame
types all exist and wait on this. No schema change.

## Shape

The catalog is the template: the controller pass arms machinery in a leaf, folds its in-memory
answer into the record, and never dials. Two new leaves under `internal/clustersvc/internal/`,
speaking native vocabulary (GVR strings, context names, JSON rows) — a leaf reaching for a
`clustersvc` type gets an import cycle, same as kubeconn and kubecatalog.

```
clusterCachedResourceController.Reconcile      (cachedresources.go — replaces the no-op)
        │ Track / Forget / Read          ▲ trigger (signal → requeue by name)
        ▼                                │
internal/kubesync     one worker per (cache, GVR): list+watch → store, publishes an Observation
        │ writes / reads
        ▼
internal/kubestore    one SQLite file per cache: objects, events, cookies, counts, change broker
        ▲
        │ snapshot + broker subscription
cacheddata.go / caches.go reads          (CachedData family, WatchStats, WatchHealth, Clear)
```

## `internal/kubestore` — the on-disk cache

**One SQLite file per cache**, `<data-dir>/caches/<cacheID>.db`, its own migration sequence via
`sqlitemigrate` (the appdb rule: never a second embed against `app.db`). Per-cache files are what
make `Caches().Clear` "delete the file" and `Stats.Exists`/`Bytes` cheap. `Bytes` checkpoints (or
counts the `-wal`/`-shm` sidecars alongside the main file) — a bare stat of the main file swings
with checkpoint timing and reports a number that moves for no reason a user can see.

**The package exports a registry, not bare stores.** The broker is in-memory state on a store
handle, so the kubesync writers and the `cacheddata.go`/`caches.go` readers must share one handle
per cache — a per-cache open/refcount/close registry, living in `deps` beside the other shared
services. Everything resolves a store through it, including `Clear`, which is what makes the
close-before-delete sequencing below enforceable.

Tables:

- `objects` — uid PK; api_version, resource, namespace, name, creation_ts, raw_json.
  managedFields and the kubectl last-applied annotation are stripped at write time
  (`ClusterCachedDataObject.RawJSON` already promises this).
- `events` — Events are an ordinary synced kind written to their own table
  (`cachedresources.go` declares it), with the newest-window retention the events watch
  serves; a row aging out of the window emits `Deleted` carrying its last-known state.
  Aging out is not a write, so nothing emits it for free: the pruner runs on every event
  write (events keep arriving on a live cluster, so this covers the common case) plus a
  tick for a quiet one; the window size and tick are parameters with production constants.
  The per-kind count rollups exclude this table — `ClusterCacheStats`'s comment already
  promises events are not a catalog kind.
- `sync_cookies` — per (api_version, resource): the watch resourceVersion and a sweep
  generation for warm-relist pruning.

**The store owns the change broker.** Every committed write publishes a row-level delta, keyed by
cache and by (cache, apiVersion, resource). The broker lives in the store because the snapshot and
the deltas must agree on ordering, and the store's transaction boundary is the only place that
ordering exists — a reader subscribes, snapshots, folds by UID, emits the `Bookmark`, then replays.
Per-kind object counts are maintained here too, feeding `WatchKinds` and the stats rollup.

`ClearKind(cacheID, gvr)` deletes the kind's rows and cookie in place. `Clear(cacheID)` goes
through the registry: close every handle, delete the files (the `-wal`/`-shm` sidecars too), then
reopen — deleting under open handles does not fail on POSIX, it silently forks the world, with the
old handle writing to the unlinked inode while a reopened store starts fresh. A broker subscriber
alive at that moment has its stream ended with a reason and reconnects into the fresh store's
snapshot. Bouncing workers is still the caller's job; the registry only sequences the handles.

## `internal/kubesync` — the worker fleet

Kubecatalog's exported shape, so the fold reads symmetrically: `Track(id, params)` /
`Forget(id)` / `Read(id) (Observation, bool)` / `Subscribe()`. `id` is the
`ClusterCachedResource` beehive name; `params` is `{cacheID, contextName, serverUID, apiVersion,
resource, namespaced}` — `cacheID` because the worker's store lives at the cache's path and the
subject name embeds the catalog's object id, not the cache's; the controller has the cache in hand
from the owner walk. Arming is policy, not interest — the controller is the only armer, so nothing
a reader does can re-arm a kind the user paused.

The shape grows one entry kubecatalog's does not have: `Bounce(id)` / `BounceCache(cacheID)`,
restarting tracked workers in place from their held params. The two `Clear`s need it — the
boundary caller holds neither the subject names nor the params to Forget-and-re-Track, and a
`Track` of an existing id is an idempotent no-op, so nothing else restarts a worker whose cookie
just died — and the poke path is the same bounce across every subject.

**Not on the probe engine**: a sync is a standing push stream, not a periodic pass. Each subject
is a worker goroutine:

1. **Connection.** One refcounted `kubeconn.Lease` per context, shared by that cache's workers;
   every use goes through `Lease.ConnFor(ctx, serverUID)`. No connection or identity mismatch →
   suspend, publish that reason, wake on the kubeconn fleet bus (the kubecatalog bridge). A
   worker never blocks in `AwaitConnFor` while holding anything shared.
2. **Cold sync.** Paged `List` (limit + continue) via `Connection.Dynamic`, pages written in
   store transactions, cookie recorded. `SyncStart`/`SyncComplete` in the event vocabulary.
3. **Watch.** From the cookie, `AllowWatchBookmarks: true`; deltas write through to the store,
   which fans them out. A bookmark updates `lastLiveAt`; a watch quiet past the staleness
   threshold flips the observation to `Stale` without tearing anything down. The threshold —
   like the backoff base/max and every other cadence here — is a parameter whose production
   value is the constant, per the repo's testing convention.
4. **Ends.** `IsResourceExpired`/`IsGone` → warm relist: bump the sweep generation, write-all,
   prune rows the new list did not touch — the prune is where the store emits the `Deleted`
   frames a client needs. `ResyncStart`/`ResyncComplete` events. Any other failure → backoff
   ladder, `SyncFailed`/`SyncDegraded`. A resourceVersion never outlives its connection.
5. **Publish.** An in-memory `Observation` per subject — phase, reason, message, object count,
   `lastUpdateAt`, `lastLiveAt` — and a conflated signal **only when the news moves**
   (phase/reason, never a count tick or timestamp), so the fold is not requeued per event.
   Timing detail stays off the record: steady state must be silent.
6. **Pacing.** Cold lists go through a bounded start (semaphore or `workqueue`) so enabling a
   cache does not fire a hundred concurrent full LISTs. Standing watches are cheap and unbounded.

A poke subscription bounces workers in place — warm resume off the cookie — which is the behavior
the poke section of `sidecar/CLAUDE.md` already reserves for this controller.

## The controller pass (`cachedresources.go`)

Mirrors `clusterCachedCatalogController.Reconcile` one level down:

- **Deletion-pending, or the owner chain gone** → `Forget(name)`, `ClearKind` the rows, settle.
  The tombstone collecting is what frees the name the catalog's `DiscoveryDraining` requeue
  waits on — this pass is the other side of that handshake.
- **`Spec.Enabled == false`** → `Forget`, keep the data (pause is not clear), `Synced=False/Paused`.
- Otherwise walk the owners (catalog → cache → cluster; the catalog's `ownersOf` shape, one level
  deeper — extract the walk if it reads well shared), `Track` with the cluster's context and the
  cache's `ServerUID`, `Read` the observation, and write `Synced` from its phase. Condition and
  events grouped in one `Within`, as the cluster pass does.

Wiring in `service.go`: `newKubesyncTrigger` (the three-line `trigger[T]` shape — the signal id
is the beehive name) plus a `resourceResync` interval registration. The fourth kind whose truth
is in-memory state the store cannot see move, joining source, cluster, and catalog. The catalog's
`Enabled` relay already lands in this kind's spec, so no dependency edge.

## The read side

- **`CachedData()`** (`cacheddata.go`): each watch resolves the cache's store, subscribes to the
  right broker key, snapshots, folds, sends the `Bookmark`, then streams. Unopened cache → the
  `Bookmark` alone. Frames carry the specified provenance (`CacheID`; objects additionally
  `APIVersion`/`Resource`). `WatchKinds` joins the store's counts onto the **beehive
  `ClusterCachedResource` records** (a scoped owned-objects watch), not kubecatalog's
  observation: the records survive a restart and their lifecycle is the workers', while the
  in-memory answer is empty until the first sweep. Two async sources means the `Bookmark` waits
  for both snapshots — never claim a snapshot complete over frames still undecided (the
  delta-watch ADR's rule). That join needs `IsCRD`, which nothing
  records today — the sweep grows the bit (it can mark kinds whose group/resource a CRD names,
  over the same connection) and `toResourceSpecs` relays it into `ClusterCachedResourceSpec`,
  where the join reads it.
- **`Caches().WatchStats`** — file size plus the count rollup. **`WatchHealth`** — the read-side
  fold over kubesync observations grouped by cache; never a stored condition
  (`ClusterCacheHealth`'s comment argues why). It re-emits on the conflated signal **and** on a
  modest per-subscription cadence (a parameter): the gauge carries `LastUpdateAt`/`LastLiveAt`,
  which move in healthy steady state precisely when the signal is silent, and a gauge is exactly
  where moving numbers were exiled to — "steady state must be silent" is a rule about the record.
- **Both `CachedData` watches and the gauges return `*Stream[T]`, not bare channels** — an
  interface change to the family signatures (so "no schema change" survives), forced by the
  boundary's own rule: anything reading a fallible upstream returns a `Stream`, and a store being
  cleared or failing under a watch needs `Err()` to be distinguishable from a graceful end —
  the distinction the watch-failure ADR exists to carry to the client.
- **`Caches().Clear`** — stop the cache's workers, clear the store through the registry, restart
  them; they cold-sync, the cookie died with the file. **`CachedResources().Clear`** — the same
  per kind via `ClearKind`.

## Order of work

1. `kubestore`: file lifecycle, schema, stripping writes, broker, counts, clears.
2. `kubesync`: one worker end to end — cold sync → watch → resume → stale/failure — with the
   observation and signal.
3. The controller pass, trigger, and resync; delete each `TestUnimplementedBoundaryPanics` entry
   as its method lands.
4. `CachedData` reads over store + broker.
5. Gauges, the two `Clear`s, poke bounce.

Each step tests in the established style: a fake connection service for kubesync, `newRunningDeps`
for the fold, a stubbed `ControllerClient` for condition writes. When it lands: fold what is true
into `sidecar/CLAUDE.md`, write an ADR for store-per-cache and worker-not-probe (and the broker
placement below), delete this spec.

## Decided: the store owns delta fan-out

The snapshot/delta ordering a reader's list+watch handoff needs exists only at the store's
transaction boundary, so the broker publishes there. The cost is a store that is more than SQLite;
the alternative — workers publish, readers reconcile snapshots by UID and generation — would put
that reconcile subtlety in every reader. Carry this reasoning into the planned ADR.
