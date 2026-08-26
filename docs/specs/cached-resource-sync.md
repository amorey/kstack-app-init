---
title: Cached resource sync
scope: sidecar
status: In progress
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

**The schema is `main`'s, carried over verbatim** —
`sidecar/internal/cluster/cache/store/migrations/0001_init.sql` on `main`, along with the store
pieces built for it as each becomes needed (`rawcodec` for the zlib-compressed `raw_json`,
`resume_cookie`, the janitor). So: `objects` with the cross-kind materialized fields, `owner_refs`
and `labels` as joinable edges, `events` with its FTS index and count triggers, `status_history`,
`kind_catalog` (which carries `is_crd` and the CRD schema), trigger-maintained `kind_counts`, and
`cluster_meta` as the sync-bookkeeping bag — the per-kind resourceVersion cookie and warm-relist
generation live there, not in a table of their own. Two traps `main` already solved travel with
it: `auto_vacuum=INCREMENTAL` must be set on the fresh writer pool *before* migrations run
(SQLite silently ignores it once any table exists), and the writer pool caps at one connection.

The event window still needs its pruner named: aging out is not a write, so nothing emits the
promised `Deleted` for free. The pruner runs on every event write (events keep arriving on a live
cluster) plus a tick for a quiet one; window and tick are parameters with production constants.
Event counts ride the schema's own hardcoded `('v1','Event')` triggers and stay out of the
per-kind object rollup.

**The store owns the change signal, and it is a coalesced ping, not a row delta.** Writers notify
per-resource and per-bus after commit; a reader subscribes first, snapshots, and on each ping
re-reads and diffs by UID to produce its frames — `main`'s `writeBus` shape, and what the served
types are built for. The full read-side design lives in the cached-data spec
(→ docs/specs/cached-data.md, "Decided here"). Per-kind counts come from the schema's
`kind_counts` triggers, feeding `WatchKinds` and the stats rollup.

`ClearKind(cacheID, gvr)` deletes the kind's rows and cookie in place. `Clear(cacheID)` goes
through the registry: close every handle, delete the files (the `-wal`/`-shm` sidecars too), then
reopen — deleting under open handles does not fail on POSIX, it silently forks the world, with the
old handle writing to the unlinked inode while a reopened store starts fresh. A broker subscriber
alive at that moment has its stream ended with a reason and reconnects into the fresh store's
snapshot. Bouncing workers is still the caller's job; the registry only sequences the handles.
`Delete(cacheID)` is the same close-and-delete without the reopen, for a cache going away with
its record: the file is named for that id, so the cache's teardown pass is the last thing that
can find it. That pass calls `ForgetCache` first — the registry sequences handles, not writers,
so only a stopped worker cannot write through the store it is about to close — and the id is
refused by `Acquire` afterwards (`ErrDeleted`), since a straggler pass still holding a
pre-teardown view of the cache would otherwise open a fresh file nothing can name again.

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
`Track` of an existing id restarts nothing while its params hold, so nothing else restarts a
worker whose cookie just died. Params that moved are a different sync — a re-pointed context, a
kind whose scope changed — and a worker fixes its REST shape and its connection at start, so
`Track` replaces the subject rather than leaving one syncing on values nobody asked for. The poke
path is the same bounce across every subject.

**Not on the probe engine**: a sync is a standing push stream, not a periodic pass. Each subject
is a worker goroutine:

1. **Connection.** One refcounted `kubeconn.Lease` per context, shared by that cache's workers;
   every use goes through `Lease.ConnFor(ctx, serverUID)`. No connection or identity mismatch →
   suspend, publish that reason, wake on the kubeconn fleet bus (the kubecatalog bridge). A
   worker never blocks in `AwaitConnFor` while holding anything shared.
2. **Cold sync.** Paged `List` (limit + continue) via `Connection.Dynamic`, pages written in
   store transactions, cookie recorded. Object writes strip managedFields and the kubectl
   last-applied annotation (`ClusterCachedDataObject.RawJSON`'s promise) and compress through
   the carried-over `rawcodec`. `SyncStart`/`SyncComplete` in the event vocabulary.
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

- **`CachedData()`** — specified in full in docs/specs/cached-data.md (the ping/re-read/diff
  loop, the store's read surface, the `Bookmark` rules, provenance). The `kind_catalog` rows it
  reads are the catalog fold's, not the workers' (→ docs/specs/kind-catalog-sync.md, which also
  carries `is_crd` from the sweep) — a worker writes objects and cookies, never catalog rows.
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

## Decided: the store owns the change signal, as a ping bus

The store carries the signal (not the workers — a reader must not know who writes), but it is a
payload-less coalesced ping per bus/key, and readers re-read and diff by UID. Row-level delta
fan-out at the transaction boundary was considered and dropped: once every read is full current
state, an early or late signal costs one idempotent re-read rather than a wrong frame, so the
ordering problem the transactional broker existed to solve disappears — and the served types'
comparability is designed for the diff. Detailed in docs/specs/cached-data.md; carry the
reasoning into the planned ADR.
