---
title: Cached resource sync
scope: sidecar
status: In progress
---

# Cached resource sync

## Goal

Make each `ClusterCachedResource` real: a live mirror of one Kubernetes kind into the local
cache. The control plane is **built** — the store registry, the worker fleet's shell, the
controller pass that arms it and folds its observation into `Synced`, the trigger, and the
teardown sequencing all exist and are tested. What remains is the **data plane**: the worker's
run body is a placeholder that publishes nothing, the store has no write path, and the gauges,
the two `Clear`s, the sync events, and the poke bounce are unbuilt. The wire contract is
unchanged throughout — schema, condition vocabulary, event reasons all exist in
`shared.go`. No schema change.

Two sibling specs carve off adjacent work; their boundaries matter:

- **docs/specs/cached-data.md** owns the read side: the `CachedData()` family, the store's
  reads (`Kinds`/`Objects`/`Events` + row structs), the reader pool, the two ping buses, and
  the registry read path. This spec's writers **feed** those buses.
- **docs/specs/kind-catalog-sync.md** owns `kind_catalog`: the catalog fold is its one
  writer. Workers never write catalog rows, and `is_crd` never rides
  `ClusterCachedResourceSpec` or worker `Params`.

## Shape (as built)

```
clusterCachedResourceController.Reconcile      (cachedresources.go — BUILT)
        │ Track / Forget / Read          ▲ trigger (conflated signal → requeue by name)
        ▼                                │
internal/kubesync     fleet shell BUILT; run body is syncPlaceholder (the remaining core)
        │ writes / reads (store dep NOT yet wired)
        ▼
internal/kubestore    registry + schema + cookies + clears BUILT; no write path, no buses
        ▲
        │ reads + bus subscriptions (→ cached-data spec)
caches.go / cacheddata.go reads          (gauges, Clears — still panic)
```

## Built — verify against the code, don't rebuild

**`internal/kubestore`** (`kubestore.go`): the refcounted `Registry`
(`Acquire`/`Handle.Store()`/`Release`), one SQLite file per cache at
`<data-dir>/caches/<cacheID>.db`. `Clear` closes, deletes (with `-wal`/`-shm`), and swaps a
fresh store into the live entry so held handles keep working; `Delete` tombstones the id
(`deleted` set, later `Acquire` refused with `ErrDeleted`) and never reopens; `ClearKind`
refuses to create a file for a cache that has none. `Stats` reports `Exists`/`Bytes`
(sidecars counted). The schema is `main`'s `0001_init.sql` verbatim — `objects` (with the
`generation` sweep column and materialized fields), `owner_refs`, `labels`, `events` + FTS,
`status_history`, `kind_catalog`, trigger-maintained `kind_counts` (including the hardcoded
`('v1','Event')` triggers), `cluster_meta`. `auto_vacuum=INCREMENTAL` is set before
migrations; the writer pool caps at one connection. `Cookie`/`SetCookie` keep the per-kind
watch resourceVersion in `cluster_meta` under `cookie/<apiVersion>/<resource>`.
`Store.ClearKind` deletes a kind's rows (resolving plural→Kind through `kind_catalog`), its
edges, its catalog row, and its cookie in one transaction. (The kind-catalog-sync spec later
changes `ClearKind` to keep the catalog row; leave that to it.)

**`internal/kubesync`** (`service.go`): `Track`/`Forget`/`Read`/`Subscribe`/`Bounce`/
`BounceCache`/`ForgetCache` exactly as the interface comment in `shared.go` describes.
`Track` is a no-op while params hold and replaces the subject when they move; `Bounce`
restarts in place, generation-guarded; `Forget`/`ForgetCache` stop and **wait** for the
worker. One `kubeconn.Lease` per subject, acquired at `Track`. The conflated signal
(`gobus/conflate`, keyed by subject id) fires only when `Observation.Reason` moves —
`commitFor` gates it against `published`. The run body is the `syncFunc` seam
(`withSync` test option); production is `syncPlaceholder`, which parks until ctx ends.

**The controller pass** (`cachedresources.go`): deletion-pending → `Forget` +
`kubestoreSvc.ClearKind`, settle (the other side of the catalog's `DiscoveryDraining`
handshake); owner chain gone (`resourceOwnersOf` walks catalog → cache → cluster) →
`Forget`, settle; `Spec.Enabled == false` → `Forget`, keep the data, `Synced=False/Paused`;
no usable context → `Forget`, `Synced=False/NoConnection`; otherwise `Track` with the
cluster's context and the cache's `ServerUID`, `Read`, and map the observation's reason onto
the condition (`observeSynced`). No answer yet → `Connecting`. Wired in `service.go`:
`newKubesyncTrigger` + `resourceResync` (10m backstop), the registry and fleet in `New` and
in `parts` (stop order: workers stop after the reconciles that arm them, before the stores
and the pool).

**Cache teardown** (`caches.go`): the cache's deletion-pending pass calls
`kubesyncSvc.ForgetCache` (workers stopped and waited on) then `kubestoreSvc.Delete` — the
file dies with the record, and nothing may recreate it (why `Acquire` tombstones).

**Still panicking** (`TestUnimplementedBoundaryPanics` is the honest inventory — delete each
entry as its method lands): `Caches().WatchStats`, `Caches().WatchHealth`, `Caches().Clear`,
`CachedResources().Clear`, the four `CachedData()` methods (cached-data spec), and
`ListEvents`/`WatchEvents` (kstack's own event log — **not this spec's**; it needs the
sync events below to have anything to show, but serving it is separate work).

## Remaining work

### 1. Data-plane seams (kubesync)

The run body needs the connection and the store; neither reaches it today.

- `syncFunc` grows the subject's lease:
  `func(ctx context.Context, p Params, lease kubeconn.Lease, commit func(Observation))`.
  `spawn` passes `sub.lease`, bound the way `commitFor` is.
- `kubesync.New(conns, stores)` — a second leaf dependency, the shape `connService` set:

  ```go
  type storeService interface {
      Acquire(cacheID int64) (*kubestore.Handle, error)
  }
  ```

  (kubesync importing kubestore is leaf→leaf, same as its kubeconn import.)
  `clustersvc.New` passes the registry.
- The worker acquires the handle at run start and releases on run exit. `ErrDeleted` at
  acquire → the cache is being torn down; park until ctx ends (`Forget` is coming, nothing
  to publish). `Handle.Store()` returning nil mid-run (a `Clear` that failed to reopen) →
  end the attempt as a failure into the backoff ladder.
- `Observation` grows `Resumed bool` — the run started from an existing cookie. The fold's
  event mapping (step 4) is the consumer; nothing else reads it. The signal is reason-gated
  (`commitFor` fires only when `Reason` moves), so the fold sees `Resumed` only as it stood
  on a commit that moved the reason — an invariant the loop must hold to: set it on the
  reason-moving commit itself (step 2 does), never flipped later under an unchanged reason,
  where it is invisible.
- `Params` drops `Namespaced`. Its only sync-side consumer on `main` was the
  `kind_catalog` `scope` column, which the kind-catalog-sync spec moves to the catalog fold;
  the worker's list/watch uses the unnamespaced dynamic form, which covers both scopes, and
  the `namespace` column reads from the body (`NOT NULL DEFAULT ''`, so a cluster-scoped
  kind stores the empty string with no help). An unread param would also make `Track`'s
  replace-on-moved-params trigger on a bit nothing uses. (`ClusterCachedResourceSpec.Namespaced`
  stays — the record serves it.)

### 2. The sync loop (the worker run body)

One worker per subject, replacing `syncPlaceholder`. `main`'s
`sidecar/internal/cluster/cache/kubesync/` + `objectsync/` + `eventsync/` are the reference
implementation for the mechanics — carry logic over, adapted to this seam; don't invent.

1. **Connection.** `lease.ConnFor(ctx, p.ServerUID)`; on refusal commit the matching
   observation — `errors.Is(err, kubeconn.ErrNoConnection)` → `ReasonNoConnection`,
   `ErrIdentityMismatch` → `ReasonIdentityMismatch` — then block in
   `kubeconn.AwaitConnFor(ctx, lease, p.ServerUID)`. Legal here: the worker is its own
   goroutine holding nothing shared (the prohibition is on probe `Run`s). Every use of the
   connection goes through `ConnFor`, never `Conn` — the identity gate is what makes the
   cookie safe to reuse (a resumed watch is guaranteed to be against the same server).
2. **Cold sync** (no cookie, or the relist path). Paged `List` (limit + continue, page size a
   parameter) via `Connection.Dynamic`, always the unnamespaced form —
   `Resource(gvr).List` mirrors every namespace, and covers cluster-scoped kinds with the
   same call; each page written in one store transaction; record
   the list's resourceVersion with `SetCookie` when complete. Commit `ReasonSyncing` (with
   `Resumed` false on a cold start, true when a cookie existed) while building, counts as
   they land.
3. **Watch.** From the cookie, `AllowWatchBookmarks: true`. Deltas write through to the
   store; every delta and bookmark updates `LastLiveAt`, every write `LastUpdateAt`. Caught
   up → `ReasonWatching`. A watch quiet past the staleness threshold (a parameter) flips the
   observation to `ReasonStale` **without tearing anything down**; the next proof of life
   flips it back.
4. **Ends.** `apierrors.IsResourceExpired`/`IsGone` → warm relist: bump the sweep
   `generation`, write-all (the list pages), then prune rows the new list did not touch —
   the prune is where deleted-while-disconnected objects finally leave the store. Any other
   failure → backoff ladder (base/max are parameters), `ReasonSyncFailed` carrying the
   error message. The in-memory resourceVersion never outlives its connection: a re-entry
   through `AwaitConnFor` resumes from the *cookie*, and a cookie the server refuses is
   exactly the expired path above.
5. **Writes.** Core `v1` `events` (`Params{APIVersion:"v1", Resource:"events"}`) go to the
   `events` table (`main`'s `eventsync` shape — upsert by UID, coalesced counts); every
   other kind goes to `objects` plus its `owner_refs`/`labels` edges, `status_history`, and
   the cross-kind materialized fields (`main`'s `objectsync/status.go` is the reference —
   the schema's own comment documents the per-kind meanings). Object bodies strip
   `managedFields` and the `kubectl.kubernetes.io/last-applied-configuration` annotation and
   compress through `rawcodec` (step 3). Workers write objects, events, and cookies —
   **never `kind_catalog` rows**.
6. **Pacing.** Cold lists pass through a bounded-concurrency gate on the `Service` (a
   semaphore; the bound is a parameter with a production constant), so enabling a cache does
   not fire a hundred concurrent full LISTs. Standing watches are unbounded.

Every cadence here — page size, staleness threshold, backoff base/max, the list gate — is a
parameter whose production value is the constant, per the repo's testing convention. Tests
drive the seam the way `kubesync`'s existing tests do (`withSync` shows the pattern from the
service side; the loop itself gets a fake `Connection`-shaped seam and a real temp-dir
store).

### 3. The store write path (kubestore)

Carried from `main`'s `sidecar/internal/cluster/cache/store/` + `objectsync/store.go` +
`eventsync/store.go`, adapted to `Store`:

- **`rawcodec`** (zlib compress on write; the read side decompresses — shared with the
  cached-data spec's `Objects` read).
- **Object upsert/delete** with the edge tables, `status_history`, materialized fields, and
  the sweep `generation` stamp; the relist prune deletes `WHERE generation < ?` for the
  kind. `kind_counts` maintenance is free — the schema's triggers own it.
- **Event upsert** by UID, and the **events pruner**: aging out is not a write, so nothing
  emits the promised `Deleted` for free. Prune on every event write (events keep arriving
  on a live cluster) plus a tick for a quiet one; window and tick are parameters with
  production constants (window doubles as the cached-data watch's diff window, `main`'s
  `defaultEventsLimit` 500).
- **Bus notifications.** Every write path above notifies after commit: object writes the
  objects ping bus under `apiVersion/resource` (plus the cache-wide key), event writes and
  pruner deletes the events bus. The buses themselves are the cached-data spec's step 2 —
  if that step hasn't landed when this does, add the notify calls behind a no-op seam it
  fills in.

**Ordering trap:** `Store.ClearKind` and the read side's plural→Kind translation resolve
through `kind_catalog`, whose one writer is the catalog fold (kind-catalog-sync spec). Until
`SyncKinds` lands, a kind's rows are written but not reachable by plural — so land
kind-catalog-sync's `SyncKinds` step **before or with** the first end-to-end sync, or a
teardown's `ClearKind` silently misses the rows it should delete.

### 4. Sync events on the record

The fold (not the worker — only a `ControllerClient` can write events) derives the
`SyncEventCategory` timeline from the `Synced` transition: compare the previous condition
(`FindCondition(obj.Conditions, ConditionSynced)`) against the new verdict, and on a move
`AddEvent` the matching reason — grouped with `SetCondition` in one `client.Within`, the
`clusters.go` idiom.

| Transition (new reason, context) | Event reason |
|---|---|
| → `Syncing`, `Resumed` false | `SyncStart` |
| → `Syncing`, `Resumed` true | `ResyncStart` (message reports the warm size) |
| `Syncing` → `Watching`, `Resumed` false | `SyncComplete` |
| `Syncing` → `Watching`, `Resumed` true | `ResyncComplete` |
| → `SyncFailed` | `SyncDegraded` |
| → `Stale` | `SyncStale` |
| → `Paused` / `NoConnection` (the disarm branches) | `SyncStopped` |

The reason constants' doc comments in `shared.go` are authoritative for message content. The
signal conflates, so an unobserved intermediate state records no event — consistent with "a
healthy steady state records no event". Beehive extends a repeated `(Category, Type, Reason)`
run rather than appending, so writing on every pass that observes the same verdict is safe.

### 5. The gauges

Both become `*Stream[T]` — the boundary rule (anything reading a fallible upstream returns a
`Stream`); `WatchHealth` already has the signature, **`WatchStats` changes from
`<-chan ClusterCacheStats`** (resolvers move to `watchStream`; the resolver-test fake changes
shape; schema untouched).

- **`Caches().WatchStats(clusterID, cacheID)`** — gate the pair exactly as the cached-data
  spec's methods do (cache record exists and is owned by that cluster). A gauge has no
  `Bookmark`, and nothing is emitted before the first measurement — so an absent or
  mismatched pair holds silent until ctx ends (the consumer renders "not observed yet"; a
  caller holding a bad id got it from a watch frame and drops the subscription itself). A
  live pair reads: `Registry.Stats` for `Exists`/`Bytes`, plus a store rollup
  read summing `kind_counts` excluding the `('v1','Event')` row (`ObjectCount` = total,
  `KindCount` = rows with count > 0). The rollup is **this spec's** addition to `Store`
  (`CountsRollup`), riding the reader pool — but the pool, the ping buses, and the
  bind-to-an-open-store registry read path are all cached-data's steps 1–2, so this gauge
  lands **after** them, the mirror of §3's notify-seam dependency. The store binds through
  that read path, never `Acquire` (a read must not create the file); while no store is open
  the gauge emits `Registry.Stats`' file facts with zero counts. Emit the first measurement,
  then re-emit on change, woken by both ping buses plus a cadence parameter (file size moves
  with no ping). Emit only when the struct differs — it is comparable.
- **`Caches().WatchHealth()`** — the read-side fold over worker observations, never a stored
  condition (`ClusterCacheHealth`'s doc comment argues why). kubesync grows one snapshot
  read:

  ```go
  type SubjectObservation struct {
      ID     string
      Params Params
      Obs    Observation
      Known  bool
  }
  func (s *Service) Observations() []SubjectObservation  // tracked fleet, one critical section
  ```

  The fold groups by `Params.CacheID`; a cache with no tracked subjects (fully paused, or
  torn down) simply has no gauge frame. Per cache: `Status=True/Watching` iff every subject
  is known with `ReasonWatching`; otherwise `False` with the most severe reason present, in
  this precedence — `IdentityMismatch`, `NoConnection`, `SyncFailed`, `Stale`, `Syncing`,
  `Connecting` (a tracked subject not yet known). `UnhealthyKindRefs` = the
  `{APIVersion, Resource}` of every non-Watching subject, sorted; `TotalKinds` = tracked
  subjects; `LastUpdateAt` = max across the observations that have one, `LastLiveAt` = the
  **oldest** among those that have one (the weakest link), nil until any kind reports.
  Current-on-subscribe (one frame per cache), then re-emit on the fleet's conflated signal
  **and** on a per-subscription cadence (a parameter) — the timestamps move in healthy
  steady state precisely when the signal is silent. Emit a cache's frame only when its value
  moved (compare with `slices.Equal` for the refs).

### 6. The two Clears and the poke bounce

The original design bounced workers around a clear; that races — `Bounce` respawns before
the registry touches the files. The controller-side sequencing already built (stop → wait →
touch store) is the model; the boundary does the same and uses beehive's `Requeue` as the
prompt re-arm, since the record pass is what re-`Track`s:

- **`Caches().Clear(id)`** — `Get` the record (nil → `(nil, nil)`, the family's absent
  idiom); `kubesyncSvc.ForgetCache(id)` (stops and waits, so nothing writes mid-clear);
  `kubestoreSvc.Clear(id)` (grow `kubestoreService` in `shared.go` with `Clear`); then
  resolve the cache's resource records (`catalogIDFor` + `resourceClient.ListOwnedObjects`)
  and `resourceClient.Requeue` each — their passes re-`Track`, and the workers cold-sync
  because the cookie died with the file. `resourceResync` is the backstop for a lost
  requeue. Return the record.
- **`CachedResources().Clear(id)`** — the same per kind: `Get` the record;
  `kubesyncSvc.Forget(obj.Name)` (the record's name **is** the subject id);
  `kubestoreSvc.ClearKind`; `Requeue(id)`. Return the record.
- **Poke bounce** — `kubesync` grows `BounceAll()` (every tracked subject, the `BounceCache`
  shape without the filter). `clusterCachedResourceController` replaces its
  `lifecycle.None` with a `Start` that subscribes `pokeSvc.Subscribe()` and calls
  `BounceAll` per poke — the in-place warm resume (cookie intact) the poke section of
  `sidecar/CLAUDE.md` reserves for this controller. Bounces run sequentially on the
  subscription goroutine; each waits for its worker, which is the point.

`BounceCache` then has no caller, and `Bounce`'s only caller is `BounceAll` — the same
argument reaches both. Delete `BounceCache` (interface entry and tests too) and demote
`Bounce` to the unexported `bounce` helper under `BounceAll`; the `kubesyncService`
interface in `shared.go` ends up carrying `BounceAll` alone as its restart entry.

## Order of work

1. Seams (step 1) + the store write path (step 3), with white-box store tests over a temp
   dir — writes are testable without a worker.
2. The sync loop (step 2), end to end against a fake connection: cold sync → watch → resume
   → stale → expired-relist → backoff. The existing `kubesync` service tests keep passing
   unchanged apart from the seam signatures.
3. Sync events (step 4) — fold-level tests against a stubbed `ControllerClient`, the
   `cachedresources_test.go` style.
4. Gauges (step 5), then the Clears + poke bounce (step 6). Delete each
   `TestUnimplementedBoundaryPanics` entry as its method lands.

Each step tests in the established style: a fake connection for kubesync, `newRunningDeps`
for fold-level tests, `testutil.Probe`/`Signal` for fake notifications, no magic sleeps —
every cadence is already a parameter by construction above.

When it lands: fold what is true into `sidecar/CLAUDE.md` (whose cluster-subsystem section
currently describes the placeholder state), write the ADRs (store-per-cache, worker-not-probe,
and the ping-bus reasoning below), and delete this spec.

## Decided: the store owns the change signal, as a ping bus

The store carries the signal (not the workers — a reader must not know who writes), but it
is a payload-less coalesced ping per bus/key, and readers re-read and diff by UID. Row-level
delta fan-out at the transaction boundary was considered and dropped: once every read is full
current state, an early or late signal costs one idempotent re-read rather than a wrong
frame. The full read-side design, including the buses themselves, lives in
docs/specs/cached-data.md; this spec's writers only notify them. Carry the reasoning into
the planned ADR.
