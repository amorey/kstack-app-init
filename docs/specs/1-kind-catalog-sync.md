---
title: Kind catalog sync
scope: sidecar
status: Planned
---

# Kind catalog sync

> **Build order — 1.** No prerequisites; everything it needs has landed. Delivers a `kind_catalog`
> populated and kept current in real time. Next: [Cached data reads](2-cached-data-reads.md),
> which serves that table to the webview.

## Goal

Populate the store's `kind_catalog` table: **the sweep writes it**, the way a `kubesync` worker
writes the objects it mirrors. Today nothing writes the table, so `WatchKinds`
(→ [spec 2](2-cached-data-reads.md)) has nothing to read and the plural→Kind translation the
store's `Objects` read will ride has no rows to ride.

**One writer per table, in the leaf that produced the data.** `kubecatalog` takes the store
manager the way `kubesync.New(conns, stores)` does. The alternative — the sweep handing the kinds
to the fold and the fold writing — has to carry the kinds across the probe engine and
re-establish, from the fold, whether the table on disk is still the answer the sweep gave it. That
is a recovery protocol whose only job is to work around the write being gated behind a change
signal.

**The write is not gated on the answer changing.** Every sweep that produced an answer upserts the
rows; the commit guard goes on governing only the *signal*. So a table wiped under the sweep is
rewritten by the next sweep with no repair protocol, and the truth is re-asserted on a cadence
rather than on an edge.

**Real time comes from the watch, and it is already there.** `kubecatalog` streams
`customresourcedefinitions` and `apiservices` — the two collections that change what a cluster
serves — and a change wakes the sweep in seconds
(→ [ADR](../adr/2026-08-26-kubecatalog-watch.md)). Today that wake produces a fold and nothing on
disk; once the sweep writes, the same wake carries a CRD through to `kind_catalog`. Nothing here
has to be built to make the catalog live.

**The fold is left alone.** It goes on building its `ClusterCachedResource` children from the
kinds the sweep commits, exactly as today. Moving it to read the rows off disk — which is what
retires the resident copy — is [spec 3](3-catalog-kinds-off-disk.md), and it is deliberately not
in the way of this one.

## kubestore changes

- **`Store.SyncKinds(ctx, rows, prune bool)`**: one transaction that reconciles the row set and
  deletes rows not in it when `prune`.
  - **The collision rule is delete-then-upsert, because the table has two unique keys and SQLite
    takes one `ON CONFLICT` target.** `PRIMARY KEY (api_version, kind)` and
    `UNIQUE (api_version, resource)` both apply, so an upsert keyed on the first still raises a
    constraint failure when a renamed Kind takes a plural another row holds. So, per incoming row:
    `DELETE FROM kind_catalog WHERE api_version = ? AND resource = ? AND kind <> ?` — the rename's
    loser — then the upsert on `(api_version, kind)`. This is `main`'s `EnsureKindCatalog` shape.
  - **`schema_json` is omitted from `DO UPDATE SET`**, not written as NULL. Nothing fills it yet
    (it is the OpenAPI v3 schema for CRDs), and a sweep that set it to NULL on every pass would
    quietly make the column unusable by whoever fills it later.
  - `rows` is the table's own vocabulary — `api_version`, `kind`, `resource`, `scope`, `is_crd` —
    so the `Namespaced` bool becomes `"Namespaced"`/`"Cluster"` on the way in, in kubecatalog,
    where this leaf's own vocabulary is written.
- **`kind_catalog` has one writer: the sweep.** `ClearKind` does not *resolve* through the table —
  it takes the whole `Kind` from the record, so a teardown never waits on this spec — but it still
  deletes the row (`store.go:430`), and that has to stop: a user-initiated
  `CachedResources().Clear` wipes a kind that is still served, and deleting its row would drop it
  from the nav until the next sweep. Rows leave the table only through the sweep's prune, when the
  kind leaves the discovery answer. **This needs a new test, not an edited one** —
  `TestClearKindDeletesRowsWithNoCatalogRow` asserts the table is *empty going in* ("nothing on the
  sync path writes catalog rows"), so deleting the statement breaks nothing today. Seed a row,
  `ClearKind`, assert it survives.
- The dead-claim and tombstone semantics the per-run claim needs have **landed** (`Manager.Remove`
  retires the entry and records the id in `removed`; `OpenOrCreate` answers `ErrRemoved`, a
  straggler `Store` answers `ErrClosed`, `Release` on it is a no-op), pinned by `manager_test.go`'s
  `TestOpenAfterRemoveIsRefused`. That is what stops a sweep in flight during a teardown from
  recreating the file the teardown just deleted, permanently orphaned.

## kubecatalog changes

- **`New(conns, stores)`**, mirroring `kubesync`: a narrow `storeManager` in this package with
  `OpenOrCreate(cacheID int64) (*kubestore.Store, error)`, wired to the same `kubestore.Manager` in
  `clustersvc.New`.
- **`Track(id string, p Params)`** — `{CacheID, ContextName, ServerUID}` — for the same reason
  `kubesync.Params` exists: the subject id embeds the *catalog's* object id, not the cache's, and
  the store is named by the cache. Everything else about `Track` is unchanged, and all four fields
  stay fixed for the id's life.
  **This and `Wake` cross the boundary**, which is the diff's real width: `shared.go`'s
  `kubecatalogService` gains `Wake` and restates `Track`, and `testutil_test.go`'s
  `fakeKubecatalog` moves with it — its `armedSubject{contextName, serverUID}`, which every
  catalog fold test asserts `armedFor[id]` against, becomes the `Params` the fake was handed.
- **`Run` writes, then commits.** After a sweep that produced an answer: claim the store, one
  `SyncKinds`, release, and only then `pass.Commit`. Commit-then-write would signal a fold — and,
  once [spec 3](3-catalog-kinds-off-disk.md) lands, a reader — over rows that are not there yet.
  - **Unconditionally, whether or not the answer moved**, which is what puts a wiped table back.
    Roughly a hundred upserts per cache per interval.
  - **Prune only when the answer is not `Partial`** — the same add-without-pruning rule the
    children follow, for the same reason: a group that went quiet has not stopped being served.
  - **A failed write fails the run** (`ReasonStoreFailed`) and commits nothing. **It outranks
    `ReasonSweepPartial`**, which is what a partial answer returns after the commit guard today:
    with the write in between, a partial sweep whose write failed committed nothing at all, and
    reporting the incomplete *answer* would point a reader at an api group that is not the
    problem. **`ErrClosed` lands here**, and it is not exotic: `Manager.Clear` retires the entry
    when it cannot close or reopen, so a claim in flight across a user's cache clear gets it. The
    retry's `OpenOrCreate` opens a fresh file.
  - **The probe registers `WithBackoff(30s, 2, sweepInterval)`.** It takes the engine's default
    today (`Base: 1s, Factor: 2, Cap: 5m`), which means a failing write is answered by a full
    `ServerPreferredResources` — dozens of round trips over every group-version — a second later,
    then two, then four. The ladder was sized for a failing API server, where retrying *is* the
    point; a failing disk makes it a retry of the healthy half, paid for at someone else's
    cluster. A 1-second base is hard to justify for either reason here, since the sweep's
    promptness comes from the watch and never from the ladder — so the base moves above the cost
    of the thing being retried, and the cap to the interval, where climbing stops being faster
    than simply waiting. This re-paces `SweepFailed`/`SweepPartial` too, deliberately.
  - **`ErrRemoved` suspends** (`ReasonStoreRemoved`): the cache is being torn down and this
    subject's `Forget` is on its way, so there is nothing to write into and nothing worth
    reporting. `kubesync` parks its goroutine here (`service.go:329`); a probe `Run` may not block,
    so it suspends and lets the teardown disarm it.
- **`Kind` grows `IsCRD`.** The sweep derives it by matching the discovery answer against the
  cluster's CustomResourceDefinitions — one list over the same connection (`crdGVR`, the collection
  its watcher already streams). **The match key is group + plural** (`spec.group` +
  `spec.names.plural`), never the version: one CRD serves several, and keying on a version would
  leave every kind discovered at another one reading as built-in. **A refused CRD list is
  best-effort `IsCRD=false`, never `Partial` and never a failed sweep**: listing CRDs is a
  cluster-scoped read RBAC commonly denies (the TODO already notes the watcher refused for exactly
  this), and failing the catalog over it would take discovery away from users the current sweep
  serves fine. The sweep seam grows a context — `discoverServedKinds` takes none today, and the CRD
  list is a `Dynamic` call — so `withSweep`'s signature and its test doubles move with it.
  `kindsFingerprint` extends to cover the new field, or a cluster whose CRD list starts answering
  moves no news.
- **`Wake(id)`** exposes the engine's wake, so a wiper can ask for the sweep that puts the rows
  back rather than leaving the table empty until the interval comes round. `Caches().Clear` calls
  it **from the deferred `requeueCacheResources` path, after the clear** — never inline ahead of
  it. A wake before `WhileCacheStopped(…Clear)` races in exactly the direction the wake exists to
  prevent: the woken sweep writes the rows, `Manager.Clear` deletes them, and the table is empty
  for a full interval. (That defer requeues the cached-resource children, a different subject;
  this rides the same deferred position.) Spec 3 retires the call, replacing it with detection in
  the fold, so that no wiper has to remember.
- The observable is unchanged (`Catalog{Kinds, Partial}`, `equal` as the commit guard), and so is
  every consumer of it. Spec 3 is what changes that.
- The claim is per-run, taken and released inside `Run`, so `Track`/`Forget` and `Close` hold
  nothing new. **A `Forget` landing mid-run needs no handling**, unlike the watcher `ensureWatcher`
  guards: the rows a finishing sweep writes are true whatever the record decided — a paused cache
  keeps its catalog, which is what pausing means — and a cache being *torn down* is covered by the
  `Manager.Remove` tombstone, which refuses the claim outright.

### Store I/O inside a probe `Run`

`sidecar/CLAUDE.md` states that `ReadyFor`/`AwaitConnFor` may not be called from a `Run` because
blocking holds one of the engine's eight shared workers — and names this package as the one that
refuses-and-suspends instead. A store write is the first thing here that could look like a
violation, so: **the rule is about waiting on something that may never arrive, not about doing
work.** This `Run` already holds its worker across `ServerPreferredResources` — a network call over
every group-version, bounded only by `sweepTimeout`'s five minutes — so a local SQLite write is far
smaller than what the run already costs.

**What bounds the wait is that nothing holds a long transaction.** The writer pool is
`SetMaxOpenConns(1)`, so contention shows up as a wait on the Go connection pool rather than as
`SQLITE_BUSY` — `busy_timeout` is not the mechanism here. What the sweep can queue behind is
whichever transaction the kind's own worker holds, and the worker never spans one across a relist:
`ReplaceSession.WritePage` and `Commit` each open their own. So the worst wait is one list page's
write, and a first create adds the migrations. Against a 10-minute interval per subject that is
noise. If it ever shows, the answer is `probe.WithWorkers` on this engine's registration, not
moving the write back across the boundary.

## The fold

One change. `converge`'s reason switch maps every unrecognized leaf reason to
`ReasonDiscoveryFailed`, which would tell a user who just clicked "clear cache" that discovery
failed. Two arms join the `Discovered` vocabulary:

- `kubecatalog.ReasonStoreFailed` → **`ReasonStoreUnavailable`**, carrying the message. The sweep's
  answer is good; the mirror is what would not take it, and the sweep's own ladder is retrying.
- `kubecatalog.ReasonStoreRemoved` → the same reason. In practice the pass never gets here (a
  removed store means the cache record is gone, so `ownersOf` returns early), but the default arm
  must not turn a teardown into a discovery failure.

Both are needed in the **`!Known()` switch too** (the "armed, no answer yet" branch), or a store
failure on a cache's very first sweep reports as `Connecting` forever.

Nothing else moves: the children are still built from `obs.Value.Kinds`.

## Order of work (red/green)

1. `kubestore`: `SyncKinds` (delete-then-upsert, prune, `schema_json` untouched, one transaction)
   and `ClearKind` keeping the row, with the **new** positive test above. Pin the rename collision
   — a plural moving to another Kind — since that is the case a single-target upsert fails on.
   Seeded-SQL tests throughout. Additive: nothing calls it yet.
2. `kubecatalog`: `Params` with `CacheID`; the store manager and the per-run claim; the write in
   `Run` with `ErrRemoved` suspending and `ErrClosed` failing; `WithBackoff`; `Wake`. Plus the
   narrow interface and its fake, the fold's two reason arms, and the deferred `Caches().Clear`
   wake — which is where a store failure becomes visible. Tests pin: **write before commit** (a
   failed write commits nothing and signals nobody), **a failed write outranking `Partial`**, **an
   unchanged answer still rewrites the rows**, and no prune on `Partial`.
   **After this step the table is populated and live**, and [spec 2](2-cached-data-reads.md) is
   unblocked.
3. `IsCRD`: the CRD list, the group+plural match, best-effort false, `kindsFingerprint` covering
   it. **Deferrable** — until it lands every row reads `is_crd = 0`, and nothing branches on the
   bit yet (the webview selects `isCRD` and types it, but the nav buckets by API group).
4. The doc edits below, **`graph/schema.graphqls` included**: the `Discovered` condition's doc
   says "the other seven are false" and sorts them into categories, so `ReasonStoreUnavailable`
   makes eight and needs its own — the answer is good and the mirror would not take it, which is
   neither of the waiting categories nor a discovery failure. `TestUnimplementedBoundaryPanics` is
   untouched: this spec serves no new boundary method.

## When it lands

Fold into `sidecar/CLAUDE.md`: `kind_catalog`'s single writer, the write-then-commit order, the
sweep's store claim, and the probe's own backoff. It currently says the `CachedData` reads "will
read empty until [the catalog fold] lands" — the writer is the sweep, and what is left before
those reads answer is spec 2.

Write the ADR for the writer's placement: one writer per table, in the leaf that produced the
data, with the write ungated by the change guard. Then delete this spec.
