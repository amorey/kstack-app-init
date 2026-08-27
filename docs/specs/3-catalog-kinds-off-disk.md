---
title: Catalog kinds off disk
scope: sidecar
status: Planned
---

# Catalog kinds off disk

> **Build order — 3.** No prerequisites: the sweep writes the rows
> (→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)) and `Store.Kinds(ctx)` reads them back
> (→ [ADR](../adr/2026-08-26-cached-data-read-loop.md)), which is what this fold reconciles from.
> **Deferrable** — nothing downstream waits on it, and skipping it leaves today's behaviour.
> Next: [Catalog sweep cadence](4-catalog-sweep-cadence.md).

## Goal

Close the TODO item "the catalog stays resident for as long as a cluster is tracked". `kubecatalog`
holds every served kind per subject — group-version, kind, resource, scope — and the rows the sweep
writes hold the same list again. Order of 90 bytes a kind, so tens of KB for a cluster with
CRDs: **listed for the duplication, not the size.**

Two things hold the kinds in memory, and the TODO says a fix has to answer both. The commit guard
needs the previous answer to compare — a fingerprint covers that. And
`clusterCachedCatalogController.Reconcile` reads the standing answer back through `Read(id)` to
rewrite its children — the `kind_catalog` rows cover that, once the fold reads them.

**This is where the subtlety of the whole sequence lives.** The sweep's write deliberately left the
fold alone, so that populating the table did not have to carry any of it. What follows is the cost of
taking the memory copy away.

## kubecatalog changes

- **The observable becomes `Catalog{Fingerprint, Partial}`** — no `Kinds`. The commit guard is that
  compare, replacing `equal`'s `slices.Equal` over the kinds, and `newsOf` reads the stored
  fingerprint rather than recomputing one.
- **The kinds are resident only for the length of one `Run`**: swept, written, fingerprinted,
  dropped. Steady state per subject is a fingerprint, a bool, and the attempt bookkeeping.
- **`SyncKinds` grows the fingerprint**: `SyncKinds(ctx, rows, prune bool, fingerprint)`, which
  records it in `cluster_meta` **in the same transaction as the rows**. That atomicity is the whole
  mechanism — it is what lets the fold tell a table the sweep wrote from one wiped under it.

## kubestore changes

- **The fingerprint keys `cluster_meta` under `kinds/fingerprint`** — its own namespace beside
  `cookieKey`'s. The column is TEXT, so the `uint64` is stored as its decimal string. There is no
  generic accessor on the bag today (only `setCookie`/`deleteCookie`/`cookieKey`), so this adds a
  `getMeta` read beside them and a `setMeta` **over an `execer`**, the shape `setCookie` already
  wears — which is what lets `SyncKinds` write it inside its own transaction.
- **An absent key is a wipe**, which is the state a fresh file is in — the fold reads it that way,
  and no migration backfills it. So is a value that will not parse.
- **The fold's read is one call: `KindsWithFingerprint(ctx)`**, the rows and the stored fingerprint
  out of **one deferred read transaction**. `Kinds` stays as the read path's own entry point and
  delegates to it, dropping the fingerprint.

  **Reading the two separately is a bug that deletes every child**, and the interleaving is the one
  this spec exists for: a clear empties the file, the fold reads no rows, the sweep rewrites them
  and stores the fingerprint it stored before (the cluster's answer did not move), and the fold
  then reads *that* fingerprint and finds it equal to the observation's. It concludes the empty
  read is what the sweep wrote and — the answer is not `Partial` — prunes the lot. One snapshot is
  the whole guard.

## The fold

`converge` changes only where the kinds come from: **the store, not the observation.** It takes
`(ctx, client, obj, obs)` today and grows the cache id (`int64(own.cache.ID)`, the form
`clearKindRows` already uses), since the store is named by the cache.

For an armed catalog whose observation is `Known()`:

1. `OpenExisting` the cache's store — **never `OpenOrCreate`**: the sweep is the creator, and a
   fold that created would resurrect a torn-down cache's file. **Check the error before `ok`** —
   it answers `(nil, false, ErrRemoved)` for a retired cache, so `!ok` alone conflates "no file
   yet" with "gone".
2. `KindsWithFingerprint(ctx)`, then release.
3. **Fingerprint equal to the observation's** → the rows are what the sweep last wrote: rewrite the
   children from them exactly as today, pruning only when the observation is not `Partial`.
4. **Different or absent, or no file at all** → the table is not what the sweep wrote — a
   `Manager.Clear` (the `clusterCacheClear` mutation lands exactly here), a replaced file, any wipe
   — and an empty table must never be read as "the cluster serves nothing": **leave the children
   alone, report `Discovered=False`/`StoreUnavailable`, wake the sweep (`kubecatalogSvc.Wake`), and
   `RequeueAfter(catalogRetryInterval)`.** `ErrClosed` is the same case seen from the inside — a
   clear swapped the file under this claim — and takes the same path.
5. **`ErrRemoved`, or any other open or read failure** → report `Discovered=False`/
   `StoreUnavailable` and **settle**, with no wake and no requeue. A removed cache is a teardown
   whose `Forget` is on its way. A file that will not open at all refuses the sweep's own mirror
   too, which fails its run and moves its reason — so the signal re-runs this fold, the sweep's
   ladder is the retry, and `catalogResyncInterval` is the backstop. Same shape as
   `DiscoveryFailed`: the fold does not carry a retry a leaf is already running.

**The wake is what repairs it, and the requeue is why it converges anyway.** `publish` fires on
`news` moving, and `news` is a projection of the committed value — so a wipe, which leaves the
cluster's answer exactly as it was, produces no commit and no signal however the sweep is written.
(Forcing a commit does not help: an identical value projects to an identical tuple. Only an epoch
field on the value would move it, which is machinery whose whole job is to move a comparison.) So
the fold asks for the sweep it needs rather than waiting for one, and requeues at 30 seconds behind
it — joining `DiscoveryDraining` as the paths that requeue. The read costs a claim and one query
per pass, against a pass that already lists and rewrites the children.

**Detecting the wipe here is what makes every wipe self-healing**, rather than each wiper having to
know the sweeper exists. So **`Caches().Clear` stops calling `kubecatalogSvc.Wake` and requeues the
catalog record instead** — one more line in `requeueCacheResources`, which already resolves the
catalog id and requeues every child under it. **Dropping the caller outright is what must not
happen**: `kind_catalog` is what `Store.Kinds` serves, so the dashboard nav reads it directly, and
nothing else would run the fold after a clear — the sweep produces no signal, and
`catalogResyncInterval` is ten minutes. The workers would cold-list their rows back while the nav
sat empty. A wiper requeueing its own subtree is what it does already; which leaf that wakes is the
fold's business.

**What the stored fingerprint identifies is the sweep's last answer, not the table's contents.** A
`Partial` sweep upserts without pruning, so the table can legitimately hold rows the fingerprint
does not cover. That is fine for what the fold asks it — "did the sweep write this table, or did
something wipe it" — and the children are safe either way, since a partial answer does not prune.
Don't read it as a checksum of the rows.

Pause, `NoConnection`, and the other disarm verdicts leave the rows as they leave the children: the
subtree survives, only discovery stops. A restart holds nothing in memory; `startupPass` re-runs
the sweep, which rewrites the rows idempotently over what the previous process left.

## Order of work (red/green)

1. `kubestore`: `setMeta`/`getMeta`, the fingerprint parameter on `SyncKinds` written in the same
   transaction, and `KindsWithFingerprint` reading both back in one. Tests pin that a failed write
   leaves neither rows nor fingerprint moved, and that the read answers one consistent pair.
2. `kubecatalog`: the fingerprint-only observable, the commit guard (`Catalog` is comparable now,
   so `equal` goes and the guard is `!=`, the form `connInfo`'s already takes), `newsOf` reading
   the stored value. Tests pin that a compacted subject's later passes are silent — recomputing a
   fingerprint from an absent kind list would hash an empty slice and fire a spurious signal — and
   assert the sweep's answer through the store, which `newTestStores` already hands them.
3. The fold: the `OpenExisting` read (error before `ok`), the fingerprint fork, children from rows,
   the wipe path's verdict-wake-requeue, the settle-only arms, and `Caches().Clear` requeueing the
   catalog instead of waking the sweeper. Tests pin: children rebuilt from disk with no kinds in
   memory, a drained name's retry converging, the wipe path leaving children untouched, a removed
   store settling without a requeue, and the Clear-recovery sequence end to end (clear → requeue →
   mismatch → wake → sweep rewrites → children converge).

**Step 3's fixtures move, and it is more than mirroring `cachedcatalogs_test.go`.** The fold gains
a store dependency, so the catalog tests need a real `kubestore.Manager` over `t.TempDir()` (the
fixture at `testutil_test.go:383` already holds one) and have to seed rows through it;
`fakeKubecatalog`'s observations must carry a fingerprint that the seeded rows match, or the fold
takes the wipe path in every test.

## When it lands

Fold into `sidecar/CLAUDE.md` (the fold reads its kinds off disk and wakes the sweep when they are
not there; a wiper requeues the catalog record and knows nothing of the sweeper). Delete the TODO
item, carrying its weighing into the ADR — which is the one this spec owes: the
fingerprint-versus-disk answer to the TODO's two blockers, and why the rows won over the diff
against the children the TODO weighed. Then delete this spec.
