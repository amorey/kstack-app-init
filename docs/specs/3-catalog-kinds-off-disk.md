---
title: Catalog kinds off disk
scope: sidecar
status: Planned
---

# Catalog kinds off disk

> **Build order — 3.** Prerequisites: [spec 1](1-kind-catalog-sync.md) for the rows,
> [spec 2](2-cached-data-reads.md) for the `Kinds(ctx)` read this fold reconciles from.
> **Deferrable** — nothing downstream waits on it, and skipping it leaves today's behaviour.
> Next: [Catalog sweep cadence](4-catalog-sweep-cadence.md).

## Goal

Close the TODO item "the catalog stays resident for as long as a cluster is tracked". `kubecatalog`
holds every served kind per subject — group-version, kind, resource, scope — and the rows spec 1
writes now hold the same list again. Order of 90 bytes a kind, so tens of KB for a cluster with
CRDs: **listed for the duplication, not the size.**

Two things hold the kinds in memory, and the TODO says a fix has to answer both. The commit guard
needs the previous answer to compare — a fingerprint covers that. And
`clusterCachedCatalogController.Reconcile` reads the standing answer back through `Read(id)` to
rewrite its children — the `kind_catalog` rows cover that, once the fold reads them.

**This is where the subtlety of the whole sequence lives.** Spec 1 deliberately left the fold
alone so that populating the table did not have to carry any of it. What follows is the cost of
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
  `setMeta`/`getMeta` pair beside them.
- **An absent key is a wipe**, which is the state a fresh file is in — the fold reads it that way,
  and no migration backfills it.

## The fold

`converge` changes only where the kinds come from: **the store, not the observation.** It takes
`(ctx, client, obj, obs)` today and grows the cache id (`int64(own.cache.ID)`, the form
`clearKindRows` already uses), since the store is named by the cache.

For an armed catalog whose observation is `Known()`:

1. `OpenExisting` the cache's store — **never `OpenOrCreate`**: the sweep is the creator, and a
   fold that created would resurrect a torn-down cache's file. **Check the error before `ok`** —
   it answers `(nil, false, ErrRemoved)` for a retired cache, so `!ok` alone conflates "no file
   yet" with "gone".
2. Read the rows (`Kinds(ctx)`) and the stored fingerprint, and release.
3. **Fingerprint equal to the observation's** → the rows are what the sweep last wrote: rewrite the
   children from them exactly as today, pruning only when the observation is not `Partial`.
4. **Different or absent** → the table is not what the sweep wrote — a `Manager.Clear` (the
   `clusterCacheClear` mutation lands exactly here), a replaced file, any wipe — and an empty table
   must never be read as "the cluster serves nothing": **leave the children alone, wake the sweep
   (`kubecatalogSvc.Wake`), and `RequeueAfter(catalogRetryInterval)`.**

**The wake is what repairs it, and the requeue is why it converges anyway.** `publish` fires on
`news` moving, and `news` is a projection of the committed value — so a wipe, which leaves the
cluster's answer exactly as it was, produces no commit and no signal however the sweep is written.
(Forcing a commit does not help: an identical value projects to an identical tuple. Only an epoch
field on the value would move it, which is machinery whose whole job is to move a comparison.) So
the fold asks for the sweep it needs rather than waiting for one, and requeues at 30 seconds behind
it — joining `DiscoveryDraining` as the paths that requeue. The store read that costs happens only
while a cache is actually wiped, and the requeue equally covers a store that could not be read at
all — a refused claim, a transient I/O failure — which otherwise waits out
`catalogResyncInterval`'s ten minutes.

**Detecting the wipe here is what makes every wipe self-healing**, rather than each wiper having to
remember to poke the catalog — so **spec 1's `Wake` call in `Caches().Clear` comes back out**. The
`Wake` seam itself stays; what goes is the caller, along with the assumption that a wiper knows the
catalog exists.

**What the stored fingerprint identifies is the sweep's last answer, not the table's contents.** A
`Partial` sweep upserts without pruning, so the table can legitimately hold rows the fingerprint
does not cover. That is fine for what the fold asks it — "did the sweep write this table, or did
something wipe it" — and the children are safe either way, since a partial answer does not prune.
Don't read it as a checksum of the rows.

Pause, `NoConnection`, and the other disarm verdicts leave the rows as they leave the children: the
subtree survives, only discovery stops. A restart holds nothing in memory; `startupPass` re-runs
the sweep, which rewrites the rows idempotently over what the previous process left.

## Order of work (red/green)

1. `kubestore`: `setMeta`/`getMeta`, and the fingerprint parameter on `SyncKinds` written in the
   same transaction. A test pins that a failed write leaves neither rows nor fingerprint moved.
2. `kubecatalog`: the fingerprint-only observable, the commit guard, `newsOf` reading the stored
   value. Tests pin that a compacted subject's later passes are silent — recomputing a fingerprint
   from an absent kind list would hash an empty slice and fire a spurious signal.
3. The fold: the `OpenExisting` read (error before `ok`), the fingerprint fork, children from rows,
   the wipe path's wake-plus-requeue, and removing spec 1's `Caches().Clear` wake. Tests pin:
   children rebuilt from disk with no kinds in memory, a drained name's retry converging, and the
   wipe path leaving children untouched — the Clear-recovery sequence end to end (clear → mismatch
   → wake → sweep rewrites → children converge).

**Step 3's fixtures move, and it is more than mirroring `cachedcatalogs_test.go`.** The fold gains
a store dependency, so the catalog tests need a real `kubestore.Manager` over `t.TempDir()` (the
fixture at `testutil_test.go:383` already holds one) and have to seed rows through it;
`fakeKubecatalog`'s observations must carry a fingerprint that the seeded rows match, or the fold
takes the wipe path in every test.

## When it lands

Fold into `sidecar/CLAUDE.md` (the fold reads its kinds off disk and wakes the sweep when they are
not there). Delete the TODO item, carrying its weighing into the ADR — which is the one this spec
owes: the fingerprint-versus-disk answer to the TODO's two blockers. Then delete this spec.
