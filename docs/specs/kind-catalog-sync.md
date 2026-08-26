---
title: Kind catalog sync
scope: sidecar
status: Planned
---

# Kind catalog sync

## Goal

Populate the store's `kind_catalog` table: the catalog fold
(`clusterCachedCatalogController.Reconcile`) writes the discovered kinds to the cache's on-disk
store in the same pass that rewrites the `ClusterCachedResource` children. Today nothing writes
the table, so `WatchKinds` (→ docs/specs/cached-data.md) has nothing to read and the
plural→Kind translation `store.Objects` rides has no rows to ride.

The disk copy then replaces the resident one: the TODO item "the catalog stays resident for as
long as a cluster is tracked" names two things that hold the `Catalog` in memory — the sweep's
commit guard needs the previous answer to compare, and the fold reads the standing answer back
to rewrite children. A fingerprint covers the first (and already exists: `kindsFingerprint`
feeds the news signal); the `kind_catalog` rows this spec writes cover the second. The TODO's
own caveat — a hash-only shape "couples the sweep's correctness to records it must not know
about" — is why the fold does the writing and the sweep stays store- and record-ignorant.

## kubecatalog changes

- **`Kind` grows `IsCRD`.** The sweep derives it by matching the discovery answer against the
  cluster's CustomResourceDefinitions — one list over the same connection, the collection its
  watcher already streams. **A refused CRD list is best-effort `IsCRD=false`, never `Partial`
  and never a failed sweep**: listing CRDs is a cluster-scoped read RBAC commonly denies (the
  TODO already notes the watcher refused for exactly this), and failing the catalog over it
  would take discovery away from users the current sweep serves fine. The row needs the bit and
  nothing upstream records it; this replaces the earlier plan of relaying it through
  `ClusterCachedResourceSpec` and worker `Params` (amended below).
- **The commit guard compares fingerprints**, not retained kind lists: extend
  `kindsFingerprint` to cover `IsCRD`, commit when the fingerprint or `Partial` moved. The
  committed value keeps `{Kinds, Partial}` plus the fingerprint — the kinds still travel to the
  fold; what changes is what must be *retained* to detect the next change.
- **`Compact(id, fingerprint)`** drops the `Kinds` slice from the retained observation, keeping
  the fingerprint, `Partial`, and the attempt bookkeeping. The fold calls it after both its
  writes landed, so a failed pass retries with the kinds still in hand. It no-ops when the
  retained fingerprint differs — a newer sweep landed between `Read` and `Compact`, and its
  answer must not be thrown away — and it never signals: nothing changed for a reader. Two
  contracts guard that:
  - **Compact needs a seam the probe engine does not have** — its contract is that a value is
    written by its probe's `Run` alone, inside the engine's critical section, and Compact's
    compare-then-drop must be atomic against the next commit. Either the engine grows a guarded
    out-of-band update (a compare-and-set applied in its critical section), or the kinds never
    enter the engine at all: the sweep commits `{Fingerprint, Partial}` and parks the kinds in a
    service-held pending map under `Service.mu` (publish fills it, Compact clears it, `Read`
    merges). Prefer the second — it leaves the engine contract untouched.
  - **`news` reads the stored fingerprint, never recomputes it from the kinds** — recomputing
    after Compact hashes an empty list and fires a spurious signal on the next pass. A test pins
    that a compact is followed by silent passes.
  - The pending map's bookkeeping, named because each piece is easy to get subtly wrong: the
    **`Run` parks the kinds through a service seam** (publish cannot fill the map — the
    committed value no longer carries them); entries **carry their fingerprint**, which is what
    Compact's no-op check and `Read`'s merge both match on; and **`Forget` clears the entry**
    beside `published`, or a re-tracked id merges a stale parked answer.
- **`Invalidate(id)`** is how the fold reports "the disk copy is gone" (see the compacted path
  below) — recovery must not wait on the cluster's answer actually changing, which may never
  happen. It must not touch the engine's value (the contract the Compact seam just preserved),
  so it is three service-side moves under `Service.mu`:
  - set a per-id **force flag** the sweep's `Run` reads through a seam (the shape `connFor`
    models), so the commit guard passes whatever the cluster answers. **The commit consumes the
    flag, not the read** — `publish` clears it on a pass that committed, under the same
    `Service.mu` — so a sweep that fails right after an invalidate leaves the flag armed for the
    ladder's retry. A read-consumed flag would be spent with no commit, and recovery would then
    cost an extra invalidate→wake→sweep round trip through the fold's re-detect;
  - **reset the id's `published` baseline** — without this the chain dies at its last hop:
    the cluster's answer has not changed, so the recommitted fingerprint is byte-identical to
    the baseline `publish` compares against, no signal fires, and the rewrite waits out the
    10-minute resync — exactly the wait Invalidate exists to avoid, with the nav and the
    `store.Objects` translation empty the whole time;
  - wake the sweep.
- Steady state per cluster is a fingerprint and attempts; the kinds are resident only between a
  sweep landing and its fold consuming it.

## kubestore changes

- **`kind_catalog` has one writer: the fold.** `ClearKind` stops deleting the row (today it
  does — change it and its test): a user-initiated `CachedResources().Clear` wipes a kind that
  is still served, and deleting its row would drop it from the nav until the next fold pass.
  Rows leave the table only through the fold's prune, when the kind leaves the discovery answer.
- **`Store.SyncKinds(ctx, rows, prune bool, fingerprint)`**: one transaction that upserts the
  row set, deletes rows not in it when `prune`, and **records the consumed fingerprint in
  `cluster_meta`**. Persisting the fingerprint beside the rows, atomically, is what lets the
  compacted path tell "this table is the answer I consumed" from "this table was wiped under
  me" — see the fold. The upsert carries `main`'s `EnsureKindCatalog` collision rule: within a
  group-version a plural names exactly one Kind (the schema's unique `(api_version, resource)`
  index), so registering a renamed Kind clears the loser in the same statement.
- **A handle outlives `Delete` as a dead claim, and a deleted id refuses reopening.**
  `Registry.Delete` removes the entry under outstanding handles — afterwards `Handle.Store()`
  answers "gone" (an error, not a fresh store) and `Release` is a no-op — **and records the id
  in a tombstone set that `Acquire` refuses with the same error**. The dead handle alone does
  not close the creation race: an acquire *in flight* during the teardown (the fold's mid-pass
  claim, a worker starting) reads its record as live before the mark lands and would recreate
  the file the teardown just deleted, permanently orphaned. Beehive never reuses an ObjectID,
  so the tombstone set is safe for the process's life, and it closes the window for the fold
  and the workers alike — the worker fleet has the same theoretical race today. A refused
  acquire fails the pass; the retry reads the dying record and disarms.

## The fold

**The claim is the pass's other job** — the pattern `clusterLeases` set. The controller holds one
registry handle per armed catalog (acquired when the pass arms the sweep; creating the file is
legitimate here, it is record-driven), released on every disarm branch — pause, teardown, owner
gone, context error — and on `Close`.

Two converge paths, split on what `Read` returned:

- **Kinds present** (a sweep landed since the last fold): `SyncKinds` through the handle —
  upsert always, prune only when the answer is not `Partial`, the same add-without-pruning rule
  the children already follow — then rewrite the children as today, then
  `Compact(id, fingerprint)`. Order is load-bearing: rows first (a `WatchKinds` reader and the
  `store.Objects` translation must never see a child whose row is missing), compact last (a
  failure anywhere retries with the kinds retained).
- **Compacted** (fingerprint set, kinds absent — a resync pass, or a `DiscoveryDraining`
  requeue): read the stored fingerprint and the `kind_catalog` rows back through the handle.
  **Stored fingerprint equal to the retained one** → the rows are exactly what the last consumed
  sweep said, and the children reconcile from them: a drained name's retry, a child something
  deleted, and the routine no-op resync all converge without the memory copy. **Different or
  absent** → the disk copy is not the consumed answer — a `Registry.Clear` (the
  `clusterCacheClear` mutation lands exactly here), a quarantine-replaced file, any wipe — and
  the empty table must not be read as "the cluster serves nothing": **leave the children
  alone** and call `Invalidate(id)`, so the next sweep run commits whatever it finds and the
  kinds-present path rewrites rows and children from a real answer. Without this, one cache
  clear would prune every child and kill sync permanently, because the fingerprint guard
  suppresses the recommit that is the only recovery. The check makes every wipe self-healing
  rather than each wiper having to remember to poke the catalog.

Pause, `NoConnection`, and the other disarm verdicts leave the rows as they leave the children:
the subtree survives, only discovery stops. A restart holds nothing in memory, and `startupPass`
re-runs the sweep; its commit (fingerprint vs. an empty observable) takes the kinds-present path
and rewrites the rows idempotently over what the previous process left.

## What this amends

- **cached-data spec**: `WatchKinds`'s rows now mean "what discovery serves and the cache
  mirrors" — an advertised kind appears with `Count` 0 before its worker has synced anything,
  which is what `ClusterCachedDataKind`'s doc always promised the nav. `is_crd` comes from the
  row the fold wrote; the worker-registration sentence goes.
- **cached-resource-sync spec**: workers no longer register `kind_catalog` rows on start, and
  the `is_crd` relay (sweep → `toResourceSpecs` → `ClusterCachedResourceSpec` → `Params` →
  registration) is deleted — the fold writes the bit straight from the sweep's answer. What
  remains on the worker is writing objects and cookies.
- **TODO.md**: the "catalog stays resident" item is superseded — delete it when this lands, and
  carry its weighing (and the fingerprint-vs-disk answer to its two blockers) into the ADR.

Both spec edits happen in the same change that adds this file.

## Order of work (red/green)

1. `kubecatalog`: `IsCRD` on `Kind` + the sweep deriving it (refused CRD list →
   best-effort false); fingerprint-based commit guard; the pending-kinds seam; `Compact` with
   its stale-fingerprint no-op and the silent-passes-after-compact pin; `Invalidate` (force
   flag, baseline reset, wake), with two pins: an invalidate followed by an *unchanged*
   cluster answer still signals promptly — the step-3 end-to-end test stalls on the resync
   backstop without it — and a sweep that *fails* right after an invalidate still commits on
   the ladder's retry, the flag having survived the failed run. White-box tests over the
   existing seams.
2. `kubestore`: `SyncKinds` (upsert/prune/collision/fingerprint, one transaction), `ClearKind`
   keeping the row, dead-handle and tombstoned-`Acquire` semantics after `Delete` (a test pins
   that an acquire after a delete is refused, not a fresh file).
3. The fold: the held handle (acquire/release on the arm/disarm branches), the two converge
   paths, compact-after-write ordering. **Depends on the cached-data spec's step 1** for the
   store's `Kinds`-style read the compacted path reconciles from — build on (or share) that
   read rather than a second one. Tests mirror `cachedcatalogs_test.go`, plus one pinning each
   of: rows-before-children ordering, no prune on `Partial`, retry-after-failed-write still
   holds kinds, the compacted path reconstructing a drained child from disk, and the
   fingerprint-mismatch path leaving children untouched and invalidating (the Clear-recovery
   sequence end to end: clear → mismatch → invalidate → recommit → rewrite).
4. The spec amendments above, and the `TestUnimplementedBoundaryPanics` inventory is untouched —
   this spec serves no new boundary method.

When it lands: fold into `sidecar/CLAUDE.md` (the fold's store claim, the compact lifecycle,
`kind_catalog`'s single writer), write the ADR, delete this spec and the TODO item.
