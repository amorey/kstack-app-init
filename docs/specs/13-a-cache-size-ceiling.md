---
title: A cache stops growing before the disk fills
scope: sidecar
status: Planned
---

# A cache stops growing before the disk fills

**Needs:** [9-bound-the-events-table.md](9-bound-the-events-table.md) should land first — it removes
the most likely cause of unbounded growth, and this spec is the backstop for everything else.
**Hands on:** nothing.

## Goal

Give a cache file a maximum size, and make hitting it visible instead of fatal.

Retention bounds each table by age. It does not bound a cluster with a hundred thousand large
objects, and no age cap helps there. Today the only limit is the disk — and a hostile cluster can
choose to fill it.

## The hard part is not the measurement

The size is already measured: `Manager.Stats` (`kubestore/manager.go:313`) reports the database,
`-wal` and `-shm` files apart and `Bytes()` sums them. **Use that sum, not `page_count ×
page_size`**: a sync's writes sit in the WAL until a checkpoint lands, so the database file alone
reads low exactly while a cache is growing fastest. The design work is everything after:

- **`kubestore` cannot raise a condition.** It lives at
  `sidecar/internal/clustersvc/internal/kubestore`, below the cluster controllers, and has no path
  to a cluster record. It reports through its bus (`f.notify`) and its return values, and that is
  the whole vocabulary it has today.
- **Something must pause the sync, and there is already a pause.** `armSync`
  (`caches.go:888`) calls `ForgetDiscovery` when `cacheSyncEnabled` is false and `TrackDiscovery`
  otherwise; the kinds stay registered under `kubesync` across a pause, so a resume restarts every
  one of them. The ceiling is one more input to that same switch — not a new mechanism, and not a
  flag inside the store.
- **And un-pause.** A ceiling that latches is a cache that never recovers; clearing the cache, or
  the user raising the limit, has to release it.
- **The UI does not read the `Synced` condition.** `clusters.tsx` leaves it out of its selection on
  purpose; what the UI renders is the health gauge (`clusterCacheHealthWatch`), whose reason is
  computed in `caches.go` around line 535. A verdict written only to the condition is invisible.

So the shape is: **the store measures and reports; `clustersvc` decides; the gauge shows.**
Concretely —

1. `kubestore`'s janitor takes a `SizeLimit` from `Retention` (a field, so a test can shrink it,
   the way `vacuumPagesPerSweep` is a var for exactly that reason; default 2 GiB). Each sweep
   compares the three-file sum against it and pings a bus key when the verdict *changes* — over to
   under, or under to over. The ping is a wake; the decision reads `Stats`.
2. The cache controller subscribes to that key, and on its pass reads `Stats`; over the limit it
   pauses through `ForgetDiscovery` and writes `Synced=False` / `ReasonSizeLimit` on the cache
   record, with a message naming the size and the limit. The health gauge grows an arm above the
   per-kind fold — beside `storeFailed` — that reports `ReasonSizeLimit` while the record says so.
3. The condition clears, and `TrackDiscovery` resumes, when a later sweep reports the size back
   under the limit. `clusterCacheClear` already exists and is the user's remedy: it reopens the
   file, and `openFile` starts a janitor whose first sweep runs at once, so the release follows the
   clear without waiting an interval.

**Do not delete objects to make room.** A cache that silently drops what the user asked it to mirror
answers questions wrongly, which is worse than answering "I stopped". The user decides.

**The ceiling is soft by one interval.** Sweeps run every five minutes, so a cache can overshoot by
whatever arrives in between. Say so in the field's doc comment; nobody should read 2 GiB as a hard
bound.

## What to build

The three steps above, in that order, each with its own tests.

**The condition already has a name; the reason does not.** `ConditionSynced` exists in
`clustersvc/shared.go:324`, and `ClusterCache.conditions` is documented in the schema as coarse.
Nothing writes one yet — the comment on `ConditionSynced` says so — so this is new work with a
settled condition name, not a new mechanism.

**Do not reuse `ReasonPaused`.** It already means sync deliberately off — "sync-disabled,
deactivated, orphaned, or archived" (`shared.go:353`), written on the health gauge at
`caches.go:536`, `:648` and `:752`. Reusing it makes "the user turned sync off" and "this cache hit
its ceiling" indistinguishable to anything keying on the reason, which is what a reason is for; a
differing message does not fix that. Add `ReasonSizeLimit` beside it.

## Tests

With the limit shrunk to a few pages:

- A sweep over the limit pings; a second sweep still over it does not; back under it pings again.
- The controller pauses discovery and writes the condition; the gauge reports `ReasonSizeLimit`.
- A sweep back under the limit clears the condition and syncs resume.
- Nothing is deleted by any of it.

## When it lands

Move the row *"Retention on cached Kubernetes events, and a size ceiling on a cache"* in
[`docs/security-model.md`](../security-model.md) out of **Not built** (spec 9 moves the events
half). Document the ceiling in `sidecar/CLAUDE.md` beside the retention paragraph.
