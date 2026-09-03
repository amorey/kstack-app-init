---
title: A cache over its ceiling stops growing
scope: sidecar
status: Planned
---

# A cache over its ceiling stops growing

**Needs:** nothing — detection has landed. This spec consumes the verdict and the wake the janitor
already publishes (`kubestore.Manager.WatchSizeLimitNews`, `Stats.OverSizeLimit`) and builds nothing of
its own below `clustersvc`. **Hands on:** nothing.

**This is the half of the security patch that enforces.** **S-5** in
[`security/2026-09-02-threat-model.md`](../security/2026-09-02-threat-model.md), answered by a
whole-file ceiling rather than per-table event retention (→ [bound the cache by total
size](../adr/2026-09-03-bound-the-cache-by-total-size.md)) — and the row it moves is *"A size ceiling on a cache"* in
[`security-model.md`](../security-model.md). Until this lands, a cluster an attacker controls can
still fill the disk; the ceiling is only a number the UI can render.

## Goal

A cache over its ceiling pauses its sync, says why in a way the UI already renders, and resumes on
its own when it is back under.

## Do not delete objects to make room

A cache that silently drops what the user asked it to mirror answers questions wrongly, and a delta
watch on top of it never recovers — it applies deltas to rows it assumes are whole, so a truncated
table renders as a complete one. Answering "I stopped" is worse for nobody and wrong for no one. The
user's remedy is `clusterCacheClear` (`graph/schema.resolvers.go:117`) or a higher limit.

## The pause already exists

`armSync` (`caches.go:888`) calls `ForgetDiscovery` when `cacheSyncEnabled` is false and
`TrackDiscovery` otherwise (`:911`, `:915`). A pause keeps the kinds registered under `kubesync`, so
a resume restarts every one of them in one call. **The ceiling is one more input to that same
switch** — not a new mechanism, and not a flag inside the store, which is the file we may be
declining to write to.

Two consequences worth stating before building:

- **It must not latch.** A ceiling that never releases is a cache that never recovers. The release
  is the same edge the detection spec publishes, in the other direction.
- **A clear releases it without waiting an interval.** `Clear` reopens the file and `openFile`
  starts a janitor whose first sweep runs immediately (`janitor.go:57`), and that sweep publishes
  because a fresh file's verdict memo starts at `unknown` (`checkSizeLimit`, `kubestore/janitor.go`).
  Without that third state the swap looks like "under, unchanged" and this release never fires.

## The UI reads the gauge, not the condition

`clusters.tsx` leaves `Synced` out of its selection on purpose; what the UI renders is
`clusterCacheHealthWatch`, folded in `readCacheHealth` (`caches.go:470`). **A verdict written only
to the condition is invisible.** The health fold needs an arm above the per-kind fold, beside the
`storeFailed` arm at `caches.go:537`, for the same reason that one is there: a paused cache arms
nothing, so every kind reads as unanswered and the default would report a stopped cache as still
connecting.

**Do not reuse `ReasonPaused`.** It means sync deliberately off — "sync-disabled, deactivated,
orphaned, or archived" (`shared.go:353`) — and is written at `caches.go:751`. Reusing it makes "the
user turned sync off" and "this cache hit its ceiling" indistinguishable to anything keying on the
reason, which is what a reason is for; a differing message does not fix that.

## What to build

**1. `ReasonSizeLimit`,** beside `ReasonPaused` in `clustersvc/shared.go`.

**2. A trigger over the size news.** In `triggers.go`, a `newKubestoreSizeTrigger` shaped exactly
like `newKubesyncDiscoveryTrigger` (`:167`) — the key is the cache id, and a cache id *is* the
record's id, so it addresses the record with no store read. Register it with the cache controller
the way the discovery trigger is registered.

**3. The controller pass decides.** On a pass it reads `Stats` (already on the `kubestoreManager`
seam) and keys on **`OverSizeLimit`** — never on `Bytes()` against a limit of its own, which is a
second definition of the ceiling and disagrees with the janitor's over a WAL mid-checkpoint. Over
the limit:

- passes the ceiling into `armSync`'s switch, so the pass calls `ForgetDiscovery`;
- writes `Synced=False` with `ReasonSizeLimit` on the cache record, message naming the size and the
  limit.

`ConditionSynced` exists (`shared.go:324`) and nothing writes one yet — its comment says so, so this
is new work with a settled condition name. Update that comment: this becomes its first writer.

Under the limit, the condition clears and `armSync` resumes `TrackDiscovery` — one path in both
directions, so the release cannot be forgotten in a branch.

**4. The health arm.** `readCacheHealth` reports `ReasonSizeLimit` while the record's condition says
so, above the per-kind fold.

## Tests

With the limit shrunk to a few pages, in `caches_test.go` and the controller's tests:

- The controller pauses discovery and writes `Synced=False` / `ReasonSizeLimit` with the size and
  the limit in the message.
- The gauge reports `ReasonSizeLimit`, and a paused-by-the-user cache still reports `ReasonPaused` —
  the two stay distinguishable.
- A sweep back under the limit clears the condition and resumes `TrackDiscovery`.
- A clear releases it without waiting an interval.
- Nothing is deleted by any of it, and the object rows survive the pause.

No sleeps: the trigger's wake is a `testutil.Signal`, the controller pass a `testutil.Probe`.

## When it lands

Move the *"A size ceiling on a cache"* row in [`security-model.md`](../security-model.md) out of
**Not built**, naming the tests that pin it. Write an ADR for the decision this spec is built on —
**a full cache stops rather than evicts** — since the reasoning is what a later reader will want to
challenge, and record that the ceiling is soft by one sweep interval. Fold the pause and its reason into
`sidecar/CLAUDE.md` beside the ceiling paragraph, and delete this spec.
