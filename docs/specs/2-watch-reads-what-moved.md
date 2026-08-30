---
title: The watch reads what moved past its cursor
scope: sidecar
status: Planned
order: 2
---

# The watch reads what moved past its cursor

**Needs:** spec 1. **Hands on:** nothing. The cursor never leaves the sidecar; a client-facing
cursor (a resumable or paged watch) would build on it.

## Goal

Remove the per-burst scan from the objects and events watches, with no change to the protocol a
client receives.

`clusterCachedDataObjectsWatch` and `clusterCachedDataEventsWatch` keep their shape exactly:
the snapshot as `Added` frames, one `Bookmark`, then `Added`/`Modified`/`Deleted` per debounced
burst. Only what `resync` reads changes: the rows above the cursor instead of the whole
collection. The kinds watch (~150 rows) keeps the diff; there is nothing to gain there.

## Design

**Each read is one transaction that also returns the position it is current at.** The pattern is
`KindsWithFingerprint` in `kubestore/reads.go`: rows and position together, or a write between
two reads changes rows the cursor claims to cover.

- The snapshot read becomes `ObjectsWithHead(ctx, apiVersion, resource)` and
  `EventsWithHead(ctx)`, returning the head beside the rows. `Objects` and `Events` have no
  caller outside the two watches and their tests, so they are renamed rather than kept.
- A new `ObjectChanges(ctx, apiVersion, resource, since)` and `EventChanges(ctx, since)` return
  the kind's trim mark, the head, the rows written above `since`, and the uids deleted above it:

```sql
-- writes: stmtSelectObjects' own column list, bounded
SELECT uid, api_version, kind, namespace, name, resource_version, created_at
  FROM objects WHERE api_version = ? AND kind = ? AND write_seq > ?;
-- deletes
SELECT uid FROM deletes WHERE api_version = ? AND kind = ? AND seq > ?;
```

Two plain range reads, no merge, and **deletes are applied before writes**. A uid can be in
both: `Clear` logs a delete per row and the restarted sync cold-lists the same objects back
above it. But the writes range only returns rows that still exist (spec 1), so the only overlap
is delete-then-write, and deletes-first lands on the live row. Writes-first would remove it from
`prev`, and on a quiet kind nothing would re-send it. `EventChanges` is the same two statements
against `events` with `stmtSelectEvents`' column list, and `('v1', 'Event')` in the log.

A row appears at most once, at its latest position, so a burst of writes to one Pod is one row
with no coalescing step; a uid deleted after the cursor is one uid in the second read and
nothing in the first.

**Resolve the kind once, and treat "no catalog row" as the diff.** `stmtSelectObjects` finds
the kind through a scalar subquery on `kind_catalog`, and a kind whose catalog row is gone
reads as empty — which today's diff turns into a `Deleted` per row, correctly. The catalog row
can go on its own: `SyncKinds`' prune removes it independently of `ClearKind`, which is why
`ClearKind` takes the whole `Kind` from the cache record. A changes read with the same subquery
would resolve `NULL`, match nothing in either table, and report nothing, leaving the client
holding rows for a kind the cache no longer serves. So `ObjectChanges` resolves the kind first;
when there is none, it reports that, and `resync` takes the diff path below, which reads empty
and deletes everything the client holds.

**The loop gains one hook and keeps its shape.** `cachedDataWatchSpec` gets
`changes func(ctx, s, since) (changes, error)`, nil for kinds, where `changes` carries the
written rows, the deleted uids, the head, the mark, and whether the kind resolved. `read`
returns the head beside the rows; the kinds watch returns `0`, since with `changes` nil nothing
reads its cursor — `KindsWithFingerprint`'s fingerprint is a hash, not a position, and is not
one. Nothing else in `pump` moves: bind before read, snapshot then `Bookmark`, one debounce per
burst, retry in place, a clean end when the file goes
(→ [ADR](../adr/2026-08-26-cached-data-read-loop.md)).

**`resync` reads the changes, or falls back to the diff.**

```
c, err := w.changes(ctx, store, cursor)
if err != nil:                           // as a failed read today: arm retry, touch nothing
    retry.Reset(w.retry); return
if !c.kindResolved || cursor < c.trimmed:
    rows, head, err := w.read(ctx, store)
    if err != nil: retry.Reset(w.retry); return
    prev = sendDiff(prev, rows)          // today's path, unchanged
    cursor = head
else:
    for uid in c.deleted:   if row, held := prev[uid]; held → Deleted(row); delete prev[uid]
                            else → nothing: it came and went inside the window
    for row in c.written:   if uid in prev → Modified(row) else Added(row); prev[uid] = row
    cursor = c.head
```

The cursor moves only after every frame of a read is out. A read that failed, or a consumer that
left mid-burst, leaves `prev` and the cursor where they were; advancing the cursor over
half-applied changes would lose those frames for good.

`prev` stays. It is the identity map the loop holds today, and it is what lets the server keep
saying `Added` versus `Modified` — the store records neither, and the client protocol does not
change. A `Deleted` frame carries `prev`'s row **verbatim**, as it does today; the log supplies
only the uid. The frame builder is untouched: objects still fetch a body per `Added`/`Modified`
frame and none for `Deleted`.

**The cursor starts at the snapshot's head — or at `0`.** A watch bound after an empty snapshot
(`boundLate`) has read nothing and starts from `0`. Its first resync takes whichever path the
mark selects — the changes read when the kind has never trimmed, the diff when it has — and
either way sends the whole kind as `Added`, exactly what the armed debounce reads in today.

**The fallback is the existing code, and a timing, not a fault.** A cursor falls below its
kind's mark when that kind's deletes older than `DeletesTTL` are trimmed past it — a watch on a
kind with deletes but no later resync for an hour. The cost is one full diff, which is what
every burst costs today, and the mark is per kind so one busy kind never triggers it for
another.

**Two things a client can observe, neither of which changes what it ends up holding.** Today's
events diff compares the whole `EventRow`; the stamp compares `resource_version`. A server write
that moves an event's `resource_version` and nothing else now reaches the client as a `Modified`
it would not have seen — objects already behave this way, and a `Modified` with equal fields folds
as a no-op. And the clear-then-relist burst is a `Deleted` then an `Added` for a row that never
left, where today's diff emits nothing when both land inside one debounce window. The client folds
a burst in one batch, so nothing flickers; and on a kind large enough for the relist to outrun the
250ms window, today's path sends the same two frames. The tests assert final state, not frame
count, for this reason.

**No file id, no per-burst `Bookmark`, no `Expired` frame.** The cursor lives in `pump` for one
subscription's life. A `Clear` installs a fresh file whose counter restarts, and closing the old
one ends this watch cleanly today; the reconnect snapshots against the new file and takes its
head. All three become necessary the moment a cursor is handed to a client.

## Tests

In `cacheddatawatch_test.go`, through the harness the existing tests use, with the spec's hooks
counted so a test can say which read ran:

- A write to a held uid after the snapshot yields one `Modified`, and the full `read` ran once
  (the snapshot), not twice.
- A new uid yields `Added`; a delete yields `Deleted` carrying the row as last sent; a uid
  written and deleted inside one burst yields nothing.
- Three writes to one uid in a burst yield one frame.
- A trim mark above the cursor takes the diff path and produces the same frames the changes
  path would have.
- A kind whose catalog row is gone yields `Deleted` for every held row.
- Clearing a kind and relisting the same uids leaves the watch holding them: the delete and the
  re-add land in one burst, and the watch ends with the rows, not without them.
- A failed changes read arms the retry and leaves `prev` and the cursor untouched; the retry
  sends the frames the failed read would have.
- A watch bound after an empty snapshot receives the kind as `Added` on its first resync.
- The kinds watch, with `changes` nil, still diffs.

In `kubestore`: `ObjectChanges`/`EventChanges` return the head, the mark and both sets from one
transaction; a `since` at the head returns nothing; a resource with no catalog row reports so.

## When it lands

Rewrite the cached-data watch paragraph in `sidecar/CLAUDE.md`: the objects and events watches
read what moved past their cursor and diff only when the kind is gone or the cursor is below the
trim mark. Write one ADR for specs 1 and 2 together — the position on the row, deletes-only,
identity-only log entries, and the tail inside the existing watch rather than a new protocol —
and delete both specs.
