---
title: Cached events age out
scope: sidecar
status: Planned
---

# Cached events age out

**Needs:** nothing. **Hands on:** [13-a-cache-size-ceiling.md](13-a-cache-size-ceiling.md) is the
other half of the same problem and is a much larger piece of design.

## Goal

Stop a cluster's events accumulating in a cache between relists.

Core `v1` events are stored one row per event UID, with the full compressed body. They are trimmed
in exactly one place: a relist's `Commit` prunes every row the relist did not rewrite
(`stmtPruneEvents`, `kubestore/store.go:641`). Between relists — and a healthy watch runs for days
— nothing removes anything. The janitor deliberately does not touch the table; its `Retention`
comment says events retention is "the server's, mirrored by the relist's prune".

That holds when the cluster expires its own events. A cluster that keeps them, or produces them
faster than we relist, grows the file with no ceiling in between.

## Decide first: which timestamp

`updated_at` is *last sync write*, not last observation. A still-live event that nothing has
rewritten for a day would be swept and then re-added by the next relist as a new row — a flap, and
a delete/add pair every watcher sees.

`last_seen` is the event's own `lastTimestamp`/`series.lastObservedTime`, which is what "old" means
to a user, and it has an index (`events_last_seen`). **Use `last_seen`, and treat a NULL `last_seen`
as not-sweepable** — a malformed event with no parseable time should not be silently dropped. Say
this in the field's doc comment; it is the non-obvious part of the change.

**And apply the same cutoff on the way in.** A server whose own retention is longer than ours
still holds an event we swept, and the next relist would write it back — the same flap by another
route, at the relist's cadence. So the write path skips an event whose `last_seen` is already past
the TTL, on relist and on watch alike, and the relist's prune then removes any stored row it
skipped. One predicate, applied in both directions, is what makes the table converge.

## What to build

**1. A new retention field.** In `kubestore/janitor.go`, add to `Retention`:

```go
// EventsTTL caps how long an event survives after the cluster last observed it. Zero
// keeps everything. Unlike the other two, this table has an upstream owner — a relist
// prunes it as well — so this is a ceiling on the gap between relists, not the primary
// trim. An event with no parseable observation time is never swept.
EventsTTL time.Duration
```

`DefaultRetention` sets it to `24 * time.Hour`.

**2. A sweep arm.** In `sweep`, guarded by `ret.EventsTTL > 0`, shaped like the `StatusHistoryTTL`
arm. Three things the existing code requires that are easy to miss:

- **It must be one transaction, and it must log the deletes.** The events prune in `Commit` calls
  `logDeletes(ctx, st, stmtLogPruneEvents, stamp, r.mark)` before deleting, so a reader with a
  cursor learns the rows went. The sweep does the same.
- **`logDeletes` needs a `writeStamp`**, and taking one is itself a write. Take it inside the
  sweep's transaction, the way `Commit` does (`store.go:630`).
- **Call `f.notify(EventsKey)` after the commit.** Without it no live watcher wakes, and the delete
  log the previous point insists on is written for nobody.

A new statement pair is needed — the existing `stmtPruneEvents` filters on `updated_at`, and its
matching `stmtLogPruneEvents` has the same predicate. Add a `last_seen`-based pair beside them
(`WHERE last_seen IS NOT NULL AND last_seen < ?`) rather than changing the relist's, and register
each in **all three** places: the `stmtID` constant, the SQL map, and the write-set map
(`statements.go` — `stmtLogPruneEvents: true` sits at :325, the delete at :347). Missing the
write-set entry compiles and fails at runtime.

The delete itself needs nothing else: the `events_fts_delete` trigger and the events kind-count
trigger keep the search index and the counts correct.

**3. The write-side cutoff.** The `file` already carries its `Retention`; in the events branch of
the upsert path (`store.go`, the `isCoreEvents()` arms), skip a row whose `last_seen` is non-NULL
and older than `now - EventsTTL`. A relist that skips a row it would otherwise have rewritten
leaves that row for its own prune, which already removes everything the relist did not touch — so
step 3 needs no delete of its own.

## Tests

In `janitor_test.go`, with `Interval` shrunk the way the existing tests do it:

- An event whose `last_seen` is older than `EventsTTL` is swept; a newer one stays.
- An event with a NULL `last_seen` is never swept.
- A zero `EventsTTL` sweeps nothing.
- A swept event lands in the deletes log, so a reader's cursor sees it go.
- A watcher subscribed to `EventsKey` wakes after a sweep that deleted something.

Beside the store's own tests:

- A relist that carries an event older than the TTL does not store it, and prunes the stored copy
  of one it previously held — no flap across a sweep and a relist.

## When it lands

Update the `Retention` doc comment in `kubestore/janitor.go` — events stop being the deliberate
exception — and the matching paragraph in `sidecar/CLAUDE.md`, saying why the relist prune is still
the primary trim. In [`docs/security-model.md`](../security-model.md), the *"Retention on cached
Kubernetes events"* row moves out of **Not built**; leave the size ceiling to
[spec 13](13-a-cache-size-ceiling.md).
