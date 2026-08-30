---
title: Every row carries its write position, and a delete leaves a log entry
scope: sidecar
status: Landed
order: 1
---

# Every row carries its write position, and a delete leaves a log entry

**Needs:** nothing.
**Hands on:** `write_seq` on `objects` and `events`, the `deletes` table, the head and the
per-kind trim mark — everything spec 2 reads.

## Goal

Let a reader ask "what changed since X?" instead of re-reading a whole kind and diffing it.

Today the cached-data watches learn about changes by re-reading every row of a collection on
each burst of writes (`clustersvc/cacheddatawatch.go`). The read is identity-only, but it is
O(collection) per burst however cheap a row is. Stamping each row with the position of its last
effective write makes the question an index range, O(changes), and gives a reader a position to
resume from.

## Design

**One counter per file, one position per write transaction.** `cluster_meta` holds a `seq` row,
created with the file at `0`. Every write transaction that stamps or logs anything first does
`UPDATE cluster_meta SET value = value + 1 WHERE key = 'seq' RETURNING value` and uses that
number for everything it writes. Rows committed together share a position; a reader sees a
transaction whole or not at all, so that costs nothing. The counter is in the file, so there is
nothing to initialize at open and nothing to reason about between goroutines — the writer pool's
single connection serializes it.

It is ours: it is **not** `resource_version`, which is Kubernetes' own string from the cluster's
etcd, neither ours nor ordered. Nothing here ever compares two of those.

**The head is the counter.** A rolled-back transaction leaves a gap below it, which is harmless:
a reader asks for positions *above* its cursor, and nothing is stamped above the head.

**`objects` and `events` gain the column.** In `0001_init.sql` — nothing has shipped, so this is
an edit to the initial schema, not a migration
(→ [ADR](../adr/2026-08-29-schema-edit-not-migration.md)):

```sql
    write_seq        INTEGER NOT NULL,    -- position of the last effective write; see deletes
...
CREATE INDEX objects_kind_seq ON objects(api_version, kind, write_seq);
CREATE INDEX events_seq       ON events(write_seq);
```

`events` also gains `resource_version TEXT NOT NULL`, read in `extractEvent` the way
`projectObject` reads it, so both tables have the same "did it change" fact.

**A row's `write_seq` moves only when its `resource_version` does.** A relist rewrites every row
of a kind unchanged. Moving the stamp on every rewrite would turn each cold list into one change
per object, and a reader would receive all of them to learn that nothing moved. Both upserts keep
the old stamp on a matching version:

```sql
    write_seq = CASE WHEN excluded.resource_version = objects.resource_version
                     THEN objects.write_seq ELSE excluded.write_seq END
```

A new uid takes the fresh stamp by definition. A re-firing event moves its `resource_version`, so
a count bump moves the stamp. This line is the whole design: without it the stamp is a write log
in disguise, and every relist is N `Modified` frames.

**A delete is logged, because the row is gone.** A reader learns of a delete after the row it
would read is deleted. So every path that removes a row first copies the doomed rows' identity
into `deletes`, in the same transaction:

```sql
-- One entry per deleted object or event row. A write's position is on the row itself
-- (write_seq); the row is gone by the time a reader learns of a delete, so the uid is kept
-- here. Identity only: the reader holds the row's last-known state and keys the removal by uid.
CREATE TABLE deletes (
    seq          INTEGER NOT NULL,       -- the counter write_seq is stamped from
    api_version  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    uid          TEXT NOT NULL,
    at           INTEGER NOT NULL        -- unix millis, for the retention sweep
) STRICT;
CREATE INDEX deletes_kind_seq ON deletes(api_version, kind, seq);
```

No sort keys. Nothing reads them: spec 2's reader keeps every row it has sent and emits the
delete from that copy. A reader that holds only a window of a kind would need them to place a
delete it cannot see; that reader is a later spec, and the columns go in with it.

The paths, and what each logs:

| Path | Statement | Entries |
| --- | --- | --- |
| watch delete | `deleteObjectRow` / `stmtDeleteEvent` | one |
| relist prune | `sweepObjects` / `stmtPruneEvents` | one per pruned row |
| kind eviction | `stmtClearObjectsOfKind` / `stmtDeleteAllEvents` | one per row |

Each is `INSERT INTO deletes … SELECT … FROM <table> WHERE <the delete's own predicate>`
followed by the delete, so the two cannot disagree. Events log under the fixed `('v1', 'Event')`
the count triggers already use.

**A uid's last action is what the tables say about it.** A row and a log entry for the same
uid can coexist — `Clear` logs a delete per row, then the restarted sync cold-lists the same
objects back at a higher position — but a uid whose *last* action was a delete has no row. So
the writes range only ever returns rows that still exist, and a reader that applies deletes
before writes always lands on the live row. Spec 2 leans on this.

**Deletes only.** A write's position is on its row, and the row is there for as long as the
object is, so a write is never missed and never expires; logging it too would store the same
facts twice on the hottest path. Nothing consumes a per-object change history — `status_history`
keeps the one transition anyone has asked about — and the counter is shared either way, so a
history is a new table beside this one if a consumer appears.

**The janitor trims the log by age, and records how far, per kind.** `kubestore/janitor.go`'s
sweep gains one transaction: read the highest `seq` being removed for each `(api_version, kind)`
(`GROUP BY` over `at < now − DeletesTTL`), delete those rows, and upsert each kind's number into
`cluster_meta` under `deletes/trimmed/<api_version>/<kind>`. A single mark would not do: cursors
are per kind, and one busy kind's deletes would push a global mark past every quiet kind's cursor
within minutes. A mark only ever goes up; a kind that has never trimmed has none, which reads as
`0`. `Retention.DeletesTTL` is one hour. Age rather than count, because a reader's cursor goes
stale by time. No index on `at`: the table holds an hour of deletes and the sweep runs every five
minutes — the same trade `status_history`'s sweep makes, with the same escape hatch. The trim and
its marks are one transaction: a reader that reads a mark and the entries above it in one
transaction cannot be trimmed between the two. A kind's mark outlives the kind — `ClearKind`
drops only the cookie and the catalog prune never touches `cluster_meta` — which at one small
row per kind ever seen is not worth a sweep of its own.

**A cursor is valid when its kind's `trimmed <= cursor`.** A cursor means "I have seen everything
up to and including this position." Every delete above it must still be in the log, so nothing
at or below the mark may be relied on. What a reader does with an invalid cursor is spec 2's.

## Tests

In `kubestore`, beside the write paths they cover:

- A first write stamps the row at the transaction's position; the same body written again leaves
  the stamp; a body with a new `resource_version` moves it above every earlier stamp.
- A relist that rewrites the kind unchanged leaves every stamp as it was.
- Each delete path logs one entry per removed row, at a position above every stamp written
  before it.
- The head after a write then a delete is the delete's position.
- A re-firing event (same uid, new count) moves its stamp; an unchanged one does not.
- Entries older than `DeletesTTL` go, newer ones stay, and each kind's mark is its highest removed
  `seq`; a kind with nothing removed keeps its mark; a sweep that removes nothing writes none.

## When it lands

Fold the column, the table, the counter and the trim into `sidecar/CLAUDE.md` beside the
`kubestore` schema notes and the janitor's job list. Write the ADR with spec 2.
