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

**A row's `write_seq` moves only when the write was effective.** A relist rewrites every row of a
kind unchanged. Moving the stamp on every rewrite would turn each cold list into one change per
object, and a reader would receive all of them to learn that nothing moved. So both upserts keep
the old stamp on a re-observation, and unchanged means unchanged in full:

```sql
    write_seq = CASE WHEN excluded.resource_version <> ''
                      AND excluded.resource_version = objects.resource_version
                      AND excluded.api_version = objects.api_version
                      AND excluded.kind = objects.kind
                     THEN objects.write_seq ELSE excluded.write_seq END
```

An *empty* `resource_version` is equal to itself forever and nothing upstream rejects one, so
reading it as unchanged would freeze the row at its first write, invisible to every later cursor.
Identity is rewritten by the same `SET` list, so a uid that moved kind has to take a fresh stamp
or it sits below its new kind's readers. The events upsert has no identity columns — its log key
is the fixed `('v1', 'Event')` — so it carries the non-empty test alone.

A new uid takes the fresh stamp by definition. A re-firing event moves its `resource_version`, so
a count bump moves the stamp. This line is the whole design: without it the stamp is a write log
in disguise, and every relist is N `Modified` frames.

**A delete is logged, because the row is gone.** A reader learns of a delete after the row it
would read is deleted. So every path that removes a row first copies the doomed rows' identity
into `deletes`, in the same transaction:

```sql
-- One entry per row a reader can no longer reach. A write's position is on the row itself
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
| identity change | `objects_identity_change_log` (trigger) | one, under the old kind |

Each of the three delete paths is
`INSERT INTO deletes … SELECT … FROM <table> WHERE <the delete's own predicate>` followed by the
delete, so the two cannot disagree. Events log under the fixed `('v1', 'Event')` the count
triggers already use.

**A row that leaves a kind is a delete to that kind's readers**, since a reader takes both a
kind's rows and its deletes by `(api_version, kind)`. A uid whose identity is rewritten in place —
a preferred-version flip reaching the upsert before the old kind's `ClearKind` — would otherwise
be in neither range: it stops matching the old kind's rows, and no path deleted it. A trigger
beside `objects_kind_count_update`, on the same `WHEN`, logs the departure under the kind the row
left, at `new.write_seq` — which is the transaction's position because the `CASE` above moves the
stamp for exactly this case. A trigger and not a statement: it is not a delete path, so no call
site would think to log it, and the write path pays nothing on the rewrites that do not move
identity.

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
`0`. `Retention.DeletesTTL` is one hour, and a zero one trims nothing — the TTLs are independent,
so a partial `Retention` must leave the table it says nothing about alone rather than take a
cutoff of now to the whole log. Age rather than count, because a reader's cursor goes
stale by time. No index on `at`: the table holds an hour of deletes and the sweep runs every five
minutes — the same trade `status_history`'s sweep makes, with the same escape hatch. The trim and
its marks are one transaction: a reader that reads a mark and the entries above it in one
transaction cannot be trimmed between the two. A kind's mark outlives the kind — `ClearKind`
drops only the cookie and the catalog prune never touches `cluster_meta` — which at one small
row per kind ever seen is not worth a sweep of its own.

**A cursor is valid when its kind's `trimmed <= cursor`.** A cursor means "I have seen everything
up to and including this position." Every delete above it must still be in the log, so nothing
at or below the mark may be relied on. What a reader does with an invalid cursor is spec 2's.

**Both positions fail closed.** A mark that will not parse, and a head that is missing (the
migration seeds the counter, so absence means the file is not one we wrote), are errors rather
than zeros. Zero is the answer that says every cursor is still valid and nothing has ever been
written — the silently missed delete this log exists to prevent.

## Tests

In `kubestore`, beside the write paths they cover:

- A first write stamps the row at the transaction's position; the same body written again leaves
  the stamp; a body with a new `resource_version` moves it above every earlier stamp.
- A relist that rewrites the kind unchanged leaves every stamp as it was.
- A body with no `resource_version` moves its stamp on every write, on both tables.
- An object whose `(api_version, kind)` changes moves its stamp and logs a delete under the kind
  it left, at that same position.
- Each delete path logs one entry per removed row, at a position above every stamp written
  before it.
- The head after a write then a delete is the delete's position.
- A re-firing event (same uid, new count) moves its stamp; an unchanged one does not.
- Entries older than `DeletesTTL` go, newer ones stay, and each kind's mark is its highest removed
  `seq`; a kind with nothing removed keeps its mark; a sweep that removes nothing writes none; a
  zero TTL removes nothing at all.
- An unparseable mark and a missing counter are errors, not zeros.

## When it lands

Fold the column, the table, the counter and the trim into `sidecar/CLAUDE.md` beside the
`kubestore` schema notes and the janitor's job list. Write the ADR with spec 2.
