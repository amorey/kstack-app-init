---
title: The all-key tables lose their rowid, by editing the initial schema rather than migrating
date: 2026-08-29
scope: sidecar
status: Accepted
---

# The all-key tables lose their rowid, by editing the initial schema rather than migrating

## Context

`cluster_meta`, `owner_refs`, `labels` and `kind_counts` are all-key or nearly-all-key tables with
a `TEXT` primary key. As rowid tables SQLite stores each row under an integer rowid **and** builds
a `sqlite_autoindex` over the declared key, so every key is on disk twice. `WITHOUT ROWID` makes
the table its own primary-key b-tree and the second copy goes.

## Decision

**The four tables are `WITHOUT ROWID`, and the change is an edit to `0001_init.sql`.** Nothing has
shipped, so the initial schema is still the whole of the schema: a `0002_*.sql` would exist only to
undo a line that can be written correctly the first time.

**That is not merely the tidier of two routes.** Rebuilding `kind_counts` in place needs
`ALTER TABLE … RENAME`, which re-parses the whole schema and fails against a trigger naming a table
the rebuild has just dropped:

```
error in trigger objects_kind_count_insert: no such table: main.kind_counts
```

Five triggers name `kind_counts` (`objects_kind_count_insert`/`delete`/`update`,
`events_kind_count_insert`/`delete`), so the migration route means dropping and recreating all
five — five trigger bodies transcribed into a second file, where a slip freezes a counter silently
while every write succeeds. After shipping, that route is the only one; this ADR is what the next
person meets the rename trap with.

**Three tables keep their rowid, each for its own reason, and the schema says so beside each.**
`objects` carries `raw_json`, and the blob sitting in overflow pages the identity read never
touches is what makes the objects watch's per-ping read cheap. `events` backs `events_fts`, which
is declared `content_rowid='rowid'` and whose triggers insert `new.rowid`/`old.rowid` — a
`WITHOUT ROWID` table has none. `kind_catalog` holds a CRD's whole OpenAPI schema in `schema_json`.
`status_history` is a keyless rowid table on purpose, so two transitions in the same millisecond
both survive.

## Consequences

**The saving is real and partly paid back.** A secondary index on a `WITHOUT ROWID` table stores
the full primary key where it stored an 8-byte rowid, and still descends the main b-tree for the
row. On 5,000 Pods with four labels and one owner reference each (`dbstat`, after `VACUUM`):

| | rowid | `WITHOUT ROWID` |
| --- | --- | --- |
| `labels` + its autoindex | 2,260,992 | 1,146,880 |
| `labels_kv` | 454,656 | 1,146,880 |
| `owner_refs` + its autoindex | 835,584 | 409,600 |
| `owner_refs_owner` | 225,280 | 401,408 |
| **file** | **6,766,592** | **6,086,656** (−10.0%) |

So the net is a measurement, not a property: a table whose secondary index is wide and whose key is
long is where it comes out flat. Adding a wide index to one of these four is what would turn it.

**The write path gets the better half.** Both edge tables are keyed by a uid prefix, so
`WITHOUT ROWID` orders the rows themselves by it and `insertObjectRow`'s and `deleteObjectRow`'s
`WHERE uid = ?` becomes one contiguous descent instead of an autoindex probe and a rowid fetch per
row:

```
rowid          SEARCH labels USING INDEX sqlite_autoindex_labels_1 (uid=?)
WITHOUT ROWID  SEARCH labels USING PRIMARY KEY (uid=?)
```

Twice per object, on the same per-object write path the statement table is about.

**An existing dev cache keeps the old form until it is deleted.** `Apply` reads
`schema_migrations` at version 1, sees the embedded set's latest is also 1, and skips. Harmless —
no column moves — but every test builds a fresh temp file and passes, so the divergence is
invisible until someone measures a cache they already had. Measure on a cache created after the
edit, or the numbers report no change and look correct.

## Alternatives considered

**A `0002_*.sql` rebuilding the tables.** The honest option once anything has shipped, and the
wrong one while nothing has: it buys nothing a schema edit does not, and costs five verbatim
trigger copies whose failure mode is silent.

**Moving `objects` too.** Declined for the reason its comment gives — the form wants small rows,
and the overflow-page body is load-bearing for the identity read
(→ [ADR: the objects watch reads identity](2026-08-29-object-read-split.md)).
