---
title: The all-key tables lose their rowid
scope: sidecar
status: Planned
order: 7
---

# The all-key tables lose their rowid

**Needs:** nothing. **Hands on:** nothing. Last because it is the smallest and the most
self-contained — nothing has shipped, so this is an edit to `0001_init.sql` rather than a
migration, and it can be built at any point.

## Goal

Stop storing four tables' keys twice.

`owner_refs`, `labels`, `cluster_meta` and `kind_counts` are all-key or nearly-all-key tables with
a `TEXT` primary key. As rowid tables SQLite stores each row under an integer rowid **and** builds
a separate `sqlite_autoindex` over the declared primary key, so every key is on disk twice.
`WITHOUT ROWID` makes the table its own primary-key b-tree and that second copy disappears.

**The secondary indexes get bigger, and that is the other half of the trade.** A secondary index
on a `WITHOUT ROWID` table stores the full primary key where it stored an 8-byte rowid, and it
still descends the main b-tree to reach the row — the row fetch does not go away, it changes what
it is keyed by. `labels_kv` carries `(key, value, uid)` instead of `(key, value, rowid)`, and
`uid` is a 36-byte UUID.

So the net is the dropped autoindex minus the index growth, which is a measurement rather than a
property. On a fixture of 5k objects with four labels each and one owner reference each:

| | rowid | `WITHOUT ROWID` |
| --- | --- | --- |
| `labels` + its autoindex | 2,158,592 | 1,118,208 |
| `labels_kv` | 425,984 | 1,118,208 |
| `owner_refs` + its autoindex | 835,584 | 409,600 |
| `owner_refs_owner` | 225,280 | 401,408 |
| **file** | **3,649,536** | **3,051,520** (−16.4%) |

Worth having, and worth knowing the shape: a table whose secondary index is wide and whose
primary key is long is where this comes out flat.

**The more interesting payoff is not size.** Both tables are keyed by a uid prefix — `labels` on
`(uid, key)`, `owner_refs` on `(child_uid, owner_uid)` — so `WITHOUT ROWID` orders the rows
themselves by that prefix, and `insertObjectRow`'s and `deleteObjectRow`'s `WHERE uid = ?` becomes
one contiguous descent of the table instead of an autoindex probe followed by a rowid fetch per
row:

```
rowid          SEARCH labels USING INDEX sqlite_autoindex_labels_1 (uid=?)
WITHOUT ROWID  SEARCH labels USING PRIMARY KEY (uid=?)
```

That is the same per-object write path the statement cache is about, twice per
object.

## Design

Four `CREATE TABLE` statements in `0001_init.sql` gain `, WITHOUT ROWID`. Nothing else moves — no
column changes, no code changes, and **no migration**: nothing has shipped, so the initial schema
is still editable and a `0002_*.sql` would exist only to undo a line that can simply be written
correctly.

**An existing dev cache keeps the old form until it is deleted.** Editing `0001_init.sql` changes
what a *new* file is created with and nothing else: `Apply` reads `schema_migrations` at version 1,
sees the embedded set's latest is also 1, and skips. Harmless — no column moves — but every test
builds a fresh temp file and passes, so the divergence is invisible until someone measures a cache
they already had.

**Editing the schema is not merely the tidier of two options.** Rebuilding `kind_counts` in place
needs `ALTER TABLE … RENAME`, which re-parses the whole schema and fails against a trigger naming
a table that the rebuild has just dropped:

```
error in trigger objects_kind_count_insert: no such table: main.kind_counts
```

Five triggers name `kind_counts` (`objects_kind_count_insert`/`delete`/`update`,
`events_kind_count_insert`/`delete`), so the migration route means dropping and recreating all
five — five trigger bodies copied verbatim out of `0001_init.sql` into a second file, where a
transcription slip freezes a counter silently. Editing the schema avoids the whole of that.

**Three tables are deliberately left alone**, and the schema says so beside each:

- **`objects`** — its rows carry `raw_json`. `WITHOUT ROWID` wants small rows, and the blob sitting
  in overflow pages the identity scan never touches is what makes the objects watch's read cheap.
- **`events`** — `events_fts` is declared `content_rowid='rowid'` and its three triggers insert
  `new.rowid`/`old.rowid`. A `WITHOUT ROWID` table has no rowid, so this would break the full-text
  index outright.
- **`status_history`** — a plain rowid table on purpose, so two transitions landing in the same
  millisecond both survive. Giving it a key would be the bug that comment prevents.

`kind_catalog` is also excluded: `schema_json` holds a CRD's OpenAPI schema, which is exactly the
large row the form is wrong for.

## Rules

- **`WITHOUT ROWID` is for small rows whose columns are mostly the key.** A table that grows a wide
  column later must be moved back.
- **A table an FTS index or a rowid-referencing trigger points at keeps its rowid.**
- **The saving is the table's, and the secondary indexes pay some of it back.** Adding a wide index
  to one of these four is what would turn the trade. That sentence is what goes in the schema
  beside each table — not a byte count, which is fixture-specific and stale the moment a column
  moves.

## Not in this pass

- **New indexes.** This changes how existing rows are stored, nothing about what is indexed.
- **Any table not named above.** Each exclusion has its own reason and they do not generalise.

## Build order

1. The four `CREATE TABLE` statements, each with its reason in a comment.
2. Tests: the four tables have no rowid and the three excluded ones do; **and an object inserted
   after the change increments its `kind_counts` row, and decrements on delete**. `kind_counts` is
   the one table here reached only through triggers, so nothing else would notice if its form
   broke them.
3. Re-measure — **on a cache created after the edit**. Delete the caches directory first, or the
   measurement runs against a file still on the old schema, reports no change, and looks correct.
4. The ADR. The durable decision is not `WITHOUT ROWID`, which the schema comments carry; it is
   **editing the initial schema rather than adding a migration, while nothing has shipped**, and
   why that is a real choice: five triggers name `kind_counts`, `ALTER TABLE … RENAME` re-parses
   the whole schema and refuses against a dangling one, so the migration route means five trigger
   bodies copied verbatim where a slip freezes a counter silently. The measured numbers go here
   too. After shipping a migration is the only option, and this is what the next person meets the
   rename trap with.

## Done when

The four tables read `WITHOUT ROWID`, the excluded three still have a rowid, the kind counters
still move, the schema records why each table has the form it has, and the ADR records the choice
not to migrate. Delete this spec when step 4 lands.
