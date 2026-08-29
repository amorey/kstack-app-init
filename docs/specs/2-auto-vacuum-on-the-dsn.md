---
title: auto_vacuum belongs on the DSN
scope: sidecar
status: Planned
order: 2
---

# auto_vacuum belongs on the DSN

**Needs:** nothing. **Hands on:** nothing. Second because it is the other half of the open
contract [spec 1](1-reader-pool-dsn.md) touches, and the two want reviewing together.

## Goal

Set the mode where it belongs, so the repair branch means what its name says.

`openFile` (`kubestore/manager.go`) opens the writer, reads `PRAGMA auto_vacuum`, and repairs a
file that is not `INCREMENTAL`:

```go
if _, err := db.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`); err != nil {
```

A brand-new database defaults to `NONE`, so this fires on **every** cache creation. It is not
expensive — the probe runs before `sqlitemigrate.Apply`, so the `VACUUM` rewrites a file with no
tables in it, measured at roughly a quarter of a millisecond per creation. The cost is not the
reason.

The reason is that a branch that fires every time says nothing when it fires. On the DSN a fresh
file is `INCREMENTAL` at creation, and the branch is left meaning one thing: this file was written
by a build that predates it.

## Design

`sqlitemigrate.OpenPool`'s DSN gains `&_pragma=auto_vacuum(incremental)`.

**It must be on the DSN and never in a migration.** SQLite ignores the pragma on a non-empty
database and inside a transaction, and a migration is both — so a migration that set it would be
a silent no-op that reads as correct.

**On the DSN it survives only because modernc orders the pragmas, and that ordering is
load-bearing.** `journal_mode=WAL` writes the database header, and the file is not empty after
that — so a connection that ran WAL first would ignore `auto_vacuum` for exactly the reason a
migration does. It does not, because `applyQueryParams` (`sqlite.go`) issues `busy_timeout` first
and then sorts the rest lexicographically, where `auto_vacuum` precedes `journal_mode`. The DSN's
written order is therefore not what makes this work, and cannot be relied on to. **This needs a
comment beside the DSN** — it is a third-party driver's internal sort, and nothing else in the
repo would record why it matters.

**The repair branch stays**, for the same reason: the DSN cannot fix a file that already exists
in `NONE` mode. Such a file would otherwise keep the mode forever, and the janitor's
`PRAGMA incremental_vacuum` would be a permanent no-op on it — the cache would never hand a page
back. What changes is that the branch stops being the common path: it fires for a file written by
a build that predates this, and not for a file this build creates.

**The reader pool does not get the pragma** once `OpenReadPool` ([spec 1](1-reader-pool-dsn.md))
has its own DSN — and inherits it harmlessly before that, since `readDB` opens after the
migrations, when the file is no longer empty and the pragma is ignored. Nothing is lost either
way: a reader never creates a file.

**`internal/appdb` inherits the mode**, since it shares `OpenPool`. It costs a pointer map on a
file that is small by construction, buys nothing until something vacuums it, and means a future
appdb janitor needs no migration to become possible. Taken deliberately rather than by splitting
the sidecar's one SQLite open contract in two.

**For appdb the split is permanent.** `appdb.Open` has no repair branch, so an existing `app.db`
stays `NONE` for its life and a future janitor would be a no-op on it. Only files created after
this ships are incremental. Acceptable — a janitor is hypothetical and the file is small — but it
is a fork in the fleet, not a uniform mode.

## Rules

- **`auto_vacuum` is set on the DSN.** Anywhere else it is a no-op that looks like a setting.
- **The repair branch is for existing files, not for new ones.** Deleting it strands every cache
  created before this change at its high-water mark.

## Not in this pass

- **The janitor's cadence or page budget** (`vacuumPagesPerSweep`), which is a separate question
  from what mode the file is in.

## Build order

1. The DSN gains the pragma, with the ordering comment beside it. `openFile`'s repair branch keeps
   its behaviour and gains a comment saying which files it is for.
2. The tests, in the two packages that can each see half of it. **Neither needs a seam**, and
   `kubestore` must not grow one: a fresh cache file ends `INCREMENTAL` whether or not the DSN
   pragma took — the probe reads 0, the repair fires, and the mode is right either way — so a test
   there that asserts only the mode can never go red.
   - **`sqlitemigrate`**: open a fresh pool and read `PRAGMA auto_vacuum` before any table exists.
     Nothing in that package has a repair branch, so an ignored pragma reads 0 and the test goes
     red. This is where the ordering hazard lives, so it is where the assertion belongs.
   - **`kubestore`**: the other half, which is observable because the mode changes — a `NONE` file
     with tables in it is `INCREMENTAL` after `openFile`.
3. The three comments this falsifies, none of which survive the change:
   - `manager.go` — `openFile`'s doc, "sets auto_vacuum before any table exists".
   - `store.go` — `file`'s doc, "auto_vacuum=INCREMENTAL is set on the fresh pool before
     migrations run (SQLite silently ignores it once any table exists)".
   - `migrations/0001_init.sql` — a five-line NOTE whose whole subject is that the pragma is not
     set there and is set "on the fresh writer pool just before migrations run". Wrong twice over
     after this, so it is rewritten rather than amended. It also points at `clustercache.Open`,
     which does not exist; the same file's line 60 has the same stale reference for
     `CompressRaw`/`DecompressRaw`. Fix both in this edit.

## Done when

A fresh pool reads `INCREMENTAL` before any table exists, a `NONE` file is still repaired on open,
and no comment still says the pragma is set after the pool opens. Delete this spec when step 3
lands.
