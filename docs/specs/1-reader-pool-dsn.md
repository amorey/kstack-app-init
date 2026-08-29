---
title: The reader pool gets its own DSN
scope: sidecar
status: Planned
order: 1
---

# The reader pool gets its own DSN

**Needs:** nothing. **Hands on:** a reader pool that refuses writes, which
[spec 6](6-prepared-statement-cache.md) needs to route a statement to a pool by what it does.
First because it is small, it closes a trap, and everything after it opens connections.

## Goal

Stop opening the reader pool with the writer's DSN.

`openFile` (`kubestore/manager.go`) opens both pools through the same
`sqlitemigrate.OpenPool`, so the four reader connections carry `_txlock=immediate`,
`journal_mode(WAL)` and `foreign_keys(on)` — none of which a reader wants, and the first of
which is a write lock.

**Nothing is broken today.** modernc only applies the DSN's begin mode when the transaction is
not read-only (`tx.go:23`), and the one reader transaction there is
(`KindsWithFingerprint`, `reads.go`) passes `ReadOnly: true`. The trap is the next one: a reader
transaction that omits the flag silently issues `BEGIN IMMEDIATE`, takes the WAL write lock, and
contends with the writer — which is the single thing the reader pool exists to avoid, and it
would show up as a latency mystery rather than an error.

## Design

`sqlitemigrate` gains `OpenReadPool(path string, maxConns int) (*sql.DB, error)` beside
`OpenPool`, with the same pool settings — `SetMaxOpenConns(maxConns)`, and whatever `OpenPool`
does about idle connections, so the two stay shaped alike. `openFile` uses it for `readDB`.

The DSN keeps one of the writer's five settings and adds one:

```
"?_pragma=busy_timeout(5000)&_pragma=query_only(true)"
```

**`query_only` is enforced**: a write on this pool fails with "attempt to write a readonly
database" rather than reaching the file.

**No `_txlock`.** `BEGIN IMMEDIATE` takes a write lock, which `query_only` refuses, so inheriting
it would fail every transaction here rather than only the ones that forgot a flag.

**No `journal_mode`, `synchronous` or `foreign_keys`.** The writer owns the journal, `synchronous`
governs fsync on write, and foreign keys are enforced where rows are written. `busy_timeout` stays:
a reader still waits on a lock.

`internal/appdb` calls `OpenPool` for its writer and is untouched.

## Rules

- **A reader pool is opened by `OpenReadPool`, never by `OpenPool`.** The one-line convenience of
  reusing the writer's opener is what put a write lock on the read path.
- **`query_only` is the enforcement; `ReadOnly: true` is not.** A caller may still pass it, but
  correctness must not depend on every future caller remembering.
- **The read DSN never takes `_txlock`.** Adding it back "for consistency" with the writer is the
  regression this spec exists to prevent.

## Not in this pass

- **Sizing the reader pool.** `readerPoolSize` stays 4.
- **`openReadOnly`'s DSN** (`manager.go`), which serves the stats read on a cache with no live
  file and has its own reason for `mode=ro`.

## Build order

1. `OpenReadPool` in `sqlitemigrate`, with three assertions: a write through it fails, a read
   succeeds, and **`BeginTx(ctx, nil)` succeeds**. The third is the one that pins the trap — under
   `query_only` an `_txlock=immediate` fails at `BEGIN`, so that assertion goes red the moment the
   writer's DSN is copied back onto this pool.
2. `openFile` uses it for `readDB`.

## Done when

A write issued on a cache's reader pool is refused, every existing read still answers, and no
reader transaction can take a write lock by forgetting a flag. Delete this spec when step 2 lands.
