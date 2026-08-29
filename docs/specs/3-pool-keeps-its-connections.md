---
title: A pool keeps the connections it opens
scope: sidecar
status: Planned
order: 3
---

# A pool keeps the connections it opens

**Needs:** nothing. **Hands on:** a reader pool that stops churning under load, which
[spec 6](6-prepared-statement-cache.md) compiles against. Third because it finishes the open
contract before anything is built on top of it.

## Goal

Stop churning half the reader pool while it is being used.

`sqlitemigrate.OpenPool` sets `SetMaxOpenConns(maxConns)` and `SetConnMaxIdleTime(5 * time.Minute)`
and leaves `MaxIdleConns` at database/sql's default of **2**. The reader pool is sized 4
(`readerPoolSize`), so a pool that opens four connections keeps two: four concurrent reads leave
`Open=2 Idle=2 MaxIdleClosed=2`.

The two that were closed are reopened by the next read that needs them, and every reopen re-runs
the DSN's pragmas on a fresh SQLite connection. This is not a rare event — a watch re-reads on a
250ms debounce, so the pool closes and reopens connections on the hot path, and closes exactly the
ones it was sized to open.

## Design

`OpenPool` and `OpenReadPool` set `SetMaxIdleConns(maxConns)`. (Before
[spec 1](1-reader-pool-dsn.md) there is one opener to change, not two.)

**`SetConnMaxIdleTime` stays.** The two settings answer different questions — how many connections
to keep while the pool is working, and how long to keep them once it is not — and only the first is
wrong today.

## Why the timeout stays

A claim on a cache is not held by whoever is looking at it.

`kubesync`'s session takes `OpenOrCreate` at `session.go` and releases it only in `close()`, so the
claim is held for as long as the cache is armed. Arming is policy, not interest
([ADR](../adr/2026-08-28-arming-is-policy-never-interest.md)), so every connected cluster holds its
cache file open whether or not that cache is on screen. Only the cached-data watch's claim is
viewer-scoped, and it is the shorter of the two.

So nothing but the clock bounds a quiet cache's connections, and each one holds up to ~2MiB of page
cache (`cache_size` defaults to `-2000`). Four readers and a writer per cache, across every
connected context, is real retention for a desktop app with no user in the loop.

**The timeout costs spec 6 almost nothing.** A reaped connection drops the statements compiled on
it, and `*sql.Stmt` re-prepares on whatever connection it lands on next — one recompile per
statement, once, after five quiet minutes. Set against the ~2,500 compilations a 500-object relist
page pays today, that is noise. A statement cache earns inside a burst, not across the gaps between
them, and the gaps are exactly what the timeout reclaims.

## Rules

- **The pool's idle count matches its open count.** A pool that keeps fewer connections than it
  opens churns exactly the connections it was sized to keep.
- **The idle timeout stays.** A claim is held by the sync session, not by a viewer, so a quiet
  cache's connections have nothing else to bound them.

## Not in this pass

- **`readerPoolSize`.** Whether 4 is the right number is a measurement, not this change.
- **Pinning the writer's connection.** The writer is one connection and is where the compile cost
  concentrates (`insertObjectRow` runs four to six statements per object), so exempting it from the
  timeout is the defensible asymmetry if a relist after a quiet period ever measures badly. It
  needs its own reason; today both pools share one opener and one contract.

## Build order

1. A test that opens a pool sized N, holds N connections out **at once**, releases them, and
   asserts `db.Stats().Idle == N` and `MaxIdleClosed == 0`. It goes red at 2 today.

   The concurrency has to be gated, not just launched: each goroutine checks out a connection and
   signals, and none releases until all N are held. Goroutines started together can otherwise be
   served by one connection, and then `Idle` falls short for a reason that has nothing to do with
   `MaxIdleConns`. No clock is involved — a connection returns to the pool when it is released, so
   the assertion is on what the pool kept, never on how long it kept it.
2. `SetMaxIdleConns(maxConns)` in both openers.
3. `OpenPool`'s doc comment, which calls itself "the single home for the sidecar's SQLite open
   contract" and states the sizing rule. The contract gains one line: a pool keeps every connection
   it opens, until the idle timeout takes it.

## Done when

A reader pool sized N holds N idle connections after its readers finish, a cache that goes quiet
still gives them back, and nothing reopens a connection mid-burst. Delete this spec when step 3
lands.
