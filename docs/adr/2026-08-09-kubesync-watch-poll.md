---
title: Sync via watch-for-latency, poll-for-correctness with graded liveness proofs
date: 2026-08-09
scope: sidecar
status: Accepted
---

# Sync via watch-for-latency, poll-for-correctness with graded liveness proofs

## Context

`internal/cluster/cache/kubesync` mirrors one Kubernetes collection into one local table, one
worker per kind, a hundred-plus per cache. Watches are cheap but lossy (dropped deltas, stale
resume cookies, 410s); exact stateful edge-handling for every failure mode is where sync
engines go to die. The machine must also tell the user the truth about staleness — including
the case where a watch *connects* but never streams.

## Decision

**Watch for latency, poll for correctness.** The watch path makes the steady state cheap and
is allowed to be lossy; pull-based backstops guarantee correctness, so known edge cases
self-heal within one interval. A run resumes from a persisted resourceVersion cookie via a
RetryWatcher (a client-go Reflector can't be seeded — it re-lists every body on every wake).
With no cookie or after a 410 it falls back to a **paginated full LIST that prunes by
mark-and-sweep**: pages land in their own transactions, `Commit` deletes rows still below the
session's mark (a keep-set would defeat pagination; per-row deletes would hold the cache's
single writer across thousands of statements). The mark is 1ms *past* session start because
`updated_at` has millisecond resolution (`TestReplacePrunesRowsWrittenInTheSameMillisecond`).
**The cookie means "a full LIST completed on disk"**: the first `WritePage` deletes it in the
same transaction as the first rows; only `Commit` rewrites it — partial rows and a cookie
that resumes past them can never coexist. A jittered 30m re-list backstops everything; where
the store supports it, resync is a metadata-only **diff** (list identities, GET only moved
bodies, one batched delete of the vanished) falling back to the full LIST for cold caches or
large deltas. `Store.ApplyChange` advances the cookie in the row's own transaction — a
trailing `PersistRV` would double writer-lock acquisitions on the hottest path and open a
row-durable-behind-cookie window.

**Liveness proofs are graded, not one "last alive" stamp.** Three stamps: `lastWriteAt` (data
landed), `lastProofAt` (write, bookmark, or empty-but-complete LIST), `firstConnectAt` (watch
established). A connect is deliberately **weak**: RetryWatcher retries 5xx internally without
ending the phase, so a watch that accepts connections and never streams would otherwise
report `Watching` forever; instead the stale clock runs from the episode's first connect and
such a loop ages into `StateStale`/`CauseWatchNotStreaming`. For the same reason
**establishing is not budget credit**: crediting the open would refill the error budget
forever and re-LIST the whole collection every backoff step. Consecutive failures spend a
per-worker error budget; exhausted, the worker drops to a slow retry cadence (3m) and reports
immediately. A 30s monitor tick reports staleness *with a cause* (`CauseListFailed` /
`CauseWatchFailed` / `CauseWatchStalled` / `CauseWatchNotStreaming`) so the controller can
render an actionable message.

**Events are an ordinary synced kind** — same controller, worker, and state machine —
differing only in store (`eventsync` vs `objectsync`): own table/FTS, retention mirrored from
the server by the relist prune (not the janitor), a dedicated write broker so event bursts
don't wake object watches, both API spellings normalized to one uid-keyed row. Hence the
discovery filter drops the *non-canonical* `events.k8s.io` spelling (two workers would fight
over the same rows) — keyed on group+plural, not the Kind name, since "Event" isn't reserved.
Discovery also seeds the `v1/events` child before anything that can fail or wait: events are
the highest-value diagnostic data, and on a throttled cluster this is the difference between
mirroring events and mirroring nothing.

## Alternatives considered

**client-go informers/Reflector.** Rejected: the Reflector can't seed from a persisted
cookie, so every process start re-lists every body of every kind — exactly the cold-start
cost the cookie exists to avoid — and its in-memory store duplicates what SQLite holds.

**Exact edge-case handling instead of backstops.** Rejected: enumerating every way a watch
can lie is open-ended; a bounded-staleness guarantee (self-heal within one relist interval)
is simpler and honest.

**One "lastAlive" timestamp.** Rejected: it can't distinguish "receiving data" from
"connected but silent", which is a real failure mode users must see.

**A bespoke events pipeline.** Rejected: the sync problem is identical; only the store
differs, and the `Store` interface is exactly that seam.

## Consequences

Correctness degrades gracefully to "at most one interval stale" under any single failure.
The invariants tests pin: cookie-clearing on first write, the 1ms mark, no credit for
establish, bookmark handling behind the watch-epoch guard. LIST-phase concurrency is bounded
by a shared per-cache limiter (`WithListLimiter`) so a hundred workers don't decode pages at
once.
