---
title: One SQLite file per cache incarnation, with routed write-notify brokers
date: 2026-08-09
scope: sidecar
status: Accepted
---

# One SQLite file per cache incarnation, with routed write-notify brokers

## Context

Each reachable cluster's objects and events are mirrored locally for offline-tolerant, fast,
SQL-queryable reads. A physical cluster can migrate identities (new kube-system UID), caches
can be cleared and rebuilt, and the file must never be deleted under a live writer or
orphaned on disk. Watch-style GraphQL readers need to know when to re-read without polling.

## Decision

One SQLite file per cache **incarnation** at `<dataDir>/clusters/<ClusterID>/<CacheObjID>.db`,
addressed by the parent Cluster and ClusterCache beehive ObjectIDs (`store.CacheRef`).
AUTOINCREMENT ids mean a delete+recreate yields a fresh, never-reused path — no
finalize-vs-recreate race. A `Manager` owns lifecycle: pure-Go modernc driver, WAL,
single-writer + 4-reader pools, integrity check with corrupt-file quarantine, embedded
migrations, a per-cluster janitor (status-history TTL, incremental vacuum — events excluded;
their retention is server-mirrored). `WatchDB(cacheID)` streams a cache's open handle across
its lifecycle (current-on-subscribe), so long-lived readers bind to a cache that opens later
or rebind across a clear-and-reopen, instead of binding once to `Lookup`.

The schema is agent-optimized: universal `objects` + edge tables (`owner_refs`, `labels`),
`status_history` (transitions only), `events` + FTS, `kind_catalog`, `kind_counts` (kept
exact by triggers on `objects`, so per-kind counts read O(kinds), never a scan), and
`cluster_meta` (resume cookies). Bodies are zlib-compressed, sanitized at write time (see
[ADR: RawJSON scalar](2026-08-09-rawjson-comparable-scalar.md)).

Each `ClusterDB` carries **two independent coalescing write-notify brokers**: objects and
events — separate so an event burst never wakes object re-reads. The object broker is
additionally **resource-routed**: subscribers register keyless (kind-catalog watch) or keyed
to one `(apiVersion, resource)`; a keyed notify wakes matching-key plus keyless subscribers,
so an unrelated kind's write costs a per-kind objects watch nothing. The routing key is the
**plural resource, not the Kind**: it's the identity watches are opened on, and it's stable
across a CRD deleted and recreated with the same resource but a different Kind — the
subscription tracks the replacement's writes instead of going stale.

## Alternatives considered

**One database for all clusters.** Rejected: per-cluster deletion becomes row-sweeping
instead of file-unlink, corruption blast radius spans clusters, and the incarnation semantics
(a migration's clean hand-over) would need epoch columns everywhere.

**A file keyed by cluster alone (stable path across incarnations).** Rejected: a UID
migration reusing the path invites the old identity's writers racing the new one's; fresh
ObjectID-keyed paths make "never deleted under a live writer" enforceable by the finalizer
chain.

**Kind-keyed broker routing.** Rejected: a CRD remap keeps the resource but changes the Kind;
Kind-keyed subscriptions would silently go stale against the dead Kind.

**Polling readers.** Rejected: a hundred kinds × N windows polling SQLite versus a debounced
ping per actual write is no contest.

**CGO SQLite.** Rejected in favor of modernc: the sidecar cross-compiles for three platforms
from `build.rs`-adjacent tooling; pure Go keeps the toolchain trivial.

## Consequences

Deletion is file-unlink behind the finalizer barrier; a fresh incarnation cold-syncs (its
resume cookies died with the old file — `ClearCache` bounces the workers for exactly this
reason). Obligations: teardown uses `Manager.Lookup`, never `Open` (re-opening
mid-teardown re-materializes the file); writers ping the broker with the right key; new
tables join the embedded migration sequence, and app-level tables go in `internal/appdb`, not
here.
