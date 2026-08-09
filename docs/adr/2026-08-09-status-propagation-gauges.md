---
title: Status is a propagation channel; gauges ride their own streams
date: 2026-08-09
scope: sidecar
status: Accepted
---

# Status is a propagation channel; gauges ride their own streams

## Context

A beehive status write bumps `resource_version`, which wakes every dependent and pushes a
frame to every watcher. The cluster subsystem has many high-frequency measurements — worker
freshness stamps, cache sizes, discovery gauges, probe outcomes, backoff countdowns — and a
cache has a hundred-plus sync children reporting on ~30s cadences. Putting any of that in
status would make the steady state a permanent wake-storm: each child heartbeat re-emitting
its cache to every watcher, every pass rewriting numbers no controller reads.

## Decision

**Status is for state a dependent must react to.** Measurements only a UI reads live in
controller memory, served on request (`GVRSyncStats`/`GVRDiscoveryStats`/`CacheStats`,
resolved onto records as `stats` fields). `ClusterCacheStatus`,
`ClusterCacheGVRDiscoveryStatus`, and `ClusterCacheGVRSyncStatus` are **empty structs**; what
stays on each object is its condition. A settled child therefore writes nothing at all
(`TestGVRSyncFoldsWorkerReports` / `TestGVRDiscoverySteadyPassWritesNothing` pin the
resource_version).

The corollary: **a gauge needs a stream keyed to what changes it, never a field on a record
whose write cadence is unrelated.** A resolver field is only as fresh as its object's watch
frames, and a settled object emits none — a cache's contents read at subscribe time would
freeze forever. So live measurements ride dedicated streams: `clusterCacheStatsWatch`
(cache contents/size, over the store's write broker, deduped), `clusterScheduleWatch`
(next-attempt countdown from beehive's `WatchSchedule` gauge, merged with the core
controller's in-flight `Probing` flag), and the probe/sync histories via the generic events
surface (`clusterEvents`/`clusterCacheEvents`/`clusterCacheGVRSyncEvents` + `…Watch`), which
rides beehive's coalescing event log — so a repeated failure bumps a run's count without
rewriting status, keeping per-probe chatter off `clustersWatch` entirely.

The one rollup a UI needs — the whole-cache sync verdict — is a **read-side projection**
(`WatchCacheSyncHealth`), not a stored condition: one process-wide fold over the per-kind
records, published as a whole-map latest-value hub. Whole-map, not deltas, deliberately: the
output is tiny (one verdict per cache), a new subscriber's first read is the complete "every
cache, right now" a window needs, and a slow subscriber coalesces onto the newest map —
correct for a gauge, where a dropped delta would be lost state. A cache whose discovery
anchor is gone is dropped from the map (the cache was collected), and the fold recomputes on
frames **and** a 10s tick, because the freshness stamps it folds live in controller memory
and move with no frame behind them.

## Alternatives considered

**Stats in status.** Rejected: the wake-storm above. This was learned concretely — cache
contents were originally a `ClusterCache.stats` read and froze at subscribe time.

**Storing the sync-health verdict on the cache.** Rejected: nothing in the object graph acts
on it, and writing it would wake the cache and every watcher each time any of a hundred-plus
children changed verdict.

**Per-subscriber folds.** Rejected: every window computes the identical verdict; per
subscriber that's two more beehive watches, another ticker, and a copy of every per-kind
record. The shared fold costs each subscriber a channel and a small map.

**A "last N probe outcomes" array in status.** Rejected in favor of the event log: beehive
coalesces same-outcome runs (a stuck-down cluster is one high-count run) and bounds retention
(`WithEventRetention`), and the history is fetched per cluster only while its diagnostics are
open.

## Consequences

Steady-state quietness is an invariant tests pin — do not add status fields (or condition
rewrites) to hot paths without checking what they wake. Every new measurement must pick:
condition (dependents act on it), stats-on-request (UI reads it rarely), or a keyed stream
(UI watches it live). The fold's subscribers get gauge semantics, so consumers must upsert by
key and derive deletion from the owning lifecycle stream, not wait for a delete frame.
