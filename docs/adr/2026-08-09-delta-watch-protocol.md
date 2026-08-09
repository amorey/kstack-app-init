---
title: Stream each kind as its own Kubernetes-style delta watch, joined client-side
date: 2026-08-09
scope: cross-cutting
status: Accepted
---

# Stream each kind as its own Kubernetes-style delta watch, joined client-side

## Context

The webview renders a live picture of the cluster control plane: Clusters, their
ClusterCaches, the caches' per-GVR discovery and sync children, plus folded sync verdicts.
These kinds form an owner chain in beehive, but they write at wildly different rates — a
per-kind sync child rewrites far more often than its cache changes, and a cache more often
than its cluster. The frontend needs current state on subscribe plus every change after.

## Decision

Each kind streams **independently** as a Kubernetes-style delta watch: an `Added` snapshot
burst, then per-object `Added`/`Modified`/`Deleted` (`clustersWatch`, `clusterCachesWatch`,
`clusterCacheGVRDiscoveriesWatch`, `clusterCacheSyncHealthWatch`, and the cache-scoped
`clusterCacheGVRSyncsWatch`). The sidecar folds beehive's per-kind `WatchList` into this shape
in `service.go`'s `watchListChan` pump; subscription resolvers emit the current snapshot
first, then deltas (`mapStream` in `graph/util.go` for hub-backed sources).

The webview keys each stream into an id-keyed map and **joins client-side, down the chain**:
caches onto clusters by `clusterID`, verdicts and discovery records onto their cache by
`cacheID` (`src/lib/clusters.tsx`). Derivations that depend on two kinds — notably which cache
is *active* (its `serverUid` matching the cluster's `status.server.uid`) — are the client's,
never a stored field.

High-volume streams are scoped so the always-mounted registry never carries them: the
per-GVR sync watch is cache-scoped (~100 records per cache), subscribed only while a sync
panel row is open.

## Alternatives considered

**Folding children onto their parent** (a `Cluster` with embedded `caches`, or a cache with
embedded per-kind syncs). Rejected: the lower kinds write far more often, so folding upward
re-emits the parent at the child's cadence — a sync child's 30s heartbeat would re-send every
cluster row to every window. This is the load-bearing reason for the shape.

**An "active" flag on the cache record.** Rejected: the parent's probed UID changes with no
cache event, so a per-cache stream can never keep such a flag fresh. Only the client's live
join against the current cluster status is correct.

**Query + poll instead of watches.** Rejected: the data is exactly what Kubernetes-style
watches were designed for, and polling either lags or hammers.

**Single merged stream of typed union frames.** Rejected: consumers would demultiplex by kind
anyway, and per-kind subscriptions let urql dedupe one shared operation per interested
component and let the sync-panel streams mount lazily.

## Consequences

The frontend owns the joins, which means reducers must guard **provenance**: a re-subscribe
after a cache swap can replay a superseded cache's retained rows, so cache-keyed streams carry
`cacheID` (objects also `apiVersion`/`resource`) on every frame, and the shared
`useCacheDeltaWatch` (`src/lib/graphql/use-cache-delta-watch.ts`) rejects frames not from the
active key. Do not "simplify" that guard away — it is what keeps a stale cache's rows from
surviving a context switch. Adding a kind means adding a stream, not a field on a parent.
