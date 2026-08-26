---
title: Mirror each kind with a standing worker, not a probe
date: 2026-08-26
scope: sidecar
status: Accepted
---

# Mirror each kind with a standing worker, not a probe

## Context

`clustersvc` already ran two `probe.Engine`s — the connection pool's five probes, and
`kubecatalog`'s discovery sweep. Both are periodic passes over subjects: a run asks the server
something, commits what it found, and is scheduled again by its own cadence and backoff ladder.

Mirroring a kind is not that. It is a LIST followed by a watch that stays open for hours, applying
deltas as they arrive. What it publishes — a verdict, a count, two freshness stamps — moves
continuously rather than once per pass.

The engine's contract also forbids the one thing a sync must do: a `Run` may not block, because it
holds an engine worker, and waiting for a connection to come back is precisely what a suspended
sync does.

## Decision

`internal/kubesync` runs its own goroutine per subject — one per (cache, GVR), keyed by the
`ClusterCachedResource` record's beehive name — with `Track`/`Forget`/`Read`/`Subscribe`,
`RestartAll` and `ForgetCache` around it. The loop (`sync.go`) is: resolve the connection
through `Lease.ConnFor`, cold-list if there is no cookie to resume from, then watch with
`AllowWatchBookmarks` until the stream ends. A position the server refuses is answered in place by
re-listing; every other failure goes up a backoff ladder.

It keeps kubecatalog's *exported shape* deliberately, so the controller folds both leaves the same
way, and it publishes the same conflated signal — but only when the reason moves, never on a count
tick or a timestamp, so a healthy sync does not requeue its record per delta.

Blocking in `kubeconn.AwaitConnFor` is legal here and only here: the worker is its own goroutine
holding nothing shared.

## Alternatives considered

**A probe per kind on the existing engine.** A run that returns cannot hold a watch open, so each
pass would have to re-LIST — the poll-only shape, at a hundred kinds per cluster. Holding the watch
inside a run instead pins an engine worker for hours, which is the same as not having a scheduler.

**One worker per cache, multiplexing every kind.** Kinds fail independently — one forbidden CRD
must not stall the other ninety-nine — and the per-kind `Synced` verdict is the whole reason
`ClusterCachedResource` is a record. A shared worker would have to reinvent per-kind state anyway.

**client-go informers.** They own their own store and their own resync, which is the part we are
replacing: the cache is on disk, is read by SQL, and outlives the process.

## Consequences

The fleet's concurrency is its own to bound. Cold LISTs go through a shared semaphore
(`defaultListBound`), so enabling a cache does not fire a hundred full lists at one API server;
standing watches are cheap and unbounded.

Nothing else restarts a worker whose params still hold, so a resume needs the explicit restart —
which is what the poke subscription on `clusterCachedResourceController` calls, and why `Track` is
deliberately a no-op for an unchanged subject.

Every cadence in the loop (page size, staleness threshold, backoff base and max, the events window
and its prune tick) is a parameter whose production value is a constant, so a test picks its own
timescale without encoding the production number.

## Revisit when

The probe engine grows a standing-stream subject type of its own — at which point the fleet is one
engine registration rather than a second goroutine pool.
