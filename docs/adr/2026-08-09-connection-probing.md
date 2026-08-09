---
title: Probe connections per-cluster in parallel, with a sentinel watch and backoff-neutral retries
date: 2026-08-09
scope: sidecar
status: Accepted
---

# Probe connections per-cluster in parallel, with a sentinel watch and backoff-neutral retries

## Context

`ClusterCoreController` owns connection/health probing. Probes are slow network calls against
clusters that may be unreachable; at startup, beehive's full pass and the kubeconfig
watcher's first snapshot enqueue every cluster at once. Users need fast connection-loss
detection, an honest "retry now" button, and probe failures that don't spam the status watch
(see [ADR: status propagation](2026-08-09-status-propagation-gauges.md) for where the history
and countdown live instead).

## Decision

**Reconciles of one cluster serialize on that cluster's own lock and re-read fresh under
it** — beehive status writes carry no resourceVersion guard, so a stale out-of-band snapshot
could otherwise clobber observations. The lock is per-cluster, not shared: distinct clusters
probe in parallel (bounded by `clusterProbeConcurrency` = 8 in both beehive's worker count
and `reconcileAll`'s pool), so one unreachable cluster's dial timeout can't delay the rest.
The lock map is never reclaimed — bounded by contexts ever seen (tens), and freeing a lock
another goroutine is about to take is the bug that avoids.

**Connection loss is detected by a sentinel watch, independent of sync.** After a successful
probe, `converge` opens one long-lived watch on the cluster's kube-system namespace, keyed by
the connection-config fingerprint (credential rotation restarts it). The controller reacts
only to the stream *closing* — the earliest loss signal — by firing one `Reprobe` and
exiting; a successful re-probe opens a fresh sentinel. A persistently-down cluster runs no
sentinel (only beehive's backoff), so it never spins. This makes loss detection a property of
the connection controller, which is why there is no sync → connection hook.
`ConfigureKubeHTTP2Keepalive()` (called once at startup) tightens client-go's h2 health check
from 30s/15s to 10s/5s so a silently-dropped connection closes the sentinel in ~15s, not ~45s.

**Out-of-band reprobes are backoff-neutral.** `RetryConnection` and the sentinel both feed an
in-process retry bus that re-runs `Reconcile` off beehive's worker: a failed "check once
more" neither resets the backoff ladder to the fast ramp nor advances the schedule — only a
successful probe clears backoff. Scheduled failures return an error so beehive applies its
exponential backoff (1s ×2, capped 2m); success returns the ~30s health cadence.

Cache management is probe-gated: only a confirmed kube-system UID ensures that identity's
`ClusterCache` and prunes superseded ones — a transient disconnect (`UID == nil`) never
prunes, so a flap can't destroy a cache.

## Alternatives considered

**One shared reconcile mutex.** Rejected: at startup a single unreachable cluster's timeout
would serialize behind it every other cluster's first probe.

**Optimistic-concurrency retries instead of locks.** Beehive status writes offer no
resourceVersion guard to build on; per-cluster locking with a fresh re-read is the available
primitive.

**Detecting loss via sync workers' errors.** Rejected: loss detection must work for clusters
with sync disabled, and a hundred workers reporting the same dead connection is noise, not a
signal.

**Polling `/readyz` faster instead of a sentinel.** Rejected: a watch's close is
event-driven and earlier than any affordable poll cadence, and costs nothing while healthy.

**Routing manual retries through spec writes.** Rejected: a spec counter persists and
re-fires; a retry is ephemeral, and riding beehive's scheduled path would let manual clicks
disturb the automatic backoff cadence.

## Consequences

Startup probes fan out in parallel; a wedged cluster degrades only itself. Obligations: keep
out-of-band paths off beehive's scheduled backoff; keep sentinel teardown on the ineligible
path (`stopSentinel`); keep cache create/prune gated on a confirmed UID and non-fatal to the
probe's status write.
