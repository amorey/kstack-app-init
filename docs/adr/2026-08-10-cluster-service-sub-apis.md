---
title: ClusterService as record-family sub-APIs
date: 2026-08-10
scope: sidecar
status: Accepted
---

# ClusterService as record-family sub-APIs

## Context

`ClusterService` is the one boundary between the GraphQL layer and everything the cluster
backend knows. It had grown to 29 methods on a single flat interface, and the names were
doing the work a type system should: `ClusterData*` ×4, `ClusterCacheGVRSync*` ×3,
`GVRSync*` ×3, `ClusterCache*Events*` ×2. Four separate methods meant "watch this thing's
event log" — `ClusterEventsWatch`, `ClusterCacheEventsWatch`,
`ClusterCacheGVRSyncEventsWatch`, `ClusterDataEventsWatch` — and told them apart only by a
prefix a reader has to parse carefully. Three meant "read this thing's live gauges."

The implementation had the same problem at file scale: `service.go` was 1938 lines with a
2280-line `service_test.go`, in a package whose convention is one test file beside each
unit. `Service`'s own fields already clustered along the same seams — the seven `syncHealth*`
fields, the three `data*Debounce` fields, the connection trio — so the groupings were
present in the state, just not in the API.

## Decision

Five record families hang off accessors on `ClusterService`: `Clusters()`, `Caches()`,
`Discovery()`, `Syncs()`, `Data()`, each its own exported interface. Methods are named
**VerbNoun, with the noun elided exactly when it equals the family's subject** — so
`Caches().Watch()` watches caches, `Caches().WatchEvents()` watches a cache's event log. The
accessor has already said the noun; repeating it is stutter. Because the elision rule is
mechanical, the repeated shapes now share one name across families: `WatchEvents` appears in
four, `GetStats` in three.

`RetryConnection` and `GetConnection` stay at the top level. They are the connection surface
— neither reads nor writes a beehive object, both go straight to `ConnectionManager` — so
they belong to no record family.

The accessors are **stateless views over the one `*Service`** (`clustersAPI{s}` and
siblings), not a split of the control plane. `Service` still owns every client, controller,
and the sync-health fold; a family method reaches them through `a.s`. Implementations move
to `service_<family>.go`, with shared plumbing in `service_streams.go` (the generic channel
pumps) and `service_events.go` (the kind-agnostic event reader), and the sync-health feature
— its public entry point and its fold — in `sync_health_fold.go`. Test files follow, and
fixtures used by more than one now live in `testutil_test.go`.

## Alternatives considered

**Leaving the flat interface and only splitting the files.** Rejected: it fixes the 1938-line
file but leaves the naming, which is the part a caller reads. The prefixes exist precisely
because the flat namespace has no other way to group.

**Embedding `*Service` in each accessor** (`type clustersAPI struct{ *Service }`). Tempting —
it makes the method bodies move without edits, since the receiver name stays `s`. Rejected:
promotion puts every other family's methods on every accessor, so `Clusters().GetConnection()`
would compile. A named field costs one mechanical rewrite and keeps each view's surface honest.

**Scoping the accessor instead of the method** (`svc.Cache(clusterID, cacheID).Stats()`).
Rejected: it reads tidier but introduces an error path at accessor time for ids that may not
resolve, and every method already takes its ids explicitly.

**Splitting `Service`'s state to match the families.** Deferred, not rejected. The fields do
cluster along these seams, but moving them is a second change with its own risks; doing it in
the same commit as 27 renamed call sites would make the diff unreviewable.

**A per-family transaction handle, as in beehive's own `storeapi`** (whose families partly
exist so `Within()` can hand out the same set bound to a transaction). Not applicable: there
is no transaction analogue here, so the families buy legibility and file decomposition only.
Worth knowing, so nobody looks for a `Within()` that was never the point.

## Consequences

Each family is asserted separately (`var _ Caches = cachesAPI{}`) in production **and** in the
resolver tests' fake — satisfying `ClusterService` now only proves the five accessors exist, so
a family missing a method would otherwise go uncaught until a call site broke.

Two pre-existing inconsistencies are now visible in the family tables, and are deliberately
left alone here: `Caches().GetStats` and everything in `Data()` take `(clusterID, cacheID)`
while `Caches().ListEvents` takes only `cacheID`; and `Caches().WatchSyncHealth` is fleet-wide
where the rest of its family is per-cache (one fold serves everyone — see
[ADR: status propagation & gauges](2026-08-09-status-propagation-gauges.md)). Normalizing the
first is a separate change; the second is intended and stated in the interface doc.

`service_streams.go` has no test file of its own: it holds generic channel plumbing exercised
through the family tests rather than directly. That is the one place this change leaves the
package's one-test-file-per-unit convention unmet, and it is a deliberate trade against
inventing tests for helpers whose contracts are already pinned by their callers.
