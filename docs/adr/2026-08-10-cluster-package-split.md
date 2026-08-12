---
title: Split internal/cluster into boundary, controllers, and domain
date: 2026-08-10
scope: sidecar
status: Accepted
---

# Split internal/cluster into boundary, controllers, and domain

## Context

`internal/cluster` was one flat package of 25 production files: the `ClusterService`
boundary and its five record families, four beehive controllers plus their worker registry
and connection machinery, and 1100 lines of domain types. The boundary had just been
reorganized into families ([ADR: ClusterService sub-APIs](2026-08-10-cluster-service-sub-apis.md)),
which made the remaining problem plain — `internal/cluster` was the package everyone imports
for `ClusterService`, but most of what a reader opened it to find was reconcile loops.

Three measurements decided the shape. The coupling was already one-directional: no
controller referenced `*Service`. The test fixtures split cleanly — of the 19 shared helpers
in `testutil_test.go`, the controller tests and the boundary tests shared exactly one, a
seven-line channel wrapper. And `helpers.go`, split by consumer rather than moved whole, sent
13 identifiers to the controllers, 7 to the boundary, and only 2 to both.

## Decision

Three packages, importing strictly downward — boundary → controllers → domain:

- **`internal/cluster`** is the boundary alone: `ClusterService`, the five family accessors,
  `Service` with `New`/`Start`/`Close`, and the connection surface. 13 files.
- **`internal/cluster/controllers`** holds the four controllers, the worker registry, the
  connection sentinel and manager, the kubeconfig importer, and the per-cache client policy.
- **`internal/cluster/domain`** is a leaf: the four beehive kinds, identity, conditions, the
  delta-watch change types, and the cached-data records. Every type the GraphQL schema binds
  lives here, so `gqlgen.yml` binds 1:1 by name to `.../internal/cluster/domain` with no
  wrapper — the schema-is-the-domain-shape property is unchanged.

The placement rule: **types the schema binds and the identity/condition vocabulary go to
`domain`; unexported helpers live with their only consumer.** A dependency between two
helpers overrides it — `LiveCondition` caps its message via `TruncateMessage`, and
`ClusterStatusEqual` compares stamps via `TimePtrEqual`, so both callees follow their callers
into `domain` rather than being duplicated.

`controllerRuntime` and the four `beehive.Register` calls moved into
`controllers.Install`. Registration options now sit beside the controllers they configure,
and `Install` returns only the handles the boundary keeps — background lifecycle, the
in-memory gauges the `stats` resolvers read, and the worker restart behind a cache clear.

## Alternatives considered

**Extracting the boundary downward instead** (`internal/cluster/clustersvc`, root keeps
domain + controllers). Measurably cheaper: 14 forced exports against 27, and zero `gqlgen.yml`
changes. Rejected because it leaves `internal/cluster` meaning "domain plus controllers"
while the thing every caller imports lives in a sub-package — a smaller price for a worse
arrangement, and callers would import both anyway.

**Naming the domain package `core`.** Rejected: `ClusterCoreController` appears 38 times in
this subsystem, where "core" already means the controller for the `Cluster` kind as opposed
to its children. `core.Cluster` beside `controllers.ClusterCoreController` reads as though
the latter lived in the former. `kinds` was rejected too — a third of the package
(`Condition`, `RawJSON`, `ObjectID`, the `*Change` types) are not kinds. `api` collides with
`clientcmd/api`, which every controller imports.

**Moving `helpers.go` whole.** Rejected once measured: it would have forced 22 exports where
splitting by consumer forces 2.

**Splitting `Service`'s state to match.** Deferred. The families are an API shape; the control
plane behind them is genuinely one object.

## Consequences

Ten identifiers were exported to cross the new lines: `NewCacheRef`, `LiveCondition`,
`KubeconfigName`, `ClusterActiveUID`, `CacheIsActive`, `TimePtrEqual`, `TruncateMessage`,
`MaxMessageLen`, and the `Events*` kind constants. `ConfigureKubeHTTP2Keepalive` moved to
`controllers` — it tightens client-go's h2 health checks for the connection sentinel — so
`internal/app` now calls `controllers.ConfigureKubeHTTP2Keepalive`.

`graph` imports both packages: `cluster` for the service interfaces, `domain` for the types.

The boundary's test suite dropped from ~25s to ~2.7s, because the controller tests that stand
up a real beehive now run in their own package. Fixtures split with them; `recv`/`recvBy` are
duplicated in each package's `testutil_test.go`, as is the ctx-aware `send`.

White-box testing is unaffected — each package keeps `package foo` tests. What the split does
change is that the boundaries are now enforced by the compiler rather than by discipline: a
controller can no longer reach into the boundary, and `domain` can reach neither.

`streams.go` still has no test file of its own; it holds generic channel plumbing exercised
through the family tests.
