# Specs

A spec describes **what we are about to build**, in enough detail to implement it and no more.
It is a plan with a shelf life.

The other two doc kinds keep their jobs: `CLAUDE.md` says what is true now, `docs/adr/` says why a
design was chosen. A spec says what comes next.

## Filenames

```
docs/specs/<n>-short-slug.md   a spec with a place in a sequence
docs/specs/short-slug.md       one that stands alone
```

No dates — a spec is edited as the work moves, not appended to. **The number is the build order**,
so the directory listing is the plan; a spec that depends on nothing and blocks nothing goes
without one. Renumber when the plan changes, and fix the links in the same edit — the numbers are
load-bearing only as far as they agree with each spec's own header.

## Lifecycle

Update the spec while the work is in progress. When it lands, fold what is now true into the
relevant `CLAUDE.md`, write an ADR if a decision needs its reasons recorded, and delete the spec.
A spec left behind after the code ships is a second source of truth.

## Index

Each spec is self-contained: read one and you can build it. Where an order is given, it is a real
dependency, and each spec's header states what it needs and what it hands to the next.

**The cache read path has landed.** The sweep writes `kind_catalog`
(→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)) and the `CachedData()` family reads it back
(→ [ADR](../adr/2026-08-26-cached-data-read-loop.md)), so the nav and the tables are live. So has
the fold's read of those rows (→ [ADR](../adr/2026-08-27-catalog-kinds-off-disk.md)). What is left
is the cleanup behind it, which blocks nothing. The numbers are the build order; 1 through 3 are
gone because they shipped.

| # | Spec | Scope | Status | Delivers |
| --- | --- | --- | --- | --- |
| 4 | [Catalog sweep cadence](4-catalog-sweep-cadence.md) | sidecar | Planned | the poll under the watch becomes rare (no user-visible change) |

**Independent.**

| Spec | Scope | Status |
| --- | --- | --- |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
