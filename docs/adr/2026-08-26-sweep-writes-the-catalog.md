---
title: The sweep writes kind_catalog, ungated by the change guard
date: 2026-08-26
scope: sidecar
status: Accepted
---

# The sweep writes `kind_catalog`, ungated by the change guard

## Context

`kind_catalog` had no writer. The store's `Objects` read resolves a plural to its Kind through
that table, so every read keyed by plural answered empty however healthy the sync beneath it was —
structurally complete and silently wrong, which is hard to tell from broken.

Two things could write it. The **sweep** produces the data and is one leaf away from the store, the
way a `kubesync` worker is. The **catalog fold** already receives the sweep's answer, and beehive
already schedules it.

## Decision

**One writer per table, in the leaf that produced the data.** `kubecatalog.New(conns, stores)`
takes the store manager beside the connection pool, exactly as `kubesync.New` does, and a run
claims the cache's store for the write alone. `Track` grows a `Params` carrying the cache id,
because the subject id embeds the *catalog's* object id and the store is named by the cache.

**The write is not gated on the answer changing.** Every sweep that produced an answer upserts the
rows — pruning only when the answer is not `Partial`, the same add-without-pruning rule the
children follow — and the commit guard goes on governing only the *signal*.

**The write comes before the commit.** A commit is the fold's wake; one over rows that are not
there yet would have the fold converge on a table the write is still catching up to. A failed write
therefore commits nothing and signals nobody.

Two consequences follow. `Store.ClearKind` stops deleting the catalog row: emptying one kind's
cache does not stop the cluster serving it, and rows leave the table through the sweep's prune
alone. And the probe registers `WithBackoff(30s, 2, sweepInterval)` rather than the engine's
default second, because what a failure retries here is a full `ServerPreferredResources` — dozens
of round trips over every group-version, paid for at someone else's cluster.

## Consequences

**A wiped table repairs itself with no protocol.** `Manager.Clear` empties the rows; the next sweep
writes them back, because it does not ask whether they changed. `Caches().Clear` calls `Wake` so
that takes seconds rather than an interval — from its **deferred** requeue path, after the clear,
since a wake ahead of it would have the sweep write the rows the clear then deletes.

**The cost is a hundred upserts per cache per interval**, in one transaction, against a writer pool
capped at one connection. The sweep already holds its engine worker across a network call bounded
by five minutes, so a local SQLite write is far smaller than what the run already costs. If it ever
shows, the answer is `probe.WithWorkers` on this engine, not moving the write back across the
boundary.

**A new failure mode reaches the record**: `ReasonStoreFailed`/`ReasonStoreRemoved` fold to the
`Discovered` condition's `StoreUnavailable`. Discovery answered and the mirror would not take it,
which is neither a wait nor a discovery failure — pointing a user at the API server would be the
wrong remedy.

## Alternatives considered

**The fold writes.** It would have to carry the kinds across the probe engine and then
re-establish, from the fold, whether the table on disk is still the answer the sweep gave it — a
compact/invalidate/force-flag protocol whose only job is to work around the write being gated
behind a change signal. Every part of that apparatus dissolves once the write moves into the sweep
and stops being gated.

**Gating the write on the commit guard.** Cheaper per sweep, and wrong: `publish` fires on `news`
moving, and `news` is a projection of the committed value, so a wipe — which leaves the cluster's
answer exactly as it was — produces no commit and no signal however the sweep is written. Nothing
would notice the table was empty until the answer happened to change.

## Follow-ups

- The fold builds its children from these rows, joined by a fingerprint recorded in the same
  transaction, and detects a wiped table itself — → [ADR](2026-08-27-catalog-kinds-off-disk.md).
- `is_crd` is filled best-effort from the cluster's CustomResourceDefinition list, matched on group
  and plural. A refused list — a cluster-scoped read RBAC commonly denies — leaves every kind
  reading as built-in rather than failing the sweep.
