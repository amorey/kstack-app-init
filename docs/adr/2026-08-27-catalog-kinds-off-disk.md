---
title: The catalog fold reads its kinds off disk, joined by fingerprint
date: 2026-08-27
scope: sidecar
status: Accepted
---

# The catalog fold reads its kinds off disk, joined by fingerprint

## Context

Every served kind was held twice. `kubecatalog`'s observable carried the whole `Catalog` per
subject — group-version, kind, resource, scope — and the `kind_catalog` rows the sweep writes
(→ [ADR](2026-08-26-sweep-writes-the-catalog.md)) held the same list again. Order of 90 bytes a
kind, so tens of KB for a cluster with CRDs: listed for the duplication, not the size. The trigger
would have been a second consumer reaching for the in-memory copy because it was there.

Two things held it resident, and a fix had to answer both. `pass.Prev()` is the sweep's commit
guard — committing only on a change is the whole of what stops a ten-minute pass waking the fold on
a cluster that moved nothing. And `clusterCachedCatalogController.Reconcile` read the standing
answer back through `Read(id)` to rewrite its `ClusterCachedResource` children.

## Decision

**The observable is `Catalog{Fingerprint, Partial}`.** The kinds are resident for the length of one
`Run` — swept, written, fingerprinted, dropped — and the commit guard is a `!=` over a comparable
struct. `Fingerprint` covers every field a consumer reads, the CRD bit included.

**The fold reads the rows back, and the fingerprint is what makes them trustworthy.**
`SyncKinds(ctx, rows, prune, fingerprint)` records the sweep's fingerprint in `cluster_meta` under
`kinds/fingerprint`, **in the rows' own transaction**; `Store.KindsWithFingerprint` reads the pair
back **out of one read transaction**; and `converge` rebuilds its children from those rows when the
stored fingerprint matches the observation's.

What the fingerprint identifies is *the sweep's last answer, not the table's contents*. A `Partial`
sweep upserts without pruning, so the table can legitimately hold rows the fingerprint does not
cover. That is exactly what the fold asks it — did the sweep write this table, or did something
wipe it — and the children are safe either way, since a partial answer does not prune.

**A wiped table is self-healing, detected in the fold**, and it is distinguished from the two
things that are not wipes. The fork is three-way:

- **No sweep has written the table** — no fingerprint recorded, no file, or a claim answering
  `ErrClosed`. The children are left alone, the verdict is `Discovered=False`/`StoreUnavailable`,
  the sweep is woken, and the pass requeues at `catalogRetryInterval`. The wake is what repairs it
  and the requeue is why it converges anyway: a wipe leaves the cluster's answer unmoved, so the
  rewrite commits nothing and signals nobody.
- **A fingerprint that is recorded but not the observation's** — the store is *ahead*. The sweep
  writes its rows and commits immediately after, so a fold reading between the two sees exactly
  this; the commit is what wakes the fold, so the pass settles silently, rebuilding nothing and
  waking nobody. Waking here would buy a full `ServerPreferredResources` at the cluster's expense
  every time an ordinary sweep raced a pass.
- **Any open or read failure** — report `StoreUnavailable` and **settle**. The sweep's own mirror
  refuses the same store, so its run fails, its reason moves, and its signal re-runs the fold; its
  ladder is the retry. `ErrRemoved` lands here too, though a cache being torn down normally stops
  at `ownersOf` long before: what reaches this arm is a file that is present and unusable.

**A wiper requeues its own subtree and knows nothing of the sweeper.** `Caches().Clear` requeues
the catalog record beside every kind under it; the catalog's pass is what notices the empty table
and asks for the sweep.

## Alternatives considered

**Keeping the kinds in memory.** The status quo, and it costs nothing until a second consumer
appears. It loses on the duplication alone — the rows are the same list, and one of the two copies
has to be the answer.

**Diffing against the children instead of the rows** (what `TODO.md` weighed). The fold already
lists its children, so the kinds could have been recovered from their specs with no store read at
all. It loses on what the children are: a projection the fold itself writes, pruned only on a
complete answer and holding tombstones for a kind that is draining. Reconstructing "what the sweep
found" from them means reasoning about the prune and tombstone rules backwards, and a `Partial`
answer makes that reconstruction wrong. The rows are the sweep's own output, and the fingerprint
says whether they still are.

**Reading the rows and the fingerprint separately.** One extra query on the same claim, and a
defect that deletes every child: a clear empties the file, the fold reads no rows, the sweep
rewrites them under the fingerprint it stored before — the cluster's answer did not move — and the
fold then reads *that* fingerprint, finds it equal to the observation's, and prunes off an empty
table. One read transaction is the whole guard.

**Leaving the wake in `Caches().Clear` and skipping detection.** Prompt, and it makes every future
wiper responsible for remembering that a sweeper exists — which was already one forgotten call away
from a nav that stays empty for a sweep interval.

**Dropping the `Clear` caller entirely, relying on detection alone.** The other extreme, and it
regresses what a user sees: `kind_catalog` is what `Store.Kinds` serves, so the dashboard nav reads
it directly, and nothing would run the fold after a clear until `catalogResyncInterval` — ten
minutes of workers cold-listing their rows back while the nav sits empty. Requeueing the record
keeps the promptness and still leaves the sweeper out of the wiper's vocabulary.

## Consequences

The steady-state cost per subject is a fingerprint, a bool, and the attempt bookkeeping. The fold
pays a claim and one query per pass, against a pass that already lists and rewrites its children.

**`clusterCacheClear` now leaves the catalog `Discovered=False`/`StoreUnavailable` for up to
`catalogRetryInterval`**, where before it stayed `True` throughout. The clear empties the table, the
fold reports what it found and wakes the sweeper, and the rewrite is silent — the cluster's answer
did not move — so the requeue is what converges it. Accepted rather than papered over: for those
seconds the catalog genuinely cannot confirm what the cluster serves, and a user who just pressed
Clear is watching the cache rebuild. The children never move, so nothing downstream stops.

Two obligations come with it. The fingerprint must stay inside `SyncKinds`' transaction, and the
rows and the fingerprint must stay inside one read — either split reintroduces the pruning defect
above. And a new wiper of a cache's file inherits the recovery for free only as long as it requeues
the cache's subtree; one that empties the store without requeueing anything leaves the nav waiting
for the resync.

`kubecatalog.Fingerprint` is exported because the pairing between an observation and the rows on
disk is a contract across the two packages: a test that fabricates one side has to hash the other
the same way.

## Revisit when

A consumer needs the kind list somewhere the store cannot be claimed — inside a probe `Run`, or off
a cache whose file is legitimately absent. The answer then is a second reader of the rows, not a
second resident copy.
