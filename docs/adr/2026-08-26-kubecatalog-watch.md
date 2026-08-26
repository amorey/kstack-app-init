---
title: Discovery is a sweep on its own engine, made prompt by a watch that only wakes
date: 2026-08-26
scope: sidecar
status: Accepted
---

# Discovery is a sweep on its own engine, made prompt by a watch that only wakes

## Context

`clusterCachedCatalogController.Reconcile` enumerated a cluster's served kinds inline —
`ServerPreferredResources`, dozens of round trips bounded by the discovery client's timeout, on a
beehive worker. It was the only pass left making a network call that way; `catalogConcurrency = 8`
was the acknowledged mitigation, not a design. Discovery was also pull-only, so a CRD installed on
a cluster waited up to 10m to appear.

## Decision

**Discovery is `internal/clustersvc/internal/kubecatalog`**, a second `probe.Engine` with one
probe, sibling to `kubeconn` and free to import it. Subjects are keyed by the catalog record's
beehive name and armed from its reconcile — `Track` while the record wants discovery, `Forget` on
pause and teardown — so a paused subtree costs zero sweeps by structure. The catalog pass now only
folds the standing answer; no reconcile dials.

**The watch only wakes; only `Run` commits.** Every run that resolves a connection stands a
watcher up over it, before sweeping: one stream each on `customresourcedefinitions` and
`apiservices`. A change wakes the sweep and the event is dropped — the sweep reads current state.
The watch's precondition is the connection rather than the answer, so it is reconciled on every
pass and never waits on a clean sweep to follow a connection that moved.
Pull-first is therefore structural: every watch failure mode costs promptness, never correctness,
with the probe's interval as the backstop under the watch and the kind's beehive resync behind the
fold.

**Streams run on a resourceVersion end to end.** A fresh one reads the collection's current
version and starts there, because a watch given none replays every existing object as a synthetic
`Added`; each remembers the version of the last event it saw and reopens from it;
`AllowWatchBookmarks` keeps a quiet stream resumable, and a bookmark never wakes. Only an end that
*proves* a gap wakes — the server refusing our version for being too old. Every other end is silent
and waits for the next sweep, because the sweep's answer to a dead watch is to stand another one
up, and waking on an end that repeats spins. Reopening is paced (`reopenDelay`), since nothing else
paces it and a proxy can hang up as fast as it accepts; the resume makes that wait cost latency
rather than events.

## Alternatives considered

**A sixth probe on kubeconn's engine.** Rejected: the wake path to the catalog kind is direct
instead of riding a `ClusterStatus` fingerprint plus a dependency edge, `kubeconn.State` stays
connection-scoped facts, and sweep workers never contend with the reachability probes.

**An engine-supervised watch capability**, or a standalone watcher supervisor. Both make the
engine or a third component own a lifecycle that belongs to the thing that established it. `Run`
maintaining its own watcher is the pattern `connectionProbe` already uses for the `Connection`,
and it needs no engine change.

**No continuation — treat every stream end as a gap.** This is what the spec deferred and the
implementation refused. Servers close watches routinely (~5m) and two streams end on their own
clocks, so a quiet timeout would cost a full sweep: 2-4x the pull-only baseline, on a desktop app
pointed at clusters it does not own, to learn nothing. Continuation is what makes the watch layer
cheaper than the poll it accelerates.

## Consequences

A CRD appears in seconds rather than minutes, and the steady-state cost is roughly the pull-only
baseline plus real change events — the watch itself adds two reopens per server timeout, and two
requests per stream at establishment.

Five obligations, each a place someone could break this without noticing:

- **A `Bookmark` must never wake.** Bookmarks arrive about every minute; waking on one would sweep
  on the bookmark cadence and cost more than not watching at all.
- **A stream must never open without a version**, or it replays: the server sends a synthetic
  `Added` for every object that already exists, every one of them reads as a change, and every
  establishment buys the redundant discovery pass this ADR claims to reject. The version read that
  prevents it is why a fresh stream costs two requests. **A test with a hand-driven stream cannot
  catch this** — a fake sends the events the test sends and no others — so the production opener
  needs its own coverage.
- **Only an end that says our version is too old is a gap.** Any other watch error is the server's
  own trouble, says nothing about what was missed, and repeats — so treating it as a gap loops
  through the sweep. The no-spin argument for the gap wake rests on the replacement starting from
  a version that has to age first, and holds for nothing else.
- **A watcher exists only for a tracked id**, and both halves of that hold under the one mutex:
  establishment checks `tracked` and stores in the same critical section, and `Forget` drops the
  subject and the watcher in the same one. A run is on a worker, so `Forget` can land under it —
  and the engine's commit refusal does not cover this, because establishment is not a commit.
  Either half split in two leaks a goroutine and two streams until the connection retires, which
  for a healthy cluster is indefinitely. The teardown half is the easier one to lose, because
  stopping a watcher waits for its streams: do it before the subject is gone and the window is as
  long as the API server takes to hang up.
- **The watcher owns its context, and every refusal stops it.** Streams opened on a pass's context
  die when the pass returns. And `conn.Done()` is not enough: a connection that goes *conflicted*
  is never retired, so a watcher left standing would go on waking a subject that can only suspend.
- **A standing watcher is only kept while it is live and over the sweep's own connection.**
  Nothing else re-establishes one, so both other states are terminal if establishment declines:
  a spent watcher has already woken this very sweep on its way out, and a watcher over a replaced
  connection holds an HTTP watch on retired credentials. Either one, read as "a watch already
  stands", drops that cluster back to the 10-minute poll for the process's life.

A resourceVersion never outlives its connection — it is one cluster's etcd revision, and the next
connection may be another cluster.

**The sweep waits for its watches.** A watch covers only from when it opened, so a sweep that read
discovery first would leave a window in which a change is in neither answer and surfaces at the
next poll. `ensureWatcher` therefore returns only once every stream has attempted its first open,
and the two reads overlap rather than abut. Establishing before the sweep is not enough on its own
— the opens are their own goroutines and can land after discovery returns.

The wait is bounded by the *run's* context and never by the streams', which live on the watcher's
own. So a server that never answers a watch cannot hold a worker past the run's budget, and a wait
cut short costs that pass the overlap and nothing else. An open that fails releases the wait like
any other: a refused watch has no promptness to offer, and a sweep held behind a handshake that
will never complete is worse than a blind one.

## Revisit when

A watcher refused by RBAC needs to be visible. It reports `Succeeded()` today, since the catalog
data is fine and only promptness degrades — so a user who may never watch CRDs sees a healthy
catalog that is quietly up to 10m stale. `TODO.md` carries it.
