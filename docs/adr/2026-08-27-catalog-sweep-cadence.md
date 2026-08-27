---
title: The catalog sweep's cadence follows the watch it stands over
date: 2026-08-27
scope: sidecar
status: Accepted
---

# The catalog sweep's cadence follows the watch it stands over

## Context

Discovery is a sweep made prompt by a watch that only wakes, with the sweep's interval as the
pull-first backstop under it (→ [ADR](2026-08-26-kubecatalog-watch.md)). That interval was 10
minutes for every cluster, whatever the watch under it was doing. On a healthy cluster the
backstop is pure cost: a full `ServerPreferredResources` is dozens of round trips per
group-version, per cache, six times an hour, to learn what the watch would have said in seconds.

Deleting the poll is not available, and for structural reasons rather than caution. `ensureWatcher`
is only ever called from `catalogProbe.Run`, and every non-gap stream end is deliberately silent —
so with no tick, a stream that dies from any non-gap error is never re-established, and the only
thing that would stand a new one up is the sweep the dead watch was supposed to wake. A watch RBAC
refuses would likewise freeze discovery at whatever the first sweep found.

## Decision

**The backstop's length is a function of whether the watch is working.** `sweepInterval` — the
registered interval — is 30 minutes. A run whose watch is not live returns
`Succeeded().RequeueAfter(sweepIntervalDegraded)`, 10 minutes, which is where every cluster sat
before. 30 minutes is chosen against the one thing the backstop still bounds: how long a watch
that dies *silently, between runs* can hide.

**`Result.RequeueAfter(d)` can only bring a run forward.** The engine takes it when it is positive
and shorter than the registered interval, and reads it on a succeeded result alone — `Fail` owns
the backoff ladder, `Suspend` schedules nothing. That clamp is what keeps the registration the
cadence everything rests on: a return path cannot push a subject past its registered bound however
wrong the value it hands back, so forgetting the ask makes a cluster slower and never wrong. It is
stored on the `Attempt` rather than applied at the commit, because the engine re-derives every
schedule on every pass from recorded state.

**It is spelled as beehive spells it**, deliberately, since a reader moving between the two
schedulers meets the same ask twice. The contracts differ in one place worth knowing: beehive's
`RequeueAfter` overrides the schedule outright, and probe's is clamped. A probe's registration
bounds requests made against someone else's cluster, which is not what a beehive resync interval
is, so the two answer "may a return path lengthen this?" differently. A zero also differs — no ask
here, where beehive lets its queue floor pace it.

**The unset value is the interval, not zero.** Every other probe returns a bare `Succeeded()`, so
a literal `min` would schedule all of them at `FinishedAt` and spin.

**The ladder caps at `sweepIntervalDegraded`.** The registration read
`WithBackoff(sweepRetryBase, 2, sweepInterval)`, so lengthening the backstop would have tripled
the failure ladder's cap by side effect. The ladder is the degraded path by construction — it runs
only for a sweep that failed or came back partial — and no watch covers either case: a CRD stream
says nothing about an aggregated `APIService` that stopped answering.

**A refused open marks the watcher spent before it reports the open.** `ensureWatcher` now returns
liveness, and a run reads it the moment `awaitOpen` returns. `stream` called `firstOpens.Done()`
before `end()`, and `Done()` is what releases `awaitOpen` — nothing ordered the two, because
nothing read `spent()` at that instant. Reversing them makes the report a fact rather than a race.

**`Catalog` carries `WatchLive`, and the fold names it.** `converge` reports
`Discovered=True` with a message naming the degraded watch. The reason stays `Discovered`: the
kinds are right, and what is degraded is how fast a new one shows up. `WatchLive` is in the commit
guard, so the standing answer keeps it current, and in `news`, so the flip signals the fold. It
stays **out of `Fingerprint`**, which covers the kinds alone — the rows on disk are matched
against that word (→ [ADR](2026-08-27-catalog-kinds-off-disk.md)), and a watch flip is not a table
that moved.

## Alternatives considered

**Deleting the poll.** See the context: two independent paths leave discovery frozen for the
process's life.

**Keeping `WatchLive` out of `news`**, on the grounds that promptness is timing and timing does not
wake the fold — the split `kubeconn` makes. It loses because the cost it avoids is not there:
`WatchLive` is recomputed only inside `Run`, so it moves at most once per sweep, and each wake
costs a fold pass that rebuilds nothing. What it would buy is a stale message — the fold's own
backstop is `catalogResyncInterval`, so a recovered watch would read as degraded for ten more
minutes. The `kubeconn` analogy does not carry: what that split excludes is attempt bookkeeping
that churns on every pass.

**A reason of its own for a degraded watch** — `DiscoveryDegraded` beside the others. It reads as
a broken catalog in every surface that keys on the reason, for a cluster whose kinds are correct.
The message is the right resolution: a user who wants to know can read it, and nothing downstream
treats it as a failure.

**Leaving the ladder capped at `sweepInterval`.** It is one word of diff, and it triples the retry
of exactly the failures the watch cannot cover.

## Consequences

A healthy cluster is swept twice an hour instead of six times. A cluster whose watch cannot open —
RBAC on a cluster-scoped collection is the common case — is unchanged at 10 minutes, and now says
so in its condition.

**A watch that dies silently between runs can hide for up to 30 minutes** rather than 10. That is
the window the backstop exists to bound, and it is the price of the change: a longer interval buys
little and lengthens it proportionally.

Liveness is read where the watch is reconciled — before the sweep, which `sweepTimeout` bounds at
five minutes — so a watch that dies *during* a long sweep pays one more run at the backstop before
the cadence follows it. The next run's own reconcile catches it, and re-reading the watcher after
the sweep would buy only that one run.

## Revisit when

A second probe wants `RequeueAfter`. It is general — nothing in the engine knows what a catalog
is — but it has one caller, and a second one is where the clamp's edges get tested.
