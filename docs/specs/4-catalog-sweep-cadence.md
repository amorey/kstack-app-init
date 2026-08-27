---
title: Catalog sweep cadence follows the watch
scope: sidecar
status: Planned
---

# Catalog sweep cadence follows the watch

> **Build order — 4.** No prerequisites. It touches `Catalog`, now a fingerprint and a flag
> (→ [ADR](../adr/2026-08-27-catalog-kinds-off-disk.md)).
> **Nothing a user sees changes.** It removes redundant load on the clusters we point at.

## Goal

Stop sweeping discovery every ten minutes on clusters whose watch is working. `kubecatalog`
already runs a watch that makes discovery prompt — one stream each on `customresourcedefinitions`
and `apiservices` (→ [ADR](../adr/2026-08-26-kubecatalog-watch.md)) — and the 10-minute
`sweepInterval` is the pull-first backstop under it. On a healthy cluster that backstop is pure
cost: dozens of round trips per group-version, per cache, six times an hour, to learn what the
watch would have said in seconds.

**This is not what makes the catalog live** — the watch already does, and the sweep carries its
wake through to the table (→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)). This spec is the cost side of the same design: with the watch doing the
work, the poll under it can be rare.

**The cadence becomes a function of whether the watch is working.** Watch live → a long backstop.
Watch refused, dead, or never opened → today's 10 minutes.

**Pull-first is untouched, and the direction of failure is what keeps it that way.** The
registered interval stays the correctness bound; a run may only ask to come back *sooner*. Forget
the ask and a cluster degrades to the backstop — slower, never wrong.

**Why not delete the poll.** Two things break, both structural. `ensureWatcher` is only ever
called from `catalogProbe.Run`, and the ADR deliberately makes every non-gap stream end silent, so
with no tick a stream that dies from any non-gap error is never re-established: the only thing that
would stand a new one up is the sweep the dead watch was supposed to wake. And a watch RBAC
refuses — a cluster-scoped read many clusters deny — would leave discovery frozen at whatever the
first sweep found. The poll is what covers both; making it rare is the win available.

## probe changes

- **`Result.DueIn(d)`**, on `Succeeded` alone. `Fail` owns the backoff ladder and `Suspend`
  schedules nothing, so neither takes one. It is the shape beehive already has one layer up —
  `.RequeueAfter(d)` on a settled result, "for a wait this pass knows the length of".
- **It may only bring the run forward: the engine takes `min(interval, d)`.** That is what keeps
  `sidecar/CLAUDE.md`'s rule intact — a cadence a probe depends on belongs at registration, where
  no return path can forget it. Under this clamp a return path *cannot* push a subject past its
  registered bound, however wrong the value it hands back, so the registration stays the
  correctness statement and `DueIn` is only ever an acceleration.
- A test pins the clamp: `DueIn` longer than the interval schedules at the interval.

## kubecatalog changes

- **Two constants.** `sweepInterval` — the registered backstop — moves to 30 minutes, and
  `sweepIntervalDegraded` (10 minutes) is what a run returns when its watch is not live. 30
  minutes is chosen against the one thing the backstop still bounds: how long a watch that dies
  *silently, between runs* can hide. A longer value buys little and lengthens that window
  proportionally.
- **`watch` returns whether the watch it left standing is live.** Nothing new has to be
  measured: a stream whose open is refused ends immediately (`watcher.go`'s `stream`, the `err !=
  nil` arm), so `w.spent()` is already true by the time `ensureWatcher` returns. The seam becomes
  `watch(ctx, id, conn) bool` and `ensureWatcher` reports `!standing.spent()`.
- **`Run` schedules on it**: `probe.Succeeded()` when live, `probe.Succeeded().DueIn(
  sweepIntervalDegraded)` when not. A watch that is flapping therefore holds the cluster at 10
  minutes rather than drifting to 30, which is the behaviour a degraded watch should have.
- **`Catalog` gains `WatchLive`**, and it is what makes the ADR's "Revisit when" answerable —
  today a user whose CRD watch is refused sees a healthy catalog that is quietly stale. Two rules
  keep it from costing anything:
  - **In the commit guard**, so the standing answer keeps it current.
  - **Out of `news`**, so a flapping watch never wakes the fold. It is a fact about promptness,
    not about what the cluster serves, and the fold picks it up on its next pass either way. This
    is the same split as `kubeconn`'s "only the values, never the timing".

## The fold

One line: `clusterCachedCatalogController.converge` reports `Discovered=True` with a message
naming the degraded watch when `obs.Value.WatchLive` is false. **The reason stays
`ReasonDiscovered`** — the catalog is correct and the kinds are right; what is degraded is how
fast a new CRD shows up. A separate reason would put a healthy cluster into a false state, and the
condition is what the user reads.

## What spec 3 already did for this

Its fold repairs a wiped table by **waking the sweep** and keeping a 30-second requeue as the
backstop, rather than polling until the sweep happens to come due. That was written knowing this
spec was coming: at a 30-minute backstop, a requeue-only fold would poll sixty times waiting for a
sweep that is not due. So there is nothing left to change there — this spec lengthens the backstop,
and the wipe path is already paced against a wake rather than against the interval.

**If spec 3 is skipped**, the interaction is simpler still: the fold reads its kinds from memory,
so a wiped table costs it nothing, and the `Caches().Clear` wake is what puts the rows back.

## Order of work (red/green)

1. `probe`: `Result.DueIn`, the `min` clamp, and the two tests (accelerates; cannot lengthen).
2. `kubecatalog`: the two constants; the `watch` seam returning liveness; `Run` scheduling on it;
   `WatchLive` on `Catalog`, in the commit guard and out of `news`. Tests pin: a refused watch
   schedules the short interval, a live one the backstop, and a `WatchLive` flip commits without
   signalling.
3. The fold's message. Nothing else in the fold moves — see above.

When it lands: fold the cadence rule into `sidecar/CLAUDE.md` (it states the 10m interval as the
correctness bound in two places), and write the ADR — see the note below.

## Open: one ADR or two

`docs/adr/README.md` says a changed decision gets a **new** ADR with the old one flipped to
Superseded. This does not change the accepted decision — pull-first stands, the watch still only
wakes, the interval is still the backstop — it makes the backstop's *length* conditional on the
watch it backs. So this reads as an ADR that extends rather than supersedes, leaving
`2026-08-26-kubecatalog-watch.md` Accepted and linking forward to it. Confirm that before writing
it; the alternative is a supersede-and-repoint, which touches every link to the old one.
