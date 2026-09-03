---
title: A cache over its ceiling stops growing
scope: sidecar
status: Planned
---

# A cache over its ceiling stops growing

**Needs:** nothing — detection has landed. This spec uses the verdict and the wake the janitor
already publishes (`kubestore.Manager.WatchSizeLimitNews`, `Stats.OverSizeLimit`) and builds nothing
of its own below `clustersvc`. **Hands on:** nothing.

**This is the half of the security patch that enforces.** **S-5** in
[`security/2026-09-02-threat-model.md`](../security/2026-09-02-threat-model.md), answered by a
whole-file ceiling rather than per-table event retention (→ [bound the cache by total
size](../adr/2026-09-03-bound-the-cache-by-total-size.md)) — and the row it moves is *"A size ceiling
on a cache"* in [`security-model.md`](../security-model.md). Until this lands, a cluster an attacker
controls can still fill the disk. The ceiling is only a number the UI can render.

## Goal

A cache that goes over its ceiling stops syncing, and says why in a way the UI already shows. It
starts again once the file is back under the ceiling. Nothing evicts, so in practice that means the
user cleared the cache or raised the limit.

## Do not delete objects to make room

A cache that quietly drops what the user asked it to mirror answers questions wrongly, and a delta
watch on top of it never recovers: it applies deltas to rows it assumes are whole, so a truncated
table looks like a complete one. Saying "I stopped" is worse for nobody. The user's remedy is
`clusterCacheClear` (`graph/schema.resolvers.go:117`) or a higher limit.

## The pause already exists

`armSync` (`caches.go:888`) calls `ForgetDiscovery` when `cacheSyncEnabled` is false and
`TrackDiscovery` otherwise (`:918`, `:921`). A pause leaves the kinds registered under `kubesync`, so
one call restarts all of them. **The ceiling is one more input to that same switch** — not a new
mechanism, and not a flag inside the store, which is the file we may be refusing to write to.

## The trap: pausing throws away the fact it was based on

This is the shape of the whole feature, and it is easy to miss.

`ForgetDiscovery` tears the session down, which gives back the store claim
(`kubesync/session.go:388`). The last claim to go closes the file (`kubestore/store.go:253`) and ends
its janitor. From then on:

- `Stats.OverSizeLimit` is **false**. It reads `file.sizeVerdict`, and there is no file. The doc
  comment says as much: false "when nobody has it open (a closed cache cannot grow)".
- Nothing sweeps the file, so nothing publishes a verdict either way.

A pass that trusted `OverSizeLimit` alone would therefore read `false` on the very next pass of the
cache it had just paused, start it again, reopen the still-oversized file, sweep, and pause again.
That is a flap, and every turn of it writes more into a cache that is already too big.

**So the record holds the decision.** `OverSizeLimit` is what *starts* the pause. `Synced=False`
with `ReasonSizeLimit` on the cache record is what *keeps* it — across passes, across restarts, and
across the closed file.

## What ends the pause

While the pause is on, the pass reads `Stats` and ends it when the file is at or under the ceiling:
`Bytes() <= SizeLimitBytes`.

Two different comparisons against one ceiling, because the two moments are different:

- **Starting the pause**, use `OverSizeLimit`. The file is being filled, so its WAL may hold pages
  the database already counts. The janitor checkpoints before it judges; a plain measurement does
  not. Only the janitor is right about a file mid-fill.
- **Ending it**, compare the bytes. There is nothing left to disagree about: a paused cache has no
  writer, and a cleanly closed SQLite file has no WAL at all. If some reader still holds the file
  open, its WAL is whatever the last write left, and the next sweep checkpoints it away.

Neither is a ceiling of its own. Both use the number `Stats` reports.

Two things actually put a paused cache back under the ceiling: a **clear**, which empties the file,
and a **raised limit**, which takes effect at the next start. Nothing shrinks it on its own, because
nothing evicts. That is the accepted consequence of the ADR, not an oversight.

### `SizeLimitBytes == 0` means two things

It is a manager with no ceiling *and* a cache whose file is not there — `Stats` returns `Stats{}, nil`
on `os.IsNotExist` (`manager.go:335`), which is exactly the state a clear leaves behind. The release
is right in both: `Bytes()` is 0 as well, so `0 <= 0` ends the pause.

Because the value is ambiguous, **a test for the unbounded case must set bytes over any plausible
limit and `SizeLimitBytes: 0`** — otherwise it passes for the missing-file reason and proves nothing
about an unbounded manager.

## A clear has to wake the pass itself

`Manager.Clear` reopens the file **only if a claim is still held** (`manager.go:255`, `:272`). A
size-paused cache has no session, and the gauge's `Subscribe` takes no claim, so usually nothing is
held: the files are deleted and no fresh file, janitor or verdict follows. `cachesAPI.Clear`
(`caches.go:708`) writes nothing to the record and requeues nothing either. Left as is, a cache would
stay paused forever after the user's remedy.

So the clear path ends the pause itself. After `kubestoreMgr.Clear` succeeds, requeue the cache
record:

```go
if err := a.s.cacheClient.Requeue(ctx, beehive.ObjectID(id), beehive.WithResetBackoff()); err != nil {
    slog.Warn("clear: requeue cluster cache", "cacheID", id, "err", err)
}
```

Three things about that call:

- **`WithResetBackoff()` is required.** `Requeue` keeps the record's retry ladder by default. A clear
  is the event that proves the old verdict stale, so the pass it asks for must not sit behind a delay
  earned by earlier failures.
- **The error is logged, not returned.** The clear itself already succeeded; failing the mutation
  would tell the user their remedy did not work. `Requeue` fails with `ErrNotFound` (the record is
  gone, so there is nothing to release) or `ErrNoController` (the process is shutting down, and the
  startup pass will re-decide).
- **It is a latency hint, not a guarantee.** `ClusterCache` is registered with triggers and a startup
  full pass, and no `WithIndividualPassInterval` (`service.go:534`) — so unlike `ClusterSource` and
  `Cluster`, no periodic pass re-decides it. If a requeue is ever lost, the pause holds until the
  next start. That is acceptable because the requeue is in-process and its two failure modes are
  "no record" and "no process", but it is the reason this path deserves a test.

## The UI reads the gauge, not the condition

`clusters.tsx` leaves `Synced` out of its selection on purpose. What the UI shows is
`clusterCacheHealthWatch`, folded in `readCacheHealth` (`caches.go:477`). **A verdict written only to
the condition is invisible.** The fold needs an arm of its own, for the same reason the `storeFailed`
arm at `caches.go:544` is there: a paused cache arms nothing, so every kind reads as unanswered and
the default would report a stopped cache as still connecting.

**Put the size arm at the top of the switch, above the all-kinds-paused arm.** Below it, a cache that
is over its ceiling *and* has every kind individually paused would report `ReasonPaused` — the exact
confusion the next section forbids.

That arm reads a stored condition, which is the opposite of what `WatchHealth`'s comment claims the
fold does ("A read-side projection, never a stored condition", `caches.go:434`). Reading it is right
here, and the comment needs updating to say why: the per-kind states are live worker facts, while
this one is a decision the controller made and wrote down — reading it back is how the pause survives
the file being closed.

`readCacheHealth` takes only a `cacheID`, but `readAllCacheHealth` (`:762`) already holds the object
with its conditions loaded. Pass the object down rather than adding a second read.

## Do not reuse `ReasonPaused`

It means sync deliberately off — "sync-disabled, deactivated, orphaned, or archived"
(`shared.go:357`) — and is written at `caches.go:759`. Reusing it makes "the user turned sync off" and
"this cache hit its ceiling" indistinguishable to anything keying on the reason, which is what a
reason is for. A different message does not fix that.

## What to build

**1. `ReasonSizeLimit`**, beside `ReasonPaused` in `clustersvc/shared.go`.

**2. A trigger over the size news.** In `triggers.go`, a `newKubestoreSizeTrigger` shaped exactly
like `newKubesyncDiscoveryTrigger` (`:167`). The key is the cache id, and a cache id *is* the
record's id, so it addresses the record with no store read. Register it with the cache controller the
way the discovery trigger is registered.

**3. The pass decides.** Before `armSync`, read `Stats` and work out one boolean, `sizeLimited`:

| condition on the record | measurement | result |
| --- | --- | --- |
| set | at or over the ceiling | stay paused |
| set | under the ceiling, or no ceiling | release |
| not set | `OverSizeLimit` | pause |
| not set | anything else | run |

`OverSizeLimit` is the **only** way in. Bytes over the limit with no verdict behind them is a file
nobody has open, which cannot be growing — starting a pause on it would pause caches nothing is
filling.

**A failed `Stats` read changes nothing**: the condition stands and no write happens. This check sits
in the arming path, so answering "under" for a failed measurement would restart a cache nobody
measured, and answering "over" would pause one because of an error.

Then:

- `sizeLimited` goes into `armSync`'s switch, so the pass calls `ForgetDiscovery`. One path both
  ways, so the release cannot be forgotten in a branch.
- On the way in, write `Synced=False` with `ReasonSizeLimit`, message naming the size and the limit.
- On the way out, `DeleteCondition(ConditionSynced)` — **only if the object actually carries one**
  (`FindCondition`, `shared.go:376`). Nothing else writes `Synced`, so a missing condition and a
  deleted one mean the same thing, and checking first keeps every ordinary pass of every cache free
  of a write.

`ConditionSynced` exists (`shared.go:328`) and nothing writes one yet — its comment says so, so this
is new work with a settled name. Update that comment: this becomes its first writer.

**4. The health arm.** `readCacheHealth` reports `ReasonSizeLimit` while the record's condition says
so, at the top of the switch.

**5. Requeue on clear**, as above.

## The cost, accepted

`Stats` is a full measurement, and the arming path uses two fields of it. For a paused cache the file
is closed, so `countsLocked` falls through to `countsFromDiskLocked` (`manager.go:415`): a read-only
SQLite open, a count, and a close, all under `m.mu` — the lock `Clear` and `OpenOrCreate` contend
for. A paused cache is exactly the one that keeps re-passing, so it is exactly the one paying.

Accept it for now; it is bounded by the pass rate of one record. If it shows up, the follow-up is a
narrower call on the seam returning bytes and the verdict without the counts.

## Tests

In `caches_test.go` and the controller's tests. The seam is `fakeKubestore` — `setStats`
(`testutil_test.go:278`) and `publishSizeLimitNews` (`:269`) — so a test sets `OverSizeLimit`,
`DBBytes` and `SizeLimitBytes` on the fake and publishes the news. There is no real limit to shrink
here; that is the kubestore janitor's own tests.

- The controller pauses discovery and writes `Synced=False` / `ReasonSizeLimit`, with the size and
  the limit in the message.
- **A paused cache stays paused once its file closes.** Condition set, fake reporting
  `OverSizeLimit: false` (what a closed file answers) and bytes still over: the next pass neither
  restarts it nor rewrites the condition. This is the flap the design turns on.
- **Bytes over the limit with `OverSizeLimit: false` and no condition does not pause.** The mirror of
  the case above, and what keeps `OverSizeLimit` the only way in.
- The gauge reports `ReasonSizeLimit`, and a user-paused cache still reports `ReasonPaused` — the two
  stay distinguishable. A cache that is both reports `ReasonSizeLimit`.
- Bytes back under the limit clear the condition and restore `TrackDiscovery`.
- An unbounded manager releases: `SizeLimitBytes: 0` with bytes well over any plausible limit.
- `Clear` requeues the cache record with the backoff reset, and the pass it wakes ends the pause.
- A failed `Stats` read changes nothing: a paused cache stays paused, a running one keeps running.
- An ordinary pass of a cache that was never over the limit writes no condition.
- Nothing is deleted by any of it, and the object rows survive the pause.

No sleeps: the trigger's wake is a `testutil.Signal`, the controller pass a `testutil.Probe`.

## When it lands

Move the *"A size ceiling on a cache"* row in [`security-model.md`](../security-model.md) out of
**Detected, not enforced**, naming the tests that pin it. Write an ADR for the decision underneath
this spec — **a full cache stops rather than evicts, and stays stopped until the user clears it** —
since that is what a later reader will want to argue with, and record that the ceiling is soft by one
sweep interval. Fold the pause, its reason, and the rule that the condition holds it into
`sidecar/CLAUDE.md` beside the ceiling paragraph, and delete this spec.
