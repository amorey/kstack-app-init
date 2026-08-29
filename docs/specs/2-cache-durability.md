---
title: Cache durability
scope: sidecar, frontend
status: Planned
order: 2
---

# Cache durability

**Needs:** nothing. **Hands on:** nothing. Second because none of it is user-visible until it
goes wrong, and then all three are: a cache file that only grows, a cache that will not open and
never says so, and a wedged request that holds a start slot for the life of the process.

Three defences the rebuild has not carried over. They share a spec because they share an owner —
the storage and transport a cache runs on — and none is large enough to be worth a plan of its
own.

## Part 1 — a janitor: reclaim free pages, bound `status_history`

`kubestore.openFile` sets `auto_vacuum=INCREMENTAL` before any table exists
(`manager.go:423`), which on its own reclaims nothing: pages return to the OS only when
`PRAGMA incremental_vacuum` runs, and nothing runs it. Every writer that frees pages — a relist's
sweep, `ClearKind`, a `Remove` — grows the freelist, and the file sits at its high-water mark
until the cache is cleared outright. `ClusterCacheStats.Bytes` reports that mark, so the number a
user watches is the worst the cache has ever been rather than what it holds.

`status_history` has no retention at all. It is append-on-change (a relist that rewrites every
row inserts nothing), so volume is small — but nothing bounds it, and it is the one table the
store owns outright rather than mirroring from the server.

**Shape.** A per-cache tick: trim `status_history` past a TTL, then read `PRAGMA freelist_count`
and hand back a bounded number of pages.

```go
// Retention is what the janitor's own tables hold. Events are absent on purpose: their
// retention is the server's, mirrored by the relist's prune, and the janitor sweeps only
// tables nothing upstream owns.
type Retention struct {
	StatusHistoryTTL time.Duration
	Interval         time.Duration
}
```

**The bound is the point.** A cache has one writer, so an unbounded vacuum blocks every kind's
sync — and the freelist is biggest exactly when that hurts most, right after a relist. ~2048
pages per sweep is ~8MiB at a 4KiB page; a backlog drains over the following sweeps. Make it a
`var` so a test can shrink it, and take `Retention` as a parameter rather than reading a
constant, per the repo's pace-by-parameter rule.

**Gate on the freelist, never on what the sweep itself deleted.** The writers that actually free
pages do not vacuum, so a rows-deleted gate would see nothing to do and strand the file forever.

**The `status_history` delete needs no bound**, unlike the vacuum. `WHERE at < ?` is a full scan —
the index is `(uid, at DESC)`, which does not serve it — but the table is append-on-change and
small by construction, and the delete is one statement rather than a page-by-page walk. If it ever
stops being small, the answer is an index on `at`, not a chunked delete.

**Where it lives: one janitor per open file**, started in `openFile` and stopped in
`(*file).close` — the shape `main` uses (`startJanitor` on the `ClusterDB`, cancelled in its
`shutdown`).

`(*file).close` (`store.go:155`), **not `Manager.closeFile`**. That is a field defaulting to
`(*file).close`, and `withCloseFile` exists so a test can substitute it "to drive a clear whose
database will not close" — a stop living in the default is bypassed the moment it is swapped,
leaving the janitor vacuuming a file the clear is unlinking. `(*file).close` is the real close,
the symmetric counterpart to the package-level `openFile`, and the one every exit runs through:
`Manager.Close` goes via the same field (`:467`).

**The stop must not wait under `m.mu`.** All three exits — `Clear`, `Remove`, `Manager.Close` —
hold the lock across the close, so a cancel-then-wait-on-done shutdown blocks `Stats` behind it.
`main`'s shape is fine because the sweep's statements run under the janitor's own context, so the
cancel aborts them mid-query and the wait is bounded by that. Keep the sweep cancellable: a
`<-done` added over a sweep that ignores its context reintroduces the stall this part rejects the
manager-level shape over, through a different door.

`Manager.Start` stays the no-op it is.

**The first sweep runs inside the janitor goroutine, and `openFile` only starts it.** A cache
should not wait a full interval for its first pass — but running that sweep inline would run
`freelist_count` and `incremental_vacuum` under `m.mu`: `OpenOrCreate` takes the lock and
delegates to `openOrCreateLocked`, and `Clear` holds it across close → unlink → reopen. That is
the exact stall this part rejects the manager-level shape over, and it is the ordinary path, not
an edge — `openOrCreateLocked` opens the existing file, which is the one with a freelist. `main`
gets this by construction: `startJanitor` spawns, and `runJanitor` sweeps first thing inside the
goroutine.

**Retention reaches `openFile` through `NewManager`**, which takes only a dir today. Not through
`newManagerWithOptions` — `option` is "a test seam, reachable only from white-box tests"
(`manager.go:105`), so production never passes one. A `Retention` argument on `NewManager` is both
the plumbing and the pace-by-parameter seam this spec's rules ask for.

**`openFile` has two call sites, and the second is the one that gets missed**:
`openOrCreateLocked` (`:152`) and `Clear` (`:254`), which reopens a fresh file mid-clear. Start the
janitor in `openFile` itself rather than at either call site, or a cleared cache silently loses its
sweeper — the cache most likely to need one, since a clear is what frees the pages.

**A manager-level goroutine over the open entries is the wrong shape**, and the reasons are worth
stating because it is the obvious first idea:

- **"Sweep once at start" would stop meaning anything.** Per file, "start" is that cache's open.
  Per manager, it is process start, and a cache opened ten minutes later waits a full interval for
  its first sweep.
- **The file goes out from under the sweep.** `Clear` closes the file, unlinks it and swaps in a
  fresh one; `Remove` closes it and drops the entry. A sweep that read `e.file` and released the
  lock is vacuuming a closed pool.
- **It cannot hold `m.mu` across the vacuum.** `Stats` deliberately takes `m.mu` for its whole
  measurement so a clear is never observed mid-swap, and a blocking `incremental_vacuum` under
  that lock would stall the size gauge in the very view holding the Clear button — the failure
  that comment exists to prevent.

Per file, all three questions answer themselves: the janitor's lifetime is the file's.

`main`'s `sidecar/internal/cluster/cache/store/janitor.go` is the worked version of the sweep
itself; read it before writing this.

## Part 2 — a cache that will not open must say so

A cache whose file will not open fails on every pass and reports nothing. `kubesync.arm`
(`service.go:662`) swallows the error:

```go
if err := sess.start(); err != nil {
	slog.Error("kubesync: arm cache", "cacheID", cacheID, "err", err)
	sess.cancel()
	...
	return
}
```

`TrackDiscovery` returns `void`, so the cache's pass never learns. With no session:

- `GetDiscoveryState` answers `!ok`, so `ClusterCacheDiscoveryStatus` stays zero — empty reason,
  empty message.
- `logDiscoveryVerdict` (`caches.go:1007`) returns early on the same `!ok`, so **no event is
  written either**. The timeline says nothing.
- Every kind's `GetKindState` is `!ok`, so the health fold reads them all as unanswered, and
  `readCacheHealth` keeps its `Connecting` default.

The result is a cluster stuck at "Syncing" with no reason, no events, no conditions, and one
`slog.Error` per pass in a log the user is not reading. **That is the defect** — not the file.

**The recovery already exists.** `openOrCreateLocked` installs no entry when `openFile` fails, and
`Clear` with no entry falls straight through to `deleteFiles(path)` and returns nil. So
`clusterCacheClear` fixes an unopenable cache today, with no new code. What is missing is any way
for the user to know that is the thing to do.

### Shape

Carry the failure out of `arm` and let it answer as the discovery verdict — `StoreFailed`, beside
the reasons already in `state.go`, carrying the open error as its message.

**It has to live outside the session**, because a failed start leaves none. A per-cache field on
the `Service`, written where the `slog.Error` is, and read by `GetDiscoveryState` when there is no
session to read.

**The map is `s.mu`'s**, not `armMu`'s. `GetDiscoveryState` reads it under `s.mu`, and both
clears below sit in `armMu`-held callers that already take `s.mu` for the sessions map — so both
piggyback on a critical section that exists rather than adding a third mutex. This package is
explicit about which lock owns what (`tearDown` carries "Called under armMu, and never under mu");
say it here too.

**Clear it in two places, and both placements are exact.**

- **In `arm`, in the `s.mu` section it already takes.** The retry path does not pass through
  `tearDown`: a failed arm deletes the session (`service.go:665`), so the next `TrackDiscovery`
  finds `sessionOf` nil, skips the `tearDown` branch entirely, and calls `arm` with the stale
  failure still recorded. Clearing here makes the invariant local and checkable — **the map holds
  exactly the caches whose most recent arm failed**.
- **In `tearDown`, beside `delete(s.sessions, cacheID)` — inside the first critical section.**
  Not after it. `tearDown` early-returns when there was no session, and a cache whose arm failed
  is precisely a cache with no session, so a clear placed anywhere below that guard is dead code
  for the only state that ever has an entry. `ForgetDiscovery` would leave the entry behind, which
  is the leak this bullet exists to prevent.

`GetDiscoveryState` reads the session first and falls back to the map only when there is none.
State that where it is written: with the invariant above it is belt-and-braces, but it is what
stops a stale entry outliving a session that has since succeeded.

**Mind the precedence rule** `DiscoveryState` already states: a suspended session beats a failing
read beats one that has yet to answer. A store failure is a failing read with no session at all,
so it ranks above "has yet to answer" and there is nothing above it to lose to.

**Cap the message where it is recorded, in kubesync.** A driver error is the first unbounded
discovery message this package has ever had — every existing one is internally generated and
short, which is why nothing downstream bounds them. It leaves by two paths: the gauge's
`ClusterCacheDiscoveryStatus`, and `logDiscoveryVerdict`, which passes `state.Message` straight
into `AddEvent` (`caches.go:1013`) with no cap and none applied after — `TruncateMessage` is only
ever used on conditions (`shared.go:338`). Capping at either boundary fixes one path and leaves
the other, and the event path is the one this spec has already ruled out touching.

So bound it at birth: a length check against a local constant in kubesync. It cannot call
`TruncateMessage` — that lives in `clustersvc`, which imports kubesync and not the reverse — so
the bound's value is written in two places. That is the cost, and it is smaller than two caps at
two boundaries. Hoisting `MaxMessageLen` into a leaf both can see is the alternative if the
duplication ever bites.

**`logDiscoveryVerdict` needs no change — keep it that way.** Once `GetDiscoveryState` answers
`(state, true)` for a failed store, its `!ok` guard does not fire, and `StoreFailed` is neither
`NoConnection` nor `IdentityMismatch`, so the existing code writes the run. That is an invariant
to pin with a test, not work to do: the guard exists because those two reasons are the cluster's
problem rather than this cache's, and `StoreFailed` is this cache's. Run-extension keeps a
permanently broken cache at one row rather than one per pass.

**The rollup needs a cache-level verdict it does not currently have.** `readCacheHealth` folds
kinds only and defaults to `ReasonConnecting`; with no session every kind is unanswered, so a
store failure reads as "connecting" forever. It must consult the discovery state and let
`StoreFailed` decide.

Schema, and the two are not the same edit:

- `ClusterCacheDiscoveryStatus.reason` (`schema.graphqls:611`) enumerates its values — add
  `StoreFailed` to the list.
- `ClusterCacheHealth.reason` (`:561`) enumerates nothing. It states a fold rule — "the dominant
  per-kind reason: a hard failure beats a stall beats a wait beats a catch-up". `StoreFailed` is
  not a per-kind reason and wins with no kind having spoken, so that sentence has to be rewritten
  to admit a cache-level verdict above the per-kind fold, not extended.

### The panel drops the discovery verdict today

`ClusterCacheSyncStatus` is queried with `discovery { reason message }` and typed for it
(`cluster-sync-panel.tsx:228`, `:252`), and then never rendered — the only field read off
`syncStatus` is `.kinds` (`:777`). So a `StoreFailed` verdict reaches the webview and is dropped.

Two of the three reports land with no frontend change. The timeline run shows up through
`useDiscoveryEvents` under "Recent kind discovery" (`:827`), and the rollup goes amber on its own,
because an unrecognized reason falls to `default: { label: 'Degraded', tone: 'attention' }`
(`:453`) — deliberately, since "an unknown reason is DEGRADED, not healthy".

What is missing is the middle one, which is the useful one: **an amber "Degraded" with no reason
and no message is not a report.** So this part carries a small frontend change — a `StoreFailed`
case in the status switch, and rendering `discovery.reason`/`discovery.message` when non-empty.
That second half is worth having on its own: every discovery verdict is currently invisible, not
just this one.

**Tone: `error`, like `SyncFailed`** — every other case in that switch justifies its tone, so this
one must too. The rule the switch actually follows is severity and whether it clears on its own:
`Stale` is amber because a quiet watch recovers, `NoConnection` is muted because nothing is wrong
with the sync itself. A store that will not open clears on its own never, and nothing syncs at all
until someone presses Clear. That is at least as hard a fault as one kind failing.

### Why this and not a quarantine

**Surfacing covers strictly more.** Renaming a corrupt file aside self-heals exactly one cause. A
full disk, a permissions change, a failed migration, a reader-pool failure — every one of those
lands in the same silent state, and none of them is corruption. Fixing the report fixes all of
them; corruption stops being a special case and becomes one legible instance.

**And a quarantine has to be right about which error it saw.** Discriminating corruption from
ENOSPC is the whole safety of it: rename a healthy cache aside and the cluster is re-downloaded.
That risk buys automation of a button the user already has, on a failure that is rare and that
this part makes visible.

Self-healing can sit on top later — `main`'s `quarantineCorrupt` (`store.go:894`) is the rename if
it is ever wanted. Build it when a corrupt cache actually shows up, not before.

## Part 3 — an idle-read timeout on non-watch requests

`kubeconn` sets HTTP/2 `READ_IDLE_TIMEOUT` (`connection.go:456`), which is connection-level
keepalive: it detects a dead peer, not a live peer that has stopped sending. A wedged LIST — the
server holding the connection open and sending nothing — is invisible to it.

That matters because of what holds a start slot. A kind sync keeps its `kindStartConcurrency`
slot until `pass.Ready()`, which fires when the watch is open, i.e. after the cold list. So one
hung cold list costs a permanent fraction of the fleet's start capacity, and the kinds behind it
never list a row.

**Shape.** A `transport.WrapperFunc` on the connection's `rest.Config`, installed in
`NewConnection` beside the QPS/burst tuning:

```go
own.Wrap(newIdleTimeoutWrapper(listIdleTimeout))
```

**Progress-based, not a deadline.** Headers and every body chunk count as progress, so a slow but
streaming LIST always completes while a stalled one is cancelled. A wall-clock deadline would
kill the large-collection case this exists to protect.

**Watches are exempt, and this is the trap.** A healthy watch is legitimately silent between
bookmarks, so an inactivity bound would kill it. `main` skips any request whose `RawQuery`
contains `watch=true`, by substring rather than by parsing — this runs on every LIST page.

**Detection is coarse on purpose.** The watchdog ticks once per window, so idle-to-cancel lands
in `[timeout, 2*timeout]`. That is fine for a stall backstop and it is why the timer re-arms only
from inside its own callback, never from the read path: a read landing exactly as the timer fires
bumps a counter, and the next tick sees the progress. Resetting from the reader would race the
firing cancel and kill a live transfer.

**A cancelled request must say why it was cancelled.** `main`'s wrapper cancels a derived context
and returns whatever the transport produces — a `url.Error` wrapping `context.Canceled`. Ported
as-is, a stalled cold list ends the run as `SyncFailed` with "context canceled" as its message,
and that string is what the sync panel puts in front of the user. The classification is otherwise
right: the run's own ctx is untouched, so `kinds.go`'s `ctx.Err() != nil` guard correctly does not
fire. Only the message is useless. Have the wrapper substitute a sentinel of its own when the
watchdog is what fired, so the verdict reads as the server having stopped sending. Cheap, and it
is the difference between a diagnosable stall and a mystery.

`main`'s `sidecar/internal/cluster/controllers/cache_idle_timeout.go` has the watchdog with its
locking already reasoned through (the `stopped`-under-mutex check is what stops a tick re-arming
a timer past `stop()`). Port it rather than re-deriving it.

## Rules

- **Pace by parameter.** `Retention`, the page bound, and the idle timeout are all arguments;
  production passes the constants. No test outwaits a production number.
- **The vacuum is bounded, always.** One writer per cache.
- **A cache that cannot open reports why.** Silence is the defect; the file is replaceable.
- **Watches never carry an idle bound.**

## Build order

Each part is independent; this is the order they are worth doing in.

1. **Report the open failure.** Smallest, and the only one that fixes a state the user cannot
   diagnose. **Two tests in two packages, and neither makes a real file fail** — the defect is the
   reporting chain, not the file, so a planted unopenable path would prove nothing the fakes do
   not.
   - **kubesync**, through the `storeManager` seam (`service.go:61`), which is one method: a fake
     whose `OpenOrCreate` returns an error is the whole setup. Assert `GetDiscoveryState` answers
     `StoreFailed` with the message, and pin both clears — a retry that succeeds stops reporting
     the old failure, and forgetting the cache drops the entry.
   - **clustersvc**, through `newFakeKubesync` (`testutil_test.go:356`), whose `discoveryStates`
     map is what its getter answers. Set it to `StoreFailed` and assert the three consumers: the
     projection carries reason and message, `logDiscoveryVerdict` writes the run, and
     `readCacheHealth` stops answering `Connecting`.

   Then the panel half: render the discovery verdict, and add the `StoreFailed` case.
2. **The janitor.** Test `sweep` directly against a store with expired `status_history` rows and
   a non-empty freelist, rather than through the ticker.
3. **The idle-read timeout.** Test with an `httptest` server that sends headers then stalls, and
   a second that streams slowly — the first is cancelled, the second completes. A third asserts a
   `watch=true` request is left alone. Assert the stall's error carries the sentinel, not
   `context.Canceled`; that is the part a port reproduces wrongly.

## Not in this pass

- **The table-shape review** — `WITHOUT ROWID` on the small keyed tables, `PRAGMA optimize` on
  close, `page_size` against the compressed-body row width, `mmap_size` for the read path. Real
  work, but it is measurement first and has never been measured. Keep it in `TODO.md`.
- **A vacuum triggered by a prune.** The tick is enough, and coupling the two puts a blocking
  write on the relist's path.
- **Events retention.** Server-mirrored by the relist's prune, deliberately.
- **Self-healing a broken cache file.** Part 2 makes the failure legible and leaves the fix to
  the Clear button that already works. Quarantining the file automatically, or probing for latent
  damage with a `PRAGMA quick_check` in the janitor, are both build-later — worth it once a
  corrupt cache has actually shown up, not before.

## Done when

A cache taken through a relist and a `clearKind` shrinks on disk within a few sweeps instead of
holding its high-water mark; a cache whose file will not open reads `StoreFailed` in the panel
with the driver's message beside it and a run on its timeline, instead of an unexplained amber;
and a kind pointed at a stalled API server releases its start slot instead of holding it, with the
other kinds listing behind it.

`sidecar/CLAUDE.md`'s `kubestore` and `kubeconn` sections gain the three behaviours in the same
commits. Delete this spec when the last part lands.
