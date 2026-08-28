---
title: The mirror on the probe engine
scope: sidecar
status: Planned
needs: kubesync seam, steps 1–3
hands to: kubesync seam, step 4
---

# The mirror on the probe engine

## Goal

Run each kind's mirror as a probe on `internal/probe`, the way discovery already does — so one
engine owns the backoff ladder, the retry countdown, the identity gate, the fleet-wide cap, and
the wake, for every job `kubesync` keeps running. `kinds.go` keeps what is about Kubernetes: the
cold list, the resume, the deltas, the stale timer, the reasons.

The engine's contract does not change. A run still ends, and its `Result` is still its schedule.
What changes is what the mirror's run *is*: not the stream, but the pass that makes sure the stream
is up. The stream outlives the run as the probe's **value**, which the engine already knows how to
hold and hand back.

## The picture

```
before                                   after

session                                  session
  └─ worker (goroutine per kind)           └─ mirrorEngine.Add("1/apps/v1/deployments")
       └─ kindSync.loop                          │
            backoff ladder                       ▼ run: is the stream up? if not, bring it up
            AwaitConnFor gate                    │   gate → cold list or resume → start goroutine
            re-enter on return                   │   commit the handle as the value → Succeeded
            └─ establish / stream                ▼
                                            handle (the value): the goroutine, its cancel, how it ended
                                                 exits → Wake(subject) → the run decides again
```

Two engines in `kubesync`: `discoveryEngine` over cache subjects, `mirrorEngine` over kind
subjects. Probes are registered before the first `Add`, and kinds are discovered at runtime, so a
kind's identity rides the subject name rather than a registration.

## Decisions this spec takes

1. **The stream is the value, not the run.** A run that blocks for the life of a watch would hold
   an engine worker for hours, need a cancel-in-flight the engine does not have, and could not
   move its verdict while it ran. A run that establishes the stream and returns holds a worker for
   one LIST, restarts by cancel-and-`Wake`, and leaves the verdict to the goroutine — and the
   engine's `Discard` hook is exactly the cleanup a goroutine-owning value needs.
2. **A death costs one rung, and the run that observes it starts nothing.** The run woken by a
   stream ending with an error records `Fail` and returns; the run the ladder schedules is the one
   that re-establishes. Re-establishing on the same wake would make a server that closes watches
   on open a hot loop; `Fail`ing without ever starting would fail forever. One bit on the handle —
   `observed` — tells the two runs apart.
3. **`Succeeded` means established.** The run returns only once the LIST has committed and the
   WATCH is open, so the ladder resets when a stream is actually standing — what `kinds.go` does
   today — and an establish that fails climbs it.
4. **No new engine abstraction.** No "job" helper that starts a goroutine and wires its exit to
   `Wake`. The mirror is the only supervised value; its handle is written plainly in `kubesync`.
   Names stay: `Run` is still what a body does, and a body that checks whether a watch is up is
   still a probe.
5. **No rename.** `probe`, `Probe`, `Run` describe both uses. `Reconcile` would fit the mirror and
   misfit the eight probes that exist.

## The engine

Two changes, both small. Nothing else the mirror needs is missing: `WithTimeout(0)` already means
unbounded, `Wake` is already redelivered to a run in flight, `Pass.Prev` is already how a run
finds what the last one committed, and `Suspend` plus the session's bridge is already the gate.

- **`Remove` and `Close` hand back the standing value.** Both drop the subject today and leave its
  value where it was; a value owning a connection was fine because `kubeconn` retires on `Release`
  itself, but a value owning a goroutine would leak unless every caller remembered to stop it
  first. The invariant becomes *the engine hands back every value it stops holding*: `Discard`
  outside the lock on both paths, for a probe that implements it. A run in flight against a
  removed subject is unchanged — its commit is refused and its value discarded, as now.
- **Docs.** The package comment gains one sentence after "a run's own Result is its schedule": a
  run may start something that outlives it and commit it as its value; the value is then the
  engine's to hand back and the next run's to find. The `Probe` comment's "a connection, a file"
  gains "a goroutine it started". → A short ADR records why the stream is the value rather than
  the run.

## The mirror as a probe

**Subject.** `"<cacheID>/<apiVersion>/<resource>"`. `apiVersion` carries its own `/` for a group,
so it is parsed as first segment, last segment, and everything between. `TrackKind` is `Add`,
`ForgetKind` is `Remove`; the session no longer holds a worker map, and what it holds per kind is
whatever the snapshot says.

**Value.** `*mirror`, the handle:

```go
type mirror struct {
    cancel context.CancelFunc
    done   chan struct{}      // closed when the goroutine returns
    err    error              // how it ended; nil for a clean stop (restart, retirement, Remove)
    // observed marks a death the run has recorded. The run that sees it unset Fails and starts
    // nothing; the run the ladder schedules sees it set and re-establishes.
    observed bool
}
```

`Discard(*mirror)` cancels and waits on `done`. That covers a subject removed mid-run, a body that
panicked, and — with the engine change above — `Remove` and `Close`.

**Run.** Wrapped in `sessionScoped`, as discovery is, so the run is counted for the session's
teardown and its context ends with the session's. Then:

```
prev alive                         → Succeeded()                       nothing to do
prev dead, err != nil, !observed   → prev.observed = true; Fail(SyncFailed, err)   start nothing
otherwise (dead+observed, clean, or none):
    ConnFor(ServerUID)             → Suspend(NoConnection | IdentityMismatch) on refusal
    cookie?  resume : cold list    → Fail(SyncFailed, err) if it will not establish
    open the WATCH
    start the goroutine, pass.Commit(handle)
    Succeeded()
```

Registered `WithInterval(10m)` — a liveness re-check, since the goroutine's exit is the real
re-entry — `WithBackoff(1s, 2, 1m)` (today's ladder), `WithTimeout(0)` because a cold list of a
large kind legitimately outlasts any bound the other probes want, and on an engine built
`WithWorkers(8)`: the worker cap **is** the cold-list gate, since a run holds a worker only through
establishment. `pacing.coldLists` goes.

**Goroutine.** `stream` as today: apply deltas, book bookmarks into the cookie, stamp `LastLiveAt`
and `LastUpdateAt`, prune events on cadence, commit `Watching`/`Stale` off the stale timer. It also
selects on `conn.Done()` and returns cleanly when the connection is retired. **On return, for any
reason, it records `err` and calls `Wake(subject, "mirror")`** — one re-entry path for a death, a
retirement, and a restart.

`Expired`/`Gone` is still the one error a resume cannot retry through. It rides the handle's `err`
and the re-establishing run reads it: `apierrors.IsResourceExpired` on `prev.err` is what makes the
next start a relist (`Resyncing`) rather than a resume.

**Restart.** `RestartAll` cancels every handle it reads off the engine's snapshots. A clean exit
carries no error, so the run it wakes re-establishes at once — off the cookie, holding its reason.

**Retirement.** The goroutine's `conn.Done()` branch is the same clean exit. The run it wakes goes
through the gate: `Suspend` if the replacement is not yet vouching, and the session's connection
bridge brings it back; resume otherwise.

## What the kind reports

`commitKind` stays the one publisher, and grows the SinceAt rule: the stamp moves when the
committed reason differs from the stored one, in one place. Two callers:

- **The body** — the run and the goroutine — commits reason and stamps as it learns them:
  `Syncing`/`Resyncing` before a list, `Watching` on establish and on every proof of life, `Stale`
  off the timer, `Resuming` when a resume outlasts `staleAfter`, `LastUpdateAt`/`LastLiveAt`.
- **`OnPass`** overlays what the engine knows onto the stored state and commits the result:
  `Restarts` from `Failures`, `NextRetryAt` from `NextAttempt.ScheduledAt`, and the reason when
  the attempt outranks the body — `NoConnection`/`IdentityMismatch` from a `Suspended` attempt,
  `SyncFailed` with the error from a `Failed` one. A `Succeeded` attempt overlays nothing on the
  reason: the body's answer stands, which is what keeps a resume holding `Watching`.

`KindState`'s fields do not change. What changes is that `Restarts` and `NextRetryAt` are read off
the engine rather than counted by hand, and `SinceAt` is stamped in `commitKind` rather than by
each writer.

## The session

- **Two engines**, both started by `Service.Start`, both closed by `Close`. `New` registers the
  three discovery probes on one and `mirror` on the other.
- **The connection bridge wakes both.** `wakeDiscoverySweepOnConnectionChange` becomes the
  session's one connection bridge: on a frame where the pool's answer moved, it wakes the discovery
  probes and every kind subject under the session. The "only when the reason moved" guard stays,
  applied once per session — the pool's answer is one fact for every kind — so a suspended cache of
  three hundred kinds is not polled per frame.
- **Runs are counted for both engines.** `enterSweep`/`leaveSweep` become `enterRun`/`leaveRun`;
  `close` and `ForgetKind` wait on the same group. `ForgetKind` is `Remove` (which discards the
  handle: cancel and join) followed by that wait — synchronous, as the seam promises.
- **`close`** removes the session's subjects from both engines, cancels, waits, and releases the
  two claims — the order it has now.
- **`awaitConn`, `untilRetired`, and the `worker` type go.** `kubeconn.AwaitConnFor` loses the
  `refused` callback it gained for the gate; nothing else calls it with one.

## What goes away

`worker` and the worker map, `kindSync.loop`, `syncKindFn`/`withSyncKindFn`/`kindRun.Prev`,
`pacing.backoff` and `pacing.coldLists`, `awaitConn`, `untilRetired`, `AwaitConnFor`'s `refused`,
`KindState.setReason`, and the comment that the body paces its own retries because the worker
re-enters it.

## Rules

- **A run is short.** It holds an engine worker through one establishment and returns. Nothing in
  a run waits for a connection, a wake, or a stream.
- **The goroutine's exit is a `Wake`, always.** There is no other way a stream's end reaches the
  schedule, and a `Wake` is never lost — the queue redelivers one that lands mid-run.
- **The run that observes a death starts nothing.** Decision 2; the `observed` bit is the whole
  mechanism.
- **A handle is handed back exactly once**, by `Discard`, and `Discard` joins. Nothing else cancels
  a goroutine except `RestartAll` and retirement, both of which leave the handle to be found dead
  and clean by the next run.
- **Nothing syncs into a cache whose connection does not vouch for its `ServerUID`** — unchanged,
  now enforced by `Suspend` rather than by blocking.
- **A resume holds its reason** — unchanged; a `Succeeded` attempt overlays nothing on it.

## Tests

`kinds_test.go` pins the behaviour and most of it survives: every test that drives a mirror
through `newMirroringService` and reads `awaitKindReason` is unchanged in what it asserts. What
moves:

- **The harness** arms through the engine. `newMirroringService` registers the real mirror;
  `newTestService`'s default substitute becomes a mirror body that `Suspend`s (so arming tests see
  a tracked kind that never writes), registered through an option that replaces `withSyncKindFn`.
- **`TestAColdListWaitingOnTheGateGivesUpWithItsRun`** becomes a test that a run queued behind the
  worker cap gives up with its session — `WithWorkers(1)` on the test engine, one run parked.
- **New:** a stream that dies is re-established after one rung, not at once (a fake watcher that
  closes on open must climb the ladder, not spin); `RestartAll` re-establishes without a rung;
  `Remove` hands the handle back (the goroutine is joined before `ForgetKind` returns);
  `TestAKindParkedAtTheGateReportsWhyItWaits` and
  `TestAConnectionRetiredUnderAMirrorIsReplacedRatherThanRetried` stay as they are and pass
  through the new path.
- **Engine:** `Remove` and `Close` call `Discard` on a standing value; a run in flight against a
  removed subject still discards its own.

No magic sleeps: the ladder is shrunk through `WithBackoff` on the test registration, and every
wait is on a reason, a probe, or a channel.

## Build order

Each step is one red/green cycle and one commit.

1. **The engine**: `Remove`/`Close` discard, the two doc sentences, the ADR.
2. **The mirror probe**: `kinds.go` split into the run and the goroutine, the handle, `Discard`,
   registered on a `mirrorEngine`; `kinds_test.go` moved onto the new harness. `session.go` still
   arms it through a worker at this step, so the old and new bodies can be diffed by their tests.
3. **The session**: `TrackKind`/`ForgetKind` as `Add`/`Remove`, the bridge waking both engines,
   `enterRun`, `RestartAll` by cancel, `OnPass` overlay into `commitKind`; the worker, `awaitConn`,
   and the `refused` callback deleted.
4. **Docs**: `sidecar/CLAUDE.md`'s "The mirror" section and the kubesync-seam spec's internal
   shape, then this spec deleted.

## Not in this pass

- **Moving the stale timer onto the schedule.** A `Succeeded().RequeueAfter(staleAfter)` could
  replace the goroutine's timer, but the timer is exact and the schedule is not, and the reason
  belongs to the goroutine that has the stream.
- **A shared "supervised value" helper in `probe`.** Decision 4; revisit at the second consumer.
- **Anything above the seam.** `clustersvc` still does not call `TrackDiscovery`; step 4 of the
  kubesync seam is unchanged by this.

## Done when

`kinds_test.go` and `session_test.go` are green on the new path with the assertions they have
now, plus the new ones above; `grep worker session.go` finds nothing; `kubeconn.AwaitConnFor` has
three parameters again; and a kind's `Restarts`/`NextRetryAt` on the seam are the engine's
`Failures`/`ScheduledAt` for that subject.
