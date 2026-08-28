---
title: The mirror on the supervisor
scope: sidecar
status: Planned
needs: kubesync seam, steps 1–3; the supervisor rename
hands to: kubesync seam, step 4
---

# The mirror on the supervisor

## Goal

Run each kind's mirror as a reconciler on `internal/supervisor`, the way discovery already does — so one
supervisor owns the backoff ladder, the retry countdown, the identity gate, the fleet-wide cap, and
the wake, for every job `kubesync` keeps running. `kinds.go` keeps what is about Kubernetes: the
cold list, the resume, the deltas, the stale timer, the reasons.

The supervisor's contract does not change. A run still ends, and its `Result` is still its schedule.
What changes is what the mirror's run *is*: not the stream, but the pass that makes sure the stream
is up. The stream outlives the run as the reconciler's **value**, which the supervisor already knows how to
hold and hand back.

## The picture

```
before                                   after

session                                  session
  └─ worker (goroutine per kind)           └─ mirrorSupervisor.Add("1/apps/v1/deployments")
       └─ kindSync.loop                          │
            backoff ladder                       ▼ run: is the stream up? if not, bring it up
            AwaitConnFor gate                    │   gate → cold list or resume → start goroutine
            re-enter on return                   │   commit the handle as the value → Succeeded
            └─ establish / stream                ▼
                                            handle (the value): the goroutine, its cancel, how it ended
                                                 first frame → Wake → the run records the streak ended
                                                 exit        → Wake → the run decides again
```

Two supervisors in `kubesync`: `discoverySupervisor` over cache subjects, `mirrorSupervisor` over kind
subjects. Reconcilers are registered before the first `Add`, and kinds are discovered at runtime, so a
kind's identity rides the subject name rather than a registration.

## Decisions this spec takes

1. **The stream is the value, not the run.** A run that blocks for the life of a watch would hold
   a supervisor worker for hours, need a cancel-in-flight the supervisor does not have, and could not
   move its verdict while it ran. A run that establishes the stream and returns holds a worker for
   one LIST, restarts by cancel-and-`Wake`, and leaves the verdict to the goroutine — and the
   supervisor's `Discard` hook is exactly the cleanup a goroutine-owning value needs.
2. **A death costs one rung, and the run that observes it starts nothing.** The run woken by a
   stream ending with an error records `Fail` and returns; the run the ladder schedules is the one
   that re-establishes. Re-establishing on the same wake would make a server that closes watches
   on open a hot loop; `Fail`ing without ever starting would fail forever. One bit on the handle —
   `observed` — tells the two runs apart.
3. **The streak ends on the first frame, and the supervisor still owns it.** `Succeeded` ends a
   failure streak, so a run that returned it on establishment would hold a server that accepts a
   watch and drops it at the base delay forever — the trap `kinds.go` guards today by clearing the
   streak in `live()`, not on the open. Waiting for a frame inside the run is not an answer: an
   idle collection's first frame is a bookmark minutes out, and a run is short. So the
   establishing run records a success that **leaves the streak standing** — a `Result` modifier,
   below — and the goroutine's first frame is a `Wake`. The run that wake dispatches finds the
   stream alive and proven, records the plain `Succeeded`, and that is what clears `Failures`.
   The ladder, the count, and the countdown stay the supervisor's; the proof stays the frame.
4. **No new supervisor abstraction.** No "job" helper that starts a goroutine and wires its exit to
   `Wake`. The mirror is the only supervised value; its handle is written plainly in `kubesync`.

## The supervisor

Three changes. Nothing else the mirror needs is missing: `WithTimeout(0)` already means unbounded,
`Wake` is already redelivered to a run in flight, `Pass.Prev` is already how a run finds what the
last one committed, and `Suspend` plus the session's bridge is already the gate.

- **`Remove` and `Close` hand back the standing value.** Both drop the subject today and leave its
  value where it was; a value owning a connection was fine because `kubeconn` retires on `Release`
  itself, but a value owning a goroutine would leak unless every caller remembered to stop it
  first. The invariant becomes *the supervisor hands back every value it stops holding*: `Discard`
  outside the lock on both paths, for a reconciler that implements it. A run in flight against a
  removed subject is unchanged — its commit is refused and its value discarded, as now.

  This reaches `register`'s signature. The only `Discard` closure today is the one `Register[T]`
  builds per run, over that run's `Pass`; a standing value needs a type-erased one, so `spec`
  grows `discard func(any)`, built once by `Register[T]` from the reconciler's `Discard(T)` (nil
  for a reconciler without one) and passed through `register`. Outside the lock is load-bearing:
  the mirror's `Discard` joins a goroutine whose exit calls `Wake`, and `Wake` takes `e.mu`.
  `Wake` after `Close` is a no-op — the queue's `Add` returns once closed — so the `Close` path
  joins safely.
- **`Provisional`**, a modifier on a succeeded `Result`, the way `RequeueAfter` is: the run is
  recorded `Succeeded` — the verdict, the interval, `OK()` for a dependant — but `Failures` and
  `FailingSince` stand. For a run that started something it cannot yet vouch for. The next plain
  `Succeeded` ends the streak; a `Fail` climbs from where it stood. Inert on `Fail` and `Suspend`.
- **Docs.** The package comment gains one sentence after "a run's own Result is its schedule": a
  run may start something that outlives it and commit it as its value; the value is then the
  supervisor's to hand back and the next run's to find. The `Reconciler` comment's "a connection, a file"
  gains "a goroutine it started". → A short ADR records why the stream is the value rather than
  the run, and why the streak's proof is a frame rather than the open.

## The mirror as a reconciler

**Subject.** `"<cacheID>/<apiVersion>/<resource>"`. `apiVersion` carries its own `/` for a group,
so it is parsed as first segment, last segment, and everything between. `TrackKind` is `Add`,
`ForgetKind` is `Remove`. The subject names the kind but does not carry it: the run is handed the
full `kubestore.Kind` — its `Kind` name, which the rows are keyed by — out of `s.tracked[cacheID]`
by `enterRun`, and a subject whose kind is no longer tracked there `Skip`s.

**Entry.** `mirror.Reconcile` resolves the session itself rather than through `sessionScoped`,
because the two things that wrapper fuses come apart here: a run ends at `return`, but the
goroutine it starts does not, and the cancel and the `leaveRun` the wrapper defers would end the
stream with the run. So:

- The run is admitted by **`enterRun(cacheID, kindID, cancel)`, one critical section under
  `s.mu`** that does three things or refuses (→ `Skip`): checks the session is armed and not
  stopping, reads the `Kind` out of `s.tracked`, and registers the run's cancel in `inFlight`.
  One acquisition is load-bearing: split across three, a `ForgetKind` could withdraw the kind
  and find `inFlight` empty between the run's `Kind` read and its registration, and the run
  would cold-list rows for a kind nobody tracks — the relist-behind-a-clear race the seam
  orders `ForgetKind` before `Store.ClearKind` to rule out. `leaveRun` clears the entry and
  counts the run out.
- The **goroutine is counted separately**, `runs.Add(1)` from inside the run, before the run
  leaves — the counter is above zero while the run holds it, which is what makes the `Add` safe
  against a `Wait` already begun — and `Done` on its exit. `close` waits on that group, so a
  teardown joins the stream and not only the pass that started it; `ForgetKind` joins per kind,
  below.
- The run's context is the supervisor's, bounded by the session's (`context.AfterFunc(sess.ctx,
  cancel)`, as discovery does), **by `conn.Done()`** — a cold list on a retired connection is
  cancelled rather than left to hold a worker until the server gives up — **and by the kind's
  own cancel**, below. A list cut by a retirement `Fail`s and is retried up the ladder like any
  failed list; the retry goes through the gate and finds the replacement. A list cut by
  `ForgetKind` returns `Skip`; its commit would be refused anyway.
- **The run in flight is cancellable per kind.** `Remove` stops a subject being scheduled and
  discards its standing handle, but does not reach a run already out — and a cold list of a
  large kind is minutes. So the session keeps, per kind, the run in flight: `inFlight
  map[kindID]runHandle{cancel, done}` under `s.mu`, written by `enterRun` and cleared by
  `leaveRun`. It is not a worker — it exists only while a run is out — and it is what
  `ForgetKind` and a rename cancel and join. The other half needs nothing: `runReconciler`
  returns before dispatch for a subject that is gone, so no run starts after a `Remove`.
- The goroutine's context derives from `sess.ctx`, never the run's, with its own cancel on the
  handle.

`sessionScoped` stays as it is, for discovery.

**Value.** `*mirror`, the handle:

```go
// mirror is one standing stream. Its fields have one ordering rule: the goroutine writes err,
// then closes done; a run reads done, then err. close(done) is the only happens-before edge,
// so nothing reads err while done is open. proven is written by the goroutine while a run may
// read it, hence atomic. observed is the runs' own — written and read only by runs of this
// subject, which the queue serializes. cancel is safe to call from anywhere, any number of times.
type mirror struct {
    cancel context.CancelFunc
    done   chan struct{}
    err    error        // how it ended: nil for a clean stop, errWatchClosed for a rotation, else a failure
    // proven is set on the first frame, and is what lets a run record the plain Succeeded.
    proven atomic.Bool
    // observed marks a death the run has recorded. The run that sees it unset Fails and starts
    // nothing; the run the ladder schedules sees it set and re-establishes.
    observed bool
}
```

`Discard(*mirror)` cancels and waits on `done`. That covers a subject removed mid-run, a body that
panicked, and — with the supervisor change above — `Remove` and `Close`.

**Run.** After the entry above:

```
prev alive, proven                 → Succeeded()                       the streak ends here
prev alive, unproven               → Succeeded().Provisional()         a liveness re-check, nothing to do
prev dead, err == errWatchClosed,
          !observed                → prev.observed = true; Fail(reasonWatchRotated, err)   one rung, verdict held
prev dead, other err, !observed    → prev.observed = true; Fail(SyncFailed, err)           one rung
otherwise (dead+observed, clean, or none):
    ConnFor(ServerUID)             → Suspend(NoConnection | IdentityMismatch) on refusal
    cookie?  resume : cold list    → Fail(SyncFailed, err) if it will not establish
    open the WATCH
    start the goroutine, pass.Commit(handle)
    Succeeded().Provisional()      established, unproven
```

The three ways a stream ends are three exit classes, read off `err`:

| `err`             | what happened                              | the run it wakes                       |
|-------------------|--------------------------------------------|----------------------------------------|
| `nil`             | restart, retirement, `Remove`              | re-establishes at once, no rung        |
| `errWatchClosed`  | the apiserver rotated the watch            | one rung; the verdict stands           |
| anything else     | a failure                                  | one rung; `SyncFailed` with the error  |

`reasonWatchRotated` is a private reason with one job: to be a `Failed` attempt `OnPass` does not
overlay onto the reason. A rotation is not news — the rows stay current across the reopen, and
reporting it would flicker every kind through `SyncFailed` every few minutes — but it is paced,
so the countdown is published under the standing verdict, as today.

Registered `WithInterval(10m)` — a liveness re-check, since the goroutine's exit is the real
re-entry — `WithBackoff(1s, 2, 1m)` (today's ladder), `WithTimeout(0)` because a cold list of a
large kind legitimately outlasts any bound the other probes want, and on a supervisor built
`WithWorkers(8)`: the worker cap **is** the cold-list gate, since a run holds a worker only through
establishment. `pacing.coldLists` goes.

The cap has a head-of-line cost the semaphore did not: while eight cold lists are in flight,
every other kind's death observation and re-establish waits for one to finish, across every
cache. Accepted for this pass: a death is paced by at least the base rung anyway, an
observation and a resume are each a few milliseconds of worker time once dispatched, and the
alternative — a run that parks on a semaphore — is a run that waits. Revisit if arming a large
cache measurably delays a flapping kind on another.

**Goroutine.** `stream` as today: apply deltas, book bookmarks into the cookie, stamp `LastLiveAt`
and `LastUpdateAt`, prune events on cadence, commit `Watching`/`Stale` off the stale timer. It also
selects on `conn.Done()` and returns cleanly when the connection is retired. Two things reach the
schedule from it, and nothing else does:

- **The first frame** — a delta or a bookmark — sets `proven` and calls `Wake(subject, "mirror")`.
  Once per stream. The run it dispatches does no I/O: it finds the stream alive and proven and
  records `Succeeded`, which is what ends the streak.
- **Return, for any reason**, records `err`, closes `done`, and calls `Wake(subject, "mirror")` —
  one re-entry path for a death, a rotation, a retirement, and a restart.

A stream that dies between its first frame's `Wake` and the run it dispatches is observed as a
death by that run, so the streak climbs once from where it stood rather than from zero. Harmless
and self-correcting: the next frame ends it.

`Expired`/`Gone` is still the one error a resume cannot retry through. It rides the handle's `err`
and the re-establishing run reads it: `apierrors.IsResourceExpired` on `prev.err` is what makes the
next start a relist (`Resyncing`) rather than a resume.

**Restart.** `RestartAll` walks each session's kinds (below), reads the handle off the
supervisor's snapshot, and cancels it. A clean exit carries no error, so the run it wakes
re-establishes at once — off the cookie, holding its reason.

**Retirement.** The goroutine's `conn.Done()` branch is the same clean exit. The run it wakes goes
through the gate: `Suspend` if the replacement is not yet vouching, and the session's connection
bridge brings it back; resume otherwise.

## What the kind reports

`commitKind` stays the one publisher, and grows the SinceAt rule: the stamp moves when the
committed reason differs from the stored one, in one place. Two callers:

- **The body** — the run and the goroutine — commits reason and stamps as it learns them:
  `Syncing`/`Resyncing` before a list, `Watching` on establish and on every proof of life, `Stale`
  off the timer, `Resuming` when a resume outlasts `staleAfter`, `LastUpdateAt`/`LastLiveAt`.
- **`OnPass`** overlays what the supervisor knows onto the stored state and commits the result:
  - `Restarts` from `Failures`. With `Provisional`, that is the streak `kinds.go` counts today —
    it climbs across every death until a frame, which is what keeps it the one field that
    catches a watch flapping every thirty seconds.
  - `NextRetryAt` from `NextAttempt.ScheduledAt` **only while the last attempt `Failed`**, and
    zero otherwise: a healthy stream has a liveness re-check scheduled, and the seam promises the
    field is zero while a stream is up.
  - The reason, when the attempt outranks the body: `NoConnection`/`IdentityMismatch` from a
    `Suspended` attempt; `SyncFailed` with the error from a `Failed` one, **except
    `reasonWatchRotated`**, which overlays nothing. A `Succeeded` attempt overlays nothing on the
    reason either: the body's answer stands, which is what keeps a resume holding `Watching`.

`KindState`'s fields do not change, and one value does: **a kind parked at the gate reads
`Restarts` 0.** `Suspend` ends a streak the way `Succeeded` does, where today's refusal callback
clears only `NextRetryAt`. Taken as the better answer — nothing is retrying at the gate, and the
count would be of a streak the connection's return does not continue — and no test asserts the
old one. Otherwise what changes is that `Restarts` and `NextRetryAt` are read off the supervisor
rather than counted by hand, and `SinceAt` is stamped in `commitKind` rather than by each writer.

## The session

- **Two supervisors**, both started by `Service.Start`, both closed by `Close`. `New` registers the
  three discovery probes on one and `mirror` on the other.
- **A session's kind subjects are `s.tracked[cacheID]`.** The supervisor has no subject listing,
  and none is added: the set of kinds a cache should mirror already lives there, under `s.mu`,
  and it is exactly the set that is `Add`ed when the cache arms. `arm` `Add`s each; `TrackKind`
  on an armed cache `Add`s; a rename under an unchanged plural is the `ForgetKind` sequence
  below followed by `Add`, since the handle and any run in flight were started with the whole
  value and the rows are keyed by the singular — a run left running would keep writing rows
  under the old one; `ForgetKind` `Remove`s. The three places that walk
  a session's kinds — `close`, the bridge, `RestartAll` — walk that map. The session holds no
  per-kind worker, and `kindStates` stays what it is: the published answers.
- **The connection bridge wakes both.** `wakeDiscoverySweepOnConnectionChange` becomes the
  session's one connection bridge: on a frame where the pool's answer moved, it wakes the discovery
  probes and every kind subject under the session. The "only when the reason moved" guard stays,
  applied once per session — the pool's answer is one fact for every kind — so a suspended cache of
  three hundred kinds is not polled per frame.
- **Runs are counted for both supervisors, and so are the goroutines.** `enterSweep`/`leaveSweep`
  become `enterRun`/`leaveRun`; the goroutine joins the same group from inside its run. `close`
  waits on it.
- **`ForgetKind` is cancel and join, per kind — the shape the seam promises, kept.** Under
  `armMu`: withdraw the kind from `s.tracked`, drop its `kindStates` entry, `Remove` the subject
  (which discards the standing handle: cancel and join the goroutine), and cancel the run in
  flight off `inFlight`. Then **release `armMu` and join the run** on its `done`. Two joins,
  placed differently on purpose:
  - The **goroutine's join stays under `armMu`**, inside `Remove`. Its tail is a delta or a
    prune waiting on the store's single writer — bounded, and exactly what `stopKind` holds
    `armMu` across today.
  - The **run's join is outside `armMu`**. Its tail before the cancel was a whole cold list;
    after it, one page request unwinding. `armMu` is the Service's, so a join of any length under
    it stalls arming and forgetting on every cache, and the run's is the one that could be long.
    It is per kind, so it never waits on another kind's list.

  `commitKind` writes only for a kind still in `s.tracked` — checked under the `s.mu` it already
  takes — so a run unwinding after its withdrawal cannot write a state back, and nothing has to
  be ordered after the join. The rename path in `TrackKind` runs the same sequence before its
  `Add`.
- **The `s.mu` rule.** `Discard` joins a goroutine whose exit path commits through `commitKind`,
  which takes `s.mu`. So every walk over `s.tracked` that ends in a `Remove` or a cancel —
  `close`, the bridge, `RestartAll` — snapshots the set under `s.mu` and acts outside it, as
  `arm` already does. Stated beside the `e.mu` rule because it is the lock that deadlocks first.
- **`close`** removes the session's subjects from both supervisors (each kind subject's `Remove`
  joins its goroutine), cancels, waits, and releases the two claims — the order it has now.
- **`awaitConn`, `untilRetired`, and the `worker` type go.** `untilRetired`'s job — bounding by
  `conn.Done()` — moves into the mirror's entry for the run and into the goroutine's select for
  the stream. `kubeconn.AwaitConnFor` loses the `refused` callback it gained for the gate; nothing
  else calls it with one.

## What goes away

`worker` and the worker map, `kindSync.loop`, `syncKindFn`/`withSyncKindFn`/`kindRun.Prev`,
`pacing.backoff` and `pacing.coldLists`, `awaitConn`, `untilRetired`, `AwaitConnFor`'s `refused`,
`KindState.setReason`, and the comment that the body paces its own retries because the worker
re-enters it.

## Rules

- **A run is short.** It holds a supervisor worker through one establishment and returns. Nothing in
  a run waits for a connection, a wake, or a stream.
- **The goroutine's exit is a `Wake`, always.** There is no other way a stream's end reaches the
  schedule, and a `Wake` is never lost — the queue redelivers one that lands mid-run.
- **The run that observes a death starts nothing.** Decision 2; the `observed` bit is the whole
  mechanism.
- **Only a frame ends a streak.** Decision 3: establishment is `Provisional`, and the plain
  `Succeeded` is recorded by the run the first frame wakes.
- **`ForgetKind` cancels before it joins** — the goroutine through `Remove`, the run in flight
  through `inFlight` — and joins the run outside `armMu`. A rename does the same.
- **A handle is handed back exactly once**, by `Discard`, and `Discard` joins. Nothing else cancels
  a goroutine except `RestartAll` and retirement, both of which leave the handle to be found dead
  and clean by the next run.
- **Nothing syncs into a cache whose connection does not vouch for its `ServerUID`** — unchanged,
  now enforced by `Suspend` rather than by blocking.
- **A resume holds its reason** — unchanged; a `Succeeded` attempt overlays nothing on it, and
  neither does a rotation.

## Tests

`kinds_test.go` pins the behaviour and most of it survives: every test that drives a mirror
through `newMirroringService` and reads `awaitKindReason` is unchanged in what it asserts. What
moves:

- **The harness** arms through the supervisor. `newMirroringService` registers the real mirror;
  `newTestService`'s default substitute becomes a mirror body that `Suspend`s (so arming tests see
  a tracked kind that never writes), registered through an option that replaces `withSyncKindFn`.
- **`TestAColdListWaitingOnTheGateGivesUpWithItsRun`** becomes a test that a run queued behind the
  worker cap never lists once its session is gone — `WithWorkers(1)` on the test supervisor, one
  run parked. A queued run is a key in `runQ`, not a goroutine, so it has no context to give up
  with: what ends it is `enterRun` refusing it for a stopping session when it is finally
  dispatched (→ `Skip`), and `close`'s `Remove` dropping the subject so it is never dispatched at
  all. The test asserts the parked kind is never listed and that `close` returns without it.
- **`TestAStreamThatSettlesBetweenClosuresRetriesAtTheFloor`** keeps its assertion — three
  settle-then-close cycles, each delay under twice the floor — and moves its setup from
  `pacing.backoff` to `WithBackoff` on the test registration. It is the sharpest pin on
  Decision 3, and it passes on the new flow: frame → `Succeeded` → `Failures` 0; close →
  `Fail(reasonWatchRotated)` → the floor.
- **`TestTheRetryStreakClearsOnAFrameRatherThanTheOpen`** and
  **`TestARotatedStreamIsRebuiltWithoutReportingAFailure`** stay as they are and are the two that
  pin Decision 3 and the rotation class: `Restarts >= 2` across two closures with nothing between,
  cleared by a frame; `Watching` held with a countdown under it across a rotation.
- **New:** a stream that dies is re-established after one rung, not at once (a fake watcher that
  closes on open must climb the ladder, not spin); `RestartAll` re-establishes without a rung;
  `Remove` hands the handle back (the goroutine is joined before `ForgetKind` returns);
  `ForgetKind` during a cold list returns promptly — the list is cancelled, not waited out — and
  a `TrackKind` on another cache is not held behind it; a `ForgetKind` landing between a run's
  dispatch and its first page writes no rows (the admission is one critical section, so the run
  is either refused or cancelled); a rename under an unchanged plural cancels the old run before
  the new one lists; a cold list cut by a retirement fails and
  is retried over the replacement;
  `TestAKindParkedAtTheGateReportsWhyItWaits` and
  `TestAConnectionRetiredUnderAMirrorIsReplacedRatherThanRetried` stay as they are and pass
  through the new path.
- **Supervisor:** `Remove` and `Close` call `Discard` on a standing value; a run in flight against a
  removed subject still discards its own; `Provisional` leaves `Failures` standing, a following
  `Fail` climbs from there, and a following plain `Succeeded` clears it.

No magic sleeps: the ladder is shrunk through `WithBackoff` on the test registration, and every
wait is on a reason, a probe, or a channel.

## Build order

Each step is one red/green cycle and one commit.

1. **The supervisor**: `Remove`/`Close` discard (with `spec.discard`), `Provisional`, the two doc
   sentences, the ADR.
2. **The mirror on the supervisor**: `kinds.go` split into the run and the goroutine, the handle,
   `Discard`, `reasonWatchRotated`, registered on a `mirrorSupervisor`; the session arms through
   `Add`/`Remove` over `s.tracked`, `ForgetKind` and the rename cancel the run in flight off
   `inFlight` and join it outside `armMu`, `commitKind` refuses an untracked kind, and
   `RestartAll` cancels handles; `kinds_test.go` moved onto the new harness. **`enterRun` lands
   here**, not in step 3: the one-acquisition admission is what closes the `ForgetKind` window,
   so it cannot wait for the rename — discovery keeps `enterSweep` until step 3 folds the two. The `worker` type, `awaitConn`, and `untilRetired` go here — nothing runs them once
   the body they ran is gone, and a step that kept both paths would arm every kind twice.
3. **The reporting**: `OnPass` overlay into `commitKind`, the SinceAt rule, the bridge waking both
   supervisors, `enterSweep` folded into `enterRun` with the goroutine counted; the `refused`
   callback deleted from `AwaitConnFor`.
4. **Docs**: `sidecar/CLAUDE.md`'s "The mirror" section and the kubesync-seam spec's internal
   shape, then this spec deleted.

## Not in this pass

- **Moving the stale timer onto the schedule.** A `Succeeded().RequeueAfter(staleAfter)` could
  replace the goroutine's timer, but the timer is exact and the schedule is not, and the reason
  belongs to the goroutine that has the stream.
- **A shared "supervised value" helper in `supervisor`.** Decision 4; revisit at the second consumer.
- **A subject listing on the supervisor.** `s.tracked` is the listing; add one only if a second
  consumer needs to enumerate without a map of its own.
- **Anything above the seam.** `clustersvc` still does not call `TrackDiscovery`; step 4 of the
  kubesync seam is unchanged by this.

## Done when

`kinds_test.go` and `session_test.go` are green on the new path with the assertions they have
now, plus the new ones above; `grep worker session.go` finds nothing; `kubeconn.AwaitConnFor` has
three parameters again; and a kind's `Restarts`/`NextRetryAt` on the seam are the supervisor's
`Failures`/`ScheduledAt` for that subject.
