---
title: Supervisor jobs and workers
scope: sidecar
status: Planned
---

# Supervisor jobs and workers

## Goal

Teach `sidecar/internal/supervisor` a second kind of thing to run.

Today it runs **reconcilers**: a body is called, does its work, returns a verdict, and is called
again later. That fits a probe or a sweep. It does not fit a watch stream, which has to keep
running after the call returns. The kind sync (`kubesync/kinds.go`) makes it fit anyway — a run
starts a goroutine, commits a handle to it, and the goroutine wakes the supervisor whenever
something happens — and pays for it with `Provisional`, `proven`, `deathRecorded`,
`standingVerdict`, `reasonWatchRotated`, a 10-minute fake interval, an atomic pointer to the
pending stream, and a `commitKind` path that keeps the stream's real state outside the supervisor.

After this spec the supervisor runs two kinds of thing:

- A **job** runs, returns, and is quiet until it is due again.
- A **worker** starts, runs until it is stopped, and reports while it runs.

The kind sync becomes a worker and the list above is deleted.

## The two kinds in one sentence each

**A job's lifetime is the call.** The supervisor calls `Run`; when `Run` returns, the job is over
and its verdict says when to call it again.

**A worker's lifetime is the supervisor's.** The supervisor calls `Run` and expects it to block;
when `Run` returns, the worker has *stopped* — either because the supervisor cancelled it or
because it died — and the verdict says how to start it again.

A rule of thumb for picking: if the work has a natural end, it is a job. If it would need a
goroutine that outlives the call, it is a worker.

## The shared surface

Both kinds have the same method and return the same `Result`; each takes its own pass:

```go
type Job[T any]    interface { Run(ctx context.Context, pass *JobPass[T]) Result }
type Worker[T any] interface { Run(ctx context.Context, pass *WorkerPass[T]) Result }

supervisor.RegisterJob(e, "probe", connectionProbe{...}, supervisor.WithInterval(30*time.Second))
supervisor.RegisterWorker(e, "sync", kindSync{...})
```

`Reconciler`, `Register`, and `Reconcile` are renamed to `Job`, `RegisterJob`, and `Run`; `Pass`
becomes `JobPass`. Nothing else about jobs changes.

The two passes share a core — `Subject`, `Prev`, `Known`, `Snapshot` — as an unexported embedded
struct, and differ in what the body can do with it:

```go
// JobPass: Commit is buffered and applied when Run returns.
func (p *JobPass[T]) Commit(v T)

// WorkerPass: Commit is applied at once. Ready says the starting phase is over.
func (p *WorkerPass[T]) Commit(v T)
func (p *WorkerPass[T]) Ready()
```

Two types rather than one so the differences are the compiler's to enforce: `Ready` on a job is
not a method, and a worker handed to `RegisterJob` does not build. `NewJobPass` and
`NewWorkerPass` stand in for the supervisor in a body's own tests.

`Result` stays one type. The verdicts are unchanged — `Succeeded`, `Fail`, `Suspend`, `Skip` —
and so are their constructors; what differs is what the supervisor does with each one, which the
next two sections spell out, and that is the supervisor's business rather than the value's.

The rule behind both choices: **a type splits when the code holding it should see a different
API, not when the supervisor treats it differently.** The pass splits because a body can call
`Ready` on one and not the other. `Result` does not because a body builds both the same way. The
observation splits (§Reading a worker's observation) because a reader judges the two differently.

## What a job can do that a worker cannot

| A job can… | A worker cannot, because… |
| --- | --- |
| **Go quiet.** Return `Succeeded` and not be called again until `WithInterval` has passed (or sooner, via `RequeueAfter`). | A worker's `Succeeded` means "I stopped cleanly — start me again". `WithInterval` is the gap before the restart, not a rest (§Pacing), and `RequeueAfter` shortens it the same way. |
| **Commit atomically with its verdict.** `Commit` is buffered and applied when `Run` returns, in the same critical section as the attempt. Nothing is visible mid-run; a `Skip` or a panic commits nothing. | A worker's `Commit` is applied the moment it is called. There is no "end of the run" to wait for. |
| **Have its whole run bounded by `WithTimeout`** (30s unless set). | For a worker the timeout bounds only the time until `Ready`, and defaults to none. After `Ready` it runs as long as it likes. |
| **Hand values back through `Discard`.** A job whose value owns something (kubeconn's connection) gets it back when the supervisor drops it. | A worker's value must own nothing. Its commits are live, and one can be refused with no hand-back — the subject was removed while it ran. |
| **Be stopped by a dependency failing.** A job that cannot run is not dispatched, and its next pass is `DependencyFailed`. | A dependency gates a worker's *start*; one that fails while the worker runs leaves it alone (§Workers in the graph). |

## What a worker can do that a job cannot

| A worker can… | A job cannot, because… |
| --- | --- |
| **Run indefinitely.** `Run` blocks until its `ctx` is cancelled or it dies. | A job that never returns is a hung job; its timeout ends it. |
| **Report while it runs.** Every `pass.Commit(v)` is applied at once and asks for a pass. | A job's commits land only when it returns. |
| **Call `pass.Ready()`.** This releases the run's slot and resets the failure streak while `Run` is still going. | A job is "ready" when it returns, so it has nothing to release early. `JobPass` has no `Ready`. |
| **Be read while running.** `InFlight` plus `Ready()` is the worker's health; a snapshot taken mid-run shows its live commits. | A job's run is invisible until it returns. |

## What both can do

- Read the pass: which subject, the last committed value, the snapshot of every sibling.
- Return any of the four verdicts. `Fail` climbs the backoff ladder; `Suspend` parks until a
  `Wake`; `Skip` records nothing and parks until a `Wake`.
- Declare `WithDependencies` and `WithWatches`, and be the far end of either.
- Be stopped. `Restart` cancels a running job or worker and runs it again; `Remove` and `Close`
  cancel it and wait for it to return. A stopped run records nothing.
- Be read through `Read`/`Snapshot`, and be published through `OnPass`.

## A worker's life, step by step

1. **Dispatch.** The dispatcher takes a slot and calls `Run` on a goroutine of its own.
   `NextAttempt.StartedAt` is set; `InFlight` is true.
2. **Starting.** The worker does whatever must happen before it is useful — a cold list, an
   open. It holds the slot. `WithTimeout`, if set, bounds this phase. It may `Commit` here; each
   commit is visible at once.
3. **`Ready()`.** The worker has proof it is up — for a stream, the first frame. The supervisor
   releases the slot, stops the startup timer, sets `Failures` to zero, and stamps
   `NextAttempt.ReadyAt`. Calling `Ready` twice is harmless.
4. **Running.** No timeout. The worker commits whenever its value changes.
5. **Exit.** `Run` returns. The supervisor records the attempt and decides what happens next:

| `Run` returned… | …after `Ready` | …before `Ready` |
| --- | --- | --- |
| `Succeeded` | recorded as `Succeeded`; restarted after the interval floor | recorded as **`Failed` / `NeverReady`**; restarted up the ladder |
| `Fail` | recorded as `Failed`; restarted up the ladder | same |
| `Suspend` | recorded as `Suspended`; parked until a `Wake` | same |
| `Skip` | nothing recorded; parked until a `Wake` | same |
| anything, after a `Restart`, `Remove`, or `Close` cancelled it | **nothing recorded**; the streak stands. Run again now after a `Restart`, gone after a `Remove`/`Close` | same |

The same last row holds for a job: a stopped job records nothing too.

Three rows carry the design:

- **`NeverReady`** is what keeps a worker that dies on startup from hot-looping. A server that
  closes every watch the moment it opens returns `Succeeded` each time, and without this rule
  each one would restart at once. With it, "you never proved you were up" is a failure, the
  ladder paces it, and the first `Ready` resets the count. This is the rule `Provisional`,
  `proven`, and `deathRecorded` implement by hand today. It is also wire-visible: a watch that
  closes before its first frame is a `Failed` record, so the kind reads `SyncFailed` with the
  server's message, where today `reasonWatchRotated` hides it. That is the better answer.
- **A stop records nothing.** A stop asked for by `Restart`, `Remove`, or `Close` is not the
  worker's doing, so it is not an attempt: `LastAttempt` and `Failures` stand as they were. This
  matters because `Failures` is the ladder's rung, and a poke restarts every kind on a cache at
  once — one that reset the streak would have a whole cache retry a
  struggling server at the base delay. A worker stopped before `Ready` is therefore not
  `NeverReady` either. **Only those three are stops.** The startup timeout is not one: it cancels
  the `ctx` the same way, but the worker's return is recorded as the rows above say — a
  `Succeeded` becomes `NeverReady`, a zero `Result` is `Internal`, as for a job — so a worker that
  hangs in startup climbs the ladder and is visible. The supervisor keeps a *stopped* flag that
  only the three set; the timer never sets it.
- **`Skip` is "not my failure".** A connection retired under a cold list is nobody's fault and
  nothing to report; the worker returns `Skip` and parks. What un-parks it is whoever knows
  why it stopped — for the kind sync, the session's connection bridge, which already wakes every
  kind when the connection changes.

`Failures` for a worker therefore counts *its own failed exits since it was last ready*: the retry
streak while it is down, which is what the ladder climbs. It says nothing about a worker that
flaps while healthy — that is `Restarts` (§Bookkeeping additions).

## Pacing

A worker has two paces, and `Fail` uses the one jobs use:

- **The ladder** paces failures — `Fail`, and `NeverReady`. `WithBackoff`, as today.
- **The floor** paces clean restarts. A worker's `WithInterval` is the gap between a
  `Succeeded` exit and the next start; unset, it is the backoff base (1s). Options apply in
  order, so the default is resolved *after* they run — a zero interval on a worker means unset,
  and is filled from whatever `WithBackoff` left — rather than read off a base a later option
  may replace.

Both are `due()`'s existing `VerdictSucceeded` branch, unchanged: `FinishedAt + interval`, or
less if the run asked for less with `RequeueAfter`. A worker that knows the wait is pointless — it
rotated on purpose, or its watch ran for an hour before closing — shortens it the way a job does.
The floor is the default for a worker that says nothing, which is what paces a server that closes
watches after one bookmark; it is not a guard against the body, which the supervisor trusts
everywhere else. A `Wake` and a `Restart` go under it too, as they go under a suspension.

The floor is what stops a watch rotation being free. A server that accepts a watch, sends one
bookmark, and closes it would otherwise reopen in a tight loop with `Watching` showing the whole
time, since the first frame reset the ladder. Today the same case costs one rung; the floor is
the same number by a plainer route.

## `Wake` and `Restart`

Two ways to ask for a run, and both mean the same for both kinds.

**`Wake`: run again, after the current run has finished.** A run already going may have read the
thing that changed before it changed, so a wake is answered by a run that starts after it. The
run in flight is left alone. This is today's `Wake`, unchanged, and it comes from one queue rule:
the run queue holds one key per (subject, registration); a key added while it is being run is
marked dirty rather than dispatched twice; the dirty key is redelivered when the current run
ends. So `Run` is never in flight twice for one key, several wakes during one run collapse into
one follow-up, and a wake that lands as a run is exiting is never lost.

1. `Wake` adds the key; it is already held, so it is marked dirty.
2. The current `Run` finishes on its own — a job when it is done, a worker when it next exits.
3. Its exit is recorded and the dirty key is redelivered.
4. `Run` is called again as soon as a slot is free, suspension and floor notwithstanding, with
   `Prev` set to what the last run committed.

For a worker that is running, then, a `Wake` means "when you next stop, start again at once" —
it un-parks a `Suspend` the worker was about to enter, and it makes a rotation's restart
immediate. A parked worker is started. A live worker is never torn down by a `Wake`, which is
what lets the kind sync's connection bridge call `wakeAll` on every state frame, as it does
today: parked kinds start, live ones are left alone.

**`Restart`: stop the current run, then run again.** The same queue steps, with one addition —
the run's `ctx` is cancelled and the *stopped* flag set, so the exit records nothing and the
redelivery is immediate. A job mid-dial against a machine that just woke, or a worker streaming
over a connection that just changed, is not left to time out or to notice on its own. This is
what `session.restart` (the resume poke) needs; today it reaches into
`committedStream`/`pendingStream` to cancel by hand.

**Neither waits.** `session.restart` walks every kind on a cache: three hundred cancels are one
poke, three hundred joins of unwinding cold lists on the poke consumer's goroutine are not. And
a call that cannot block is one that is safe to make from anywhere, a lock held or not.

**`Remove` and `Close` cancel and wait**, for both kinds, outside the lock. When they return, no
run is in flight against the subject: a job's value can be handed back through `Discard` with
nothing still using it, and a worker's goroutine is gone. A cancelled job unwinds as fast as a
cancelled worker — every body already returns on its `ctx`, since the timeout uses it — so the
wait is short. Because they join, neither may be called from inside a `Run`; no body does.

One race is accepted for `Restart`: one that lands while the run is already returning `Fail`
sets the stopped flag first, so the failure is not recorded and the restart is immediate rather
than laddered. A caller that restarts often enough can hold the streak at zero; the kind sync's
only restart is the poke, so nothing does.

`WakeAll` gets a `RestartAll` counterpart.

## Workers in the graph

The two edges keep their meanings — a dependency answers "can this run?", a watch answers "is
this answer stale?" — and each direction costs little:

- **A worker that depends on a job** is not started until the job has succeeded. `due()` and
  dispatch already check this before a run starts, so a worker registered
  `WithDependencies("catalog")` waits for the catalog's first run exactly as a job would, and is
  recorded `DependencyFailed` and re-armed on recovery by the same code. **A dependency gates the
  start only.** One that fails while the worker runs does not stop it — it is checked again at
  the worker's next start. The kind sync already handles its runtime dependency this way: it
  watches `conn.Done()` itself and exits on its own.
- **A worker that watches a job** is restarted when the watched value moves: the watch edge is
  a `Restart` for a worker where it is a `Wake` for a job, since a worker's input moving means
  the one it is running on is stale. The cancel is non-blocking, so making it from `commit`
  under the lock is fine.
- **A job that watches a worker** runs when the worker commits. A live commit walks the same
  watcher list a job's commit does.
- **A job that depends on a worker** reads the worker's health, which `dependenciesOf` has to
  learn to judge: `Ready()` is OK; starting is unanswered; parked, or a last exit that failed,
  is failing. `Ready` asks for a pass so dependents are scheduled when the worker comes up.

## Live commits and `OnPass`

A worker's `Commit` writes the value and `ChangedAt` under the supervisor's lock and asks for a
pass, exactly as a job's commit does when the run returns — so `OnPass` fires for it from the
pass loop, serialized, as every publication is today. `OnPass` keeps its name and its meaning: it
fires on every pass, schedule-only passes included, because kubeconn's next-probe countdown is
visible nowhere else.

**Commit when the verdict moved, not when a frame arrived.** Every commit takes the process-wide
lock and fires a pass. A worker that committed per delta would run `publishKind` for every object
on a busy cluster. So the kind worker's `T` is the *verdict* — `Reason`, `Message`, `SinceAt` —
and it commits only when `setReason` moved it. The two liveness stamps, `LastUpdateAt` and
`LastLiveAt`, move on every frame and stay out of the supervisor: the session keeps them in a
small per-kind map under `Service.mu`, written per frame as `commitKind` writes today, and read
by `GetKindState`. What leaves the map is the verdict, which the supervisor now holds.

### Reading a worker's observation

A job's value is a fact and a worker's is a status, and a reader judges them differently. So
`Observation` becomes two types, read through `GetJobObservation[T]` and
`GetWorkerObservation[T]` — each panics on a name registered as the other kind, as `Get` panics
today on the wrong `T`:

```go
type JobObservation[T any] struct {   // today's Observation, renamed
    Value      T
    LastSeenAt time.Time              // when a run last confirmed Value
    Attempts
}

type WorkerObservation[T any] struct {
    Value     T
    ChangedAt time.Time               // when the worker last committed
    Attempts
}

// Live reports that Value is current: the worker is running and has called Ready.
func (o WorkerObservation[T]) Live() bool
```

The two differences the split enforces:

- **`ChangedAt`, not `LastSeenAt`.** A job confirms its value by running again; a worker confirms
  it by still running. A live worker that has not committed for an hour has a current value and
  an hour-old stamp, so the stamp is named for what it is — when the value last changed — and
  freshness is `Live()`.
- **`Live()`, because the value does not outlive the worker.** A job's `identified, as of 10:00`
  still holds after the read that follows it fails; a worker's `Watching` is false the moment
  the worker exits. After an exit, `Value` is the last verdict before it and `Attempts` says how
  it ended. `publishKind`'s outranking rule is this join for the kind sync; `Live()` is it for
  everyone.

`Known()` means the same on both: a value has been committed.

`GetKindState` is computed at read: the subject's snapshot (`Value` for the verdict, `Attempts`
for `Restarts` and `NextRetryAt`, the outranking rule from `publishKind`) joined with the stamps.

**`KindState.Restarts` changes meaning.** Today it is `Attempts.Failures`: the retry streak while
the kind is down. After this spec it is `Attempts.Restarts`: how many times the stream has come
back within the current healthy stretch, which is what its doc already says it is for. The retry
streak stays readable as a non-zero `NextRetryAt`. Wire-visible, like `NeverReady`.
Nothing stores the overlay.

`publishKind` decides whether the record is woken, and for that it needs a baseline it no longer
has: today it compares against the reason stored in `sess.kindStates`, and `OnPass` fires on
every pass, schedule-only ones included. So the session keeps a last-published reason per kind —
`published map[kindID]Reason`, what kubeconn's `Service.published` is for — and `publishKind`
wakes the record only when the reason it derives differs from it. Without this, every pass wakes
every record.

## Mechanics

This is the part of the supervisor that changes. Today the slot *is* a `runLoop` goroutine:
`WithWorkers(n)` starts n of them, each takes a key, runs the body synchronously, and calls
`Done`. A blocking `Run` would pin one forever and hold its key with it.

**One dispatcher, a slot semaphore, a goroutine per run.**

```
runQ ──▶ dispatcher ──acquire slot──▶ go run(k)
                                         job:    body → release slot → record → Done(k)
                                         worker: body … Ready → release slot … exit → record → Done(k)
```

- `WithWorkers(n)` is renamed **`WithStartConcurrency(n)`** — "worker" now means something else,
  and what it bounds is starts, not runs — and sizes a semaphore of n slots. A job is starting
  for its whole run; a worker only until `Ready`, after which it holds no slot. So eight is
  eight cold lists however many streams are up. The dispatcher acquires a slot before starting
  a run and blocks when none is
  free: the run queue is ordered, so this is the gate. The acquire selects on the loop's
  context as well — a dispatcher blocked on a full semaphore is not blocked on `runQ.Next`, and
  would not otherwise see `Close` or the stop. A job releases when it returns; a worker at
  `Ready` or return, whichever is first. `WithStartConcurrency(8)` on the kind supervisor means eight cold
  lists at once, however many kinds are streaming.

  **It panics below one**, as `WithWorkers` does. A semaphore of zero admits nothing: every
  subject queues and no run is ever dispatched, silently — the one failure a caller cannot read
  off anything the supervisor reports. A settings struct built without the field is a table
  wired wrong at boot, which this package refuses rather than papers over.
- **The key is held for the whole run.** `Done(k)` is called when `Run` returns, for both kinds.
  That is what makes a wake mid-run a dirty mark and a redelivery rather than a second `Run`
  beside the live one, and it needs no separate "restart" path.
- **A running run is reachable.** The subject's observable holds the run's `cancel` and `done`
  while it runs, for both kinds. `Restart` calls `cancel` and sets the stopped flag; `Remove`
  and `Close` do the same and then wait on `done` outside the lock, after which the exit finds
  the subject gone, records nothing, and hands a job's value back.
- **The startup timeout** is a `time.AfterFunc` that cancels the run's `ctx`, stopped by `Ready`.
  A worker's registration defaults it to zero; a job's stays at 30s.
- **`Ready` and a live `Commit`** are calls back into the supervisor, so `WorkerPass` carries an
  unexported hook the supervisor sets at dispatch. `NewWorkerPass` (the body-test constructor)
  sets none: on a test-built pass `Ready` records itself and `Commit` buffers, readable through
  `pass.IsReady()` and `Updated()`.
- `pass()` keeps skipping in-flight registrations when it re-derives a schedule, so a timer can
  never schedule over a running worker; the exit's record is what re-enters the schedule.

## Bookkeeping additions

```go
type Attempt struct {
    ScheduledAt, StartedAt time.Time
    ReadyAt                time.Time // workers: when Ready was called; zero for a job
    FinishedAt             time.Time
    ...
}

type Attempts struct {
    ...
    Failures     int
    FailingSince time.Time
    HealthySince time.Time // when the current healthy stretch began; zero while not healthy
    Restarts     int       // runs started since HealthySince began; zero while not healthy
}

// Ready reports a worker that is running and has called Ready. Always false for a job.
func (a Attempts) Ready() bool

const ReasonNeverReady Reason = "NeverReady" // a worker exited before Ready
```

`HealthySince` is `FailingSince`'s mirror, and answers what `ReadyAt` cannot: a rotation is a
`Succeeded` exit and a fresh `ReadyAt`, so "this watch has been up for a minute" is readable
and "this kind has been healthy for an hour" is not. It is set, if zero, when health begins — at
`Ready` for a worker, at a `Succeeded` record for a job — and cleared by a `Failed` or
`Suspended` record. A stop records nothing, so it stands across a `Restart`; a `NeverReady` is a
`Failed` record, so it clears. Same rule for both kinds: a job reads "succeeding since".

`Restarts` is what `HealthySince` makes countable: it is set to zero when `HealthySince` is set
fresh, and goes up by one at every dispatch after that. A clean rotation and a `Restart` both
count — both are the stream going down and coming back, whoever asked — and a `Failed` record
clears it along with `HealthySince`. "Healthy for an hour, restarted 120 times" is then one read,
which is the flapping question `Failures` cannot answer: a watch that rotates every thirty
seconds reads `Watching` whenever it is looked at, and `Failures` stays at zero through every
rotation. For a job it counts the runs in the current succeeding stretch, which is harmless.

`Attempts` stays one type for both kinds: it is the supervisor's ledger, written by the one
scheduler, and the only members a job never uses — `ReadyAt` and `Ready()` — are zero and false
there and say so. `ReadyAt`, `HealthySince`, and `Restarts` are exported and so part of the
snapshot contract, like the other stamps. `OK()` keeps its meaning (the last *finished* attempt succeeded); a running worker's
health is `Ready()`. `Provisional` and the `provisional` field go.

## Use cases

### The connection probe — a job that owns something

`kubeconn.connectionProbe` dials a kube-context, checks who answers, and commits the connection
as its value. It is due again every thirty seconds and bounded by a ten-second timeout. When the
supervisor drops the value — the subject is removed, or a later run's commit replaces it — the
probe gets it back through `Discard` and retires it.

Every one of those is a job property: a natural end, a cadence, a whole-run timeout, and a value
that must be handed back. Unchanged by this spec except `Register` → `RegisterJob` and
`Reconcile` → `Run`.

### The discovery sweep — jobs wired to each other

Three jobs per cache: `apiVersions` and `apiGroups` GET one document each; `resources` fans out
over the group-versions `apiGroups` committed and writes the kind catalog. `resources` declares
`WithWatches("apiGroups")`, so it runs again whenever the group list moves, and
`WithDependencies` on both, so a cluster that cannot answer `/api` costs one `DependencyFailed`
record instead of a fan-out of timeouts.

Also jobs: each has an answer, commits it, and is done. Unchanged by this spec.

### The kind sync — a worker

One worker per (cache, kind), on its own supervisor with `WithStartConcurrency`; it declares no edges,
since its dependency is the connection, which it gates on itself. Its `Run`:

```go
func (w kindSync) Run(ctx context.Context, pass *supervisor.WorkerPass[KindVerdict]) supervisor.Result {
    conn, err := w.connFor(ctx)                  // the identity gate
    if err != nil {
        return supervisor.Suspend(connectionReason(err, ReasonSyncFailed), err.Error())
    }
    watcher, err := w.open(ctx, conn, pass)      // cold list, or resume from the cookie; holds the slot
    if err != nil {
        select {
        case <-conn.Done():
            return supervisor.Skip()             // retired under the list: not this kind's failure
        default:
            return supervisor.Fail(ReasonSyncFailed, err)
        }
    }
    return w.applyDeltas(ctx, watcher, pass)     // Ready() on the first frame; Commit when the reason moves
}
```

`applyDeltas` returns `Succeeded` when the apiserver closes the watch (a rotation), `Skip` when
the connection is retired under it, and `Fail` on anything else. The tables do the rest: a
rotation after the first frame restarts after the floor and stays `Watching`; a watch that closes
before any frame is `NeverReady` and climbs the ladder; a `410 Gone` fails, and the next start
relists.

**The relist intent is the session's, not the worker's `T`.** A `410` answered by the watch
*open* establishes nothing, so it commits nothing — and the two ordinary ways in carry no value
to read either: a reopen after a clean stop, and a cold start off a cookie on disk, where nothing
has ever committed. Decided from `T` alone, the next run resumes from the same dead cookie and the
kind sits at the backoff cap forever.

So the session holds a relist flag per kind, marked from **both** carriers — a dead stream's error
and a refused open — and cleared only once a cold list has landed. The last attempt's error is not
enough on its own: it holds only the most recent one, so a relist that is itself refused for an
unrelated reason loses the intent and resumes from the cookie it was replacing. The intent has to
outlast every failure between learning the position is dead and acting on it. It need not outlast
a restart: there the first resume is refused again, which marks it. The relisting run reports
`Resyncing` — the rows are served throughout.

`KindVerdict` (`Reason`, `Message`, `SinceAt`) is the worker's `T`; the stamps stay in the
session's map; `KindState` is what `GetKindState` assembles from its `WorkerObservation` and the
stamps. `DiscoveryState`'s three embedded observations are jobs' and become `JobObservation`
by rename alone.

Stopping is the supervisor's: `ForgetKind` is `Remove`, a session teardown is `Remove` per kind,
and a poke is `Restart`. The first two join; the last does not.

The worker still runs *under its session*, the way the sweep does: it enters through
`enterRun`/`leaveRun`, so `sess.runs` counts it and `session.wait` joins it before the store and
lease claims are released, and it derives its `ctx` from `sess.ctx` as well as the supervisor's.
The per-kind `Remove` is what ends it; the session's cancel is the backstop behind that, and
step 3 tests it — a teardown with a worker mid-list must not release the store under it.

`session.wakeAll` stays as it is: the connection bridge's `Wake` starts a kind parked at the
gate and leaves a live one alone, so calling it on ordinary state frames stays cheap. A worker
whose connection was retired under it has exited with `Skip` on its own and is parked, so the
bridge's next wake starts it. `session.restart` (the poke) becomes `Restart` on the sweep and
every kind — the machine slept, and whatever is running is running on a stale connection.

## What this deletes from kubesync

- `kindStream`, `kindRun`, `sess.kindRuns`, `enterKindRun`/`leaveKindRun`, `cancelKindRun`,
  `committedStream`, and `pendingStream` — the supervisor owns the lifetime. `sess.runs` stays:
  the sweep counts on it too, and the worker joins it through `enterRun`; only the stream
  goroutine's own `Add`/`Done` goes.
- `standingVerdict`, `deathRecorded`, `proven`, the first-frame `Wake`, and
  `reasonWatchRotated` — the `NeverReady` rule and the floor replace them.
- `kindSyncInterval` — the interval is the floor now, not a backstop.
- The verdict half of `commitKind` and `sess.kindStates` — the supervisor holds the verdict. The
  stamps keep a smaller map.
- `kindReconciler.Discard` — the supervisor joins the worker.

And from the supervisor: `Result.Provisional` and `Attempt.provisional`.

## Build order

1. **Supervisor.** Renames (`Job`, `RegisterJob`, `Run`, `JobPass`, `JobObservation` with
   `LastSeenAt`, `GetJobObservation`, `WithStartConcurrency`). The dispatcher and the
   semaphore (§Mechanics). `RegisterWorker`, `WorkerPass` with `Ready` and the live commit hook, `WorkerObservation` with
   `Live` and `GetWorkerObservation`, `Attempt.ReadyAt`,
   `Attempts.Ready`, `Attempts.HealthySince`, `Attempts.Restarts`, `ReasonNeverReady`, the worker floor, `Restart`/`RestartAll`, cancel-and-join
   on `Remove`/`Close`, the exit table, a worker's health in `dependenciesOf`, and the watch edge
   as a `Restart` for a worker. Remove `Provisional`. Tests: one per row of the exit table; the
   slot released at `Ready` and a job's at return; a `Wake` leaving a running worker running and
   restarting it on its next exit; a `Restart` cancelling a running job and a running worker, each
   followed by exactly one new `Run`; a `Restart` from inside a commit not deadlocking; `Remove`
   joining a job and a worker; a stopped run leaving `Failures` alone; a live commit visible in `Read`
   before the run ends; the startup timeout stopped by `Ready`, and one that fires recording
   `NeverReady`; `HealthySince` standing across a rotation and a `Restart` with `Restarts`
   counting each, and both clearing on a `Fail`; the dispatcher stopping while blocked on a full semaphore; a worker not started
   until its dependency has succeeded, and left running when it later fails; a job depending on
   a worker scheduled when it becomes ready.
2. **kubeconn and discovery.** Mechanical renames only.
3. **kubesync kind sync.** Rewrite `kinds.go` as the worker above; delete the list above; assemble
   `KindState` in `GetKindState`; add the `published` baseline to `publishKind`; make
   `session.restart` a `Restart`. New tests: a state frame leaves a live worker alone; a poke
   reopens it; a teardown mid-list joins the worker before releasing the store. Most
   `kinds_test.go` scenarios carry over with new setup —
   stale, resume, relist on `Gone` — from an `Error` frame AND from a refused open, which are
   different carriers — forget mid-list, connection retired under a list. The
   rotation scenario changes meaning: it asserts a rung today and asserts the floor and
   `Watching` after this.
4. **Docs.** Fold into `sidecar/CLAUDE.md`. Write an ADR for the job/worker split; mark
   `2026-08-28-the-stream-is-the-value.md` superseded by it and repoint its links. Update
   `2026-08-28-supervisor-vocabulary.md` for the renames.

## Open questions

- **A job calling `Ready`.** `JobPass` has no such method. If a long job ever wants to release
  its slot early, add one with the same meaning as for a worker; nothing else has to change.
- **A dependency failing under a running worker.** This spec leaves the worker running and
  re-checks at its next start. If a worker ever needs to be stopped by its dependency, that is a
  cancel from the dependency's exit record and a `pass()` that no longer skips in-flight workers;
  nothing needs it yet.
