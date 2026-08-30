---
title: Probing in flight
scope: sidecar
status: Planned
---

# Probing in flight

## Goal

`clusterScheduleWatch.probing` is `true` while the connection probe is dialing, and `false`
otherwise. Today it is always `false`.

The frontend is already built for it: `useNextCheck` in
`src/components/widgets/cluster-sync-panel.tsx` renders a "checking…" spinner when `probing` is
set and holds the countdown otherwise, and its tests push `probing: true` frames. Nothing on the
webview side changes. The sidecar just never sends the frame.

## The problem

The supervisor publishes in exactly one place: `pass`
(`sidecar/internal/supervisor/supervisor.go`). It runs on the single pass-loop goroutine, takes a
`Snapshot` under the lock, and fires `OnPass` with it. `kubeconn.Service.publish` is that
callback; it projects the snapshot into `State`, and `clusterSchedule`
(`sidecar/internal/clustersvc/clusters.go`) reads `State.Connection.InFlight()` into `probing`.

A job's life, as the pass loop sees it:

1. A pass finds the job due. It queues the run and publishes. The run has not started, so
   `NextAttempt.StartedAt` is zero — `InFlight()` is false.
2. The dispatcher picks the run up. `runOne` calls `begin`, which sets `StartedAt`. **Nothing
   asks for a pass here.** The probe dials the server.
3. The run finishes. `commit` records the attempt and asks for a pass. That pass publishes —
   with the run already finished, so `InFlight()` is false again.

For a job, every snapshot anyone sees is from step 1 or step 3. The in-flight window between
them is real — the probe's whole round-trip, up to its timeout — but no pass runs inside it.
`clusterSchedule` reads the flag correctly and a unit test pins it
(`TestClustersWatchScheduleReportsARunInFlight`); the test hands it a hand-built `State`, which is
why it passes while the live feed never carries one.

Workers are different, and that is the precedent: `markReady` and `commitLive` both
`passQ.Add` from the run's own goroutine while the run is out, so a worker's in-flight state
already reaches snapshots. Jobs just never ask.

What the user sees: the countdown reaches zero and sits at zero for as long as the probe takes,
with no sign anything is happening. The schema field exists precisely to avoid that.

## Why it is not "publish from `runOne`"

The obvious fix is to fire `OnPass` from `runOne`, right after `begin`. That runs on the run's
own goroutine, and `OnPass` promises its caller serialized delivery per subject. A begin-publish
on a run goroutine can be delayed past the commit-publish on the pass loop and land after it,
overwriting a fresh state with a stale one. `kubeconn` would then tell claim watchers a probe is
running that has already finished, and `Service.published` — its baseline for "did the news
move" — would be stale for the next real pass.

So a second publisher needs its own ordering guarantee. The supervisor already has one.

## Design

**A dial beginning asks for a pass.** In `runOne`, inside the `dependenciesOK` branch, after
the lock is dropped and before `dispatch`:

```go
case dependenciesOK:
	req.ctx = runCtx
	e.passQ.Add(k.subject)
	out = e.dispatch(sp, k, sub, h, req, release)
	ran = true
```

That is the whole supervisor change — the same move `markReady` and `commitLive` make. It
works because of three things `pass` already does:

- **It leaves a run in flight alone.** `pass` skips any registration whose `InFlight()` is true,
  so the extra pass does not reschedule the run or touch its `NextAttempt`, and an in-flight
  registration never contributes to `soonest` — so the `stopTimer`/re-arm on an extra pass
  cannot lose a wakeup. It re-derives the other registrations' schedules, which every pass does
  and which is idempotent.
- **It snapshots under the lock, at pass time.** The snapshot carries whatever is true when the
  pass runs. If the run is still out, `InFlight()` is true and that is published. If the run has
  already committed by then, the snapshot shows the finished attempt. There is no way for it to
  publish a state older than the last one it published.
- **It is serialized, and the add is never dropped.** `passLoop` is one goroutine. `passQ` is a
  `workqueue`: a subject already waiting dedupes, and one added while the loop holds it mid-pass
  goes `heldDirty` and re-enqueues on `Done`. So a begin-add always produces a pass, and
  `OnPass`'s contract — after every pass, serialized per subject — holds unchanged.
  `kubeconn.publish` needs no edit.

The two passes may coalesce. A run that finishes before the begin-add is taken has both adds
collapse into one pass, which publishes the finished state. That is correct: there was no
in-flight window long enough to show. A slow run gets two passes, and the first says in flight.

**Only the dial asks.** The other two branches of `runOne` — dependencies failing or unanswered —
record or skip without dialing anything, and `probing` means "a network probe is running"
(`Schedule.Probing`'s own doc). A begin-add there would flash `probing: true` for a run that
touched no socket. Putting the add in the `dependenciesOK` branch is what keeps the flag's
meaning literal.

**Downstream, the extra passes wake nothing.** `kubeconn.newsOf` carries no timing —
`Phase()` reads `LastAttempt` only — so `changed` never flips on a begin-pass: no fleet signal,
no cluster-record write. In `kubesync`, `recordKindReason` compares `Reason` alone, and
`kindStateOf` at begin serves the reason it last published. So the extra passes reach the
`stateHub` and nothing else.

### `nextRequeueAt` while probing

While a run is in flight, `NextAttempt.ScheduledAt` still holds the time it was dispatched at —
`begin` leaves it, and `pass` never rewrites an in-flight registration. That is not "the next
reconcile"; it is the current one. `clusterSchedule` nulls it while probing:

```go
func clusterSchedule(st kubeconn.State) Schedule {
	sched := Schedule{Probing: st.Connection.InFlight()}
	if at := st.Connection.NextAttempt.ScheduledAt; !sched.Probing && !at.IsZero() {
		sched.NextRequeueAt = &at
	}
	return sched
}
```

This matches what the frontend already assumes (`useNextCheck`: "Null `nextRequeueAt` =
reconcile in flight (`probing`) or nothing scheduled"). `WatchSchedule`'s first-frame gate
(`sched.NextRequeueAt != nil || sched.Probing`) still emits.

Two doc strings say "null when nothing is scheduled" and become wrong; both change to "null
while a probe is in flight or nothing is scheduled":

- `sidecar/graph/schema.graphqls`, `Schedule.nextRequeueAt`.
- `sidecar/internal/clustersvc/shared.go`, `Schedule.NextRequeueAt`.

### What is not changed

- **`OnPass` keeps its name and its contract.** "After every pass" stays literally true; there
  is one more pass. The name is ratified by
  [ADR: supervisor vocabulary](../adr/2026-08-28-supervisor-vocabulary.md) alongside
  `JobPass`/`WorkerPass`, and reversing that is its own change with its own ADR.
- **No second hook, no version or sequence number.** The pass loop is the serialization point.
- **The run queue's debounce** (`docs/TODO.md`) is untouched. A pass is arithmetic under the
  lock plus one snapshot clone; one extra per dial is not a cost to measure.

## Costs

**One extra `State` frame per dial.** `kubeconn`'s `stateHub` carries every pass, so each claim
watcher sees one more frame per job run, and `WatchSchedule` sends one more `Schedule` frame.
Five probes run on their own cadences (readiness every 30s), and today each of their commits
already produces a schedule frame in which the connection's schedule did not move. This doubles
that: two frames per 30s instead of one, per watched cluster. Fine.

**The frame is not guaranteed delivery.** `stateHub` is a level hub: a reader that falls behind
sees only the latest frame, so a fast probe's in-flight frame can be overwritten by its finish
before a slow reader takes it. Irrelevant for a probe that takes seconds; worth knowing before
anyone builds something that counts frames.

**`kubesync` wakes twice as often while parked.** `session.wakeOnConnectionChange` calls
`wakeAll()` on every state frame while a session is parked, so the extra frames double that
rate. Harmless — a wake is a queue add, deduped — but it is part of the accounting.

**A pre-existing duplicate-run race widens slightly.** A pass landing between `runQ.Next` and
`begin` marks the held key `heldDirty` and buys a redundant run on `Done`. That race exists today
(it is the debounce item in `docs/TODO.md`); more passes give it more chances, they do not
create it.

## Tests

**Supervisor** (`supervisor_test.go`):

- `TestOnPassFiresAfterEveryPassInOrderPerSubject` stands as is. `runNext` calls `runOne` on the
  test goroutine and only then `settle()`s, so the begin-add and the commit-add are one queued
  key before any pass runs — the count stays deterministically 2.
- That is also why the new test **cannot use `runNext`**: the in-flight pass only exists when
  the pass loop runs concurrently with the run. Either `Start` the real loops, or run `runOne`
  on its own goroutine and `settle()` from the test. The body blocks on a channel the test
  closes, `OnPass` reports into a `testutil.Probe`, and the test asserts a snapshot with
  `Attempts(id).InFlight()` true arrives while the body is held, then one with
  `LastAttempt.Done()` after release. Holding the run open is what makes the in-flight pass
  deterministic. Wait on the callback, never on the clock.
- A test that a dependency-failed run does **not** ask for a pass at begin: same shape, and
  the only snapshot seen carries `ReasonDependencyFailed` with `InFlight()` false. This pins
  the branch placement.

**clustersvc** (`clusters_test.go`): extend `TestClustersWatchScheduleReportsARunInFlight` to
assert `NextRequeueAt` is nil while probing.

**Frontend**: nothing. `cluster-sync-panel.test.tsx` already covers `probing: true` frames.

## Docs to touch when it lands

- `sidecar/CLAUDE.md`, the supervisor section: a dial beginning asks for a pass, so a job's
  `InFlight()` is visible on the feed for as long as the dial is out. The `kubeconn` publishing
  paragraph is still true as written.
- The two `nextRequeueAt` doc strings above.
- `docs/TODO.md`: delete the `probing` bullet. In the "four non-connection probes" bullet, drop
  the sentence naming this as a prerequisite — the per-probe row it describes reads `InFlight()`
  off the same snapshot and now needs nothing more from the supervisor.
- `docs/specs/README.md`: remove this spec's index row. Delete this spec.

## Related TODO items

Three neighbours in `docs/TODO.md` were weighed for bundling:

- **Per-probe detail row** ("Nothing exposes the four non-connection probes"). This spec is its
  stated prerequisite and discharges it, since every job's in-flight state now reaches a
  snapshot, not just the connection's. The row itself is a schema addition, a new subscription,
  and UI — its own spec. Not bundled.
- **`Wake`/`WakeAll` rename.** Same file, two call sites, one `CLAUDE.md` line, no design
  question left open. Worth doing in the same change since `supervisor.go` and its docs are
  already being edited, but it is independent — drop it if it grows the diff more than it is
  worth.
- **Run-queue debounce.** Unrelated in mechanism (see "Costs"). Not bundled.
