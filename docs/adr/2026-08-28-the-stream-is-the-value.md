---
title: A supervised stream is the reconciler's value, not its run
date: 2026-08-28
scope: sidecar
status: Accepted
---

# A supervised stream is the reconciler's value, not its run

## Context

`internal/supervisor` was extracted for bodies that fetch: a run requests, classifies, and returns,
and [its `Result` is its schedule](2026-08-24-probe-engine.md). Every body it had fit — `kubeconn`'s
five reads and `kubesync`'s three discovery reads all answer and end.

A kind's sync does not. It cold-lists a collection, then holds a WATCH open for hours, applying
deltas. `kubesync` ran each on its own goroutine with a hand-written retry loop, which meant a
second backoff ladder, a second retry countdown, a second identity gate and a second fleet-wide
cap living beside the supervisor's — for the same job, paced differently, reported differently.

## Decision

The kind sync runs on the supervisor, and **the stream is the reconciler's value rather than its run.**
A run establishes the stream — gate, cold list or resume, open the WATCH — commits the handle, and
returns. The goroutine outlives it as the committed value, which the supervisor already holds and
hands back.

Two consequences make it work, and both are the supervisor's:

**`Remove` and `Close` hand back the standing value.** Dropping a subject used to leave its value
where it was, which was safe while a value owned a connection the pool retires itself. A value
owning a goroutine leaks unless the reconciler is told, so the invariant is now *the supervisor
hands back every value it stops holding* — `Discard` on both paths, outside the lock, since a
`Discard` that joins a goroutine can wait on an exit that calls `Wake`.

**`Provisional`, a modifier on a succeeded `Result`: the streak stands.** `Succeeded` ends a
failure streak, so a run returning it on establishment would reset the ladder on the open alone —
and a server that accepts a watch and drops it would sit at the base delay forever. The
establishing run returns `Succeeded().Provisional()`, and the stream's **first frame** is a `Wake`
whose run records the plain `Succeeded`. The proof is a frame, which is what `kinds.go` already
believed; what moved is that the supervisor owns the count.

## Alternatives considered

**The run *is* the stream — block for the life of the watch.** The obvious shape, and wrong three
ways: it holds a supervisor worker for hours, it needs a cancel-in-flight the supervisor does not
have, and a run cannot move its verdict while it is still running — so `Watching`, `Stale` and the
deltas behind them would have no way out.

**Wait for the first frame inside the establishing run.** It would keep `Succeeded` honest with no
new modifier. But an idle collection's first frame is a bookmark minutes away, and a run that waits
is a worker held — the thing decision one exists to avoid.

**Let the kind sync keep its own ladder and put only the wake on the supervisor.** Half the duplication
stays, and the two ladders disagree about what a retry countdown means: the seam publishes one
`NextRetryAt` per kind and would have to pick.

**A "supervised job" helper in `supervisor`** that starts a goroutine and wires its exit to `Wake`.
There is one consumer. The handle is fifteen lines of `kubesync`, and a second consumer is what
would show which of those lines are general.

## Consequences

`kubesync` keeps what is about Kubernetes — the cold list, the resume, the deltas, the stale timer,
the reasons — and nothing about pacing. A kind's `Restarts` and `NextRetryAt` on the seam are the
supervisor's `Failures` and `NextAttempt.ScheduledAt`, projected in `OnPass`.

The supervisor gained one concept, `Provisional`, and one strengthened invariant. Both are general:
any reconciler that starts something it cannot immediately vouch for wants them, and a reconciler
that never commits a goroutine notices neither.

A kind parked at the identity gate now reads `Restarts` 0, where the hand-written loop carried the
count across the wait. A suspension ends a streak the way a success does, and nothing is retrying
at the gate.

## Revisit when

A second reconciler commits a goroutine. The handle, its `deathRecorded` and `proven` bits, and the
first-frame `Wake` are the shape a helper would generalise — written plainly in one place until
there is a second to compare it against.
