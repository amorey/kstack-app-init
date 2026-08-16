---
title: Lifecycle package
scope: sidecar
status: In progress
---

# Lifecycle package

## Goal

One definition of the start/stop/close shape, shared by every service in the sidecar.

`clustersvc` already has it — `startCloser`, `startAll`, `closeAll` — and composing a level with
it is a slice instead of a hand-written stop func. `internal/app` composes the same way but by
hand, and every service the app is about to gain (kubeconfig, kubeconnection) needs the same
shape. Hoisting it is a move, plus two adapters at the app.

## Design

A new `sidecar/internal/lifecycle`, a peer of `drain` — small, no domain knowledge. It uses
`drain` for its tests, which is the only dependency.

```go
// package internal/lifecycle

// StartCloser is the three-phase shape: Start bounds startup with ctx and returns the func
// that drains the background work, taking a drain deadline; Close releases what the drain
// left. Two methods, three phases — stop is Start's return value, so it cannot be called
// before there is anything to stop.
//
// ctx bounds STARTUP ONLY. Background work must outlive it and end only when the stop func
// is called, so a caller can time-limit startup without killing what started.
//
// A stop func must be idempotent and must drain with drain.WithContext: composition calls
// them uniformly, so a retry or a double drain anywhere above must not panic or return
// before the work is joined.
type StartCloser interface {
    Start(ctx context.Context) (func(context.Context) error, error)
    io.Closer
}

// None supplies the lifecycle for something with no background work. Embed it, and
// override either method when it grows some.
type None struct{}

// StartAll starts each in order and returns one stop func. A failed start unwinds what
// already started, on a context detached from the startup one, and Closes nothing — that
// stays the caller's.
func StartAll(ctx context.Context, ls []StartCloser) (func(context.Context) error, error)

// CloseAll closes in reverse, mirroring the stop order.
func CloseAll(ls []StartCloser) error
```

Moved as-is from `clustersvc/service.go:312-385`, along with `stopAll`, `unwind`, and
`unwindTimeout`, which stay unexported.

Slice order is start order; stop and close run in reverse. That is the whole ordering contract,
and it replaces comments explaining evaluation order at each call site.

### clustersvc

Mechanical rename: `startCloser` → `lifecycle.StartCloser`, `noBackground` → `lifecycle.None`,
same for the helpers. Behavior unchanged.

The tests for the moved helpers move with them, but **`stubStartCloser` is copied, not moved** —
`TestStartDrainsBeehiveWhenAControllerFails` stays behind and still uses it. Its copy goes in
`clustersvc/testutil_test.go`.

### internal/app

`App.Start` builds a slice and calls `StartAll`; `App.Close` calls `CloseAll`. Two of the
services need an adapter first:

- **`poke.Service`** and **`cloud.Service`** have `Start` but no `Close` (cloud's godoc says its
  stop func replaces one).
- **`auth.Service`** has neither and stays out of the slice, as it is today.

Adapt them with small wrapper structs **in `internal/app`**, not by adding methods to those
packages — a leaf should not import `lifecycle` to be composable, and the wrapper is the right
place for the other two jobs below. `beehiveRuntime` in `clustersvc` is the existing example of
the same move.

Each wrapper does three things:

1. Supplies `Close` as a no-op, since the stop func already releases everything.
2. **Passes `context.Background()` to the wrapped `Start`.** Both derive their run context from
   the ctx they are given, so a bounded startup context would kill their background work the
   moment startup finished. Their stop funcs cancel and drain on their own, so detaching loses
   nothing. This is the `ctx bounds startup only` rule; `clustersvc`'s leaves already honor it.
3. **Wraps the start error** with the service's name, keeping today's `start poke service: %w`
   context that `StartAll` cannot supply.

The gRPC and GraphQL servers stay outside the slice: they have their own shutdown protocol
(`NotifyShutdown`, `DrainWithContext`), and `App.Close` still calls `grpcServer.Stop()` first,
before `CloseAll`.

**The slice at this step is poke, cluster, cloud** — kubeconfig and kubeconnection join it as
those specs land. Reversing it stops cloud before cluster, where today the order is cluster →
cloud → poke. That is a deliberate change and a safe one: the two are independent, and what the
current order actually protects is poke stopping **last**, after its subscribers, which reversal
preserves.

## Rules

- **The interface means the same thing at every level**, leaves included. A `Start` that does not
  return a drain func, or a `Close` safe to call before the drain, breaks composition silently.
- **`ctx` bounds startup, never the run.** Background work ends via the stop func alone.
- **Stop funcs are idempotent and drain with `drain.WithContext`.**
- **Two methods, three phases.** Do not add a `Stop` — it would be callable before `Start`.
- Slice order is start order. Stop and close reverse it.
- A failed `StartAll` stops what started; closing stays the caller's. `main.go` exits on a start
  failure without calling `App.Close`, which this keeps correct.

## Build order

Each step is one red/green cycle and one commit.

1. Create `internal/lifecycle` with the moved code and the moved helper tests. Copy
   `stubStartCloser` into `clustersvc/testutil_test.go` first, so `clustersvc` still builds.
2. `clustersvc` uses the package; delete the local copies. Repoint the idempotent-stop comment in
   the kubeconfig watcher, which names `startAll`/`stopAll` by hand, at the `StartCloser` godoc
   that now carries the rule.
3. `internal/app` composes with `StartAll`/`CloseAll`, adding the poke and cloud wrappers.

## Done when

One package defines the shape, `clustersvc` and `internal/app` both compose with it, and a new
service satisfies it by construction.

`sidecar/CLAUDE.md` is updated in the same change: the "One lifecycle shape at every level —
`startCloser` in `service.go`" paragraph now names `internal/lifecycle`, and the composition-root
bullet's stop chain (`clusterSvcStop → cloudSvcStop → pokeSvcStop`, with the `errors.Join`
evaluation-order note) becomes the reversed slice.

See [Kubeconfig service](kubeconfig-service.md) and [Kubeconnection service](kubeconnection-service.md),
both of which satisfy the interface.
