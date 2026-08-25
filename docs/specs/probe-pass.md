---
title: Probe pass object
scope: sidecar
status: Planned
---

# Probe pass object

## Goal

Replace `Probe[T]`'s four-parameter, two-return signature with a pass object:

```go
// today
Run(ctx context.Context, subjectName string, prev T, snap Snapshot) (Result, *T)

// after
Run(ctx context.Context, pass *Pass[T]) Result
```

The run's inputs move onto `Pass`; the committed value moves to `pass.Commit(v)`, callable
anywhere in the body. The `Result` stays the single return.

## Why

The `*T` return forces every body to thread its value through every branch to a single exit
point: `kubeconn`'s connection probe builds `next` in three of four cases and funnels each through
a local `moved(prev, next)` helper, and nil-means-keep is a contract only the doc states. With
`Commit`, the branch that found news records it where it stands, and a body with nothing to
say — most of them — just returns.

## Semantics

**`Commit` is inline to call, atomic to apply.** The engine buffers the value and commits it
when the run returns, in the same critical section as the attempt record — nothing is published
mid-run. That one rule is what keeps four existing invariants standing:

- A value is always explained by the attempt beside it. `Observation` pairs the value with the
  run that produced it; a mid-run write would publish a value while `LastAttempt` still describes
  the previous run.
- Containment stays all-or-nothing. A body that panics, or returns the zero `Result`, is recorded
  as `Internal` and commits nothing — the buffered value is discarded with the wreckage.
- `Skip` still records nothing. A body can call `Commit` and then hit the branch that
  concludes `Skip`; the buffered value is discarded, so the classification never has to precede
  the observation in the body's control flow.
- Watchers wake after classification, once per run. The `WithWatches` wake rides the commit's
  critical section, so a watcher never runs against a value whose run has not classified.

**Last call wins.** A body that updates twice commits the second value, one attempt, one watcher
wake. Calling `Commit` on a `Pass` after its run returned writes to a buffer nothing will
read again — a stashed pass is inert, not a backdoor.

**Not calling it keeps the previous value**, exactly as returning nil does today.

## An unchanged value

`Commit` asserts the value moved. The engine commits it and wakes every watcher without
comparing it to `prev` — equality is the probe's domain, for the same reason `Reason`
classification is: `connInfo` holds a `*Connection` whose `rest.Config` carries funcs, so a
generic deep-compare would read an unchanged connection as changed on every run, and `==` exists
only for comparable value types (`ComponentStatus` holds a slice). An engine-side compare would
therefore either misfire or silently apply to some value types and not others.

So calling `Commit(v)` with `v` equal to `prev` is legal and harmless but wasteful: the
identical value is written, and every watcher re-runs against news that is not news. The waste is
bounded — the watch graph is acyclic by registration order, and each watcher's own guarded body
commits nothing, so the wave dies in one hop — but a probe on a 10-minute interval woken every 30
seconds by an unguarded upstream has had its registration undone. The body owns the guard, with
the equality it knows is right:

```go
if next != pass.Prev() { // connInfo is comparable; pointer identity is the right meaning
    pass.Commit(next)
}
```

## API

```go
// Probe is one probe's body: request against the subject, classify, and return the result.
// News is recorded on the pass; a body with none just returns.
type Probe[T any] interface {
    Run(ctx context.Context, pass *Pass[T]) Result
}

// Pass is one run of one probe: what it is against, what it knew going in, and what it records.
func (p *Pass[T]) Subject() string     // the subject this run is against
func (p *Pass[T]) Prev() T             // this probe's own last committed value; zero T until one lands
func (p *Pass[T]) Snapshot() Snapshot  // every probe's observable, for sibling reads via Get
func (p *Pass[T]) Commit(v T)          // commit v with this run's result; asserts the value moved

// The write side above is the engine's; these two are for a probe body's own tests, the same
// arrangement Result's accessors have.
func NewPass[T any](subject string, prev T, snap Snapshot) *Pass[T]
func (p *Pass[T]) Updated() (T, bool)  // what the body recorded, and whether it did
```

## Engine changes

Small: the type-erased wrapper `Register` builds is the only code that touches `Pass`.

```go
run := func(ctx context.Context, subjectName string, prev any, snap Snapshot) (Result, any) {
    var pv T
    if prev != nil {
        pv = prev.(T)
    }
    pass := &Pass[T]{subject: subjectName, prev: pv, snap: snap}
    res := p.Run(ctx, pass)
    if pass.next == nil {
        return res, nil
    }
    return res, *pass.next
}
```

`dispatch` and `commit` are untouched: the panic recover already discards the value (the wrapper
never reaches the extraction), the zero-`Result` path already returns nil, and `commit` already
applies the value only on a recording result — which is what makes "discarded on `Skip`"
mechanical rather than a new rule.

## How kubeconn maps

The connection probe records where it classifies, and `moved` is deleted:

```go
func (p *connectionProbe) Run(ctx context.Context, pass *probe.Pass[connInfo]) probe.Result {
    next := pass.Prev()
    _, _, err := p.kubecfg.RESTConfig(pass.Subject())
    switch {
    case errors.Is(err, kubeconfig.ErrNotRead):
        return probe.Skip()
    case errors.Is(err, kubeconfig.ErrContextNotFound):
        next.departed = true
    case err != nil:
        next.departed = false
    default:
        next.departed = false
    }
    if next != pass.Prev() {
        pass.Commit(next)
    }
    switch {
    case errors.Is(err, kubeconfig.ErrContextNotFound):
        return probe.Suspend(ReasonContextNotFound, "kubeconfig no longer names this context")
    case err != nil:
        return probe.Fail(ReasonResolveFailed, err)
    default:
        return probe.Suspend(ReasonResolved, "resolved; nothing dials yet")
    }
}
```

(Whether the body reads better as one switch with the guard per branch or as the split above is
the implementer's call; the spec fixes only the contract.)

The four `unimplemented` stubs shrink to a return with nothing else to say. `probe_test.go`'s
`connect` helper builds a `NewPass` and asserts through `Updated()` instead of applying a returned
pointer. The engine's own tests port mechanically: `steered` gains a `Pass`-shaped `Run`, and the
`probeFunc` adapter follows the new signature.

## What this deletes

`kubeconn`: `moved`. `probe`: the `*T` return and its nil contract, from the interface, the
erased `spec.run` boundary, and every doc that states it.

## Build order

1. `Pass`, `NewPass`, `Updated`; the `Probe[T]` signature change; the wrapper in `Register`.
2. Port the engine tests; add one test each for last-call-wins, discarded-on-`Skip`, and
   discarded-on-panic (the buffered value, not just the record).
3. Move `kubeconn`'s five probes and its tests; delete `moved`.
4. Fold the landed contract into `sidecar/CLAUDE.md` and delete this spec.
