---
title: Probe engine
scope: sidecar
status: Planned
---

# Probe engine

## Goal

Move `kubeconn`'s scheduling machinery into `sidecar/internal/probe`, a reusable engine that runs a
set of periodic observations over a set of subjects. `kubeconn` is left with five probe bodies and a
`State` projection.

The engine is the controller pattern without Kubernetes: a work queue, a level-triggered pass, and a
schedule derived from recorded state. Nothing in it knows what a kube-context is.

## Why now

`kubeconn` still owes six pieces of work. Five of them are engine concerns, not Kubernetes ones:
panic recovery in the worker, a per-probe deadline, the backoff ladder, worker count as an option,
and logs/metrics. Written in place, they turn `Service` from a scheduler into a scheduler with
Kubernetes-shaped holes in it.

The sixth is the tell that the seam is already being crossed: `Service.due` interleaves generic
rules (succeeded → interval, failed → retry) with `ReasonContextNotFound`, `ReasonUnsupported`, and
`PhasePending`.

The package is also at its most testable right now — 99% coverage, with tests pinning the review
findings that landed in `due` and the in-flight guard. That safety net is what the move spends, and
it will only thin as the remaining probe slices land.

## Four decisions

**1. Scheduling policy returns to the probe, as the result of a run.** The engine cannot own `due`
as-is, because it would have to know what `ContextNotFound` means. So a run's result carries its
own schedule, built by one of four constructors:

- `Succeeded()` — record success; due again after the probe's interval.
- `Fail(reason, err)` — record failure; due again up the backoff ladder.
- `Suspend(reason, msg)` — record why; nothing due until a `Wake`.
- `Skip()` — record nothing; nothing due until a `Wake`. For a run that learned nothing usable:
  an unread kubeconfig, a run cancelled by shutdown.

Every Kubernetes-specific rule returns to the probe that owns it, matching the rule already in
force for dependencies — declared where the probe is, not tested in the scheduler. `Skip` also
deletes the engine's would-be `Canceled` special case: a run that learned nothing leaves no record,
so there is no attempt to keep out of the failure streak.

**2. The success-path poll is the default.** `Succeeded` means due again after the interval, no
exception — a connection that dies silently must be re-examined. A probe with nothing to poll for
opts out through `Suspend`, and a `Wake` is what brings it back.

**3. Staleness is a `Wake`, not a generation.** `kubecfgGen` exists because "the file moved" must
survive a run in flight. `Wake(subject, ids...)` replaces it: it puts those keys straight on the
run queue, whose held/dirty machinery already guarantees an add landing mid-run is redelivered when
that run commits, never folded into it. A `Wake` is the one thing no derivation could produce — an
external fact that a recorded answer went stale — so it does not break "derived, never armed".

**4. Probes register one at a time.** `Register(name, run, opts...)` returns the probe's `ID`, and
`probe.Needs` takes IDs already returned — a dependency must exist before its dependent, so the
API cannot express a cycle, and the registration order is the dependency order a reader wants
anyway. The set is fixed once `Start` runs.

## API

```go
package probe

type ID int // a probe's registration index, returned by Register

// Result is what a run concluded: the record it leaves and the schedule it earns.
// Built by Succeeded, Fail, Suspend, or Skip; the zero Result is invalid.
type Result struct{ /* verdict, reason, message, err */ }

// Run is one probe's body: request against the snapshot, classify, and hand back the
// result plus a commit — nil to write nothing.
type Run[S any] func(ctx context.Context, snap S) (Result, func(*S))

func New[S any](opts ...Option[S]) *Engine[S]

// Register adds one probe and returns its ID. It panics on a Needs entry not yet
// registered, a duplicate name, or a call after Start — a table wired wrong at boot,
// not a runtime error.
func (e *Engine[S]) Register(name string, run Run[S], opts ...ProbeOption) ID

// Per-probe options; a registration states only what deviates from the defaults.
func Needs(ids ...ID) ProbeOption                 // dependencies — see the lifecycle below
func WithInterval(d time.Duration) ProbeOption    // Succeeded → due again this long after the run
func WithBackoff(base time.Duration, factor float64, cap time.Duration) ProbeOption
func WithTimeout(d time.Duration) ProbeOption     // per run, enforced by the engine

func (e *Engine[S]) Add(subject string)             // track a subject; every probe derives from zero
func (e *Engine[S]) Remove(subject string)
func (e *Engine[S]) Wake(subject string, ids ...ID) // these answers are stale: run again, suspension notwithstanding
func (e *Engine[S]) WakeAll(ids ...ID)
func (e *Engine[S]) Read(subject string) (S, []Attempts, bool) // attempts indexed by ID
func (e *Engine[S]) Start(context.Context) (func(context.Context) error, error)
func (e *Engine[S]) Close() error
```

The defaults are the package's: interval 1m, timeout 30s, ladder 1s × 2 capped at 5m. The ladder
stays a pure function of `Failures`, whatever the knobs say.

`Run` takes a snapshot copied under the engine's lock and returns the result plus a commit. The
commit is applied by the engine under that lock, together with the attempt bookkeeping and the pass
that follows — one critical section over a subject's values and its schedule, as `kubeconn` has
today. A nil commit writes nothing.

`Attempts` is today's type plus `LastSeen`, which the engine stamps when a run succeeds — so a
commit writes the observed value and nothing else. The engine owns two reason strings,
`"Succeeded"` and `"DependencyFailed"`; a caller's reason vocabulary aligns by value, as
`kubeconn`'s already does.

Options carry what the engine cannot guess: `WithWorkers(n)`, `WithOnChange(func(subject string,
s S, at []Attempts))`, `WithClock`, `WithLogger`. `OnChange` fires after every pass, outside the
engine's lock but serialized per subject; the caller decides what is worth announcing — `kubeconn`
keeps its `news` comparison and its two hubs.

## Dependencies

`probe.Needs` declares that a probe's question can only be answered over another probe's success —
identity, version, readiness, and principal all need the connection. Because it takes IDs
`Register` already returned, the graph is acyclic by construction; the panic in `Register` is the
backstop for a hand-forged ID. The lifecycle has three states, all derived by the pass:

| dependencies' state | the dependent probe |
| --- | --- |
| none has answered yet | untouched — no record, nothing scheduled. There is nothing to say about a server nobody has tried. |
| any is failing | one `"DependencyFailed"` record, then suspended for the rest of the outage — a dead cluster costs one timeout per cycle, not one per probe. |
| all OK | scheduled normally. A recovery makes a `DependencyFailed` probe due at once — the re-arm is read off the state, so nothing has to notice the recovery and go looking for what suspended on it. |

Two rules keep the lifecycle honest:

- **Suspending records why.** The dependent's suspension reason lives in its own last attempt,
  which is why the outage writes a record instead of going quiet.
- **`Needs` is rechecked at dispatch.** A dependency that failed between the pass and the worker
  picking the key up means the run is recorded as `DependencyFailed`, not dialed — a worker must
  never spend a timeout learning what the state already said.

## How kubeconn maps

`probe.go` becomes one registration function and the five `Run` bodies. The registrations stay in
one function on purpose — the set's rules (every dependent needs exactly the connection, intervals
ordered by volatility) are checked by eye, and that only works when they read side by side:

```go
// probeIDs is what registration returned, held by Service: the names Wake, Read, and
// the State projection address probes by.
type probeIDs struct {
	connection, readiness, serverUID, serverVersion, principal probe.ID
}

func registerProbes(e *probe.Engine[values], kubecfg kubeconfigService) probeIDs {
	var p probeIDs
	p.connection = e.Register("connection", runConnection(kubecfg),
		probe.WithInterval(30*time.Second))
	p.readiness = e.Register("readiness", runReadiness,
		probe.WithInterval(30*time.Second), probe.Needs(p.connection))
	p.serverUID = e.Register("serverUID", runServerUID,
		probe.WithInterval(10*time.Minute), probe.Needs(p.connection))
	...
	return p
}
```

The kubeconfig service is captured by the `Run` closures, so `probeArgs` goes away. The connection
probe keeps owning the context's lifecycle — resolving the file is the first step of dialing — and
returns:

| the run finds | returns |
| --- | --- |
| context resolves — nothing dials yet | `Suspend("Resolved")` — scaffolding; becomes a real dial returning `Succeeded` |
| context resolves — once dialing lands | `Succeeded()`, commit writes the connection |
| `ErrContextNotFound` | `Suspend("ContextNotFound")` — the watch reports the file moving; polling asks nothing new |
| any other resolve failure | `Fail("ResolveFailed", err)` — a file to fix, kept on the ladder because nothing here can see the fix |
| `ErrNotRead` | `Skip()` — an unread file names nothing, and the watch wakes us when it is read |

The four dependent probes declare `probe.Needs(p.connection)` and contain only their request and
its classification.

`Service` keeps leases and publishing, plus the `probeIDs`: `Acquire`'s first holder calls
`engine.Add`, the last `Release` calls `engine.Remove`, the kubeconfig watch calls
`engine.WakeAll(p.connection)`, and the `clusterConnectionRetry` mutation becomes
`engine.Wake(subject, p.connection)` when it lands. `State` keeps its public shape —
`Observation[T]` embeds `probe.Attempts` — and is assembled from `engine.Read` at publish time
rather than mutated in place, indexing the attempts by the held IDs. `clustersvc.foldState` is
untouched.

Two behavior changes ride the move, both deliberate:

- **A departed context's failure streak clears.** `Suspend` resets `Failures` — it parks a
  question rather than failing at one, and `ReasonContextNotFound`'s own doc already calls
  departure the user's edit, not a fault. Today the streak grows by one per kubeconfig change for
  as long as the claim is held.
- **`Phase` gains one scaffolding case** — `"Resolved"` reads as `PhasePending` — deleted when
  dialing lands. The `forget()` reset it replaces exists only because nothing dials.

## Rules

The invariants the move must preserve. Each has a test in `kubeconn` today; each moves to the
engine's tests with it.

- **The schedule is derived, never armed.** Every pass recomputes what each probe should do from
  recorded state, so no path can leave a run un-armed by forgetting it. `Wake` is the one input
  the derivation cannot produce, and it goes through the queue, not around the pass.
- **One timer per subject, and it is a wake, not a cadence.** It brings the pass back; the pass
  decides again per probe.
- **A run in flight is left alone.** `NextAttempt` *is* that run; writing a schedule over it
  erases both the in-flight mark and the schedule it was dispatched on. Its commit reconciles,
  and a `Wake` that landed mid-run is redelivered then — pin the interleaving with a test, it is
  where this package has been wrong before.
- **A finished run ends and reschedules in one critical section**, or a reader between the two
  sees a probe that quietly stopped.
- **A subject removed and re-added is a different subject.** A run dispatched against the old
  registration commits nothing.
- **The queue keys by `{subject, probe}`**, so one probe never runs twice at once and an ask
  arriving mid-run earns a fresh run rather than joining it.
- **A panicking run still commits and gives its key back** — recorded as `Fail("Internal")`.
  Otherwise it wedges twice over: the probe reads as in flight forever, and its key stays held in
  the queue.
- **`ScheduledAt` is data.** The frontend renders the ladder, so the next time must be derivable
  from recorded state and stable across passes. That rules out stateful rate limiters —
  `client-go`'s `TypedRateLimitingInterface` keeps its ladder in a private map — and jitter inside
  the schedule, which would move the rendered countdown on every pass.

## What this deletes

From `kubeconn`: `probeQ`, `probeLoop`, `probeWorkers`, `runProbe`, `commitProbe`, `reconcile`,
`reconcileLocked`, `due`, `wanted`, `entry.timer`, `stopTimer`, `observations()`, `probeArgs`,
`Attempts` and its methods, `kubecfgGen`, and `bumpKubeconfig`.

## Build order

1. `internal/probe`: `ID`, `Result`, `Attempts`, `Register` and its options, and the engine over
   the existing `internal/workqueue`. Port `kubeconn`'s scheduling tests as the engine's own,
   including the wake-mid-run interleaving.
2. Panic recovery and the per-probe deadline — the two latent hangs.
3. The backoff ladder, as a pure function of `Failures` paced by `WithBackoff`.
4. Move `kubeconn` onto the engine: `registerProbes`, `values`, `State` assembled at publish. The only
   step that changes `kubeconn`'s behaviour, and its tests are the ones that prove it.
5. Worker count, logs, metrics as options.

Steps 1–3 land with the engine unused.

## Not in this pass

- Promotion to a standalone module. It lands at `sidecar/internal/probe`, beside
  `internal/workqueue`. Promote when a second subject type proves the shape.
- Dependencies beyond "needs another probe to be OK". No ordering, no graphs.
- A probe picking its own next run time. The four results cover every schedule `kubeconn` needs;
  a `RequeueAfter` analogue waits for a probe that needs one.
- Anything from `beehive`: no persistence, no conditions, no GC cascade, no multi-kind support.
  The engine is deliberately in-memory and single-purpose, and it sits underneath `clustersvc`,
  which is the beehive layer.

## Done when

- `internal/probe` owns the queue, the pass, the timer, backoff, deadlines, and panic recovery,
  with its own tests for every rule above.
- `kubeconn/probe.go` is `registerProbes` and the five `Run` bodies; `kubeconn/service.go` is
  leases and publishing.
- `kubeconn`'s public surface — `Lease`, `State`, `Observation`, `Reason`, `Phase` — is unchanged
  but for the two behavior changes above, and `clustersvc` needed no edit.
- What is then true is folded into `sidecar/CLAUDE.md`, and this spec is deleted.
