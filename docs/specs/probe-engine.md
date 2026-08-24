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

| Gap | Belongs to |
| --- | --- |
| Panic recovery in the worker | engine |
| Per-probe deadline | engine |
| Backoff ladder | engine |
| Worker count as an option | engine |
| Logs and metrics | engine |
| Poll on the success path | **mixed** |

Written in place, those five turn `Service` from a scheduler into a scheduler with Kubernetes-shaped
holes in it. The sixth is the tell that the seam is already being crossed: `Service.due` interleaves
generic rules (succeeded → interval, failed → retry) with `ReasonContextNotFound`,
`ReasonUnsupported`, and `PhasePending`.

The package is also at its most testable right now — 99% coverage, with tests pinning the five
review findings that landed in `due` and the in-flight guard. That safety net is what the move
spends, and it will only thin as the remaining probe slices land.

## The decision that makes it possible

`due` cannot move as-is, because the engine would have to know what `ContextNotFound` means.

**Terminality becomes part of what a probe returns, not a case in a shared switch.** A run reports a
`Disposition` alongside its reason:

- `Succeeded` — due again after the probe's interval.
- `Retry` — due again up the backoff ladder.
- `Suspend` — nothing is due until something wakes it.

`Suspend` is how a probe says *do not ask again* without the engine learning why.
`ReasonContextNotFound` and `ReasonUnsupported` both become `Suspend`; a resolve failure becomes
`Retry`. Every Kubernetes-specific scheduling rule returns to the probe that owns it, matching the
rule already in force for dependencies — declared where the probe is, not tested in the scheduler.

## API

```go
package probe

type ID int
type Key struct { Subject string; Probe ID }

type Disposition int
const (
	Succeeded Disposition = iota
	Retry
	Suspend
)

// Outcome is what a run concluded. Reason is the caller's vocabulary, opaque here.
type Outcome struct {
	Disposition
	Reason, Message string
	Err             error
}

// Spec declares one probe over subject type S.
type Spec[S any] struct {
	Name     string
	Interval time.Duration
	Backoff  Backoff       // base, factor, cap
	Timeout  time.Duration
	Needs    []ID          // recorded rather than dialed while any of these is not OK
	Run      func(ctx context.Context, snap S) (Outcome, func(*S))
}

type Engine[S any] struct{ /* queue, workers, timer, lock, subjects */ }

func New[S any](specs []Spec[S], opts ...Option[S]) *Engine[S]

func (e *Engine[S]) Add(subject string)
func (e *Engine[S]) Remove(subject string)
func (e *Engine[S]) Wake(subject string, ids ...ID)
func (e *Engine[S]) WakeAll(ids ...ID)
func (e *Engine[S]) Read(subject string) (S, []Attempts, bool)
func (e *Engine[S]) Start(context.Context) (func(context.Context) error, error)
func (e *Engine[S]) Close() error
```

`Run` takes a snapshot copied under the lock and returns the outcome plus a commit. **The commit is
applied by the engine under its own lock**, together with the attempt bookkeeping and the pass that
follows — which is what keeps one lock over one subject's values and its schedule, as `kubeconn` has
today.

Options carry what the engine cannot guess: `WithWorkers(n)`, `WithOnChange(func(subject string, s S,
at []Attempts))`, `WithDependencyReason(string)`, `WithClock`/`WithLogger`.

`OnChange` fires after every pass. The caller decides what is worth announcing — `kubeconn` keeps its
`news` comparison and its two hubs.

## What kubeconn keeps

`probe.go` becomes five declarations and nothing else:

```go
const (
	probeConnection probe.ID = iota
	probeReadiness
	probeServerUID
	probeServerVersion
	probePrincipal
)

func specs(kubecfg kubeconfigService) []probe.Spec[values] {
	return []probe.Spec[values]{
		probeConnection: {
			Name: "connection", Interval: 30 * time.Second,
			Run: func(ctx context.Context, _ values) (probe.Outcome, func(*values)) { ... },
		},
		probeServerUID: {
			Name: "serverUID", Interval: 10 * time.Minute,
			Needs: []probe.ID{probeConnection},
			Run:   ...,
		},
		...
	}
}
```

The kubeconfig service is captured by the closures, so `probeArgs` goes away with the rest.

`Service` keeps leases and publishing:

```go
type Service struct {
	engine    *probe.Engine[values]
	signalHub *conflate.Hub[string, struct{}]
	stateHub  *watch.Hub[string, State]

	mu        sync.Mutex
	holders   map[string]int   // claims are kubeconn's; subjects are the engine's
	published map[string]news
}
```

`Acquire`'s first holder calls `engine.Add`; the last `Release` calls `engine.Remove`. The kubeconfig
watch calls `engine.WakeAll(probeConnection)`.

`State` keeps its public shape: `Observation[T]` still embeds `Attempts`, now `probe.Attempts`. Only
its construction changes — assembled from `engine.Read` at publish time rather than mutated in place.
`clustersvc.foldState` reads `*kubeconn.State` and is untouched.

## What this deletes

From `kubeconn`: `probeQ`, `probeLoop`, `probeWorkers`, `runProbe`, `commitProbe`, `reconcile`,
`reconcileLocked`, `due`, `wanted`, `entry.timer`, `stopTimer`, `observations()`, `probeArgs`, and
`Attempts` with its methods.

## Rules

These are the invariants the move must preserve. Each has a test in `kubeconn` today; each moves to
the engine's tests with it.

- **The schedule is derived, never armed.** Every pass recomputes what each probe should do from
  recorded state. No path arms a future run directly, so no path can leave one un-armed by
  forgetting it.
- **One timer per subject, and it is a wake, not a cadence.** It brings the pass back; the pass
  decides again per probe.
- **A run in flight is left alone.** `NextAttempt` *is* that run. Writing a schedule over it erases
  both the mark saying it is still out and the schedule it was dispatched on. Its commit reconciles.
- **A finished run ends and reschedules in one critical section.** Ending leaves the probe with
  nothing scheduled, which reads as suspended; a reader between the two would see a probe that had
  quietly stopped.
- **`wanted` is tested before the run is marked**, or an early return leaves the probe reading as
  running and nothing schedules it again.
- **A subject removed and re-added is a different subject.** A run dispatched against the old
  registration commits nothing.
- **The queue keys by probe, not by subject**, so one probe never runs twice at once and an ask
  arriving mid-run earns a fresh run rather than joining it.
- **`ScheduledAt` is data.** The backoff ladder is rendered by the frontend, so the next time must be
  derivable from recorded state and stable across passes. That rules out every stateful rate limiter
  — including `client-go`'s `TypedRateLimitingInterface`, whose ladder lives in a private map — and
  it rules out random jitter inside the schedule, which would move the rendered countdown on every
  pass.

## Build order

1. `internal/probe`: `ID`, `Key`, `Outcome`, `Spec`, `Attempts`, and the engine over the existing
   `internal/workqueue`. Port `kubeconn`'s scheduling tests as the engine's own.
2. Panic recovery and per-probe deadline — the two that are latent hangs, done where they belong.
   A panicking run currently wedges twice over: the probe stays marked in flight, and its key stays
   held in the queue because `Done` is never reached.
3. Backoff ladder, as a pure function of `Failures`, paced by `Backoff` on the `Spec`.
4. Move `kubeconn` onto the engine: five `Spec`s, `values`, `State` assembled at publish.
5. Worker count, logs, metrics as options.

Steps 1–3 land with the engine unused; step 4 is the only one that changes `kubeconn`'s behaviour,
and its tests are the ones that prove it.

## Open questions

**Does `kubecfgGen` survive?** It exists because a file change landing mid-run must not be lost: the
pass skips an in-flight probe, so the generation is what makes the next pass re-derive. With `Wake`
adding straight to the queue, `Add` on a held key marks it dirty and redelivers on `Done` — the same
machinery, one step shorter. Re-test rather than assume; the in-flight interleaving is where this
package has been wrong before.

**Where does the success-path poll live?** `due` currently suspends a connection probe that
succeeded, because nothing dials yet. Once one does, `Succeeded` must mean *due again after
`Interval`* with no exception, or a connection that dies silently is never re-examined. The engine
should make the unconditional poll the default and force a probe to opt out through `Suspend`.

## Not in this pass

- Promotion to a standalone module. It lands at `sidecar/internal/probe`, beside `internal/workqueue`.
  Promote when a second subject type proves the shape.
- Dependencies beyond "needs another probe to be OK". No ordering, no graphs.
- Anything from `beehive`: no persistence, no conditions, no GC cascade, no multi-kind support. The
  engine is deliberately in-memory and single-purpose, and it sits underneath `clustersvc`, which is
  the beehive layer.

## Done when

- `internal/probe` owns the queue, the pass, the timer, backoff, deadlines, and panic recovery, with
  its own tests for every rule above.
- `kubeconn/probe.go` is five `Spec`s; `kubeconn/service.go` is leases and publishing.
- `kubeconn`'s public surface — `Lease`, `State`, `Observation`, `Reason`, `Phase` — is unchanged,
  and `clustersvc` needed no edit.
- The two open questions are answered in the code, and the answers are folded into `sidecar/CLAUDE.md`.
