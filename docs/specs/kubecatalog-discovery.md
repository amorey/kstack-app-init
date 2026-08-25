---
title: kubecatalog discovery
scope: sidecar
status: In progress
---

# kubecatalog discovery

> **Progress.** Landed: the `kubecatalog` leaf (engine, probe, `Track`/`Forget`/`Read`/
> `Subscribe`, the connection bridge), the trigger, and the controller-as-fold — the inline
> sweep, `catalogConcurrency`, and the flat `NoConnection` requeue are gone. Remaining: the
> watcher (§ "The watcher"), then the landing steps in "Open questions".

## Goal

Move the served-kinds discovery sweep off the catalog reconcile's goroutine, and make it
watch-prompted with the poll kept as the correctness backstop.

`clusterCachedCatalogController.Reconcile` calls `discoverServedKinds` inline: dozens of
round-trips bounded by the discovery client's timeout, on a beehive worker. It is the only pass
left that makes a network call this way — `catalogConcurrency = 8` is the acknowledged mitigation,
not the design. Discovery is also pull-only, so a CRD installed on the cluster waits up to
`catalogDiscoveryInterval` (10m) to appear.

## Decisions

1. **Discovery gets its own `probe.Engine` instance**, in a new leaf
   `internal/clustersvc/internal/kubecatalog` (sibling of `kubeconn`, free to import it). Not a
   sixth probe on kubeconn's engine: the wake path to the catalog kind is direct instead of riding
   a `ClusterStatus` fingerprint plus a dependency edge, kubeconn's `State` stays
   connection-scoped facts, and sweep workers never contend with the reachability probes.
2. **The watcher is state the probe's `Run` maintains, not a lifecycle the engine schedules.**
   `Run` sweeps, then ensures a watch is standing over the current connection; an async watcher
   death or watch event calls `engine.Wake`, which re-runs `Run`. The same pattern as
   `connectionProbe` maintaining the `Connection`. No engine changes.
3. **The watch wakes; only `Run` commits.** Pull-first made structural: the sweep is the sole
   source of truth, so every watch failure mode costs promptness, never correctness. The probe's
   interval is the backstop under the watch; the kind's beehive resync is the backstop behind the
   fold.
4. **Subjects are keyed by the catalog's beehive name** (`cachedcatalog/{cacheID}`), opaque to the
   leaf. The catalog's reconcile arms and disarms its own subject — holding is what arms the
   probe, the `ensureLease`/`dropLease` shape — so a paused or torn-down catalog costs zero sweeps
   by structure. Grain note: discovery is a fact about the server a context reaches, but keying by
   catalog makes the trigger mapping the identity function, and only the active cache's catalog is
   ever armed, so no context sweeps twice.

## The leaf: `internal/clustersvc/internal/kubecatalog`

### Vocabulary

```go
// Kind is one served kind a cache can mirror.
type Kind struct {
	GroupVersion string
	Kind         string
	Resource     string
	Namespaced   bool
}

// Catalog is one sweep's answer: the mirrorable kinds, sorted, and whether the list is
// the whole truth — an aggregated group that failed to answer makes it partial.
type Catalog struct {
	Kinds   []Kind
	Partial bool
}
```

The mirrorable filters and the deterministic sort (`servedKinds`, `mirrorableGroup`,
`mirrorableResource`, the `EventsAltGroup` drop) move here from `cachedcatalogs.go` — they are
properties of the sweep, not of the beehive kind. `clustersvc` translates `Kind` →
`ClusterCachedResourceSpec`.

### Service

```go
func New(kubeconnSvc *kubeconn.Service) *Service   // a lifecycle.StartCloser

// Track arms discovery for id over contextName's connection; idempotent.
// Forget disarms it and releases everything Track took. Both callable any time.
func (s *Service) Track(id, contextName string)
func (s *Service) Forget(id string)

// Read is the discovery observation for id: the Catalog beside its attempts.
func (s *Service) Read(id string) (probe.Observation[Catalog], bool)

// Subscribe reports every id whose news changed — value, partial, verdict, never
// timing. A gobus hub keyed by id, the same shape kubeconn.Subscribe has.
func (s *Service) Subscribe() Subscription
```

Internally, per tracked id: the subject in the engine, one `kubeconn.Lease` (acquired at `Track`,
released at `Forget` — a boundary caller's own claim, refcounted beside `clusterController`'s),
and the reverse map `contextName → ids` the bridge below reads.

### The probe

One registration: `probe.Register(e, nameCatalog, p, probe.WithInterval(10*time.Minute))`. The
engine's `WithWorkers` default replaces `catalogConcurrency` — the bound moves to where the
concurrency actually is.

`Run(ctx, pass)`, in order:

1. `lease.Conn(ctx)` → `ErrNoConnection` → stop the subject's watcher if one stands,
   `Suspend(ReasonNoConnection, …)`. Re-armed by the bridge, so no flat retry interval.
2. Sweep: `conn.Discovery.ServerPreferredResources()`.
   - Hard error → the `failed(err)` shape: `context.Canceled` → `Skip()`; anything else →
     `Fail(classified, err)` — the ladder, with the standing value kept (an `Observation` keeps
     its value through a failure).
   - `ErrGroupDiscoveryFailed` → build `Catalog{…, Partial: true}`, commit on change,
     `Fail(ReasonDiscoveryPartial, err)` — the ladder retries sooner than the interval, and the
     standing partial value lets the fold add-without-prune meanwhile.
   - Complete → build `Catalog`, commit on change, ensure the watcher, `Succeeded()`.
3. Commit guard — the value carries slices, so the readiness shape rather than `==`:
   `!pass.Known() || kinds moved || partial flipped`. The `Known()` half matters: a cluster
   serving nothing mirrorable has the zero `Catalog` as a legitimate first answer.
4. Ensure the watcher: if none is standing for this subject over this `*Connection`, establish one
   (a synchronous open). **Establishment refused (RBAC, an old server) is still `Succeeded()`** —
   the observation is about catalog data and the data is fine; only promptness degrades, to the
   interval, which is sized for exactly the watchless case. `Fail` here would report a healthy
   catalog as broken forever for a user who can never watch CRDs. The refusal is logged; surfacing
   it on the observation is deferred.

Timeout note: client-go discovery takes no context, so the engine's `WithTimeout` cannot bound the
sweep — the discovery client's own per-request timeout remains the real bound, as today. Set
`WithTimeout` generously so the engine's ctx is not cancelled under a healthy long sweep.

### The watcher

One goroutine per armed subject, held by the probe (keyed by subject, under a mutex; `Forget`
stops it directly — same package). It opens two watches over `conn.Dynamic`:
`apiextensions.k8s.io/v1 customresourcedefinitions` and `apiregistration.k8s.io/v1 apiservices` —
the two things that change what a cluster serves.

- **Any event → `engine.Wake(subject, nameCatalog)`.** The event's content is dropped; the sweep
  reads current state. No debounce is needed: the run queue keys by (subject, probe), so a burst
  of 50 CRD events queues at most one pending re-run, and one arriving mid-run is redelivered on
  `Done` — the queue's own semantics are the coalescing.
- **Exit on**: stop (from `Forget`, or `Run` replacing it), `conn.Done()`, or the streams ending.
  Any end sends one `Wake` on the way out — a re-list after a dropped watch is the Kubernetes
  answer to the gap — and the next `Run` re-establishes. Servers close watches routinely (~5m), so
  the effective sweep cadence while watched is roughly the server's watch timeout;
  resourceVersion continuation to avoid that is a deferred optimization.
- **No retry loop of its own** — establishment lives in `Run`, so a watch that cannot stand is
  retried at sweep cadence, and the ladder and interval already pace that.

### The bridge

One goroutine on `kubeconnSvc.Subscribe()`: for each context whose news changed, `engine.Wake`
every id tracked over it. This is what re-arms a `Suspend(ReasonNoConnection)` — the same shape as
kubeconn's own kubeconfig-watch → `WakeAll(nameConnection)`.

### Publishing

`OnPass` compares news — a fingerprint of `Kinds` plus `Partial` plus the verdict's
`OK()`/`Reason`, never timestamps — against what was last published per id, and sends on the keyed
hub only on a change (kubeconn's `publish`/`newsOf` discipline; `OnPass` fires every pass and
attempts always churn, so the projection is mandatory).

## `clustersvc` changes

**`deps`** gains the service behind a narrow interface in `shared.go` (a field, never a
constructor parameter). `clustersvc.New` builds it beside `kubeconn`; it enters `service.parts` as
a `lifecycle.Part`.

**Trigger**: `newKubecatalogTrigger(svc)` — the existing three-line `trigger[T]` shape over
`svc.Subscribe()`, with `name` returning the event's key verbatim (the id *is* the beehive name —
decision 4). Registered on the `ClusterCachedCatalog` kind with `WithTriggerByName`, beside the
other two.

**`clusterCachedCatalogController.Reconcile` becomes a fold** — no network, no lease of its own:

| State | Action | Result |
| --- | --- | --- |
| deletion-requested / owner chain gone | `Forget(name)` | `Settled()` |
| `!Spec.Enabled` | `Forget(name)`, `relayPause` (unchanged) | as today |
| enabled, context resolves | `Track(name, contextName)`, read `svc.Read(name)`, fold ↓ | |
| observation `!Known()` | `Discovered=False`, reason from `LastAttempt` (`NoConnection` / `Connecting`) | `Settled()` — the trigger wakes the fold when news lands; the kind's resync is the backstop, so `catalogRetryInterval`'s flat 30s requeue goes |
| `Known()` | `converge` with the standing `Catalog` (translate `Kind` → spec; `Partial` gates prune; `draining` unchanged); condition from `Partial`/`draining`/`OK()` as today | `Settled()`, except draining keeps a short `RequeueAfter` — a tombstone releasing is not an event anything reports |

**Deleted outright**: the `discover` seam, `connect()`, `catalogConcurrency` and its registration
override (the pass is cheap now), `kindCatalog`, `discoverServedKinds` / `servedKinds` /
`mirrorable*` (moved to the leaf), and most of `catalogRetryInterval`. The `Discovered` condition
vocabulary is unchanged on the wire.

**Teardown safety**: `Forget` on the deletion-requested pass is sufficient — beehive's soft-delete
mark is an ordinary `Modified` that reconciles before collection, so the disarm path always runs,
and `Forget` is idempotent for the paths that race it.

## Invariants

- **Only `Run` commits; a watch only wakes.** A watch-maintained observable is a different, bigger
  design (incremental fold, resync, partial answers) and must be its own decision.
- **The interval is the correctness bound; the watch is promptness.** Nothing may grow to depend
  on the watch being up.
- **Commit only on a change** — a committed value drives `Subscribe`'s news and therefore the
  fold's wakes.
- **Arm and disarm only from the catalog's reconcile** — subject membership mirrors record state,
  and nothing else touches it.

## Testing

Probe body via `probe.NewPass` against a fake sweep and conn (the seam is the probe struct's
fields, as `connectionProbe{kubecfgSvc}`). Watcher against a fake watch source, its exits asserted
by channel, never a sleep. Controller tests swap the narrow `deps` interface for a fake serving
canned `Observation[Catalog]`s — `go test ./internal/clustersvc` stays fast because the network
mechanism lives in the leaf. A cadence a test would otherwise outwait becomes a parameter whose
production value is the constant.

## Deferred

Adaptive cadence (stretch the interval while the watch is healthy — needs a per-result interval on
the engine, a `Succeeded().After(d)` mirroring beehive's `RequeueAfter`); resourceVersion
continuation on the watches; aggregated discovery (one request instead of dozens on 1.30+
servers); watch health as a first-class field on the observation; metadata-only watch payloads.

## Open questions

- Partial answers as `Fail(ReasonDiscoveryPartial)` (ladder retry, sooner than the interval — as
  specced) versus `Succeeded` with the flag, which waits out the interval.
- When the work lands: fold the outcome into `sidecar/CLAUDE.md`, rewrite `TODO.md`'s
  "mitigated, not fixed" entry, and record decisions 1–3 as an ADR (two real rejected
  alternatives: an engine-supervised watch capability, and a standalone watcher supervisor).
