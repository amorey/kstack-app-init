---
title: kubecatalog discovery
scope: sidecar
status: In progress
---

# kubecatalog discovery

> **Progress.** Landed: the `kubecatalog` leaf (engine, probe, `Track`/`Forget`/`Read`/
> `Subscribe`, the connection bridge), the trigger, the controller-as-fold, and identity-scoped
> subjects — the inline sweep, `catalogConcurrency`, and the flat `NoConnection` requeue are gone.
> Partial answers settled as `Fail(ReasonSweepPartial)`, so the ladder retries sooner than the
> interval. **Remaining: the watcher alone** (§ "The watcher"), then the landing steps in "Open
> questions". Its sections were written before identity scoping — what changed for it is that
> `Run` reaches its connection through `ConnFor`, and that stopping the watcher is now part of
> every refusal rather than something `conn.Done()` covers. resourceVersion continuation moved
> from "Deferred" into the watcher: without it the watch layer costs more than it buys.

## Goal

Move the served-kinds discovery sweep off the catalog reconcile's goroutine, and make it
watch-prompted with the poll kept as the correctness backstop.

`clusterCachedCatalogController.Reconcile` calls `discoverServedKinds` inline: dozens of
round-trips bounded by the discovery client's timeout, on a beehive worker. It is the only pass
left that makes a network call this way — `catalogConcurrency = 8` is the acknowledged mitigation,
not the design. Discovery is also pull-only, so a CRD installed on the cluster waits up to the
sweep interval (10m) to appear.

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
   fold. **The streams resume from a resourceVersion** so an uneventful server-side timeout costs
   a reopen rather than a sweep — the one thing that keeps the watch layer cheaper than the poll
   it accelerates (§ "The load this costs").
4. **Subjects are keyed by the catalog's beehive name** (`cachedcatalog/{cacheID}`), opaque to the
   leaf. The catalog's reconcile arms and disarms its own subject — holding is what arms the
   probe, the `ensureLease`/`dropLease` shape — so a paused or torn-down catalog costs zero sweeps
   by structure. Grain note: discovery is a fact about the server a context reaches, but keying by
   catalog makes the trigger mapping the identity function.

   **The key is not enough on its own.** "Only the active cache's catalog is ever armed" is true
   only once a UID change has reached the record, and that is three reconciles downstream — while
   the pool wakes every subject over a context whose identity moved, so the superseded cache's
   sweep runs against the new server first. So a subject is bound to a **server** as well as a
   context, and a run whose context no longer answers as that server suspends with
   `IdentityMismatch` instead of sweeping.

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
func New(conns connService) *Service   // a lifecycle.StartCloser; conns is the narrow
                                      // Acquire/Subscribe half of *kubeconn.Service

// Track arms discovery for id over contextName's connection, for as long as that
// context answers as serverUID; idempotent.
// Forget disarms it and releases everything Track took. Both callable any time.
func (s *Service) Track(id, contextName, serverUID string)
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

One registration:
`probe.Register(e, nameCatalog, p, probe.WithInterval(sweepInterval), probe.WithTimeout(sweepTimeout))`.
The engine's `WithWorkers` default replaces `catalogConcurrency` — the bound moves to where the
concurrency actually is.

`Run(ctx, pass)`, in order:

1. `connFor` → `lease.ConnFor(ctx, sub.serverUID)`. Either refusal stops the subject's watcher if
   one stands and suspends: `ErrNoConnection` → `Suspend(ReasonNoConnection, …)`,
   `ErrIdentityMismatch` → `Suspend(ReasonIdentityMismatch, …)`. Both are re-armed by the bridge,
   so neither takes a flat retry interval.
2. Sweep: `conn.Discovery.ServerPreferredResources()`.
   - Hard error → the `failed(err)` shape: `context.Canceled` → `Skip()`; anything else →
     `Fail(classified, err)` — the ladder, with the standing value kept (an `Observation` keeps
     its value through a failure).
   - `ErrGroupDiscoveryFailed` → build `Catalog{…, Partial: true}`, commit on change,
     `Fail(ReasonSweepPartial, err)` — the ladder retries sooner than the interval, and the
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

One goroutine per armed subject, **keyed on the `Service` beside `tracked`, under the same
mutex** (`Forget` stops it directly — same package). Beside `tracked` rather than on the probe
because establishment has to be measured against whether the id is still tracked, and the two
must not be read separately.

It opens two watches over `conn.Dynamic`:
`apiextensions.k8s.io/v1 customresourcedefinitions` and `apiregistration.k8s.io/v1 apiservices` —
the two things that change what a cluster serves.

**It stands over the connection the sweep just used, and only that one.** `Run` reaches its
connection through `connFor` → `Lease.ConnFor(ctx, sub.serverUID)`, so the watcher inherits the
identity scoping for free: it is established after a successful sweep, over the connection that
sweep ran on.

- **A change event → `engine.Wake(subject, nameCatalog)`.** The event's content is dropped; the
  sweep reads current state. No debounce is needed: the run queue keys by (subject, probe), so a
  burst of 50 CRD events queues at most one pending re-run, and one arriving mid-run is
  redelivered on `Done` — the queue's own semantics are the coalescing.
- **A `Bookmark` never wakes.** Each stream sets `AllowWatchBookmarks: true`, so the server sends
  a periodic event carrying only a resourceVersion. It is how the stream stays resumable while
  nothing changes — waking on it would sweep on the bookmark cadence and cost more than no watch
  at all.
- **Exit on**: stop (from `Forget`, or `Run` replacing it), `conn.Done()`, or a stream ending
  unresumably. See "Continuation" below for which ends are which. An unresumable end sends one
  `Wake` on the way out — a re-list after a gap is the Kubernetes answer — and the next `Run`
  re-establishes.
- **Every early return from `Run` stops the watcher**, refusals included. `conn.Done()` is not
  enough on its own: a connection that goes *conflicted* is never retired, so `Done` never fires
  and a watcher left standing would go on waking a probe that can only suspend — a CRD event on
  the replacement cluster spinning a subject that will never sweep again. The rule is the boring
  one: the watcher exists only while the last `Run` got a connection it was allowed to use.
- **No retry loop of its own** — establishment lives in `Run`, so a watch that cannot stand is
  retried at sweep cadence, and the ladder and interval already pace that.
- **Establishment registers only if the id is still tracked**, checked and written in one critical
  section. `Run` is on a worker: `Forget` can stop the watcher and release the lease while a sweep
  is in flight, and the sweep then finishes and establishes a fresh watcher for an id nothing
  tracks. Its wakes are harmless no-ops, but the goroutine and its two streams stand until the
  connection retires — indefinitely, for a healthy cluster — so it is a leak per pause or teardown
  that races a sweep. **The engine's commit refusal does not cover this**: establishment is not a
  commit. This is the check `publish` makes against the entry and `connFor` makes against the
  subject, applied to the third thing a run leaves behind. A watcher that loses the check is
  stopped rather than stored.

**Continuation: a clean end resumes, it does not sweep.** Each stream remembers the
resourceVersion of the last event it saw — change or bookmark — and reopens from it. This is the
difference between a watch that costs promptness and one that costs load: without it, a quiet
server-side timeout is indistinguishable from a stream that dropped events, so every end has to
be treated as a gap and swept.

- **Per stream, not per subject.** CRDs and APIServices are separate resources with separate
  versions; each tracks its own and they resume independently. One resuming while the other
  re-lists is normal.
- **Resumable**: the stream ended and a resourceVersion is known. Reopen from it, silently — no
  `Wake`. Anything missed during the reconnect is delivered on resume, so a real change still
  wakes; only the empty case goes quiet.
- **Not resumable**, and each sends one `Wake` before the watcher exits: `410 Gone` /
  `apierrors.IsResourceExpired` (the version aged out of the server's watch cache), no version
  known yet (the stream ended before its first event or bookmark), or any other error. The
  watcher does not retry these itself — establishment lives in `Run`.
- **A resourceVersion never crosses a connection.** It lives in the watcher's own state and dies
  with it, which the identity scoping already guarantees: the watcher is stopped whenever the
  connection is replaced or goes conflicted. Do not cache one on the subject to "resume after a
  blip" — a version is one cluster's etcd revision, and the next connection may be a different
  cluster, where it is either rejected or, worse, accepted and streamed from an arbitrary point.

**What this does not change** is decision 3: the watch still only wakes, and `Run` is still the
only thing that commits. Resuming makes the wakes rarer and no more trusted — a resumed stream
that somehow lost events costs promptness until the interval, which is the same backstop every
other watch failure falls to. If anything the interval matters *more* now, since a healthy watched
subject may go a long time without waking at all.

**The watcher owns its context, and it is not `Run`'s.** `Run`'s ctx ends with the pass and is
bounded by `sweepTimeout` regardless, so streams opened on it die when the sweep returns — the
watcher would be established and immediately dead, once per sweep, forever. Derive the watcher's
ctx from the service's, cancel it from the same stop path that removes it from the map, and use
`Run`'s ctx at most for the opening handshake.

### The load this costs

**Roughly the pull-only baseline, plus a change event's worth of sweeps.** A sweep is
`ServerPreferredResources`: one request for `/apis` plus one per group-version, so 25–30 round
trips on an ordinary cluster — call it ~90 requests/hour/cluster on the 10m interval alone. With
continuation a healthy watched subject sweeps on that same interval and on real changes; what the
watch adds is two reopens per server timeout (~24/hour/cluster), each a single request.

**Without continuation it would be 2–4× the baseline**, which is why it is in the design rather
than deferred. Servers close watches routinely (~5m) and two streams end on their own clocks, so
treating every end as a gap would put the steady-state sweep cadence at the watch timeout or half
it — 180–360 requests/hour/cluster to learn nothing, on a desktop app pointed at clusters it does
not own.

Two things still cost a sweep and should stay rare: a version aged out of the server's watch cache
(`410`), and a connection replaced. Both are real gaps, and both are backstopped by the interval
if the sweep they trigger also fails.

### The bridge

One goroutine on `kubeconnSvc.Subscribe()`: for each context whose news changed, `engine.Wake`
every id tracked over it. This is what re-arms a suspended sweep — the same shape as kubeconn's
own kubeconfig-watch → `WakeAll(nameConnection)`. It covers both refusals: `ReasonNoConnection`
when the pool reaches the server again, and `ReasonIdentityMismatch` when the connection is
rebuilt and re-stamped, which `news.vouchedFor` is what signals. **This is the only thing that
restarts a watcher stopped by a refusal**, since a suspended probe has nothing due.

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
| enabled, context resolves | `Track(name, contextName, cache.Spec.ServerUID)`, read `svc.Read(name)`, fold ↓ | |
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
- **Never hold a connection across runs.** `Run` asks `ConnFor` each time and keeps nothing; the
  connection is replaced without warning, and a cached one outlives the identity it was checked
  under. The watcher is the one thing that spans runs, which is why stopping it is part of every
  exit rather than a special case.
- **A resourceVersion never outlives its connection.** It is one cluster's etcd revision; the next
  connection may be another cluster. The watcher holds it and dies with it, which the identity
  scoping already gives — nothing may cache one anywhere longer-lived.
- **Nothing a run leaves behind outlives the subject.** A run is on a worker and `Forget` can land
  under it, so anything a run stores — the committed value, and now the watcher — is written only
  against a subject still tracked, in the same critical section as the check. The engine enforces
  this for commits; the watcher is ours to enforce.

## Testing

Probe body via `probe.NewPass` against a fake sweep and conn (the seam is the probe struct's
fields, as `connectionProbe{kubecfgSvc}`). Watcher against a fake watch source, its exits asserted
by channel, never a sleep — including the exit no `Done` reports: a `ConnFor` that starts refusing
must stop a watcher that is already standing. Controller tests swap the narrow `deps` interface
for a fake serving canned `Observation[Catalog]`s — `go test ./internal/clustersvc` stays fast
because the network mechanism lives in the leaf. A cadence a test would otherwise outwait becomes
a parameter whose production value is the constant.

The two races worth their own tests, since neither has a natural signal to wait on: a `Forget`
landing while a sweep is in flight must leave no watcher behind, and a watcher must survive the
sweep that established it (a stream opened on `Run`'s ctx dies when the pass returns, which a test
that only asserts "a watcher exists" would not catch).

Continuation is four cases against a fake stream, and the negative ones are the point: a bookmark
must not wake, and a clean end with a version known must reopen from it without waking. Then the
two that must: an expired version, and an end with no version yet. Assert the version the reopen
carries, not just that one happened — a resume that silently restarts from "now" looks identical
until events go missing.

## Deferred

Adaptive cadence (stretch the interval while the watch is healthy — needs a per-result interval on
the engine, a `Succeeded().After(d)` mirroring beehive's `RequeueAfter`); aggregated discovery (one
request instead of dozens on 1.30+ servers); watch health as a first-class field on the
observation; metadata-only watch payloads.

## Open questions

- Whether a watcher refused by RBAC should be visible. The probe still reports `Succeeded()` (the
  catalog data is fine; only promptness degrades), so a user who may never watch CRDs sees a
  healthy catalog that is quietly always up to 10m stale. Deferred as "watch health on the
  observation" — the question is whether the fold should say anything about it at all.
- When the work lands: fold the outcome into `sidecar/CLAUDE.md`, delete this spec, close
  `TODO.md`'s watch-prompted entry, and record decisions 1–3 as an ADR (two real rejected
  alternatives: an engine-supervised watch capability, and a standalone watcher supervisor).
  Decision 4's identity half already has its own ADR, so leave it out.
