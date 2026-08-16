---
title: Kubeconnection service
scope: sidecar
status: In progress
---

# Kubeconnection service

## Goal

One connection per cluster, shared by everything that talks to it.

Every part of the sidecar that reaches a cluster — the probe, discovery, the sync workers, log
tailing — should ride the same TCP/TLS connection rather than opening its own. Reuse is the one
requirement that shapes this design.

Its first consumer is the connection probe, which fills in `Cluster.status.server.uid`. Nothing
writes that today, so no `ClusterCache` records are created and the whole cache subtree stays
empty. This spec unblocks it.

Credentials and their key come from `kubeconfig.Service.RESTConfig`, which has landed.

## The service

App-owned, a peer of `poke` and `kubeconfig`. It holds pooled connections and nothing else: it
never decides which clusters exist, when to dial, or when to probe.

It satisfies `lifecycle.StartCloser` by embedding `lifecycle.None`, since it has no background
work yet, and overriding `Close` to close the pooled connections. The idle sweep below fills in
`Start` when it lands.

It enters `App.parts` **between `kubeconfig` and `clustersvc`**: the prober takes connections from
it, and the slice is start order with close reversed, so it opens before its consumer and closes
after it.

```go
// package internal/kubeconnect

// Connection is one set of credentials and the clients built over them. The clients share
// one http.Client, so they share one connection pool — with HTTP/2 that is a single TCP
// connection carrying every concurrent request to that API server.
type Connection struct {
    Config *rest.Config
    // BaseURL is cfg.Host resolved to an absolute URL, carrying the scheme and any path
    // prefix. Raw paths join onto it.
    BaseURL *url.URL
    // HTTPClient is authenticated and pooled. Raw API paths go through it directly.
    HTTPClient *http.Client
    Dynamic    dynamic.Interface
}

// Get returns the shared Connection for these credentials, building it on first use.
// key identifies the credentials; the caller supplies it, because only the caller knows
// what makes them different. Concurrent callers for one key get the same Connection.
func (s *Service) Get(cfg *rest.Config, key string) (*Connection, error)
```

Building one is `rest.HTTPClientFor(cfg)` for the shared client, then
`dynamic.NewForConfigAndClient` over that same client. A `Connection` never changes after it is
built. Credentials change by arriving under a new key, which builds a new one.

`BaseURL` comes from **`rest.DefaultServerUrlFor(cfg)`**, resolved once here, along with the
versioned API path it returns beside it. `cfg.Host` may carry no scheme or a path prefix, and every
raw request and log line needs the absolute form — derived per call site, each gets it subtly
wrong. Not `DefaultServerURL` with a hardcoded `defaultTLS`: `DefaultServerUrlFor` derives it from
whether the config actually has CA or client-cert data or is explicitly insecure, so a scheme-less
plain-HTTP endpoint (`localhost:8080` from a port-forward) stays HTTP instead of becoming `https://`
and failing at the handshake.

**`Get` stamps `QPS`, `Burst` and `UserAgent` itself** before building, from its own constants. The
key fingerprints credentials only, so two callers under one key with different tuning would
otherwise silently share whichever connection built first. Callers supply credentials; the service
supplies the tuning.

Two fields are deliberately *not* stamped. **`WrapTransport` is composed, not owned** — it is a
chain, and `cfg.Wrap` is how you add to one; assigning it would silently drop whatever a caller had
already put there. And **`ContentType` must stay JSON**: the dynamic client decodes unstructured, so
stamping protobuf would break it.

Keying on credentials rather than on a cluster id means two records aimed at the same cluster
share one connection, and a cloud-sourced cluster pools the same way a kubeconfig-sourced one
does — it just resolves its config elsewhere.

### The probe

A plain function over a `Connection`, so the service stays free of any notion of a cluster:

```go
// Identity is what one probe learns. Every field is optional: a probe that reaches the
// API server succeeds, and reports whatever this user was allowed to read.
type Identity struct {
    ServerUID     string
    ServerVersion string
    Username      string
    // UIDErr is why ServerUID is empty, when a response came back saying so. The
    // caller renders it, so it must survive as an error rather than as a bool: a 403
    // (no RBAC on kube-system) and a 404 are different things to tell a user. It also
    // makes Identity uncomparable — == on an error panics for a dynamic type that is
    // itself uncomparable, so compare the fields.
    UIDErr error
}

func Probe(ctx context.Context, conn *Connection) (Identity, error)
```

Three **independent** requests on the shared HTTP client, issued **concurrently** — they share one
HTTP/2 connection, so concurrency is free and the probe costs one round trip instead of three. Each
is decoded with `encoding/json`, and each fails on its own:

- `GET /version` → the server version.
- `GET /api/v1/namespaces/kube-system` → `metadata.uid`. That UID is the cluster's identity;
  Kubernetes has no cluster-level UID of its own.
- `POST /apis/authentication.k8s.io/v1/selfsubjectreviews` → `status.userInfo.username`. The `v1`
  group is 1.28+, so an older server 404s here.

**Only a transport failure fails the probe.** A request that gets an HTTP response got there, which
is the thing being probed; the field it would have filled stays empty. One `Probe` returning one
error for three requests would let a namespace-scoped user's missing RBAC read as a down cluster.

The version and the username are decorations, and empty is a fine value for both. **The UID is
not** — it names the `ClusterCache`, so a cluster that cannot read it gets no cache, and its whole
mirror subtree stays empty. That is the outcome for a user scoped to their own namespaces, which is
common on shared clusters, so it is a state to report rather than one to leave looking broken.

**Reporting it pulls one condition write into this pass.** The vocabulary is already built and
already on the wire: `LiveCondition(ConditionConnected, …)`, the reasons in `shared.go`, and
`Cluster.conditions` — which are beehive object rows, written with `client.SetCondition`, not part
of the status blob. So the controller writes `Connected` after each pass, over the whole state
space rather than the happy path:

| State | Status | Reason |
| --- | --- | --- |
| Ineligible — untracked this pass | False | `ReasonInactive` |
| Tracked, no result yet | Unknown | `ReasonConnecting` |
| Context would not resolve (`ErrContextNotFound`, `ErrNotRead`) | False | `ReasonResolveFailed` |
| Probe failed at the transport | False | `ReasonProbeFailed` |
| Probed, UID unreadable | True | `ReasonIdentityUnavailable` (new) |
| Probed clean | True | `ReasonConnected` |

The two states with no obvious reason are the ones that need naming most. **Never probed** is the
normal state at boot — the same case rule 3 handles for status — and `ReasonProbeFailed` would be a
lie while silence leaves the condition absent; `ReasonConnecting` at `shared.go:211` is written for
exactly it. **Untracked** is the one that rots: liveness only downgrades across a process restart,
so an orphaned cluster would otherwise wear `Connected: True` for the whole session.
`ReasonResolveFailed` likewise exists to keep a vanished kube-context distinct from a cluster that
would not answer — folding it into `ProbeFailed` throws away the distinction the constant was made
for.

**The message must be stable across identical outcomes.** beehive suppresses an unchanged condition
(`conditionUnchanged`, which compares status, reason **and message**), and that suppression is what
makes a write-every-pass condition free. A raw `err.Error()` defeats it: Go's net errors carry the
ephemeral port, and a cluster behind a rotating IP carries the address, so the message differs every
probe — a version bump per cluster per probe, and a frame to every webview, since
`src/lib/clusters.tsx` selects `{ type status reason message lastTransitionedAt }`. Normalize to the
classified reason plus a canonical detail; never pass a transport error through verbatim.

Amend `ConditionConnected`'s doc comment in the same commit: it currently promises the probe
"resolved its identity facts", which `ReasonIdentityUnavailable` makes untrue. The rest of the
conditions stay out of scope.

Plain HTTP means `httptest` covers this without a cluster — including the 403 and 404 paths, which
are the ones worth writing.

## First consumer: the cluster prober

Connecting is background work on its own clock, not part of converging a record. So it runs
beside the controller, not inside it, in the shape `clustersvc` already uses twice: a leaf does
the I/O on its own schedule, and `Reconcile` reads its latest answer and writes it to the record.

The prober lives in `clusters.go` beside the kubeconfig importer — both are the Cluster kind's
machinery, and both run outside beehive. It decides membership and cadence; the two app-owned
services supply credentials and connections.

```go
// Track declares that a cluster should be connected, at contextName. Idempotent; a
// changed context restarts that cluster's loop.
func (p *prober) Track(id ClusterID, contextName string)
// Untrack stops probing a cluster.
func (p *prober) Untrack(id ClusterID)

// Result is the last probe outcome, or ok=false when a cluster has never been probed.
func (p *prober) Result(id ClusterID) (probeResult, bool)

// Await returns the last result and a channel closed when the next one lands. Taking
// both under one lock is what makes a wakeup raceless; see Service.Connection.
// tracked=false for a cluster the prober does not know.
func (p *prober) Await(id ClusterID) (res probeResult, done <-chan struct{}, tracked bool)

// Reprobe kicks one cluster's loop now, and reports whether it reached a tracked
// cluster. Backs Service.RetryConnection.
func (p *prober) Reprobe(id ClusterID) bool
```

`probeResult` is an `Identity`, the time of the probe, and the error if it failed.

**`ok=false` means "no news", never "no identity".** An unprobed cluster is the normal state at
boot — the prober's map is empty and the startup full pass reconciles everything before any probe
lands — so a fold that read it as an absent identity would blank `Server` and `Principal` on every
record, write that, push a `Modified` per cluster to every webview, and leave `ensureCache`
skipping until the first probe. The fold leaves the stored values alone and returns. `Untrack`
drops the in-memory result for the same reason it drops the loop, and never blanks the record.

One goroutine per tracked cluster, looping: resolve the context through the kubeconfig service,
get the connection for that key, probe, store the result, close the wakeup channel, requeue the
object, then wait for the next trigger — the cadence while the cluster is under demand, a
`Reprobe`, or shutdown. The prober is a `lifecycle.StartCloser` on
`clusterController.machinery()`, so it starts and stops with the rest of the kind's machinery.

Reuse needs no work here: an unchanged context resolves to the same key, and the pool returns the
connection it already has.

**Every tracked cluster is probed once; the cadence is what waits for demand.** `Track` probes
immediately, so identity and the `ClusterCache` land for every cluster without anything asking.
Repeating that probe is what waits for `Service.Connection` or a `Reprobe`; until then the loop
probes once and parks.

The split follows the cost. One probe is one round trip, and for an exec-based context one helper
invocation whose token then caches — a bounded, one-time price at boot. The recurring price is the
one worth avoiding: a 30-context kubeconfig re-probing on a schedule means `aws`/`gcloud` running
against 30 clusters forever, many of them VPN-only or long dead, some prompting for MFA or a
keychain unlock. Cadence-on-demand keeps that scoped to the clusters the user actually opened,
while the identity every cluster needs still costs one dial.

A cluster whose one probe failed reports the failure and does not retry until asked — which is what
`RetryConnection` is for, and what the failure ladder covers for a cluster under demand.

**The first probes are bounded.** The startup full pass tracks every enabled cluster at once, so
without a cap a 30-context kubeconfig dials 30 servers and runs 30 credential helpers inside the
first second — a burst of keychain prompts, and a different thing for the machine than the same
work spread out. The probes go through a small semaphore (a buffered channel, a handful wide);
whether a cluster waits for a slot changes nothing about the record it eventually writes.

The loop re-resolves the context on every pass, so a credential rotation that keeps the context
name is picked up on the next probe without anything telling the prober — within one cadence for a
cluster under demand, and on the next `Connection` for a parked one, which is the moment it
matters. `Track` with an unchanged context is a no-op for exactly that reason: the loop is already
converging.

Cadence and the backoff ladder are constructor parameters — production passes 30s and a ladder
capped at 5 minutes — so tests shrink them rather than outwaiting them.

**Failure is not an error to anyone else.** A failed probe records the error in the result, backs
off on the prober's own ladder, and requeues like any other outcome. It never fails a reconcile —
the store is converged; the cluster is down.

**The prober writes nothing to the store.** Its output reaches the record only through the
requeue, so the controller stays the single status writer. Two writers against a status blob that
beehive replaces whole, with no version guard, would clobber each other.

### clusterController

`Reconcile` gains the tracking call, the fold, and the condition write, and makes no network call.
The order matters more than the steps do — both early returns already in the function are traps for
one of them:

1. **Deleting → `prober.Untrack(id)`, then return.** Ahead of the existing early return, not
   behind it. A record on its way out is the one transition that must reach `Untrack`, and it is
   the only one the level-triggered argument below cannot cover: there is no later pass, because
   there is no later record. Miss it and the goroutine, the pooled connection and the exec-plugin
   refreshes outlive the cluster for the life of the process.
2. Return early if the kubeconfig has not loaded yet. Nothing is untracked here — an unread config
   is transient, and the requeue is already the backstop.
3. Observe what the kubeconfig says about the record's context, and fold `prober.Result(id)` into
   the same in-memory status: server, principal. **This step computes; it never returns.**
   No result means no change, not an empty identity.
4. Eligible — enabled, context present — → `prober.Track(id, contextName)`, otherwise
   `prober.Untrack(id)`. Note this is `Enabled`, not `SyncEnabled`: someone tailing logs on an
   unsynced cluster still needs a connection.
5. `ensureCache` off the folded UID, still **ahead of the settle fast-path** where it sits today.
   It reads this pass's status rather than the stored one, so a first UID creates its cache in the
   same pass that learns it.
6. The `Connected` condition write — **after** the cache write, never between it and the fold. It
   is store I/O that can fail and return, which is exactly what the invariant below forbids there.
7. The existing fast-path and status write, unchanged but for a widened equality check.

The invariant that ties 3 and 5 together: **nothing between the fold and the cache write may
return.** `ensureCache` is ahead of the fast-path for a reason the existing comment states — a
record whose generation is already settled can still be missing its cache — and a restart where the
identity is already stored is exactly the startup full pass that would hit it.

No `RequeueAfter` cadence and no error return for an unreachable cluster. Beehive already
serializes reconciles per object, and the controller is the only status writer, so no lock and no
re-read are needed.

**The fold writes `Server` and `Principal`, and nothing else.** It compares against the stored
status as the pass already does, so a probe that confirms an unchanged identity writes nothing.
**By value, not by `==`**: both are structs of `*string`, so the obvious comparison is pointer
identity, every probe allocates fresh strings, and every pass would write and push a frame. This is
the trap `sameKubeconfigObservation` already dereferences around (`clusters.go:513`), and the
fast-path at `clusters.go:478` has to widen past it to cover the folded fields too.
Stamping a time on every probe would mean a status write per cluster per cadence forever — a
`Modified` frame per cluster to every open webview, and behind each one a cache reconcile and a
catalog pass, since `clusterCacheController` watches the cluster record. That is the trap the gauge
rule names: a measurement must never wake a record's dependents.

**`lastConnectedAt` is therefore left unwritten in this pass**, and stays null. Gating it on the
rest of the status changing would be worse than not writing it: it would freeze at the last
*identity* change, so a cluster probed a minute ago renders as last connected days back, and
`src/lib/clusters.tsx` selects it. It is a measurement, so its home is a gauge — the same shape
`clusterCacheHealthWatch` already has, and the same reason.

Membership now lives in two places — the record's spec and the prober's tracked set. `Reconcile`
is the only caller of `Track`/`Untrack`, which keeps them converging: it is level-triggered, and
the startup full pass re-declares every cluster on boot. Deletion is the exception that step 1
exists for — a record that is gone gets no further passes, so its `Untrack` has to happen on the
pass that sees it going.

## Other consumers

A package that needs a connection declares what it needs and takes it from the composition root:

```go
// package logs
type clusterConnector interface {
    Connection(ctx context.Context, id clustersvc.ClusterID) (*kubeconnect.Connection, error)
}
```

`clustersvc.Service.Connection(id)` replaces `GetConnection`: it resolves the record's context and
returns the pooled connection, and errors immediately for a cluster that is unknown, disabled, or
absent from the kubeconfig. A consumer that already knows a context can use the two services
directly instead.

**The last result decides whether it waits, and only a success short-circuits.**

- Last probe succeeded → return the pooled connection immediately. No wait, no probe.
- Last probe failed, or there is none → `Reprobe` and wait for **that probe's** result, returning
  its error if it fails too.

Returning a stored failure would break the model: `Connection` is the demand signal, so the one
call that should restart a parked cluster's probing would be short-circuited by the very failure it
exists to retry — a cluster that was down at boot would stay unreachable for the process's life,
retry button included. Waiting for a *successful* result instead is the opposite failure: a log
tail against a genuinely down cluster would hang until its request context died.

The wait is `Await` first, then `Reprobe`, then select on the channel or `ctx.Done()` — in that
order. `Await` hands back the last result and the next-result channel under one lock, so a probe
that lands between the two calls is already in hand rather than missed while the waiter parks on a
channel that will not fire again for a full cadence, or ever. `tracked=false` is an immediate
error: an eligible cluster whose `Reconcile` has not run yet is neither unknown nor disabled nor
absent from the kubeconfig, and without this it would wait on a channel nothing will fire.

**Hold the connector, not the connection.** Fetch per request, and re-fetch when a stream
reconnects. A component that stashes a `*Connection` in a struct field ends up on stale
credentials after a rotation.

## Rules

- **Never set `rest.Config.Timeout`.** It becomes `http.Client.Timeout` and would cut off long
  watches. Bound a single call with `context.WithTimeout` instead.
- **Build clients with `NewForConfigAndClient`.** `NewForConfig` builds a fresh `*http.Client` and
  a fresh pool.
- **No typed Kubernetes clients.** `kubernetes.Clientset` adds 27 MB to the shipped binary and
  `discovery.DiscoveryClient` adds 18 MB, against 23 MB today — the cost is the generated API
  types, so even one typed group client pays most of it. Raw paths on `HTTPClient` plus the
  dynamic client cost nothing, and the sidecar stores raw JSON anyway. Apimachinery types
  (`metav1.APIGroupList`, `PartialObjectMetadata`) are already linked and stay free.
- **Closing a connection only drops idle sockets** (`CloseIdleConnections`), so in-flight streams
  are never cut. That is what makes replacing a rotated connection safe with no reference
  counting.
- **`QPS`/`Burst` throttle the dynamic client only.** The token bucket lives in
  `rest.RESTClient`, not in the transport: `rest.HTTPClientFor` returns an unthrottled client, so
  every raw path on `Connection.HTTPClient` — the probe included — is unlimited. Rate-limit those
  yourself if they ever need it; don't reach for `QPS` expecting it to cover them.
- **Pass the prober's cadence and backoff as parameters**, with production supplying the
  constants, so tests can shrink them.
- Call `ConfigureKubeHTTP2Keepalive()` once at startup. It tightens client-go's HTTP/2 health check
  so a dropped connection surfaces in about 15 seconds instead of 45. It left with the deleted
  `internal/cluster/controllers` package in this repo — recover it from git history, not from
  upstream; `TODO.md` carries the entry.

## Build order

Each step is one red/green cycle and one commit.

1. `kubeconnect.Connection` + `Service.Get`, including two concurrent calls for one key getting
   one connection.
2. `kubeconnect.Probe` against an `httptest` server.
3. `internal/app` builds the service and passes it to `clustersvc`.
4. `prober.Track` starts a loop that probes once and stores the result, then parks; `Untrack`
   stops it.
5. A second pass reuses the pooled connection; a changed key gets a new one.
6. The prober requeues the object after each probe.
7. A failed probe records the error; under demand it backs off and retries, and the next success
   clears the backoff.
8. `Reconcile` tracks an eligible cluster and untracks an ineligible one — including a deleted
   one, which is its own test.
9. `Reconcile` folds the prober's result into the status write, and an unchanged identity writes
   nothing.
10. `ensureCache` runs off the probed UID — a fake probe now produces `Cluster` → `ClusterCache`
    → `ClusterCachedCatalog` end to end. Running the app against a real cluster produces the same
    chain, since step 4 probes on `Track`.
11. The `Connected` condition write across the whole table, including
    `ReasonIdentityUnavailable` from a 403 on `kube-system`, and a repeated identical outcome
    writing no new version.
12. `Service.Connection` and `Service.RetryConnection`, replacing `GetConnection`. That signature
    change also touches `graph/cluster_testutils_test.go`'s `fakeClusterService` and the
    `TestUnimplementedBoundaryPanics` inventory in `service_test.go`.

The prober takes its `probe` function as a field, defaulted to `kubeconnect.Probe`, so its tests
never touch the network.

## Not in this pass

- **Idle reclaim, for the pool and for the cadence alike.** A rotation leaves the old pool entry
  behind, and demand is a one-way gate: a cluster the user opened once keeps re-probing until
  `Untrack`, so the recurring exec/MFA cost comes back permanently for every cluster ever visited.
  Both want the same sweep — close a connection nobody has asked for in a while, and park the loop
  that goes with it — not reference counting, which the `CloseIdleConnections` rule above makes
  unnecessary. Accepted meanwhile: entries and cadences are bounded by the number of contexts, and
  a user who opened a cluster is the user most likely to open it again.
- **`lastConnectedAt` as a gauge**, alongside `clusterScheduleWatch`. Null until then.
- **A derived cluster identity.** A user who cannot read `kube-system` gets no cache at all. Naming
  the cache from the connection instead — host plus CA fingerprint, most of which
  `kubeconfig.fingerprint` already computes — would give them one, at the cost of an identity that
  changes if their RBAC later widens, orphaning the cache it named. Worth revisiting once pruning
  superseded caches exists to clean up after it.
- The remaining conditions and probe events, the `/readyz` health check, the sentinel watch that
  detects connection loss early, the `Probing` gauge behind `WatchSchedule`, and pruning caches
  whose identity is superseded. All belong to the prober, not the controller.

## Done when

**Run the app against a real cluster and the chain is visible**: every enabled context is probed
once, its record carries the probed identity, and the cache and catalog exist beneath it. A cluster
under demand re-probes on the cadence over that same pooled connection; `Service.Connection`
returns the same object across probes, and no reconcile makes a network call. A cluster whose user
cannot read `kube-system` still connects, and its `Connected` condition says why it has no cache.

Docs land in the same commits: `sidecar/CLAUDE.md` gains the `internal/kubeconnect` entry and the
prober's place in `clusterController.machinery()`, and its promise that
`clustersvc.New(dataDir, kubeconfigSvc, pokeSvc)` keeps its signature is corrected when step 3
breaks it. `TODO.md` loses the keepalive entry. Delete this spec when the last step lands.
