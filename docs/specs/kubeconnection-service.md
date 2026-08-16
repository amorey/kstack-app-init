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

`BaseURL` comes from `rest.DefaultServerURL(cfg.Host, cfg.APIPath, schema.GroupVersion{}, true)`,
resolved once here. `cfg.Host` may carry no scheme or a path prefix, and every raw request and log
line needs the absolute form — derived per call site, each gets it subtly wrong.

**`Get` stamps the transport fields itself** — `QPS`, `Burst`, `UserAgent`, `ContentType`,
`WrapTransport` — before building, from its own constants. The key fingerprints credentials only,
so two callers under one key with different tuning would otherwise silently share whichever
connection built first. Callers supply credentials; the service supplies everything else.

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
}

func Probe(ctx context.Context, conn *Connection) (Identity, error)
```

Three **independent** requests on the shared HTTP client, each decoded with `encoding/json`, each
failing on its own:

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
common on shared clusters, so it is a state to report rather than one to leave looking broken: the
controller records the missing identity as a stated reason on the cluster. The connection itself is
good, so log tailing against that cluster still works.

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

// Reprobe kicks one cluster's loop now, and is what first probes a tracked cluster.
// Backs Service.RetryConnection.
func (p *prober) Reprobe(id ClusterID)
```

`probeResult` is an `Identity`, the time of the probe, and the error if it failed.

One goroutine per tracked cluster, looping: resolve the context through the kubeconfig service,
get the connection for that key, probe, store the result, requeue the object, then wait for the
cadence, a `Reprobe`, or shutdown. The prober is a `lifecycle.StartCloser` on
`clusterController.machinery()`, so it starts and stops with the rest of the kind's machinery.

Reuse needs no work here: an unchanged context resolves to the same key, and the pool returns the
connection it already has.

**Tracking is membership; demand is what starts the dialing.** A tracked cluster sits idle until
something asks about it — `Service.Connection`, or an explicit `Reprobe` — and only then probes and
settles into the cadence. Probing eagerly would mean a 30-context kubeconfig dialing 30 API servers
at boot, most of them VPN-only or long dead, and for EKS/GKE contexts invoking `aws`/`gcloud` on a
repeating schedule — credential helpers that prompt for MFA or unlock the keychain. An unprobed
cluster reports no identity, which is a state the record and the UI already carry.

The loop re-resolves the context on every pass, so a credential rotation that keeps the context
name is picked up within one cadence without anything telling the prober. `Track` with an unchanged
context is a no-op for exactly that reason: the loop is already converging.

Cadence and the backoff ladder are constructor parameters — production passes 30s and a ladder
capped at 5 minutes — so tests shrink them rather than outwaiting them.

**Failure is not an error to anyone else.** A failed probe records the error in the result, backs
off on the prober's own ladder, and requeues like any other outcome. It never fails a reconcile —
the store is converged; the cluster is down.

**The prober writes nothing to the store.** Its output reaches the record only through the
requeue, so the controller stays the single status writer. Two writers against a status blob that
beehive replaces whole, with no version guard, would clobber each other.

### clusterController

`Reconcile` gains two steps and no I/O. The order matters more than the steps do — both early
returns already in the function are traps for one of them:

1. **Deleting → `prober.Untrack(id)`, then return.** Ahead of the existing early return, not
   behind it. A record on its way out is the one transition that must reach `Untrack`, and it is
   the only one the level-triggered argument below cannot cover: there is no later pass, because
   there is no later record. Miss it and the goroutine, the pooled connection and the exec-plugin
   refreshes outlive the cluster for the life of the process.
2. Return early if the kubeconfig has not loaded yet. Nothing is untracked here — an unread config
   is transient, and the requeue is already the backstop.
3. Observe what the kubeconfig says about the record's context, and fold `prober.Result(id)` into
   the same in-memory status: server, principal. **This step computes; it never returns.**
4. Eligible — enabled, context present — → `prober.Track(id, contextName)`, otherwise
   `prober.Untrack(id)`. Note this is `Enabled`, not `SyncEnabled`: someone tailing logs on an
   unsynced cluster still needs a connection.
5. `ensureCache` off the folded UID, still **ahead of the settle fast-path** where it sits today.
   It reads this pass's status rather than the stored one, so a first UID creates its cache in the
   same pass that learns it.
6. The existing fast-path and status write, unchanged.

The invariant that ties 3 and 5 together: **nothing between the fold and the cache write may
return.** `ensureCache` is ahead of the fast-path for a reason the existing comment states — a
record whose generation is already settled can still be missing its cache — and a restart where the
identity is already stored is exactly the startup full pass that would hit it.

No `RequeueAfter` cadence and no error return for an unreachable cluster. Beehive already
serializes reconciles per object, and the controller is the only status writer, so no lock and no
re-read are needed.

**`lastConnectedAt` rides the status write; it never causes one.** The fold compares against the
stored status as the pass already does, so a probe that confirms an unchanged identity writes
nothing. Stamping the time on every probe would mean a status write per cluster per cadence
forever — a `Modified` frame per cluster to every open webview, and behind each one a cache
reconcile and a catalog pass, since `clusterCacheController` watches the cluster record. That is
the trap the gauge rule names: a measurement must never wake a record's dependents.

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

For a cluster that has never been probed it `Reprobe`s and waits for **that probe's result** — not
for a successful one. The prober closes a per-cluster channel on each result, which is the wakeup;
`Connection` waits on that channel or `ctx.Done()`, and returns the probe's error if it failed.
Waiting for success instead would hang a log tail against a down cluster until the request context
died, with nothing explaining why.

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
4. `prober.Track` starts a loop that probes once and stores the result; `Untrack` stops it.
5. A second pass reuses the pooled connection; a changed key gets a new one.
6. The prober requeues the object after each probe.
7. A failed probe records the error and backs off; the next success clears the backoff.
8. `Reconcile` tracks an eligible cluster and untracks an ineligible one — including a deleted
   one, which is its own test.
9. `Reconcile` folds the prober's result into the status write, and an unchanged identity writes
   nothing.
10. `ensureCache` runs off the probed UID — a fake probe now produces `Cluster` → `ClusterCache`
    → `ClusterCachedCatalog` end to end.
11. `Service.Connection` and `Service.RetryConnection`, replacing `GetConnection`. That signature
    change also touches `graph/cluster_testutils_test.go`'s `fakeClusterService` and the
    `TestUnimplementedBoundaryPanics` inventory in `service_test.go`.

The prober takes its `probe` function as a field, defaulted to `kubeconnect.Probe`, so its tests
never touch the network.

## Not in this pass

- **Idle reclaim.** A rotation leaves the old pool entry behind. The fix is a sweep that closes a
  connection nobody has asked for in a while — not reference counting, which the
  `CloseIdleConnections` rule above makes unnecessary. Entries accumulate meanwhile, bounded by
  the number of contexts.
- **A derived cluster identity.** A user who cannot read `kube-system` gets no cache at all. Naming
  the cache from the connection instead — host plus CA fingerprint, most of which
  `kubeconfig.fingerprint` already computes — would give them one, at the cost of an identity that
  changes if their RBAC later widens, orphaning the cache it named. Worth revisiting once pruning
  superseded caches exists to clean up after it.
- **Warming the pool ahead of demand.** Nothing dials until something asks, so the first
  `Connection` for a cluster pays the TLS handshake and any credential-helper exec. Pre-probing the
  active kube-context is the obvious next step, once the sidecar is told which one that is.
- Connection conditions and probe events, the `/readyz` health check, the sentinel watch that
  detects connection loss early, the `Probing` gauge behind `WatchSchedule`, and pruning caches
  whose identity is superseded. All belong to the prober, not the controller.

## Done when

A tracked cluster is probed on a cadence over one pooled connection, the record carries the probed
identity, and the cache and catalog exist beneath it. `Service.Connection` returns the same object
across probes, and no reconcile makes a network call. A cluster whose user cannot read
`kube-system` still connects, and says why it has no cache.

Docs land in the same commits: `sidecar/CLAUDE.md` gains the `internal/kubeconnect` entry and the
prober's place in `clusterController.machinery()`, and its promise that
`clustersvc.New(dataDir, kubeconfigSvc, pokeSvc)` keeps its signature is corrected when step 3
breaks it. `TODO.md` loses the keepalive entry. Delete this spec when the last step lands.
