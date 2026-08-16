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

Depends on [Kubeconfig service](kubeconfig-service.md) for resolved credentials.

## The service

App-owned, a peer of `poke` and `kubeconfig`. It holds pooled connections and nothing else: it
never decides which clusters exist, when to dial, or when to probe.

It satisfies `lifecycle.StartCloser` (see [Lifecycle package](lifecycle-package.md)) by embedding
`lifecycle.None`, since it has no background work yet, and overriding `Close` to close the pooled
connections. The idle sweep below fills in `Start` when it lands.

```go
// package internal/kubeconnect

// Connection is one set of credentials and the clients built over them. The clients share
// one http.Client, so they share one connection pool — with HTTP/2 that is a single TCP
// connection carrying every concurrent request to that API server.
type Connection struct {
    Config *rest.Config
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

Keying on credentials rather than on a cluster id means two records aimed at the same cluster
share one connection, and a cloud-sourced cluster pools the same way a kubeconfig-sourced one
does — it just resolves its config elsewhere.

### The probe

A plain function over a `Connection`, so the service stays free of any notion of a cluster:

```go
// Identity is what one probe learns.
type Identity struct {
    ServerUID     string
    ServerVersion string
    Username      string
}

func Probe(ctx context.Context, conn *Connection) (Identity, error)
```

Three requests on the shared HTTP client, each decoded with `encoding/json`:

- `GET /version` → the server version.
- `GET /api/v1/namespaces/kube-system` → `metadata.uid`. That UID is the cluster's identity;
  Kubernetes has no cluster-level UID of its own.
- `POST /apis/authentication.k8s.io/v1/selfsubjectreviews` → `status.userInfo.username`.

Plain HTTP means `httptest` covers this without a cluster.

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

// Reprobe kicks one cluster's loop now. Backs Service.RetryConnection.
func (p *prober) Reprobe(id ClusterID)
```

`probeResult` is an `Identity`, the time of the probe, and the error if it failed.

One goroutine per tracked cluster, looping: resolve the context through the kubeconfig service,
get the connection for that key, probe, store the result, requeue the object, then wait for the
cadence, a `Reprobe`, or shutdown. The prober is a `lifecycle.StartCloser` on
`clusterController.machinery()`, so it starts and stops with the rest of the kind's machinery.

Reuse needs no work here: an unchanged context resolves to the same key, and the pool returns the
connection it already has. Because the loop runs on a cadence, the pool is warm before anything
else asks for it.

**Failure is not an error to anyone else.** A failed probe records the error in the result, backs
off on the prober's own ladder, and requeues like any other outcome. It never fails a reconcile —
the store is converged; the cluster is down.

**The prober writes nothing to the store.** Its output reaches the record only through the
requeue, so the controller stays the single status writer. Two writers against a status blob that
beehive replaces whole, with no version guard, would clobber each other.

### clusterController

`Reconcile` gains two steps and no I/O:

1. Return early if the record is being deleted, or the kubeconfig has not loaded yet.
2. Observe what the kubeconfig says about the record's context.
3. Eligible — enabled, context present, not deleting — → `prober.Track(id, contextName)`,
   otherwise `prober.Untrack(id)`. Note this is `Enabled`, not `SyncEnabled`: someone tailing
   logs on an unsynced cluster still needs a connection.
4. Fold `prober.Result(id)` into the single status write the pass already makes: server,
   principal, `lastConnectedAt`.
5. On a confirmed UID, create the `ClusterCache` for it. `ensureCache` moves here, after the
   fold, since only a real UID may name a cache.

No `RequeueAfter` cadence and no error return for an unreachable cluster. Beehive already
serializes reconciles per object, and the controller is the only status writer, so no lock and no
re-read are needed.

Membership now lives in two places — the record's spec and the prober's tracked set. `Reconcile`
is the only caller of `Track`/`Untrack`, which keeps them converging: it is level-triggered, and
the startup full pass re-declares every cluster on boot.

## Other consumers

A package that needs a connection declares what it needs and takes it from the composition root:

```go
// package logs
type clusterConnector interface {
    Connection(ctx context.Context, id clustersvc.ClusterID) (*kubeconnect.Connection, error)
}
```

`clustersvc.Service.Connection(id)` replaces `GetConnection`: it resolves the record's context and
returns the pooled connection, waiting for the first successful probe rather than returning nil,
and erroring immediately for a cluster that is unknown, disabled, or absent from the kubeconfig. A
consumer that already knows a context can use the two services directly instead.

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
- **`QPS`/`Burst` are per-connection**, so every consumer of a cluster shares one bucket. Set them
  for the sum of consumers, not for the prober alone.
- **Pass the prober's cadence and backoff as parameters**, with production supplying the
  constants, so tests can shrink them.
- Call `ConfigureKubeHTTP2Keepalive()` once at startup (port from `main`). It tightens client-go's
  HTTP/2 health check so a dropped connection surfaces in about 15 seconds instead of 45.

## Build order

Each step is one red/green cycle and one commit. [Lifecycle package](lifecycle-package.md) and
[Kubeconfig service](kubeconfig-service.md) land first.

1. `kubeconnect.Connection` + `Service.Get`, including two concurrent calls for one key getting
   one connection.
2. `kubeconnect.Probe` against an `httptest` server.
3. `internal/app` builds the service and passes it to `clustersvc`.
4. `prober.Track` starts a loop that probes once and stores the result; `Untrack` stops it.
5. A second pass reuses the pooled connection; a changed key gets a new one.
6. The prober requeues the object after each probe.
7. A failed probe records the error and backs off; the next success clears the backoff.
8. `Reconcile` tracks an eligible cluster and untracks an ineligible one.
9. `Reconcile` folds the prober's result into the status write.
10. `ensureCache` runs off the probed UID — a fake probe now produces `Cluster` → `ClusterCache`
    → `ClusterCachedCatalog` end to end.
11. `Service.Connection` and `Service.RetryConnection`.

The prober takes its `probe` function as a field, defaulted to `kubeconnect.Probe`, so its tests
never touch the network.

## Not in this pass

- **Idle reclaim.** A rotation leaves the old pool entry behind. The fix is a sweep that closes a
  connection nobody has asked for in a while — not reference counting, which the
  `CloseIdleConnections` rule above makes unnecessary. Entries accumulate meanwhile, bounded by
  the number of contexts.
- Connection conditions and probe events, the `/readyz` health check, the sentinel watch that
  detects connection loss early, the `Probing` gauge behind `WatchSchedule`, and pruning caches
  whose identity is superseded. All belong to the prober, not the controller.

## Done when

A tracked cluster is probed on a cadence over one pooled connection, the record carries the probed
identity, and the cache and catalog exist beneath it. `Service.Connection` returns the same object
across probes, and no reconcile makes a network call.
