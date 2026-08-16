---
title: Kubeconfig service
scope: sidecar
status: In progress
---

# Kubeconfig service

## Goal

One owner for the user's kubeconfig, owned by the app rather than by `clustersvc`.

The watcher already exists and works — this is a move plus one new method. It follows the
`poke` service: process-wide, no domain state, built and started by `internal/app`.

## Design

### The move

`clustersvc/internal/kubeconfig` becomes `sidecar/internal/kubeconfig`, a peer of `poke` and
`drain`. `Watcher` becomes `Service`, matching `poke.Service`, in `kubeconfig.go` /
`kubeconfig_test.go`. It satisfies `lifecycle.StartCloser` already, which is how the app composes
it. Its existing surface is unchanged:

```go
func New(kubeconfigPath string, pokeSvc *poke.Service) *Service
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error)
func (s *Service) Close() error
func (s *Service) Get() (*api.Config, bool)
func (s *Service) Subscribe() Subscription
```

### Credential resolution moves with it

The service also answers what a context resolves to, in `restconfig.go` / `restconfig_test.go`:

```go
// RESTConfig resolves one context to credentials, and the key identifying them. The key
// changes when the credentials change; it is what the connection pool caches on.
func (s *Service) RESTConfig(contextName string) (*rest.Config, string, error)
```

`clientcmd` for the config; the key is a hash of the config's auth and TLS fields plus the
context's raw `proxy-url` — port it from `connections.Fingerprint` on `main`. It has to be
computed here: `clientcmd` compiles the proxy URL into a func that cannot be hashed, and this is
the only place that still sees the raw value.

This is why resolution belongs to this service and not to the connection pool. Reading the
kubeconfig is this service's job, and nowhere else in the sidecar should call `clientcmd`.

Three things it must get right:

- **One snapshot per call.** Take `Get()` once and derive both the `rest.Config` and the proxy URL
  from it. `main` calls `Get()` twice, so a reload landing between them keys one snapshot's
  credentials under another's proxy URL — and since the key is the pool's identity, that serves a
  connection under a key that does not describe it until the next rotation.
- **The key excludes the context name**, so two records aimed at the same cluster through
  different contexts share one connection. That is load-bearing for the
  [kubeconnection service](kubeconnection-service.md); it is not an oversight to fold the name in.
- **Before the first read, return a distinguishable error** — the caller must be able to tell
  "not read yet" from "context not found", because the empty seed makes every context look absent
  and a prober would otherwise record a real cluster as gone. An empty `contextName` is an error
  too: `clientcmd` would silently fall back to the current context, which is never what a record
  naming no context means.

**Not in this pass: memoizing.** Every call re-copies the config, runs `ConfirmUsable`, and rebuilds
TLS and auth material, and the prober calls it per cluster per period forever, only to recompute a
key that changes rarely. The service knows when the file changed, so a per-context memo invalidated
on publish would make the steady state a map lookup. Correctness does not depend on it; do it when
the prober's cadence makes it show.

### Wiring

`internal/app` builds it between the poke bus and `clustersvc`:

```go
pokeSvc := poke.New()
kubeconfigSvc := kubeconfig.New(cfg.KubeconfigPath, pokeSvc)
clusterSvc, err := clustersvc.New(cfg.DataDir, kubeconfigSvc, pokeSvc)
```

`clustersvc.New` takes the service instead of a path, so `kubeconfigPath` stops being threaded
through `New` → `registerControllers` → `newClusterController`, and resolving *where* the
kubeconfig lives stays app's job. The service joins `deps` beside `pokeSvc`.

**`clusterController` gives up ownership.** Drop the watcher from `machinery()`, leaving the
importer alone; the controller holds the service as an injected dependency and neither starts nor
closes it. Without that it is started twice and closed twice, and that `Close` closes the
`watch.Hub` — ending every kubeconfig subscription in the process.

`clusterController` keeps depending on a narrow interface rather than the concrete service, so
tests hand it a static fake instead of writing files. Widen today's `kubeconfigSource` once, to
`Get` (which `Reconcile` reads directly) and `RESTConfig` (which the prober needs), and rename it
— it is no longer the watcher as subscribers see it.

`Service.GetConnection(id) *rest.Config` on the `clustersvc` boundary still panics; the
[kubeconnection service](kubeconnection-service.md) replaces it with a `Connection`. It is not an
exception to the rule below.

## Rules

- **One reader of the file.** Nothing else in the sidecar watches the kubeconfig, calls
  `clientcmd`, or builds a `rest.Config`. A package that wants to know about a context reads the
  cluster records.
- **Resolution goes through `loadingRules.Load()`'s config, never a hand-built `api.Config`.**
  Loading is what resolves relative paths — `certificate-authority: ca.crt` against the
  kubeconfig's own directory — so any other route silently breaks CA and client-cert loading.
  Pin it with a relative-path fixture.
- The service is a `lifecycle.StartCloser`, and its place in the app's slice is after poke and
  before `clustersvc`. Stop and close reverse that. That ordering is what the importer already
  relies on: reverse close drains it before the hub closes, and the watcher's synchronous first
  read still precedes the importer's current-on-subscribe subscription.

## Build order

Each step is one red/green cycle and one commit.

1. Move the package, rename `Watcher` to `Service`, update imports. No behavior change.
2. `internal/app` builds it and owns its lifecycle; `clustersvc.New` takes it; drop the
   `kubeconfigPath` parameter and the watcher's `machinery()` entry.
3. `RESTConfig` — a present context resolves (including a relative CA path), a missing one, an
   empty one, and a pre-read service each error distinguishably.
4. The key is stable across two resolutions of an unchanged context, changes when the context's
   credentials or proxy URL change, and is equal for two contexts that differ only by name.

## Done when

`internal/app` owns the kubeconfig service, `clustersvc` receives it, and no other package reads
the kubeconfig or resolves credentials. `sidecar/CLAUDE.md` reflects the move in both the
`internal/` inventory and the `clustersvc` layout block, in the same change. See
[Kubeconnection service](kubeconnection-service.md) for the consumer that turns a resolved config
into a pooled connection.
