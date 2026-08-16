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
`drain`. `Watcher` becomes `Service`, matching `poke.Service`. It satisfies `lifecycle.StartCloser`
already, which is how the app composes it. Its existing surface is unchanged:

```go
func New(kubeconfigPath string, pokeSvc *poke.Service) *Service
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error)
func (s *Service) Close() error
func (s *Service) Get() (*api.Config, bool)
func (s *Service) Subscribe() Subscription
```

### Credential resolution moves with it

The service also answers what a context resolves to:

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

`clusterController` keeps depending on a narrow interface rather than the concrete service, so
tests hand it a static fake instead of writing files. Today's `kubeconfigSource` covers
`Subscribe`; widen it to `Get` as well, since `Reconcile` reads that directly.

## Rules

- **One reader of the file.** Nothing else in the sidecar watches the kubeconfig, calls
  `clientcmd`, or builds a `rest.Config`. A package that wants to know about a context reads the
  cluster records.
- The service is a `lifecycle.StartCloser`, and its place in the app's slice is after poke and
  before `clustersvc`. Stop and close reverse that.

## Build order

Each step is one red/green cycle and one commit.

1. Move the package, rename `Watcher` to `Service`, update imports. No behavior change.
2. `internal/app` builds it; `clustersvc.New` takes it; drop the `kubeconfigPath` parameter.
3. `RESTConfig` — a present context resolves, a missing one errors.
4. The key is stable across two resolutions of an unchanged context, and changes when the
   context's credentials or proxy URL change.

## Done when

`internal/app` owns the kubeconfig service, `clustersvc` receives it, and no other package reads
the kubeconfig or resolves credentials. See [Kubeconnection service](kubeconnection-service.md)
for the consumer that turns a resolved config into a pooled connection.
