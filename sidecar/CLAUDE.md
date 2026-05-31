# sidecar — Go backend

A standalone Go binary started by the Tauri host. It exposes the app's GraphQL API (and a gRPC control channel) and owns all Kubernetes and kstack-cloud logic. **No TCP** — it listens on a Unix domain socket (named pipe on Windows), prints `READY unix:<path>` to stdout so the host can dial it, and shuts down on `SIGINT`/`SIGTERM` or **stdin EOF** (the host closing its end = "parent gone").

## Layout

The codebase mirrors the **kstack-cloud / kubetail layout**: `main.go` is lifecycle only, `internal/app` is the composition root + routing, and the GraphQL server lives in `graph` (alongside the resolvers) — the sidecar's analogue of kstack-cloud's `graph.Server`. There is **no `server` package**; "the server" is the thin listener in `main.go`.

- `main.go` — **lifecycle only**. Parses flags, binds the socket, builds the `*app.App`, mounts it on an `http.Server`, registers `app.NotifyShutdown` via `srv.RegisterOnShutdown`, serves, and drives graceful shutdown (`srv.Shutdown` → `app.DrainWithContext` → `app.Close`). No composition logic.
- `internal/app/` — **composition root + routing + lifecycle owner**. `app.go` (`app.New(Config{...})`) builds the engine-shared instances (store, hub, sync engine, creds, kube-config watcher), constructs the `graph.Resolver`, wires the GraphQL server (`graph.NewServer`) and gRPC server (`grpcserver.NewServer`), mounts them plus the control endpoints on a mux, and multiplexes onto one h2c handler (`grpcserver.NewH2CHandler`). `App` is an `http.Handler` owning `Start` (sync engine) and the `NotifyShutdown`/`DrainWithContext`/`Close` surface that fans out to the two sub-servers. `control.go` holds the **host-only control endpoints** (`/control/credentials`, `/control/wake`); `paths.go` holds the prefs-file layout helpers (`DefaultPrefsPath`, `SyncPath`).
- `graph/` — GraphQL: `schema.graphqls`, generated code, resolvers, and **`server.go`** — `graph.Server` (`NewServer(*Resolver)`), the gqlgen handler with the bearer-token plumbing, SSE shutdown lifecycle, and `NotifyShutdown`/`DrainWithContext` (see Shutdown below). A bare `&Resolver{}` is tolerated (nil `Sync` → Offline) so tests can stand up a minimal surface.
- `grpc/` — gRPC surface. `grpc/server.go`: `grpcserver.NewServer` wraps a `*grpc.Server` (registering `KubeContextService`, reusing `k8shelpers.KubeConfigWatcher`) with the `NotifyShutdown`/`DrainWithContext`/`Stop` lifecycle surface; the lower-level `New` stays exported for tests that drive the serving context directly. The **h2c multiplexer** `NewH2CHandler` also lives here (it owns the "what is a gRPC request" rule). `grpc/kubecontextpb/` holds the **committed** protoc output for the repo-root `proto/kubecontext.proto`. Regenerate with `make proto` (or `go generate ./grpc/...`); never hand-edit `*.pb.go`.
- `internal/` — building blocks: `app` (composition root, above), `ipc` (binds the per-OS IPC endpoint — AF_UNIX socket / named pipe — with user-only access; `main.go` uses `ipc.Listen`/`DefaultSocketPath`/`Scheme`), `k8shelpers` (kubeconfig watcher), `sync` (cloud sync engine), `syncstore` + `prefs` (local state), `hub` (pub/sub), `cloud`, `authcreds`, `mutationqueue`, `atomicjson`, `logging`.

## gRPC + GraphQL over one socket (h2c)

gRPC needs HTTP/2; GraphQL stays HTTP/1.1. `grpcserver.NewH2CHandler` (in the `grpc/` package — it owns the "what is a gRPC request" rule, so `graph`/`app` don't depend on gRPC) wraps the GraphQL/control mux in `h2c.NewHandler` and dispatches HTTP/2 `application/grpc` requests to the `*grpc.Server`, everything else to the mux. HTTP/1.1 GraphQL POST + SSE are untouched (they keep `ProtoMajor==1` and a Flusher-backed writer). An idle `Watch` stream is kept alive under the server's 60s `IdleTimeout` by a gRPC keepalive ping. The gRPC `KubeContextService` and the GraphQL `kubeConfigWatch`/`setCurrentContext` resolvers share the one `KubeConfigWatcher`, so a change via either transport fans out to both. The webview uses GraphQL; the host (tray) uses gRPC.

**Shutdown** is driven through the `App` surface from `main.go`. `srv.RegisterOnShutdown(app.NotifyShutdown)` fires the moment `srv.Shutdown` begins, then:

1. **`app.NotifyShutdown()`** fans out to both sub-servers: `grpcServer.NotifyShutdown()` cancels the serving context (every `Watch` handler returns `nil` → grpc writes its OK trailers → each client sees a clean `io.EOF`), and `graphqlServer.NotifyShutdown()` closes a `shutdownCh` that cancels each active SSE subscription's request context (gqlgen flushes its terminal `event: complete`).
2. **`srv.Shutdown(ctx)`** stops accepting and waits for the non-hijacked GraphQL requests to finish — the SSE streams, now cancelled, flush and return.
3. **`app.DrainWithContext(ctx)`** waits on *both* sub-servers' `WaitGroup`s. This is the essential wait for the **hijacked h2c gRPC streams** that `srv.Shutdown` does not track; for GraphQL it's belt-and-suspenders.
4. **`app.Close()`** stops the gRPC transports (`grpcServer.Stop()`), stops the sync engine (bounded), and closes the kube-config watcher.

Why it's built this way (the non-obvious parts): grpc's own `GracefulStop` **panics** on the `ServeHTTP`/h2c path, so the stream must end on its own serving context instead, and `Stop` only runs in step 4 after the streams have drained. The GraphQL SSE drain is **per-request** (`graph.Server.ServeHTTP` wires each SSE request's context to `shutdownCh`), deliberately **not** via `http.Server.BaseContext`: a `BaseContext` cancel would tear down the shared h2c connection carrying gRPC mid-stream. Because nothing touches `BaseContext`, the two transports are independent — `NotifyShutdown` signals both at once and there is no required gRPC-before-GraphQL ordering. (The h2c dispatcher also routes gRPC away *before* the GraphQL server's `ServeHTTP`, so a long-lived gRPC connection is never counted in the GraphQL `WaitGroup`.)

## GraphQL via gqlgen — the schema is the source of truth

`graph/schema.graphqls` is authoritative — it is **also consumed by the frontend's codegen** (`codegen.ts` at the repo root). After editing it:

```sh
cd sidecar && go run github.com/99designs/gqlgen generate
```

This rewrites `graph/generated.go` + `graph/model/models_gen.go` and **appends resolver stubs** (panicking `not implemented`) to `graph/schema.resolvers.go` — implement those. **Never hand-edit `generated.go` / `models_gen.go`.** `tools.go` pins gqlgen so the command works on a fresh checkout. (Also re-run the frontend `pnpm codegen` after schema changes.)

## Patterns

- **Nil-tolerant resolver**: a bare `&graph.Resolver{}` (used by tests / surfaces that don't run the engine) must not panic. `graph.NewServer` defaults a nil `Sync` to an Offline `noopStatus`; resolvers nil-guard their optional deps (e.g. `KubeConfigWatch`, `SetCurrentContext` guard `KubeConfigWatcher == nil`). Preserve this when adding resolvers.
- **Pub/sub**: `internal/hub` and `github.com/amorey/gochan/watch` hubs fan snapshots to subscribers. Watchers (`k8shelpers`) use fsnotify + a debounce, then publish the new snapshot.
- **Subscription resolvers**: return a channel that **emits the current snapshot first, then deltas** (see `streamWithSnapshot`, and the `kubeConfigWatch` resolver). Honor `ctx.Done()`.
- **Single-writer sync engine** (`internal/sync`): it owns the only cloud connection. Reads are served from the local `syncstore` and never touch the network; write-through mutations publish to the hub and let the engine persist on the cloud echo.

## Tests & checks

- testify + `net/http/httptest`. Resolver-level GraphQL tests stand up `graph.NewServer(&graph.Resolver{...})` and `POST /graphql` (in `graph/`); h2c/control/lifecycle tests stand up `app.New(...)` (in `internal/app/`). Filesystem via `t.TempDir()` / `os.MkdirTemp`. Prefer **hand-written fakes** over generated mocks.
- **Avoid magic sleeps.** Don't `time.Sleep` to wait for async work (fsnotify reloads, hub publishes) — block on the actual event with `<-sub.Chan()` guarded by a `select { case … : case <-time.After(deadline): t.Fatal(…) }` timeout, as the watcher tests do. A `time.After` deadline is fine; a fixed sleep is flaky and slow.
- `make test-go` (`cd sidecar && go test ./...`).
- `make lint-go` (gofmt) and `make vet-go` (`go vet`). Run `gofmt -w` before committing.

When you change the sidecar's schema workflow, wiring, or conventions, update this `CLAUDE.md` in the same change.
