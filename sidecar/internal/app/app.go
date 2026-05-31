// Package app is the sidecar's composition root and lifecycle owner. It builds
// the engine-shared instances (store, hub, sync engine, creds, kube-config
// watcher), wires the GraphQL and gRPC servers, and multiplexes them onto one
// h2c handler. main() stays thin: it binds the listener and drives the shutdown
// surface this package exposes — NotifyShutdown / DrainWithContext / Close —
// mirroring the server/app split used across the kubetail and kstack-cloud
// services.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	syncengine "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
)

// Config is the subset of process configuration the composition root needs.
// main() resolves these from flags/env and hands them over.
type Config struct {
	// CloudURL is the kstack cloud base URL (without /graphql); the cloud
	// client appends the path itself.
	CloudURL string
	// PrefsPath is the local preferences cache file; the engine derives its
	// sync state files alongside it. Empty falls back to server.DefaultPrefsPath().
	PrefsPath string
	// KubeconfigPath is an explicit kubeconfig path; empty uses clientcmd's
	// default-loading rules.
	KubeconfigPath string
}

// App owns the composed sidecar: one h2c handler fronting the GraphQL and gRPC
// servers, plus the sync engine and kube-config watcher they share. It is an
// http.Handler; main() mounts it on the listener.
type App struct {
	handler http.Handler
	graphql *graph.Server
	grpcSrv *grpcserver.Server
	watcher *k8shelpers.KubeConfigWatcher
	engine  *syncengine.Engine

	engineCancel context.CancelFunc
	engineDone   chan struct{}
}

// New builds the composition root. The engine must exist before the Resolver
// (it is the syncStatus source) yet needs the credential Holder, so this
// function owns the shared instances and threads them both ways — the cycle
// only the composition root can break. The kube-config watcher is shared by the
// GraphQL resolver and the gRPC KubeContextService, so a context switch over
// either transport fans out to both.
func New(cfg Config) (*App, error) {
	prefsPath := cfg.PrefsPath
	if prefsPath == "" {
		prefsPath = DefaultPrefsPath()
	}

	syncStore := syncstore.NewStore[prefs.Settings](SyncPath(prefsPath, "settings.json"))
	hub := prefs.NewHub()
	creds := authcreds.NewHolder()
	cloudClient := cloud.New(cfg.CloudURL)
	queue := mutationqueue.New(SyncPath(prefsPath, "mutations.json"))

	// Always non-nil: the watcher tolerates missing/malformed kubeconfigs and
	// seeds with an empty *api.Config so resolvers stay safe. Only fatal here is
	// a kernel-level fsnotify failure (ENOMEM, ulimit).
	watcher, err := k8shelpers.NewKubeConfigWatcher(cfg.KubeconfigPath)
	if err != nil {
		return nil, err
	}

	engine := syncengine.New(
		syncengine.NewCloudUpstream(cloudClient, creds),
		syncStore,
		hub,
		syncengine.Options{
			// Drain offline-queued mutations whenever the upstream is healthy. A
			// failed drain stays queued and retries on the next Live via the
			// engine's own reconnect/backoff.
			OnConnected: func(ctx context.Context) {
				if err := queue.Drain(ctx, func(ctx context.Context, in cloud.UpdateInput) error {
					_, err := cloudClient.UpdateSettings(ctx, creds.Token(), in)
					return err
				}); err != nil {
					slog.Debug("mutation queue drain failed", "err", err)
				}
			},
		},
	)

	// The Resolver reads the same syncstore the engine maintains, subscribes the
	// Hub it publishes to, exposes its Status (Sync), and write-throughs settings
	// via the shared cloud client; the KubeConfigWatcher is shared with the gRPC
	// KubeContextService so a context switch over either transport fans out to both.
	graphql := graph.NewServer(&graph.Resolver{
		Cloud:             cloudClient,
		Store:             syncStore,
		Hub:               hub,
		Sync:              engine,
		Queue:             queue,
		KubeConfigWatcher: watcher,
	})
	grpcSrv := grpcserver.NewServer(watcher)

	// Routing: the GraphQL server at /graphql, the host-only control endpoints
	// alongside it. The credential push writes the same Holder the engine
	// authenticates with; /control/wake triggers an immediate engine resync.
	mux := http.NewServeMux()
	mux.Handle("/control/credentials", controlCredentials(creds))
	mux.Handle("/control/wake", controlWake(engine.Poke))
	mux.Handle("/graphql", graphql)

	return &App{
		// gRPC (host-internal control channel) shares the socket with GraphQL
		// via h2c: NewH2CHandler routes HTTP/2 application/grpc requests to the
		// gRPC server and everything else (HTTP/1.1 GraphQL POST/SSE, /control/*)
		// to the mux above.
		handler: grpcserver.NewH2CHandler(mux, grpcSrv.GRPC()),
		graphql: graphql,
		grpcSrv: grpcSrv,
		watcher: watcher,
		engine:  engine,
	}, nil
}

// ServeHTTP implements http.Handler, dispatching to the h2c multiplexer.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

// Start launches the sync engine, bound to a context derived from ctx so a
// cancel of ctx (or Close) stops it. Call once, before serving.
func (a *App) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	a.engineCancel = cancel
	a.engineDone = make(chan struct{})
	go func() {
		a.engine.Run(ctx)
		close(a.engineDone)
	}()
}

// NotifyShutdown signals both transports' long-lived streams to close cleanly:
// gRPC Watch handlers return (OK trailers flush) and SSE subscriptions flush
// their terminal frame. Wire it into http.Server.RegisterOnShutdown so it fires
// the moment Shutdown begins. The two transports are independent — neither
// ordering nor BaseContext is involved — so a single fan-out is safe.
func (a *App) NotifyShutdown() {
	a.grpcSrv.NotifyShutdown()
	a.graphql.NotifyShutdown()
}

// DrainWithContext waits for both transports' handlers to unwind (so their
// terminal frames/trailers are flushed) or ctx to expire. Call after
// http.Server.Shutdown, which has already drained the non-hijacked GraphQL
// requests; this additionally waits on the hijacked h2c gRPC streams that
// Shutdown does not track.
func (a *App) DrainWithContext(ctx context.Context) error {
	errs := make(chan error, 2)
	go func() { errs <- a.graphql.DrainWithContext(ctx) }()
	go func() { errs <- a.grpcSrv.DrainWithContext(ctx) }()
	return errors.Join(<-errs, <-errs)
}

// Close releases resources after DrainWithContext returns: it stops the gRPC
// transports, stops the sync engine (bounded so a wedged engine can't hang
// exit), and closes the kube-config watcher. Safe to call without Start.
func (a *App) Close() error {
	a.grpcSrv.Stop()
	if a.engineCancel != nil {
		a.engineCancel()
		select {
		case <-a.engineDone:
		case <-time.After(2 * time.Second):
			slog.Warn("sync engine did not stop within 2s")
		}
	}
	a.watcher.Close()
	return nil
}
