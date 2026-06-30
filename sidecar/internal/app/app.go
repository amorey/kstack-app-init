// Package app is the sidecar's composition root and lifecycle owner. It builds
// the shared instances (the poke bus and the cluster, auth, and cloud services),
// wires the GraphQL and gRPC servers, and multiplexes them onto one h2c handler.
// main() stays thin: it binds the listener and drives the shutdown
// surface this package exposes — NotifyShutdown / DrainWithContext / Close —
// mirroring the server/app split used across the kubetail and kstack-cloud
// services.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// defaultKeychainService is the OS-keychain service name the sidecar stores the
// auth token under when Config.KeychainService is empty. It is a product-level,
// frontend-neutral name (not the desktop app's): the sidecar owns the one
// credential and future frontends — e.g. a TUI — read it through the sidecar
// under this same name. The host overrides it (via env) to a dev-specific name
// in development builds so a dev run and an installed release don't share — and
// clobber — the same stored sign-in.
const defaultKeychainService = "Kstack"

// Config is the subset of process configuration the composition root needs.
// main() resolves these from flags/env and hands them over.
type Config struct {
	// KubeconfigPath is an explicit kubeconfig path; empty uses clientcmd's
	// default-loading rules.
	KubeconfigPath string
	// DataDir is the host-supplied per-machine app data dir: it holds the
	// beehive SQLite store, the per-cluster SQLite caches, and the cloud
	// settings-sync file/queue. Required — New errors when empty, so the store
	// can never be created relative to an arbitrary working directory.
	DataDir string
	// CloudURL is the kstack-cloud API base URL. Empty disables the cloud account
	// subsystem (standalone/dev/test runs ⇒ signed-out, no network).
	// Configured via KSTACK_CLOUD_API_URL.
	CloudURL string
	// OAuthIssuerURL is the Hydra OAuth issuer base URL. The auth service derives
	// every endpoint (authorize/token/jwks/revocation) from it, baking in Hydra's
	// standard path layout, so only the issuer + client id need to be configured.
	OAuthIssuerURL string
	// OAuthClientID is the public (PKCE/loopback) OAuth client id.
	OAuthClientID string
	// KeychainService is the OS-keychain service name the auth token is stored
	// under. Empty uses defaultKeychainService; the host sets a dev-specific name
	// in development so dev and release runs don't share the same keychain entry.
	KeychainService string
}

// App owns the composed sidecar: one h2c handler fronting the GraphQL and gRPC
// servers, plus the cluster service (which owns the beehive control plane and
// the kube-config watcher), the auth and cloud services, and the poke bus they
// share. It is an http.Handler; main() mounts it on the listener.
type App struct {
	handler       http.Handler
	graphqlServer *graph.Server
	grpcServer    *grpcserver.Server
	clusterSvc    *cluster.Service
	authSvc       auth.Service
	cloudSvc      *cloud.Service
	pokeSvc       *poke.Service
}

// New builds the composition root, wiring the beehive control-plane, auth, and
// cloud subsystems into the GraphQL and gRPC servers that share one h2c socket.
func New(cfg Config) (*App, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir is required (--data-dir / KSTACK_DATA_DIR)")
	}

	// Tighten client-go's HTTP/2 keepalive so a silently-dropped API-server
	// connection is detected in ~15s instead of client-go's ~45s default — the
	// connection controller's liveness sentinel then sees its watch close promptly
	// and re-probes the cluster's connection. Set once, before any kube client is
	// built.
	cluster.ConfigureKubeHTTP2Keepalive()

	// The resync broadcaster is the shared, cross-subsystem poke bus. It owns the
	// wall-clock gap detector (machine sleep/resume backstop) and accepts pokes
	// from the host via the gRPC PokeService. Built before the cluster service so
	// it can be handed to both it and cloud.
	pokeSvc := poke.New()

	// The cluster service is the whole cluster control plane behind one boundary:
	// it owns the kubeconfig watcher, the beehive store + instance (at
	// <data-dir>/beehive.db), the two controllers (which subscribe to the poke
	// bus for resync), the kubeconfig importer, and the per-cluster cache
	// manager. app hands it the data dir, the kubeconfig path, and the poke bus,
	// then drives Start/Close.
	clusterSvc, err := cluster.New(cfg.DataDir, cfg.KubeconfigPath, pokeSvc)
	if err != nil {
		return nil, err
	}

	// The local-first auth/identity service.
	keychainService := cfg.KeychainService
	if keychainService == "" {
		keychainService = defaultKeychainService
	}
	authSvc, err := auth.New(auth.Config{
		IssuerURL:       cfg.OAuthIssuerURL,
		ClientID:        cfg.OAuthClientID,
		KeychainService: keychainService,
	})
	if err != nil {
		return nil, err
	}

	// The cloud-synced settings service depends on auth.
	cloudSvc, err := cloud.New(cfg.DataDir, cfg.CloudURL, authSvc, pokeSvc)
	if err != nil {
		return nil, err
	}

	graphqlServer := graph.NewServer(&graph.Resolver{
		ClusterSvc: clusterSvc,
		Auth:       authSvc,
	})

	grpcServer := grpcserver.NewServer(authSvc, pokeSvc)

	// Routing: the GraphQL server at /graphql.
	mux := http.NewServeMux()
	mux.Handle("/graphql", graphqlServer)

	// gRPC (host-internal control channel) shares the socket with GraphQL via
	// h2c. The dispatcher routes requests matching grpcserver.IsGRPCRequest
	// (HTTP/2 application/grpc) to the gRPC server and everything else (HTTP/1.1
	// GraphQL POST/SSE, /control/*) to the mux above.
	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if grpcserver.IsGRPCRequest(r) {
			grpcServer.GRPC().ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}), &http2.Server{})

	return &App{
		handler:       handler,
		graphqlServer: graphqlServer,
		grpcServer:    grpcServer,
		clusterSvc:    clusterSvc,
		authSvc:       authSvc,
		cloudSvc:      cloudSvc,
		pokeSvc:       pokeSvc,
	}, nil
}

// ServeHTTP implements http.Handler, dispatching to the h2c multiplexer.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

// Start launches the services' background loops. ctx bounds startup only; the
// returned stop func accepts a drain-deadline context, blocks until all
// background work finishes, and must be called before Close.
func (a *App) Start(ctx context.Context) (func(context.Context) error, error) {
	a.pokeSvc.Start(ctx)

	// Start the cluster service: it starts beehive (the controller harness) and
	// the kubeconfig watcher, then the kubeconfig importer (creates one Cluster
	// per kube-context). The controllers subscribe to the poke bus themselves for
	// resync (connection re-probe + sync-engine restart).
	clusterSvcStop, err := a.clusterSvc.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start cluster service: %w", err)
	}

	a.cloudSvc.Start(ctx)

	stop := func(ctx context.Context) error {
		return errors.Join(clusterSvcStop(ctx), a.cloudSvc.Close())
	}
	return stop, nil
}

// NotifyShutdown signals both transports' long-lived streams to close cleanly:
// gRPC streaming handlers return (OK trailers flush) and SSE subscriptions flush
// their terminal frame.
func (a *App) NotifyShutdown() {
	a.grpcServer.NotifyShutdown()
	a.graphqlServer.NotifyShutdown()
}

// DrainWithContext waits for both transports' handlers to unwind or ctx to
// expire. Call after http.Server.Shutdown.
func (a *App) DrainWithContext(ctx context.Context) error {
	errs := make(chan error, 2)
	go func() { errs <- a.graphqlServer.DrainWithContext(ctx) }()
	go func() { errs <- a.grpcServer.DrainWithContext(ctx) }()
	return errors.Join(<-errs, <-errs)
}

// Close releases OS resources after Stop returns: stops the gRPC transports,
// closes the poke broadcaster, and closes the cluster store.
func (a *App) Close() error {
	a.grpcServer.Stop()
	a.pokeSvc.Close()
	return a.clusterSvc.Close()
}
