// Package app is the sidecar's composition root and lifecycle owner. It builds
// the shared instances (the poke bus and the cluster, auth, and cloud services),
// wires the GraphQL and gRPC servers, and multiplexes them onto one h2c handler.
// main() stays thin: it binds the listener and drives the shutdown surface this
// package exposes — NotifyShutdown / DrainWithContext / Close.
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
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// defaultKeychainService is the OS-keychain service name used when
// Config.KeychainService is empty; deliberately frontend-neutral.
const defaultKeychainService = "Kstack"

// Config is the subset of process configuration the composition root needs.
// main() resolves these from flags/env and hands them over.
type Config struct {
	// KubeconfigPath is an explicit kubeconfig path; empty uses clientcmd's
	// default-loading rules.
	KubeconfigPath string
	// DataDir holds the beehive store, per-cluster caches, and cloud settings
	// file/queue. Required — New errors when empty, so stores are never created
	// relative to an arbitrary working directory.
	DataDir string
	// CloudURL is the kstack-cloud API base URL. Empty disables the cloud
	// subsystem (signed-out, no network).
	CloudURL string
	// OAuthIssuerURL is the Hydra OAuth issuer base URL; auth derives every
	// endpoint from it via Hydra's standard path layout.
	OAuthIssuerURL string
	// OAuthClientID is the public (PKCE/loopback) OAuth client id.
	OAuthClientID string
	// KeychainService is the OS-keychain service name for the auth token. Empty
	// uses defaultKeychainService; dev runs set a distinct name so dev and
	// release don't share a keychain entry.
	KeychainService string
}

// App owns the composed sidecar: one h2c handler fronting the GraphQL and gRPC
// servers, over the services in parts. It is an http.Handler; main() mounts it on
// the listener.
type App struct {
	handler       http.Handler
	graphqlServer *graph.Server
	grpcServer    *grpcserver.Server

	// parts is start order; stop and close run in reverse, which is what keeps poke's
	// hub open until its subscribers have drained.
	parts []lifecycle.Part
}

// New builds the composition root, wiring the beehive control-plane, auth, and
// cloud subsystems into the GraphQL and gRPC servers that share one h2c socket.
func New(cfg Config) (*App, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir is required (--data-dir / KSTACK_DATA_DIR)")
	}

	// Shared cross-subsystem poke bus (wall-clock gap detector + host pokes via
	// gRPC PokeService); see docs/adr/2026-08-09-poke-resync-fanout.md.
	pokeSvc := poke.New()

	// The cluster backend behind one boundary. Mid-rebuild: beehive, the four
	// controllers, and the kubeconfig watcher/notifier are wired and run, but they
	// reconcile to no-ops and every read panics.
	// One reader of the user's kubeconfig, shared by everything that resolves a
	// context. Closing it ends every subscription, so it is the app's alone.
	kubeconfigSvc := kubeconfig.New(cfg.KubeconfigPath, pokeSvc)

	// One connection per set of credentials, shared by everything that talks to a
	// cluster. Keyed on credentials rather than on a cluster, so two contexts aimed at
	// one server as one user are one socket.
	kubeconnSvc := kubeconn.New(kubeconn.DefaultBudget)

	clusterSvc, err := clustersvc.New(cfg.DataDir, kubeconfigSvc, pokeSvc)
	if err != nil {
		return nil, err
	}

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

	// cloud depends on auth, never the reverse; see
	// docs/adr/2026-08-09-local-first-auth-settings.md.
	cloudSvc, err := cloud.New(cfg.DataDir, cfg.CloudURL, authSvc, pokeSvc)
	if err != nil {
		return nil, err
	}

	graphqlServer := graph.NewServer(&graph.Resolver{
		ClusterSvc: clusterSvc,
		Auth:       authSvc,
	})

	grpcServer := grpcserver.NewServer(authSvc, pokeSvc)

	mux := http.NewServeMux()
	mux.Handle("/graphql", graphqlServer)

	// gRPC shares the socket with GraphQL via h2c: HTTP/2 application/grpc goes
	// to the gRPC server, everything else to the mux. See
	// docs/adr/2026-08-09-single-socket-h2c.md.
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
		parts: []lifecycle.Part{
			{Name: "poke service", StartCloser: lifecycle.StartFunc(pokeSvc.Start)},
			{Name: "kubeconfig service", StartCloser: kubeconfigSvc},
			{Name: "kubeconn service", StartCloser: kubeconnSvc},
			{Name: "cluster service", StartCloser: clusterSvc},
			{Name: "cloud service", StartCloser: lifecycle.StartFunc(cloudSvc.Start)},
		},
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
	return lifecycle.StartAll(ctx, a.parts)
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

// Close releases OS resources after the stop func returns. The gRPC transports go
// first, outside the composed parts: GracefulStop panics on the h2c path, so Stop only
// ever runs here, after the drain.
func (a *App) Close() error {
	a.grpcServer.Stop()
	return lifecycle.CloseAll(a.parts)
}
