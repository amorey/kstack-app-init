// Package app is the sidecar's composition root and lifecycle owner. It builds
// the shared instances (the kube-config watcher and, when a data dir is given,
// the cluster-cache service), wires the GraphQL and gRPC servers, and
// multiplexes them onto one h2c handler. main() stays thin: it binds the
// listener and drives the shutdown surface this package exposes — NotifyShutdown
// / DrainWithContext / Close — mirroring the server/app split used across the
// kubetail and kstack-cloud services.
package app

import (
	"context"
	"errors"
	"net/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
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
	// DataDir is the host-supplied per-machine app data dir for the per-cluster
	// SQLite cache. Empty disables the cluster cache (standalone/dev/test runs);
	// the cluster-data resolvers then degrade to empty results. It also holds the
	// cloud settings-sync file/queue when cloud is configured.
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
// servers, plus the cluster-cache service and kube-config watcher they share. It
// is an http.Handler; main() mounts it on the listener.
type App struct {
	handler           http.Handler
	graphqlServer     *graph.Server
	grpcServer        *grpcserver.Server
	kubeConfigWatcher *k8shelpers.KubeConfigWatcher
	clusterSvc        *cluster.Service
	authSvc           auth.Service
	cloudSvc          *cloud.Service
	pokeSvc           *poke.Service
}

// New builds the composition root. The kube-config watcher is shared by the
// GraphQL resolver and the gRPC KubeContextService, so a context switch over
// either transport fans out to both.
func New(cfg Config) (*App, error) {
	// Always non-nil: the watcher tolerates missing/malformed kubeconfigs and
	// seeds with an empty *api.Config so resolvers stay safe. Only fatal here is
	// a kernel-level fsnotify failure (ENOMEM, ulimit).
	kubeConfigWatcher, err := k8shelpers.NewKubeConfigWatcher(cfg.KubeconfigPath)
	if err != nil {
		return nil, err
	}

	// The resync broadcaster is the shared, cross-subsystem poke bus. It owns the
	// wall-clock gap detector (machine sleep/resume backstop) and accepts pokes
	// from the host via the gRPC PokeService. App owns its lifecycle; the cluster
	// cache and cloud settings engine subscribe to it. Built before cluster/cloud
	// so it can be handed to both.
	pokeSvc := poke.New()

	// The cluster-cache service owns the per-cluster SQLite cache, the durable
	// registry, and the coordinator that keeps both in lockstep with the
	// kube-config watcher. Enabled only when the host supplied a data dir; with
	// an empty DataDir it degrades to a Reader that yields empty results and a
	// nil ClusterManager, so the cluster resolvers stay safe. The poke bus lets a
	// machine-wake / host network-on event restart its per-cluster reflectors.
	clusterSvc, err := cluster.New(cfg.DataDir, kubeConfigWatcher, pokeSvc)
	if err != nil {
		return nil, err
	}

	// The local-first auth/identity service. The sidecar owns the OS keychain
	// directly (loopback OAuth already pins the browser to this machine), so we
	// hand auth a keychain service name and it builds its own keyring store — no
	// host channel. auth.New degrades when neither a service name nor a store is
	// given (we always pass a name here, so it's signed-out until the user signs
	// in); its Session surface is always non-nil and safe.
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

	// The cloud-synced settings service depends on auth (for the token source and
	// the session change signal). It degrades when CloudURL/DataDir are empty
	// (standalone/test runs ⇒ no settings sync).
	cloudSvc, err := cloud.New(cfg.DataDir, cfg.CloudURL, authSvc, pokeSvc)
	if err != nil {
		return nil, err
	}

	// The KubeConfigWatcher is shared with the gRPC KubeContextService so a
	// context switch over either transport fans out to both.
	graphqlServer := graph.NewServer(&graph.Resolver{
		KubeConfigWatcher: kubeConfigWatcher,
		ClusterData:       clusterSvc.Reader(),
		ClusterManager:    clusterSvc.Manager(),
		Auth:              authSvc,
	})
	grpcServer := grpcserver.NewServer(kubeConfigWatcher, authSvc, pokeSvc)

	// Routing: the GraphQL server at /graphql.
	mux := http.NewServeMux()
	mux.Handle("/graphql", graphqlServer)

	// gRPC (host-internal control channel) shares the socket with GraphQL via
	// h2c. The dispatcher routes requests matching grpcserver.IsGRPCRequest
	// (HTTP/2 application/grpc) to the gRPC server and everything else (HTTP/1.1
	// GraphQL POST/SSE, /control/*) to the mux above — grpc/ owns the "what is a
	// gRPC request" rule, the composition here owns that the two surfaces share
	// one socket. h2c.NewHandler serves HTTP/1.1 unchanged and upgrades only the
	// HTTP/2 prior-knowledge preface (what the Rust tonic client sends), so SSE
	// keeps its Flusher-backed writer and streams unbuffered.
	handler := h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if grpcserver.IsGRPCRequest(r) {
			grpcServer.GRPC().ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}), &http2.Server{})

	return &App{
		handler:           handler,
		graphqlServer:     graphqlServer,
		grpcServer:        grpcServer,
		kubeConfigWatcher: kubeConfigWatcher,
		clusterSvc:        clusterSvc,
		authSvc:           authSvc,
		cloudSvc:          cloudSvc,
		pokeSvc:           pokeSvc,
	}, nil
}

// ServeHTTP implements http.Handler, dispatching to the h2c multiplexer.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

// Start launches the cluster-cache service's reconcile loop, bound to a context
// derived from ctx so a cancel of ctx (or Close) stops it. Call once, before
// serving.
func (a *App) Start(ctx context.Context) {
	a.pokeSvc.Start(ctx)
	a.clusterSvc.Start(ctx)
	a.cloudSvc.Start(ctx)
}

// NotifyShutdown signals both transports' long-lived streams to close cleanly:
// gRPC Watch handlers return (OK trailers flush) and SSE subscriptions flush
// their terminal frame. Wire it into http.Server.RegisterOnShutdown so it fires
// the moment Shutdown begins. The two transports are independent — neither
// ordering nor BaseContext is involved — so a single fan-out is safe.
func (a *App) NotifyShutdown() {
	a.grpcServer.NotifyShutdown()
	a.graphqlServer.NotifyShutdown()
}

// DrainWithContext waits for both transports' handlers to unwind (so their
// terminal frames/trailers are flushed) or ctx to expire. Call after
// http.Server.Shutdown, which has already drained the non-hijacked GraphQL
// requests; this additionally waits on the hijacked h2c gRPC streams that
// Shutdown does not track.
func (a *App) DrainWithContext(ctx context.Context) error {
	errs := make(chan error, 2)
	go func() { errs <- a.graphqlServer.DrainWithContext(ctx) }()
	go func() { errs <- a.grpcServer.DrainWithContext(ctx) }()
	return errors.Join(<-errs, <-errs)
}

// Close releases resources after DrainWithContext returns: it stops the gRPC
// transports, closes the cluster-cache service (which stops the coordinator,
// drains the per-cluster cache, and closes app.db), and closes the kube-config
// watcher. The service is closed before the watcher so the coordinator's
// reconcile loop — the watcher's only subscriber here — has stopped before its
// source goes away. Safe to call without Start.
func (a *App) Close() error {
	a.grpcServer.Stop()
	cloudErr := a.cloudSvc.Close()
	clusterErr := a.clusterSvc.Close()
	a.pokeSvc.Close()
	a.kubeConfigWatcher.Close()
	return errors.Join(cloudErr, clusterErr)
}
