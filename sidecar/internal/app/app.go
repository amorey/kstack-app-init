// Package app is the sidecar's composition root and lifecycle owner. It builds
// the shared instances (the kube-config watcher, beehive store, and controllers),
// wires the GraphQL and gRPC servers, and multiplexes them onto one h2c
// handler. main() stays thin: it binds the listener and drives the shutdown
// surface this package exposes — NotifyShutdown / DrainWithContext / Close —
// mirroring the server/app split used across the kubetail and kstack-cloud
// services.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache"
	cachestore "github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustersource"
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
// servers, plus the beehive store, controllers, and kube-config watcher they
// share. It is an http.Handler; main() mounts it on the listener.
type App struct {
	handler       http.Handler
	graphqlServer *graph.Server
	grpcServer    *grpcserver.Server
	bh            *beehive.Beehive
	bhStore       beehive.Store
	bhStop        func(context.Context) error
	importer      *clustersource.KubeconfigImporter
	watcher       *k8shelpers.KubeConfigWatcher
	cacheManager  *cachestore.Manager
	clusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]
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

	// Open the beehive SQLite store at <data-dir>/beehive.db. The beehive instance
	// owns the three resource kinds (ClusterSource, Cluster, ClusterCache) and
	// drives their controllers level-triggered.
	store, err := beehivesqlite.Open(filepath.Join(cfg.DataDir, "beehive.db"))
	if err != nil {
		return nil, fmt.Errorf("open beehive store: %w", err)
	}
	bh, err := beehive.New(store)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("init beehive: %w", err)
	}

	// The resync broadcaster is the shared, cross-subsystem poke bus. It owns the
	// wall-clock gap detector (machine sleep/resume backstop) and accepts pokes
	// from the host via the gRPC PokeService. Built before controllers so it can
	// be handed to both.
	pokeSvc := poke.New()

	// The kubeconfig watcher publishes full *api.Config snapshots on every
	// kubeconfig change (fsnotify + 100ms debounce). The importer and
	// ClusterController consume it.
	watcher, err := k8shelpers.NewKubeConfigWatcher(cfg.KubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("init kubeconfig watcher: %w", err)
	}

	// Build beehive clients — one per kind. Resolvers and helpers hold these.
	srcClient := beehive.NewClient[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus](bh, controllers.ClusterSourceGroupKind)
	clusterClient := beehive.NewClient[controllers.ClusterSpec, controllers.ClusterConnectionStatus](bh, controllers.ClusterGroupKind)
	cacheClient := beehive.NewClient[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus](bh, controllers.ClusterCacheGroupKind)

	// The store.Manager owns the per-cluster SQLite cache files under
	// <data-dir>/clusters/.
	cacheManager := cachestore.NewManager(cfg.DataDir)

	// Register the three controllers. The ClusterCacheController uses the
	// production sync engine (real network).
	srcCtrl := clustersource.NewClusterSourceController(clusterClient)
	clusterCtrl := cluster.NewClusterController(watcher, cacheClient, nil, nil)
	cacheCtrl := clustercache.NewClusterCacheController(watcher, clusterClient, cacheManager)

	if err := beehive.Register(bh, controllers.ClusterSourceGroupKind, srcCtrl); err != nil {
		return nil, fmt.Errorf("register ClusterSource controller: %w", err)
	}
	if err := beehive.Register(bh, controllers.ClusterGroupKind, clusterCtrl); err != nil {
		return nil, fmt.Errorf("register Cluster controller: %w", err)
	}
	if err := beehive.Register(bh, controllers.ClusterCacheGroupKind, cacheCtrl); err != nil {
		return nil, fmt.Errorf("register ClusterCache controller: %w", err)
	}

	// The kubeconfig importer creates/updates/orphans ClusterSource objects as
	// the kubeconfig changes.
	importer := clustersource.NewKubeconfigImporter(watcher, srcClient)

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
		ClusterClient: clusterClient,
		CacheClient:   cacheClient,
		SrcClient:     srcClient,
		CacheManager:  cacheManager,
		Auth:          authSvc,
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
		bh:            bh,
		bhStore:       store,
		importer:      importer,
		watcher:       watcher,
		cacheManager:  cacheManager,
		clusterClient: clusterClient,
		authSvc:       authSvc,
		cloudSvc:      cloudSvc,
		pokeSvc:       pokeSvc,
	}, nil
}

// ServeHTTP implements http.Handler, dispatching to the h2c multiplexer.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

// Start launches the services' background loops. ctx reaches the poke and
// cloud services (whose loops it can stop). Call once, before serving.
func (a *App) Start(ctx context.Context) error {
	// Start beehive (the controller harness + store subscription loop). ctx
	// bounds startup only; the returned stop tears the control plane down at
	// Close. The long-lived reconcile loops outlive ctx.
	stop, err := a.bh.Start(ctx)
	if err != nil {
		return fmt.Errorf("start beehive: %w", err)
	}
	a.bhStop = stop

	a.pokeSvc.Start(ctx)

	// Start the kubeconfig watcher; its fsnotify loop publishes snapshots.
	a.watcher.Start()

	// Start the importer; it subscribes to the watcher and seeds ClusterSource
	// objects for every kubeconfig context.
	a.importer.Start()

	// Forward every poke to ClusterCache objects so running sync engines bounce.
	go func() {
		ch, unsub := a.pokeSvc.Subscribe()
		defer unsub()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				// Bump PokeSyncGeneration on all Cluster objects.
				a.pokeAllClusters(ctx)
			}
		}
	}()

	a.cloudSvc.Start(ctx)
	return nil
}

// pokeAllClusters increments PokeSyncGeneration on every Cluster object so the
// ClusterCacheController bounces all running sync engines.
func (a *App) pokeAllClusters(ctx context.Context) {
	objs, err := a.clusterClient.List(ctx)
	if err != nil {
		return
	}
	for _, obj := range objs {
		spec := obj.Spec
		spec.PokeSyncGeneration++
		_, _ = a.clusterClient.Update(ctx, obj.ID, spec)
	}
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

// Close releases resources after DrainWithContext returns. Stops the gRPC
// transports, stops the importer and watcher, stops beehive (all controllers
// and engines) and closes its store, shuts down the cache manager, then closes
// cloud and auth.
func (a *App) Close() error {
	a.grpcServer.Stop()
	a.pokeSvc.Close()
	a.importer.Stop()
	a.watcher.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if a.bhStop != nil {
		_ = a.bhStop(stopCtx)
	}
	// beehive does not own the store's lifecycle, so close it ourselves — after
	// the control plane has stopped, since its writers and watchers run against it.
	var storeErr error
	if a.bhStore != nil {
		storeErr = a.bhStore.Close()
	}
	cacheErr := a.cacheManager.Shutdown(stopCtx)
	return errors.Join(storeErr, cacheErr, a.cloudSvc.Close())
}
