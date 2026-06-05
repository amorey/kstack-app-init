// Package cluster is the cluster-cache subsystem's composition + lifecycle
// owner — the umbrella over the clustercache/clusterdata/clusterregistry/
// clustersync sub-packages. Its Service bundles the per-cluster SQLite cache,
// the durable cluster registry (in app.db), and the coordinator that keeps both
// in lockstep with the kube-config watcher — the three pieces that are enabled
// together only when the host supplies a data dir. The sidecar's composition
// root (internal/app) builds one Service, threads its Reader + Manager into the
// GraphQL resolver, runs it via Start, and tears it down via Close. Keeping the
// wiring and the teardown ordering here (rather than spread across app.go) means
// the coordinator/cache/appDB that already depend on each other can't drift apart.
package cluster

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/appdb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clustercache"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clusterdata"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clusterregistry"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clustersync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// cacheShutdownTimeout bounds the per-cluster cache drain in Close so a wedged
// sync goroutine can't hang process exit.
const cacheShutdownTimeout = 2 * time.Second

// Service owns the cluster-cache subsystem: the per-cluster SQLite cache, the
// durable registry (in app.db), and the coordinator that reconciles them
// against the kube-config watcher. The cache/coordinator/appDB are nil when no
// data dir was supplied (standalone/dev/test) — Start and Close then no-op for
// them — but the Reader is always non-nil so the cluster-data resolvers can
// call it unconditionally and simply read empty.
type Service struct {
	cache       *clustercache.Manager
	coordinator *clustersync.Coordinator
	appDB       *appdb.DB
	reader      *clusterdata.Reader

	// cancel stops the coordinator's reconcile loop (the context Start derives);
	// nil until Start runs (and always nil with no data dir).
	cancel context.CancelFunc
}

// New builds the cluster-cache subsystem. With an empty dataDir it returns a
// degraded Service: no cache/registry/coordinator, and a Reader over a nil
// cache that yields empty snapshots and closed watches. With a dataDir it opens
// app.db, builds the registry + per-cluster cache, and wires a coordinator that
// keeps them in lockstep with watcher. The returned Service is always non-nil.
// pokeSvc is the shared resync broadcaster handed to the per-cluster cache so a
// poke (machine wake / host network-on) restarts the cluster's reflectors; nil
// disables resync (degraded/test runs).
func New(dataDir string, watcher *k8shelpers.KubeConfigWatcher, pokeSvc *poke.Service) (*Service, error) {
	s := &Service{}
	if dataDir != "" {
		s.cache = clustercache.NewManager(dataDir, nil, pokeSvc)
		db, err := appdb.Open(filepath.Join(dataDir, "app.db"))
		if err != nil {
			return nil, err
		}
		s.appDB = db
		registry := clusterregistry.New(db.SQL())
		s.coordinator = clustersync.NewCoordinator(s.cache, watcher, registry)
	}
	// Reader is always non-nil and tolerates a nil cache, so nil-tolerance lives
	// here once rather than at every resolver call site.
	s.reader = clusterdata.NewReader(s.cache)
	return s, nil
}

// Reader is the cluster-data read side handed to the resolver as ClusterData.
// Always non-nil.
func (s *Service) Reader() *clusterdata.Reader {
	return s.reader
}

// Manager is the cluster read+control surface handed to the resolver as
// ClusterManager. It is a nil interface (not a typed-nil) when no data dir was
// supplied, so the resolver's nil-guard degrades the cluster surface cleanly.
func (s *Service) Manager() clustersync.Manager {
	if s.coordinator == nil {
		return nil
	}
	return s.coordinator
}

// Start launches the coordinator's reconcile loop, bound to a context derived
// from ctx so a cancel of ctx (or Close) stops it. No-op without a data dir.
// Call once, before serving.
func (s *Service) Start(ctx context.Context) {
	if s.coordinator == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	// The coordinator reconciles the cluster cache against the kube-config
	// watcher for the app's lifetime; it stops when this ctx is cancelled (by
	// Close or a parent cancel). Per-cluster sync goroutines it opens are torn
	// down by cache.Shutdown in Close.
	go s.coordinator.Run(ctx)
}

// Close stops the coordinator's reconcile loop, drains the per-cluster cache
// (bounded), and closes app.db — in that order. Stopping the loop first means
// no new clusters can open while the cache drains, and appDB closes last so the
// (now-stopped) coordinator + per-cluster sync goroutines have finished writing
// the registry. Safe to call without Start and with no data dir.
func (s *Service) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.cache != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cacheShutdownTimeout)
		if err := s.cache.Shutdown(shutdownCtx); err != nil {
			slog.Warn("cluster cache shutdown", "err", err)
		}
		cancel()
	}
	if s.appDB != nil {
		if err := s.appDB.Close(); err != nil {
			slog.Warn("app db close", "err", err)
		}
	}
	return nil
}
