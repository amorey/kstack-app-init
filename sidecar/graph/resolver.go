package graph

//go:generate go run github.com/99designs/gqlgen generate

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"os"
	"strconv"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clusterdata"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clustersync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// Resolver carries the dependencies every operation needs: the kube-config
// watcher (shared with the gRPC KubeContextService) and the per-cluster cache
// read + control surfaces.
type Resolver struct {
	KubeConfigWatcher *k8shelpers.KubeConfigWatcher
	// ClusterData is the read side of the per-cluster SQLite mirror. Always
	// non-nil (the composition root constructs it unconditionally); it tolerates
	// a nil registry internally when the sidecar ran without --data-dir, so the
	// cluster-data resolvers degrade to empty results without nil-guarding.
	ClusterData *clusterdata.Reader
	// ClusterManager is the cluster read+control surface (the `clusters`/
	// `clustersWatch` reads and the enable/delete mutations). nil when no cache
	// is configured; the resolvers guard that and degrade to empty.
	ClusterManager clustersync.Manager
	// Auth is the local-first identity subsystem: its Current/Subscribe read+watch
	// surface backs the `authState` query and `authStateWatch` subscription (identity
	// lives here), and its Login/Logout control backs the `login`/`logout`
	// mutations. nil when no cloud account is configured; the resolvers guard that
	// and degrade to signed-out / no-op.
	Auth auth.Service
}

// tickInterval is the cadence of the `tick` subscription. Overridable via
// SIDECAR_TICK_INTERVAL_MS so the server_test suite can use a sub-second
// cadence without dragging the whole test run.
//
// Lives here (not in schema.resolvers.go) because gqlgen treats that file
// as regenerated and would relocate helper funcs into a comment block on
// every run.
func tickInterval() time.Duration {
	if v := os.Getenv("SIDECAR_TICK_INTERVAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Second
}
