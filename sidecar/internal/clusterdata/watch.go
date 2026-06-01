package clusterdata

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
)

// WatchPods streams the cluster's pod list: a snapshot first, then a fresh
// snapshot every time the local cache changes.
func (r *Reader) WatchPods(ctx context.Context, clusterUUID string) (<-chan []Pod, error) {
	return watch(ctx, r.cache, clusterUUID, "pods", loadPods)
}

// WatchServices streams the cluster's service list, same shape as WatchPods.
func (r *Reader) WatchServices(ctx context.Context, clusterUUID string) (<-chan []Service, error) {
	return watch(ctx, r.cache, clusterUUID, "services", loadServices)
}

// WatchDeployments streams the cluster's deployment list.
func (r *Reader) WatchDeployments(ctx context.Context, clusterUUID string) (<-chan []Deployment, error) {
	return watch(ctx, r.cache, clusterUUID, "deployments", loadDeployments)
}

// WatchNodes streams the cluster's node list.
func (r *Reader) WatchNodes(ctx context.Context, clusterUUID string) (<-chan []Node, error) {
	return watch(ctx, r.cache, clusterUUID, "nodes", loadNodes)
}

// watch is the shared subscription body for every typed watcher. It opens the
// cluster cache once, subscribes to its change notifier, pushes an initial
// snapshot, then re-snapshots on every wake. Reads run against the reader pool
// so they never block the upstream writer. The returned channel closes when ctx
// is cancelled or the cache's notifier closes.
//
// A watch only attaches to a cluster the cache already has OPEN (via Lookup,
// never Open): a read must not open/resume a cluster's sync. So no configured
// cache (nil Manager), a disabled/frozen cluster, or an absent one all yield an
// already-closed channel — subscriptions degrade gracefully exactly like the
// snapshot queries do.
//
// A free function rather than a method because Go methods can't take their own
// type parameter.
func watch[T any](
	ctx context.Context,
	cache *clustercache.Manager,
	clusterUUID string,
	kind string,
	load func(ctx context.Context, db *sql.DB, clusterUUID string) ([]T, error),
) (<-chan []T, error) {
	var cdb *clustercache.ClusterDB
	if cache != nil {
		cdb = cache.Lookup(clusterUUID)
	}
	if cdb == nil {
		out := make(chan []T)
		close(out)
		return out, nil
	}
	notify, unsub := cdb.Subscribe()

	out := make(chan []T)
	go func() {
		defer close(out)
		defer unsub()

		if snap, err := load(ctx, cdb.Reader(), clusterUUID); err == nil {
			select {
			case out <- snap:
			case <-ctx.Done():
				return
			}
		} else {
			slog.Warn("clusterdata watch initial snapshot failed", "kind", kind, "err", err)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-notify:
				if !ok {
					return
				}
			}
			snap, err := load(ctx, cdb.Reader(), clusterUUID)
			if err != nil {
				slog.Warn("clusterdata watch re-query failed", "kind", kind, "err", err)
				continue
			}
			select {
			case out <- snap:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
