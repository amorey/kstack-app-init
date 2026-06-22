package graph

//go:generate go run github.com/99designs/gqlgen generate

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"context"
	"errors"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/store"
)

// Resolver carries the dependencies every operation needs. Every field must be
// non-nil — the composition root (internal/app) always wires them and the
// resolvers call them without guards; degraded behavior lives inside the
// services.
type Resolver struct {
	// ClusterClient is the beehive client for the Cluster kind (connection/health
	// status, spec mutations, and watching for changes).
	ClusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]
	// CacheClient is the beehive client for the ClusterCache kind (sync status).
	CacheClient beehive.Client[controllers.ClusterCacheSpec, controllers.ClusterCacheStatus]
	// SrcClient is the beehive client for the ClusterSource kind (used when
	// deleting a cluster to find and delete the parent source so beehive GC
	// cascades to Cluster → ClusterCache).
	SrcClient beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]
	// CacheManager owns the per-cluster SQLite cache files. Nil fields mean the
	// cache is not open for that cluster.
	CacheManager *store.Manager
	// Auth is the local-first identity subsystem: its Current/Subscribe read+watch
	// surface backs the `authState` query and `authStateWatch` subscription
	// (identity lives here), and its StartLogin/Logout control backs the
	// `authLoginStart`/`authLogout` mutations. auth.New degrades internally
	// (signed-out, erroring login) when no cloud account is configured.
	Auth auth.Service
}

// buildCluster assembles a domain controllers.Cluster from a Cluster beehive
// object, joining in ClusterCache sync status from the CacheClient.
func (r *Resolver) buildCluster(ctx context.Context, obj *beehive.Object[controllers.ClusterSpec, controllers.ClusterConnectionStatus]) controllers.Cluster {
	var id controllers.ClusterID
	if obj.Slug != nil {
		id = controllers.ClusterIDFromSlug(*obj.Slug)
	}

	c := controllers.Cluster{
		ID:         id,
		Generation: obj.Generation,
		CreatedAt:  obj.CreatedAt,
		Spec:       obj.Spec,
	}
	if obj.DeletionRequestedAt != nil {
		t := *obj.DeletionRequestedAt
		c.DeletedAt = &t
	}
	if obj.Status != nil {
		c.Status.Source = obj.Status.Source
		c.Status.Server = obj.Status.Server
		c.Status.Principal = obj.Status.Principal
		c.Status.LastConnectedAt = obj.Status.LastConnectedAt
		c.Status.Conditions = obj.Status.Conditions
	}

	// Join in ClusterCache sync status.
	cacheObj, err := r.CacheClient.GetBySlug(ctx, controllers.ClusterCacheSlug(id))
	if err == nil && cacheObj.Status != nil {
		c.Status.SyncStatus = *cacheObj.Status
	}

	return c
}

// buildCacheStats converts a clustercache lookup into the domain CacheStats.
func (r *Resolver) buildCacheStats(ctx context.Context, id controllers.ClusterID) (*controllers.CacheStats, error) {
	bytes, exists := r.CacheManager.CacheBytes(string(id))
	if !exists {
		return &controllers.CacheStats{}, nil
	}
	db := r.CacheManager.Lookup(string(id))
	if db == nil {
		return &controllers.CacheStats{Exists: true, Bytes: bytes}, nil
	}
	rss, err := db.ResourceStats(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]controllers.CachedResourceStats, len(rss))
	for i, rs := range rss {
		resources[i] = controllers.CachedResourceStats{
			Resource:      rs.Resource,
			Count:         rs.Count,
			LastUpdatedAt: rs.LastUpdatedAt,
		}
	}
	return &controllers.CacheStats{Exists: true, Bytes: bytes, Resources: resources}, nil
}

// clusterByID is a convenience helper used by several mutation resolvers.
func (r *Resolver) clusterByID(ctx context.Context, id string) (*beehive.Object[controllers.ClusterSpec, controllers.ClusterConnectionStatus], error) {
	obj, err := r.ClusterClient.GetBySlug(ctx, controllers.ClusterSlug(controllers.ClusterID(id)))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, controllers.ErrNotFound
	}
	return obj, err
}
