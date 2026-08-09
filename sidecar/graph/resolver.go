package graph

//go:generate go run github.com/99designs/gqlgen generate

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
)

// Resolver carries the dependencies every operation needs. Every field must be
// non-nil — the composition root (internal/app) always wires them and the
// resolvers call them without guards; degraded behavior lives inside the
// services.
type Resolver struct {
	// ClusterSvc is the boundary to the cluster backend: every cluster query,
	// mutation, and subscription delegates to it. It hides beehive (names, the
	// Cluster → ClusterCache owner chain, the spec/status split, the
	// cache-status join, the watch merge) behind the domain Cluster type.
	// (Named ClusterSvc, not Clusters, to avoid shadowing the generated
	// queryResolver.Clusters method.)
	ClusterSvc cluster.ClusterService
	// Auth is the local-first identity subsystem: its Current/Subscribe read+watch
	// surface backs the `authState` query and `authStateWatch` subscription
	// (identity lives here), and its StartLogin/Logout control backs the
	// `authLoginStart`/`authLogout` mutations. auth.New degrades internally
	// (signed-out, erroring login) when no cloud account is configured.
	Auth auth.Service
}
