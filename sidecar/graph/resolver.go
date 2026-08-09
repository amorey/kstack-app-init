package graph

//go:generate go run github.com/99designs/gqlgen generate

// Hand-written; gqlgen never regenerates this file. Add resolver dependencies here.

import (
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
)

// Resolver carries every operation's dependencies. Each field MUST be non-nil — the
// composition root always wires them and the resolvers call them without guards;
// degraded behavior lives inside the services.
type Resolver struct {
	// ClusterSvc is the boundary to the cluster backend, hiding beehive behind the
	// domain types. (Named ClusterSvc to avoid shadowing the generated
	// queryResolver.Clusters method.)
	ClusterSvc cluster.ClusterService
	// Auth backs the authState query/watch and the login/logout mutations; it degrades
	// internally when no cloud account is configured.
	Auth auth.Service
}
