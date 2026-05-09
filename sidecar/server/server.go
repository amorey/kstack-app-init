// Package server wires the gqlgen executable schema into an http.Handler.
// Keeping this isolated from main() lets us unit-test the GraphQL surface
// without binding a real port.
package server

import (
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"

	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// NewHandler returns an http.Handler that serves GraphQL at /graphql.
func NewHandler() http.Handler {
	srv := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}}))
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})

	mux := http.NewServeMux()
	mux.Handle("/graphql", srv)
	return mux
}
