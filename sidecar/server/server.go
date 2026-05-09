// Package server wires the gqlgen executable schema into an http.Handler.
// Keeping this isolated from main() lets us unit-test the GraphQL surface
// without binding a real port.
package server

import (
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/gorilla/websocket"

	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// NewHandler returns an http.Handler that serves GraphQL at /graphql.
func NewHandler() http.Handler {
	srv := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}}))
	// Websocket must be registered first: gqlgen tries transports in order
	// and the WS transport is the only one that handles HTTP Upgrade frames.
	// The Tauri host is the sole client (UDS-local), so accept any origin.
	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})

	mux := http.NewServeMux()
	mux.Handle("/graphql", srv)
	return mux
}
