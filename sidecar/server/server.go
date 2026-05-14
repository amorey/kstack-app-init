// Package server wires the gqlgen executable schema into an http.Handler.
// Keeping this isolated from main() lets us unit-test the GraphQL surface
// without binding a real port.
package server

import (
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/gorilla/websocket"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// Config bundles the configurable pieces of the sidecar handler. main()
// builds one from flags/env; tests build one inline. CloudURL is the cloud
// API's base URL — `cloud.New` appends `/graphql` itself.
type Config struct {
	CloudURL  string
	PrefsPath string
}

// NewHandler returns an http.Handler that serves GraphQL at /graphql,
// using a Resolver constructed from cfg.
func NewHandler(cfg Config) http.Handler {
	r := &graph.Resolver{
		Cloud: cloud.New(cfg.CloudURL),
		Store: prefs.NewStore(cfg.PrefsPath),
		Hub:   prefs.NewHub(),
	}
	return NewHandlerWithResolver(r)
}

// NewHandlerWithResolver builds a handler around a fully-constructed
// Resolver. Used by tests that need to inject a fake cloud endpoint or
// inspect the local store directly.
func NewHandlerWithResolver(r *graph.Resolver) http.Handler {
	srv := handler.New(graph.NewExecutableSchema(graph.Config{Resolvers: r}))
	// Websocket must be registered first: gqlgen tries transports in order
	// and the WS transport is the only one that handles HTTP Upgrade frames.
	// The Tauri host is the sole client (UDS-local), so accept any origin.
	srv.AddTransport(transport.Websocket{
		KeepAlivePingInterval: 10 * time.Second,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		// graphql-transport-ws lets clients ship credentials in the
		// connection_init payload. The Tauri host uses this for the WS
		// path (the upgrade headers go through urql's
		// graphql-ws-client, which can't easily inject custom headers
		// per-connection on every platform).
		InitFunc: func(ctx context.Context, initPayload transport.InitPayload) (context.Context, *transport.InitPayload, error) {
			auth, _ := initPayload["Authorization"].(string)
			return graph.WithAuthHeader(ctx, auth), nil, nil
		},
	})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.SSE{KeepAlivePingInterval: 10 * time.Second})

	// Single seam for resolver / parse errors. We log operation name and
	// path but not `variables` — they can carry auth tokens or PII.
	srv.SetErrorPresenter(func(ctx context.Context, e error) *gqlerror.Error {
		err := graphql.DefaultErrorPresenter(ctx, e)
		op, opName := "", ""
		if oc := graphql.GetOperationContext(ctx); oc != nil {
			opName = oc.OperationName
			op = oc.RawQuery
		}
		slog.ErrorContext(ctx, "graphql error",
			"op", opName,
			"path", err.Path.String(),
			"error", err.Message,
			"raw", op,
		)
		return err
	})

	mux := http.NewServeMux()
	// Wrap the GraphQL handler in a middleware that lifts the
	// Authorization header off the *http.Request and into the
	// context that resolvers see — the standard pattern for keeping
	// resolvers HTTP-transport-agnostic.
	mux.Handle("/graphql", withBearer(srv))
	return mux
}

func withBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := graph.WithRequestContext(r.Context(), r)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// DefaultPrefsPath returns the on-disk path used when --prefs-path is not
// supplied. Falls back to /tmp on systems where UserConfigDir errors —
// good enough for a desktop POC; main() can override.
func DefaultPrefsPath() string {
	dir, err := userConfigDir()
	if err != nil {
		dir = "/tmp"
	}
	return filepath.Join(dir, "kstack", "preferences.json")
}
