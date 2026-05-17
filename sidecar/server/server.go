// Package server wires the gqlgen executable schema into an http.Handler.
// Keeping this isolated from main() lets us unit-test the GraphQL surface
// without binding a real port.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"path/filepath"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/gorilla/websocket"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// noopStatus is the default StatusSource when none is wired (Config{} in
// tests / surfaces that don't run the engine): reports Offline and an
// already-closed watch, so syncStatus/syncStatusWatch degrade gracefully
// instead of nil-panicking — matching the Store/Hub/Creds nil-defaults.
type noopStatus struct{}

func (noopStatus) Status() syncpkg.Status {
	return syncpkg.Status{State: syncpkg.StateOffline}
}

func (noopStatus) WatchStatus() (<-chan syncpkg.Status, func()) {
	ch := make(chan syncpkg.Status)
	close(ch)
	return ch, func() {}
}

// Config bundles the configurable pieces of the sidecar handler. CloudURL
// is the cloud API's base URL — `cloud.New` appends `/graphql` itself.
// Store/Hub/Creds/Sync are the engine-shared instances main() builds: the
// Resolver reads the same syncstore the engine writes, subscribes the same
// Hub it publishes to, the /control/credentials endpoint writes the same
// Holder the engine authenticates with, and syncStatus reads the engine.
// main() owns these because the engine must exist before the Resolver
// (Sync) yet needs Creds — a cycle only the composition root can break.
// nil ⇒ fresh empties for tests that don't touch those surfaces.
type Config struct {
	CloudURL string
	Store    *syncstore.Store[prefs.Settings]
	Hub      *prefs.Hub
	Creds    *authcreds.Holder
	Sync     graph.StatusSource
}

// NewHandler returns an http.Handler serving GraphQL at /graphql plus the
// host-only /control/credentials endpoint.
func NewHandler(cfg Config) http.Handler {
	store := cfg.Store
	if store == nil {
		// Tests that pass Config{} never exercise the settings surface;
		// a never-read store keeps the Resolver non-nil.
		store = syncstore.NewStore[prefs.Settings](
			filepath.Join(filepath.Dir(DefaultPrefsPath()), "sync", "settings.json"))
	}
	hub := cfg.Hub
	if hub == nil {
		hub = prefs.NewHub()
	}
	creds := cfg.Creds
	if creds == nil {
		creds = authcreds.NewHolder()
	}
	status := cfg.Sync
	if status == nil {
		// Consistent with the other nil-defaults: a Config{} handler must
		// not panic on syncStatus — report Offline, stream nothing.
		status = noopStatus{}
	}
	r := &graph.Resolver{
		Cloud: cloud.New(cfg.CloudURL),
		Store: store,
		Hub:   hub,
		Sync:  status,
	}
	mux := http.NewServeMux()
	mux.Handle("/control/credentials", controlCredentials(creds))
	// Everything else (GraphQL, WS) goes to the resolver handler.
	mux.Handle("/", NewHandlerWithResolver(r))
	return mux
}

// controlCredentials accepts the host's token push. Kept off the GraphQL
// surface deliberately: setting process credentials is host-only, and the
// UDS is already user-restricted (0600). A malformed or empty-token push
// is rejected and leaves the existing credentials intact, so a bad push
// can never blank a working token.
func controlCredentials(creds *authcreds.Holder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Token     string    `json:"token"`
			ExpiresAt time.Time `json:"expiresAt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
			http.Error(w, "bad credentials payload", http.StatusBadRequest)
			return
		}
		creds.Set(authcreds.Credentials{Token: body.Token, ExpiresAt: body.ExpiresAt})
		w.WriteHeader(http.StatusNoContent)
	})
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
