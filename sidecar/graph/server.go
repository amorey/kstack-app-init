package graph

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
)

// noopStatus is the default StatusSource when a Resolver is built without one
// (bare resolvers in tests / surfaces that don't run the engine): it reports
// Offline and an already-closed watch, so syncStatus/syncStatusWatch degrade
// gracefully instead of nil-panicking. Production always wires the engine.
type noopStatus struct{}

func (noopStatus) Status() syncpkg.Status {
	return syncpkg.Status{State: syncpkg.StateOffline}
}

func (noopStatus) WatchStatus() (<-chan syncpkg.Status, func()) {
	ch := make(chan syncpkg.Status)
	close(ch)
	return ch, func() {}
}

// Server is the GraphQL surface: the gqlgen handler plus the shutdown lifecycle
// the app layer drives. It owns a shutdownCh that NotifyShutdown closes to end
// active SSE subscriptions, and a WaitGroup DrainWithContext blocks on until
// their handlers unwind. Mount it at /graphql (see internal/app); it is the
// sidecar's analogue of kstack-cloud's graph.Server.
//
// SSE cancellation is deliberately per-request (a goroutine in ServeHTTP wired
// to shutdownCh), not via http.Server.BaseContext: the shared h2c connection
// that carries gRPC must not be torn down by a BaseContext cancel, so the
// GraphQL drain is kept entirely off that mechanism. gRPC drains on its own
// path (see grpcserver.Server).
type Server struct {
	h          http.Handler
	shutdownCh chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
}

// NewServer builds the GraphQL server around a fully-constructed Resolver. A
// bare Resolver (nil Sync) is tolerated — syncStatus degrades to Offline rather
// than panicking — so tests can stand up a minimal surface.
func NewServer(r *Resolver) *Server {
	if r.Sync == nil {
		r.Sync = noopStatus{}
	}

	srv := handler.New(NewExecutableSchema(Config{Resolvers: r}))
	// Subscriptions ride SSE (transport.SSE): the host opens one
	// `POST /graphql` with `Accept: text/event-stream` per subscription and
	// reads `event: next` / `event: complete` frames off the streaming body.
	// SSE goes through the same bearer-token plumbing as POST/GET (see
	// ServeHTTP), so auth is uniform across every operation. The keep-alive
	// ping keeps an idle stream's UDS connection warm.
	//
	// SSE must be registered before POST: gqlgen picks the first transport
	// whose Supports() matches, and POST greedily matches any application/json
	// POST regardless of the Accept header. SSE additionally requires
	// `Accept: text/event-stream`, so it only claims subscription streams and
	// leaves plain queries/mutations to POST.
	srv.AddTransport(transport.SSE{KeepAlivePingInterval: 10 * time.Second})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})

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

	return &Server{h: srv, shutdownCh: make(chan struct{})}
}

// ServeHTTP runs the GraphQL handler, tracking every request so DrainWithContext
// can wait for in-flight handlers. It lifts the Authorization header off the
// request into the context resolvers see (keeping them HTTP-transport-agnostic),
// the single auth path shared by queries, mutations, and SSE. SSE subscriptions
// (the only long-lived requests) additionally get a context cancelled when the
// server shuts down, so gqlgen flushes their terminal `event: complete` instead
// of the stream being cut mid-frame.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.wg.Add(1)
	defer s.wg.Done()

	r = r.WithContext(WithRequestContext(r.Context(), r))

	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		go func() {
			defer cancel()
			select {
			case <-ctx.Done():
			case <-s.shutdownCh:
			}
		}()
		s.h.ServeHTTP(w, r.WithContext(ctx))
		return
	}

	s.h.ServeHTTP(w, r)
}

// NotifyShutdown signals active SSE subscriptions to flush their terminal frame
// and return. Wired into http.Server.RegisterOnShutdown by the app layer.
// Idempotent.
func (s *Server) NotifyShutdown() {
	s.once.Do(func() { close(s.shutdownCh) })
}

// DrainWithContext blocks until every in-flight request handler has returned or
// ctx is done. Called after http.Server.Shutdown, which has already drained the
// non-hijacked requests; this is the belt-and-suspenders wait that the SSE
// handlers signalled by NotifyShutdown have fully unwound.
func (s *Server) DrainWithContext(ctx context.Context) error {
	return drain.WithContext(ctx, s.wg.Wait)
}
