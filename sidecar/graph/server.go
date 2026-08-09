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
)

// Server is the GraphQL surface: the gqlgen handler plus the shutdown lifecycle the app
// layer drives — NotifyShutdown closes shutdownCh to end active SSE subscriptions, and
// DrainWithContext waits on their handlers.
//
// **Never cancel via http.Server.BaseContext**: that would tear down the shared h2c
// connection carrying gRPC mid-stream, so SSE cancellation is per-request instead.
// See docs/adr/2026-08-09-single-socket-h2c.md.
type Server struct {
	h          http.Handler
	shutdownCh chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
}

// NewServer builds the GraphQL server around a fully-wired Resolver (see Resolver on the
// non-nil requirement).
func NewServer(r *Resolver) *Server {
	srv := handler.New(NewExecutableSchema(Config{Resolvers: r}))
	// Subscriptions ride SSE: one `POST /graphql` with `Accept: text/event-stream`
	// per subscription, the keep-alive ping holding the idle UDS connection warm.
	//
	// SSE MUST be registered before POST: gqlgen takes the first transport that
	// Supports() a request, and POST greedily matches any application/json POST
	// regardless of Accept.
	srv.AddTransport(transport.SSE{KeepAlivePingInterval: 10 * time.Second})
	srv.AddTransport(transport.POST{})
	srv.AddTransport(transport.GET{})

	// One seam for resolver/parse errors. Log the operation and path but NEVER
	// `variables` — they can carry auth tokens or PII.
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

// ServeHTTP runs the handler, tracking every request for DrainWithContext. SSE
// subscriptions — the only long-lived requests — get a context cancelled at shutdown, so
// gqlgen flushes `event: complete` instead of the stream being cut mid-frame.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.wg.Add(1)
	defer s.wg.Done()

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

// NotifyShutdown tells active SSE subscriptions to flush their terminal frame and return.
// Idempotent; wired into http.Server.RegisterOnShutdown.
func (s *Server) NotifyShutdown() {
	s.once.Do(func() { close(s.shutdownCh) })
}

// DrainWithContext blocks until every in-flight handler returns or ctx is done — run
// after http.Server.Shutdown to confirm the SSE handlers unwound.
func (s *Server) DrainWithContext(ctx context.Context) error {
	return drain.WithContext(ctx, s.wg.Wait)
}
