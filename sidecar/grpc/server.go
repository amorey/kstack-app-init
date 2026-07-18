// Package grpcserver builds the sidecar's gRPC surface. It rides the same
// socket as GraphQL (h2c multiplexing in internal/app, keyed on
// IsGRPCRequest) and is
// consumed only by the native host (the tray), never the webview.
//
// Today it exposes AuthService (the tray's account section) and PokeService
// (the host's wake/network-return resync nudge).
package grpcserver

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/grpc/pokepb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// IsGRPCRequest reports whether r is a gRPC call: HTTP/2 carrying the gRPC
// content-type. This is the definition of "a gRPC request", so it lives beside
// the gRPC server; the composition root (internal/app) uses it in its h2c
// dispatcher to route gRPC traffic to the gRPC server while HTTP/1.1 GraphQL
// (POSTs + SSE) falls through. Requiring the content-type too (not just HTTP/2)
// means a future HTTP/2 GraphQL client still falls through.
func IsGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// Server bundles the *grpc.Server with the lifecycle surface the app layer
// drives: NotifyShutdown signals its in-flight streams to close, DrainWithContext
// waits for them to unwind, and Stop closes the transports. It owns the serving
// context, so callers never thread a context through composition just to cancel
// it at shutdown.
type Server struct {
	grpc   *grpc.Server
	cancel context.CancelFunc
	drain  func()
	once   sync.Once
}

// NewServer registers AuthService and PokeService on a fresh *grpc.Server bound
// to a serving context it owns. nil authSvc/pokeSvc keeps every method safe
// (degraded).
func NewServer(authSvc auth.Service, pokeSvc *poke.Service) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	srv, drainStreams := newServer(ctx, authSvc, pokeSvc)
	return &Server{grpc: srv, cancel: cancel, drain: drainStreams}
}

// GRPC exposes the underlying *grpc.Server so the h2c dispatcher (in internal/app)
// can hand it HTTP/2 application/grpc requests (see IsGRPCRequest).
func (s *Server) GRPC() *grpc.Server { return s.grpc }

// NotifyShutdown cancels the serving context so every Watch handler returns nil
// and grpc flushes its OK trailers — the gRPC analogue of an SSE stream emitting
// its terminal frame. Wired into http.Server.RegisterOnShutdown by the app
// layer. Idempotent.
func (s *Server) NotifyShutdown() {
	s.once.Do(s.cancel)
}

// DrainWithContext blocks until the streaming handlers signalled by
// NotifyShutdown have unwound (so their trailers are flushed) or ctx is done.
// Call it before Stop so Stop never cuts a stream mid-trailer.
func (s *Server) DrainWithContext(ctx context.Context) error {
	return drain.WithContext(ctx, s.drain)
}

// Stop closes the gRPC transports. Use Stop (not GracefulStop, which panics on
// the ServeHTTP/h2c path) and only after DrainWithContext has returned.
func (s *Server) Stop() { s.grpc.Stop() }

// newServer returns a *grpc.Server with AuthService and PokeService registered,
// plus a drainStreams func the shutdown sequence uses to close streaming RPCs.
// NewServer wraps this with the lifecycle surface; this level stays separate only
// to keep that wiring readable.
//
// Shutdown is a two-step dance because grpc.Server.GracefulStop panics on the
// ServeHTTP (h2c) path: the caller (1) cancels servingCtx so every streaming
// handler returns nil (grpc writes OK trailers, the client sees a clean end),
// then (2) calls drainStreams to wait for those handlers to unwind so the
// trailers flush before Stop closes the transports.
//
// The keepalive ping keeps an idle AuthStateWatch stream's h2c connection alive
// under the HTTP server's 60s IdleTimeout.
func newServer(servingCtx context.Context, authSvc auth.Service, pokeSvc *poke.Service) (srv *grpc.Server, drainStreams func()) {
	var streams sync.WaitGroup
	srv = grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    30 * time.Second,
		Timeout: 10 * time.Second,
	}))
	authpb.RegisterAuthServiceServer(srv, &authServer{
		auth:       authSvc,
		servingCtx: servingCtx,
		streams:    &streams,
	})
	// PokeService is unary-only (no long-lived streams), so it doesn't join the
	// streams WaitGroup — there's nothing to drain at shutdown.
	pokepb.RegisterPokeServiceServer(srv, &pokeServer{pokeSvc: pokeSvc})
	return srv, streams.Wait
}
