// Package grpcserver builds the sidecar's gRPC surface: AuthService (the tray's account
// section) and PokeService (the host's wake/network-return nudge). It rides the same
// socket as GraphQL via h2c multiplexing keyed on IsGRPCRequest, and is consumed only by
// the native host, never the webview.
// See docs/adr/2026-08-09-single-socket-h2c.md.
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

// IsGRPCRequest IS the definition of a gRPC request — HTTP/2 plus the gRPC content-type —
// so it lives beside the server the h2c dispatcher routes with. Requiring the
// content-type, not just HTTP/2, keeps a future HTTP/2 GraphQL client falling through.
func IsGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// Server bundles the *grpc.Server with its shutdown lifecycle (NotifyShutdown → drain →
// Stop). It owns the serving context, so callers never thread one through composition
// just to cancel it at shutdown.
type Server struct {
	grpc   *grpc.Server
	cancel context.CancelFunc
	drain  func()
	once   sync.Once
}

// NewServer registers both services on a fresh *grpc.Server bound to a serving context it
// owns; a nil authSvc/pokeSvc degrades safely.
func NewServer(authSvc auth.Service, pokeSvc *poke.Service) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	srv, drainStreams := newServer(ctx, authSvc, pokeSvc)
	return &Server{grpc: srv, cancel: cancel, drain: drainStreams}
}

// GRPC exposes the underlying *grpc.Server for the h2c dispatcher.
func (s *Server) GRPC() *grpc.Server { return s.grpc }

// NotifyShutdown cancels the serving context so every Watch handler returns nil and grpc
// flushes OK trailers — the analogue of an SSE terminal frame. Idempotent.
func (s *Server) NotifyShutdown() {
	s.once.Do(s.cancel)
}

// DrainWithContext blocks until the handlers NotifyShutdown signalled have unwound (their
// trailers flushed) or ctx is done. Call it before Stop, which would otherwise cut a
// stream mid-trailer.
func (s *Server) DrainWithContext(ctx context.Context) error {
	return drain.WithContext(ctx, s.drain)
}

// Stop closes the gRPC transports, only after DrainWithContext returns. Never
// GracefulStop — it PANICS on the ServeHTTP/h2c path; see
// docs/adr/2026-08-09-single-socket-h2c.md.
func (s *Server) Stop() { s.grpc.Stop() }

// newServer registers both services and returns the drainStreams func shutdown waits on.
//
// Shutdown is two steps because GracefulStop panics on the h2c path: cancel servingCtx so
// every streaming handler returns nil (grpc writes OK trailers), then drainStreams so
// those trailers flush before Stop closes the transports.
//
// The keepalive ping holds an idle AuthStateWatch's h2c connection open under the HTTP
// server's 60s IdleTimeout.
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
	// PokeService is unary-only, so it doesn't join the WaitGroup — nothing to drain.
	pokepb.RegisterPokeServiceServer(srv, &pokeServer{pokeSvc: pokeSvc})
	return srv, streams.Wait
}
