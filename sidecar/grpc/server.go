// Package grpcserver builds the sidecar's gRPC surface. It rides the same
// socket as GraphQL (h2c multiplexing in internal/app, keyed on
// IsGRPCRequest) and is
// consumed only by the native host (the tray), never the webview.
//
// The KubeContextService reuses the one *k8shelpers.KubeConfigWatcher that
// main() also hands to the GraphQL resolver, so a context switch made over
// either transport is observed by the other (single watch.Hub fan-out).
package grpcserver

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/grpc/kubecontextpb"
	"github.com/kubetail-org/kstack-app/sidecar/grpc/pokepb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// kubeContextServer implements kubecontextpb.KubeContextServiceServer on top
// of the shared kubeconfig watcher. A nil watcher keeps every method safe
// (Unavailable / empty stream), parallel to the GraphQL resolver nil-guards
// and the Config{}-must-not-panic convention.
type kubeContextServer struct {
	kubecontextpb.UnimplementedKubeContextServiceServer
	watcher *k8shelpers.KubeConfigWatcher
	// servingCtx is cancelled when the sidecar begins shutting down, ending
	// long-lived streams cleanly. It exists because grpc.Server.GracefulStop
	// panics on the ServeHTTP (h2c) path, so we can't lean on grpc's own
	// graceful drain — the stream must end on its own context instead.
	servingCtx context.Context
	// streams tracks in-flight server-streaming handlers so shutdown can wait
	// for them to unwind (and flush their terminal trailers) before the HTTP
	// server tears the shared h2c connections down. See New's returned wait.
	streams *sync.WaitGroup
}

// Watch streams a full KubeContextState snapshot first, then a fresh snapshot
// on every kubeconfig change. Mirrors the GraphQL KubeConfigWatch resolver
// loop: subscribe, emit the hub's current value, honor ctx.Done(), skip the
// defensive nil slot. Unlike GraphQL it carries no ADDED/MODIFIED marker —
// the host only renders contexts + current, so every message is just "latest".
//
// It also returns when servingCtx is cancelled (sidecar shutdown), so the
// stream ends cleanly on its own context the way an SSE subscription flushes
// its terminal frame — rather than being cut by the abrupt grpc.Server.Stop.
func (s *kubeContextServer) Watch(_ *kubecontextpb.WatchRequest, stream kubecontextpb.KubeContextService_WatchServer) error {
	if s.watcher == nil {
		// A handler wired without a watcher must not stream forever; end
		// immediately like the resolver's closed-channel default.
		return nil
	}

	s.streams.Add(1)
	defer s.streams.Done()

	sub := s.watcher.Subscribe()
	defer sub.Close()

	ctx := stream.Context()
	ch := sub.Chan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.servingCtx.Done():
			// Sidecar is shutting down: end the stream cleanly.
			return nil
		case cfg, ok := <-ch:
			if !ok {
				return nil
			}
			if cfg == nil {
				continue
			}
			if err := stream.Send(toState(cfg)); err != nil {
				return err
			}
		}
	}
}

// SetCurrentContext persists the requested context and lets the watcher's
// fan-out deliver the change to active Watch streams (and the webview's
// GraphQL subscription). Validation failures map to InvalidArgument so the
// host can surface a clean error.
func (s *kubeContextServer) SetCurrentContext(_ context.Context, req *kubecontextpb.SetCurrentContextRequest) (*kubecontextpb.SetCurrentContextResponse, error) {
	if s.watcher == nil {
		return nil, status.Error(codes.Unavailable, "no kubeconfig watcher")
	}
	if err := s.watcher.SetCurrentContext(req.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &kubecontextpb.SetCurrentContextResponse{}, nil
}

// toState projects an *api.Config down to the wire snapshot the host needs.
func toState(cfg *api.Config) *kubecontextpb.KubeContextState {
	out := &kubecontextpb.KubeContextState{CurrentContext: cfg.CurrentContext}
	for name := range cfg.Contexts {
		out.Contexts = append(out.Contexts, &kubecontextpb.Context{Name: name})
	}
	return out
}

// IsGRPCRequest reports whether r is a gRPC call: HTTP/2 carrying the gRPC
// content-type. This *is* the definition of "a gRPC request", so it lives here
// beside the gRPC server rather than in whatever multiplexes the socket — the
// composition root (internal/app) uses it in its h2c dispatcher to route HTTP/2
// gRPC traffic to the gRPC server while HTTP/1.1 GraphQL (POSTs + SSE) falls
// through. gRPC is
// inherently HTTP/2, so the ProtoMajor check belongs in the predicate; requiring
// the content-type too means a future HTTP/2 GraphQL client would not match.
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

// NewServer registers KubeContextService, AuthService and PokeService on a fresh
// *grpc.Server bound to a serving context it owns. The webview never reaches
// gRPC (h2c routes it to the host's tray); nil watcher/authSvc/pokeSvc keeps
// every method safe, mirroring the GraphQL resolver nil-guards.
func NewServer(watcher *k8shelpers.KubeConfigWatcher, authSvc auth.Service, pokeSvc *poke.Service) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	srv, drainStreams := newServer(ctx, watcher, authSvc, pokeSvc)
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

// newServer returns a *grpc.Server with the KubeContextService registered, plus
// a drainStreams func the shutdown sequence calls to gracefully close streaming
// RPCs. NewServer wraps this with the lifecycle surface and owns the serving
// context; this lower level stays separate only to keep that wiring readable.
//
// Shutdown is deliberately a two-step dance because grpc.Server.GracefulStop
// panics on the ServeHTTP (h2c) path. On shutdown the caller: (1) cancels
// servingCtx so every Watch handler returns nil — grpc then writes its OK
// trailers and the client sees a clean stream end; (2) calls drainStreams to
// block until those handlers have unwound, so the trailers are flushed *before*
// Stop closes the transports.
//
// The keepalive ping keeps an idle Watch stream's h2c connection alive under the
// HTTP server's 60s IdleTimeout (an idle kubeconfig can sit unchanged for far
// longer than that).
func newServer(servingCtx context.Context, watcher *k8shelpers.KubeConfigWatcher, authSvc auth.Service, pokeSvc *poke.Service) (srv *grpc.Server, drainStreams func()) {
	var streams sync.WaitGroup
	srv = grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    30 * time.Second,
		Timeout: 10 * time.Second,
	}))
	kubecontextpb.RegisterKubeContextServiceServer(srv, &kubeContextServer{
		watcher:    watcher,
		servingCtx: servingCtx,
		streams:    &streams,
	})
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
