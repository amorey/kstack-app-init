package grpcserver

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// authServer implements authpb.AuthServiceServer on top of the shared
// auth.Service. A nil auth degrades safely (Unavailable / empty stream),
// mirroring kubeContextServer's nil-watcher behavior and the GraphQL resolver's
// nil-Auth guard.
type authServer struct {
	authpb.UnimplementedAuthServiceServer
	auth auth.Service
	// servingCtx is cancelled when the sidecar begins shutting down, ending
	// long-lived streams cleanly. Shared with kubeContextServer (both use the
	// same serving context and streams WaitGroup).
	servingCtx context.Context
	// streams tracks in-flight AuthStateWatch handlers so shutdown can wait for
	// them to unwind before the HTTP server tears the h2c connections down.
	streams *sync.WaitGroup
}

// AuthStateWatch streams the current AuthState immediately (current-on-subscribe,
// mirroring auth.Service.Subscribe's latest-value semantics), then a fresh
// snapshot on every session change. Returns when the client cancels or the
// sidecar shuts down (servingCtx).
func (s *authServer) AuthStateWatch(_ *authpb.AuthStateWatchRequest, stream authpb.AuthService_AuthStateWatchServer) error {
	if s.auth == nil {
		// Nil-tolerant: end immediately with a clean OK status, like
		// kubeContextServer.Watch does for a nil watcher.
		return nil
	}

	s.streams.Add(1)
	defer s.streams.Done()

	ch, cancel := s.auth.Subscribe()
	defer cancel()

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.servingCtx.Done():
			// Sidecar is shutting down: end the stream cleanly.
			return nil
		case st, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(toAuthState(st)); err != nil {
				return err
			}
		}
	}
}

// StartLogin runs the synchronous login setup (loopback bind + browser open)
// and returns its result. The async sign-in tail runs in a bounded background
// goroutine; its completion is delivered via AuthStateWatch.
func (s *authServer) StartLogin(ctx context.Context, _ *authpb.StartLoginRequest) (*authpb.StartLoginResponse, error) {
	if s.auth == nil {
		return nil, status.Error(codes.Unavailable, "no auth service")
	}
	if err := s.auth.StartLogin(ctx); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &authpb.StartLoginResponse{}, nil
}

// Logout clears local credentials and revokes the refresh token (fire-and-forget
// revocation). Returns an error only on keychain write failure.
func (s *authServer) Logout(ctx context.Context, _ *authpb.LogoutRequest) (*authpb.LogoutResponse, error) {
	if s.auth == nil {
		return nil, status.Error(codes.Unavailable, "no auth service")
	}
	if err := s.auth.Logout(ctx); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &authpb.LogoutResponse{}, nil
}

// toAuthState projects an auth.State down to the wire snapshot the host needs.
// Identity is only set while signed in, mirroring toState (kubecontext) and
// toGraphAuthState (cloud_mappers.go).
func toAuthState(s auth.State) *authpb.AuthState {
	out := &authpb.AuthState{Authenticated: s.Authenticated}
	if s.Identity != nil {
		out.Identity = &authpb.Identity{
			UserId: s.Identity.UserID,
			Email:  s.Identity.Email,
			Name:   s.Identity.Name,
		}
	}
	return out
}
