package grpcserver

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// authServer implements authpb.AuthServiceServer; a nil auth degrades safely
// (Unavailable / empty stream).
type authServer struct {
	authpb.UnimplementedAuthServiceServer
	auth auth.Service
	// servingCtx is cancelled at shutdown to end long-lived streams cleanly.
	servingCtx context.Context
	// streams tracks in-flight handlers, so shutdown waits for them before the HTTP
	// server tears the h2c connections down.
	streams *sync.WaitGroup
}

// AuthStateWatch streams the AuthState current-on-subscribe, then on every session
// change, returning when the client cancels or the sidecar shuts down.
func (s *authServer) AuthStateWatch(_ *authpb.AuthStateWatchRequest, stream authpb.AuthService_AuthStateWatchServer) error {
	if s.auth == nil {
		return nil // clean OK
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
			return nil // shutting down; end cleanly
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

// StartLogin runs the synchronous login setup; the sign-in tail completes in the
// background and is delivered via AuthStateWatch.
func (s *authServer) StartLogin(ctx context.Context, _ *authpb.StartLoginRequest) (*authpb.StartLoginResponse, error) {
	if s.auth == nil {
		return nil, status.Error(codes.Unavailable, "no auth service")
	}
	if err := s.auth.StartLogin(ctx); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &authpb.StartLoginResponse{}, nil
}

// Logout clears local credentials and revokes fire-and-forget; errors only on a keychain
// write failure.
func (s *authServer) Logout(ctx context.Context, _ *authpb.LogoutRequest) (*authpb.LogoutResponse, error) {
	if s.auth == nil {
		return nil, status.Error(codes.Unavailable, "no auth service")
	}
	if err := s.auth.Logout(ctx); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &authpb.LogoutResponse{}, nil
}

// toAuthState projects an auth.State to the wire; Identity is set only while signed in.
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
