package grpcserver_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/grpc/authpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// fakeAuthSvc is a hand-written auth.Service for the grpc server tests.
// It mirrors the fakeAuth in graph/testutils_test.go: signed-out by default,
// StartLogin signs in and publishes, Logout signs out and publishes, Subscribe
// returns a latest-value channel (current-on-subscribe then changes).
type fakeAuthSvc struct {
	mu       sync.Mutex
	signedIn bool
	identity auth.Identity
	loginAs  auth.Identity

	loginErr error // when set, StartLogin returns this error without signing in

	subs   map[int]chan auth.State
	nextID int
}

func newFakeAuthSvc(loginAs auth.Identity) *fakeAuthSvc {
	return &fakeAuthSvc{loginAs: loginAs, subs: map[int]chan auth.State{}}
}

func signedInFakeAuthSvc(id auth.Identity) *fakeAuthSvc {
	return &fakeAuthSvc{signedIn: true, identity: id, loginAs: id, subs: map[int]chan auth.State{}}
}

func (f *fakeAuthSvc) Current(context.Context) (auth.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stateLocked(), nil
}

func (f *fakeAuthSvc) StartLogin(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loginErr != nil {
		return f.loginErr
	}
	f.signedIn = true
	f.identity = f.loginAs
	f.publishLocked()
	return nil
}

func (f *fakeAuthSvc) Logout(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signedIn = false
	f.identity = auth.Identity{}
	f.publishLocked()
	return nil
}

func (f *fakeAuthSvc) TokenSource(context.Context) oauth2.TokenSource { return nil }

func (f *fakeAuthSvc) Subscribe() (<-chan auth.State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan auth.State, 8)
	ch <- f.stateLocked() // current-on-subscribe
	f.subs[id] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if c, ok := f.subs[id]; ok {
			delete(f.subs, id)
			close(c)
		}
	}
}

func (f *fakeAuthSvc) stateLocked() auth.State {
	st := auth.State{Authenticated: f.signedIn}
	if f.signedIn {
		id := f.identity
		st.Identity = &id
	}
	return st
}

func (f *fakeAuthSvc) publishLocked() {
	st := f.stateLocked()
	for _, ch := range f.subs {
		select {
		case ch <- st:
		default:
		}
	}
}

// grpcTestServer stands up an h2c HTTP server over the gRPC server and returns
// a connected client connection. Cleaned up on t.Cleanup.
func newGRPCTestConn(t *testing.T, grpcSrv *grpcserver.Server) *grpc.ClientConn {
	t.Helper()
	h := h2c.NewHandler(grpcSrv.GRPC(), &http2.Server{})
	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: h}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestAuthStateWatchSnapshotThenDelta verifies the server-streaming behavior:
// the first message is the current state (signed-out on subscribe), and after
// StartLogin the next message delivers the authenticated identity.
func TestAuthStateWatchSnapshotThenDelta(t *testing.T) {
	svc := newFakeAuthSvc(auth.Identity{Email: "ada@example.com", Name: "Ada"})
	grpcSrv := grpcserver.NewServer(svc, nil)
	conn := newGRPCTestConn(t, grpcSrv)
	t.Cleanup(grpcSrv.Stop)

	client := authpb.NewAuthServiceClient(conn)
	stream, err := client.AuthStateWatch(context.Background(), &authpb.AuthStateWatchRequest{})
	require.NoError(t, err)

	// First frame: current-on-subscribe = signed out.
	first, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, first.GetAuthenticated())
	require.Nil(t, first.GetIdentity())

	// StartLogin synchronously signs in on the fake; the watch must deliver a delta.
	_, err = client.StartLogin(context.Background(), &authpb.StartLoginRequest{})
	require.NoError(t, err)

	// Second frame: signed in with identity.
	second, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, second.GetAuthenticated())
	require.NotNil(t, second.GetIdentity())
	require.Equal(t, "ada@example.com", second.GetIdentity().GetEmail())
	require.Equal(t, "Ada", second.GetIdentity().GetName())
}

// TestStartLoginAndLogoutRPC verifies the unary RPCs succeed on a functional
// fake and that Logout causes the next watch to start signed-out.
func TestStartLoginAndLogoutRPC(t *testing.T) {
	svc := signedInFakeAuthSvc(auth.Identity{Email: "ada@example.com"})
	grpcSrv := grpcserver.NewServer(svc, nil)
	conn := newGRPCTestConn(t, grpcSrv)
	t.Cleanup(grpcSrv.Stop)

	client := authpb.NewAuthServiceClient(conn)

	// StartLogin should succeed (fake does not error by default).
	_, err := client.StartLogin(context.Background(), &authpb.StartLoginRequest{})
	require.NoError(t, err)

	// Logout should succeed.
	_, err = client.Logout(context.Background(), &authpb.LogoutRequest{})
	require.NoError(t, err)

	// A fresh watch must now start signed-out.
	stream, err := client.AuthStateWatch(context.Background(), &authpb.AuthStateWatchRequest{})
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, first.GetAuthenticated())
}

// TestAuthRPCsNilServiceTolerant verifies that passing nil auth to NewServer
// degrades safely: AuthStateWatch ends immediately (clean io.EOF), and the
// unary RPCs return Unavailable — mirroring the GraphQL resolver's nil-Auth
// degrade.
func TestAuthRPCsNilServiceTolerant(t *testing.T) {
	grpcSrv := grpcserver.NewServer(nil, nil)
	conn := newGRPCTestConn(t, grpcSrv)
	t.Cleanup(grpcSrv.Stop)

	client := authpb.NewAuthServiceClient(conn)

	// AuthStateWatch on a nil service must end immediately with io.EOF.
	stream, err := client.AuthStateWatch(context.Background(), &authpb.AuthStateWatchRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	// Unary RPCs must return Unavailable.
	_, err = client.StartLogin(context.Background(), &authpb.StartLoginRequest{})
	st, _ := status.FromError(err)
	require.Equal(t, codes.Unavailable, st.Code())

	_, err = client.Logout(context.Background(), &authpb.LogoutRequest{})
	st, _ = status.FromError(err)
	require.Equal(t, codes.Unavailable, st.Code())
}

// TestAuthStateWatchDrainsOnShutdown verifies the stream participates in the
// shared streams WaitGroup: NotifyShutdown cancels the serving context, the
// handler returns nil (grpc flushes OK trailers), the client sees io.EOF, and
// DrainWithContext returns.
func TestAuthStateWatchDrainsOnShutdown(t *testing.T) {
	svc := newFakeAuthSvc(auth.Identity{})
	grpcSrv := grpcserver.NewServer(svc, nil)
	conn := newGRPCTestConn(t, grpcSrv)

	client := authpb.NewAuthServiceClient(conn)
	stream, err := client.AuthStateWatch(context.Background(), &authpb.AuthStateWatchRequest{})
	require.NoError(t, err)
	// Consume the initial snapshot so the handler is blocked in its select loop.
	_, err = stream.Recv()
	require.NoError(t, err)

	recvErr := make(chan error, 1)
	go func() {
		_, e := stream.Recv()
		recvErr <- e
	}()

	grpcSrv.NotifyShutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, grpcSrv.DrainWithContext(ctx))

	select {
	case err := <-recvErr:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(3 * time.Second):
		t.Fatal("AuthStateWatch stream did not drain after NotifyShutdown")
	}

	grpcSrv.Stop()
}
