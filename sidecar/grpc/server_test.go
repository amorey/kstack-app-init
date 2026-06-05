package grpcserver_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
	"github.com/kubetail-org/kstack-app/sidecar/grpc/kubecontextpb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// TestServerLifecycleDrainsWatch exercises the grpcserver.Server lifecycle
// surface (NotifyShutdown / DrainWithContext / Stop) that the app layer drives.
// A live Watch stream must end cleanly when NotifyShutdown cancels the serving
// context: the handler returns nil, grpc flushes OK trailers, the client sees
// io.EOF, and DrainWithContext returns once the handler has unwound.
func TestServerLifecycleDrainsWatch(t *testing.T) {
	w, err := k8shelpers.NewKubeConfigWatcher("")
	require.NoError(t, err)
	t.Cleanup(w.Close)

	grpcSrv := grpcserver.NewServer(w, nil, nil)
	conn := newGRPCTestConn(t, grpcSrv)
	client := kubecontextpb.NewKubeContextServiceClient(conn)

	stream, err := client.Watch(context.Background(), &kubecontextpb.WatchRequest{})
	require.NoError(t, err)
	_, err = stream.Recv() // first snapshot: the stream is now live on the server
	require.NoError(t, err)

	recvErr := make(chan error, 1)
	go func() {
		_, e := stream.Recv()
		recvErr <- e
	}()

	// Shutdown sequence the app layer drives: signal, then wait to drain.
	grpcSrv.NotifyShutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, grpcSrv.DrainWithContext(ctx))

	select {
	case err := <-recvErr:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(3 * time.Second):
		t.Fatal("Watch stream did not drain after NotifyShutdown")
	}

	grpcSrv.Stop() // safe now that streams have drained
}

// IsGRPCRequest is the gRPC half of the socket's routing rule: HTTP/2 *and* the
// gRPC content-type. HTTP/1.1 (GraphQL POST/SSE) and a hypothetical HTTP/2
// GraphQL client must both fall through.
func TestIsGRPCRequest(t *testing.T) {
	tests := []struct {
		name        string
		protoMajor  int
		contentType string
		want        bool
	}{
		{"http2 grpc", 2, "application/grpc", true},
		{"http2 grpc+proto", 2, "application/grpc+proto", true},
		{"http1 grpc", 1, "application/grpc", false},
		{"http2 graphql json", 2, "application/json", false},
		{"http2 no content-type", 2, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Request{ProtoMajor: tt.protoMajor, Header: http.Header{}}
			if tt.contentType != "" {
				r.Header.Set("Content-Type", tt.contentType)
			}
			require.Equal(t, tt.want, grpcserver.IsGRPCRequest(r))
		})
	}
}
