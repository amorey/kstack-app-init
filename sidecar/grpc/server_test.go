package grpcserver_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

	grpcSrv := grpcserver.NewServer(w)
	h := grpcserver.NewH2CHandler(http.NotFoundHandler(), grpcSrv.GRPC())

	srv := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: h}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
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
