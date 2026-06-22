package grpcserver_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	grpcserver "github.com/kubetail-org/kstack-app/sidecar/grpc"
)

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
