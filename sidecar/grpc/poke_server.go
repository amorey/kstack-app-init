package grpcserver

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubetail-org/kstack-app/sidecar/grpc/pokepb"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// pokeServer implements pokepb.PokeServiceServer over the shared resync
// broadcaster. A nil broadcaster degrades safely (Unavailable).
type pokeServer struct {
	pokepb.UnimplementedPokeServiceServer
	pokeSvc *poke.Service
}

// Poke broadcasts a SourceHost resync to every in-process subscriber. Unary and
// best-effort: poke.Service.Poke never blocks, so the response returns as soon
// as the signal is queued. The host calls this on OS resume / network-on.
func (s *pokeServer) Poke(_ context.Context, _ *pokepb.PokeRequest) (*pokepb.PokeResponse, error) {
	if s.pokeSvc == nil {
		return nil, status.Error(codes.Unavailable, "no poke service")
	}
	s.pokeSvc.Poke(poke.SourceHost)
	return &pokepb.PokeResponse{}, nil
}
