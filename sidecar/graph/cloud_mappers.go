package graph

import (
	"github.com/kubetail-org/kstack-app/sidecar/graph/model"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// This file holds the presentation-only mapping between the cloud session domain
// type and the gqlgen model.* types. Keeping it here lets schema.resolvers.go
// stay a thin set of one-line delegations.

// toGraphAuthState maps the auth State to the GraphQL model. Authenticated is the
// explicit sign-in signal; Identity is null while signed out (so the client can't
// read stale claims) and non-null only when authenticated.
func toGraphAuthState(s auth.State) *model.AuthState {
	out := &model.AuthState{Authenticated: s.Authenticated}
	if s.Identity != nil {
		out.Identity = &model.Identity{
			Sub:   s.Identity.UserID,
			Email: s.Identity.Email,
			Name:  s.Identity.Name,
		}
	}
	return out
}
