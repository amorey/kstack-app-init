package graph

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// Resolver carries the dependencies every operation needs: a Client for
// outbound cloud calls, a Store for the local cache, and a Hub to fan out
// settingsWatch events to all local subscribers.
type Resolver struct {
	Cloud *cloud.Client
	Store *prefs.Store
	Hub   *prefs.Hub
}

// tickInterval is the cadence of the `tick` subscription. Overridable via
// SIDECAR_TICK_INTERVAL_MS so the server_test suite can use a sub-second
// cadence without dragging the whole test run.
//
// Lives here (not in schema.resolvers.go) because gqlgen treats that file
// as regenerated and would relocate helper funcs into a comment block on
// every run.
func tickInterval() time.Duration {
	if v := os.Getenv("SIDECAR_TICK_INTERVAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Second
}

// Bearer-token plumbing ------------------------------------------------------
//
// The sidecar is stateless re: auth. Each inbound HTTP request carries an
// `Authorization: Bearer <token>` header; the resolver layer pulls that token
// out of the request context and passes it to the cloud client. WS clients
// have two ways to supply credentials (header during the upgrade, or
// `connection_init` payload); we read whichever is set.

type ctxKey int

const bearerKey ctxKey = 0

// WithRequestContext is the per-request hook installed on the gqlgen handler:
// copy the bearer token from the HTTP request into the GraphQL context so
// resolvers can read it without seeing http.Request directly.
func WithRequestContext(ctx context.Context, r *http.Request) context.Context {
	return WithAuthHeader(ctx, r.Header.Get("Authorization"))
}

// WithAuthHeader overlays the bearer token parsed from a raw `Authorization`
// header onto ctx. Empty header → ctx returned unchanged. Also used by the
// Websocket transport's InitFunc, which receives the `connection_init`
// payload's `Authorization` entry in the same shape.
func WithAuthHeader(ctx context.Context, header string) context.Context {
	const prefix = "Bearer "
	switch {
	case header == "":
		return ctx
	case len(header) > len(prefix) && header[:len(prefix)] == prefix:
		return context.WithValue(ctx, bearerKey, header[len(prefix):])
	default:
		return context.WithValue(ctx, bearerKey, header)
	}
}

// bearer returns the token attached to ctx, or "" if none.
func bearer(ctx context.Context) string {
	v, _ := ctx.Value(bearerKey).(string)
	return v
}

// toGraphSettings converts the persistence model into gqlgen's generated
// Settings type. They have the same shape today, but keeping the
// conversion explicit means schema and cache can evolve independently.
func toGraphSettings(s prefs.Settings) *Settings {
	return &Settings{Placeholder: s.Placeholder}
}

// logResolverErr logs cloud failures at warn — they're expected during
// transient connectivity loss and shouldn't pollute error logs.
func logResolverErr(ctx context.Context, op string, err error) {
	slog.WarnContext(ctx, "cloud call failed", "op", op, "err", err)
}
