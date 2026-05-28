package graph

//go:generate go run github.com/99designs/gqlgen generate

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph/model"
)

// Resolver carries the dependencies every operation needs: a Client for
// outbound cloud calls, a Store for the local cache, and a Hub to fan out
// settingsWatch events to all local subscribers.
// Resolver carries the dependencies operations need. Post-cutover the
// read path is served from the engine-maintained syncstore and the shared
// Hub (the engine is the only cloud talker); Cloud is retained only for
// the `updateSettings` write-through.
type Resolver struct {
	Cloud             *cloud.Client
	Store             *syncstore.Store[prefs.Settings]
	Hub               *prefs.Hub
	Sync              StatusSource
	Queue             *mutationqueue.Queue
	KubeConfigWatcher *k8shelpers.KubeConfigWatcher
}

// StatusSource is the slice of the always-on engine the syncStatus
// resolvers need. An interface (not *sync.Engine) so server tests can
// inject a fake without standing up a real engine.
type StatusSource interface {
	Status() syncpkg.Status
	WatchStatus() (<-chan syncpkg.Status, func())
}

// cloudInput converts the gqlgen-generated input type into the cloud
// client's mirror type. Lives here (hand-written file) so gqlgen doesn't
// relocate it into a regenerated resolver file.
func cloudInput(in model.UpdateSettingsInput) cloud.UpdateInput {
	return cloud.UpdateInput{Placeholder: in.Placeholder}
}

// graphSyncState maps the engine's State enum onto the generated GraphQL
// enum. The four engine states are total; default is defensive only.
func graphSyncState(s syncpkg.State) model.SyncState {
	switch s {
	case syncpkg.StateConnecting:
		return model.SyncStateConnecting
	case syncpkg.StateLive:
		return model.SyncStateLive
	case syncpkg.StateBackoff:
		return model.SyncStateBackoff
	default:
		return model.SyncStateOffline
	}
}

// toGraphSyncStatus maps the engine's Status onto the generated GraphQL
// type. Timestamps are Unix-millis ints (0 when zero-valued); RetryAt is
// only meaningful while backing off (the engine's Status documents this
// invariant — the mapper just honors it).
func toGraphSyncStatus(s syncpkg.Status) *model.SyncStatus {
	ms := func(t time.Time) int {
		if t.IsZero() {
			return 0
		}
		return int(t.UnixMilli())
	}
	retry := 0
	if s.State == syncpkg.StateBackoff {
		retry = ms(s.RetryAt)
	}
	return &model.SyncStatus{
		State:        graphSyncState(s.State),
		LastError:    s.LastError,
		LastSyncedAt: ms(s.LastSyncedAt),
		RetryAt:      retry,
	}
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
func toGraphSettings(s prefs.Settings) *model.Settings {
	return &model.Settings{Placeholder: s.Placeholder}
}
