package prefsync

import (
	"context"
	"errors"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// errNoCreds is returned before any network call when the host hasn't
// pushed a token yet. The engine treats it like any upstream failure —
// Offline + backoff — so a logged-out / not-yet-restored process never
// hammers the cloud unauthenticated.
var errNoCreds = errors.New("sync: no credentials")

// CloudUpstream adapts the cloud client + the host-pushed credential holder
// to the engine's Upstream. It pulls the current token per call (the holder
// is refreshed out-of-band by the credential pusher), so a token rotation
// is picked up on the next snapshot/reconnect without restarting anything.
type CloudUpstream struct {
	cloud *cloud.Client
	creds *authcreds.Holder
}

// NewCloudUpstream binds a cloud client to the credential holder.
func NewCloudUpstream(c *cloud.Client, creds *authcreds.Holder) *CloudUpstream {
	return &CloudUpstream{cloud: c, creds: creds}
}

func (u *CloudUpstream) Snapshot(ctx context.Context) (prefs.Settings, error) {
	tok := u.creds.Token()
	if tok == "" {
		return prefs.Settings{}, errNoCreds
	}
	return u.cloud.GetSettings(ctx, tok)
}

func (u *CloudUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, error) {
	tok := u.creds.Token()
	if tok == "" {
		return nil, errNoCreds
	}
	return u.cloud.WatchSettings(ctx, tok)
}
