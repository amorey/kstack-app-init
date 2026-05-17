// Package authcreds holds the process-wide cloud credentials for the
// always-on engine.
//
// The sidecar is stateless re: auth on the request path — every inbound
// GraphQL request carries its own bearer token. The always-on SyncEngine
// has no inbound request, so the Tauri host (which owns the OAuth keychain
// and refresh) pushes a fresh token here via the host-only
// /control/credentials endpoint; the engine's cloud adapter reads it. The
// Holder is empty until the host pushes, so the engine stays Offline
// rather than calling the cloud unauthenticated.
package authcreds

import (
	"sync"
	"time"
)

// Credentials is the bearer token plus its expiry, as pushed by the host.
// ExpiresAt is advisory (for logging/diagnostics); the engine treats any
// rejected call as Offline regardless.
type Credentials struct {
	Token     string
	ExpiresAt time.Time // zero = unknown
}

// Holder is the current credentials, written by the host control path and
// read by the engine. Safe for concurrent use.
type Holder struct {
	mu sync.RWMutex
	c  Credentials
}

// NewHolder returns an empty Holder.
func NewHolder() *Holder {
	return &Holder{}
}

// Set replaces the current credentials (a refresh pushes a new token over
// the old).
func (h *Holder) Set(c Credentials) {
	h.mu.Lock()
	h.c = c
	h.mu.Unlock()
}

// Get returns the current credentials, or the zero value if none pushed.
func (h *Holder) Get() Credentials {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.c
}

// Token returns just the bearer token, "" if none has been pushed.
func (h *Holder) Token() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.c.Token
}
