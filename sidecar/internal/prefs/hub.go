package prefs

import "github.com/kubetail-org/kstack-app/sidecar/internal/hub"

// Hub is the in-process fan-out for Settings events: the settingsWatch
// resolver subscribes here; the cloud SSE reader (via the engine) publishes
// here. It's the generic hub.Hub specialised to Settings — kept as a named
// alias so the many `*prefs.Hub` call sites read in domain terms.
type Hub = hub.Hub[Settings]

// NewHub returns an empty Settings hub.
func NewHub() *Hub { return hub.New[Settings]() }
