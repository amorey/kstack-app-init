package prefs

import "sync"

// Hub is an in-process fan-out for Settings events. The sidecar's
// settingsWatch resolver subscribes here; the cloud SSE reader publishes
// here. Pattern mirrors the cloud API's `settingsSubs` but simpler — the
// sidecar is single-user, so no per-user keying.
type Hub struct {
	mu   sync.Mutex
	subs map[chan Settings]struct{}
}

// NewHub returns an empty Hub.
func NewHub() *Hub {
	return &Hub{subs: make(map[chan Settings]struct{})}
}

// Subscribe returns a buffered channel of Settings events and an unsubscribe
// func. The channel is closed when the unsubscribe func is called; calling
// it more than once is a no-op.
func (h *Hub) Subscribe() (<-chan Settings, func()) {
	// Small buffer absorbs a single bursty publish; a permanently slow
	// consumer falls behind silently (publishes drop in Publish).
	ch := make(chan Settings, 4)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subs[ch]; ok {
				delete(h.subs, ch)
				close(ch)
			}
			h.mu.Unlock()
		})
	}
	return ch, unsub
}

// Publish delivers v to every current subscriber. A subscriber whose buffer
// is full has the message dropped — better than blocking the publisher and
// stalling the cloud reader.
func (h *Hub) Publish(v Settings) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- v:
		default:
		}
	}
}
