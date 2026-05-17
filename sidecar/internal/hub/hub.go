// Package hub is a generic in-process fan-out. A subscriber gets a
// buffered channel of T plus an unsubscribe func; Publish delivers to
// every current subscriber and drops the value for any whose buffer is
// full — a slow consumer must never stall the publisher (e.g. the cloud
// SSE reader) or the other subscribers. The sidecar is single-user, so
// there is no per-key routing. Reused for Settings events (prefs.Hub) and
// engine status transitions (sync.Engine).
package hub

import "sync"

// buffer absorbs a single bursty publish; a permanently slow consumer
// falls behind silently (its publishes drop).
const buffer = 4

type Hub[T any] struct {
	mu   sync.Mutex
	subs map[chan T]struct{}
}

// New returns an empty Hub.
func New[T any]() *Hub[T] {
	return &Hub[T]{subs: make(map[chan T]struct{})}
}

// Subscribe returns a buffered channel of T and an unsubscribe func. The
// channel is closed when the unsubscribe func is called; calling it more
// than once is a no-op.
func (h *Hub[T]) Subscribe() (<-chan T, func()) {
	ch := make(chan T, buffer)
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

// Publish delivers v to every current subscriber. A subscriber whose
// buffer is full has the message dropped — better than blocking the
// publisher and stalling everyone else.
func (h *Hub[T]) Publish(v T) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- v:
		default:
		}
	}
}
