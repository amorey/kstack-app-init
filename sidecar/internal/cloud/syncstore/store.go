// Package syncstore is the local cache for cloud-synced state: a payload wrapped in an
// Envelope carrying what the engine needs to resume after a sleep/outage. The crash-safe
// file mechanics live in internal/atomicjson; this package adds the Envelope and
// serializes writers.
package syncstore

import (
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
)

// Envelope is the on-disk shape: the payload plus the bookkeeping the engine resyncs
// from on reconnect.
type Envelope[T any] struct {
	Data T `json:"data"`
	// Version is an opaque upstream cursor; "" means "full snapshot resync".
	Version string `json:"version"`
	// Unix millis of the last successful reconcile / last applied stream event.
	LastSyncedAt int64 `json:"lastSyncedAt"`
	LastEventAt  int64 `json:"lastEventAt"`
}

// Store persists an Envelope[T] to a JSON file; safe for concurrent use.
type Store[T any] struct {
	path string
	mu   sync.Mutex
}

// NewStore reads/writes at path, creating the file lazily on first Save.
func NewStore[T any](path string) *Store[T] {
	return &Store[T]{path: path}
}

// Load returns the persisted Envelope, or the zero value when the file doesn't exist yet
// — "nothing reconciled", which the engine treats as a full resync, not an error.
func (s *Store[T]) Load() (Envelope[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Load[Envelope[T]](s.path)
}

// Save writes the Envelope atomically. Concurrent Saves serialize, so the file is always
// a complete document — last writer wins, but never a torn file.
func (s *Store[T]) Save(e Envelope[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Save(s.path, e)
}
