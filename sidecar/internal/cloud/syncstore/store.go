// Package syncstore is the sidecar's local cache for cloud-synced,
// reconciled state. Unlike internal/cloud/prefs (which stores a bare
// payload), syncstore wraps the payload in an Envelope carrying the sync
// metadata the sync engine needs to resume after a sleep/outage: a
// version/cursor and last-synced / last-event timestamps.
//
// The crash-safe file mechanics (tmp + rename) live in internal/atomicjson;
// this package adds the generic Envelope shape — keeping the engine
// resource-agnostic — and serializes writers behind a mutex.
package syncstore

import (
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
)

// Envelope is the on-disk shape: the reconciled payload plus the sync
// bookkeeping the engine uses to decide what to resync on reconnect.
//
//   - Version      — opaque upstream cursor/resourceVersion if the cloud
//     supplies one; "" means "no cursor, do a full snapshot resync".
//   - LastSyncedAt — unix millis of the last successful snapshot reconcile.
//   - LastEventAt  — unix millis of the last applied stream event.
type Envelope[T any] struct {
	Data         T      `json:"data"`
	Version      string `json:"version"`
	LastSyncedAt int64  `json:"lastSyncedAt"`
	LastEventAt  int64  `json:"lastEventAt"`
}

// Store persists an Envelope[T] to a JSON file. Safe for concurrent use;
// one in-flight write at a time per Store instance.
type Store[T any] struct {
	path string
	mu   sync.Mutex
}

// NewStore returns a Store that reads/writes at path. The file is created
// lazily on the first Save.
func NewStore[T any](path string) *Store[T] {
	return &Store[T]{path: path}
}

// Load returns the persisted Envelope, or the zero value if the file does
// not exist yet. A missing file is not an error — it's the "nothing
// reconciled yet" state the engine treats as "do a full resync".
func (s *Store[T]) Load() (Envelope[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Load[Envelope[T]](s.path)
}

// Save writes the Envelope to disk atomically. Concurrent Save calls
// serialize on s.mu so the on-disk bytes are always a complete document —
// the loser of the race overwrites the winner, but neither produces a torn
// file.
func (s *Store[T]) Save(e Envelope[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Save(s.path, e)
}
