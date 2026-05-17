// Package prefs is the sidecar's local cache for cloud-synced user
// preferences. The on-disk document is written crash-safely via
// internal/atomicjson (tmp + rename); this package adds the Settings shape
// and serializes writers behind a mutex.
package prefs

import (
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
)

// Settings mirrors the cloud API's Settings type. Today it carries a single
// free-form `placeholder` string — the POC's only synced field. Add fields
// here as the schema grows; JSON tags must match the GraphQL field names so
// the same bytes can be reused on the wire.
type Settings struct {
	Placeholder string `json:"placeholder"`
}

// Store persists Settings to a JSON file. Safe for concurrent use; one
// in-flight write at a time per Store instance.
type Store struct {
	path string
	mu   sync.Mutex
}

// NewStore returns a Store that reads/writes at path. The file is created
// lazily on the first Save.
func NewStore(path string) *Store {
	return &Store{path: path}
}

// Load returns the persisted Settings, or the zero value if the file does
// not exist yet. A missing file is not an error — it's the "no cache yet"
// state the resolver layer treats as "ask the cloud".
func (s *Store) Load() (Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Load[Settings](s.path)
}

// Save writes Settings to disk atomically. Concurrent Save calls serialize
// on s.mu so the on-disk bytes are always a complete document — the loser
// of the race overwrites the winner, but neither produces a torn file.
func (s *Store) Save(v Settings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicjson.Save(s.path, v)
}
