// Package prefs is the sidecar's local cache for cloud-synced user
// preferences. The store is a single JSON file, written atomically
// (tmp + rename) so a crash mid-write can't leave a partial document
// for the next Load to choke on.
package prefs

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Settings{}, nil
		}
		return Settings{}, err
	}
	var out Settings
	if err := json.Unmarshal(data, &out); err != nil {
		return Settings{}, err
	}
	return out, nil
}

// Save writes Settings to disk atomically. Concurrent Save calls serialize
// on s.mu so the on-disk bytes are always a complete document — the loser
// of the race overwrites the winner, but neither produces a torn file.
func (s *Store) Save(v Settings) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	// Tmp file in the same directory so rename is atomic (same filesystem).
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".preferences-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if anything below fails before the rename.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
