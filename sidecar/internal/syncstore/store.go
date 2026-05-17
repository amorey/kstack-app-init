// Package syncstore is the sidecar's local cache for cloud-synced,
// reconciled state. Unlike internal/prefs (which stores a bare payload),
// syncstore wraps the payload in an Envelope carrying the sync metadata the
// SyncEngine needs to resume after a sleep/outage: a version/cursor and
// last-synced / last-event timestamps.
//
// Storage is a single JSON file per resource, written atomically
// (tmp + rename in the same directory) so a crash mid-write can never leave
// a partial document for the next Load to choke on. The generic Store[T]
// keeps the engine resource-agnostic — Settings today, cluster state later.
package syncstore

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
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

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Envelope[T]{}, nil
		}
		return Envelope[T]{}, err
	}
	var out Envelope[T]
	if err := json.Unmarshal(data, &out); err != nil {
		return Envelope[T]{}, err
	}
	return out, nil
}

// Save writes the Envelope to disk atomically. Concurrent Save calls
// serialize on s.mu so the on-disk bytes are always a complete document —
// the loser of the race overwrites the winner, but neither produces a torn
// file.
func (s *Store[T]) Save(e Envelope[T]) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}

	data, err := json.Marshal(e)
	if err != nil {
		return err
	}

	// Tmp file in the same directory so rename is atomic (same filesystem).
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".syncstore-*.json")
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
