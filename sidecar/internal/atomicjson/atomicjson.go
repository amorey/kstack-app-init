// Package atomicjson reads and writes a JSON document as a single file,
// crash-safe: writes go to a temp file in the same directory and are
// atomically renamed into place, so a crash mid-write can never leave a
// torn document for the next Load. A missing file is reported as the zero
// value — callers treat "no file yet" as "nothing cached".
//
// The functions are not internally synchronized. Callers that allow
// concurrent writers must serialize them (e.g. behind a sync.Mutex); the
// temp+rename then guarantees every on-disk state is a complete document,
// with last-writer-wins between racing Saves.
package atomicjson

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Temp files share one prefix; the name is otherwise random per write, so
// concurrent Saves never collide on the temp path before the rename.
const tmpPattern = ".atomicjson-*.json"

// Load reads path and unmarshals it into T. A missing file yields the zero
// T and a nil error; a present-but-corrupt file yields the zero T and the
// unmarshal error (never silently valid).
func Load[T any](path string) (T, error) {
	var zero T
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return zero, nil
		}
		return zero, err
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return zero, err
	}
	return out, nil
}

// Save marshals v and writes it to path atomically. The parent directory
// is created 0700 if absent; the file ends up 0600.
func Save[T any](path string, v T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), tmpPattern)
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
	return os.Rename(tmpName, path)
}
