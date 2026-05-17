package server

import (
	"os"
	"path/filepath"
)

// userConfigDir is a thin wrapper so tests can substitute via a build tag
// later if needed. Today it just defers to the stdlib.
func userConfigDir() (string, error) {
	return os.UserConfigDir()
}

// SyncPath returns the engine's per-resource state file: `<prefs-dir>/sync/
// <file>`. One definition of the layout so the Store/Queue defaults and
// main()'s wiring can't drift.
func SyncPath(prefsPath, file string) string {
	return filepath.Join(filepath.Dir(prefsPath), "sync", file)
}
