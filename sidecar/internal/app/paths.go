package app

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
// <file>`. One definition of the layout so the Store/Queue defaults and the
// composition root's wiring can't drift.
func SyncPath(prefsPath, file string) string {
	return filepath.Join(filepath.Dir(prefsPath), "sync", file)
}

// DefaultPrefsPath returns the on-disk path used when --prefs-path is not
// supplied. Falls back to /tmp on systems where UserConfigDir errors —
// good enough for a desktop POC; main() can override.
func DefaultPrefsPath() string {
	dir, err := userConfigDir()
	if err != nil {
		dir = "/tmp"
	}
	return filepath.Join(dir, "kstack", "preferences.json")
}
