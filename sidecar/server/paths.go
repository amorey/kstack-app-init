package server

import "os"

// userConfigDir is a thin wrapper so tests can substitute via a build tag
// later if needed. Today it just defers to the stdlib.
func userConfigDir() (string, error) {
	return os.UserConfigDir()
}
