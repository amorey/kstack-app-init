//go:build !windows

package ipc

import (
	"net"
	"path/filepath"
	"testing"
)

// testEndpoint returns a fresh socket path for one test.
func testEndpoint(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "s.sock")
}

func dialEndpoint(path string) (net.Conn, error) {
	return net.Dial("unix", path)
}
