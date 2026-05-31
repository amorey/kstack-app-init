//go:build !windows

// Package ipc binds the host↔sidecar IPC endpoint with user-only access.
// The transport is platform-native: an AF_UNIX socket on Unix, a named
// pipe on Windows. main.go consumes Listen, DefaultSocketPath, and Scheme.
package ipc

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// Scheme labels the IPC transport in human-readable diagnostics
// (e.g. the `READY <scheme>:<path>` line in main.go). The host doesn't
// parse this; it's a hint for logs and external tooling. The Windows
// build uses `pipe:` for named pipes.
const Scheme = "unix"

// DefaultSocketPath returns the platform-native default IPC endpoint: an
// AF_UNIX socket in the temp directory, namespaced by pid so concurrent
// sidecars don't collide.
func DefaultSocketPath() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kstack-sidecar-%d.sock", os.Getpid()))
}

// Listen binds the IPC endpoint and applies user-only access.
//
// Order matters for security: net.Listen creates the socket file with
// mode 0777 & ~umask, so on a Linux box with default umask 022 and
// /tmp at 1777, another local user could connect in the window between
// bind and chmod. Tightening umask to 0177 first makes the socket land
// at 0600 atomically; the explicit Chmod afterwards is belt-and-
// suspenders against unusual umask states.
func Listen(path string) (net.Listener, error) {
	_ = os.Remove(path)
	prev := syscall.Umask(0o177)
	ln, err := net.Listen("unix", path)
	syscall.Umask(prev)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("chmod: %w", err)
	}
	return ln, nil
}
