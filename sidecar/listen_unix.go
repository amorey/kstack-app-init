//go:build !windows

package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

// defaultSocketPath returns the platform-native default IPC endpoint: an
// AF_UNIX socket in the temp directory, namespaced by pid so concurrent
// sidecars don't collide.
func defaultSocketPath() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("kstack-sidecar-%d.sock", os.Getpid()))
}

// listenSocket binds the IPC endpoint and applies user-only access.
//
// Order matters for security: net.Listen creates the socket file with
// mode 0777 & ~umask, so on a Linux box with default umask 022 and
// /tmp at 1777, another local user could connect in the window between
// bind and chmod. Tightening umask to 0177 first makes the socket land
// at 0600 atomically; the explicit Chmod afterwards is belt-and-
// suspenders against unusual umask states.
func listenSocket(path string) (net.Listener, error) {
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
