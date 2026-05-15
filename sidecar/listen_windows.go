//go:build windows

package main

import (
	"fmt"
	"net"
	"os"

	"github.com/Microsoft/go-winio"
)

// defaultSocketPath returns a named-pipe path. The Rust client's
// `interprocess` crate maps file-style names on Windows to named pipes,
// so `\\.\pipe\...` is what the host actually dials.
func defaultSocketPath() string {
	return fmt.Sprintf(`\\.\pipe\kstack-sidecar-%d`, os.Getpid())
}

// listenSocket binds a named pipe restricted to the current user.
// The SDDL DACL grants Generic All to the process owner only — analogous
// to chmod 0600 on Unix.
func listenSocket(path string) (net.Listener, error) {
	return winio.ListenPipe(path, &winio.PipeConfig{
		SecurityDescriptor: "D:P(A;;GA;;;OW)",
	})
}
