//go:build windows

package ipc

import (
	"fmt"
	"net"
	"os"

	"github.com/Microsoft/go-winio"
)

// DefaultSocketPath returns a `\\.\pipe\...` path the host dials verbatim. Only for a
// standalone run — the host passes its own via `--socket`.
func DefaultSocketPath() string {
	return fmt.Sprintf(`\\.\pipe\kstack-sidecar-%d`, os.Getpid())
}

// Scheme labels the transport in main.go's `READY <scheme>:<path>` line; the host
// doesn't parse it.
const Scheme = "pipe"

// ownerOnlyDACL grants Generic All to the process owner (`OW`) alone; `D:P` protects the
// DACL from inherited ACEs, so this is the entire access policy. Like chmod 0600.
const ownerOnlyDACL = "D:P(A;;GA;;;OW)"

// Listen binds a named pipe restricted to the current user.
func Listen(path string) (net.Listener, error) {
	return winio.ListenPipe(path, &winio.PipeConfig{
		SecurityDescriptor: ownerOnlyDACL,
	})
}
