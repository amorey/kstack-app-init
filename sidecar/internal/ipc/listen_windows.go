//go:build windows

package ipc

import (
	"fmt"
	"net"
	"os"

	"github.com/Microsoft/go-winio"
)

// DefaultSocketPath returns a named-pipe path in the `\\.\pipe\...` form
// the Rust host dials verbatim. Only used for a standalone run; the host
// passes its own per-instance path via `--socket`.
func DefaultSocketPath() string {
	return fmt.Sprintf(`\\.\pipe\kstack-sidecar-%d`, os.Getpid())
}

// Scheme labels the IPC transport in human-readable diagnostics (the
// `READY <scheme>:<path>` line in main.go); the host doesn't parse it.
// The Unix build uses `unix`.
const Scheme = "pipe"

// ownerOnlyDACL grants Generic All to the process owner only via the
// well-known SID `OW` (Owner Rights). `D:P` makes the DACL protected — no
// inherited ACEs — so the listed ACE is the entire access policy.
// Analogous to chmod 0600 on Unix.
const ownerOnlyDACL = "D:P(A;;GA;;;OW)"

// Listen binds a named pipe restricted to the current user.
func Listen(path string) (net.Listener, error) {
	return winio.ListenPipe(path, &winio.PipeConfig{
		SecurityDescriptor: ownerOnlyDACL,
	})
}
