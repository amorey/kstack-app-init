//go:build windows

package main

import (
	"fmt"
	"net"
	"os"

	"github.com/Microsoft/go-winio"
)

// defaultSocketPath returns a named-pipe path. The Rust host dials this
// via `tokio::net::windows::named_pipe::ClientOptions::open`, which takes
// the `\\.\pipe\...` form verbatim. In practice the host passes its own
// per-instance path via `--socket`; this default only applies when the
// sidecar is run standalone.
func defaultSocketPath() string {
	return fmt.Sprintf(`\\.\pipe\kstack-sidecar-%d`, os.Getpid())
}

// socketScheme labels the IPC transport in human-readable diagnostics
// (e.g. the `READY <scheme>:<path>` line in main.go). The host doesn't
// parse this; it's a hint for logs and external tooling. The Unix build
// uses `unix:` for AF_UNIX paths.
const socketScheme = "pipe"

// ownerOnlyDACL grants Generic All to the process owner only via the
// well-known SID `OW` (Owner Rights). `D:P` makes the DACL protected —
// no inherited ACEs from the parent — so the listed ACE is the entire
// access policy. Analogous to chmod 0600 on Unix.
const ownerOnlyDACL = "D:P(A;;GA;;;OW)"

// listenSocket binds a named pipe restricted to the current user.
func listenSocket(path string) (net.Listener, error) {
	return winio.ListenPipe(path, &winio.PipeConfig{
		SecurityDescriptor: ownerOnlyDACL,
	})
}
