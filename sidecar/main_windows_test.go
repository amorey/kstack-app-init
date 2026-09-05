//go:build windows

package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/ipc"
)

// socketPath returns a bindable pipe name for one test, pid-namespaced so a
// concurrent run of the package doesn't collide.
func socketPath(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf(`\\.\pipe\kstack-test-%d-%s`, os.Getpid(), t.Name())
}

// unbindableEndpoint names a pipe this test already holds: the first instance
// of a name is created exclusively, so a second bind collides.
func unbindableEndpoint(t *testing.T) string {
	t.Helper()
	path := socketPath(t)
	ln, err := ipc.Listen(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return path
}
