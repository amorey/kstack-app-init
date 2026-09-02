//go:build !windows

package ipc

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// peerOf must report the process on the other end, not this one's idea of
// itself — the whole peer check is downstream of it being right.
func TestPeerOf_ReportsConnectingProcess(t *testing.T) {
	path := testEndpoint(t)
	ln, err := Listen(path)
	require.NoError(t, err)
	defer ln.Close()

	accepted := acceptOne(ln)
	client, err := dialEndpoint(path)
	require.NoError(t, err)
	defer client.Close()

	server, ok := <-accepted
	require.True(t, ok)
	defer server.Close()

	p, err := peerOf(server)
	require.NoError(t, err)
	assert.Equal(t, os.Getpid(), p.pid)
	assert.Equal(t, os.Getuid(), p.uid)
}
