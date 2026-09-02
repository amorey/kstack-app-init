//go:build !windows

package ipc

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// peerOf must report the process on the other end, not this one's idea of
// itself — the whole peer check is downstream of it being right.
func TestPeerOf_ReportsConnectingProcess(t *testing.T) {
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "s.sock"))
	require.NoError(t, err)
	defer ln.Close()

	client, err := net.Dial("unix", ln.Addr().String())
	require.NoError(t, err)
	defer client.Close()

	server, err := ln.Accept()
	require.NoError(t, err)
	defer server.Close()

	p, err := peerOf(server)
	require.NoError(t, err)
	assert.Equal(t, os.Getpid(), p.pid)
	assert.Equal(t, os.Getuid(), p.uid)
}
