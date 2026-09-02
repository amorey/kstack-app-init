package ipc

import (
	"errors"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listenAuth binds a real endpoint (UDS or named pipe) behind the policy, and
// returns the wrapped listener with the address a client dials.
func listenAuth(t *testing.T, p Policy) (net.Listener, string) {
	t.Helper()
	path := testEndpoint(t)
	base, err := Listen(path)
	require.NoError(t, err)
	ln := Authenticated(base, p)
	t.Cleanup(func() { _ = ln.Close() })
	return ln, path
}

// The point of the whole package: a process that is not the host gets its
// connection closed before it can speak a byte of GraphQL or gRPC.
func TestAuthenticated_RejectsForeignPeer(t *testing.T) {
	ln, path := listenAuth(t, Policy{HostPID: os.Getpid() + 1})

	client, err := dialEndpoint(path)
	require.NoError(t, err)
	defer client.Close()

	// Accept drives the check. A served peer would get the byte back; a
	// rejected one gets a closed connection. Either way the Read below
	// returns, so nothing waits on a clock.
	go func() {
		if conn, err := ln.Accept(); err == nil {
			_, _ = conn.Write([]byte{42})
		}
	}()

	_, err = client.Read(make([]byte, 1))
	assert.Error(t, err, "rejected peer must not be served")
}

// A rejection is not a listener error: returning it from Accept would let any
// unauthorized process kill the sidecar's serving loop.
func TestAuthenticated_KeepsAcceptingAfterRejection(t *testing.T) {
	ln, path := listenAuth(t, Policy{HostPID: os.Getpid()})

	// Reject the first peer only, so the second exercises the recovery path.
	first := true
	ln.(*authListener).peerOf = func(net.Conn) (peer, error) {
		if first {
			first = false
			return peer{pid: os.Getpid() + 1, uid: os.Getuid()}, nil
		}
		return peer{pid: os.Getpid(), uid: os.Getuid()}, nil
	}

	rejected, err := dialEndpoint(path)
	require.NoError(t, err)
	defer rejected.Close()

	assertRoundTrip(t, ln, path)

	_, err = rejected.Read(make([]byte, 1))
	assert.Error(t, err, "first peer should have been rejected")
}

func TestAuthenticated_AcceptsHostPeer(t *testing.T) {
	ln, path := listenAuth(t, Policy{HostPID: os.Getpid()})
	assertRoundTrip(t, ln, path)
}

// A standalone sidecar run has no host pid to pin, and falls back to the uid.
func TestAuthenticated_WithoutHostPIDAcceptsSameUID(t *testing.T) {
	ln, path := listenAuth(t, Policy{})
	assertRoundTrip(t, ln, path)
}

// A closed listener still reports its error, so Serve can exit.
func TestAuthenticated_PropagatesListenerClose(t *testing.T) {
	ln, _ := listenAuth(t, Policy{})
	require.NoError(t, ln.Close())

	_, err := ln.Accept()
	require.Error(t, err)
	assert.True(t, errors.Is(err, net.ErrClosed), "got %v", err)
}

// assertRoundTrip dials the endpoint and checks a byte survives the trip, i.e.
// the connection was accepted and handed to the server.
func assertRoundTrip(t *testing.T, ln net.Listener, path string) {
	t.Helper()
	client, err := dialEndpoint(path)
	require.NoError(t, err)
	defer client.Close()

	server, err := ln.Accept()
	require.NoError(t, err)
	defer server.Close()

	_, err = server.Write([]byte{42})
	require.NoError(t, err)

	buf := make([]byte, 1)
	_, err = client.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, byte(42), buf[0])
}
