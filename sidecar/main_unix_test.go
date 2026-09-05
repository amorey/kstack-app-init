//go:build !windows

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// socketPath returns a bindable AF_UNIX path inside the test's temp dir.
func socketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "sidecar.sock")
}

// unbindableEndpoint names a socket under a directory that does not exist.
func unbindableEndpoint(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "missing", "sidecar.sock")
}

// The full lifecycle: bind, announce READY, serve, then shut down on stdin EOF
// (the host's parent-died signal) and clean the socket file up. Serving is
// proven by a real request over the socket, which is also what makes the
// shutdown deterministic — no wall-clock wait for the server to come up.
func TestRunServesUntilStdinCloses(t *testing.T) {
	sock := socketPath(t)
	stdin, hostEnd := net.Pipe() // hostEnd.Close() is the host exiting
	out := &syncBuffer{}

	var code int
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		code = run([]string{"--socket", sock, "--data-dir", t.TempDir(), "--kubeconfig", filepath.Join(t.TempDir(), "none")}, stdin, out)
	}()

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	// The socket appears when run binds it; a served response proves Serve is up.
	require.Eventually(t, func() bool {
		resp, err := client.Get("http://sidecar/graphql")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	}, 10*time.Second, 10*time.Millisecond, "sidecar never served over its socket")

	hostEnd.Close()
	done.Wait()

	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if got := out.String(); !strings.Contains(got, "READY unix:"+sock) {
		t.Errorf("stdout = %q, want the READY line for %s", got, sock)
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("socket file still present after shutdown: %v", err)
	}
}

// syncBuffer collects run's stdout while the test reads it from another
// goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
