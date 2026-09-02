//go:build windows

package ipc

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/Microsoft/go-winio"
)

// testEndpoint returns a fresh pipe name for one test.
func testEndpoint(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf(`\\.\pipe\kstack-test-%d-%s`, os.Getpid(), t.Name())
}

func dialEndpoint(path string) (net.Conn, error) {
	return winio.DialPipe(path, nil)
}
