// Sidecar entry point. Started by the Tauri host, listens on a Unix
// domain socket (no TCP port), and prints the socket path to stdout as
// `READY unix:<path>` so the host can dial it.
//
// Shutdown signals (any one is sufficient):
//   - SIGINT / SIGTERM
//   - stdin EOF (the Tauri host closes its end when it exits, which is
//     the most reliable cross-platform "parent gone" indicator)
//
// AF_UNIX is supported on macOS, Linux, and Windows 10 build 17063+,
// so a single UDS path works for all our targets.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/server"
)

func main() {
	defaultSock := filepath.Join(os.TempDir(), fmt.Sprintf("kstack-sidecar-%d.sock", os.Getpid()))
	sockPath := flag.String("socket", defaultSock, "path to the Unix domain socket to listen on")
	flag.Parse()

	// Stale socket from a crashed previous run would block bind; remove it.
	_ = os.Remove(*sockPath)

	ln, err := net.Listen("unix", *sockPath)
	if err != nil {
		log.Fatalf("listen %s: %v", *sockPath, err)
	}
	// 0600: only the user that started us can connect. Windows ignores
	// POSIX modes (Chmod only toggles the read-only bit there); skip the
	// call to avoid a false sense of enforcement. NTFS DACL inherited from
	// the temp directory is the access boundary on Windows.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(*sockPath, 0o600); err != nil {
			log.Fatalf("chmod %s: %v", *sockPath, err)
		}
	}
	defer os.Remove(*sockPath)

	const maxRequestBytes = 64 * 1024 * 1024
	srv := &http.Server{
		Handler:           http.MaxBytesHandler(server.NewHandler(), maxRequestBytes),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Announce. Host parses this line to learn the socket path.
	fmt.Printf("READY unix:%s\n", *sockPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Watch stdin for EOF as a parent-died signal.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
}
