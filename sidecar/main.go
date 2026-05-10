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
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
	"github.com/kubetail-org/kstack-app/sidecar/server"
)

func main() {
	slog.SetDefault(logging.Init(os.Stderr, logging.ParseLevel(os.Getenv("KSTACK_LOG"))))

	defaultSock := filepath.Join(os.TempDir(), fmt.Sprintf("kstack-sidecar-%d.sock", os.Getpid()))
	sockPath := flag.String("socket", defaultSock, "path to the Unix domain socket to listen on")
	flag.Parse()

	// Stale socket from a crashed previous run would block bind; remove it.
	_ = os.Remove(*sockPath)

	ln, err := net.Listen("unix", *sockPath)
	if err != nil {
		slog.Error("listen", "socket", *sockPath, "err", err)
		os.Exit(1)
	}
	// 0600: only the user that started us can connect. Windows ignores
	// POSIX modes (Chmod only toggles the read-only bit there); skip the
	// call to avoid a false sense of enforcement. NTFS DACL inherited from
	// the temp directory is the access boundary on Windows.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(*sockPath, 0o600); err != nil {
			slog.Error("chmod", "socket", *sockPath, "err", err)
			os.Exit(1)
		}
	}
	defer os.Remove(*sockPath)

	slog.Info("sidecar starting", "socket", *sockPath, "pid", os.Getpid())

	const maxRequestBytes = 64 * 1024 * 1024
	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	wrapped, waitForHijacked := server.AttachGracefulShutdown(srv, server.NewHandler())
	srv.Handler = http.MaxBytesHandler(wrapped, maxRequestBytes)

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

	reason := "signal"
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve", "err", err)
			os.Exit(1)
		}
		reason = "serve returned"
	}

	slog.Info("sidecar shutting down", "reason", reason)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	// http.Server.Shutdown doesn't wait for hijacked connections (per its
	// docs); explicit wait so WS handlers finish writing their close frames
	// before we exit and the OS reaps the socket.
	waitForHijacked()
}
