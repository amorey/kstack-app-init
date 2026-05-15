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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
	"github.com/kubetail-org/kstack-app/sidecar/server"
)

func main() {
	slog.SetDefault(logging.Init(os.Stderr, logging.ParseLevel(os.Getenv("KSTACK_LOG"))))

	sockPath := flag.String("socket", defaultSocketPath(), "path to the IPC endpoint (Unix domain socket on Unix, named pipe on Windows) to listen on")
	// Default points at production; override via env (or --cloud-url) for
	// local dev. The cloud GraphQL client appends `/graphql` itself, so
	// callers only ever name the host.
	cloudURL := flag.String("cloud-url", envOr("KSTACK_CLOUD_URL", "https://api.kstack.sh"), "base URL of the kstack cloud (without /graphql)")
	prefsPath := flag.String("prefs-path", server.DefaultPrefsPath(), "path to the local preferences cache file")
	flag.Parse()

	// Per-OS binding: AF_UNIX socket on Unix, named pipe on Windows.
	// Both endpoints are restricted to the current user (chmod 0600 / DACL).
	ln, err := listenSocket(*sockPath)
	if err != nil {
		slog.Error("listen", "socket", *sockPath, "err", err)
		os.Exit(1)
	}
	// Named pipes vanish with their listener; only the UDS file needs explicit
	// cleanup. Remove is a no-op for non-existent paths so unconditional is fine.
	defer os.Remove(*sockPath)

	slog.Info("sidecar starting",
		"socket", *sockPath,
		"pid", os.Getpid(),
		"cloud_url", *cloudURL,
		"prefs_path", *prefsPath,
	)

	const maxRequestBytes = 64 * 1024 * 1024
	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	wrapped, waitForHijacked := server.AttachGracefulShutdown(srv, server.NewHandler(server.Config{
		CloudURL:  *cloudURL,
		PrefsPath: *prefsPath,
	}))
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

// envOr returns the value of env var `key`, or `fallback` if unset/empty.
// Used to give flags a "production default that env can override" shape
// without each call site spelling out the lookup.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
