// Sidecar entry point. Started by the Tauri host, listens on a Unix
// domain socket (no TCP port), and prints the socket path to stdout as
// `READY unix:<path>` so the host can dial it.
//
// This file is lifecycle only: parse flags, bind the listener, build the App
// (the composition root lives in internal/app), serve, and drive graceful
// shutdown. Shutdown signals (any one is sufficient):
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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/app"
	"github.com/kubetail-org/kstack-app/sidecar/internal/ipc"
	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
)

func main() {
	// Before anything can create a file.
	setOwnerOnlyUmask()

	slog.SetDefault(logging.Init(os.Stderr, logging.ParseLevel(os.Getenv("KSTACK_LOG_LEVEL"))))

	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout))
}

// run is main without the process: it takes the command line and the two
// streams the host talks over, and returns the exit code instead of calling
// os.Exit. Everything main does after logging setup lives here so the boot and
// shutdown sequence is reachable from a test.
func run(args []string, stdin io.Reader, stdout io.Writer) int {
	cfg, err := configFromArgs(args)
	if err != nil {
		return 2
	}

	ln, err := ipc.Listen(cfg.Socket)
	if err != nil {
		slog.Error("listen", "socket", cfg.Socket, "err", err)
		return 1
	}
	ln = ipc.Authenticated(ln, ipc.Policy{HostPID: cfg.HostPID})
	// Named pipes vanish with their listener; only the UDS file needs cleanup.
	defer os.Remove(cfg.Socket)

	slog.Info("sidecar starting",
		"socket", cfg.Socket,
		"pid", os.Getpid(),
		"host_pid", cfg.HostPID,
		"data_dir", cfg.App.DataDir,
		"cloud_url", cfg.App.CloudURL,
		"oauth_issuer", cfg.App.OAuthIssuerURL,
	)

	application, err := app.New(cfg.App)
	if err != nil {
		slog.Error("app init", "err", err)
		return 1
	}

	const maxRequestBytes = 64 * 1024 * 1024
	srv := &http.Server{
		Handler:           http.MaxBytesHandler(application, maxRequestBytes),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Fired as Shutdown begins so long-lived streams end and Shutdown's wait for
	// in-flight requests can complete; hijacked h2c gRPC streams (which Shutdown
	// can't see) are drained afterwards by DrainWithContext.
	srv.RegisterOnShutdown(application.NotifyShutdown)

	// The host matches the `READY ` prefix; scheme+path are informational (the
	// host picked the path and passed it via --socket).
	fmt.Fprintf(stdout, "READY %s:%s\n", ipc.Scheme, cfg.Socket)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Watch stdin for EOF as a parent-died signal.
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		cancel()
	}()

	stop, err := application.Start(ctx)
	if err != nil {
		slog.Error("app start", "err", err)
		return 1
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	reason := "signal"
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("serve", "err", err)
			return 1
		}
		reason = "serve returned"
	}

	slog.Info("sidecar shutting down", "reason", reason)

	// Order matters: Shutdown (fires NotifyShutdown, waits for non-hijacked
	// GraphQL requests) → DrainWithContext (waits for hijacked h2c gRPC streams)
	// → stop → Close. See docs/adr/2026-08-09-single-socket-h2c.md.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	if err := application.DrainWithContext(shutdownCtx); err != nil {
		slog.Warn("drain did not complete", "err", err)
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStop()
	if err := stop(stopCtx); err != nil {
		slog.Warn("stop did not complete cleanly", "err", err)
	}
	_ = application.Close()
	return 0
}
