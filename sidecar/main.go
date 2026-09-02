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
	"flag"
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
	slog.SetDefault(logging.Init(os.Stderr, logging.ParseLevel(os.Getenv("KSTACK_LOG_LEVEL"))))

	sockPath := flag.String("socket", ipc.DefaultSocketPath(), "path to the IPC endpoint (Unix domain socket on Unix, named pipe on Windows) to listen on")
	kubeconfigPath := flag.String("kubeconfig", "", "explicit kubeconfig path; empty uses the clientcmd default-loading rules ($KUBECONFIG / ~/.kube/config)")
	// Zero (the default) leaves the endpoint open to any process of this user,
	// which is what a standalone dev run wants; the host always passes its own.
	hostPID := flag.Int("host-pid", 0, "pid of the host process; the only process allowed to connect (0 allows any process of this user)")
	// The host passes its app_local_data_dir(); required — app.New errors when empty.
	dataDir := flag.String("data-dir", envOr("KSTACK_DATA_DIR", ""), "app data dir for app.db and the per-cluster caches (defaults to KSTACK_DATA_DIR; required)")
	flag.Parse()

	ln, err := ipc.Listen(*sockPath)
	if err != nil {
		slog.Error("listen", "socket", *sockPath, "err", err)
		os.Exit(1)
	}
	ln = ipc.Authenticated(ln, ipc.Policy{HostPID: *hostPID})
	// Named pipes vanish with their listener; only the UDS file needs cleanup.
	defer os.Remove(*sockPath)

	slog.Info("sidecar starting",
		"socket", *sockPath,
		"pid", os.Getpid(),
		"host_pid", *hostPID,
		"data_dir", *dataDir,
	)

	// Cloud/OAuth defaults are the kstack production endpoints (env-overridable).
	// The OAuth client is public (PKCE/loopback, no secret), so baking the
	// defaults into the binary leaks nothing.
	application, err := app.New(app.Config{
		KubeconfigPath: *kubeconfigPath,
		DataDir:        *dataDir,
		CloudURL:       envOr("KSTACK_CLOUD_API_URL", "https://api.kstack.sh"),
		OAuthIssuerURL: envOr("KSTACK_OAUTH_ISSUER", "https://oauth.kstack.sh"),
		OAuthClientID:  envOr("KSTACK_OAUTH_CLIENT_ID", "kstack-desktop"),
		// Empty ⇒ the "Kstack" default; the host sets a dev-specific name so
		// dev and release runs don't share one keychain entry.
		KeychainService: os.Getenv("KSTACK_KEYCHAIN_SERVICE"),
	})
	if err != nil {
		slog.Error("app init", "err", err)
		os.Exit(1)
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
	fmt.Printf("READY %s:%s\n", ipc.Scheme, *sockPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Watch stdin for EOF as a parent-died signal.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()

	stop, err := application.Start(ctx)
	if err != nil {
		slog.Error("app start", "err", err)
		os.Exit(1)
	}

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
}

// envOr returns env var `key`, or `fallback` if unset/empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
