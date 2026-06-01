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
	// Default points at production; override via env (or --cloud-url) for
	// local dev. The cloud GraphQL client appends `/graphql` itself, so
	// callers only ever name the host.
	cloudURL := flag.String("cloud-url", envOr("KSTACK_CLOUD_URL", "https://api.kstack.sh"), "base URL of the kstack cloud (without /graphql)")
	prefsPath := flag.String("prefs-path", app.DefaultPrefsPath(), "path to the local preferences cache file")
	kubeconfigPath := flag.String("kubeconfig", "", "explicit kubeconfig path; empty uses the clientcmd default-loading rules ($KUBECONFIG / ~/.kube/config)")
	// The Tauri host passes its app_local_data_dir() here so per-cluster SQLite
	// caches land in the OS-correct per-machine data location. Standalone runs
	// (tests, dev) may omit it; the cluster cache is then disabled.
	dataDir := flag.String("data-dir", envOr("KSTACK_DATA_DIR", ""), "host-supplied app data dir for the per-cluster cluster cache (defaults to KSTACK_DATA_DIR; empty disables the cache)")
	flag.Parse()

	// Per-OS binding: AF_UNIX socket on Unix, named pipe on Windows.
	// Both endpoints are restricted to the current user (chmod 0600 / DACL).
	ln, err := ipc.Listen(*sockPath)
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
		"data_dir", *dataDir,
	)

	application, err := app.New(app.Config{
		CloudURL:       *cloudURL,
		PrefsPath:      *prefsPath,
		KubeconfigPath: *kubeconfigPath,
		DataDir:        *dataDir,
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
	// Fire the app's shutdown signal the instant Shutdown begins: gRPC Watch
	// handlers return (OK trailers flush) and SSE subscriptions flush their
	// terminal frame, so Shutdown's wait for in-flight GraphQL requests can
	// complete. The hijacked h2c gRPC streams it does not track are drained
	// afterwards by DrainWithContext.
	srv.RegisterOnShutdown(application.NotifyShutdown)

	// Announce. The host matches the `READY ` prefix to know the listener
	// is up; the scheme + path are for human-readable logs (the host
	// already knows the path — it picked it pre-spawn and passed it via
	// --socket).
	fmt.Printf("READY %s:%s\n", ipc.Scheme, *sockPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Watch stdin for EOF as a parent-died signal.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()

	application.Start(ctx)

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

	// Stop accepting connections and fire RegisterOnShutdown (app.NotifyShutdown),
	// which signals both transports' long-lived streams. Shutdown then waits for
	// the non-hijacked GraphQL requests (SSE flushes its terminal frame on the
	// cancelled per-request context). DrainWithContext additionally waits for the
	// hijacked h2c gRPC streams, and only then does Close stop the gRPC transports,
	// the sync engine, and the kube-config watcher.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	if err := application.DrainWithContext(shutdownCtx); err != nil {
		slog.Warn("drain did not complete", "err", err)
	}
	_ = application.Close()
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
