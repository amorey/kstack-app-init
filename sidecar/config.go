package main

import (
	"flag"
	"os"

	"github.com/kubetail-org/kstack-app/sidecar/internal/app"
	"github.com/kubetail-org/kstack-app/sidecar/internal/ipc"
)

// config is everything the command line decides: the app's own configuration
// plus the two values main needs to bind the listener.
type config struct {
	App    app.Config
	Socket string
	// Zero leaves the endpoint open to any process of this user, which is what
	// a standalone dev run wants; the host always passes its own.
	HostPID int
}

// configFromArgs parses the sidecar's command line. Endpoints are arguments,
// never environment: the sidecar inherits the host's environment, and anything
// that can set a variable there could otherwise redirect sign-in. Only a build
// tagged `debug` lets applyEnvOverrides put the environment back in play.
//
// On a bad flag the flag package has already written the problem and the usage
// to stderr; the caller only needs to exit.
func configFromArgs(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("kstack-sidecar", flag.ContinueOnError)
	fs.StringVar(&cfg.Socket, "socket", ipc.DefaultSocketPath(), "path to the IPC endpoint (Unix domain socket on Unix, named pipe on Windows) to listen on")
	fs.IntVar(&cfg.HostPID, "host-pid", 0, "pid of the host process; the only process allowed to connect (0 allows any process of this user)")
	fs.StringVar(&cfg.App.KubeconfigPath, "kubeconfig", "", "explicit kubeconfig path; empty uses the clientcmd default-loading rules ($KUBECONFIG / ~/.kube/config)")
	fs.StringVar(&cfg.App.DataDir, "data-dir", "", "app data dir for app.db and the per-cluster caches (required)")
	// The OAuth client is public (PKCE/loopback, no secret), so baking the
	// production defaults into the binary leaks nothing.
	fs.StringVar(&cfg.App.CloudURL, "cloud-url", "https://api.kstack.sh", "kstack cloud API base URL")
	fs.StringVar(&cfg.App.OAuthIssuerURL, "oauth-issuer", "https://oauth.kstack.sh", "OAuth issuer URL")
	fs.StringVar(&cfg.App.OAuthClientID, "oauth-client-id", "kstack-desktop", "OAuth client id")
	// The host passes its install's name, so a dev run and an installed
	// release never share one keychain entry.
	fs.StringVar(&cfg.App.KeychainService, "keychain-service", "", "keyring service name; empty is the Kstack default")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	applyEnvOverrides(&cfg)
	return cfg, nil
}

// applyEnvOverrides lets a standalone dev run — no host to pass arguments —
// point the sidecar somewhere else. It is a no-op unless the binary was built
// with `-tags debug` (`make sidecar-dev`); the body stays compiled in every
// build so vet and the tests always see it.
func applyEnvOverrides(cfg *config) {
	if !debugBuild {
		return
	}
	overrides := []struct {
		dst *string
		env string
	}{
		{&cfg.App.CloudURL, "KSTACK_CLOUD_API_URL"},
		{&cfg.App.OAuthIssuerURL, "KSTACK_OAUTH_ISSUER"},
		{&cfg.App.OAuthClientID, "KSTACK_OAUTH_CLIENT_ID"},
		{&cfg.App.DataDir, "KSTACK_DATA_DIR"},
		{&cfg.App.KeychainService, "KSTACK_KEYCHAIN_SERVICE"},
	}
	for _, o := range overrides {
		if v := os.Getenv(o.env); v != "" {
			*o.dst = v
		}
	}
}
