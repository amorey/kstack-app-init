package main

import (
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/app"
	"github.com/kubetail-org/kstack-app/sidecar/internal/ipc"
)

// What a bare command line yields: the production endpoints and nothing else.
var productionConfig = config{
	App: app.Config{
		CloudURL:       "https://api.kstack.sh",
		OAuthIssuerURL: "https://oauth.kstack.sh",
		OAuthClientID:  "kstack-desktop",
	},
	Socket: ipc.DefaultSocketPath(),
}

func mustConfigFromArgs(t *testing.T, args []string) config {
	t.Helper()
	cfg, err := configFromArgs(args)
	if err != nil {
		t.Fatalf("configFromArgs(%q): %v", args, err)
	}
	return cfg
}

func TestConfigFromArgsDefaultsToProduction(t *testing.T) {
	if cfg := mustConfigFromArgs(t, nil); cfg != productionConfig {
		t.Errorf("config = %+v, want %+v", cfg, productionConfig)
	}
}

// Pins the tag boundary. `go test ./...` builds untagged, so the ordinary run
// asserts the environment never reaches the config; `go test -tags debug`
// asserts the override a `make sidecar-dev` build honours.
func TestConfigFromArgsIgnoresEnvironment(t *testing.T) {
	t.Setenv("KSTACK_CLOUD_API_URL", "https://cloud.override")
	t.Setenv("KSTACK_OAUTH_ISSUER", "https://issuer.override")
	t.Setenv("KSTACK_OAUTH_CLIENT_ID", "override")
	t.Setenv("KSTACK_DATA_DIR", "/tmp/override")
	t.Setenv("KSTACK_KEYCHAIN_SERVICE", "Kstack-override")

	want := productionConfig
	if debugBuild {
		want.App.CloudURL = "https://cloud.override"
		want.App.OAuthIssuerURL = "https://issuer.override"
		want.App.OAuthClientID = "override"
		want.App.DataDir = "/tmp/override"
		want.App.KeychainService = "Kstack-override"
	}
	if cfg := mustConfigFromArgs(t, nil); cfg != want {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}

func TestConfigFromArgsReadsFlags(t *testing.T) {
	want := config{
		App: app.Config{
			KubeconfigPath:  "/tmp/kubeconfig",
			DataDir:         "/tmp/data",
			CloudURL:        "https://cloud.example",
			OAuthIssuerURL:  "https://issuer.example",
			OAuthClientID:   "other",
			KeychainService: "Kstack-dev",
		},
		Socket:  "/tmp/sock",
		HostPID: 42,
	}
	cfg := mustConfigFromArgs(t, []string{
		"--socket", "/tmp/sock",
		"--host-pid", "42",
		"--kubeconfig", "/tmp/kubeconfig",
		"--data-dir", "/tmp/data",
		"--cloud-url", "https://cloud.example",
		"--oauth-issuer", "https://issuer.example",
		"--oauth-client-id", "other",
		"--keychain-service", "Kstack-dev",
	})
	if cfg != want {
		t.Errorf("config = %+v, want %+v", cfg, want)
	}
}
