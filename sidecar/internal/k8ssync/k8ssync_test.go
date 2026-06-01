package k8ssync

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// configFingerprint must change when any auth-related field is edited — including
// exec/auth-provider/impersonation — so a kubeconfig edit while the app runs
// restarts sync instead of leaving reflectors on stale credentials.
func TestConfigFingerprintCoversAuthFields(t *testing.T) {
	base := func() *rest.Config {
		return &rest.Config{Host: "https://x:6443", BearerToken: "tok"}
	}
	fp := configFingerprint(base())
	require.Equal(t, fp, configFingerprint(base()), "identical configs hash equal")

	edits := map[string]func(*rest.Config){
		"exec command": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token"}
		},
		"exec args": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Args: []string{"--v2"}}
		},
		"exec env": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Env: []clientcmdapi.ExecEnvVar{{Name: "K", Value: "V"}}}
		},
		"auth provider": func(c *rest.Config) {
			c.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc", Config: map[string]string{"client-id": "abc"}}
		},
		"impersonate user": func(c *rest.Config) {
			c.Impersonate = rest.ImpersonationConfig{UserName: "admin"}
		},
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			c := base()
			edit(c)
			require.NotEqual(t, fp, configFingerprint(c), "edit must change the fingerprint")
		})
	}

	// Editing an existing exec field (not just adding the block) must also change it.
	e1, e2 := base(), base()
	e1.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--a"}}
	e2.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--b"}}
	require.NotEqual(t, configFingerprint(e1), configFingerprint(e2), "editing exec args changes the fingerprint")
}
