// Copyright 2024 The Kubetail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package k8shelpers

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// Helper function to create a unique pathname
func generateUniquePathname(dirname string) string {
	return filepath.Join(dirname, fmt.Sprintf("config-%s", uuid.New().String()))
}

// Helper function to create a temporary directory with a sample kubeconfig file
func createKubeConfig(kubeconfigPath string) (*clientcmdapi.Config, error) {
	uuid := uuid.New().String()

	cluster := fmt.Sprintf("cluster-%s", uuid)
	user := fmt.Sprintf("user-%s", uuid)
	context := fmt.Sprintf("context-%s", uuid)

	// Create a new empty config
	cfg := clientcmdapi.NewConfig()

	// Populate the config
	cfg.Clusters[cluster] = &clientcmdapi.Cluster{}
	cfg.AuthInfos[user] = &clientcmdapi.AuthInfo{}
	cfg.Contexts[context] = &clientcmdapi.Context{}
	cfg.CurrentContext = context

	// Write the config to a file
	if err := clientcmd.WriteToFile(*cfg, kubeconfigPath); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Helper function to create a kubeconfig file with the given named
// contexts (each with its own cluster + user) and current-context.
func createKubeConfigWithContexts(kubeconfigPath, current string, contexts ...string) (*clientcmdapi.Config, error) {
	cfg := clientcmdapi.NewConfig()
	for _, name := range contexts {
		cluster := fmt.Sprintf("cluster-%s", name)
		user := fmt.Sprintf("user-%s", name)
		cfg.Clusters[cluster] = &clientcmdapi.Cluster{}
		cfg.AuthInfos[user] = &clientcmdapi.AuthInfo{}
		cfg.Contexts[name] = &clientcmdapi.Context{Cluster: cluster, AuthInfo: user}
	}
	cfg.CurrentContext = current

	if err := clientcmd.WriteToFile(*cfg, kubeconfigPath); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Helper function to merge two maps
func mergeMaps[K comparable, V any](a, b map[K]V) map[K]V {
	out := make(map[K]V, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}

// Helper function to assert that two maps have the same keys
func compareMaps[K comparable, V any](t *testing.T, m1 map[K]*V, m2 map[K]*V) {
	assert.Equal(t, len(m1), len(m2))
	for k := range m1 {
		_, ok := m2[k]
		assert.True(t, ok)
	}
}

func TestKubeConfigWatcherGet(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	t.Run("single file", func(t *testing.T) {
		// Create pathname
		kubeconfigPath := generateUniquePathname(tempDir)

		// Create config file
		cfgExpected, err := createKubeConfig(kubeconfigPath)
		if err != nil {
			t.Fatal(err)
		}

		// Initialize watcher
		watcher, err := NewKubeConfigWatcher(kubeconfigPath)
		if err != nil {
			t.Fatal(err)
		}
		defer watcher.Close()

		// Check config
		cfgActual := watcher.Get()
		compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
		compareMaps(t, cfgExpected.AuthInfos, cfgActual.AuthInfos)
		compareMaps(t, cfgExpected.Contexts, cfgActual.Contexts)
		assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
	})

	t.Run("multiple files", func(t *testing.T) {
		// Create pathnames
		p1 := generateUniquePathname(tempDir)
		p2 := generateUniquePathname(tempDir)

		// Create config files
		cfg1, err := createKubeConfig(p1)
		require.NoError(t, err)
		cfg2, err := createKubeConfig(p2)
		require.NoError(t, err)

		// Set environment
		sep := string(os.PathListSeparator)
		t.Setenv(clientcmd.RecommendedConfigPathEnvVar, fmt.Sprintf("%s%s%s", p1, sep, p2))

		// Init watcher
		watcher, err := NewKubeConfigWatcher("")
		if err != nil {
			t.Fatal(err)
		}
		defer watcher.Close()

		// Check config
		cfgActual := watcher.Get()

		expectedClusters := mergeMaps(cfg1.Clusters, cfg2.Clusters)
		compareMaps(t, expectedClusters, cfgActual.Clusters)

		expectedAuthInfos := mergeMaps(cfg1.AuthInfos, cfg2.AuthInfos)
		compareMaps(t, expectedAuthInfos, cfgActual.AuthInfos)

		expectedContexts := mergeMaps(cfg1.Contexts, cfg2.Contexts)
		compareMaps(t, expectedContexts, cfgActual.Contexts)

		assert.Equal(t, cfg1.CurrentContext, cfgActual.CurrentContext)
	})
}

func TestKubeConfigWatcherSubscribeModified(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Create pathnames
	p1 := generateUniquePathname(tempDir)
	p2 := generateUniquePathname(tempDir)

	// Create config files
	cfg1, err := createKubeConfig(p1)
	require.NoError(t, err)
	_, err = createKubeConfig(p2)
	require.NoError(t, err)

	// Set environment
	sep := string(os.PathListSeparator)
	t.Setenv(clientcmd.RecommendedConfigPathEnvVar, fmt.Sprintf("%s%s%s", p1, sep, p2))

	// Initialize watcher
	watcher, err := NewKubeConfigWatcher("")
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	// Subscribe to changes
	sub := watcher.Subscribe()
	defer sub.Close()

	// Drain the seeded initial value
	<-sub.Chan()

	// Modify one of the files
	cfg2, err := createKubeConfig(p2)
	require.NoError(t, err)

	// Get change
	cfgActual := <-sub.Chan()

	// Check new config
	expectedClusters := mergeMaps(cfg1.Clusters, cfg2.Clusters)
	compareMaps(t, expectedClusters, cfgActual.Clusters)

	expectedAuthInfos := mergeMaps(cfg1.AuthInfos, cfg2.AuthInfos)
	compareMaps(t, expectedAuthInfos, cfgActual.AuthInfos)

	expectedContexts := mergeMaps(cfg1.Contexts, cfg2.Contexts)
	compareMaps(t, expectedContexts, cfgActual.Contexts)

	assert.Equal(t, cfg1.CurrentContext, cfgActual.CurrentContext)
}

// SetCurrentContext on a valid context must (1) persist to disk so kubectl
// and other tools see the change, (2) update the in-memory snapshot, and
// (3) republish the new config to subscribers without waiting on the
// debounced fsnotify echo.
func TestSetCurrentContext_ValidPersistsAndReloads(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	kubeconfigPath := filepath.Join(tempDir, "config")
	_, err = createKubeConfigWithContexts(kubeconfigPath, "context-A", "context-A", "context-B")
	require.NoError(t, err)

	w, err := NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	defer w.Close()

	sub := w.Subscribe()
	defer sub.Close()

	// Drain the seeded initial value.
	select {
	case <-sub.Chan():
	case <-time.After(time.Second):
		t.Fatal("expected initial seeded value")
	}

	require.NoError(t, w.SetCurrentContext("context-B"))

	// In-memory snapshot reflects the change.
	assert.Equal(t, "context-B", w.Get().CurrentContext)

	// Persisted to disk (kubectl-visible).
	onDisk, err := clientcmd.LoadFromFile(kubeconfigPath)
	require.NoError(t, err)
	assert.Equal(t, "context-B", onDisk.CurrentContext)

	// Subscribers receive a config carrying the new current-context.
	select {
	case cfgActual := <-sub.Chan():
		assert.Equal(t, "context-B", cfgActual.CurrentContext)
	case <-time.After(2 * time.Second):
		t.Fatal("expected republish after SetCurrentContext")
	}
}

// SetCurrentContext on an unknown context must error and leave both disk
// and the in-memory snapshot untouched.
func TestSetCurrentContext_UnknownContextErrors(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	kubeconfigPath := filepath.Join(tempDir, "config")
	_, err = createKubeConfigWithContexts(kubeconfigPath, "context-A", "context-A", "context-B")
	require.NoError(t, err)

	w, err := NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	defer w.Close()

	err = w.SetCurrentContext("does-not-exist")
	require.Error(t, err)

	assert.Equal(t, "context-A", w.Get().CurrentContext)

	onDisk, err := clientcmd.LoadFromFile(kubeconfigPath)
	require.NoError(t, err)
	assert.Equal(t, "context-A", onDisk.CurrentContext)
}

// A missing kubeconfig is non-fatal: the watcher constructs successfully
// with an empty *api.Config seed so resolvers stay safe and the renderer
// can surface the empty state. (Picking up a file that appears later is
// a separate enhancement.)
func TestKubeConfigWatcher_FileNotFound(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	nonExistentPath := filepath.Join(tempDir, "non-existent-config")

	w, err := NewKubeConfigWatcher(nonExistentPath)
	require.NoError(t, err)
	defer w.Close()

	cfg := w.Get()
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.AuthInfos)
	assert.Empty(t, cfg.Clusters)
	assert.Empty(t, cfg.Contexts)
}

// atomicReplaceKubeConfig writes a fresh config to a temp sibling and
// renames it over the target — the editor-style "write to .swp / rename"
// pattern that detaches a file-level fsnotify watch on the first rename.
// (clientcmd.WriteToFile is in-place truncate, so it can't exercise this.)
func atomicReplaceKubeConfig(t *testing.T, kubeconfigPath string) *clientcmdapi.Config {
	t.Helper()
	tmp := kubeconfigPath + ".tmp"
	cfg, err := createKubeConfig(tmp)
	require.NoError(t, err)
	require.NoError(t, os.Rename(tmp, kubeconfigPath))
	return cfg
}

// Editors and many config-management tools use write-tmp-then-rename,
// which detaches a file-level fsnotify watch on the first rename. The
// watcher must remain observant across repeated atomic writes — emit one
// reload per write, indefinitely, without restart.
func TestKubeConfigWatcher_HandlesRepeatedAtomicWrites(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	kubeconfigPath := filepath.Join(tempDir, "config")

	// File exists at startup so the watcher takes the file-present code path.
	_, err = createKubeConfig(kubeconfigPath)
	require.NoError(t, err)

	w, err := NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	defer w.Close()

	sub := w.Subscribe()
	defer sub.Close()

	// Drain the seeded initial value.
	select {
	case <-sub.Chan():
	case <-time.After(time.Second):
		t.Fatal("expected initial seeded value")
	}

	for i := 0; i < 2; i++ {
		cfgExpected := atomicReplaceKubeConfig(t, kubeconfigPath)

		select {
		case cfgActual := <-sub.Chan():
			compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
			assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
		case <-time.After(2 * time.Second):
			t.Fatalf("reload %d did not arrive — file watch likely detached after the first atomic write", i+1)
		}
	}
}

// When the configured path doesn't exist at start time, the watcher must
// still publish a reload event once the file appears on disk. Without
// parent-dir watching, fsnotify can't observe a CREATE on an absent file.
func TestKubeConfigWatcher_PicksUpFileCreatedAfterStart(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	kubeconfigPath := filepath.Join(tempDir, "config")

	w, err := NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	defer w.Close()

	sub := w.Subscribe()
	defer sub.Close()

	// Drain the empty seed so the next Chan read is the post-create reload.
	select {
	case <-sub.Chan():
	case <-time.After(time.Second):
		t.Fatal("expected initial seeded value")
	}

	cfgExpected, err := createKubeConfig(kubeconfigPath)
	require.NoError(t, err)

	select {
	case cfgActual := <-sub.Chan():
		compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
		compareMaps(t, cfgExpected.AuthInfos, cfgActual.AuthInfos)
		compareMaps(t, cfgExpected.Contexts, cfgActual.Contexts)
		assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not publish reload after kubeconfig was created")
	}
}
