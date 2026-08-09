// Copyright 2026 The Kstack Authors
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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func generateUniquePathname(dirname string) string {
	return filepath.Join(dirname, fmt.Sprintf("config-%s", uuid.New().String()))
}

// createKubeConfig writes a sample kubeconfig (one cluster/user/context) to path.
func createKubeConfig(kubeconfigPath string) (*clientcmdapi.Config, error) {
	uuid := uuid.New().String()

	cluster := fmt.Sprintf("cluster-%s", uuid)
	user := fmt.Sprintf("user-%s", uuid)
	context := fmt.Sprintf("context-%s", uuid)

	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[cluster] = &clientcmdapi.Cluster{}
	cfg.AuthInfos[user] = &clientcmdapi.AuthInfo{}
	cfg.Contexts[context] = &clientcmdapi.Context{}
	cfg.CurrentContext = context

	if err := clientcmd.WriteToFile(*cfg, kubeconfigPath); err != nil {
		return nil, err
	}

	return cfg, nil
}

func mergeMaps[K comparable, V any](a, b map[K]V) map[K]V {
	out := make(map[K]V, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}

// compareMaps asserts that two maps have the same keys.
func compareMaps[K comparable, V any](t *testing.T, m1 map[K]*V, m2 map[K]*V) {
	assert.Equal(t, len(m1), len(m2))
	for k := range m1 {
		_, ok := m2[k]
		assert.True(t, ok)
	}
}

func TestKubeConfigWatcherGet(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	t.Run("single file", func(t *testing.T) {
		kubeconfigPath := generateUniquePathname(tempDir)

		cfgExpected, err := createKubeConfig(kubeconfigPath)
		if err != nil {
			t.Fatal(err)
		}

		watcher, err := NewKubeConfigWatcher(kubeconfigPath)
		if err != nil {
			t.Fatal(err)
		}
		defer watcher.Close()

		cfgActual := watcher.Get()
		compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
		compareMaps(t, cfgExpected.AuthInfos, cfgActual.AuthInfos)
		compareMaps(t, cfgExpected.Contexts, cfgActual.Contexts)
		assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
	})

	t.Run("multiple files", func(t *testing.T) {
		p1 := generateUniquePathname(tempDir)
		p2 := generateUniquePathname(tempDir)

		cfg1, err := createKubeConfig(p1)
		require.NoError(t, err)
		cfg2, err := createKubeConfig(p2)
		require.NoError(t, err)

		sep := string(os.PathListSeparator)
		t.Setenv(clientcmd.RecommendedConfigPathEnvVar, fmt.Sprintf("%s%s%s", p1, sep, p2))

		watcher, err := NewKubeConfigWatcher("")
		if err != nil {
			t.Fatal(err)
		}
		defer watcher.Close()

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
	tempDir, err := os.MkdirTemp("", "kube-config-watcher-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	p1 := generateUniquePathname(tempDir)
	p2 := generateUniquePathname(tempDir)

	cfg1, err := createKubeConfig(p1)
	require.NoError(t, err)
	_, err = createKubeConfig(p2)
	require.NoError(t, err)

	sep := string(os.PathListSeparator)
	t.Setenv(clientcmd.RecommendedConfigPathEnvVar, fmt.Sprintf("%s%s%s", p1, sep, p2))

	watcher, err := NewKubeConfigWatcher("")
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	watcher.Start()

	sub := watcher.Subscribe()
	defer sub.Close()

	// Drain the seeded initial value.
	testutil.Recv(t, sub.Chan(), "the initial seeded value")

	// Modify one of the files.
	cfg2, err := createKubeConfig(p2)
	require.NoError(t, err)

	cfgActual := testutil.Recv(t, sub.Chan(), "the reload published after the second file changed")

	expectedClusters := mergeMaps(cfg1.Clusters, cfg2.Clusters)
	compareMaps(t, expectedClusters, cfgActual.Clusters)

	expectedAuthInfos := mergeMaps(cfg1.AuthInfos, cfg2.AuthInfos)
	compareMaps(t, expectedAuthInfos, cfgActual.AuthInfos)

	expectedContexts := mergeMaps(cfg1.Contexts, cfg2.Contexts)
	compareMaps(t, expectedContexts, cfgActual.Contexts)

	assert.Equal(t, cfg1.CurrentContext, cfgActual.CurrentContext)
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
	w.Start()

	sub := w.Subscribe()
	defer sub.Close()

	// Drain the seeded initial value.
	testutil.Recv(t, sub.Chan(), "the initial seeded value")

	for i := 0; i < 2; i++ {
		cfgExpected := atomicReplaceKubeConfig(t, kubeconfigPath)

		cfgActual := testutil.Recv(t, sub.Chan(),
			fmt.Sprintf("reload %d (a file watch that detached after the first atomic write never delivers it)", i+1))
		compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
		assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
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
	w.Start()

	sub := w.Subscribe()
	defer sub.Close()

	// Drain the empty seed so the next Chan read is the post-create reload.
	testutil.Recv(t, sub.Chan(), "the initial seeded value")

	cfgExpected, err := createKubeConfig(kubeconfigPath)
	require.NoError(t, err)

	cfgActual := testutil.Recv(t, sub.Chan(), "the reload published after the kubeconfig was created")
	compareMaps(t, cfgExpected.Clusters, cfgActual.Clusters)
	compareMaps(t, cfgExpected.AuthInfos, cfgActual.AuthInfos)
	compareMaps(t, cfgExpected.Contexts, cfgActual.Contexts)
	assert.Equal(t, cfgExpected.CurrentContext, cfgActual.CurrentContext)
}
