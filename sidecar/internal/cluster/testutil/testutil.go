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

// Package testutil provides shared test helpers for the controllers sub-packages.
package testutil

import (
	"context"
	"testing"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// StaticWatcher is a KubeConfigSource pinned to one config: Get always returns
// it, Subscribe delivers it once (current-on-subscribe) then closes.
type StaticWatcher struct {
	cfg *api.Config
}

// NewStaticWatcher creates a KubeConfigSource that delivers cfg as its current
// snapshot. cfg may be nil (an empty config is delivered then).
func NewStaticWatcher(t *testing.T, cfg *api.Config) cluster.KubeConfigSource {
	t.Helper()
	if cfg == nil {
		cfg = &api.Config{Contexts: map[string]*api.Context{}}
	}
	return &StaticWatcher{cfg: cfg}
}

func (s *StaticWatcher) Get() *api.Config { return s.cfg }

func (s *StaticWatcher) Subscribe() k8shelpers.KubeConfigSubscription {
	return watch.New(s.cfg).Receiver()
}

// OpenMemoryStore opens a fresh in-memory SQLite store for testing.
func OpenMemoryStore() (beehive.Store, error) {
	return sqlite.OpenMemory()
}

// TestKubeConfig builds an in-memory kubeconfig resolvable for contextName
// without touching the network. The cluster entry is "<ctx>-cluster", user is
// "<ctx>-user", server is always https://127.0.0.1:6443.
func TestKubeConfig(contextName string) *api.Config {
	return &api.Config{
		CurrentContext: contextName,
		Contexts: map[string]*api.Context{
			contextName: {Cluster: contextName + "-cluster", AuthInfo: contextName + "-user"},
		},
		Clusters: map[string]*api.Cluster{
			contextName + "-cluster": {Server: "https://127.0.0.1:6443"},
		},
		AuthInfos: map[string]*api.AuthInfo{
			contextName + "-user": {Token: "test-token"},
		},
	}
}

// NewTestBeehiveUnstarted builds a beehive with no controllers registered and
// not yet started. Callers register controllers before calling bh.Start().
func NewTestBeehiveUnstarted(t *testing.T) *beehive.Beehive {
	t.Helper()
	st, err := OpenMemoryStore()
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bh, err := beehive.New(st, beehive.WithResyncInterval(0))
	require.NoError(t, err)
	return bh
}

// NoopController satisfies beehive.Controller without doing anything.
type NoopController[Spec, Status any] struct {
	Client beehive.ControllerClient[Status]
}

func (c *NoopController[Spec, Status]) Start(cl beehive.ControllerClient[Status]) error {
	c.Client = cl
	return nil
}
func (c *NoopController[Spec, Status]) Stop(_ context.Context) error { return nil }
func (c *NoopController[Spec, Status]) Reconcile(_ context.Context, _ *beehive.Object[Spec, Status]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// NewTestBeehive builds a beehive with both controller kinds registered as
// no-ops and starts it. Use for tests that only exercise the importer or need a
// fully-running beehive but don't care about controller behaviour.
func NewTestBeehive(t *testing.T) *beehive.Beehive {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)
	require.NoError(t, beehive.Register(bh, cluster.ClusterGroupKind, &NoopController[cluster.ClusterSpec, cluster.ClusterConnectionStatus]{}))
	require.NoError(t, beehive.Register(bh, cluster.ClusterCacheGroupKind, &NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{}))
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })
	return bh
}
