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
	"sync"
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

// MutableWatcher is a KubeConfigSource whose config can change at runtime via
// Set, which publishes the new snapshot to subscribers — for tests that exercise
// the controller's reaction to kubeconfig changes (departures, renames, etc.).
type MutableWatcher struct {
	mu  sync.RWMutex
	cfg *api.Config
	hub *watch.Hub[*api.Config]
	tx  *watch.Sender[*api.Config]
}

// NewMutableWatcher creates a KubeConfigSource seeded with cfg (nil → empty).
func NewMutableWatcher(cfg *api.Config) *MutableWatcher {
	if cfg == nil {
		cfg = &api.Config{Contexts: map[string]*api.Context{}}
	}
	hub := watch.New(cfg)
	return &MutableWatcher{cfg: cfg, hub: hub, tx: hub.Sender()}
}

func (m *MutableWatcher) Get() *api.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

func (m *MutableWatcher) Subscribe() k8shelpers.KubeConfigSubscription {
	return m.hub.Receiver()
}

// Set swaps the current config and publishes it to subscribers.
func (m *MutableWatcher) Set(cfg *api.Config) {
	if cfg == nil {
		cfg = &api.Config{Contexts: map[string]*api.Context{}}
	}
	m.mu.Lock()
	m.cfg = cfg
	m.mu.Unlock()
	_ = m.tx.Send(cfg)
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
type NoopController[Spec, Status any] struct{}

func (c *NoopController[Spec, Status]) Reconcile(_ context.Context, _ beehive.ControllerClient[Status], _ *beehive.Object[Spec, Status]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// NewTestBeehive builds a beehive with both controller kinds registered as
// no-ops and starts it. Use for tests that only exercise the importer or need a
// fully-running beehive but don't care about controller behaviour.
func NewTestBeehive(t *testing.T) *beehive.Beehive {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)
	_, err := beehive.Register(bh, cluster.ClusterGroupKind, &NoopController[cluster.ClusterSpec, cluster.ClusterStatus]{})
	require.NoError(t, err)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })
	return bh
}
