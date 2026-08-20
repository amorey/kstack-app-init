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

// Fixtures shared across this package's test files. Anything used by one file lives
// beside it instead.
package clustersvc

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// newTestBeehive returns a beehive over an in-memory store, closed on cleanup. The
// bootstrap New does, minus the disk. opts let a test shrink a cadence it would
// otherwise have to outwait.
func newTestBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	store, err := beehivesqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	bh, err := beehive.New(store, opts...)
	require.NoError(t, err)
	return bh
}

// newTestDeps returns the shared set over a test beehive — the same newDeps the
// composition root calls, so a test never assembles its own. The controllers are
// registered so a requeue reaches a real reconciler, but beehive is never run, so
// nothing reconciles or collects behind these tests.
//
// One store for every kind, which the owner edges need: beehive refuses an owner in
// another store.
func newTestDeps(t *testing.T) deps {
	t.Helper()
	d, _ := newTestDepsAndBeehive(t)
	return d
}

// fakeKubeidentity answers per context from a map, and records who was asked for. The
// zero value has nothing to say about anything, which is the state before a probe has
// answered — what a cluster pass finds unless a test says otherwise.
type fakeKubeidentity struct {
	states map[string]identityAnswer
	asked  []string
}

// identityAnswer pairs what the service knows with whether it knows anything, so a test
// can hand back "asked, unanswered" as easily as an answer.
type identityAnswer struct {
	state kubeidentity.State
	known bool
}

func (f *fakeKubeidentity) Get(contextName string) (kubeidentity.State, bool) {
	f.asked = append(f.asked, contextName)
	s := f.states[contextName]
	return s.state, s.known
}

func (f *fakeKubeidentity) Subscribe() kubeidentity.Subscription {
	panic("a cluster pass reads, it does not subscribe")
}

// answering is a service that answers for "prod" with an identity and an error.
func answering(id kubeidentity.Identity, err error) *fakeKubeidentity {
	return &fakeKubeidentity{states: map[string]identityAnswer{
		"prod": {state: kubeidentity.State{Identity: id, Err: err}, known: true},
	}}
}

// newTestDepsAndBeehive is newTestDeps plus the beehive behind it, for a test that has
// to write what only a pass can — see newClusterStatusDeps.
func newTestDepsAndBeehive(t *testing.T) (deps, *beehive.Beehive) {
	t.Helper()
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), &fakeKubeidentity{}, nil)

	_, err := registerControllers(bh, d)
	require.NoError(t, err)

	// The anchors the real service creates at startup: clusterController.Reconcile
	// reads one to declare its dependency edge.
	require.NoError(t, ensureClusterSources(context.Background(), d.sourceClient))
	return d, bh
}

// newClusterStatusDeps returns the shared set plus the client that stores a Cluster
// status from outside a pass, standing in for the connection probe: a fixture needs a
// stored status because the reconciles under test re-read it. Beehive is never run, so
// nothing reconciles behind these tests — which is also the only state an admin write
// is safe in.
func newClusterStatusDeps(t *testing.T) (deps, *beehive.AdminClient[ClusterStatus]) {
	t.Helper()
	bh := newTestBeehive(t)
	return newDeps(bh, newTestKubeconfig(t), &fakeKubeidentity{}, nil), beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
}

// newTestKubeconfig returns a started kubeconfig service over an empty temp dir, so
// it has read and every context is absent. Started because a reconcile defers until
// the first read, which Start does synchronously.
func newTestKubeconfig(t *testing.T) *kubeconfig.Service {
	t.Helper()
	svc := kubeconfig.New(filepath.Join(t.TempDir(), "config"), nil)

	stop, err := svc.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, svc.Close())
	})
	return svc
}

// newRunningBeehive is newTestBeehive started, so a watch's tail is live. No
// controller is registered: a reconcile writing status would put frames on a watch
// that the test never asked for.
func newRunningBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	bh := newTestBeehive(t, opts...)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return bh
}

// newRunningDeps is the shared set over a running beehive, for the watch tests:
// without one the tail is dead and no change is ever reported. Nothing reconciles, so
// every frame is the test's own doing.
func newRunningDeps(t *testing.T, opts ...beehive.Option) deps {
	t.Helper()
	return newDeps(newRunningBeehive(t, opts...), newTestKubeconfig(t), &fakeKubeidentity{}, nil)
}

// fakeKubeconfigSource is a hub the test publishes into, standing in for the
// watcher: same current-on-subscribe contract, driven by hand.
type fakeKubeconfigSource struct{ hub *watch.Hub[*api.Config] }

func newFakeKubeconfigSource(initial *api.Config) *fakeKubeconfigSource {
	return &fakeKubeconfigSource{hub: watch.New(initial)}
}

func (f *fakeKubeconfigSource) Subscribe() kubeconfig.Subscription { return f.hub.Receiver() }

// publish pushes a new snapshot to every subscriber.
func (f *fakeKubeconfigSource) publish(cfg *api.Config) { f.hub.Sender().Send(cfg) }

// close ends every subscription, standing in for the watcher shutting down.
func (f *fakeKubeconfigSource) close() { f.hub.Close() }

// cfgWith builds a snapshot holding one context per name, each naming its own cluster
// and user entry.
func cfgWith(names ...string) *api.Config {
	cfg := &api.Config{Contexts: map[string]*api.Context{}}
	for _, n := range names {
		cfg.Contexts[n] = &api.Context{Cluster: n + "-cluster", AuthInfo: n + "-user"}
	}
	return cfg
}

// cfgCurrent is cfgWith with one of the contexts marked current.
func cfgCurrent(current string, names ...string) *api.Config {
	cfg := cfgWith(names...)
	cfg.CurrentContext = current
	return cfg
}
