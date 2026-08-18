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
	"sync"
	"testing"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
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
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), nil, nil)

	_, err := registerControllers(bh, d)
	require.NoError(t, err)

	// The anchors the real service creates at startup: clusterController.Reconcile
	// reads one to declare its dependency edge.
	require.NoError(t, ensureClusterSources(context.Background(), d.sourceClient))
	return d
}

// newClusterStatusDeps returns the shared set plus a writer for the Cluster kind's
// status. A status write is reachable only from inside a reconcile, so the writer is
// the registered controller for that kind — and the only one, so nothing else
// reconciles behind these tests.
func newClusterStatusDeps(t *testing.T) (deps, *clusterStatusWriter) {
	t.Helper()
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), nil, nil)

	w := &clusterStatusWriter{
		clusters: d.clusterClient,
		pending:  map[beehive.ObjectID]ClusterStatus{},
		wrote:    testutil.NewProbe[beehive.ObjectID](4),
	}
	require.NoError(t, beehive.Register(bh, ClusterGroupKind, w))

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return d, w
}

// clusterStatusWriter is a Cluster controller that writes the status a test parks for
// an object, standing in for the connection probe. A fixture needs a stored status —
// the reconciles under test re-read it — and beehive admits a status write from
// nowhere else.
type clusterStatusWriter struct {
	clusters beehive.Client[ClusterSpec, ClusterStatus]
	wrote    *testutil.Probe[beehive.ObjectID]

	mu      sync.Mutex
	pending map[beehive.ObjectID]ClusterStatus
}

func (w *clusterStatusWriter) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterStatus],
	obj *beehive.Object[ClusterSpec, ClusterStatus],
) beehive.ReconcileResult {
	w.mu.Lock()
	status, ok := w.pending[obj.ID]
	w.mu.Unlock()
	if !ok {
		return beehive.Settled(0)
	}
	if err := client.UpdateStatus(ctx, obj.ID, status); err != nil {
		return beehive.Fail(err)
	}
	w.wrote.Fire(obj.ID)
	return beehive.Settled(0)
}

// Set parks status for id and returns once the pass that wrote it has run. The drain
// is what makes a second Set wait for its own pass rather than an earlier one's.
func (w *clusterStatusWriter) Set(t *testing.T, id beehive.ObjectID, status ClusterStatus) {
	t.Helper()
	w.mu.Lock()
	w.pending[id] = status
	w.mu.Unlock()

	w.wrote.Drain()
	require.NoError(t, w.clusters.Requeue(context.Background(), id))
	assert.Equal(t, id, w.wrote.Await(t, "the cluster status write"))
}

// assertFailedWith asserts a pass failed carrying want. A ReconcileResult keeps its
// error unexported, so equality against the Fail the controller built is the only read
// there is — which pins the wrapping the failure carries too.
func assertFailedWith(t *testing.T, want error, res beehive.ReconcileResult) {
	t.Helper()
	assert.Equal(t, beehive.Fail(want), res)
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
	return newDeps(newRunningBeehive(t, opts...), newTestKubeconfig(t), nil, nil)
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
