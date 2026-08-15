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

package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// clusterObj builds a Cluster object whose probed UID is uid, or one that has never
// been probed when uid is "".
func clusterObj(uid string) *beehive.Object[ClusterSpec, ClusterStatus] {
	obj := &beehive.Object[ClusterSpec, ClusterStatus]{Status: &ClusterStatus{}}
	if uid != "" {
		obj.Status.Server.UID = &uid
	}
	return obj
}

func TestClusterActiveUID(t *testing.T) {
	assert.Equal(t, "uid-1", ClusterActiveUID(clusterObj("uid-1")))
	assert.Empty(t, ClusterActiveUID(clusterObj("")), "probed, but no UID yet")
	assert.Empty(t, ClusterActiveUID(&beehive.Object[ClusterSpec, ClusterStatus]{}),
		"beehive leaves Status nil until first written")
}

// The single definition of "active cache", read by both the cache controller's sync
// gate and the service's join. The unprobed case is the one that matters: an
// unknown identity must match nothing, or a disconnected cluster would sync every
// cache it has ever owned.
func TestCacheIsActive(t *testing.T) {
	assert.True(t, CacheIsActive(clusterObj("uid-1"), "uid-1"))
	assert.False(t, CacheIsActive(clusterObj("uid-1"), "uid-2"), "a superseded identity")
	assert.False(t, CacheIsActive(clusterObj(""), ""), "unknown identity matches nothing — not even an empty UID")
	assert.False(t, CacheIsActive(clusterObj(""), "uid-1"))
}

// The beehive name is a per-source uniqueness key, so the prefix is what keeps a
// future source from colliding with a kube-context of the same name.
func TestKubeconfigName(t *testing.T) {
	assert.Equal(t, "kubeconfig/prod", KubeconfigName("prod"))
	assert.Equal(t, "kubeconfig/", KubeconfigName(""))
}

// --- kubeconfigImporter ---

// testRetryInterval paces the importer's retry in tests, shrunk from
// importRetryInterval through the build seam so nothing here encodes it.
const testRetryInterval = 5 * time.Millisecond

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

// cfgWith builds a snapshot holding one context per name.
func cfgWith(names ...string) *api.Config {
	cfg := &api.Config{Contexts: map[string]*api.Context{}}
	for _, n := range names {
		cfg.Contexts[n] = &api.Context{}
	}
	return cfg
}

// startImporter wires an importer onto src with the reconcile step replaced by a
// probe, and joins it on cleanup. Every reconcile's snapshot lands on the probe;
// results, consumed in order, decide what each call returns.
func startImporter(t *testing.T, src *fakeKubeconfigSource, results ...error) (*kubeconfigImporter, func(context.Context) error, *testutil.Probe[*api.Config]) {
	t.Helper()
	var calls int
	return startImporterWith(t, src, func(*api.Config) error {
		defer func() { calls++ }()
		if calls < len(results) {
			return results[calls]
		}
		return nil
	})
}

// startImporterWith is startImporter with the import step supplied whole, for a test
// that has to act from inside one.
func startImporterWith(t *testing.T, src *fakeKubeconfigSource, reconcile func(cfg *api.Config) error) (*kubeconfigImporter, func(context.Context) error, *testutil.Probe[*api.Config]) {
	t.Helper()
	seen := testutil.NewProbe[*api.Config](16)

	im := newKubeconfigImporter(src, nil)
	im.retryInterval = testRetryInterval
	im.reconcile = func(_ context.Context, cfg *api.Config) error {
		seen.Fire(cfg)
		return reconcile(cfg)
	}

	stop, err := im.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return im, stop, seen
}

// The importer subscribes current-on-subscribe, so whatever the kubeconfig already
// held is imported at startup rather than waiting for the user to edit the file.
func TestImporterReconcilesTheStartupSnapshot(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, _, seen := startImporter(t, src)

	cfg := seen.Await(t, "startup reconcile")
	assert.Contains(t, cfg.Contexts, "prod")
}

func TestImporterReconcilesEachSnapshot(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, _, seen := startImporter(t, src)
	seen.Await(t, "startup reconcile")

	src.publish(cfgWith("prod", "staging"))

	cfg := seen.Await(t, "reconcile after change")
	assert.Contains(t, cfg.Contexts, "staging")
}

// A failed import must be retried against the SAME snapshot: the loop is driven by
// kubeconfig changes, and the usual cause (a name held by a draining Cluster) clears
// with no write behind it, so "the next snapshot fixes it" can mean never.
func TestImporterRetriesTheSameSnapshotAfterFailure(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, _, seen := startImporter(t, src, fmt.Errorf("%w: prod", errNameHeld))

	first := seen.Await(t, "first attempt")
	second := seen.Await(t, "retry")
	assert.Same(t, first, second, "the retry must re-import the snapshot that failed")
}

// assertNoFurtherReconcile is a negative assertion, so there is nothing to wait for:
// the window spans several shrunk retry intervals, and it fails the moment another
// attempt arrives rather than at the deadline.
func assertNoFurtherReconcile(t *testing.T, seen *testutil.Probe[*api.Config]) {
	t.Helper()
	select {
	case cfg := <-seen.Chan():
		t.Fatalf("reconciled again after success: %v", cfg)
	case <-time.After(10 * testRetryInterval):
	}
}

// The retry is armed by failure alone. Without this the loop would re-import on a
// timer forever, re-listing every Cluster each tick for a kubeconfig nobody touched.
func TestImporterDoesNotRetryAfterSuccess(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, _, seen := startImporter(t, src)
	seen.Await(t, "startup reconcile")

	assertNoFurtherReconcile(t, seen)
}

// A success must disarm the retry an earlier failure armed, not just decline to
// re-arm it: the snapshot branch and the retry branch run the same attempt, so a
// newer snapshot importing cleanly leaves a timer pointed at work already done.
func TestImporterDisarmsTheRetryAfterALaterSuccess(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))

	var calls int
	_, _, seen := startImporterWith(t, src, func(*api.Config) error {
		calls++
		if calls > 1 {
			return nil
		}
		// Published from inside the failing import, so the newer snapshot is already
		// queued by the time the retry is armed. The loop then takes the snapshot
		// branch with a whole retry interval of head start rather than racing it.
		src.publish(cfgWith("prod", "staging"))
		return errors.New("boom")
	})

	seen.Await(t, "first attempt, which fails and arms the retry")
	seen.Await(t, "the newer snapshot, which succeeds")

	assertNoFurtherReconcile(t, seen)
}

// An error keeps retrying until one attempt succeeds, then stops.
func TestImporterRetriesUntilSuccess(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, _, seen := startImporter(t, src, errors.New("boom"), errors.New("boom"))

	seen.Await(t, "first attempt")
	seen.Await(t, "second attempt")
	seen.Await(t, "third attempt, which succeeds")

	assertNoFurtherReconcile(t, seen)
}

// The watcher closing is an ordinary shutdown: the loop ends rather than spinning on
// a closed channel.
func TestImporterStopsWhenTheSourceCloses(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	im, _, seen := startImporter(t, src)
	seen.Await(t, "startup reconcile")

	src.close()

	// Join the loop directly rather than through the stop func, which also cancels
	// the base context — either exit would then satisfy the assertion. Waiting on the
	// WaitGroup alone leaves the closed channel as the only way out.
	testutil.WaitReturn(t, im.wg.Wait, "import loop to end when the source closed")
}

// The stop func must join the loop even mid-wait, so service.Close never races an
// import.
func TestImporterStopJoinsTheLoop(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, stop, seen := startImporter(t, src)
	seen.Await(t, "startup reconcile")

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")
}

// --- clusterController ---

// The controller owns the watcher and importer, so its stop closure is what takes
// them both down; a leaked one would outlive the service.
func TestClusterControllerStartStop(t *testing.T) {
	c := newClusterController(t.TempDir()+"/config", nil)

	stop, err := c.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "controller stop to return")

	assert.NoError(t, c.Close())
}
