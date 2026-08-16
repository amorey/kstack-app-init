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

// testRetryInterval paces the importer's retry in tests, shrunk from importRetryBase
// through the build seam so nothing here encodes it.
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

// startImporter wires an importer onto src with the reconcile step replaced by a
// probe, and joins it on cleanup. Every reconcile's snapshot lands on the probe;
// results, consumed in order, decide what each call returns.
func startImporter(t *testing.T, src *fakeKubeconfigSource, results ...error) (*kubeconfigImporter, func(context.Context) error, *testutil.Probe[*api.Config]) {
	t.Helper()
	var calls int
	im, seen := newTestImporter(src, func(context.Context, *api.Config) error {
		defer func() { calls++ }()
		if calls < len(results) {
			return results[calls]
		}
		return nil
	})
	return im, startTestImporter(t, im), seen
}

// newTestImporter builds an unstarted importer whose import step reports every
// snapshot it sees on the probe. The step is handed the loop's own context. Only the
// retry is shrunk: a production-cadence backstop tick cannot fire under a test that is
// pacing the retry.
func newTestImporter(src *fakeKubeconfigSource, reconcile func(ctx context.Context, cfg *api.Config) error) (*kubeconfigImporter, *testutil.Probe[*api.Config]) {
	seen := testutil.NewProbe[*api.Config](16)

	im := newKubeconfigImporter(src, nil)
	im.retryBase = testRetryInterval
	im.retryMax = testRetryInterval
	im.sync = func(ctx context.Context, cfg *api.Config) error {
		seen.Fire(cfg)
		return reconcile(ctx, cfg)
	}
	return im, seen
}

func startTestImporter(t *testing.T, im *kubeconfigImporter) func(context.Context) error {
	t.Helper()
	stop, err := im.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return stop
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
	_, _, seen := startImporter(t, src, errors.New("boom"))

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
		t.Fatalf("reconciled again: %v", cfg)
	case <-time.After(10 * testRetryInterval):
	}
}

// The retry is armed by failure alone. Without this the loop would re-run on a timer
// forever, re-listing every Cluster for a kubeconfig nobody touched.
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
	im, seen := newTestImporter(src, func(context.Context, *api.Config) error {
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
	startTestImporter(t, im)

	seen.Await(t, "first attempt, which fails and arms the retry")
	seen.Await(t, "the newer snapshot, which succeeds")

	assertNoFurtherReconcile(t, seen)
}

// A failure keeps retrying until one attempt succeeds, then stops.
func TestImporterRetriesUntilSuccess(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	boom := errors.New("boom")
	_, _, seen := startImporter(t, src, boom, boom)

	seen.Await(t, "first attempt")
	seen.Await(t, "second attempt")
	seen.Await(t, "third attempt, which succeeds")

	assertNoFurtherReconcile(t, seen)
}

// Shutdown mid-import is not an import failure. The store call is cancelled along
// with the loop, so its error describes the stop, and reporting it would put an ERROR
// in the log on every clean shutdown.
func TestImporterTreatsShutdownAsQuiet(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	im, seen := newTestImporter(src, func(ctx context.Context, _ *api.Config) error {
		<-ctx.Done()
		return ctx.Err()
	})
	stop := startTestImporter(t, im)
	seen.Await(t, "startup reconcile, which then blocks")

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) },
		"stop to return with an import in flight")
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

// The ladder itself, asserted at production scale: the tests above shrink both knobs
// to the same value, so the loop that drives it never leaves the base.
func TestNextRetryDelay(t *testing.T) {
	assert.Equal(t, 4*time.Second, nextRetryDelay(2*time.Second, time.Minute), "doubles")
	assert.Equal(t, time.Minute, nextRetryDelay(40*time.Second, time.Minute), "clamped at the cap")
	assert.Equal(t, time.Minute, nextRetryDelay(time.Minute, time.Minute), "held at the cap")
	assert.Equal(t, importRetryMax, nextRetryDelay(importRetryMax, importRetryMax),
		"the production pair re-levels rather than growing without bound")
}

// --- syncClusterSet ---

// newTestClusterClient returns a Cluster client over a test beehive. The controllers
// are registered so a requeue reaches a real reconciler, but beehive is never run, so
// nothing reconciles or collects behind these tests.
func newTestClusterClient(t *testing.T) beehive.Client[ClusterSpec, ClusterStatus] {
	t.Helper()
	bh := newTestBeehive(t)
	_, err := registerControllers(bh, newClusterController(t.TempDir()+"/config", nil, nil))
	require.NoError(t, err)
	return beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
}

// importerOver returns an importer writing through client, with no kubeconfig source:
// these tests call the import step directly.
func importerOver(client beehive.Client[ClusterSpec, ClusterStatus]) *kubeconfigImporter {
	return newKubeconfigImporter(nil, client)
}

// liveClusters returns the live records keyed by the context each references.
func liveClusters(t *testing.T, client beehive.Client[ClusterSpec, ClusterStatus]) map[string]*beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	objs, err := client.List(context.Background())
	require.NoError(t, err)

	byContext := map[string]*beehive.Object[ClusterSpec, ClusterStatus]{}
	for _, obj := range objs {
		if src := obj.Spec.Source.Kubeconfig; src != nil && obj.DeletionRequestedAt == nil {
			byContext[src.Context] = obj
		}
	}
	return byContext
}

func TestImportCreatesOneClusterPerContext(t *testing.T) {
	client := newTestClusterClient(t)
	require.NoError(t, importerOver(client).syncClusterSet(context.Background(), cfgWith("prod", "staging")))

	live := liveClusters(t, client)
	require.Len(t, live, 2)
	assert.Contains(t, live, "staging")

	prod := live["prod"]
	assert.Equal(t, KubeconfigName("prod"), prod.Name)
	assert.True(t, prod.Spec.Enabled, "an imported context must be usable without being switched on")
	assert.True(t, prod.Spec.SyncEnabled)
}

// The pass runs on every snapshot and every backstop tick, so re-importing an
// unchanged kubeconfig has to be a no-op down to the record ids: a new id would
// orphan whatever the old one owned.
func TestImportIsIdempotent(t *testing.T) {
	client := newTestClusterClient(t)
	im := importerOver(client)
	cfg := cfgWith("prod")

	require.NoError(t, im.syncClusterSet(context.Background(), cfg))
	first := liveClusters(t, client)["prod"]
	require.NoError(t, im.syncClusterSet(context.Background(), cfg))

	second := liveClusters(t, client)["prod"]
	require.NotNil(t, second)
	assert.Equal(t, first.ID, second.ID, "the second pass must not create a second record")
}

// The referenced set is scoped by the source discriminant, not by the name prefix: a
// record from another source names its own things and claims no kube-context.
func TestImportIgnoresRecordsFromAnotherSource(t *testing.T) {
	client := newTestClusterClient(t)
	ctx := context.Background()
	_, err := client.Create(ctx, "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	require.NoError(t, importerOver(client).syncClusterSet(ctx, cfgWith("prod")))
	assert.Contains(t, liveClusters(t, client), "prod")
}

// The pass wakes the records that predate it, which is how a departed context — absent
// from the snapshot the create loop walks — is ever observed absent. Records created by
// this same pass are left out: beehive already owes them a first reconcile.
func TestSyncWakesTheRecordsItDidNotCreate(t *testing.T) {
	ctx := context.Background()

	var woke []beehive.ObjectID
	stub := stubClusterClient{
		objs:    []*beehive.Object[ClusterSpec, ClusterStatus]{{ID: 7}, {ID: 8}},
		create:  func(string) error { return nil },
		requeue: func(id beehive.ObjectID) error { woke = append(woke, id); return nil },
	}

	require.NoError(t, importerOver(stub).syncClusterSet(ctx, cfgWith("prod")))
	assert.Equal(t, []beehive.ObjectID{7, 8}, woke)
}

// A record already collecting owes no observation, and beehive is running its teardown
// rather than waiting to hear about the kubeconfig.
func TestSyncDoesNotWakeADeletingRecord(t *testing.T) {
	deleting := &beehive.Object[ClusterSpec, ClusterStatus]{ID: 7}
	now := time.Now()
	deleting.DeletionRequestedAt = &now

	var woke []beehive.ObjectID
	stub := stubClusterClient{
		objs:    []*beehive.Object[ClusterSpec, ClusterStatus]{deleting},
		create:  func(string) error { return nil },
		requeue: func(id beehive.ObjectID) error { woke = append(woke, id); return nil },
	}

	require.NoError(t, importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod")))
	assert.Empty(t, woke)
}

// A lost wake is the one failure nothing else re-levels — an unchanged kubeconfig
// republishes nothing, so no later pass is owed — which is why it has to drive the
// retry ladder rather than being logged and dropped.
func TestSyncReportsAFailedWake(t *testing.T) {
	boom := errors.New("boom")
	stub := stubClusterClient{
		objs:    []*beehive.Object[ClusterSpec, ClusterStatus]{{ID: 7}},
		create:  func(string) error { return nil },
		requeue: func(beehive.ObjectID) error { return boom },
	}

	err := importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod"))

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "7", "the error must name the record that stayed stale")
}

// One record's failed wake must not cost the others theirs, for the same reason a
// failed create does not abort the pass.
func TestSyncWakesTheRestPastAFailedWake(t *testing.T) {
	var woke []beehive.ObjectID
	stub := stubClusterClient{
		objs:   []*beehive.Object[ClusterSpec, ClusterStatus]{{ID: 7}, {ID: 8}},
		create: func(string) error { return nil },
		requeue: func(id beehive.ObjectID) error {
			if id == 7 {
				return errors.New("boom")
			}
			woke = append(woke, id)
			return nil
		},
	}

	require.Error(t, importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod")))
	assert.Equal(t, []beehive.ObjectID{8}, woke)
}

// A record collected between the List and the wake owes no observation, and no later
// pass would find it — so retrying the pass over it would never clear.
func TestSyncIgnoresAWakeForACollectedRecord(t *testing.T) {
	stub := stubClusterClient{
		objs:    []*beehive.Object[ClusterSpec, ClusterStatus]{{ID: 7}},
		create:  func(string) error { return nil },
		requeue: func(beehive.ObjectID) error { return beehive.ErrNotFound },
	}

	assert.NoError(t, importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod")))
}

// stubClusterClient answers the calls the import and the requeue pass make, so a test
// can fail any of them. The embedded interface is nil: anything else they might start
// calling panics rather than passing silently.
type stubClusterClient struct {
	beehive.Client[ClusterSpec, ClusterStatus]
	objs    []*beehive.Object[ClusterSpec, ClusterStatus]
	listErr error
	create  func(name string) error
	requeue func(id beehive.ObjectID) error
}

func (c stubClusterClient) List(context.Context, ...beehive.LoadOption) ([]*beehive.Object[ClusterSpec, ClusterStatus], error) {
	return c.objs, c.listErr
}

func (c stubClusterClient) Requeue(_ context.Context, id beehive.ObjectID, _ ...beehive.RequeueOption) error {
	return c.requeue(id)
}

func (c stubClusterClient) Create(_ context.Context, name string, _ ClusterSpec, _ ...beehive.Option) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	return nil, c.create(name)
}

// A store read that fails must abort the pass. Carrying on would import against a
// record set that was never read, and every context in the file would look
// unclaimed.
func TestImportReportsAFailedList(t *testing.T) {
	boom := errors.New("boom")
	err := importerOver(stubClusterClient{listErr: boom}).syncClusterSet(context.Background(), cfgWith("prod"))

	require.ErrorIs(t, err, boom)
}

func TestImportReportsAFailedCreate(t *testing.T) {
	boom := errors.New("boom")
	stub := stubClusterClient{create: func(string) error { return boom }}
	err := importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod"))

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "prod", "the error must name the context that could not be imported")
}

// One context's failure must not cost the others their import. Contexts come out of a
// map, so stopping at the first one would leave a subset that differs run to run.
func TestImportContinuesPastAFailedCreate(t *testing.T) {
	boom := errors.New("boom")
	var created []string
	stub := stubClusterClient{create: func(name string) error {
		if name == KubeconfigName("prod") {
			return boom
		}
		created = append(created, name)
		return nil
	}}

	err := importerOver(stub).syncClusterSet(context.Background(), cfgWith("prod", "staging", "dev"))

	require.ErrorIs(t, err, boom)
	assert.ElementsMatch(t, []string{KubeconfigName("staging"), KubeconfigName("dev")}, created)
}

// --- observeKubeconfig / Reconcile ---

// kubeconfigObj builds a kubeconfig-sourced record for contextName, carrying status
// if one was already observed.
func kubeconfigObj(contextName string, status *ClusterStatus) *beehive.Object[ClusterSpec, ClusterStatus] {
	return &beehive.Object[ClusterSpec, ClusterStatus]{
		ID:     1,
		Name:   KubeconfigName(contextName),
		Spec:   ClusterSpec{Source: ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName}}},
		Status: status,
	}
}

// kubeconfigSrc is the spec-side source reference for contextName.
func kubeconfigSrc(contextName string) *ClusterSpecSourceKubeconfig {
	return &ClusterSpecSourceKubeconfig{Context: contextName}
}

func TestObserveKubeconfigRecordsAPresentContext(t *testing.T) {
	observed := observeKubeconfig(cfgCurrent("prod", "prod", "staging"), kubeconfigSrc("prod"), nil)

	require.NotNil(t, observed)
	assert.Equal(t, ClusterStatusSourceKubeconfig{
		Cluster: "prod-cluster", User: "prod-user", IsPresent: true, IsDefault: true,
	}, *observed)
}

func TestObserveKubeconfigMarksANonCurrentContext(t *testing.T) {
	observed := observeKubeconfig(cfgCurrent("prod", "prod", "staging"), kubeconfigSrc("staging"), nil)

	require.NotNil(t, observed)
	assert.False(t, observed.IsDefault)
	assert.True(t, observed.IsPresent)
}

// A departed context keeps its last-known names: an orphaned record has to stay
// identifiable in a list, and blanking it would leave the row nameless.
func TestObserveKubeconfigKeepsLastKnownNamesWhenAbsent(t *testing.T) {
	prev := &ClusterStatusSourceKubeconfig{
		Cluster: "prod-cluster", User: "prod-user", IsPresent: true, IsDefault: true,
	}

	observed := observeKubeconfig(cfgCurrent("staging", "staging"), kubeconfigSrc("prod"), prev)

	require.NotNil(t, observed)
	assert.Equal(t, ClusterStatusSourceKubeconfig{Cluster: "prod-cluster", User: "prod-user"}, *observed)
	assert.True(t, prev.IsPresent, "the previous observation is the caller's, not this fold's to clear")
}

// A never-present context has nothing to keep, and must not read as present.
func TestObserveKubeconfigMarksAnUnseenContextAbsent(t *testing.T) {
	observed := observeKubeconfig(cfgCurrent("staging", "staging"), kubeconfigSrc("prod"), nil)

	require.NotNil(t, observed)
	assert.False(t, observed.IsPresent)
	assert.Empty(t, observed.Cluster)
}

// Another source's record is not this observation's to write — its own variant is,
// and overwriting would claim a kube-context it never referenced.
func TestObserveKubeconfigLeavesAnotherSourceAlone(t *testing.T) {
	assert.Nil(t, observeKubeconfig(cfgCurrent("prod", "prod"), nil, nil))
}

// stubControllerClient captures what a reconcile reports — a status write, or the
// generation alone. The embedded interface is nil: Reconcile calls nothing else on it.
type stubControllerClient struct {
	beehive.ControllerClient[ClusterStatus]
	updated     *ClusterStatus
	updateErr   error
	observed    *int64
	observedErr error
}

func (c *stubControllerClient) UpdateStatus(_ context.Context, _ beehive.ObjectID, _ int64, status ClusterStatus) error {
	c.updated = &status
	return c.updateErr
}

func (c *stubControllerClient) SetObservedGeneration(_ context.Context, _ beehive.ObjectID, generation int64) error {
	c.observed = &generation
	return c.observedErr
}

// controllerOver returns a controller over an empty kubeconfig, so every context is
// absent. The watcher is started because Reconcile defers until it has read, and its
// first read is synchronous in Start.
func controllerOver(t *testing.T) *clusterController {
	t.Helper()
	c := newClusterController(t.TempDir()+"/config", nil, nil)

	stop, err := c.kubeconfigWatcher.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, c.kubeconfigWatcher.Close())
	})
	return c
}

// beehive starts ahead of the controllers, so an owed pass can reach a record before
// the watcher's first read. Observing the pre-read config would report a present
// context absent and wake the kind's watches for a flap.
func TestReconcileDefersUntilTheKubeconfigIsRead(t *testing.T) {
	c := newClusterController(t.TempDir()+"/config", nil, nil)

	client := &stubControllerClient{}
	res, err := c.Reconcile(context.Background(), client, kubeconfigObj("prod", nil))

	require.NoError(t, err)
	assert.Nil(t, client.updated, "nothing observed, so nothing to write")
	assert.Nil(t, client.observed, "and nothing settled: the record still owes an observation")
	assert.Positive(t, res.RequeueAfter, "it has to be revisited once the watcher has read")
}

func TestReconcileWritesTheObservation(t *testing.T) {
	client := &stubControllerClient{}

	res, err := controllerOver(t).Reconcile(context.Background(), client, kubeconfigObj("prod", nil))

	require.NoError(t, err)
	assert.Equal(t, beehive.Result{}, res)
	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Source.Kubeconfig)
	assert.False(t, client.updated.Source.Kubeconfig.IsPresent, "the seed config holds no contexts")
}

// The observation is written into the status already there, not over it: everything
// else on the blob belongs to a probe this reconcile did not run.
func TestReconcileKeepsTheRestOfTheStatus(t *testing.T) {
	uid := "uid-1"
	obj := kubeconfigObj("prod", &ClusterStatus{Server: ClusterServer{UID: &uid}})

	client := &stubControllerClient{}
	_, err := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Server.UID)
	assert.Equal(t, uid, *client.updated.Server.UID)
}

// A failed status write is the reconcile's failure, so beehive retries it.
func TestReconcileReportsAFailedStatusWrite(t *testing.T) {
	boom := errors.New("boom")

	_, err := controllerOver(t).Reconcile(context.Background(), &stubControllerClient{updateErr: boom}, kubeconfigObj("prod", nil))

	assert.ErrorIs(t, err, boom)
}

// A spec write this observation does not depend on still bumps the generation, and an
// object left unsettled is re-dispatched by beehive's owed pass forever — so the
// generation has to be recorded even when there is no status to write.
func TestReconcileRecordsTheGenerationWhenNothingMoved(t *testing.T) {
	// The seed config holds no contexts, so this is what the reconcile will observe.
	obj := kubeconfigObj("prod", &ClusterStatus{Source: ClusterStatusSource{
		Kubeconfig: &ClusterStatusSourceKubeconfig{Cluster: "prod-cluster", User: "prod-user"},
	}})
	obj.Generation = 3

	client := &stubControllerClient{}
	_, err := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	assert.Nil(t, client.updated, "an unchanged observation must not write status")
	require.NotNil(t, client.observed)
	assert.Equal(t, int64(3), *client.observed, "the generation reported is the one handed in")
}

// A record from another source has no observation of its own, and two of those are as
// unchanged as any other repeat.
func TestReconcileRecordsTheGenerationForAnotherSource(t *testing.T) {
	// beehive's generations start at 1; 0 is not a generation any object is handed.
	obj := &beehive.Object[ClusterSpec, ClusterStatus]{ID: 1, Generation: 1, Status: &ClusterStatus{}}

	client := &stubControllerClient{}
	_, err := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	assert.Nil(t, client.updated)
	assert.NotNil(t, client.observed)
}

// The steady state: the importer's wake reaches a record whose generation is already
// settled and whose observation did not move. beehive would clamp the report to a
// no-op, but only after a transaction and a row read per record per snapshot.
func TestReconcileWritesNothingWhenAlreadySettled(t *testing.T) {
	obj := kubeconfigObj("prod", &ClusterStatus{Source: ClusterStatusSource{
		Kubeconfig: &ClusterStatusSourceKubeconfig{Cluster: "prod-cluster", User: "prod-user"},
	}})
	obj.Generation = 3
	settled := int64(3)
	obj.ObservedGeneration = &settled

	client := &stubControllerClient{}
	_, err := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	assert.Nil(t, client.updated)
	assert.Nil(t, client.observed, "a settled generation is nothing left to report")
}

// The generation write is the reconcile's report, so failing it fails the reconcile
// exactly as a failed status write does.
func TestReconcileReportsAFailedGenerationWrite(t *testing.T) {
	boom := errors.New("boom")
	obj := kubeconfigObj("prod", &ClusterStatus{Source: ClusterStatusSource{
		Kubeconfig: &ClusterStatusSourceKubeconfig{Cluster: "prod-cluster", User: "prod-user"},
	}})

	_, err := controllerOver(t).Reconcile(context.Background(), &stubControllerClient{observedErr: boom}, obj)

	assert.ErrorIs(t, err, boom)
}

// A record on its way out has nothing to observe, and writing status to it would be a
// write beehive has to carry through a collect that is already under way.
func TestReconcileSkipsADeletingRecord(t *testing.T) {
	obj := kubeconfigObj("prod", nil)
	now := time.Now()
	obj.DeletionRequestedAt = &now

	client := &stubControllerClient{}
	_, err := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NoError(t, err)
	assert.Nil(t, client.updated)
}

// --- clusterController ---

// The controller owns the watcher and importer, so its stop closure is what takes
// them both down; a leaked one would outlive the service.
func TestClusterControllerStartStop(t *testing.T) {
	// A real client, not nil: the importer runs its first import as soon as it starts.
	c := newClusterController(t.TempDir()+"/config", nil, newTestClusterClient(t))

	stop, err := c.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "controller stop to return")

	assert.NoError(t, c.Close())
}
