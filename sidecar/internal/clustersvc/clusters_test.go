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
	"cmp"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
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

// A resync runs the pass again against the snapshot already in hand, which is what
// reaches a context freed with no kubeconfig edit behind it.
func TestImporterResyncRunsAPass(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	im, _, seen := startImporter(t, src)
	seen.Await(t, "startup reconcile")

	im.Resync()

	assert.Contains(t, seen.Await(t, "the resync pass").Contexts, "prod")
}

// A resync before the first snapshot has nothing to run against, and the snapshot is
// on its way regardless.
func TestImporterResyncBeforeTheFirstSnapshotIsQuiet(t *testing.T) {
	im, _ := newTestImporter(newFakeKubeconfigSource(nil), func(context.Context, *api.Config) error { return nil })
	im.Resync()

	startTestImporter(t, im)
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
	d := newTestDeps(t)
	require.NoError(t, importerOver(d.clusterClient).syncClusterSet(context.Background(), cfgWith("prod", "staging")))

	live := liveClusters(t, d.clusterClient)
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
	d := newTestDeps(t)
	im := importerOver(d.clusterClient)
	cfg := cfgWith("prod")

	require.NoError(t, im.syncClusterSet(context.Background(), cfg))
	first := liveClusters(t, d.clusterClient)["prod"]
	require.NoError(t, im.syncClusterSet(context.Background(), cfg))

	second := liveClusters(t, d.clusterClient)["prod"]
	require.NotNil(t, second)
	assert.Equal(t, first.ID, second.ID, "the second pass must not create a second record")
}

// The referenced set is scoped by the source discriminant, not by the name prefix: a
// record from another source names its own things and claims no kube-context.
func TestImportIgnoresRecordsFromAnotherSource(t *testing.T) {
	d := newTestDeps(t)
	ctx := context.Background()
	_, err := d.clusterClient.Create(ctx, "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	require.NoError(t, importerOver(d.clusterClient).syncClusterSet(ctx, cfgWith("prod")))
	assert.Contains(t, liveClusters(t, d.clusterClient), "prod")
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
// absent. Its clients are promoted from the embedded deps, so a test reading back what
// a reconcile wrote goes through c.clusterClient / c.cacheClient.
func controllerOver(t *testing.T) *clusterController {
	t.Helper()
	return newClusterController(newTestDeps(t))
}

// The app owns the kubeconfig service and hands it to every reader, so the
// controller must not close it: Close ends every subscription the process holds, not
// just this controller's.
func TestControllerCloseLeavesTheKubeconfigServiceOpen(t *testing.T) {
	c := controllerOver(t)

	stop, err := c.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	require.NoError(t, c.Close())

	sub := c.kubeconfigSvc.Subscribe()
	defer sub.Close()
	testutil.Recv(t, sub.Chan(), "the current config from a service still open")
}

// --- ClusterCache creation ---

// probedCluster stores a cluster record and hands back the object a reconcile would
// be given, carrying the UID a probe recorded. Status is set in memory: nothing has
// written one yet, which is exactly the state the probe will leave behind.
func probedCluster(t *testing.T, clusters beehive.Client[ClusterSpec, ClusterStatus], uid string) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	obj := createCluster(t, clusters, "prod")
	obj.Status = &ClusterStatus{Server: ClusterServer{UID: &uid}}
	return obj
}

// liveCaches returns every cache the reconcile wrote, owner edge loaded.
func liveCaches(t *testing.T, caches beehive.Client[ClusterCacheSpec, ClusterCacheStatus]) []*beehive.Object[ClusterCacheSpec, ClusterCacheStatus] {
	t.Helper()
	objs, err := caches.List(context.Background(), beehive.LoadOwner())
	require.NoError(t, err)
	return objs
}

// A cluster owns a mirror slot per identity it has been probed at, so the pass that
// knows the identity is the one that creates it.
func TestReconcileCreatesACacheForTheProbedIdentity(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	_, err := c.Reconcile(context.Background(), &stubControllerClient{}, obj)
	require.NoError(t, err)

	objs := liveCaches(t, c.cacheClient)
	require.Len(t, objs, 1)
	assert.Equal(t, ClusterCacheName(ClusterID(obj.ID), "uid-1"), objs[0].Name)
	assert.Equal(t, "uid-1", objs[0].Spec.ServerUID)

	// The owner edge is what carries the cluster join AND what beehive's GC cascades
	// on, so a cache created without one outlives the cluster it mirrors.
	owner, ok, err := objs[0].Owner()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, obj.ID, owner.ID)
}

// Every pass ensures the cache, and the importer wakes every record on every
// kubeconfig snapshot — so the second pass is the common case, not the edge.
func TestReconcileCreatesTheCacheOnlyOnce(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	for range 2 {
		_, err := c.Reconcile(context.Background(), &stubControllerClient{}, obj)
		require.NoError(t, err)
	}

	assert.Len(t, liveCaches(t, c.cacheClient), 1)
}

// A cluster that has never connected has no identity to mirror. Creating one anyway
// would name it after the empty UID — a cache CacheIsActive matches against nothing,
// so it could never sync and never be superseded either.
func TestReconcileCreatesNoCacheBeforeTheFirstProbe(t *testing.T) {
	c := controllerOver(t)

	_, err := c.Reconcile(context.Background(), &stubControllerClient{}, probedCluster(t, c.clusterClient, ""))
	require.NoError(t, err)

	assert.Empty(t, liveCaches(t, c.cacheClient))
}

// A rebuilt cluster reuses its record under a new kube-system UID. The mirror cannot
// be reused with it, so the pass adds a second cache and leaves the first in place to
// drain — which is why Cluster.caches is a list.
func TestReconcileAddsACacheForAMigratedIdentity(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	_, err := c.Reconcile(context.Background(), &stubControllerClient{}, obj)
	require.NoError(t, err)

	migrated := "uid-2"
	obj.Status.Server.UID = &migrated
	_, err = c.Reconcile(context.Background(), &stubControllerClient{}, obj)
	require.NoError(t, err)

	uids := make([]string, 0, 2)
	for _, cache := range liveCaches(t, c.cacheClient) {
		uids = append(uids, cache.Spec.ServerUID)
	}
	assert.Equal(t, []string{"uid-1", "uid-2"}, uids, "the superseded cache stays until its subtree drains")
}

// stubCacheClient reports every cache absent and fails the create that follows. The
// embedded interface is nil: the ensure calls nothing else on it.
type stubCacheClient struct {
	beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	err error
}

func (c stubCacheClient) GetByName(context.Context, string, ...beehive.LoadOption) (*beehive.Object[ClusterCacheSpec, ClusterCacheStatus], error) {
	return nil, beehive.ErrNotFound
}

func (c stubCacheClient) GetOrCreate(context.Context, string, ClusterCacheSpec, ...beehive.Option) (*beehive.Object[ClusterCacheSpec, ClusterCacheStatus], bool, error) {
	return nil, false, c.err
}

// The cache is part of the pass, so failing to ensure it fails the reconcile — and
// nothing downstream runs. Settling the generation here instead would leave a cluster
// holding an identity with no mirror and nothing left to re-level it.
func TestReconcileReportsAFailedCacheCreate(t *testing.T) {
	boom := errors.New("boom")
	c := controllerOver(t)
	c.cacheClient = stubCacheClient{err: boom}

	client := &stubControllerClient{}
	_, err := c.Reconcile(context.Background(), client, probedCluster(t, c.clusterClient, "uid-1"))

	require.ErrorIs(t, err, boom)
	assert.Nil(t, client.updated)
	assert.Nil(t, client.observed, "an unsettled generation is what brings the pass back")
}

// beehive starts ahead of the controllers, so an owed pass can reach a record before
// the kubeconfig's first read. Observing the pre-read config would report a present
// context absent and wake the kind's watches for a flap.
func TestReconcileDefersUntilTheKubeconfigIsRead(t *testing.T) {
	// Unstarted, which is the only way to hold a service that has not read yet.
	c := newClusterController(deps{kubeconfigSvc: kubeconfig.New(t.TempDir()+"/config", nil)})

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

// --- toCluster ---

func TestToCluster(t *testing.T) {
	now := time.Now()
	uid := "uid-1"
	obj := &beehive.Object[ClusterSpec, ClusterStatus]{
		ID:         7,
		Generation: 3,
		CreatedAt:  now,
		Spec:       ClusterSpec{Enabled: true, SyncEnabled: true, Source: ClusterSpecSource{Kubeconfig: kubeconfigSrc("prod")}},
		Status:     &ClusterStatus{Server: ClusterServer{UID: &uid}},
		Conditions: []Condition{LiveCondition(ConditionConnected, beehive.ConditionTrue, "Reachable", "")},
	}

	c := toCluster(obj)

	assert.Equal(t, ClusterID(7), c.ID)
	assert.Equal(t, int64(3), c.Generation)
	assert.Equal(t, now, c.CreatedAt)
	assert.Nil(t, c.DeletionRequestedAt)
	assert.True(t, c.Spec.Enabled)
	assert.Equal(t, "prod", c.Spec.Source.Kubeconfig.Context)
	require.NotNil(t, c.Status.Server.UID)
	assert.Equal(t, uid, *c.Status.Server.UID)
	require.Len(t, c.Conditions, 1, "conditions are object rows, not part of the status blob")
	assert.Equal(t, string(ConditionConnected), c.Conditions[0].Type)
}

// beehive leaves Status nil until a controller first writes it, so every record
// built before that would carry a nil deref rather than a zero status.
func TestToClusterWithNoStatus(t *testing.T) {
	c := toCluster(&beehive.Object[ClusterSpec, ClusterStatus]{ID: 7})

	assert.Equal(t, ClusterStatus{}, c.Status)
}

// The tombstone is surfaced as-is: a consumer that renders a record on its way out
// has no other way to know.
func TestToClusterCarriesTheDeletionTombstone(t *testing.T) {
	now := time.Now()
	obj := &beehive.Object[ClusterSpec, ClusterStatus]{ID: 7, DeletionRequestedAt: &now}

	c := toCluster(obj)

	require.NotNil(t, c.DeletionRequestedAt)
	assert.Equal(t, now, *c.DeletionRequestedAt)
}

// --- Clusters() reads ---

// serviceOver returns a service reading through d, with no background work: these
// tests drive the store directly. The controller is real but unstarted, because Delete
// reaches its importer.
func serviceOver(t *testing.T, d deps) *service {
	t.Helper()
	return &service{
		deps:        d,
		clusterCtrl: newClusterController(d),
	}
}

// createCluster creates a kubeconfig-sourced record for contextName.
func createCluster(t *testing.T, client beehive.Client[ClusterSpec, ClusterStatus], contextName string) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	obj, err := client.Create(context.Background(), KubeconfigName(contextName), ClusterSpec{
		Enabled: true,
		Source:  ClusterSpecSource{Kubeconfig: kubeconfigSrc(contextName)},
	})
	require.NoError(t, err)
	return obj
}

func TestClustersGet(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")

	got, err := serviceOver(t, d).Clusters().Get(context.Background(), ClusterID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, ClusterID(obj.ID), got.ID)
	assert.Equal(t, "prod", got.Spec.Source.Kubeconfig.Context)
}

// An unknown id is not an error: the caller holds an id from a watch frame, and a
// record collected in between is an ordinary race rather than a bad request.
func TestClustersGetUnknownIsNotAnError(t *testing.T) {
	got, err := serviceOver(t, newTestDeps(t)).Clusters().Get(context.Background(), 404)

	require.NoError(t, err)
	assert.Nil(t, got)
}

// A record on its way out is served like any other, wearing its tombstone: only the
// consumer knows whether to render it or hide it.
func TestClustersGetCarriesADeletingRecord(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	require.NoError(t, d.clusterClient.Delete(context.Background(), obj.ID))

	got, err := serviceOver(t, d).Clusters().Get(context.Background(), ClusterID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotNil(t, got.DeletionRequestedAt, "the tombstone is what the consumer decides on")
}

func TestClustersList(t *testing.T) {
	d := newTestDeps(t)
	createCluster(t, d.clusterClient, "prod")
	createCluster(t, d.clusterClient, "staging")

	got, err := serviceOver(t, d).Clusters().List(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2)
	contexts := []string{got[0].Spec.Source.Kubeconfig.Context, got[1].Spec.Source.Kubeconfig.Context}
	assert.ElementsMatch(t, []string{"prod", "staging"}, contexts)
}

// The collection a reader sees matches what Get would answer for each id.
func TestClustersListCarriesADeletingRecord(t *testing.T) {
	d := newTestDeps(t)
	createCluster(t, d.clusterClient, "prod")
	deleting := createCluster(t, d.clusterClient, "staging")
	require.NoError(t, d.clusterClient.Delete(context.Background(), deleting.ID))

	got, err := serviceOver(t, d).Clusters().List(context.Background())

	require.NoError(t, err)
	assert.Len(t, got, 2, "the store as it is, tombstones and all")
}

// sortKeysOf returns each listed cluster's display label, in the order served.
func sortKeysOf(t *testing.T, svc *service) []string {
	t.Helper()
	got, err := svc.Clusters().List(context.Background())
	require.NoError(t, err)

	keys := make([]string, len(got))
	for i, c := range got {
		keys[i] = cmp.Or(deref(c.Spec.Name), c.Spec.Source.Kubeconfig.Context)
	}
	return keys
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// The schema promises name order, and beehive lists in storage order — so a caller
// paging or diffing the list would otherwise see it shuffle between reads.
func TestClustersListIsSortedByName(t *testing.T) {
	d := newTestDeps(t)
	for _, name := range []string{"staging", "alpha", "prod"} {
		createCluster(t, d.clusterClient, name)
	}

	assert.Equal(t, []string{"alpha", "prod", "staging"}, sortKeysOf(t, serviceOver(t, d)))
}

// The order is over what a view renders, so renaming a cluster moves it — the
// kube-context it happens to track is not what a reader is scanning.
func TestClustersListSortsByDisplayNameWhenSet(t *testing.T) {
	d := newTestDeps(t)
	svc := serviceOver(t, d)
	for _, name := range []string{"alpha", "prod"} {
		createCluster(t, d.clusterClient, name)
	}
	// "alpha" renamed to sort last, against its context's own order.
	obj, err := d.clusterClient.GetByName(context.Background(), KubeconfigName("alpha"))
	require.NoError(t, err)
	renamed := "zulu"
	obj.Spec.Name = &renamed
	_, err = d.clusterClient.Update(context.Background(), obj.ID, obj.Spec)
	require.NoError(t, err)

	assert.Equal(t, []string{"prod", "zulu"}, sortKeysOf(t, svc))
}

func TestClustersListIsEmptyWithNoRecords(t *testing.T) {
	got, err := serviceOver(t, newTestDeps(t)).Clusters().List(context.Background())

	require.NoError(t, err)
	assert.Empty(t, got)
}

// watchClusters opens a cluster list watch bounded by the test.
func watchClusters(t *testing.T, d deps) *Stream[ClusterWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Clusters().WatchList(ctx)
	require.NoError(t, err)
	return stream
}

// The snapshot arrives as Added frames closed by exactly one Bookmark — the frame a
// consumer renders its empty state on, and never before.
func TestClustersWatchListEmitsTheSnapshotThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	createCluster(t, d.clusterClient, "prod")
	createCluster(t, d.clusterClient, "staging")

	stream := watchClusters(t, d)

	// The snapshot's order is the store's, which no consumer relies on.
	var contexts []string
	for range 2 {
		f := testutil.Recv(t, stream.Frames, "a snapshot frame")
		require.Equal(t, DeltaFrameAdded, f.Type)
		require.NotNil(t, f.Cluster)
		contexts = append(contexts, f.Cluster.Spec.Source.Kubeconfig.Context)
	}
	assert.ElementsMatch(t, []string{"prod", "staging"}, contexts)

	bookmark := testutil.Recv(t, stream.Frames, "the bookmark closing the snapshot")
	assert.Equal(t, DeltaFrameBookmark, bookmark.Type)
	assert.Nil(t, bookmark.Cluster, "the bookmark carries no entity")
}

// An empty collection is definitively empty rather than pending, so the bookmark
// still lands: without it a populated table and an empty one look alike.
func TestClustersWatchListBookmarksAnEmptyCollection(t *testing.T) {
	stream := watchClusters(t, newRunningDeps(t))

	first := testutil.Recv(t, stream.Frames, "the bookmark")
	assert.Equal(t, DeltaFrameBookmark, first.Type)
}

// awaitBookmark drains the snapshot up to and including the bookmark.
func awaitBookmark(t *testing.T, stream *Stream[ClusterWatchFrame]) {
	t.Helper()
	for {
		if testutil.Recv(t, stream.Frames, "the bookmark").Type == DeltaFrameBookmark {
			return
		}
	}
}

func TestClustersWatchListReportsACreate(t *testing.T) {
	d := newRunningDeps(t)
	stream := watchClusters(t, d)
	awaitBookmark(t, stream)

	createCluster(t, d.clusterClient, "prod")

	f := testutil.Recv(t, stream.Frames, "the create")
	assert.Equal(t, DeltaFrameAdded, f.Type)
	require.NotNil(t, f.Cluster)
	assert.Equal(t, "prod", f.Cluster.Spec.Source.Kubeconfig.Context)
}

func TestClustersWatchListReportsAnUpdate(t *testing.T) {
	d := newRunningDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	stream := watchClusters(t, d)
	awaitBookmark(t, stream)

	_, err := d.clusterClient.Update(context.Background(), obj.ID, ClusterSpec{
		Enabled: false,
		Source:  ClusterSpecSource{Kubeconfig: kubeconfigSrc("prod")},
	})
	require.NoError(t, err)

	f := testutil.Recv(t, stream.Frames, "the update")
	assert.Equal(t, DeltaFrameModified, f.Type)
	require.NotNil(t, f.Cluster)
	assert.False(t, f.Cluster.Spec.Enabled)
}

// The soft-delete mark reaches a subscriber carrying the tombstone, which is what a
// consumer renders or hides on. The frame TYPE is not pinned here: GC can collect the
// row before this reads, and beehive folds the mark and the removal into one Deleted
// when both land in a single tail page. TestClustersWatchListReportsTheMarkAsModified
// pins the type over a change stream nothing else can reorder.
func TestClustersWatchListReportsADeletionMark(t *testing.T) {
	d := newRunningDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	stream := watchClusters(t, d)
	awaitBookmark(t, stream)

	require.NoError(t, d.clusterClient.Delete(context.Background(), obj.ID))

	f := testutil.Recv(t, stream.Frames, "the deletion mark")
	require.NotNil(t, f.Cluster)
	assert.Equal(t, ClusterID(obj.ID), f.Cluster.ID)
	assert.NotNil(t, f.Cluster.DeletionRequestedAt)
}

// pumpFrames runs the watch pump over a snapshot and a hand-driven change stream, and
// collects what it produced. A departure spans two log entries whose grouping is beehive's to decide, so
// driving the pump directly is the only way to pin both shapes.
func pumpFrames(t *testing.T, snapshot []*beehive.Object[ClusterSpec, ClusterStatus], changes ...beehive.ObjectChange[ClusterSpec, ClusterStatus]) []ClusterWatchFrame {
	t.Helper()
	src := make(chan beehive.ObjectChange[ClusterSpec, ClusterStatus], len(changes))
	for _, c := range changes {
		src <- c
	}
	close(src)

	out := make(chan ClusterWatchFrame, len(snapshot)+len(changes)+1)
	require.NoError(t, pumpClusterWatch(context.Background(), out, snapshot, src, func() error { return nil }))
	close(out)

	var frames []ClusterWatchFrame
	for f := range out {
		frames = append(frames, f)
	}
	return frames
}

// deletingCluster is a tombstoned row: what both halves of a departure carry.
func deletingCluster(id beehive.ObjectID) *beehive.Object[ClusterSpec, ClusterStatus] {
	now := time.Now()
	return &beehive.Object[ClusterSpec, ClusterStatus]{ID: id, DeletionRequestedAt: &now}
}

// A removal whose final row beehive could not decode carries no object, and nothing
// later in the log mentions the id. The frame still has to name it: a consumer drops a
// change with no entity, so the record would sit in its map until the subscription ends.
func TestClustersWatchListReportsAnUndecodableDeparture(t *testing.T) {
	frames := pumpFrames(t, nil,
		beehive.ObjectChange[ClusterSpec, ClusterStatus]{Type: beehive.Deleted, ID: 7, Object: nil},
	)

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameDeleted, frames[1].Type)
	require.NotNil(t, frames[1].Cluster, "the bookmark is the only frame that carries no entity")
	assert.Equal(t, ClusterID(7), frames[1].Cluster.ID)
}

// The tombstone mark is an ordinary field change on a row that is still there, so it
// arrives as Modified; Deleted is the row's removal alone.
func TestClustersWatchListReportsTheMarkAsModified(t *testing.T) {
	obj := deletingCluster(7)

	frames := pumpFrames(t, nil,
		beehive.ObjectChange[ClusterSpec, ClusterStatus]{Type: beehive.Modified, ID: 7, Object: obj},
		beehive.ObjectChange[ClusterSpec, ClusterStatus]{Type: beehive.Deleted, ID: 7, Object: obj},
	)

	require.Len(t, frames, 3)
	assert.Equal(t, DeltaFrameModified, frames[1].Type)
	require.NotNil(t, frames[1].Cluster)
	assert.NotNil(t, frames[1].Cluster.DeletionRequestedAt, "the mark the consumer decides on")
	assert.Equal(t, DeltaFrameDeleted, frames[2].Type)
}

// A record already tombstoned when the snapshot is taken is in it, like any other row.
func TestClustersWatchListSnapshotCarriesADeletingRecord(t *testing.T) {
	frames := pumpFrames(t, []*beehive.Object[ClusterSpec, ClusterStatus]{deletingCluster(7)})

	require.Len(t, frames, 2)
	assert.Equal(t, DeltaFrameAdded, frames[0].Type)
	require.NotNil(t, frames[0].Cluster)
	assert.NotNil(t, frames[0].Cluster.DeletionRequestedAt)
	assert.Equal(t, DeltaFrameBookmark, frames[1].Type)
}

// Cancellation is an ordinary teardown, so Frames closes with nothing to report: a
// consumer reads Err on close, and a reason there is rendered as a dead watch.
func TestClustersWatchListCancellationIsQuiet(t *testing.T) {
	d := newRunningDeps(t)
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := serviceOver(t, d).Clusters().WatchList(ctx)
	require.NoError(t, err)
	awaitBookmark(t, stream)

	cancel()

	testutil.WaitClosed(t, stream.Frames, "the frames to close on cancellation")
	assert.NoError(t, stream.Err())
}

// watchCluster opens a single-record watch bounded by the test.
func watchCluster(t *testing.T, d deps, id ClusterID) *Stream[ClusterWatchFrame] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stream, err := serviceOver(t, d).Clusters().Watch(ctx, id)
	require.NoError(t, err)
	return stream
}

func TestClustersWatchEmitsTheRecordThenABookmark(t *testing.T) {
	d := newRunningDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")

	stream := watchCluster(t, d, ClusterID(obj.ID))

	first := testutil.Recv(t, stream.Frames, "the record")
	assert.Equal(t, DeltaFrameAdded, first.Type)
	require.NotNil(t, first.Cluster)
	assert.Equal(t, ClusterID(obj.ID), first.Cluster.ID)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

// An id naming nothing is bookmark-only rather than an error: the record may not
// exist yet, and the same subscription reports it arriving.
func TestClustersWatchBookmarksAnUnknownID(t *testing.T) {
	stream := watchCluster(t, newRunningDeps(t), 404)

	assert.Equal(t, DeltaFrameBookmark, testutil.Recv(t, stream.Frames, "the bookmark").Type)
}

func TestClustersWatchReportsChangesToItsRecord(t *testing.T) {
	d := newRunningDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	stream := watchCluster(t, d, ClusterID(obj.ID))
	awaitBookmark(t, stream)

	require.NoError(t, d.clusterClient.Delete(context.Background(), obj.ID))

	// Type unpinned for the same reason as the list watch: GC may collect the row
	// first, and beehive folds the pair into one Deleted when it does.
	f := testutil.Recv(t, stream.Frames, "the deletion mark")
	require.NotNil(t, f.Cluster)
	assert.Equal(t, ClusterID(obj.ID), f.Cluster.ID)
	assert.NotNil(t, f.Cluster.DeletionRequestedAt)
}

func TestClustersSetEnabled(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")

	got, err := serviceOver(t, d).Clusters().SetEnabled(context.Background(), ClusterID(obj.ID), false)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Spec.Enabled)
	assert.Equal(t, "prod", got.Spec.Source.Kubeconfig.Context, "the rest of the spec is untouched")
}

func TestClustersSetSyncEnabled(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")

	got, err := serviceOver(t, d).Clusters().SetSyncEnabled(context.Background(), ClusterID(obj.ID), true)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Spec.SyncEnabled)
	assert.True(t, got.Spec.Enabled, "the other axis is untouched")
}

// The two setters read, edit one field, and write the whole spec, so without
// serialization the later write restores what the earlier one changed.
func TestClustersSettersDoNotLoseConcurrentUpdates(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	clusters := serviceOver(t, d).Clusters()
	id := ClusterID(obj.ID)

	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := clusters.SetEnabled(context.Background(), id, false)
		assert.NoError(t, err)
	})
	wg.Go(func() {
		_, err := clusters.SetSyncEnabled(context.Background(), id, true)
		assert.NoError(t, err)
	})
	wg.Wait()

	got, err := clusters.Get(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Spec.Enabled, "SetEnabled's write survived")
	assert.True(t, got.Spec.SyncEnabled, "SetSyncEnabled's write survived")
}

// A mutation against a record that is gone is the caller's to see: unlike a read, it
// asked for a change that did not happen. It reports the boundary's own sentinel —
// graph matches that one, and beehive is not supposed to be visible through here.
func TestClustersSetEnabledReportsAnUnknownID(t *testing.T) {
	_, err := serviceOver(t, newTestDeps(t)).Clusters().SetEnabled(context.Background(), 404, false)

	assert.ErrorIs(t, err, ErrNotFound)
	assert.NotErrorIs(t, err, beehive.ErrNotFound, "the store's sentinel must not leak")
}

func TestClustersDelete(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	clusters := serviceOver(t, d).Clusters()

	require.NoError(t, clusters.Delete(context.Background(), ClusterID(obj.ID)))

	got, err := clusters.Get(context.Background(), ClusterID(obj.ID))
	require.NoError(t, err)
	require.NotNil(t, got, "the row is still there until beehive collects it")
	assert.NotNil(t, got.DeletionRequestedAt, "wearing the tombstone Delete asked for")
}

// Deleting a record whose kube-context is still in the file frees that context, and
// nothing in the kubeconfig changed — so without this the importer, whose only other
// trigger is a snapshot, would not re-import until the user next edits the file.
func TestClustersDeleteAsksTheImporterForAPass(t *testing.T) {
	d := newTestDeps(t)
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")

	require.NoError(t, svc.Clusters().Delete(context.Background(), ClusterID(obj.ID)))

	assert.Len(t, svc.clusterCtrl.kubeconfigImporter.resync, 1, "a pass is pending")
}

// Deleting a record that is already gone is the outcome the caller asked for, and
// beehive answers the same way whether the id was ever alive.
func TestClustersDeleteIsIdempotent(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	clusters := serviceOver(t, d).Clusters()
	require.NoError(t, clusters.Delete(context.Background(), ClusterID(obj.ID)))

	assert.NoError(t, clusters.Delete(context.Background(), ClusterID(obj.ID)), "a second delete")
	assert.NoError(t, clusters.Delete(context.Background(), 404), "an id that never existed")
}

// --- clusterController ---

// The controller owns the importer, so its stop closure is what takes it down; a
// leaked one would outlive the service.
func TestClusterControllerStartStop(t *testing.T) {
	// A real client, not nil: the importer runs its first import as soon as it starts.
	c := newClusterController(newTestDeps(t))

	stop, err := c.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "controller stop to return")

	assert.NoError(t, c.Close())
}
