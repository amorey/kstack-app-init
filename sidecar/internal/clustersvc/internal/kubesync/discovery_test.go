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

package kubesync

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestASweepWritesWhatTheClusterServesToTheCatalog(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true), listable("Namespace", "namespaces", false))
	cluster.serve("apps/v1", listable("Deployment", "deployments", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	assert.Equal(t, []kubestore.KindRow{
		{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: kubestore.ScopeNamespaced},
		{APIVersion: "v1", Kind: "Namespace", Resource: "namespaces", Scope: kubestore.ScopeCluster},
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, catalogOf(t, svc, 1))
}

func TestASweepKeepsOnlyWhatAWorkerCanMirror(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1",
		listable("Pod", "pods", true),
		// A subresource has no collection behind it.
		listable("Pod", "pods/log", true),
		// A create-only kind is a worker that can only fail.
		metav1.APIResource{Kind: "TokenReview", Name: "tokenreviews", Verbs: metav1.Verbs{"create"}},
		listable("Event", "events", true))
	// One store backs both spellings of Event, and v1/events is the one that is synced.
	cluster.serve("events.k8s.io/v1", listable("Event", "events", true))
	// Every served version mirrors the same objects again, so only the preferred one is kept.
	cluster.group("apps", "apps/v1", "apps/v1beta1")
	cluster.serve("apps/v1", listable("Deployment", "deployments", true))
	cluster.serve("apps/v1beta1", listable("Deployment", "deployments", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	assert.Equal(t, []kubestore.KindRow{
		{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Scope: kubestore.ScopeNamespaced},
		{APIVersion: "v1", Kind: "Event", Resource: "events", Scope: kubestore.ScopeNamespaced},
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, catalogOf(t, svc, 1))
}

func TestAGroupThatWillNotAnswerIsPartialAndPrunesNothing(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	cluster.group("metrics.k8s.io", "metrics.k8s.io/v1beta1")
	cluster.serve("metrics.k8s.io/v1beta1", listable("NodeMetrics", "nodes", false))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)
	require.Len(t, catalogOf(t, svc, 1), 2)

	// The aggregated API goes down. Its kinds' own workers report their own verdicts, so the
	// sweep degrades rather than dropping rows it could not confirm.
	cluster.breakPath("/apis/metrics.k8s.io/v1beta1")
	svc.RestartAll()
	awaitReason(t, svc, 1, ReasonPartial)

	assert.Len(t, catalogOf(t, svc, 1), 2, "a partial answer keeps what it could not confirm")
}

func TestAnUnchangedCatalogIsWrittenOnceAndAnnouncedOnce(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)
	cluster.reads.Drain()

	// Subscribed after the cache settled, so what arrives is the second sweep's alone.
	news := svc.WatchDiscoveryNews()
	t.Cleanup(news.Close)

	// A sweep that changed nothing is not news: the reason did not move, and the write is
	// skipped on the fingerprint the table already carries.
	svc.RestartAll()
	cluster.awaitRead(t, "/apis")
	testutil.NoRecv(t, news.Chan(), quietWindow, "an unchanged catalog wakes nobody")
}

func TestACatalogThatMovedIsAnnouncedWithItsReasonUnmoved(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// Subscribed after the cache settled, so what arrives is the second sweep's alone.
	news := svc.WatchDiscoveryNews()
	t.Cleanup(news.Close)

	// Two sweeps both settling on Discovered with a kind appearing between them: a
	// reason-only feed would leave the new kind unmirrored until something unrelated moved.
	cluster.serve("v1", listable("Pod", "pods", true), listable("Secret", "secrets", true))
	svc.RestartAll()
	testutil.Recv(t, news.Chan(), "the cache is woken for the kind that appeared")

	got, ok := svc.GetDiscoveryState(1)
	require.True(t, ok)
	assert.Equal(t, ReasonDiscovered, got.Reason, "the reason did not move")
	assert.Len(t, catalogOf(t, svc, 1), 2)
}

func TestASweepWithNoConnectionSuspendsAndIsWokenByTheBridge(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitReason(t, svc, 1, ReasonNoConnection)
	assert.Empty(t, catalogOf(t, svc, 1))

	// A suspended sweep schedules nothing, so the session's wake loop is the only thing that
	// brings it back.
	pool.lease("prod").connect(t, cluster, "uid-1")
	awaitDiscovered(t, svc, 1)
	assert.Len(t, catalogOf(t, svc, 1), 1)
}

func TestASweepRefusesAConnectionAnsweringAsAnotherCluster(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-other")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitReason(t, svc, 1, ReasonIdentityMismatch)
	assert.Empty(t, catalogOf(t, svc, 1), "nothing syncs into a cache the connection does not vouch for")
}

func TestIsCRDComesFromTheCRDList(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	cluster.group("example.com", "example.com/v1")
	cluster.serve("example.com/v1", listable("Widget", "widgets", true))
	// Matched by (group, plural) with no version: one definition serves several, and a kind
	// found at any of them is the same custom resource.
	cluster.crd("example.com", "widgets")

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	assert.Equal(t, []kubestore.KindRow{
		{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: kubestore.ScopeNamespaced, IsCRD: true},
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, catalogOf(t, svc, 1))
}

func TestARefusedCRDListLeavesEveryKindBuiltIn(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	cluster.group("example.com", "example.com/v1")
	cluster.serve("example.com/v1", listable("Widget", "widgets", true))
	cluster.crd("example.com", "widgets")
	// Listing CRDs is a cluster-scoped read RBAC commonly denies, and failing a sweep over it
	// would take discovery away from users it otherwise serves.
	cluster.forbidCRDs()

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	assert.Equal(t, []kubestore.KindRow{
		{APIVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Scope: kubestore.ScopeNamespaced},
		{APIVersion: "v1", Kind: "Pod", Resource: "pods", Scope: kubestore.ScopeNamespaced},
	}, catalogOf(t, svc, 1))
}

func TestACRDWriteWakesTheSweep(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)
	cluster.reads.Drain()

	// The cache mirrors CustomResourceDefinitions like any other kind, so its own write is
	// what tells the sweep the catalog moved — a private watch here would be a second watch
	// on the same collection over the same connection.
	writeRow(t, svc, 1, testKind("apiextensions.k8s.io/v1", "CustomResourceDefinition", "customresourcedefinitions"))
	cluster.awaitRead(t, "/api/v1")
}

func TestACRDBringingANewGroupWithItIsDiscovered(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// A CRD for a group the cluster did not serve, so /apis moved and not just one group's
	// document: a wake that carried the fan-out alone would sweep the old list and leave
	// this kind unseen until the group list's own interval came round.
	cluster.serve("example.com/v1", listable("Widget", "widgets", true))
	cluster.crd("example.com", "widgets")
	writeRow(t, svc, 1, testKind("apiextensions.k8s.io/v1", "CustomResourceDefinition", "customresourcedefinitions"))

	require.Eventually(t, func() bool {
		return slices.ContainsFunc(catalogOf(t, svc, 1), func(r kubestore.KindRow) bool {
			return r.Resource == "widgets" && r.IsCRD
		})
	}, testutil.Timeout, time.Millisecond, "the new group's kind to reach the catalog")
}

func TestAGroupListThatFailsLeavesTheCatalogStanding(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	cluster.serve("apps/v1", listable("Deployment", "deployments", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// The group list stops answering, so the fan-out sweeps the last one it read. That list
	// is never older than the rows on disk — they were written from it — and a group-version
	// on it that stopped serving fails its own read, which is what makes the sweep partial.
	// So the catalog stands rather than being pruned against an answer nobody confirmed.
	cluster.breakPath("/apis")
	svc.RestartAll()
	awaitReason(t, svc, 1, ReasonDiscoveryFailed)

	assert.Len(t, catalogOf(t, svc, 1), 2, "a sweep whose group list failed deletes nothing")
}

func TestForgetDiscoveryCancelsAndJoinsASweepInFlight(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)
	cluster.reads.Drain()

	// Park the fan-out mid-request. Dropping the engine's subject stops a result being
	// applied but neither cancels the run nor joins it, so a teardown that only did that
	// would give the store back under a sweep still about to write through it.
	// Released whatever happens: a run left parked here would outlive the test and hang the
	// shutdown that waits for it.
	release := cluster.hold("/api/v1")
	defer release()

	svc.RestartAll()
	cluster.awaitRead(t, "/api/v1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.ForgetDiscovery(1)
	}()

	// The cancel: without it the run would sit here until the probe's own timeout, which
	// outlasts the failsafe several times over.
	cluster.cancelled.Await(t, "the sweep in flight to be cancelled")

	// The wait: the run is deliberately still unwinding, and a teardown that returned now
	// would give the store back under it. A negative assertion, so it needs a window.
	testutil.NoRecv(t, done, quietWindow, "the teardown returns before the run is done")
	release()
	testutil.Wait(t, done, "the teardown to return once the run is done")
}

func TestForgettingACacheStopsItsSweep(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	svc.ForgetDiscovery(1)
	cluster.reads.Drain()
	svc.RestartAll()
	cluster.noRead(t, "a forgotten cache is no longer swept")
}

func TestASweepSkipsASubjectThatIsNotAnArmedCache(t *testing.T) {
	svc, _ := newTestService(t)
	ran := false
	body := func(context.Context, *session, *kubeconn.Connection, *probe.Pass[uint64]) probe.Result {
		ran = true
		return probe.Succeeded()
	}

	// A subject this package did not name, and one whose cache nobody armed. Neither is a
	// state a caller can reach — both are runs that must record nothing.
	for _, subject := range []string{"not-a-cache", subjectOf(404)} {
		result := underSession(svc, body).Run(t.Context(), probe.NewPass[uint64](subject, nil, probe.Snapshot{}))
		assert.True(t, result.IsSkip(), "a run against %s records nothing", subject)
	}
	assert.False(t, ran, "and never reaches the body")
}

func TestAPoolThatWillNotAnswerFailsTheSweep(t *testing.T) {
	svc, pool := newTestService(t)
	start(t, svc)
	pool.lease("prod").refuse(errors.New("the pool is broken"))

	svc.TrackDiscovery(1, testParams)

	awaitReason(t, svc, 1, ReasonDiscoveryFailed)
}

func TestAGroupNamingNoPreferredVersionFallsBackToItsFirst(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1")
	cluster.serve("apps/v1", listable("Deployment", "deployments", true))
	cluster.unpreferGroup("apps")

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	rows := catalogOf(t, svc, 1)
	require.Len(t, rows, 1)
	assert.Equal(t, "apps/v1", rows[0].APIVersion, "the group's first version stands in for the one it did not name")
}

func TestADocumentThatIsNotJSONFailsTheSweep(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serveRaw("/api", "not a document")

	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)
	svc.TrackDiscovery(1, testParams)

	awaitReason(t, svc, 1, ReasonDiscoveryFailed)
}

func TestACacheRemovedUnderASweepFailsItRatherThanWritingThrough(t *testing.T) {
	cluster := newFakeCluster(t)
	cluster.serve("v1", listable("Pod", "pods", true))
	svc, pool := newTestService(t)
	pool.lease("prod").connect(t, cluster, "uid-1")
	start(t, svc)

	svc.TrackDiscovery(1, testParams)
	awaitDiscovered(t, svc, 1)

	// The file the session holds is retired under it, which is what a cache deleted while
	// its sweep still runs looks like: the write has nowhere to land.
	require.NoError(t, svc.storeMgr.(*kubestore.Manager).Remove(1))
	svc.RestartAll()

	awaitReason(t, svc, 1, ReasonDiscoveryFailed)
}

func TestACatalogWriteThatFailsIsNotReportedAsWritten(t *testing.T) {
	rows := []kubestore.KindRow{{APIVersion: "v1", Kind: "Pod", Resource: "pods"}}

	wrote, err := commitCatalog(t.Context(), refusingCatalog{read: errors.New("read")}, rows, true, 1)
	assert.False(t, wrote)
	assert.Error(t, err, "a fingerprint that cannot be read is not a write")

	wrote, err = commitCatalog(t.Context(), refusingCatalog{write: errors.New("write")}, rows, true, 1)
	assert.False(t, wrote)
	assert.Error(t, err, "a write that fails is not a write")
}

// refusingCatalog is a catalog whose reads or writes refuse, which is what a file retired
// under a sweep looks like from here.
type refusingCatalog struct{ read, write error }

func (c refusingCatalog) KindsWithFingerprint(context.Context) ([]kubestore.KindRow, uint64, bool, error) {
	return nil, 0, false, c.read
}

func (c refusingCatalog) SyncKinds(context.Context, []kubestore.KindRow, bool, uint64) error {
	return c.write
}

func TestAConnectionWithAnUnusableBaseURLIsReported(t *testing.T) {
	conn := &kubeconn.Connection{BaseURL: &url.URL{Scheme: "http", Host: "not a host"}}

	err := getJSON(t.Context(), conn, "/api", &metav1.APIVersions{})
	assert.ErrorContains(t, err, "build request for /api")
}
