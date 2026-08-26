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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
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

// --- kubeconfigObservation / Reconcile ---

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

// stubControllerClient captures what a reconcile writes. The embedded interface is
// nil: Reconcile calls nothing else on it.
type stubControllerClient struct {
	beehive.ControllerClient[ClusterStatus]
	updated    *ClusterStatus
	updateErr  error
	conditions []Condition
	setErr     error
	dependsOn  []beehive.ObjectID
	dependErr  error
}

func (c *stubControllerClient) UpdateStatus(_ context.Context, status ClusterStatus) error {
	c.updated = &status
	return c.updateErr
}

func (c *stubControllerClient) SetCondition(_ context.Context, cond Condition) error {
	c.conditions = append(c.conditions, cond)
	return c.setErr
}

// Within runs fn inline: the pass groups its writes so a watcher never sees half of
// them, and what this stub records is that each was attempted.
func (c *stubControllerClient) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (c *stubControllerClient) AddDependency(_ context.Context, toID beehive.ObjectID) error {
	c.dependsOn = append(c.dependsOn, toID)
	return c.dependErr
}

// reconcileCluster runs one cluster pass and requires it settled, which every pass
// over a read kubeconfig does.
func reconcileCluster(t *testing.T, c *clusterController, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) {
	t.Helper()
	require.Equal(t, beehive.Settled(), c.Reconcile(context.Background(), client, obj))
}

// controllerOver returns a controller over an empty kubeconfig, so every context is
// absent. Its clients are promoted from the embedded deps, so a test reading back what
// a reconcile wrote goes through c.clusterClient / c.cacheClient.
func controllerOver(t *testing.T) *clusterController {
	t.Helper()
	return &clusterController{deps: newTestDeps(t)}
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

// --- the identity a probe found ---

func strPtr(s string) *string { return &s }

// identityControllerOver returns a controller whose pool answers with what.
func identityControllerOver(t *testing.T, what *fakeKubeconn) *clusterController {
	t.Helper()
	d := newTestDeps(t)
	d.kubeconnSvc = what
	return &clusterController{deps: d}
}

// reportedCondition is the Connected verdict a pass wrote — the one most tests here are
// about. conditionOfType reaches the other two.
func reportedCondition(t *testing.T, client *stubControllerClient) Condition {
	t.Helper()
	return conditionOfType(t, client, ConditionConnected)
}

// conditionOfType is the verdict a pass wrote for one aspect, or a failure when it wrote
// none for it.
func conditionOfType(t *testing.T, client *stubControllerClient, ct ConditionType) Condition {
	t.Helper()
	cond := FindCondition(client.conditions, ct)
	require.NotNil(t, cond, "a pass reports %s", ct)
	return *cond
}

// Every condition here describes process-scoped state, so a previous process's write
// must read as Unknown until this one re-confirms it.
func TestReconcileReportsLiveness(t *testing.T) {
	c := identityControllerOver(t, answering(kubeconn.Identity{ServerUID: "uid-1"}, nil))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.True(t, reportedCondition(t, client).Liveness)
}

// A cluster the user switched off is not probed, and says so rather than looking like
// one that failed to connect.
func TestReconcileReportsInactiveWhenDisabled(t *testing.T) {
	svc := answering(kubeconn.Identity{ServerUID: "uid-1"}, nil)
	c := identityControllerOver(t, svc)
	obj := createCluster(t, c.clusterClient, "prod")
	obj.Spec.Enabled = false

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Empty(t, svc.asked, "a disabled cluster is not asked about at all")
	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonInactive, cond.Reason)
}

// Releasing is what stops the probe, so a cluster switched off after it connected has to
// give its claim back — a record that costs nothing to keep should cost no dial.
func TestReconcileReleasesTheClaimWhenTheClusterIsDisabled(t *testing.T) {
	svc := answering(kubeconn.Identity{ServerUID: "uid-1"}, nil)
	c := identityControllerOver(t, svc)
	obj := createCluster(t, c.clusterClient, "prod")
	reconcileCluster(t, c, &stubControllerClient{}, obj)
	require.Equal(t, []string{"prod"}, svc.asked, "the enabled pass claims")

	obj.Spec.Enabled = false
	reconcileCluster(t, c, &stubControllerClient{}, obj)

	assert.Equal(t, []string{"prod"}, svc.released)
}

// A record whose source stopped naming credentials has none to hold a claim on, so the
// claim goes back for the same reason a disabled one's does.
func TestReconcileReleasesTheClaimWhenTheSourceStopsNamingIt(t *testing.T) {
	svc := answering(kubeconn.Identity{ServerUID: "uid-1"}, nil)
	c := identityControllerOver(t, svc)
	obj := createCluster(t, c.clusterClient, "prod")
	reconcileCluster(t, c, &stubControllerClient{}, obj)
	require.Empty(t, svc.released)

	obj.Spec.Source.Kubeconfig = nil
	reconcileCluster(t, c, &stubControllerClient{}, obj)

	assert.Equal(t, []string{"prod"}, svc.released)
}

// The read asks for the probe and returns whatever is known, which on the first pass is
// nothing. Neither connected nor failed, and the signal the probe publishes is what
// brings this record back.
func TestReconcileReportsConnectingUntilAnswered(t *testing.T) {
	svc := &fakeKubeconn{}
	c := identityControllerOver(t, svc)
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Equal(t, []string{"prod"}, svc.asked, "asking is what queues the probe")
	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionUnknown, cond.Status)
	assert.Equal(t, ReasonConnecting, cond.Reason)
}

// A record from another source names no context to resolve, so a verdict about reaching
// its server is one no pass here produced.
func TestReconcileReportsNoConditionForAnotherSource(t *testing.T) {
	svc := answering(kubeconn.Identity{ServerUID: "uid-1"}, nil)
	c := identityControllerOver(t, svc)
	obj, err := c.clusterClient.Create(context.Background(), "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Empty(t, client.conditions)
	assert.Empty(t, svc.asked)
	assert.Nil(t, client.updated.Server.UID)
}

// A condition the store refuses is this pass's failure, not the cluster's: settling
// would report a verdict nothing recorded.
func TestReconcileReportsAFailedConditionWrite(t *testing.T) {
	boom := errors.New("boom")
	c := identityControllerOver(t, answering(kubeconn.Identity{ServerUID: "uid-1"}, nil))
	obj := createCluster(t, c.clusterClient, "prod")

	res := c.Reconcile(context.Background(), &stubControllerClient{setErr: boom}, obj)

	assert.ErrorIs(t, res.Err(), boom)
}

// The identity comes from the probe, not from the record: this is the write that turns
// a tracked context into one with a mirror.
func TestReconcileWritesTheProbedUID(t *testing.T) {
	c := identityControllerOver(t, answering(kubeconn.Identity{ServerUID: "uid-1"}, nil))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Server.UID)
	assert.Equal(t, "uid-1", *client.updated.Server.UID)
}

// One probe answers three questions, and dropping two would throw away requests it
// already paid for. The version and the principal have their own status homes.
func TestReconcileWritesTheWholeIdentity(t *testing.T) {
	c := identityControllerOver(t, answering(kubeconn.Identity{
		ServerUID: "uid-1", ServerVersion: "v1.29.3", Username: "admin@example",
	}, nil))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Server.Version)
	assert.Equal(t, "v1.29.3", *client.updated.Server.Version)
	require.NotNil(t, client.updated.Principal.Username)
	assert.Equal(t, "admin@example", *client.updated.Principal.Username)
}

// The endpoint is what tells two contexts naming one server apart from two servers, so a
// pass that reached one records where it went.
func TestReconcileWritesTheEndpointAndGroups(t *testing.T) {
	c := identityControllerOver(t, knowing(kubeconn.State{
		Connection: answeredWith("https://prod.example:6443"),
		ServerUID:  answeredWith("uid-1"),
		Principal: answeredWith(kubeconn.Principal{
			Username: "admin@example",
			Groups:   []string{"system:masters", "system:authenticated"},
		}),
	}))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Server.Endpoint)
	assert.Equal(t, "https://prod.example:6443", *client.updated.Server.Endpoint)
	assert.Equal(t, []string{"system:authenticated", "system:masters"}, client.updated.Principal.Groups,
		"sorted, so a server that re-orders them does not re-emit the record")
}

// Nothing has been probed, so there is nothing to report about the server — a status
// carrying invented blanks would be indistinguishable from one that read them.
func TestReconcileWritesNoServerFactsWhileNothingIsProbed(t *testing.T) {
	c := identityControllerOver(t, &fakeKubeconn{})
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated)
	assert.Nil(t, client.updated.Server.Endpoint)
	assert.Nil(t, client.updated.Server.UID)
	assert.Empty(t, client.updated.Principal.Groups)
}

// A probe refused the kube-system read but reached the server, so it knows who it
// connected as and not what it connected to. Each fact stands on its own: reporting
// none of them would hide a cluster that is up.
func TestReconcileWritesThePartOfTheIdentityAProbeReached(t *testing.T) {
	c := identityControllerOver(t, knowing(kubeconn.State{
		Connection:    answeredWith("https://prod.example:6443"),
		Readiness:     answeredWith(kubeconn.ComponentStatus{}),
		ServerVersion: answeredWith(kubeconn.VersionInfo{GitVersion: "v1.29.3"}),
		Principal:     answeredWith(kubeconn.Principal{Username: "reader@example"}),
		ServerUID:     forbidden("namespaces \"kube-system\" is forbidden"),
	}))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated)
	assert.Nil(t, client.updated.Server.UID, "nothing read it, so nothing claims it")
	require.NotNil(t, client.updated.Principal.Username)
	assert.Equal(t, "reader@example", *client.updated.Principal.Username)
	assert.Empty(t, liveCaches(t, c.cacheClient), "and no cache, since none is named yet")
	assert.Equal(t, ConditionTrue, reportedCondition(t, client).Status,
		"a user refused kube-system still reached a cluster that is up")
}

// Reaching a server needs no authorization and naming it does, so the two verdicts move
// apart — and this is the one that explains a connected cluster that never gets a cache.
func TestReconcileReportsUnidentifiedWhenTheUIDIsRefused(t *testing.T) {
	c := identityControllerOver(t, knowing(kubeconn.State{
		Connection: answeredWith("https://prod.example:6443"),
		Readiness:  answeredWith(kubeconn.ComponentStatus{}),
		ServerUID:  forbidden("namespaces \"kube-system\" is forbidden"),
	}))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Equal(t, ConditionTrue, conditionOfType(t, client, ConditionConnected).Status)
	cond := conditionOfType(t, client, ConditionIdentified)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonUIDUnreadable, cond.Reason)
	assert.Contains(t, cond.Message, "forbidden")
}

// Identity is not a fact about a server nothing reached, so a failed probe leaves it
// unassessed rather than claiming the cluster is unidentifiable.
func TestReconcileLeavesIdentityUnassessedWhenTheProbeFailed(t *testing.T) {
	c := identityControllerOver(t, answering(kubeconn.Identity{}, errors.New("connection refused")))
	obj := createCluster(t, c.clusterClient, "prod")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Equal(t, ReasonProbeFailed, conditionOfType(t, client, ConditionConnected).Reason)
	assert.Equal(t, ReasonNoConnection, conditionOfType(t, client, ConditionIdentified).Reason)
}

// The pass that learns an identity is the one that creates its cache. Nothing else
// would: a status write moves no generation, so no owed pass lists this record again,
// and the cache that would be its dependent is the thing that does not exist yet.
func TestReconcileCreatesTheCacheInTheSamePassThatLearnsTheUID(t *testing.T) {
	c := identityControllerOver(t, answering(kubeconn.Identity{ServerUID: "uid-1"}, nil))
	obj := createCluster(t, c.clusterClient, "prod")

	reconcileCluster(t, c, &stubControllerClient{}, obj)

	objs := liveCaches(t, c.cacheClient)
	require.Len(t, objs, 1)
	assert.Equal(t, "uid-1", objs[0].Spec.ServerUID)
}

// An unprobed record reports nothing, which must not be read as "no identity": clearing
// the UID would deactivate a live cache because a probe had not answered yet.
func TestReconcileKeepsTheLastKnownUIDWhileNothingIsProbed(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	require.NotNil(t, client.updated.Server.UID, "an unanswered probe must not clear it")
	assert.Equal(t, "uid-1", *client.updated.Server.UID)
	assert.Len(t, liveCaches(t, c.cacheClient), 1, "and the cache it already calls for stands")
}

// A cluster owns a mirror slot per identity it has been probed at, so the pass that
// knows the identity is the one that creates it.
func TestReconcileCreatesACacheForTheProbedIdentity(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	reconcileCluster(t, c, &stubControllerClient{}, obj)

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

// Every pass ensures the cache, and the source wakes every record on every
// kubeconfig snapshot — so the second pass is the common case, not the edge.
func TestReconcileCreatesTheCacheOnlyOnce(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	for range 2 {
		reconcileCluster(t, c, &stubControllerClient{}, obj)
	}

	assert.Len(t, liveCaches(t, c.cacheClient), 1)
}

// A cluster that has never connected has no identity to mirror. Creating one anyway
// would name it after the empty UID — a cache CacheIsActive matches against nothing,
// so it could never sync and never be superseded either.
func TestReconcileCreatesNoCacheBeforeTheFirstProbe(t *testing.T) {
	c := controllerOver(t)

	reconcileCluster(t, c, &stubControllerClient{}, probedCluster(t, c.clusterClient, ""))

	assert.Empty(t, liveCaches(t, c.cacheClient))
}

// A rebuilt cluster reuses its record under a new kube-system UID. The mirror cannot
// be reused with it, so the pass adds a second cache and leaves the first in place to
// drain — which is why Cluster.caches is a list.
func TestReconcileAddsACacheForAMigratedIdentity(t *testing.T) {
	c := controllerOver(t)
	obj := probedCluster(t, c.clusterClient, "uid-1")

	reconcileCluster(t, c, &stubControllerClient{}, obj)

	migrated := "uid-2"
	obj.Status.Server.UID = &migrated
	reconcileCluster(t, c, &stubControllerClient{}, obj)

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

// The cache is part of the pass, so failing to ensure it fails the reconcile: settling
// instead would leave a cluster holding an identity with no mirror and nothing left to
// re-level it. The observation is published anyway — it is a different fact with
// different consumers, and holding it back would leave a departed context marked
// present for as long as an unrelated create keeps failing.
func TestReconcileReportsAFailedCacheCreate(t *testing.T) {
	boom := errors.New("boom")
	c := controllerOver(t)
	c.cacheClient = stubCacheClient{err: boom}
	obj := probedCluster(t, c.clusterClient, "uid-1")

	client := &stubControllerClient{}
	res := c.Reconcile(context.Background(), client, obj)

	name := ClusterCacheName(ClusterID(obj.ID), "uid-1")
	assert.Equal(t, fmt.Errorf("create cluster cache %s: %w", name, boom), res.Err())
	assert.NotNil(t, client.updated, "the observation is not the cache's to hold back")
}

// beehive starts ahead of the controllers, so an owed pass can reach a record before
// the kubeconfig's first read. Observing the pre-read config would report a present
// context absent and wake the kind's watches for a flap.
func TestReconcileDefersUntilTheKubeconfigIsRead(t *testing.T) {
	// Unstarted, which is the only way to hold a service that has not read yet.
	c := &clusterController{deps: deps{kubeconfigSvc: kubeconfig.New(t.TempDir()+"/config", nil)}}

	client := &stubControllerClient{}
	res := c.Reconcile(context.Background(), client, kubeconfigObj("prod", nil))

	assert.Nil(t, client.updated, "nothing observed, so nothing to write")
	assert.Equal(t, beehive.Unsettled().RequeueAfter(startupRequeue), res,
		"nothing settled, and it has to be revisited once the watcher has read")
}

func TestReconcileWritesTheObservation(t *testing.T) {
	client := &stubControllerClient{}

	res := controllerOver(t).Reconcile(context.Background(), client, kubeconfigObj("prod", nil))

	assert.Equal(t, beehive.Settled(), res)
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
	reconcileCluster(t, controllerOver(t), client, obj)

	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.Server.UID)
	assert.Equal(t, uid, *client.updated.Server.UID)
}

// A failed status write is the reconcile's failure, so beehive retries it.
func TestReconcileReportsAFailedStatusWrite(t *testing.T) {
	boom := errors.New("boom")

	res := controllerOver(t).Reconcile(context.Background(), &stubControllerClient{updateErr: boom}, kubeconfigObj("prod", nil))

	assert.ErrorIs(t, res.Err(), boom)
}

// A spec write this observation does not depend on still bumps the generation, and an
// object left unsettled is re-dispatched by beehive's owed pass forever — so a pass
// that observes nothing new still has to report that it settled.
//
// What it reports is what was already stored: beehive compares that against the status
// it handed out and reaches the store only for a difference, so an unchanged
// observation writes nothing without this pass deciding it.
func TestReconcileSettlesWhenNothingMoved(t *testing.T) {
	// The seed config holds no contexts, so this is what the reconcile will observe.
	obj := kubeconfigObj("prod", &ClusterStatus{Source: ClusterStatusSource{
		Kubeconfig: &ClusterStatusSourceKubeconfig{Cluster: "prod-cluster", User: "prod-user"},
	}})
	obj.Generation = 3

	client := &stubControllerClient{}
	res := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NotNil(t, client.updated)
	assert.Equal(t, *obj.Status, *client.updated, "nothing observed moved")
	assert.Equal(t, beehive.Settled(), res)
}

// A record from another source has no observation of its own, and two of those are as
// unchanged as any other repeat.
func TestReconcileSettlesForAnotherSource(t *testing.T) {
	// beehive's generations start at 1; 0 is not a generation any object is handed.
	obj := &beehive.Object[ClusterSpec, ClusterStatus]{ID: 1, Generation: 1, Status: &ClusterStatus{}}

	client := &stubControllerClient{}
	res := controllerOver(t).Reconcile(context.Background(), client, obj)

	require.NotNil(t, client.updated)
	assert.Equal(t, ClusterStatus{}, *client.updated, "no observation of its own to report")
	assert.Equal(t, beehive.Settled(), res)
}

// A record on its way out has nothing to observe, and writing status to it would be a
// write beehive has to carry through a collect that is already under way.
func TestReconcileSkipsADeletingRecord(t *testing.T) {
	obj := kubeconfigObj("prod", nil)
	now := time.Now()
	obj.DeletionRequestedAt = &now

	client := &stubControllerClient{}
	reconcileCluster(t, controllerOver(t), client, obj)

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
// tests drive the store directly.
func serviceOver(t *testing.T, d deps) *service {
	t.Helper()
	return &service{deps: d, gaugeCadence: defaultGaugeCadence}
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

// deletingCluster is a tombstoned row: what both halves of a departure carry.
func deletingCluster(id beehive.ObjectID) *beehive.Object[ClusterSpec, ClusterStatus] {
	now := time.Now()
	return &beehive.Object[ClusterSpec, ClusterStatus]{ID: id, DeletionRequestedAt: &now}
}

// A removal whose final row beehive could not decode carries no object, and nothing
// later in the log mentions the id. The frame still has to name it: a consumer drops a
// change with no entity, so the record would sit in its map until the subscription ends.
func TestClustersWatchListReportsAnUndecodableDeparture(t *testing.T) {
	frames := pumpFrames(t, clusterWatch, nil,
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

	frames := pumpFrames(t, clusterWatch, nil,
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
	frames := pumpFrames(t, clusterWatch, []*beehive.Object[ClusterSpec, ClusterStatus]{deletingCluster(7)})

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

// --- the schedule gauge ---

// watchSchedule opens the gauge for id over a cancellable context, handing back the
// cancel so a test can pin what ending the stream does.
func watchSchedule(t *testing.T, d deps, id ClusterID) (<-chan Schedule, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch, err := serviceOver(t, d).Clusters().WatchSchedule(ctx, id)
	require.NoError(t, err)
	return ch, cancel
}

// scheduledAt is a probe with a run queued for at and none in flight.
func scheduledAt(at time.Time) kubeconn.Observation[string] {
	return kubeconn.Observation[string]{Attempts: kubeconn.Attempts{NextAttempt: kubeconn.Attempt{ScheduledAt: at}}}
}

// The countdown is the connection's own. The other four run on their own clocks, and a
// countdown to whichever was due next would not be the one a retry acts on.
func TestClustersWatchScheduleReportsTheConnectionProbe(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	dialAt := probedAt.Add(30 * time.Second)
	d.kubeconnSvc = knowing(kubeconn.State{
		Connection: scheduledAt(dialAt),
		ServerUID:  scheduledAt(probedAt.Add(10 * time.Second)),
	})

	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))

	got := testutil.Recv(t, ch, "the current schedule")
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, dialAt, *got.NextRequeueAt)
	assert.False(t, got.Probing)
}

// Every probe but the connection suspends while it is down, so a disconnected cluster's
// countdown is its retry — the case the gauge exists for.
func TestClustersWatchScheduleIgnoresTheOtherProbes(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.kubeconnSvc = knowing(kubeconn.State{ServerUID: scheduledAt(probedAt.Add(10 * time.Second))})
	pool := d.kubeconnSvc.(*fakeKubeconn)
	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))

	// Nothing to report while only another probe is scheduled, so drive one pass that
	// arms the connection to prove the stream is live rather than merely silent.
	testutil.NoRecv(t, ch, 50*time.Millisecond, "a schedule from another probe")
	pool.publishState("prod", kubeconn.State{Connection: scheduledAt(probedAt)})

	assert.NotNil(t, testutil.Recv(t, ch, "the connection's schedule").NextRequeueAt)
}

func TestClustersWatchScheduleFollowsThePool(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.kubeconnSvc = knowing(kubeconn.State{Connection: scheduledAt(probedAt.Add(30 * time.Second))})
	pool := d.kubeconnSvc.(*fakeKubeconn)
	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))
	testutil.Recv(t, ch, "the current schedule")

	next := probedAt.Add(time.Minute)
	pool.publishState("prod", kubeconn.State{Connection: scheduledAt(next)})

	got := testutil.Recv(t, ch, "the schedule the pass moved")
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, next, *got.NextRequeueAt)
}

// Probing is asserted from the run, never inferred from a countdown that has run out.
func TestClustersWatchScheduleReportsARunInFlight(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.kubeconnSvc = knowing(kubeconn.State{
		Connection: kubeconn.Observation[string]{Attempts: kubeconn.Attempts{
			NextAttempt: kubeconn.Attempt{ScheduledAt: probedAt, StartedAt: probedAt},
		}},
	})

	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))

	assert.True(t, testutil.Recv(t, ch, "the current schedule").Probing)
}

// A gauge says nothing before its first measurement, and a claim whose first pass has
// not landed has made none — reporting its zero state would say "nothing is scheduled"
// about a cluster that is about to be dialed.
func TestClustersWatchScheduleSaysNothingBeforeTheFirstPass(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	pool := d.kubeconnSvc.(*fakeKubeconn)
	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))

	// A negative assertion has no event to wait for, so it needs a bounded window; the
	// stream emits on its own goroutine the moment it is opened, so a short one is enough.
	testutil.NoRecv(t, ch, 50*time.Millisecond, "a schedule for an unprobed claim")

	pool.publishState("prod", kubeconn.State{Connection: scheduledAt(probedAt)})
	assert.NotNil(t, testutil.Recv(t, ch, "the first pass").NextRequeueAt)
}

// Once something has been measured, "nothing is scheduled" is news: a suspended
// connection is what a context the kubeconfig stopped naming looks like.
func TestClustersWatchScheduleReportsASuspendedConnection(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.kubeconnSvc = knowing(kubeconn.State{Connection: scheduledAt(probedAt)})
	pool := d.kubeconnSvc.(*fakeKubeconn)
	ch, _ := watchSchedule(t, d, ClusterID(obj.ID))
	testutil.Recv(t, ch, "the current schedule")

	pool.publishState("prod", kubeconn.State{})

	assert.Nil(t, testutil.Recv(t, ch, "the suspended schedule").NextRequeueAt)
}

func TestClustersWatchScheduleReleasesItsClaimWhenTheStreamEnds(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	pool := d.kubeconnSvc.(*fakeKubeconn)
	ch, cancel := watchSchedule(t, d, ClusterID(obj.ID))

	cancel()

	testutil.WaitClosed(t, ch, "the gauge to close on cancellation")
	assert.Equal(t, []string{"prod"}, pool.released)
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

// The webview only offers Remove for an orphaned row, and the boundary enforces the
// same rule: deleting a record its source still declares would re-import it under a
// fresh id, so the mutation would report success and then undo itself.
func TestClustersDeleteRejectsARecordItsSourceDeclares(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	d.kubeconfigSvc = fakeKubeconfigService{cfg: cfgWith("prod"), loaded: true}
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")
	observePresence(t, status, obj, true)

	err := svc.Clusters().Delete(context.Background(), ClusterID(obj.ID))

	require.ErrorIs(t, err, ErrDeclaredBySource)
	got, err := d.clusterClient.Get(context.Background(), obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt, "the record is untouched")
}

// The case the affordance exists for: the context left the file, so nothing will
// re-import the record and the local state is the user's to forget.
func TestClustersDeleteAcceptsAnOrphanedRecord(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")
	observePresence(t, status, obj, false)

	require.NoError(t, svc.Clusters().Delete(context.Background(), ClusterID(obj.ID)))

	got, err := d.clusterClient.Get(context.Background(), obj.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

// A record from no source is nobody's to declare, so nothing re-creates it.
func TestClustersDeleteAcceptsARecordFromAnotherSource(t *testing.T) {
	d, _ := newClusterStatusDeps(t)
	svc := serviceOver(t, d)
	obj, err := d.clusterClient.Create(context.Background(), "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	require.NoError(t, svc.Clusters().Delete(context.Background(), ClusterID(obj.ID)))

	got, err := d.clusterClient.Get(context.Background(), obj.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.DeletionRequestedAt)
}

// The window the guard exists for: discovery has just created the record and its first
// reconcile has not written status yet, so there is no observation to read. The
// kubeconfig still declares the context, and the webview renders such a record as
// orphaned — so its Remove button is live for exactly this window.
func TestClustersDeleteRejectsADeclaredRecordBeforeItsFirstObservation(t *testing.T) {
	d, _ := newClusterStatusDeps(t)
	d.kubeconfigSvc = fakeKubeconfigService{cfg: cfgWith("prod"), loaded: true}
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")

	err := svc.Clusters().Delete(context.Background(), ClusterID(obj.ID))

	require.ErrorIs(t, err, ErrDeclaredBySource)
	got, err := d.clusterClient.Get(context.Background(), obj.ID)
	require.NoError(t, err)
	assert.Nil(t, got.DeletionRequestedAt)
}

// The same record with the context gone from the file: no observation either, but
// nothing will re-import it, so the delete is the caller's to make.
func TestClustersDeleteAcceptsAnUndeclaredRecordBeforeItsFirstObservation(t *testing.T) {
	d, _ := newClusterStatusDeps(t)
	d.kubeconfigSvc = fakeKubeconfigService{cfg: cfgWith("staging"), loaded: true}
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")

	require.NoError(t, svc.Clusters().Delete(context.Background(), ClusterID(obj.ID)))
}

// The file outranks the record's own observation, which is only a cached view of it: a
// context put back before the record re-observed must not be deletable.
func TestClustersDeleteRejectsAReturnedContextAheadOfTheObservation(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	observePresence(t, status, obj, false)

	// The file put the context back; the record has not re-observed it yet.
	d.kubeconfigSvc = fakeKubeconfigService{cfg: cfgWith("prod"), loaded: true}

	err := serviceOver(t, d).Clusters().Delete(context.Background(), ClusterID(obj.ID))
	require.ErrorIs(t, err, ErrDeclaredBySource)
}

// Nothing to read at all — no observation, and the file not yet read. Refusing is
// recoverable; allowing costs the record's id and the user's toggles.
func TestClustersDeleteRejectsWhenNothingCanConfirmTheContextIsGone(t *testing.T) {
	d, _ := newClusterStatusDeps(t)
	d.kubeconfigSvc = fakeKubeconfigService{}
	svc := serviceOver(t, d)
	obj := createCluster(t, d.clusterClient, "prod")

	err := svc.Clusters().Delete(context.Background(), ClusterID(obj.ID))
	require.ErrorIs(t, err, ErrDeclaredBySource)
}

// The stored observation is what answers while the file is unread.
func TestClustersDeleteFallsBackToTheObservationWhileUnread(t *testing.T) {
	d, status := newClusterStatusDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	observePresence(t, status, obj, false)
	d.kubeconfigSvc = fakeKubeconfigService{}

	require.NoError(t, serviceOver(t, d).Clusters().Delete(context.Background(), ClusterID(obj.ID)))
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

// --- ensureKubeconfigClusters ---

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

func TestEnsureCreatesOneClusterPerContext(t *testing.T) {
	d := newTestDeps(t)
	require.NoError(t, ensureKubeconfigClusters(context.Background(), d.clusterClient, cfgWith("prod", "staging")))

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
func TestEnsureClustersIsIdempotent(t *testing.T) {
	d := newTestDeps(t)
	ctx := context.Background()
	cfg := cfgWith("prod")

	require.NoError(t, ensureKubeconfigClusters(ctx, d.clusterClient, cfg))
	first := liveClusters(t, d.clusterClient)["prod"]
	require.NoError(t, ensureKubeconfigClusters(ctx, d.clusterClient, cfg))

	second := liveClusters(t, d.clusterClient)["prod"]
	require.NotNil(t, second)
	assert.Equal(t, first.ID, second.ID, "the second pass must not create a second record")
}

// The referenced set is scoped by the source discriminant, not by the name prefix: a
// record from another source names its own things and claims no kube-context.
func TestEnsureClustersIgnoresRecordsFromAnotherSource(t *testing.T) {
	d := newTestDeps(t)
	ctx := context.Background()
	_, err := d.clusterClient.Create(ctx, "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	require.NoError(t, ensureKubeconfigClusters(ctx, d.clusterClient, cfgWith("prod")))
	assert.Contains(t, liveClusters(t, d.clusterClient), "prod")
}

// A record on its way out leaves its context unclaimed and owed a fresh one, but the
// draining row still holds the name. The pass must report that rather than skip it:
// beehive's backoff is the only thing that retries once the row drains.
func TestEnsureClustersReportsANameHeldByADrainingRecord(t *testing.T) {
	d := newTestDeps(t)
	ctx := context.Background()
	obj := createCluster(t, d.clusterClient, "prod")
	require.NoError(t, d.clusterClient.Delete(ctx, obj.ID))

	err := ensureKubeconfigClusters(ctx, d.clusterClient, cfgWith("prod"))
	assert.Error(t, err, "the name is still held, so the re-import cannot land yet")
}

// stubClusterClient answers the calls the create pass makes, so a test can fail any
// of them. The embedded interface is nil: anything else it might start calling panics
// rather than passing silently.
type stubClusterClient struct {
	beehive.Client[ClusterSpec, ClusterStatus]
	objs    []*beehive.Object[ClusterSpec, ClusterStatus]
	listErr error
	create  func(name string) error
}

func (c stubClusterClient) List(context.Context, ...beehive.LoadOption) ([]*beehive.Object[ClusterSpec, ClusterStatus], error) {
	return c.objs, c.listErr
}

func (c stubClusterClient) Create(_ context.Context, name string, _ ClusterSpec, _ ...beehive.Option) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	return nil, c.create(name)
}

// A store read that fails must abort the pass. Carrying on would import against a
// record set that was never read, and every context in the file would look unclaimed.
func TestEnsureClustersReportsAFailedList(t *testing.T) {
	boom := errors.New("boom")
	err := ensureKubeconfigClusters(context.Background(), stubClusterClient{listErr: boom}, cfgWith("prod"))

	require.ErrorIs(t, err, boom)
}

func TestEnsureClustersReportsAFailedCreate(t *testing.T) {
	boom := errors.New("boom")
	stub := stubClusterClient{create: func(string) error { return boom }}
	err := ensureKubeconfigClusters(context.Background(), stub, cfgWith("prod"))

	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "prod", "the error must name the context that could not be imported")
}

// One context's failure must not cost the others their import. Contexts come out of a
// map, so stopping at the first one would leave a subset that differs run to run.
func TestEnsureClustersContinuesPastAFailedCreate(t *testing.T) {
	boom := errors.New("boom")
	var created []string
	stub := stubClusterClient{create: func(name string) error {
		if name == KubeconfigName("prod") {
			return boom
		}
		created = append(created, name)
		return nil
	}}

	err := ensureKubeconfigClusters(context.Background(), stub, cfgWith("prod", "staging", "dev"))

	require.ErrorIs(t, err, boom)
	assert.ElementsMatch(t, []string{KubeconfigName("staging"), KubeconfigName("dev")}, created)
}

// The observation reads the kubeconfig service rather than the object, so nothing
// about a file change reaches this record on its own. The edge onto the anchor is the
// whole wake path: without it a departed context stays marked present.
func TestReconcileDependsOnItsSource(t *testing.T) {
	c := controllerOver(t)
	obj := createCluster(t, c.clusterClient, "prod")
	src, err := c.sourceClient.GetByName(context.Background(), ClusterSourceNameKubeconfig)
	require.NoError(t, err)

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Contains(t, client.dependsOn, src.ID)
}

// A record from another source has no kubeconfig anchor to wake it, and claiming one
// would put it on a pass that could never say anything about it.
func TestReconcileDeclaresNoSourceEdgeForAnotherSource(t *testing.T) {
	c := controllerOver(t)
	obj, err := c.clusterClient.Create(context.Background(), "cloud/prod", ClusterSpec{})
	require.NoError(t, err)

	client := &stubControllerClient{}
	reconcileCluster(t, c, client, obj)

	assert.Empty(t, client.dependsOn, "no source anchor and no identity to depend on")
}

// A failed edge must fail the pass. Carrying on would write the observation once and
// then never be woken to write it again.
func TestReconcileReportsAFailedSourceEdge(t *testing.T) {
	c := controllerOver(t)
	obj := createCluster(t, c.clusterClient, "prod")

	boom := errors.New("boom")
	res := c.Reconcile(context.Background(), &stubControllerClient{dependErr: boom}, obj)

	assert.Equal(t, fmt.Errorf("depend cluster %d on its source: %w", obj.ID, boom), res.Err())
}

// beehive heads service.parts and dispatches its startup pass asynchronously, so a
// record stored by a previous process can reach a reconcile before the bootstrap has
// created the anchors. Failing there would drop every stored record into backoff on
// every boot and skip the observation with it, so the window is waited out instead.
func TestReconcileWaitsForAMissingSourceAnchor(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.sourceClient = &stubSourceClient{getErr: beehive.ErrNotFound}
	c := &clusterController{deps: d}

	client := &stubControllerClient{}
	res := c.Reconcile(context.Background(), client, obj)

	assert.Equal(t, beehive.Unsettled().RequeueAfter(startupRequeue), res)
	assert.Nil(t, client.updated, "nothing observed until the record can be woken again")
}

// Any other store failure is a real one: the record would otherwise write an
// observation it can never be woken to refresh.
func TestReconcileReportsAFailedSourceLookup(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	boom := errors.New("boom")
	d.sourceClient = &stubSourceClient{getErr: boom}
	c := &clusterController{deps: d}

	res := c.Reconcile(context.Background(), &stubControllerClient{}, obj)

	assert.Equal(t, fmt.Errorf("read kubeconfig cluster source: %w", boom), res.Err())
}

// observePresence writes the observation Delete gates on, the way a reconcile does.
func observePresence(t *testing.T, status *beehive.AdminClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus], present bool) {
	t.Helper()
	require.NoError(t, status.UpdateStatus(context.Background(), obj.ID, ClusterStatus{
		Source: ClusterStatusSource{Kubeconfig: &ClusterStatusSourceKubeconfig{
			Cluster: "prod-cluster", User: "prod-user", IsPresent: present,
		}},
	}))
}
