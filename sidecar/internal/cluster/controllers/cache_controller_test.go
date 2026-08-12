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

package controllers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// These tests cover the ClusterCacheController's own responsibilities: eligibility +
// active-identity gating (which Synced condition it writes) and the deletion/finalizer
// teardown. The actual sync — workers, per-GVR status, the cache-level rollup — is the
// ClusterCacheGVRSyncController's job and is exercised by its own tests as those phases
// land; here it's a real controller whose EnsureCache/RemoveCache are no-ops, so converge
// exercises the delegation without a fake.

// newCacheTestBeehive builds a beehive with the real ClusterCacheController wired to a real
// (stub) ClusterCacheGVRSyncController, plus a presenceController stamping the parent
// Cluster. Returns the Cluster and ClusterCache clients.
func newCacheTestBeehive(t *testing.T) (
	beehive.Client[domain.ClusterSpec, domain.ClusterStatus],
	beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus],
	beehive.Client[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
	beehive.Client[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus],
) {
	t.Helper()
	coreClient, cacheClient, gvrSyncClient, discoveryClient, _ := newCacheTestBeehivePresence(t)
	return coreClient, cacheClient, gvrSyncClient, discoveryClient
}

// newCacheTestBeehivePresence is newCacheTestBeehive plus the presenceController itself, for
// the tests that need to move the parent's identity (a migration) mid-run.
func newCacheTestBeehivePresence(t *testing.T) (
	beehive.Client[domain.ClusterSpec, domain.ClusterStatus],
	beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus],
	beehive.Client[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
	beehive.Client[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus],
	*presenceController,
) {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	cacheClient := beehive.NewClient[domain.ClusterCacheSpec, domain.ClusterCacheStatus](bh, domain.ClusterCacheGroupKind)
	gvrSyncClient := beehive.NewClient[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus](bh, domain.ClusterCacheGVRSyncGroupKind)
	discoveryClient := beehive.NewClient[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus](bh, domain.ClusterCacheGVRDiscoveryGroupKind)

	// The pools must close before TempDir's RemoveAll: on Windows an open file can't
	// be unlinked, so a leaked cache handle fails the cleanup.
	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	rt := &controllerRuntime{bh: bh, cacheManager: mgr, connMgr: connections.NewManager()}

	ctrl := NewClusterCacheController(rt)

	presence := &presenceController{}
	_, err := beehive.Register(bh, domain.ClusterGroupKind, presence)
	require.NoError(t, err)
	_, err = beehive.Register(bh, domain.ClusterCacheGroupKind, ctrl)
	require.NoError(t, err)
	// The GVR-discovery child is the cache's sync anchor. Its real controller is registered
	// (with no credentials in the connection manager it only ever reports NoConnection,
	// touching no network) so the child is reconciled and collected on delete — an
	// unregistered kind would linger and wedge the cache's deletion barrier.
	_, err = beehive.Register(bh, domain.ClusterCacheGVRDiscoveryGroupKind, NewClusterCacheGVRDiscoveryController(rt))
	require.NoError(t, err)
	// The per-kind sync children carry a drain finalizer, so their controller must be
	// registered for one to ever be collected. The cache seeds the Event child directly
	// (see ensureEventsSync), so one exists here even with no discovery pass.
	gvrSyncCtrl := NewClusterCacheGVRSyncController(rt)
	gvrSyncCC, err := beehive.Register(bh, domain.ClusterCacheGVRSyncGroupKind, gvrSyncCtrl)
	require.NoError(t, err)
	gvrSyncCtrl.SetControllerClient(gvrSyncCC)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return coreClient, cacheClient, gvrSyncClient, discoveryClient, presence
}

// awaitCacheSyncedReason blocks on the ClusterCache object's beehive watch until its Synced
// condition reaches want (matching on Reason, since every current outcome is
// ConditionFalse), then returns that condition. Matching on Reason — not Status — is what
// distinguishes a transient Paused (parent not yet stamped active) from the settled Syncing
// an eligible+active cache lands on.
func awaitCacheSyncedReason(t *testing.T, cl beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus], id beehive.ObjectID, want string) domain.Condition {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snap, ch, err := cl.Watch(ctx, id)
	require.NoError(t, err)
	if snap.Object != nil {
		if c := findCacheConditionOK(snap.Object.Conditions, domain.ConditionSynced); c != nil && c.Reason == want {
			return *c
		}
	}

	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed before Synced reason=%s on ClusterCache", want)
			}
			if ev.Object == nil {
				continue
			}
			if c := findCacheConditionOK(ev.Object.Conditions, domain.ConditionSynced); c != nil && c.Reason == want {
				return *c
			}
		case <-timeout:
			t.Fatalf("timed out waiting for Synced reason=%s on ClusterCache", want)
		}
	}
}

func eligibleClusterSpec(contextName string) domain.ClusterSpec {
	return domain.ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: domain.ClusterSpecSource{
			Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: contextName},
		},
	}
}

// testCacheUID is the kube-system UID the cache tests' parent Cluster reports as its
// connected identity. A ClusterCache is active (and syncs) only when its spec UID matches
// the parent's Status.Server.UID, so the tests create their cache with this UID and
// presenceController stamps it.
const testCacheUID = "kube-system-uid"

// presenceController is the test stand-in for the Cluster kind's controller. The cache
// controller gates on the parent's observed presence (Source.Kubeconfig.IsPresent) AND on
// the cache being the active identity (its UID == Server.UID) — both written by the real
// ClusterCoreController after a probe. This minimal controller stamps both (its status
// write wakes the ClusterCache dependent, exercising the real trigger path) without the
// probing machinery.
type presenceController struct {
	// uid overrides the kube-system UID stamped on the parent. Unset means testCacheUID.
	// Swapping it mid-test is a physical-cluster migration — the one thing that flips an
	// existing cache from active to inactive while the cluster stays sync-eligible.
	uid atomic.Pointer[string]
	// absent stamps IsPresent=false — the kubeconfig context momentarily gone, as a
	// rewrite of ~/.kube/config by kubectx or a cloud CLI produces.
	absent atomic.Bool
}

// setUID makes the controller stamp a different identity from the next reconcile on. The
// caller must still wake the Cluster (e.g. a spec write); changing the field alone doesn't.
func (p *presenceController) setUID(uid string) { p.uid.Store(&uid) }

// setPresent makes the controller stamp the context as present or gone from the next
// reconcile on. Like setUID, the caller must still wake the Cluster.
func (p *presenceController) setPresent(present bool) { p.absent.Store(!present) }

func (p *presenceController) serverUID() string {
	if v := p.uid.Load(); v != nil {
		return *v
	}
	return testCacheUID
}

func (p *presenceController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[domain.ClusterStatus],
	obj *beehive.Object[domain.ClusterSpec, domain.ClusterStatus],
) (beehive.Result, error) {
	kc := obj.Spec.Source.Kubeconfig
	if kc == nil {
		return beehive.Result{}, nil
	}
	wantSrc := domain.ClusterStatusSourceKubeconfig{
		Cluster:   kc.Context + "-cluster",
		User:      kc.Context + "-user",
		IsPresent: !p.absent.Load(),
	}
	uid := p.serverUID()
	if obj.Status != nil && obj.Status.Source.Kubeconfig != nil &&
		*obj.Status.Source.Kubeconfig == wantSrc &&
		obj.Status.Server.UID != nil && *obj.Status.Server.UID == uid {
		return beehive.Result{}, nil // already stamped: no rewrite
	}
	status := domain.ClusterStatus{
		Source: domain.ClusterStatusSource{Kubeconfig: &wantSrc},
		Server: domain.ClusterServer{UID: &uid},
	}
	return beehive.Result{}, client.UpdateStatus(ctx, obj.ID, obj.Generation, status)
}

// TestCacheControllerEligibleActiveCacheSyncs verifies that an eligible + active cache
// converges to Synced=False/Syncing — the placeholder condition converge writes after
// delegating to EnsureCache (until the P4 rollup replaces it with the real summary).
func TestCacheControllerEligibleActiveCacheSyncs(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)
	assert.Equal(t, domain.ConditionFalse, synced.Status)
}

// awaitEventsSyncEnabled polls the cache's Event sync child by its deterministic name until
// it exists with Spec.Enabled == want, then returns it. Polling on the spec (not mere
// existence) is the point: the child is created unconditionally and outlives every pause, so
// a pause is observable only as the spec flipping. Any read error other than NotFound fails
// the test.
//
// Events are an ordinary per-kind sync child, seeded by the cache controller so they don't
// wait on a discovery pass — hence the name is keyed on the discovery anchor, not the cache.
func awaitEventsSyncEnabled(
	t *testing.T,
	cl beehive.Client[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus],
	dc beehive.Client[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus],
	cacheID beehive.ObjectID,
	want bool,
) *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus] {
	t.Helper()
	ctx := context.Background()
	name := ""
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		// The anchor is created in the same pass that seeds the child, so resolve it here
		// rather than up front — on the first tick it may not exist yet.
		if name == "" {
			anchor, err := dc.GetByName(ctx, domain.ClusterCacheGVRDiscoveryName(cacheID))
			if err != nil && !errors.Is(err, beehive.ErrNotFound) {
				require.NoError(t, err)
			}
			if err == nil {
				name = domain.ClusterCacheGVRSyncName(anchor.ID, domain.EventsAPIVersion, domain.EventsResource)
			}
		}
		if name != "" {
			obj, err := cl.GetByName(ctx, name)
			if err != nil && !errors.Is(err, beehive.ErrNotFound) {
				require.NoError(t, err)
			}
			if err == nil && obj.Spec.Enabled == want {
				return obj
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for Event sync child enabled=%v (cache %d)", want, cacheID)
		case <-tick.C:
		}
	}
}

// TestCacheControllerEligibleActiveCreatesEventsSyncChild verifies that an eligible + active
// cache gets an enabled Event sync child straight away, owned by the discovery anchor like
// every other per-kind child.
//
// The cache seeds this one itself rather than waiting for a discovery pass: events are the
// highest-value diagnostic data in the cache, and a pass has to reach the API server before
// it can report a single kind.
func TestCacheControllerEligibleActiveCreatesEventsSyncChild(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, gvrSyncClient, discoveryClient := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	child := awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, true)
	owner, ok, err := gvrSyncClient.GetOwner(ctx, child.ID)
	require.NoError(t, err)
	require.True(t, ok)
	anchor, err := discoveryClient.GetByName(ctx, domain.ClusterCacheGVRDiscoveryName(cacheObj.ID))
	require.NoError(t, err)
	assert.Equal(t, anchor.ID, owner.ID, "per-kind children hang off the discovery anchor")
}

// TestCacheControllerPauseDisablesEventsSyncChild verifies that pausing sync (SyncEnabled
// =false) flips the child's Spec.Enabled instead of deleting it — the child (and the status
// and event history it will carry) survives the pause, and the worker stops because its own
// controller obeys the spec.
func TestCacheControllerPauseDisablesEventsSyncChild(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, gvrSyncClient, discoveryClient := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	created := awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, true)

	// Pause sync — the child must be disabled, not removed.
	pausedSpec := eligibleClusterSpec("alpha")
	pausedSpec.SyncEnabled = false
	_, err = coreClient.Update(ctx, clusterObj.ID, pausedSpec)
	require.NoError(t, err)

	paused := awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, false)
	// Same incarnation, not a delete-and-recreate.
	assert.Equal(t, created.ID, paused.ID)

	// Unpause — the same object comes back enabled (no GC name-release wait).
	_, err = coreClient.Update(ctx, clusterObj.ID, eligibleClusterSpec("alpha"))
	require.NoError(t, err)

	resumed := awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, true)
	assert.Equal(t, created.ID, resumed.ID)
}

// TestCacheControllerIneligibleCreatesDisabledEventsSyncChild verifies the child is created
// even for a cache that has never been eligible to sync — existence is unconditional, so the
// anchor is there to carry status from the start rather than appearing on first sync.
func TestCacheControllerIneligibleCreatesDisabledEventsSyncChild(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, gvrSyncClient, discoveryClient := newCacheTestBeehive(t)

	spec := eligibleClusterSpec("alpha")
	spec.SyncEnabled = false
	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, false)
}

// TestCacheControllerDeletionCascadesToEventsSyncChild verifies the one removal path: deleting
// the cache GC-cascades to the child. The cache holds a live DependsOn edge to it (for the
// coming health rollup), so this also pins down that the edge can't wedge the collection.
func TestCacheControllerDeletionCascadesToEventsSyncChild(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, gvrSyncClient, discoveryClient := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID),
		beehive.WithFinalizers("kstack.io/cache-files"))
	require.NoError(t, err)

	child := awaitEventsSyncEnabled(t, gvrSyncClient, discoveryClient, cacheObj.ID, true)

	require.NoError(t, cacheClient.Delete(ctx, cacheObj.ID))

	require.Eventually(t, func() bool {
		_, err := gvrSyncClient.Get(ctx, child.ID)
		return errors.Is(err, beehive.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "the Event sync child must be GC'd with its owning cache")
}

// syncStoppedRuns returns the SyncStopped runs on a cache's own timeline — the timeline the
// sync-detail panel subscribes to (clusterCacheEventsWatch).
func syncStoppedRuns(t *testing.T, cl beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus], id beehive.ObjectID) []beehive.Event {
	t.Helper()
	runs, err := cl.ListEvents(context.Background(), id, beehive.WithEventCategory(domain.SyncEventCategory))
	require.NoError(t, err)
	var out []beehive.Event
	for _, r := range runs {
		if r.Reason == domain.ReasonSyncStopped {
			out = append(out, r)
		}
	}
	return out
}

// TestCacheControllerPauseRecordsSyncStopped verifies a user-facing pause leaves a mark on the
// cache's timeline. Without it the log just ends at the last sync event, which reads the same
// as a healthy cluster that went quiet.
func TestCacheControllerPauseRecordsSyncStopped(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	// Must reach the running state first — SyncStopped marks a transition, not a state.
	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)
	require.Empty(t, syncStoppedRuns(t, cacheClient, cacheObj.ID))

	pausedSpec := eligibleClusterSpec("alpha")
	pausedSpec.SyncEnabled = false
	_, err = coreClient.Update(ctx, clusterObj.ID, pausedSpec)
	require.NoError(t, err)

	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)

	runs := syncStoppedRuns(t, cacheClient, cacheObj.ID)
	require.Len(t, runs, 1)
	assert.Equal(t, beehive.EventNormal, runs[0].Type)
	// One run, count 1: the cache keeps re-reconciling while paused, and the condition-read
	// transition guard is what stops each of those passes from re-recording.
	assert.Equal(t, 1, runs[0].Count)
}

// TestCacheControllerNeverSyncedRecordsNoSyncStopped verifies the transition guard's other
// half: a cache that was never syncing has nothing to stop, so an ineligible cluster that
// settles straight into Paused records no event.
func TestCacheControllerNeverSyncedRecordsNoSyncStopped(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	spec := eligibleClusterSpec("alpha")
	spec.SyncEnabled = false
	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)
	assert.Empty(t, syncStoppedRuns(t, cacheClient, cacheObj.ID))
}

// TestCacheControllerMigrationRecordsNoSyncStopped verifies the distinction this event exists
// to make: a cache stopped because the physical cluster moved on (a migration left it behind)
// is an internal hand-over, not something the user did, so it records nothing — even though it
// really was running and really did stop. The cluster stays sync-eligible throughout, which is
// exactly what separates this from a pause.
func TestCacheControllerMigrationRecordsNoSyncStopped(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _, presence := newCacheTestBeehivePresence(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)

	// The cluster now answers with a different kube-system UID: same context, different
	// physical cluster. Our cache is no longer the active identity. The spec write is only
	// there to wake the Cluster so presenceController re-stamps.
	presence.setUID("migrated-uid")
	renamed := eligibleClusterSpec("alpha")
	newName := "alpha-renamed"
	renamed.Name = &newName
	_, err = coreClient.Update(ctx, clusterObj.ID, renamed)
	require.NoError(t, err)

	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)
	assert.Empty(t, syncStoppedRuns(t, cacheClient, cacheObj.ID))
}

// A kubeconfig rewrite is not a pause. kubectx and the cloud CLIs replace ~/.kube/config
// rather than editing it, so a context briefly disappears and comes back — and the SyncStopped
// guard, keyed on full sync eligibility, read that as the user pausing sync and appended a
// run to the cache's timeline on every round trip. The event is about the two spec switches,
// which nothing but the user touches.
func TestCacheControllerKubeconfigBlipRecordsNoSyncStopped(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _, presence := newCacheTestBeehivePresence(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)
	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)

	// The context vanishes. The spec write is only there to wake the Cluster so the
	// presence controller re-stamps; the user's switches are untouched throughout.
	presence.setPresent(false)
	wake := eligibleClusterSpec("alpha")
	gone := "alpha-gone"
	wake.Name = &gone
	_, err = coreClient.Update(ctx, clusterObj.ID, wake)
	require.NoError(t, err)
	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)

	// And comes back one write later.
	presence.setPresent(true)
	back := eligibleClusterSpec("alpha")
	backName := "alpha-back"
	back.Name = &backName
	_, err = coreClient.Update(ctx, clusterObj.ID, back)
	require.NoError(t, err)
	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)

	assert.Empty(t, syncStoppedRuns(t, cacheClient, cacheObj.ID),
		"nobody paused anything; the kubeconfig was just rewritten")
}

// TestCacheControllerIneligibleClusterPaused verifies that a sync-ineligible cluster
// (SyncEnabled=false) converges to Synced=False/Paused.
func TestCacheControllerIneligibleClusterPaused(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	spec := eligibleClusterSpec("alpha")
	spec.SyncEnabled = false
	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)
	assert.Equal(t, domain.ConditionFalse, synced.Status)
}

// TestCacheControllerInactiveCachePaused verifies that a cache whose ServerUID does NOT
// match the parent's connected identity (a migration left-behind) is paused, even though
// the cluster is otherwise sync-eligible — so its workers never write the new cluster's
// data into a stale identity's file.
func TestCacheControllerInactiveCachePaused(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	// Cache for a *different* identity than the one presenceController stamps.
	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, "stale-uid"), domain.ClusterCacheSpec{ServerUID: "stale-uid"},
		beehive.WithOwner(clusterObj.ID))
	require.NoError(t, err)

	synced := awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonPaused)
	assert.Equal(t, domain.ConditionFalse, synced.Status)
}

// TestCacheControllerDeletionClearsFinalizerAndFiles verifies the cache teardown path:
// deleting a ClusterCache that carries the file-cleanup finalizer flushes the on-disk file
// and clears the finalizer so GC collects the row (without which the deletion-pending row
// would linger forever).
func TestCacheControllerDeletionClearsFinalizerAndFiles(t *testing.T) {
	ctx := context.Background()
	coreClient, cacheClient, _, _ := newCacheTestBeehive(t)

	clusterObj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleClusterSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(clusterObj.ID)

	// Create the cache as ensureClusterCache does: owned, named, and carrying the
	// file-cleanup finalizer (the literal must match the package's cacheFilesFinalizer).
	cacheObj, err := cacheClient.Create(ctx, domain.ClusterCacheName(id, testCacheUID), domain.ClusterCacheSpec{ServerUID: testCacheUID},
		beehive.WithOwner(clusterObj.ID),
		beehive.WithFinalizers("kstack.io/cache-files"))
	require.NoError(t, err)

	// Let it converge first so the object is fully live.
	awaitCacheSyncedReason(t, cacheClient, cacheObj.ID, domain.ReasonSyncing)

	// Delete → controller flushes files + clears the finalizer → GC removes the row.
	require.NoError(t, cacheClient.Delete(ctx, cacheObj.ID))

	require.Eventually(t, func() bool {
		_, err := cacheClient.Get(ctx, cacheObj.ID)
		return errors.Is(err, beehive.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "cache row must be GC'd once its finalizer is cleared")
}

func findCacheConditionOK(conds []domain.Condition, typ domain.ConditionType) *domain.Condition {
	return domain.FindCondition(conds, typ)
}
