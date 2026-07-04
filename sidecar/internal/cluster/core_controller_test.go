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

package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// staticProbe returns a ProbeFunc that always yields the given server/principal.
func staticProbe(server ClusterServer, principal ClusterPrincipal) ProbeFunc {
	return func(context.Context, *rest.Config) (ClusterServer, ClusterPrincipal, error) {
		return server, principal, nil
	}
}

// errProbe returns a ProbeFunc that always fails.
func errProbe(err error) ProbeFunc {
	return func(context.Context, *rest.Config) (ClusterServer, ClusterPrincipal, error) {
		return ClusterServer{}, ClusterPrincipal{}, err
	}
}

// mutableProbe returns a successful ProbeFunc whose reported kube-system UID can be
// changed between reconciles, so a test can simulate a physical-cluster migration: the
// same kube-context now resolving to a different cluster identity. The read is
// mutex-guarded because the controller probes from its own goroutine.
func mutableProbe(initial string) (ProbeFunc, func(string)) {
	var mu sync.Mutex
	uid := initial
	probe := func(context.Context, *rest.Config) (ClusterServer, ClusterPrincipal, error) {
		mu.Lock()
		u := uid
		mu.Unlock()
		return ClusterServer{UID: &u}, ClusterPrincipal{}, nil
	}
	return probe, func(n string) { mu.Lock(); uid = n; mu.Unlock() }
}

// waitForCacheBySlug blocks until a ClusterCache with the given slug exists, then
// returns it (or fails on timeout). It is event-driven over WatchList — current-on-
// subscribe, then live deltas — so it observes a cache that exists already as well as
// one created after the subscribe (the first Added carrying the slug), with no polling.
func waitForCacheBySlug(t *testing.T, cl beehive.Client[ClusterCacheSpec, ClusterCacheStatus], slug string) *beehive.Object[ClusterCacheSpec, ClusterCacheStatus] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := cl.WatchList(ctx)
	require.NoError(t, err)
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed before ClusterCache %q appeared", slug)
			}
			if ev.Object != nil && ev.Object.Slug != nil && *ev.Object.Slug == slug {
				return ev.Object
			}
		case <-timeout:
			t.Fatalf("timed out waiting for ClusterCache %q", slug)
		}
	}
}

// staticCheck returns a CheckFunc that always yields phase.
func staticCheck(phase HealthPhase) CheckFunc {
	return func(context.Context, *rest.Config) (HealthPhase, *string) {
		return phase, nil
	}
}

// signalingProbe returns a successful ProbeFunc that signals every invocation on
// the returned channel, so a test can wait on the probe event instead of polling
// a counter. The send is non-blocking (buffered + select/default) so a slow
// reader can never stall the controller's reconcile.
func signalingProbe() (ProbeFunc, chan struct{}) {
	ch := make(chan struct{}, 16)
	probe := func(context.Context, *rest.Config) (ClusterServer, ClusterPrincipal, error) {
		select {
		case ch <- struct{}{}:
		default:
		}
		uid := "uid"
		return ClusterServer{UID: &uid}, ClusterPrincipal{}, nil
	}
	return probe, ch
}

// awaitProbe blocks until the probe fires once, or fails on timeout.
func awaitProbe(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for probe")
	}
}

// drainProbes consumes any already-buffered probe signals so a following
// awaitProbe observes only probes that fire after this point.
func drainProbes(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// newClusterTestBeehive builds a beehive with the real ClusterCoreController using
// the given probe/check fakes plus NoopControllers for the other kinds.
func newClusterTestBeehive(t *testing.T, w KubeConfigSource, probe ProbeFunc, check CheckFunc, connMgr *ConnectionManager) (beehive.Client[ClusterSpec, ClusterStatus], beehive.Client[ClusterCacheSpec, ClusterCacheStatus]) {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)

	ctrl := NewClusterCoreController(w, coreClient, cacheClient, connMgr, nil, probe, check)
	cc, err := beehive.Register(bh, ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	return coreClient, cacheClient
}

// waitCondition blocks on the object's beehive watch until it carries the named
// condition, then returns that object. beehive's Watch is current-on-subscribe
// (a snapshot Added event, then live Modified events), so this is event-driven —
// no polling.
func waitCondition(t *testing.T, cl beehive.Client[ClusterSpec, ClusterStatus], id beehive.ObjectID, condType ClusterConditionType) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	return waitForStatus(t, cl, id, func(s *ClusterStatus) bool {
		for _, c := range s.Conditions {
			if c.Type == condType {
				return true
			}
		}
		return false
	})
}

// waitForStatus blocks on the object's beehive watch until its status satisfies
// pred, then returns the object. Event-driven (current-on-subscribe), no polling.
func waitForStatus(t *testing.T, cl beehive.Client[ClusterSpec, ClusterStatus], id beehive.ObjectID, pred func(*ClusterStatus) bool) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := cl.Watch(ctx, id)
	require.NoError(t, err)
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("watch closed before status predicate met")
			}
			if ev.Object != nil && ev.Object.Status != nil && pred(ev.Object.Status) {
				return ev.Object
			}
		case <-timeout:
			t.Fatal("timed out waiting for status predicate")
		}
	}
}

// waitForObservedGeneration blocks (event-driven) until the object's Connected
// condition has been re-stamped by a reconcile that observed at least gen — i.e.
// the reconcile triggered by the generation-bumping spec edit has written status.
func waitForObservedGeneration(t *testing.T, cl beehive.Client[ClusterSpec, ClusterStatus], id beehive.ObjectID, gen int64) *beehive.Object[ClusterSpec, ClusterStatus] {
	t.Helper()
	return waitForStatus(t, cl, id, func(s *ClusterStatus) bool {
		for _, c := range s.Conditions {
			if c.Type == ClusterConditionConnected && c.ObservedGeneration >= gen {
				return true
			}
		}
		return false
	})
}

// eligibleSpec builds a Cluster spec that passes ConnectionEligible — provided
// contextName is present in the test watcher's kubeconfig, since the controller
// observes presence live from the kubeconfig.
func eligibleSpec(contextName string) ClusterSpec {
	return ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: ClusterSpecSource{
			Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName},
		},
	}
}

func TestClusterCoreControllerSuccessfulProbeWritesConditions(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	coreClient, cacheClient := newClusterTestBeehive(t, w,
		staticProbe(
			ClusterServer{UID: &uid, Version: &ver},
			ClusterPrincipal{Username: &user},
		),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	got := waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)
	require.NotNil(t, got.Status)

	connected := findCondition(t, got.Status.Conditions, ClusterConditionConnected)
	assert.Equal(t, ConditionTrue, connected.Status)
	assert.Equal(t, ReasonConnected, connected.Reason)

	healthy := findCondition(t, got.Status.Conditions, ClusterConditionHealthy)
	assert.Equal(t, ConditionTrue, healthy.Status)

	assert.NotNil(t, got.Status.LastConnectedAt)

	// ClusterCache child must have been created, keyed by the probed kube-system UID.
	cacheObj, err := cacheClient.GetBySlug(ctx, ClusterCacheSlug(id, uid))
	require.NoError(t, err, "ClusterCache child must exist after successful reconcile")
	assert.Equal(t, uid, cacheObj.Spec.ServerUID, "cache spec records the identity it mirrors")
}

// TestClusterCoreControllerUIDSwitchPrunesSupersededCache verifies the server-UID
// switch behavior: when a probe reports a new kube-system UID (the kube-context now
// points at a different physical cluster), the controller creates a cache for the new
// identity and requests deletion of the superseded one — while the Cluster's own id
// (its beehive ObjectID) stays consistent. The old cache is held in a deletion-pending
// state by its finalizer (the NoopController here never clears it), which is exactly
// what gates the cache controller's on-disk file cleanup in production.
func TestClusterCoreControllerUIDSwitchPrunesSupersededCache(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	probe, setUID := mutableProbe("uid-old")
	coreClient, cacheClient := newClusterTestBeehive(t, w, probe, staticCheck(HealthPhaseHealthy), nil)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	// First probe creates the cache for the original identity — carrying the finalizer
	// that gates its deletion on file cleanup.
	oldCache := waitForCacheBySlug(t, cacheClient, ClusterCacheSlug(id, "uid-old"))
	assert.Contains(t, oldCache.Finalizers, "kstack.io/cache-files",
		"a created cache must carry the file-cleanup finalizer")

	// Simulate the migration, then force a re-probe with a spec edit (bumps generation).
	setUID("uid-new")
	renamed := eligibleSpec("alpha")
	name := "renamed"
	renamed.Name = &name
	_, err = coreClient.Update(ctx, beehive.ObjectID(id), renamed)
	require.NoError(t, err)

	// A cache for the new identity is created...
	newCache := waitForCacheBySlug(t, cacheClient, ClusterCacheSlug(id, "uid-new"))
	assert.Equal(t, "uid-new", newCache.Spec.ServerUID)
	assert.NotEqual(t, oldCache.ID, newCache.ID, "a migration mints a fresh cache, not a reuse")

	// ...and the superseded one is requested for deletion (lingering on its finalizer).
	require.Eventually(t, func() bool {
		got, err := cacheClient.Get(ctx, oldCache.ID)
		return err == nil && got.DeletionRequestedAt != nil
	}, 2*time.Second, 10*time.Millisecond, "superseded cache must be deletion-requested")

	// The Cluster identity is unchanged across the switch.
	assert.Equal(t, beehive.ObjectID(id), obj.ID)
}

func TestClusterCoreControllerProbeFailureSetsConnectedFalse(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("connection refused")),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, ClusterConditionConnected)
	assert.Equal(t, ConditionFalse, connected.Status)
	assert.Equal(t, ReasonProbeFailed, connected.Reason)
}

func TestClusterCoreControllerIneligibleClusterDoesNotProbe(t *testing.T) {
	probeCalled := false
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	probe := func(_ context.Context, _ *rest.Config) (ClusterServer, ClusterPrincipal, error) {
		probeCalled = true
		return ClusterServer{}, ClusterPrincipal{}, nil
	}
	coreClient, _ := newClusterTestBeehive(t, w, probe, staticCheck(HealthPhaseHealthy), nil)
	ctx := context.Background()

	// Enabled=false → ineligible.
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, ClusterConditionConnected)
	assert.Equal(t, ConditionFalse, connected.Status)
	assert.Equal(t, ReasonInactive, connected.Reason)
	assert.False(t, probeCalled, "probe must not be called for ineligible cluster")
}

// A successful probe is recorded in the connection-attempt history as an OK
// attempt (the failure case is covered by TestClusterCoreControllerRecordsConnectionAttempts).
func TestClusterCoreControllerRecordsAttemptOnSuccessfulProbe(t *testing.T) {
	uid := "kube-system-uid"
	user := "alice"
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(ClusterServer{UID: &uid}, ClusterPrincipal{Username: &user}),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)
	evs, err := coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(ConnectionEventCategory))
	require.NoError(t, err)
	require.NotEmpty(t, evs, "a successful probe records an attempt")
	latest := evs[0] // ListEvents is newest-run-first
	assert.Equal(t, beehive.EventNormal, latest.Type, "a successful probe is recorded as Normal")
	assert.Equal(t, ReasonConnected, latest.Reason)
}

func TestClusterCoreControllerIneligibleClusterRecordsNoAttempt(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(ClusterServer{}, ClusterPrincipal{}),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	// Enabled=false → ineligible → no probe is attempted.
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)
	evs, err := coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(ConnectionEventCategory))
	require.NoError(t, err)
	assert.Empty(t, evs, "an ineligible cluster makes no attempt, so the history stays empty")
}

func TestClusterCoreControllerRecordsConnectionAttempts(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("connection refused")),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	var evs []beehive.Event
	require.Eventually(t, func() bool {
		evs, _ = coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(ConnectionEventCategory))
		return len(evs) >= 1
	}, 2*time.Second, 10*time.Millisecond, "a failed probe is recorded in the attempt history")

	latest := evs[0] // ListEvents is newest-run-first
	assert.Equal(t, beehive.EventWarning, latest.Type, "a failed probe is recorded as Warning")
	assert.Equal(t, ReasonProbeFailed, latest.Reason)
	assert.Equal(t, "connection refused", latest.Message)
	assert.False(t, latest.LastAt.IsZero(), "the attempt carries a timestamp")
}

func TestClusterCoreControllerSuccessfulProbePopulatesConnectionManager(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(
			ClusterServer{UID: &uid, Version: &ver},
			ClusterPrincipal{Username: &user},
		),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	obj, err := coreClient.Create(context.Background(), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.NotNil(t, got, "ConnectionManager must have a REST config after successful probe")
}

func TestClusterCoreControllerProbeFailureDeletesFromConnectionManager(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("dial failed")),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	ctx := context.Background()
	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	// Wait for the first reconcile (failed probe) to land its Connected condition.
	waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)

	// Pre-seed a stale entry, then force a fresh reconcile (a spec edit bumps
	// generation) so the probe-failure path runs again with the seed in place —
	// seeding before this second reconcile is what makes the clear observable.
	connMgr.Set(id, &rest.Config{Host: "https://stale"})
	renamed := eligibleSpec("alpha")
	name := "renamed"
	renamed.Name = &name
	updated, err := coreClient.Update(ctx, beehive.ObjectID(id), renamed)
	require.NoError(t, err)

	// teardownConnection (which clears connMgr) runs before the status write in the
	// same reconcile, so observing the second reconcile's status — its Connected
	// condition stamped with the new generation — means the clear has landed.
	waitForObservedGeneration(t, coreClient, obj.ID, updated.Generation)
	assert.Nil(t, connMgr.Get(id), "ConnectionManager must not hold a config after probe failure")
}

func TestClusterCoreControllerIneligibleDeletesFromConnectionManager(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(ClusterServer{}, ClusterPrincipal{}),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	ctx := context.Background()
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	// Wait for the first reconcile (ineligible) to land its Connected condition.
	waitCondition(t, coreClient, obj.ID, ClusterConditionConnected)

	// Pre-seed a stale entry, then force a fresh reconcile (a spec edit bumps
	// generation, staying ineligible) so the teardown path runs with the seed in place.
	connMgr.Set(id, &rest.Config{Host: "https://stale"})
	renamed := spec
	name := "renamed"
	renamed.Name = &name
	updated, err := coreClient.Update(ctx, beehive.ObjectID(id), renamed)
	require.NoError(t, err)

	// teardownConnection (which clears connMgr) runs before the status write in the
	// same reconcile, so observing the second reconcile's status — its Connected
	// condition stamped with the new generation — means the clear has landed.
	waitForObservedGeneration(t, coreClient, obj.ID, updated.Generation)
	assert.Nil(t, connMgr.Get(id), "ConnectionManager must not hold a config for an ineligible cluster")
}

func findCondition(t *testing.T, conds []ClusterCondition, typ ClusterConditionType) ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found in %v", typ, conds)
	return ClusterCondition{}
}

// TestClusterCoreControllerPokeReprobes verifies the controller subscribes to the
// poke bus and forces an immediate re-probe of every eligible cluster, rather
// than waiting for the next scheduled (30s) reconcile.
func TestClusterCoreControllerPokeReprobes(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	probe, probeCh := signalingProbe()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	ctrl := NewClusterCoreController(w, coreClient, cacheClient, nil, pk, probe, staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	_, err = coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	awaitProbe(t, probeCh)
	drainProbes(probeCh)

	// A poke forces an immediate re-probe without waiting for the 30s cadence.
	pk.Poke(poke.SourceHost)
	awaitProbe(t, probeCh)
}

// TestClusterCoreControllerReprobeOne verifies the in-process retry bus: Reprobe
// forces an immediate out-of-band re-probe of one targeted cluster, rather than
// waiting for the next scheduled (30s) reconcile.
func TestClusterCoreControllerReprobeOne(t *testing.T) {
	ctx := context.Background()

	probe, probeCh := signalingProbe()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	// pokeSvc is nil — the retry bus is in-process, independent of the poke bus.
	ctrl := NewClusterCoreController(w, coreClient, cacheClient, nil, nil, probe, staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := ClusterID(obj.ID)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	awaitProbe(t, probeCh)
	drainProbes(probeCh)

	// Reprobe forces an immediate re-probe of the targeted
	ctrl.Reprobe(id)
	awaitProbe(t, probeCh)
}

// WatchProbe streams the in-flight probe state per cluster: current-on-subscribe
// (a mid-probe subscriber sees true), then one value per transition, filtered to
// the subscribed id. This exercises the hub directly (setProbing is what converge
// calls around the network probe), so no beehive/network is needed.
func TestClusterCoreControllerWatchProbe(t *testing.T) {
	var coreClient beehive.Client[ClusterSpec, ClusterStatus]
	var cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	ctrl := NewClusterCoreController(nil, coreClient, cacheClient, nil, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const a, b = ClusterID(1), ClusterID(2)

	// Current-on-subscribe: no probe in flight → false.
	sub := ctrl.WatchProbe(ctx, a)
	assert.False(t, recv(t, sub), "idle cluster starts not-probing")

	// A probe on a starts → true; finishes → false.
	ctrl.setProbing(a, true)
	assert.True(t, recv(t, sub))
	ctrl.setProbing(a, false)
	assert.False(t, recv(t, sub))

	// Transitions for a different cluster are filtered out — a is idle, so its
	// stream must stay silent while b probes.
	ctrl.setProbing(b, true)
	select {
	case v := <-sub:
		t.Fatalf("cluster a's stream saw cluster b's transition: %v", v)
	case <-time.After(50 * time.Millisecond):
	}

	// A subscriber opening while b is mid-probe sees true immediately.
	subB := ctrl.WatchProbe(ctx, b)
	assert.True(t, recv(t, subB), "mid-probe subscriber sees checking now")

	// ctx cancel closes the stream.
	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-subB:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "stream closes on ctx cancel")
}

// The controller observes the kubeconfig live and writes it to status.Source.
// Its watcher subscription re-reconciles on
// a kubeconfig change, so a departed context flips IsPresent=false — keeping its
// last-known names — and goes Inactive.
func TestClusterCoreControllerObservesKubeconfigAndDeparture(t *testing.T) {
	ctx := context.Background()
	uid, ver, user := "u", "v1.31.0", "alice"
	w := NewMutableWatcher(testKubeConfig("alpha"))

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	cacheClient := beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](bh, ClusterCacheGroupKind)
	ctrl := NewClusterCoreController(w, coreClient, cacheClient, nil, nil,
		staticProbe(ClusterServer{UID: &uid, Version: &ver}, ClusterPrincipal{Username: &user}),
		staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	// Present: the observation is written to status from the kubeconfig.
	got := waitForStatus(t, coreClient, obj.ID, func(s *ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && s.Source.Kubeconfig.IsPresent
	})
	kc := got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster)
	assert.Equal(t, "alpha-user", kc.User)
	assert.True(t, kc.IsDefault, "alpha is the current-context")

	// Depart: the context leaves the kubeconfig → watcher wake → re-reconcile.
	w.Set(&api.Config{Contexts: map[string]*api.Context{}})

	got = waitForStatus(t, coreClient, obj.ID, func(s *ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && !s.Source.Kubeconfig.IsPresent
	})
	kc = got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster, "departed record keeps its last-known names")
	assert.Equal(t, "alpha-user", kc.User)
	assert.False(t, kc.IsDefault)
	connected := findCondition(t, got.Status.Conditions, ClusterConditionConnected)
	assert.Equal(t, ConditionFalse, connected.Status)
	assert.Equal(t, ReasonInactive, connected.Reason)
}

// Aggregation into runs and per-object retention are now beehive's (RecordEvent +
// WithEventRetention), covered by its own tests; here we only pin the local
// message-truncation helper.

func TestTruncateMessage(t *testing.T) {
	assert.Equal(t, "short", truncateMessage("short"))
	long := strings.Repeat("x", maxAttemptMessageLen+50)
	got := truncateMessage(long)
	assert.LessOrEqual(t, len(got), maxAttemptMessageLen+len("…"))
	assert.True(t, strings.HasSuffix(got, "…"))
}
