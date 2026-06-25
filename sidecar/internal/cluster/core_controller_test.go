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

package cluster_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/testutil"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// staticProbe returns a ProbeFunc that always yields the given server/principal.
func staticProbe(server cluster.ClusterServer, principal cluster.ClusterPrincipal) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		return server, principal, nil
	}
}

// errProbe returns a ProbeFunc that always fails.
func errProbe(err error) cluster.ProbeFunc {
	return func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		return cluster.ClusterServer{}, cluster.ClusterPrincipal{}, err
	}
}

// mutableProbe returns a successful ProbeFunc whose reported kube-system UID can be
// changed between reconciles, so a test can simulate a physical-cluster migration: the
// same kube-context now resolving to a different cluster identity. The read is
// mutex-guarded because the controller probes from its own goroutine.
func mutableProbe(initial string) (cluster.ProbeFunc, func(string)) {
	var mu sync.Mutex
	uid := initial
	probe := func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		mu.Lock()
		u := uid
		mu.Unlock()
		return cluster.ClusterServer{UID: &u}, cluster.ClusterPrincipal{}, nil
	}
	return probe, func(n string) { mu.Lock(); uid = n; mu.Unlock() }
}

// waitForCacheBySlug blocks until a ClusterCache with the given slug exists, then
// returns it (or fails on timeout). It is event-driven over WatchList — current-on-
// subscribe, then live deltas — so it observes a cache that exists already as well as
// one created after the subscribe (the first Added carrying the slug), with no polling.
func waitForCacheBySlug(t *testing.T, cl beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus], slug string) *beehive.Object[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus] {
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
func staticCheck(phase cluster.HealthPhase) cluster.CheckFunc {
	return func(context.Context, *rest.Config) (cluster.HealthPhase, *string) {
		return phase, nil
	}
}

// signalingProbe returns a successful ProbeFunc that signals every invocation on
// the returned channel, so a test can wait on the probe event instead of polling
// a counter. The send is non-blocking (buffered + select/default) so a slow
// reader can never stall the controller's reconcile.
func signalingProbe() (cluster.ProbeFunc, chan struct{}) {
	ch := make(chan struct{}, 16)
	probe := func(context.Context, *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		select {
		case ch <- struct{}{}:
		default:
		}
		uid := "uid"
		return cluster.ClusterServer{UID: &uid}, cluster.ClusterPrincipal{}, nil
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
func newClusterTestBeehive(t *testing.T, w cluster.KubeConfigSource, probe cluster.ProbeFunc, check cluster.CheckFunc, connMgr *cluster.ConnectionManager) (beehive.Client[cluster.ClusterSpec, cluster.ClusterStatus], beehive.Client[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]) {
	t.Helper()
	bh := testutil.NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)

	ctrl := cluster.NewClusterCoreController(w, coreClient, cacheClient, connMgr, nil, probe, check)
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
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
func waitCondition(t *testing.T, cl beehive.Client[cluster.ClusterSpec, cluster.ClusterStatus], id beehive.ObjectID, condType cluster.ClusterConditionType) *beehive.Object[cluster.ClusterSpec, cluster.ClusterStatus] {
	t.Helper()
	return waitForStatus(t, cl, id, func(s *cluster.ClusterStatus) bool {
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
func waitForStatus(t *testing.T, cl beehive.Client[cluster.ClusterSpec, cluster.ClusterStatus], id beehive.ObjectID, pred func(*cluster.ClusterStatus) bool) *beehive.Object[cluster.ClusterSpec, cluster.ClusterStatus] {
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

// eligibleSpec builds a Cluster spec that passes ConnectionEligible — provided
// contextName is present in the test watcher's kubeconfig, since the controller
// now observes presence live from the kubeconfig (it is no longer parked in spec).
func eligibleSpec(contextName string) cluster.ClusterSpec {
	return cluster.ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: cluster.ClusterSpecSource{
			Kubeconfig: &cluster.ClusterSpecSourceKubeconfig{Context: contextName},
		},
	}
}

func TestClusterCoreControllerSuccessfulProbeWritesConditions(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	coreClient, cacheClient := newClusterTestBeehive(t, w,
		staticProbe(
			cluster.ClusterServer{UID: &uid, Version: &ver},
			cluster.ClusterPrincipal{Username: &user},
		),
		staticCheck(cluster.HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	require.NotNil(t, got.Status)

	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionTrue, connected.Status)
	assert.Equal(t, cluster.ReasonConnected, connected.Reason)

	healthy := findCondition(t, got.Status.Conditions, cluster.ClusterConditionHealthy)
	assert.Equal(t, cluster.ConditionTrue, healthy.Status)

	assert.NotNil(t, got.Status.LastConnectedAt)

	// ClusterCache child must have been created, keyed by the probed kube-system UID.
	cacheObj, err := cacheClient.GetBySlug(ctx, cluster.ClusterCacheSlug(id, uid))
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
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	probe, setUID := mutableProbe("uid-old")
	coreClient, cacheClient := newClusterTestBeehive(t, w, probe, staticCheck(cluster.HealthPhaseHealthy), nil)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)

	// First probe creates the cache for the original identity — carrying the finalizer
	// that gates its deletion on file cleanup.
	oldCache := waitForCacheBySlug(t, cacheClient, cluster.ClusterCacheSlug(id, "uid-old"))
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
	newCache := waitForCacheBySlug(t, cacheClient, cluster.ClusterCacheSlug(id, "uid-new"))
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
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("connection refused")),
		staticCheck(cluster.HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionFalse, connected.Status)
	assert.Equal(t, cluster.ReasonProbeFailed, connected.Reason)
}

func TestClusterCoreControllerIneligibleClusterDoesNotProbe(t *testing.T) {
	probeCalled := false
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	probe := func(_ context.Context, _ *rest.Config) (cluster.ClusterServer, cluster.ClusterPrincipal, error) {
		probeCalled = true
		return cluster.ClusterServer{}, cluster.ClusterPrincipal{}, nil
	}
	coreClient, _ := newClusterTestBeehive(t, w, probe, staticCheck(cluster.HealthPhaseHealthy), nil)
	ctx := context.Background()

	// Enabled=false → ineligible.
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, spec)
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)
	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionFalse, connected.Status)
	assert.Equal(t, cluster.ReasonInactive, connected.Reason)
	assert.False(t, probeCalled, "probe must not be called for ineligible cluster")
}

func TestClusterCoreControllerSuccessfulProbePopulatesConnectionManager(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(
			cluster.ClusterServer{UID: &uid, Version: &ver},
			cluster.ClusterPrincipal{Username: &user},
		),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	obj, err := coreClient.Create(context.Background(), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.NotNil(t, got, "ConnectionManager must have a REST config after successful probe")
}

func TestClusterCoreControllerProbeFailureDeletesFromConnectionManager(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("dial failed")),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	obj, err := coreClient.Create(context.Background(), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)
	// Pre-seed a stale entry so we can confirm it gets cleared.
	connMgr.Set(id, &rest.Config{Host: "https://stale"})

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.Nil(t, got, "ConnectionManager must not hold a config after probe failure")
}

func TestClusterCoreControllerIneligibleDeletesFromConnectionManager(t *testing.T) {
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))
	connMgr := cluster.NewConnectionManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(cluster.ClusterServer{}, cluster.ClusterPrincipal{}),
		staticCheck(cluster.HealthPhaseHealthy),
		connMgr,
	)

	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(context.Background(), spec)
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)
	connMgr.Set(id, &rest.Config{Host: "https://stale"})

	waitCondition(t, coreClient, obj.ID, cluster.ClusterConditionConnected)

	got := connMgr.Get(id)
	assert.Nil(t, got, "ConnectionManager must not hold a config for an ineligible cluster")
}

func findCondition(t *testing.T, conds []cluster.ClusterCondition, typ cluster.ClusterConditionType) cluster.ClusterCondition {
	t.Helper()
	for _, c := range conds {
		if c.Type == typ {
			return c
		}
	}
	t.Fatalf("condition %s not found in %v", typ, conds)
	return cluster.ClusterCondition{}
}

// TestClusterCoreControllerPokeReprobes verifies the controller subscribes to the
// poke bus and forces an immediate re-probe of every eligible cluster, rather
// than waiting for the next scheduled (30s) reconcile.
func TestClusterCoreControllerPokeReprobes(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	probe, probeCh := signalingProbe()

	bh := testutil.NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	ctrl := cluster.NewClusterCoreController(w, coreClient, cacheClient, nil, pk, probe, staticCheck(cluster.HealthPhaseHealthy))
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
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

	bh := testutil.NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)
	w := testutil.NewStaticWatcher(t, testutil.TestKubeConfig("alpha"))

	// pokeSvc is nil — the retry bus is in-process, independent of the poke bus.
	ctrl := cluster.NewClusterCoreController(w, coreClient, cacheClient, nil, nil, probe, staticCheck(cluster.HealthPhaseHealthy))
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)
	id := cluster.ClusterID(obj.ID)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	awaitProbe(t, probeCh)
	drainProbes(probeCh)

	// Reprobe forces an immediate re-probe of the targeted cluster.
	ctrl.Reprobe(id)
	awaitProbe(t, probeCh)
}

// The controller observes the kubeconfig live and writes it to status.Source (the
// importer no longer parks it in spec). Its watcher subscription re-reconciles on
// a kubeconfig change, so a departed context flips IsPresent=false — keeping its
// last-known names — and goes Inactive.
func TestClusterCoreControllerObservesKubeconfigAndDeparture(t *testing.T) {
	ctx := context.Background()
	uid, ver, user := "u", "v1.31.0", "alice"
	w := testutil.NewMutableWatcher(testutil.TestKubeConfig("alpha"))

	bh := testutil.NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[cluster.ClusterSpec, cluster.ClusterStatus](bh, cluster.ClusterGroupKind)
	cacheClient := beehive.NewClient[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus](bh, cluster.ClusterCacheGroupKind)
	ctrl := cluster.NewClusterCoreController(w, coreClient, cacheClient, nil, nil,
		staticProbe(cluster.ClusterServer{UID: &uid, Version: &ver}, cluster.ClusterPrincipal{Username: &user}),
		staticCheck(cluster.HealthPhaseHealthy))
	cc, err := beehive.Register(bh, cluster.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, cluster.ClusterCacheGroupKind, &testutil.NoopController[cluster.ClusterCacheSpec, cluster.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, eligibleSpec("alpha"))
	require.NoError(t, err)

	// Present: the observation is written to status from the kubeconfig.
	got := waitForStatus(t, coreClient, obj.ID, func(s *cluster.ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && s.Source.Kubeconfig.IsPresent
	})
	kc := got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster)
	assert.Equal(t, "alpha-user", kc.User)
	assert.True(t, kc.IsDefault, "alpha is the current-context")

	// Depart: the context leaves the kubeconfig → watcher wake → re-reconcile.
	w.Set(&api.Config{Contexts: map[string]*api.Context{}})

	got = waitForStatus(t, coreClient, obj.ID, func(s *cluster.ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && !s.Source.Kubeconfig.IsPresent
	})
	kc = got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster, "departed record keeps its last-known names")
	assert.Equal(t, "alpha-user", kc.User)
	assert.False(t, kc.IsDefault)
	connected := findCondition(t, got.Status.Conditions, cluster.ClusterConditionConnected)
	assert.Equal(t, cluster.ConditionFalse, connected.Status)
	assert.Equal(t, cluster.ReasonInactive, connected.Reason)
}
