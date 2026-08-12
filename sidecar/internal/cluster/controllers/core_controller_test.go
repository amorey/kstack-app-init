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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// staticProbe returns a ProbeFunc that always yields the given server/principal.
func staticProbe(server domain.ClusterServer, principal domain.ClusterPrincipal) ProbeFunc {
	return func(context.Context, *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		return server, principal, nil
	}
}

// errProbe returns a ProbeFunc that always fails.
func errProbe(err error) ProbeFunc {
	return func(context.Context, *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		return domain.ClusterServer{}, domain.ClusterPrincipal{}, err
	}
}

// mutableProbe returns a successful ProbeFunc whose reported kube-system UID can change
// between reconciles, so a test can simulate a migration (the same kube-context now
// resolving to a different identity). The read is mutex-guarded because the controller
// probes from its own goroutine.
func mutableProbe(initial string) (ProbeFunc, func(string)) {
	var mu sync.Mutex
	uid := initial
	probe := func(context.Context, *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		mu.Lock()
		u := uid
		mu.Unlock()
		return domain.ClusterServer{UID: &u}, domain.ClusterPrincipal{}, nil
	}
	return probe, func(n string) { mu.Lock(); uid = n; mu.Unlock() }
}

// waitForCacheByName blocks until a ClusterCache with the given name exists, then
// returns it (or fails on timeout). Event-driven over WatchList — the snapshot covers a
// cache that already exists, the stream one created after the subscribe — so no polling.
func waitForCacheByName(t *testing.T, cl beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus], name string) *beehive.Object[domain.ClusterCacheSpec, domain.ClusterCacheStatus] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snap, ch, err := cl.WatchList(ctx)
	require.NoError(t, err)
	for _, obj := range snap.Objects {
		if obj.Name == name {
			return obj
		}
	}
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed before ClusterCache %q appeared", name)
			}
			if ev.Object != nil && ev.Object.Name == name {
				return ev.Object
			}
		case <-timeout:
			t.Fatalf("timed out waiting for ClusterCache %q", name)
		}
	}
}

// staticCheck returns a CheckFunc that always yields phase.
func staticCheck(phase HealthPhase) CheckFunc {
	return func(context.Context, *rest.Config) (HealthPhase, *string) {
		return phase, nil
	}
}

// A /readyz?verbose=true listing runs to kilobytes, and a raw client-go error is
// unbounded too. Both were being written verbatim into a condition Message — persisted,
// and re-serialized to every clustersWatch subscriber on each cluster frame — while the
// same text on the event surface was capped at 200 bytes. Every condition this package
// builds is now capped at its constructor.
func TestClusterCoreControllerCapsConditionMessages(t *testing.T) {
	verbose := strings.Repeat("[+]etcd ok\n", 300)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	uid, ver := "kube-system-uid", "v1.31.0"
	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(domain.ClusterServer{UID: &uid, Version: &ver}, domain.ClusterPrincipal{}),
		func(context.Context, *rest.Config) (HealthPhase, *string) {
			return HealthPhaseDegraded, &verbose
		},
		nil,
	)

	obj, err := coreClient.Create(context.Background(), domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, domain.ConditionHealthy)
	healthy := findCondition(t, got.Conditions, domain.ConditionHealthy)
	require.Equal(t, domain.ReasonReadyzFailed, healthy.Reason)
	assert.LessOrEqual(t, len(healthy.Message), domain.MaxMessageLen+len("…"),
		"an unbounded probe body must not be persisted and re-sent on every frame")
	assert.NotEmpty(t, healthy.Message, "the reading is still shown, just bounded")
}

// signalingProbe returns a successful ProbeFunc announcing every invocation on the
// returned probe, so a test can wait on the probe event instead of polling.
func signalingProbe() (ProbeFunc, *testutil.Probe[struct{}]) {
	fired := testutil.NewProbe[struct{}](16)
	probe := func(context.Context, *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		fired.Fire(struct{}{})
		uid := "uid"
		return domain.ClusterServer{UID: &uid}, domain.ClusterPrincipal{}, nil
	}
	return probe, fired
}

// newClusterTestBeehive builds a beehive with the real ClusterCoreController using
// the given probe/check fakes plus NoopControllers for the other kinds.
func newClusterTestBeehive(t *testing.T, w KubeConfigSource, probe ProbeFunc, check CheckFunc, connMgr *connections.Manager) (beehive.Client[domain.ClusterSpec, domain.ClusterStatus], beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus]) {
	t.Helper()
	bh := NewTestBeehiveUnstarted(t)

	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	cacheClient := beehive.NewClient[domain.ClusterCacheSpec, domain.ClusterCacheStatus](bh, domain.ClusterCacheGroupKind)

	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh, connMgr: connMgr}, w, probe, check)
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, domain.ClusterCacheGroupKind, &NoopController[domain.ClusterCacheSpec, domain.ClusterCacheStatus]{})
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
func waitCondition(t *testing.T, cl beehive.Client[domain.ClusterSpec, domain.ClusterStatus], id beehive.ObjectID, condType domain.ConditionType) *beehive.Object[domain.ClusterSpec, domain.ClusterStatus] {
	t.Helper()
	return waitForObject(t, cl, id, func(o *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) bool {
		return domain.FindCondition(o.Conditions, condType) != nil
	})
}

// waitForStatus blocks on the object's beehive watch until its status satisfies
// pred, then returns the object. Event-driven — the watch's snapshot carries current
// state and the stream the changes above it — so no polling.
func waitForStatus(t *testing.T, cl beehive.Client[domain.ClusterSpec, domain.ClusterStatus], id beehive.ObjectID, pred func(*domain.ClusterStatus) bool) *beehive.Object[domain.ClusterSpec, domain.ClusterStatus] {
	t.Helper()
	return waitForObject(t, cl, id, func(o *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) bool {
		return o.Status != nil && pred(o.Status)
	})
}

// waitForObject is waitForStatus's primitive: it blocks on the object's beehive watch
// until the whole object satisfies pred. Conditions live on the object rather than in
// status, so a condition predicate needs this form.
func waitForObject(t *testing.T, cl beehive.Client[domain.ClusterSpec, domain.ClusterStatus], id beehive.ObjectID, pred func(*beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) bool) *beehive.Object[domain.ClusterSpec, domain.ClusterStatus] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snap, ch, err := cl.Watch(ctx, id)
	require.NoError(t, err)
	if snap.Object != nil && pred(snap.Object) {
		return snap.Object
	}
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatal("watch closed before object predicate met")
			}
			if ev.Object != nil && pred(ev.Object) {
				return ev.Object
			}
		case <-timeout:
			t.Fatal("timed out waiting for object predicate")
		}
	}
}

// waitForObservedGeneration blocks (event-driven) until a reconcile that observed at
// least gen has settled — i.e. the pass triggered by the generation-bumping spec edit
// has landed. It reads beehive's own handshake (Object.ObservedGeneration) rather than a
// per-condition stamp, so it holds whether that pass reported status, conditions, or
// only the handshake itself.
func waitForObservedGeneration(t *testing.T, cl beehive.Client[domain.ClusterSpec, domain.ClusterStatus], id beehive.ObjectID, gen int64) *beehive.Object[domain.ClusterSpec, domain.ClusterStatus] {
	t.Helper()
	return waitForObject(t, cl, id, func(o *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) bool {
		return o.ObservedGeneration != nil && *o.ObservedGeneration >= gen
	})
}

// eligibleSpec builds a Cluster spec that passes ConnectionEligible — provided
// contextName is present in the test watcher's kubeconfig, since the controller
// observes presence live from the kubeconfig.
func eligibleSpec(contextName string) domain.ClusterSpec {
	return domain.ClusterSpec{
		Enabled:     true,
		SyncEnabled: true,
		Source: domain.ClusterSpecSource{
			Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: contextName},
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
			domain.ClusterServer{UID: &uid, Version: &ver},
			domain.ClusterPrincipal{Username: &user},
		),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	got := waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)
	require.NotNil(t, got.Status)

	connected := findCondition(t, got.Conditions, domain.ConditionConnected)
	assert.Equal(t, domain.ConditionTrue, connected.Status)
	assert.Equal(t, domain.ReasonConnected, connected.Reason)

	healthy := findCondition(t, got.Conditions, domain.ConditionHealthy)
	assert.Equal(t, domain.ConditionTrue, healthy.Status)

	assert.NotNil(t, got.Status.LastConnectedAt)

	// ClusterCache child must have been created, keyed by the probed kube-system UID.
	cacheObj, err := cacheClient.GetByName(ctx, domain.ClusterCacheName(id, uid))
	require.NoError(t, err, "ClusterCache child must exist after successful reconcile")
	assert.Equal(t, uid, cacheObj.Spec.ServerUID, "cache spec records the identity it mirrors")
}

// TestClusterCoreControllerUIDSwitchPrunesSupersededCache verifies that when a probe
// reports a new kube-system UID, the controller creates a cache for the new identity
// and requests deletion of the superseded one, while the Cluster's own ObjectID stays
// consistent. The old cache is held deletion-pending by its finalizer (the
// NoopController never clears it), which is what gates the file cleanup in production.
func TestClusterCoreControllerUIDSwitchPrunesSupersededCache(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	probe, setUID := mutableProbe("uid-old")
	coreClient, cacheClient := newClusterTestBeehive(t, w, probe, staticCheck(HealthPhaseHealthy), nil)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	// First probe creates the cache for the original identity — carrying the finalizer
	// that gates its deletion on file cleanup.
	oldCache := waitForCacheByName(t, cacheClient, domain.ClusterCacheName(id, "uid-old"))
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
	newCache := waitForCacheByName(t, cacheClient, domain.ClusterCacheName(id, "uid-new"))
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

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)
	connected := findCondition(t, got.Conditions, domain.ConditionConnected)
	assert.Equal(t, domain.ConditionFalse, connected.Status)
	assert.Equal(t, domain.ReasonProbeFailed, connected.Reason)
}

func TestClusterCoreControllerIneligibleClusterDoesNotProbe(t *testing.T) {
	probeCalled := false
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	probe := func(_ context.Context, _ *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		probeCalled = true
		return domain.ClusterServer{}, domain.ClusterPrincipal{}, nil
	}
	coreClient, _ := newClusterTestBeehive(t, w, probe, staticCheck(HealthPhaseHealthy), nil)
	ctx := context.Background()

	// Enabled=false → ineligible.
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)

	got := waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)
	connected := findCondition(t, got.Conditions, domain.ConditionConnected)
	assert.Equal(t, domain.ConditionFalse, connected.Status)
	assert.Equal(t, domain.ReasonInactive, connected.Reason)
	assert.False(t, probeCalled, "probe must not be called for ineligible cluster")
}

// A successful probe is recorded in the connection-attempt history as an OK
// attempt (the failure case is covered by TestClusterCoreControllerRecordsConnectionAttempts).
func TestClusterCoreControllerRecordsAttemptOnSuccessfulProbe(t *testing.T) {
	uid := "kube-system-uid"
	user := "alice"
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(domain.ClusterServer{UID: &uid}, domain.ClusterPrincipal{Username: &user}),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)
	evs, err := coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(domain.ConnectionEventCategory))
	require.NoError(t, err)
	require.NotEmpty(t, evs, "a successful probe records an attempt")
	latest := evs[0] // ListEvents is newest-run-first
	assert.Equal(t, beehive.EventNormal, latest.Type, "a successful probe is recorded as Normal")
	assert.Equal(t, domain.ReasonConnected, latest.Reason)
}

func TestClusterCoreControllerIneligibleClusterRecordsNoAttempt(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(domain.ClusterServer{}, domain.ClusterPrincipal{}),
		staticCheck(HealthPhaseHealthy),
		nil,
	)
	ctx := context.Background()

	// Enabled=false → ineligible → no probe is attempted.
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)

	waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)
	evs, err := coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(domain.ConnectionEventCategory))
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

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	var evs []beehive.Event
	require.Eventually(t, func() bool {
		evs, _ = coreClient.ListEvents(ctx, obj.ID, beehive.WithEventCategory(domain.ConnectionEventCategory))
		return len(evs) >= 1
	}, 2*time.Second, 10*time.Millisecond, "a failed probe is recorded in the attempt history")

	latest := evs[0] // ListEvents is newest-run-first
	assert.Equal(t, beehive.EventWarning, latest.Type, "a failed probe is recorded as Warning")
	assert.Equal(t, domain.ReasonProbeFailed, latest.Reason)
	assert.Equal(t, "connection refused", latest.Message)
	assert.False(t, latest.LastAt.IsZero(), "the attempt carries a timestamp")
}

func TestClusterCoreControllerSuccessfulProbePopulatesConnectionManager(t *testing.T) {
	uid := "kube-system-uid"
	ver := "v1.31.0"
	user := "alice"
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := connections.NewManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(
			domain.ClusterServer{UID: &uid, Version: &ver},
			domain.ClusterPrincipal{Username: &user},
		),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	obj, err := coreClient.Create(context.Background(), domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)

	got, fingerprint := connMgr.Get(id)
	assert.NotNil(t, got, "connections.Manager must have a REST config after successful probe")
	assert.NotEmpty(t, fingerprint,
		"the probe must store the fingerprint too — only this controller can compute it, and the sync layer reads it back to detect a rotation")
}

func TestClusterCoreControllerProbeFailureDeletesFromConnectionManager(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := connections.NewManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		errProbe(errors.New("dial failed")),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	ctx := context.Background()
	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	// Wait for the first reconcile (failed probe) to land its Connected condition.
	waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)

	// Pre-seed a stale entry, then force a fresh reconcile (a spec edit bumps generation)
	// so the probe-failure path runs again with the seed in place — which is what makes
	// the clear observable.
	connMgr.Set(id, &rest.Config{Host: "https://stale"}, "fp")
	renamed := eligibleSpec("alpha")
	name := "renamed"
	renamed.Name = &name
	updated, err := coreClient.Update(ctx, beehive.ObjectID(id), renamed)
	require.NoError(t, err)

	// teardownConnection (which clears connMgr) runs before the status write in the
	// same reconcile, so observing the second reconcile's status — its Connected
	// condition stamped with the new generation — means the clear has landed.
	waitForObservedGeneration(t, coreClient, obj.ID, updated.Generation)
	failedCfg, _ := connMgr.Get(id)
	assert.Nil(t, failedCfg, "connections.Manager must not hold a config after probe failure")
}

func TestClusterCoreControllerIneligibleDeletesFromConnectionManager(t *testing.T) {
	w := NewStaticWatcher(t, testKubeConfig("alpha"))
	connMgr := connections.NewManager()

	coreClient, _ := newClusterTestBeehive(t, w,
		staticProbe(domain.ClusterServer{}, domain.ClusterPrincipal{}),
		staticCheck(HealthPhaseHealthy),
		connMgr,
	)

	ctx := context.Background()
	spec := eligibleSpec("alpha")
	spec.Enabled = false
	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), spec)
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	// Wait for the first reconcile (ineligible) to land its Connected condition.
	waitCondition(t, coreClient, obj.ID, domain.ConditionConnected)

	// Pre-seed a stale entry, then force a fresh reconcile (a spec edit bumps
	// generation, staying ineligible) so the teardown path runs with the seed in place.
	connMgr.Set(id, &rest.Config{Host: "https://stale"}, "fp")
	renamed := spec
	name := "renamed"
	renamed.Name = &name
	updated, err := coreClient.Update(ctx, beehive.ObjectID(id), renamed)
	require.NoError(t, err)

	// teardownConnection (which clears connMgr) runs before the status write in the
	// same reconcile, so observing the second reconcile's status — its Connected
	// condition stamped with the new generation — means the clear has landed.
	waitForObservedGeneration(t, coreClient, obj.ID, updated.Generation)
	ineligibleCfg, _ := connMgr.Get(id)
	assert.Nil(t, ineligibleCfg, "connections.Manager must not hold a config for an ineligible cluster")
}

func findCondition(t *testing.T, conds []domain.Condition, typ domain.ConditionType) domain.Condition {
	t.Helper()
	if c := domain.FindCondition(conds, typ); c != nil {
		return *c
	}
	t.Fatalf("condition %s not found in %v", typ, conds)
	return domain.Condition{}
}

// TestClusterCoreControllerPokeReprobes verifies the controller subscribes to the
// poke bus and forces an immediate re-probe of every eligible cluster, rather
// than waiting for the next scheduled (30s) reconcile.
func TestClusterCoreControllerPokeReprobes(t *testing.T) {
	ctx := context.Background()
	pk := poke.New()

	probe, probeCh := signalingProbe()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh, pokeSvc: pk}, w, probe, staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, domain.ClusterCacheGroupKind, &NoopController[domain.ClusterCacheSpec, domain.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	_, err = coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	probeCh.Await(t, "a probe")
	probeCh.Drain()

	// A poke forces an immediate re-probe without waiting for the 30s cadence.
	pk.Poke(poke.SourceHost)
	probeCh.Await(t, "a probe")
}

// TestClusterCoreControllerReprobeOne verifies the in-process retry bus: Reprobe
// forces an immediate out-of-band re-probe of one targeted cluster, rather than
// waiting for the next scheduled (30s) reconcile.
func TestClusterCoreControllerReprobeOne(t *testing.T) {
	ctx := context.Background()

	probe, probeCh := signalingProbe()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	// pokeSvc is nil — the retry bus is in-process, independent of the poke bus.
	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh}, w, probe, staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, domain.ClusterCacheGroupKind, &NoopController[domain.ClusterCacheSpec, domain.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	// The initial scheduled reconcile probes once (then requeues ~30s out).
	probeCh.Await(t, "a probe")
	probeCh.Drain()

	// Reprobe forces an immediate re-probe of the targeted
	ctrl.Reprobe(id)
	probeCh.Await(t, "a probe")
}

// The probe hub is a bounded broadcast shared by every cluster, so a subscriber that is
// slow — a window mid-render, a paused webview — can be overrun and see gochan's lagged
// signal instead of the values it missed. Consuming the raw channel silently swallowed
// that, and the values lost may have included this cluster's probe FINISHING: the row then
// sat on "checking now" until some later probe happened to transition it.
//
// On a lag the stream must re-read the authoritative flag rather than infer anything.
func TestClusterCoreControllerWatchProbeResyncsAfterLag(t *testing.T) {
	ctrl := NewClusterCoreController(&controllerRuntime{}, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const id = domain.ClusterID(1)

	// A probe is in flight, and the subscriber is about to miss its end.
	ctrl.setProbing(id, true)
	sub := ctrl.WatchProbe(ctx, id)
	assert.True(t, recv(t, sub), "mid-probe subscriber sees checking now")

	// Overrun the hub's buffer without the subscriber reading: the probe ends somewhere in
	// here, and that transition is one of the ones overwritten.
	ctrl.setProbing(id, false)
	for range probeHubCapacity * 4 {
		ctrl.setProbing(domain.ClusterID(99), true)
		ctrl.setProbing(domain.ClusterID(99), false)
	}

	// The stream must converge on the truth — not-probing — rather than stay stuck.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case v, ok := <-sub:
			if !ok {
				t.Fatal("probe stream closed")
			}
			if !v {
				return // resynced to the authoritative flag
			}
		case <-deadline:
			t.Fatal("a lagged probe stream never recovered the finished state")
		}
	}
}

// WatchProbe streams the in-flight probe state per cluster: current-on-subscribe (a
// mid-probe subscriber sees true), then one value per transition, filtered to the
// subscribed id. Exercises the hub directly via setProbing, so no beehive/network is
// needed.
func TestClusterCoreControllerWatchProbe(t *testing.T) {
	// No beehive/network is exercised — only the probe hub — so an empty runtime and nil
	// clients are fine (the minted clients are never called).
	ctrl := NewClusterCoreController(&controllerRuntime{}, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const a, b = domain.ClusterID(1), domain.ClusterID(2)

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

// The controller observes the kubeconfig live and writes it to status.Source. Its
// watcher subscription re-reconciles on a change, so a departed context flips
// IsPresent=false — keeping its last-known names — and goes Inactive.
func TestClusterCoreControllerObservesKubeconfigAndDeparture(t *testing.T) {
	ctx := context.Background()
	uid, ver, user := "u", "v1.31.0", "alice"
	w := NewMutableWatcher(testKubeConfig("alpha"))

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh}, w,
		staticProbe(domain.ClusterServer{UID: &uid, Version: &ver}, domain.ClusterPrincipal{Username: &user}),
		staticCheck(HealthPhaseHealthy))
	ctrl.SetSentinelWatcher(liveSentinelWatch)
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, domain.ClusterCacheGroupKind, &NoopController[domain.ClusterCacheSpec, domain.ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	// Present: the observation is written to status from the kubeconfig.
	got := waitForStatus(t, coreClient, obj.ID, func(s *domain.ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && s.Source.Kubeconfig.IsPresent
	})
	kc := got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster)
	assert.Equal(t, "alpha-user", kc.User)
	assert.True(t, kc.IsDefault, "alpha is the current-context")

	// Depart: the context leaves the kubeconfig → watcher wake → re-reconcile.
	w.Set(&api.Config{Contexts: map[string]*api.Context{}})

	got = waitForStatus(t, coreClient, obj.ID, func(s *domain.ClusterStatus) bool {
		return s.Source.Kubeconfig != nil && !s.Source.Kubeconfig.IsPresent
	})
	kc = got.Status.Source.Kubeconfig
	assert.Equal(t, "alpha-cluster", kc.Cluster, "departed record keeps its last-known names")
	assert.Equal(t, "alpha-user", kc.User)
	assert.False(t, kc.IsDefault)
	connected := findCondition(t, got.Conditions, domain.ConditionConnected)
	assert.Equal(t, domain.ConditionFalse, connected.Status)
	assert.Equal(t, domain.ReasonInactive, connected.Reason)
}

// Aggregation into runs and per-object retention are beehive's (RecordEvent +
// WithEventRetention), covered by its own tests; here we only pin the local
// message-truncation helper the event and condition writes share.

func TestTruncateMessage(t *testing.T) {
	assert.Equal(t, "short", domain.TruncateMessage("short"))
	long := strings.Repeat("x", domain.MaxMessageLen+50)
	got := domain.TruncateMessage(long)
	assert.LessOrEqual(t, len(got), domain.MaxMessageLen+len("…"))
	assert.True(t, strings.HasSuffix(got, "…"))
}

// gatedProbe returns a successful ProbeFunc that blocks each call until release is
// closed, announcing every entry on entered. It lets a test hold one cluster's probe
// open and count how many other probes get to run meanwhile.
func gatedProbe() (fn ProbeFunc, entered *testutil.Probe[struct{}], release chan struct{}) {
	entered = testutil.NewProbe[struct{}](8)
	release = make(chan struct{})
	fn = func(ctx context.Context, cfg *rest.Config) (domain.ClusterServer, domain.ClusterPrincipal, error) {
		entered.Fire(struct{}{})
		select {
		case <-release:
		case <-ctx.Done():
			return domain.ClusterServer{}, domain.ClusterPrincipal{}, ctx.Err()
		}
		uid := "uid"
		return domain.ClusterServer{UID: &uid}, domain.ClusterPrincipal{}, nil
	}
	return fn, entered, release
}

// Reconciles of DIFFERENT clusters must run in parallel: the reconcile lock is per
// cluster, so one cluster stuck in a slow (or timing-out) probe must not delay
// another's. This is what keeps an unreachable cluster from serializing startup
// behind it — with a single shared lock the second probe never starts.
func TestClusterCoreControllerReconcilesDistinctClustersInParallel(t *testing.T) {
	ctx := context.Background()
	probe, entered, release := gatedProbe()
	defer close(release)

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha", "beta"))

	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh}, w, probe, staticCheck(HealthPhaseHealthy))
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)

	alpha, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)
	beta, err := coreClient.Create(ctx, domain.KubeconfigName("beta"), eligibleSpec("beta"))
	require.NoError(t, err)

	// Drive Reconcile directly (not through beehive's workers) so the test pins the
	// controller's own locking rather than beehive's configured worker count.
	for _, obj := range []*beehive.Object[domain.ClusterSpec, domain.ClusterStatus]{alpha, beta} {
		go func() { _, _ = ctrl.Reconcile(ctx, cc, obj) }()
	}

	// Both probes must be in flight at once. Under a single shared lock only one
	// entry ever arrives and this times out.
	for i := range 2 {
		entered.Await(t, fmt.Sprintf("probe %d of 2 (distinct clusters must not serialize)", i+1))
	}
}

// The other half of the invariant: reconciles of the SAME cluster must NOT overlap.
// beehive status writes carry no resourceVersion guard, so an interleaved
// read-modify-write would let a stale snapshot clobber a newer observation.
func TestClusterCoreControllerSerializesSameClusterReconciles(t *testing.T) {
	ctx := context.Background()
	probe, entered, release := gatedProbe()
	defer close(release)

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh}, w, probe, staticCheck(HealthPhaseHealthy))
	cc, err := beehive.Register(bh, domain.ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)

	obj, err := coreClient.Create(ctx, domain.KubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	for range 2 {
		go func() { _, _ = ctrl.Reconcile(ctx, cc, obj) }()
	}

	// One probe enters; the second must stay blocked on the first's lock.
	entered.Await(t, "the first probe")
	select {
	case <-entered.Chan():
		t.Fatal("a second reconcile of the same cluster ran concurrently")
	case <-time.After(200 * time.Millisecond):
	}
}
