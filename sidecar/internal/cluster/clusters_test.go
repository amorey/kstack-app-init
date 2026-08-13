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

// White-box (package cluster): the service test seeds beehive objects directly and
// exercises the data/mutation/watch surface in isolation from the (network-touching)
// real controllers, using the shared helpers in testutil_test.go.
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestServiceListAndGet(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	idAlpha := seedCluster(t, s, "alpha")
	seedCluster(t, s, "beta")

	list, err := s.Clusters().List(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)

	c, err := s.Clusters().Get(ctx, idAlpha)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, idAlpha, c.ID)
	require.NotNil(t, c.Spec.Name)
	assert.Equal(t, "alpha", *c.Spec.Name)

	// Unknown id is (nil, nil), not an error.
	missing, err := s.Clusters().Get(ctx, domain.ClusterID(999999))
	require.NoError(t, err)
	assert.Nil(t, missing)
}

// WatchCaches streams each ClusterCache standalone: the snapshot carries an Added
// change per cache with its parent ClusterID resolved from the owner edge, its
// ServerUID, and its conditions. The kind has no status (it measures nothing itself), and
// active-ness is a client-side join, so neither is asserted here.

func TestServiceGetDeletionPendingIsNil(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, s.coreClient.Delete(ctx, obj.ID))

	c, err := s.Clusters().Get(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, c)
}

func TestServiceSetEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.Clusters().SetEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.Enabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.Enabled)
}

func TestServiceSetSyncEnabled(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	c, err := s.Clusters().SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.False(t, c.Spec.SyncEnabled)

	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.False(t, obj.Spec.SyncEnabled)
}

// RetryConnection dispatches an out-of-band re-probe without mutating the spec. Via the
// fakeCoreController we pin that the dispatch reaches Reprobe and the spec is untouched;
// an unknown id errors before any dispatch.

func TestServiceDeleteTombstonesCluster(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newServiceTest(t)

	// Seed with a finalizer so the soft-delete tombstone is observable without a race:
	// beehive GC is a no-op while an object holds a finalizer, so the deletion-pending
	// row lingers deterministically. Without it, the noop controller collects the
	// finalizer-less row on the reconcile Delete enqueues, and that physical delete
	// races the Get below.
	name := "alpha"
	obj, err := s.coreClient.Create(ctx, domain.KubeconfigName("alpha"), domain.ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: "alpha"}},
	}, beehive.WithFinalizers("test/hold"))
	require.NoError(t, err)
	id := domain.ClusterID(obj.ID)

	require.NoError(t, s.Clusters().Delete(ctx, id))

	// Delete tombstones the Cluster (soft delete); beehive GC then cascades to its
	// ClusterCache once the finalizers clear.
	obj, err = s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	assert.NotNil(t, obj.DeletionRequestedAt)
}

func TestServiceWatchEmitsSnapshotThenDeltas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	id := seedCluster(t, s, "alpha")

	ch, err := s.Clusters().Watch(ctx)
	require.NoError(t, err)

	// Snapshot: one Added change for the seeded cluster, closed by the Bookmark.
	seed := recv(t, ch)
	assert.Equal(t, domain.FrameAdded, seed.Type)
	require.NotNil(t, seed.Cluster)
	assert.Equal(t, id, seed.Cluster.ID)
	bm := recv(t, ch)
	requireBookmark(t, bm.Type, bm.Cluster)

	// A spec change emits a Modified change carrying the new state. WatchList replays
	// current state on subscribe, so drain until the change lands.
	_, err = s.Clusters().SetSyncEnabled(ctx, id, false)
	require.NoError(t, err)

	deadline := time.After(2 * time.Second)
	for {
		ev := recvBy(t, ch, deadline)
		require.NotNil(t, ev.Cluster)
		if !ev.Cluster.Spec.SyncEnabled {
			assert.Equal(t, domain.FrameModified, ev.Type)
			return
		}
	}
}

// fakeScheduleClient drives the schedule-watch wrapper with a test-controlled channel,
// so the mapping (beehive.Schedule → Schedule, zero → nil) and the ctx lifecycle are
// exercised deterministically — beehive's own tests cover the queue/gauge semantics.

// fakeScheduleClient drives the schedule-watch wrapper with a test-controlled channel,
// so the mapping (beehive.Schedule → Schedule, zero → nil) and the ctx lifecycle are
// exercised deterministically — beehive's own tests cover the queue/gauge semantics.
type fakeScheduleClient struct{ ch chan beehive.Schedule }

func (f *fakeScheduleClient) WatchSchedule(context.Context, beehive.ObjectID) (<-chan beehive.Schedule, error) {
	return f.ch, nil
}

// scheduleWatch maps the beehive Schedule gauge to the domain Schedule: a
// non-zero NextRequeueAt becomes a NextRequeueAt pointer, the zero time becomes
// nil (nothing scheduled). The out channel closes when the source closes.

// scheduleWatch maps the beehive Schedule gauge to the domain Schedule: a
// non-zero NextRequeueAt becomes a NextRequeueAt pointer, the zero time becomes
// nil (nothing scheduled). The out channel closes when the source closes.
func TestServiceScheduleWatchMapsGauge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Service{}
	fake := &fakeScheduleClient{ch: make(chan beehive.Schedule, 1)}

	out, err := s.scheduleWatch(ctx, fake, 1)
	require.NoError(t, err)

	// a scheduled time → NextRequeueAt pointer
	at := time.Now().Add(time.Hour).UTC()
	fake.ch <- beehive.Schedule{NextRequeueAt: at}
	got := recv(t, out)
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, at, *got.NextRequeueAt)

	// the zero time → nil (nothing scheduled)
	fake.ch <- beehive.Schedule{}
	got = recv(t, out)
	assert.Nil(t, got.NextRequeueAt)

	// source close → out closes
	close(fake.ch)
	testutil.RecvClosed(t, out, "out when the schedule source closes")
}

// mergeSchedule folds the schedule gauge (NextRequeueAt) and the in-flight probe
// signal (Probing) into one Schedule stream, re-emitting the combined latest as
// either side moves; a closed sub-source is dropped without ending the stream,
// and the out channel closes only when both close.

// mergeSchedule folds the schedule gauge (NextRequeueAt) and the in-flight probe
// signal (Probing) into one Schedule stream, re-emitting the combined latest as
// either side moves; a closed sub-source is dropped without ending the stream,
// and the out channel closes only when both close.
func TestMergeScheduleCombinesGaugeAndProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schedCh := make(chan domain.Schedule, 1)
	probeCh := make(chan bool, 1)
	out := mergeSchedule(ctx, schedCh, probeCh)

	// A probe starts → Probing true, no scheduled time yet.
	probeCh <- true
	got := recv(t, out)
	assert.True(t, got.Probing)
	assert.Nil(t, got.NextRequeueAt)

	// A scheduled time arrives → NextRequeueAt set, Probing still asserted (the
	// combined latest carries both).
	at := time.Now().Add(time.Hour).UTC()
	schedCh <- domain.Schedule{NextRequeueAt: &at}
	got = recv(t, out)
	assert.True(t, got.Probing)
	require.NotNil(t, got.NextRequeueAt)
	assert.Equal(t, at, *got.NextRequeueAt)

	// The probe finishes → Probing clears, the scheduled time is retained.
	probeCh <- false
	got = recv(t, out)
	assert.False(t, got.Probing)
	require.NotNil(t, got.NextRequeueAt)

	// Closing one source keeps the other flowing.
	close(probeCh)
	schedCh <- domain.Schedule{}
	got = recv(t, out)
	assert.Nil(t, got.NextRequeueAt)
	assert.False(t, got.Probing)

	// Closing both → out closes.
	close(schedCh)
	testutil.RecvClosed(t, out, "out when both sub-sources close")
}

// ClusterScheduleWatch, through the ClusterService interface, streams the real
// beehive schedule gauge for a cluster (snapshot on subscribe, then live).

// ClusterScheduleWatch, through the ClusterService interface, streams the real
// beehive schedule gauge for a cluster (snapshot on subscribe, then live).
func TestClusterScheduleWatchPublicSurface(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _, _ := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")

	ch, err := svc.Clusters().WatchSchedule(ctx, id)
	require.NoError(t, err)
	// The snapshot arrives (value depends on queue state; the contract is that the
	// stream is live and closes on ctx).
	recv(t, ch)

	cancel()
	assert.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "stream should close on ctx cancel")
}

// The generic event reader maps beehive's coalesced runs to the domain Event wire
// shape, newest-run-first, honoring the category filter and limit. beehive coalesces
// same-(category,type,reason) occurrences into one run, so a repeated reason bumps
// Count and a changed reason starts a new run.

// The kind-scoped public surface (ClusterEvents / ClusterEventsWatch), reached through
// the ClusterService interface, delegates to the generic reader/watch against the
// Cluster kind client. Asserting via the interface value locks the public API shape.
func TestClusterEventsPublicSurface(t *testing.T) {
	ctx := t.Context()
	s, coreCC, _ := newServiceTest(t)
	var svc ClusterService = s
	id := seedCluster(t, s, "alpha")
	oid := beehive.ObjectID(id)

	const cat = "connection"
	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventWarning, Reason: "ReasonA", Message: "boom",
	}))

	// ClusterEvents: point read, filtered to the category
	category := cat
	evs, err := svc.Clusters().ListEvents(ctx, id, &category, nil)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "ReasonA", evs[0].Reason)

	// ClusterEventsWatch: snapshot replays the existing run, then a live run arrives
	ch, err := svc.WatchObjectEvents(ctx, domain.ObjectID(id), &category)
	require.NoError(t, err)

	e := recvRun(t, ch)
	assert.Equal(t, "ReasonA", e.Reason)
	recvEventBookmark(t, ch)

	require.NoError(t, coreCC.AddEvent(ctx, oid, beehive.EventSpec{
		Category: cat, Type: beehive.EventNormal, Reason: "ReasonB",
	}))
	e = recvRun(t, ch)
	assert.Equal(t, "ReasonB", e.Reason)
}

// ClusterCacheEvents / ClusterCacheEventsWatch are the ClusterCache-kind counterparts:
// same generic reader/watch, but against the cache client and keyed by the
// ClusterCache's own ObjectID (the sync-event timeline under category "sync"). Asserting
// via the interface value locks the public API shape.
