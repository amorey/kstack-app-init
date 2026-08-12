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
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// registerWorker puts one worker in the set. Its fakeWorker has no factory: nothing here
// calls Start, only drains.
func registerWorker(t *testing.T, set *workerSet, objID beehive.ObjectID, cacheID int64, stopErr error) (*syncEntry, *fakeWorker) {
	t.Helper()
	w := &fakeWorker{stopErr: stopErr}
	entry := &syncEntry{
		objID:  objID,
		worker: w,
		kind:   objectsync.Kind{APIVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true},
		ref:    store.CacheRef{ClusterID: 1, CacheID: cacheID},
	}
	ok, err := set.putIfAbsent(entry)
	require.NoError(t, err)
	require.True(t, ok, "the set must accept a worker for an unused object id")
	return entry, w
}

// stopAll is the shutdown path: it runs AFTER beehive drains and BEFORE the cache manager
// closes, so every worker must be drained by the time it returns — a worker it skipped is
// one still writing into a .db about to be closed.
func TestWorkerSetStopAllDrainsEveryWorker(t *testing.T) {
	set := newWorkerSet()
	_, w1 := registerWorker(t, set, 1, 10, nil)
	_, w2 := registerWorker(t, set, 2, 10, nil)
	// A second cache's worker: shutdown is fleet-wide, unlike a cache-scoped restart.
	_, w3 := registerWorker(t, set, 3, 20, nil)

	require.NoError(t, set.stopAll(context.Background()))

	assert.True(t, w1.isStopped(), "worker 1 must be drained")
	assert.True(t, w2.isStopped(), "worker 2 must be drained")
	assert.True(t, w3.isStopped(), "another cache's worker must be drained too")
	assert.Empty(t, set.entries(), "a clean drain must leave the registry empty")
}

// One wedged drain must not cost the others theirs: shutdown has no retry after it.
func TestWorkerSetStopAllKeepsGoingPastAFailedDrain(t *testing.T) {
	set := newWorkerSet()
	// All three fail, each with its own error. stopAll walks a map, so requiring every error
	// is the order-independent assertion — an early break can only ever produce one.
	boom1, boom2, boom3 := errors.New("wedged 1"), errors.New("wedged 2"), errors.New("wedged 3")
	failing, wf := registerWorker(t, set, 1, 10, boom1)
	registerWorker(t, set, 2, 10, boom2)
	registerWorker(t, set, 3, 20, boom3)

	err := set.stopAll(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, boom1, "every failure must reach the caller")
	assert.ErrorIs(t, err, boom2, "a drain after a failed one must still be attempted")
	assert.ErrorIs(t, err, boom3, "another cache's worker must be attempted too")
	assert.False(t, wf.isStopped(), "the wedged worker never finished")

	// A failed drain stays REGISTERED and draining — that is what keeps a deletion barrier
	// real, so the wedged goroutine is never mistaken for gone.
	assert.True(t, set.holds(failing), "a failed drain must stay registered")
	assert.False(t, set.isCurrent(failing), "a failed drain must still read as draining")
}

// A restart sequence holds the cache's gate from its snapshot through the restarts. A
// caller that gives up waiting must still release its claim, or the gate leaks a waiter.
func TestAcquireCacheRestartHonoursContextCancellation(t *testing.T) {
	set := newWorkerSet()
	release, err := set.acquireCacheRestart(context.Background(), 10)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already dead: the second acquire cannot win the gate
	_, err = set.acquireCacheRestart(ctx, 10)
	require.ErrorIs(t, err, context.Canceled)

	// The abandoned waiter dropped its reference on the way out, so releasing the holder
	// leaves none and the gate is reclaimed.
	release()
	set.mu.Lock()
	_, stillThere := set.restartGates[10]
	set.mu.Unlock()
	assert.False(t, stillThere, "the gate must be reclaimed once nobody holds or awaits it")
}

// The two refusals from putIfAbsent mean different things to the caller, so they must stay
// distinguishable: "somebody else got there first" is final, while "the cache is
// restarting" says the handle is about to close and another reconcile must rebuild.
func TestPutIfAbsentRefusesARestartingCacheDistinctly(t *testing.T) {
	set := newWorkerSet()
	registerWorker(t, set, 1, 10, nil)

	// Same object id, no restart in flight: refused, but not an error.
	dup := &syncEntry{objID: 1, worker: &fakeWorker{}, ref: store.CacheRef{ClusterID: 1, CacheID: 10}}
	ok, err := set.putIfAbsent(dup)
	assert.False(t, ok)
	assert.NoError(t, err, "an already-worked object is a plain refusal, not an error")

	set.beginCacheRestart(10)
	t.Cleanup(func() { set.endCacheRestart(10) })

	// A fresh object id, but its cache is restarting: refused WITH errCacheRestarting.
	fresh := &syncEntry{objID: 2, worker: &fakeWorker{}, ref: store.CacheRef{ClusterID: 1, CacheID: 10}}
	ok, err = set.putIfAbsent(fresh)
	assert.False(t, ok)
	assert.ErrorIs(t, err, errCacheRestarting)

	// A different cache is unaffected — the refusal is scoped, not global.
	other := &syncEntry{objID: 3, worker: &fakeWorker{}, ref: store.CacheRef{ClusterID: 1, CacheID: 20}}
	ok, err = set.putIfAbsent(other)
	assert.True(t, ok)
	assert.NoError(t, err)
}

// beginCacheRestart snapshots only the named cache's workers, ordered by object id so
// every sequence takes lifecycle locks in the same order and two can't deadlock.
func TestBeginCacheRestartSnapshotsOneCacheInIDOrder(t *testing.T) {
	set := newWorkerSet()
	registerWorker(t, set, 3, 10, nil)
	registerWorker(t, set, 1, 10, nil)
	registerWorker(t, set, 2, 20, nil)
	t.Cleanup(func() { set.endCacheRestart(10) })

	mine := set.beginCacheRestart(10)

	require.Len(t, mine, 2, "only cache 10's workers belong to its restart")
	assert.Equal(t, beehive.ObjectID(1), mine[0].objID)
	assert.Equal(t, beehive.ObjectID(3), mine[1].objID)
}

// The sink is the out-of-band write path, so its guard is all that stands between a worker
// on its way out and the status of the worker that replaced it.
//
// Pins which reports land, driving Report sequentially. Does NOT pin the second,
// under-writeMu re-check — that needs a replacement landing between the first check and the
// lock, which the sink exposes no seam to force. Deleting it leaves this test green.
func TestSyncSinkDropsReportsFromDepartedWorkers(t *testing.T) {
	set := newWorkerSet()
	entry, _ := registerWorker(t, set, 1, 10, nil)

	var mu sync.Mutex
	var applied []kubesync.Status
	sink := &syncSink{
		set:     set,
		entry:   entry,
		writeMu: &mu,
		apply: func(_ context.Context, _ *syncEntry, st kubesync.Status) error {
			applied = append(applied, st)
			return nil
		},
		name: "test",
	}

	sink.Report(kubesync.Status{State: kubesync.StateWatching})
	require.Len(t, applied, 1, "the live worker's report must be folded")

	// Draining: reports from a worker being torn down must not land, even though its entry
	// is still registered (it stays until the drain succeeds).
	entry.draining.Store(true)
	sink.Report(kubesync.Status{State: kubesync.StateStale})
	assert.Len(t, applied, 1, "a draining worker's report must be dropped")

	// Replaced: a stale report must not clobber the status of the worker that took over.
	entry.draining.Store(false)
	set.mu.Lock()
	set.workers[1] = &syncEntry{objID: 1, worker: &fakeWorker{}}
	set.mu.Unlock()
	sink.Report(kubesync.Status{State: kubesync.StateErrored})
	assert.Len(t, applied, 1, "a replaced worker's report must be dropped")
}

// syncedCondition is the current-state naming; every kubesync state must map onto a Synced
// condition, because an unmapped one would read as healthy by omission.
func TestSyncedConditionMapsEveryState(t *testing.T) {
	tests := []struct {
		name       string
		st         kubesync.Status
		wantStatus domain.ConditionStatus
		wantReason string
	}{
		{"watching is the only healthy state", kubesync.Status{State: kubesync.StateWatching}, domain.ConditionTrue, domain.ReasonWatching},
		{"errored surfaces the failure", kubesync.Status{State: kubesync.StateErrored, LastError: "boom"}, domain.ConditionFalse, domain.ReasonSyncFailed},
		{"stale is a fault, not a wait", kubesync.Status{State: kubesync.StateStale, Cause: kubesync.CauseWatchStalled}, domain.ConditionFalse, domain.ReasonStale},
		{"syncing is the catch-up state", kubesync.Status{State: kubesync.StateSyncing}, domain.ConditionFalse, domain.ReasonSyncing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := syncedCondition(tt.st, "deployments")
			assert.Equal(t, tt.wantStatus, got.Status)
			assert.Equal(t, tt.wantReason, got.Reason)
			assert.True(t, got.Liveness, "a worker-reported condition is a live observation")
		})
	}
}

// Errored carries the worker's own message through, since it is the only text naming what
// actually broke.
func TestSyncedConditionCarriesTheWorkerError(t *testing.T) {
	got := syncedCondition(kubesync.Status{State: kubesync.StateErrored, LastError: "forbidden: cannot list deployments"}, "deployments")
	assert.Contains(t, got.Message, "cannot list deployments")
}

// syncEvent is the TRANSITION vocabulary, distinct from syncedCondition's current-state
// naming. ColdStart splits each phase into a start/complete pair, and prev distinguishes a
// first catch-up from a recovery.
func TestSyncEventNamesEachTransition(t *testing.T) {
	tests := []struct {
		name       string
		st         kubesync.Status
		prev       kubesync.State
		wantType   beehive.EventType
		wantReason string
		wantInMsg  string
	}{
		{
			name:       "cold start opens with SyncStart",
			st:         kubesync.Status{State: kubesync.StateSyncing, ColdStart: true},
			wantType:   beehive.EventNormal,
			wantReason: domain.ReasonSyncStart,
			wantInMsg:  "Starting initial deployments sync",
		},
		{
			name:       "a warm start names the cache it is resuming from",
			st:         kubesync.Status{State: kubesync.StateSyncing, CachedItems: 12},
			wantType:   beehive.EventNormal,
			wantReason: domain.ReasonResyncStart,
			wantInMsg:  "12 deployments",
		},
		{
			name:       "cold catch-up completes with SyncComplete and its count",
			st:         kubesync.Status{State: kubesync.StateWatching, ColdStart: true, SyncedItems: 7, CaughtUpIn: 4200 * time.Millisecond},
			wantType:   beehive.EventNormal,
			wantReason: domain.ReasonSyncComplete,
			wantInMsg:  "cached 7 deployments in 4.2s",
		},
		{
			name:       "recovery out of stale is a resync completion, not a first sync",
			st:         kubesync.Status{State: kubesync.StateWatching, ColdStart: true},
			prev:       kubesync.StateStale,
			wantType:   beehive.EventNormal,
			wantReason: domain.ReasonResyncComplete,
			wantInMsg:  "Watch recovered",
		},
		{
			name:       "recovery out of errored is also a recovery",
			st:         kubesync.Status{State: kubesync.StateWatching},
			prev:       kubesync.StateErrored,
			wantType:   beehive.EventNormal,
			wantReason: domain.ReasonResyncComplete,
			wantInMsg:  "Watch recovered",
		},
		{
			name:       "a worker failure is a warning",
			st:         kubesync.Status{State: kubesync.StateErrored, LastError: "boom"},
			wantType:   beehive.EventWarning,
			wantReason: domain.ReasonSyncDegraded,
			wantInMsg:  "boom",
		},
		{
			name:       "going stale is a warning naming the cause",
			st:         kubesync.Status{State: kubesync.StateStale, Cause: kubesync.CauseWatchFailed},
			wantType:   beehive.EventWarning,
			wantReason: domain.ReasonSyncStale,
			wantInMsg:  "watch cannot be established",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, reason, msg := syncEvent(tt.st, tt.prev, "deployments")
			assert.Equal(t, tt.wantType, typ)
			assert.Equal(t, tt.wantReason, reason)
			assert.Contains(t, msg, tt.wantInMsg)
		})
	}
}

// A clean warm resume re-fetches nothing, so naming a count would read as instant work on
// a cache that did none. Only a fallback to a full LIST reports one.
func TestResyncCompleteMessageOmitsTheCountOnACleanResume(t *testing.T) {
	clean := resyncCompleteMessage(kubesync.Status{State: kubesync.StateWatching, CaughtUpIn: 300 * time.Millisecond}, "deployments")
	assert.Contains(t, clean, "resumed the watch in 300ms")
	assert.NotContains(t, clean, "deployments", "a clean resume pulled nothing, so it names no count")

	fell := resyncCompleteMessage(kubesync.Status{
		State: kubesync.StateWatching, Resynced: true, ResyncedItems: 9, CaughtUpIn: 1500 * time.Millisecond,
	}, "deployments")
	assert.Contains(t, fell, "saved position expired")
	assert.Contains(t, fell, "re-synced 9 deployments in 1.5s")
}

// The stale message is the actionable half of a fault — it must name the cause, so an
// operator reads "grant watch" rather than "something is wrong".
func TestStaleMessageNamesTheCause(t *testing.T) {
	tests := []struct {
		cause     kubesync.Cause
		wantInMsg string
	}{
		{kubesync.CauseListFailed, "the list is failing"},
		{kubesync.CauseWatchFailed, "the watch cannot be established"},
		{kubesync.CauseWatchStalled, "the watch has stalled"},
		{kubesync.CauseWatchNotStreaming, "keeps reconnecting without streaming"},
		{kubesync.Cause("SomethingNew"), "the cache may be behind"},
	}
	for _, tt := range tests {
		t.Run(string(tt.cause), func(t *testing.T) {
			msg := staleMessage(tt.cause, "deployments")
			assert.Contains(t, msg, "Not receiving deployments", "the message must name what is missing")
			assert.Contains(t, msg, tt.wantInMsg)
		})
	}
}

// noun comes off the kind, so a message can't fall out of step with what the worker syncs.
func TestSyncEntryNounIsThePluralResource(t *testing.T) {
	e := &syncEntry{kind: objectsync.Kind{APIVersion: "apps/v1", Kind: "StatefulSet", Resource: "statefulsets", Namespaced: true}}
	assert.Equal(t, "statefulsets", e.noun())
}
