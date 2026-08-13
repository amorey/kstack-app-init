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
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// syncHealthTick paces the rollup's periodic recompute: the freshness stamps it
// folds live in controller memory and move with no frame at all. Emission stays
// change-gated, so a quiet fleet sends nothing.
const syncHealthTick = 10 * time.Second

// syncHealthSnapshot is every cache's current verdict, keyed by cache id — the
// fold's whole output as one latest value. Published snapshots are immutable
// (fresh map per publish; subscribers read concurrently).
type syncHealthSnapshot map[domain.ClusterCacheID]domain.ClusterCacheSyncHealth

// WatchSyncHealth implements Caches — every cache's sync verdict as a
// latest-value stream. Subscribers share ONE fold (syncHealthReceiver); this
// adapter is the per-subscriber half, sending only what changed for THIS
// subscriber — which is what gives a late joiner every cache on its first read.
// See docs/adr/2026-08-09-status-propagation-gauges.md.
func (a cachesAPI) WatchSyncHealth(ctx context.Context) (<-chan domain.ClusterCacheSyncHealth, error) {
	rx, err := a.s.syncHealthReceiver()
	if err != nil {
		return nil, err
	}
	out := make(chan domain.ClusterCacheSyncHealth, 1)
	go func() {
		defer close(out)
		defer rx.Close()
		sent := syncHealthSnapshot{}
		for {
			snap, err := rx.RecvContext(ctx)
			if err != nil {
				return // ctx ended, or the hub closed at shutdown
			}
			// Forget caches that left the snapshot. No delete FRAME: this is a gauge;
			// the consumer drops a verdict when the cache leaves clusterCachesWatch,
			// which owns that lifecycle.
			for cacheID := range sent {
				if _, ok := snap[cacheID]; !ok {
					delete(sent, cacheID)
				}
			}
			for cacheID, health := range snap {
				if prev, ok := sent[cacheID]; ok && syncHealthEqual(prev, health) {
					continue
				}
				sent[cacheID] = health
				if !send(ctx, out, health) {
					return
				}
			}
		}
	}()
	return out, nil
}

// syncHealthReceiver returns a receiver on the shared fold, starting it on first
// use — lazy so an unwatched fleet isn't folded, and the fold outlives any one
// subscriber. Once started it runs until Close (no refcount: little gain, a
// teardown race).
func (s *Service) syncHealthReceiver() (*watch.Receiver[syncHealthSnapshot], error) {
	s.syncHealthMu.Lock()
	defer s.syncHealthMu.Unlock()
	if s.syncHealthClosed {
		return nil, errors.New("cluster: sync-health fold is shut down")
	}
	if s.syncHealth == nil {
		hub, stop, done, err := s.startSyncHealthFold()
		if err != nil {
			return nil, err // nothing cached: the next subscriber retries
		}
		s.syncHealth, s.syncHealthStop, s.syncHealthDone = hub, stop, done
	}
	return s.syncHealth.Receiver(), nil
}

// startSyncHealthFold opens the two watches the fold reads and runs it, publishing
// each recomputed snapshot to a latest-value hub. The hub carries the whole map,
// not deltas — the output is tiny (one per cache), a new subscriber's first read is
// "every cache, right now", and a slow subscriber coalesces to the newest (correct
// for a gauge, where a dropped delta would be lost state).
func (s *Service) startSyncHealthFold() (*watch.Hub[syncHealthSnapshot], context.CancelFunc, chan struct{}, error) {
	// Background, not a caller's: the fold outlives every subscriber. Cancelled by
	// stopSyncHealthFold, or by the fold itself on any other exit.
	ctx, cancel := context.WithCancel(context.Background())
	syncStream, err := s.gvrSyncClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	discStream, err := s.gvrDiscoveryClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	hub := watch.New(syncHealthSnapshot{})
	done := make(chan struct{})
	go func() {
		// Declared first so it runs LAST: done means every defer below has run —
		// see stopSyncHealthFold.
		defer close(done)
		defer func() {
			if s.syncHealthFoldExit != nil {
				s.syncHealthFoldExit()
			}
		}()
		// Cancel our OWN context on any exit: a fold can end with nobody calling the
		// stop func (beehive terminating a watch, a source closing), and the two
		// fleet-wide WatchList leases are registered against this context alone —
		// forgetSyncHealthFold clears the cached stop func, so nothing else would
		// ever release them.
		defer cancel()
		defer hub.Close()
		// Forget this hub on the way out: a closed hub hands later subscribers
		// pre-closed receivers, so a self-terminated fold (e.g. ErrWatchTooOld
		// during a cold-sync write storm) would otherwise silently end sync status
		// until restart. Clearing lets the next subscriber start a fresh fold.
		defer s.forgetSyncHealthFold(hub)
		f := &syncHealthFold{
			syncs:         map[beehive.ObjectID]gvrSyncRec{},
			cacheOf:       map[domain.ClusterCacheGVRDiscoveryID]domain.ClusterCacheID{},
			byDiscovery:   map[domain.ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}{},
			discoveriesOf: map[domain.ClusterCacheID]map[domain.ClusterCacheGVRDiscoveryID]struct{}{},
			published:     syncHealthSnapshot{},
			dirty:         map[domain.ClusterCacheID]struct{}{},
			stats:         s.Syncs().SnapshotStats,
			out:           hub.Sender(),
		}
		for _, obj := range discStream.Objects {
			f.putDiscovery(obj)
		}
		for _, obj := range syncStream.Objects {
			f.putSync(obj)
		}
		// A cache with an anchor but no kinds still needs its "nothing observed" verdict.
		f.markAll()
		f.flush()

		tick := time.NewTicker(syncHealthTick)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case ch, ok := <-discStream.Changes:
				if !ok {
					logSyncHealthWatchEnd(ctx, "ClusterCacheGVRDiscovery", discStream.Err())
					return
				}
				f.applyDiscovery(ch)
			case ch, ok := <-syncStream.Changes:
				if !ok {
					logSyncHealthWatchEnd(ctx, "ClusterCacheGVRSync", syncStream.Err())
					return
				}
				f.applySync(ch)
			case <-tick.C:
				// The only thing that refreshes freshness on a settled cache — the
				// stamps move with no frame behind them.
				f.markAll()
			}
			f.flush()
		}
	}()
	return hub, cancel, done, nil
}

// logSyncHealthWatchEnd reports why a fold watch stopped, once its stream closed.
// A nil Err is the fold's own context ending, which is not a fault.
func logSyncHealthWatchEnd(ctx context.Context, kind string, err error) {
	if err != nil && ctx.Err() == nil {
		slog.Warn("clusterservice: sync-health watch ended", "kind", kind, "err", err)
	}
}

// forgetSyncHealthFold drops the cached hub when its fold has ended. It clears
// only if the cache still points at THIS hub, and leaves the shutdown latch alone.
func (s *Service) forgetSyncHealthFold(hub *watch.Hub[syncHealthSnapshot]) {
	s.syncHealthMu.Lock()
	defer s.syncHealthMu.Unlock()
	if s.syncHealth != hub {
		return
	}
	s.syncHealth = nil
	s.syncHealthStop = nil
}

// stopSyncHealthFold cancels the fold and closes its hub, ending every subscriber.
func (s *Service) stopSyncHealthFold(ctx context.Context) {
	s.syncHealthMu.Lock()
	s.syncHealthClosed = true
	stop, done := s.syncHealthStop, s.syncHealthDone
	s.syncHealth, s.syncHealthStop, s.syncHealthDone = nil, nil, nil
	// Unlock BEFORE the wait: the fold's teardown takes this same lock.
	s.syncHealthMu.Unlock()
	if stop == nil {
		return
	}
	stop()
	// Join it: the WatchList leases come back in its defers, so returning early
	// would let beehive drain while the fold is mid-flush. Bounded by the caller's
	// drain deadline.
	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("cluster: sync-health fold did not unwind before the drain deadline")
	}
}

// gvrSyncRec is the slice of one per-kind record the fold needs.
type gvrSyncRec struct {
	discoveryID domain.ClusterCacheGVRDiscoveryID
	apiVersion  string
	resource    string
	conditions  []domain.Condition
}

// kindRef is this record's kind identity, as the verdict reports it.
func (r gvrSyncRec) kindRef() domain.SyncedKindRef {
	return domain.SyncedKindRef{APIVersion: r.apiVersion, Resource: r.resource}
}

// compareKindRefs orders kind refs by the plural first, since that is what a UI shows;
// the api group only breaks a tie between two kinds sharing one.
func compareKindRefs(a, b domain.SyncedKindRef) int {
	if c := cmp.Compare(a.Resource, b.Resource); c != 0 {
		return c
	}
	return cmp.Compare(a.APIVersion, b.APIVersion)
}

// syncHealthFold accumulates the two watches into a per-cache verdict. Single-goroutine;
// no locking.
type syncHealthFold struct {
	syncs   map[beehive.ObjectID]gvrSyncRec
	cacheOf map[domain.ClusterCacheGVRDiscoveryID]domain.ClusterCacheID
	// byDiscovery / discoveriesOf are the reverse indexes of cacheOf, so a flush
	// walks only the dirty caches' records — without them the dirty set scopes
	// which verdicts recompute but not how much is read (the quadratic part).
	byDiscovery   map[domain.ClusterCacheGVRDiscoveryID]map[beehive.ObjectID]struct{}
	discoveriesOf map[domain.ClusterCacheID]map[domain.ClusterCacheGVRDiscoveryID]struct{}
	// published is the last value sent per cache, so an unchanged recompute sends nothing.
	published map[domain.ClusterCacheID]domain.ClusterCacheSyncHealth
	// dirty scopes recompute to caches that may have moved — at cold start each of a
	// cache's hundred-plus kinds delivers its own frame.
	dirty map[domain.ClusterCacheID]struct{}
	// stats reads every kind's freshness in one lock acquisition — see StatsSnapshot.
	stats func() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats
	// out publishes each recomputed snapshot; latest-value, so a burst collapses.
	out *watch.Sender[syncHealthSnapshot]
}

// mark flags one cache for recompute; markAll flags every cache the fold knows about
// (used by the tick, which re-reads stamps nothing else announces).
func (f *syncHealthFold) mark(cacheID domain.ClusterCacheID) {
	if cacheID != 0 {
		f.dirty[cacheID] = struct{}{}
	}
}

func (f *syncHealthFold) markAll() {
	for _, cacheID := range f.cacheOf {
		f.mark(cacheID)
	}
	for cacheID := range f.published {
		f.mark(cacheID)
	}
}

func (f *syncHealthFold) putDiscovery(obj *beehive.Object[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]) {
	id := domain.ClusterCacheGVRDiscoveryID(obj.ID)
	cacheID := domain.ClusterCacheID(ownerObjectID(obj))
	// An anchor that moved caches must leave the one it came from, or its records
	// would count toward both.
	if prev, ok := f.cacheOf[id]; ok && prev != cacheID {
		f.unlinkDiscovery(id, prev)
	}
	f.cacheOf[id] = cacheID
	if f.discoveriesOf[cacheID] == nil {
		f.discoveriesOf[cacheID] = map[domain.ClusterCacheGVRDiscoveryID]struct{}{}
	}
	f.discoveriesOf[cacheID][id] = struct{}{}
	f.mark(cacheID)
}

// unlinkDiscovery drops one anchor from its cache's index, and the cache's entry with it
// once empty, so neither map outlives the objects it describes.
func (f *syncHealthFold) unlinkDiscovery(id domain.ClusterCacheGVRDiscoveryID, cacheID domain.ClusterCacheID) {
	ids := f.discoveriesOf[cacheID]
	delete(ids, id)
	if len(ids) == 0 {
		delete(f.discoveriesOf, cacheID)
	}
	f.mark(cacheID)
}

func (f *syncHealthFold) applyDiscovery(ch beehive.ObjectChange[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]) {
	// Deletion-pending counts as gone (same rule as watchListChan): a child on its
	// way out must not keep counting toward its cache's verdict.
	if ch.Object == nil || ch.Type == beehive.Deleted || ch.Object.DeletionRequestedAt != nil {
		id := domain.ClusterCacheGVRDiscoveryID(ch.ID)
		f.unlinkDiscovery(id, f.cacheOf[id])
		delete(f.cacheOf, id)
		return
	}
	f.putDiscovery(ch.Object)
}

func (f *syncHealthFold) putSync(obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]) {
	rec := gvrSyncRec{
		discoveryID: ownerObjectID(obj),
		apiVersion:  obj.Spec.APIVersion,
		resource:    obj.Spec.Resource,
		conditions:  obj.Conditions,
	}
	// Mark both the old and the new cache: a record that moved anchors leaves the one it
	// came from with a stale count.
	if prev, ok := f.syncs[obj.ID]; ok && prev.discoveryID != rec.discoveryID {
		f.unlinkSync(obj.ID, prev.discoveryID)
	}
	f.syncs[obj.ID] = rec
	if f.byDiscovery[rec.discoveryID] == nil {
		f.byDiscovery[rec.discoveryID] = map[beehive.ObjectID]struct{}{}
	}
	f.byDiscovery[rec.discoveryID][obj.ID] = struct{}{}
	f.mark(f.cacheOf[rec.discoveryID])
}

// unlinkSync drops one per-kind record from its anchor's index, marking the cache it left.
func (f *syncHealthFold) unlinkSync(id beehive.ObjectID, discoveryID domain.ClusterCacheGVRDiscoveryID) {
	ids := f.byDiscovery[discoveryID]
	delete(ids, id)
	if len(ids) == 0 {
		delete(f.byDiscovery, discoveryID)
	}
	f.mark(f.cacheOf[discoveryID])
}

func (f *syncHealthFold) applySync(ch beehive.ObjectChange[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]) {
	// Deletion-pending counts as gone — see applyDiscovery.
	if ch.Object == nil || ch.Type == beehive.Deleted || ch.Object.DeletionRequestedAt != nil {
		if prev, ok := f.syncs[ch.ID]; ok {
			f.unlinkSync(ch.ID, prev.discoveryID)
		}
		delete(f.syncs, ch.ID)
		return
	}
	f.putSync(ch.Object)
}

// flush recomputes the caches marked dirty and republishes when any of them moved.
func (f *syncHealthFold) flush() {
	if len(f.dirty) == 0 {
		return
	}
	// One lock acquisition for the whole flush — see StatsSnapshot.
	stats := f.stats()

	changed := false
	for cacheID := range f.dirty {
		anchors := f.discoveriesOf[cacheID]
		// No anchor left ⇒ the cache is gone (the controller ensures the anchor for
		// a cache's whole life), so its verdict is DROPPED, not recomputed —
		// recomputing would republish a permanent "no kinds yet" Unknown forever and
		// grow every snapshot without bound.
		if len(anchors) == 0 {
			if _, ok := f.published[cacheID]; ok {
				delete(f.published, cacheID)
				changed = true
			}
			continue
		}

		acc := &syncHealthAcc{}
		for discoveryID := range anchors {
			for id := range f.byDiscovery[discoveryID] {
				acc.add(f.syncs[id], stats[domain.ClusterCacheGVRSyncID(id)])
			}
		}
		health := acc.verdict(cacheID)
		if prev, ok := f.published[cacheID]; ok && syncHealthEqual(prev, health) {
			continue
		}
		f.published[cacheID] = health
		changed = true
	}
	clear(f.dirty)

	if !changed {
		return
	}
	// Fresh map: the published one is read concurrently.
	next := make(syncHealthSnapshot, len(f.published))
	maps.Copy(next, f.published)
	_ = f.out.Send(next) // only fails once the hub is closed, which ends the fold anyway
}

// syncHealthEqual compares two rollups, dereferencing the stamps (pointers to equal times
// are still equal readings).
func syncHealthEqual(a, b domain.ClusterCacheSyncHealth) bool {
	return a.Status == b.Status && a.Reason == b.Reason &&
		slices.Equal(a.UnhealthyKindRefs, b.UnhealthyKindRefs) &&
		a.TotalKinds == b.TotalKinds && a.UnhealthyKinds == b.UnhealthyKinds &&
		domain.TimePtrEqual(a.LastUpdateAt, b.LastUpdateAt) && domain.TimePtrEqual(a.LastLiveAt, b.LastLiveAt)
}
