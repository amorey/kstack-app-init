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
	"log/slog"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// gvrSyncAnchorFilter keeps only the sync records owned by one cache's discovery
// anchor (resolve returns it; 0 = no anchor yet; onErr fires on failed reads).
// The underlying watch is fleet-wide, so it memoizes the anchors it has RULED OUT —
// otherwise each frame costs a point query while our anchor is unresolved. The memo
// is licensed because a verdict never flips: anchor ids are AUTOINCREMENT and never
// reused, so a not-ours anchor cannot become ours.
func gvrSyncAnchorFilter(
	resolve func() (beehive.ObjectID, error),
	onErr func(error),
) func(domain.ClusterCacheGVRSyncWatchFrame) []domain.ClusterCacheGVRSyncWatchFrame {
	notOurs := map[beehive.ObjectID]bool{}
	// Frames received while the anchor could not be read are HELD, not dropped:
	// beehive re-emits an object only when it changes, so a frame lost in a
	// transient read error would leave that kind invisible for the subscription's
	// life. Released once a read succeeds.
	var undecided []domain.ClusterCacheGVRSyncWatchFrame

	return func(c domain.ClusterCacheGVRSyncWatchFrame) []domain.ClusterCacheGVRSyncWatchFrame {
		// The Bookmark carries no record to judge, but it must not overtake held
		// frames — it would declare the snapshot complete while part of it is still
		// undecided. Queued behind them (past the cap too: losing it would leave the
		// client loading forever, which is worse than one frame over budget).
		if c.Type == domain.ChangeBookmark {
			if len(undecided) > 0 {
				undecided = append(undecided, c)
				return nil
			}
			return []domain.ClusterCacheGVRSyncWatchFrame{c}
		}
		// A hard Deleted carries no owner edge; forward on id alone (removal of an
		// id the client never added is a no-op).
		if c.Sync.DiscoveryID == 0 {
			return []domain.ClusterCacheGVRSyncWatchFrame{c}
		}
		theirs := beehive.ObjectID(c.Sync.DiscoveryID)
		// Take the read whenever frames are held: held frames need an anchor to be
		// judged against, and post-recovery traffic is mostly ruled-out ids, so
		// short-circuiting on the memo alone would hold them forever.
		if notOurs[theirs] && len(undecided) == 0 {
			return nil
		}
		anchor, err := resolve()
		if err != nil {
			onErr(err)
			// A failed read must not rule the id out, nor lose the frame — unless
			// it's already known not-ours.
			if !notOurs[theirs] && len(undecided) < maxUndecidedSyncFrames {
				undecided = append(undecided, c)
			}
			return nil
		}

		// Judge everything held, oldest first — the consumer upserts by id, so a
		// superseded frame ahead of its successor is harmless.
		var out []domain.ClusterCacheGVRSyncWatchFrame
		for _, held := range undecided {
			if held.Type == domain.ChangeBookmark || beehive.ObjectID(held.Sync.DiscoveryID) == anchor {
				out = append(out, held)
			}
		}
		undecided = nil

		if anchor == theirs {
			return append(out, c)
		}
		notOurs[theirs] = true
		return out
	}
}

// maxUndecidedSyncFrames bounds held frames: covers a cold start's burst, caps the
// memory a permanently broken read can cost.
const maxUndecidedSyncFrames = 4096

func (a syncsAPI) Watch(ctx context.Context, cacheID domain.ClusterCacheID) (<-chan domain.ClusterCacheGVRSyncWatchFrame, error) {
	// The anchor is resolved lazily and re-resolved as the stream runs — a
	// subscribe-time miss (a just-created cache) must not leave the stream
	// permanently empty; while unresolved, dropping frames is correct (an
	// anchorless cache owns no sync records).
	anchorName := domain.ClusterCacheGVRDiscoveryName(beehive.ObjectID(cacheID))
	var (
		discoveryID beehive.ObjectID
		loggedErr   bool
	)
	// resolveAnchor separates "no anchor yet" (normal) from "the read failed"
	// (surfaced: fail at subscribe, log once mid-stream — a silently dropped frame
	// is never replayed). A resolved id is kept for the stream's life: an anchor
	// lives as long as its cache and ids are never reused, so there is no
	// "same cache, different anchor" to invalidate for.
	resolveAnchor := func() (beehive.ObjectID, error) {
		if discoveryID != 0 {
			return discoveryID, nil
		}
		anchor, err := a.s.gvrDiscoveryClient.GetByName(ctx, anchorName)
		switch {
		case err == nil:
			discoveryID = anchor.ID
			return discoveryID, nil
		case errors.Is(err, beehive.ErrNotFound):
			return 0, nil // not created yet; try again on the next frame
		default:
			return 0, err
		}
	}
	if _, err := resolveAnchor(); err != nil {
		return nil, err
	}

	snap, src, err := a.s.gvrSyncClient.WatchList(ctx, beehive.WithLoads(beehive.LoadOwner()))
	if err != nil {
		return nil, err
	}
	return filterChan(ctx, watchListChan(ctx, "ClusterCacheGVRSync", snap, src,
		func(t domain.ChangeType, id beehive.ObjectID, obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]) domain.ClusterCacheGVRSyncWatchFrame {
			if t == domain.ChangeBookmark {
				return domain.ClusterCacheGVRSyncWatchFrame{Type: t}
			}
			if obj == nil {
				// A hard Deleted carries no object; forwarded — a stray removal is a
				// no-op for the client.
				return domain.ClusterCacheGVRSyncWatchFrame{Type: t, Sync: &domain.ClusterCacheGVRSync{ID: domain.ClusterCacheGVRSyncID(id)}}
			}
			gs := buildGVRSync(obj)
			return domain.ClusterCacheGVRSyncWatchFrame{Type: t, Sync: &gs}
		}), gvrSyncAnchorFilter(resolveAnchor, func(err error) {
		if loggedErr {
			return
		}
		loggedErr = true
		slog.Warn("clusterservice: resolve cache discovery anchor", "cache", cacheID, "err", err)
	})), nil
}

// Get implements Syncs. An unknown or deletion-pending id is (nil, nil), not an
// error — the same posture as Clusters().Get. The owner edge is eager-loaded so
// the record carries its DiscoveryID.
func (a syncsAPI) Get(ctx context.Context, id domain.ClusterCacheGVRSyncID) (*domain.ClusterCacheGVRSync, error) {
	obj, err := a.s.gvrSyncClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if obj.DeletionRequestedAt != nil {
		return nil, nil
	}
	gs := buildGVRSync(obj)
	return &gs, nil
}

// List implements Syncs — one cache's per-kind records in creation order, or every
// tracked record when cacheID is nil. Scoped by the cache, not its discovery anchor:
// there is exactly one anchor per cache and its name is derived from the cache id, so
// the hop is an implementation detail the caller never supplies (Watch keys the same
// way). A cache whose discovery pass has never run has no anchor and therefore no
// records — empty, not an error.
func (a syncsAPI) List(ctx context.Context, cacheID *domain.ClusterCacheID) ([]*domain.ClusterCacheGVRSync, error) {
	var (
		objs []*beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]
		err  error
	)
	if cacheID != nil {
		anchorName := domain.ClusterCacheGVRDiscoveryName(beehive.ObjectID(*cacheID))
		anchor, anchorErr := a.s.gvrDiscoveryClient.GetByName(ctx, anchorName)
		if errors.Is(anchorErr, beehive.ErrNotFound) {
			return nil, nil
		}
		if anchorErr != nil {
			return nil, anchorErr
		}
		objs, err = a.s.gvrSyncClient.ListOwnedObjects(ctx, anchor.ID, beehive.LoadOwner())
	} else {
		objs, err = a.s.gvrSyncClient.List(ctx, beehive.LoadOwner())
	}
	if err != nil {
		return nil, err
	}
	syncs := make([]*domain.ClusterCacheGVRSync, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		gs := buildGVRSync(obj)
		syncs = append(syncs, &gs)
	}
	return syncs, nil
}

// buildGVRSync assembles a domain ClusterCacheGVRSync from a single beehive object, its
// owning discovery anchor read off the eager-loaded owner edge.
func buildGVRSync(obj *beehive.Object[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]) domain.ClusterCacheGVRSync {
	return domain.ClusterCacheGVRSync{
		ID:          domain.ClusterCacheGVRSyncID(obj.ID),
		DiscoveryID: ownerObjectID(obj),
		Spec:        obj.Spec,
		Conditions:  obj.Conditions,
	}
}

// GetStats implements Syncs — one synced kind's freshness stamps, read straight
// from the controller whose worker reported them. A nil controller (a test
// service with no control plane) reads as "nothing reported".
func (a syncsAPI) GetStats(_ context.Context, id domain.ClusterCacheGVRSyncID) (*domain.ClusterCacheGVRSyncStats, error) {
	if a.s.gvrSyncCtrl == nil {
		return nil, nil
	}
	st, ok := a.s.gvrSyncCtrl.Stats(beehive.ObjectID(id))
	if !ok {
		return nil, nil
	}
	return &st, nil
}

// SnapshotStats implements Syncs — every synced kind's stamps under one lock,
// for a caller folding a whole cache's worth at once.
func (a syncsAPI) SnapshotStats() map[domain.ClusterCacheGVRSyncID]domain.ClusterCacheGVRSyncStats {
	if a.s.gvrSyncCtrl == nil {
		return nil
	}
	return a.s.gvrSyncCtrl.StatsSnapshot()
}

// ListEvents implements Syncs — the sync-transition history lives on the
// per-kind child, so the caller keys on the child's id.
func (a syncsAPI) ListEvents(ctx context.Context, id domain.ClusterCacheGVRSyncID, category *string, limit *int) ([]domain.Event, error) {
	return a.s.events(ctx, a.s.gvrSyncClient, beehive.ObjectID(id), category, limit)
}

// WatchEvents implements Syncs — one synced kind's entrypoint to the generic
// event watch.
func (a syncsAPI) WatchEvents(ctx context.Context, id domain.ClusterCacheGVRSyncID, category *string) (<-chan domain.Event, error) {
	return a.s.watchEvents(ctx, a.s.gvrSyncClient, beehive.ObjectID(id), category)
}
