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
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// ListKinds implements Data. The caller names the exact cache whose catalog it
// wants; nil when that cache's db isn't open (never synced / sync paused),
// matching Caches().GetStats' degrade-to-empty posture.
func (a dataAPI) ListKinds(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) ([]domain.ClusterDataKind, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	db := a.s.cacheManager.Lookup(ref.CacheID)
	if db == nil {
		return nil, nil
	}
	rows, err := db.Kinds(ctx)
	if err != nil {
		return nil, err
	}
	return toDataKinds(rows), nil
}

// toDataKinds maps a catalog read onto the domain records, preserving the reader's
// (api_version, kind) order the delta watch's Added burst relies on. Shared by the
// query and its live counterpart so the projections can't disagree.
func toDataKinds(rows []store.KindRow) []domain.ClusterDataKind {
	kinds := make([]domain.ClusterDataKind, len(rows))
	for i, r := range rows {
		kinds[i] = toDataKind(r)
	}
	return kinds
}

// toDataKind maps a store KindRow onto the domain ClusterDataKind 1:1.
func toDataKind(r store.KindRow) domain.ClusterDataKind {
	return domain.ClusterDataKind{
		APIVersion: r.APIVersion,
		Kind:       r.Kind,
		Resource:   r.Resource,
		Scope:      r.Scope,
		IsCRD:      r.IsCRD,
		Count:      r.Count,
	}
}

// dataKindKey keys the diff: APIVersion + Resource is unique per cache.
func dataKindKey(k domain.ClusterDataKind) string {
	return k.APIVersion + "/" + k.Resource
}

// WatchKinds implements Data: the kind catalog as a delta watch (an object
// write that changes a count re-emits its kind as Modified). cacheDeltaWatch
// owns the cache-lifecycle + coalescing loop.
func (a dataAPI) WatchKinds(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterDataKindChange, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, a.s.cacheManager, ref.CacheID, a.s.dataKindsDebounce,
		catalogSubscribe,
		func(ctx context.Context, db *store.ClusterDB) ([]domain.ClusterDataKind, error) {
			rows, err := db.Kinds(ctx)
			if err != nil {
				return nil, err
			}
			return toDataKinds(rows), nil
		},
		dataKindKey,
		func(t domain.ChangeType, k domain.ClusterDataKind) domain.ClusterDataKindChange {
			return domain.ClusterDataKindChange{Type: t, Kind: k, CacheID: cacheID}
		},
	), nil
}

// catalogSubscribe wakes on a write to EITHER broker: the catalog's counts span
// both tables (object triggers + event triggers), and object-broker-only would
// freeze the Events badge on an event-busy, object-quiet cluster. This doesn't
// undercut the broker split — the split protects the EXPENSIVE per-kind object
// re-reads; Kinds is O(kinds) and debounced besides.
func catalogSubscribe(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
	objects, stopObjects := db.ObjectsSubscribe()
	events, stopEvents := db.EventsSubscribe()

	out := make(chan store.WriteWake, 1)
	done := make(chan struct{})
	go func() {
		// Closing out signals the db went away — but only when BOTH brokers have
		// closed; one alone still leaves the other's writes worth reporting.
		defer close(out)
		for {
			select {
			case <-done:
				return
			case _, ok := <-objects:
				if !ok {
					objects = nil // the broker closed; keep serving the other
					if events == nil {
						return
					}
					continue
				}
			case _, ok := <-events:
				if !ok {
					events = nil
					if objects == nil {
						return
					}
					continue
				}
			}
			// Coalesce: a buffered ping already says "something changed".
			select {
			case out <- store.WriteWake{}:
			default:
			}
		}
	}()
	return out, func() {
		close(done)
		stopObjects()
		stopEvents()
	}
}

// WatchEvents implements Data: the newest events window as a delta watch keyed
// by UID, on the events-only broker. The read is a bounded window, so an event
// aging out surfaces as Deleted even though its row may still exist — fine for
// a "latest events" table.
func (a dataAPI) WatchEvents(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID) (<-chan domain.ClusterDataEventChange, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, a.s.cacheManager, ref.CacheID, a.s.dataEventsDebounce,
		(*store.ClusterDB).EventsSubscribe,
		func(ctx context.Context, db *store.ClusterDB) ([]domain.ClusterDataEvent, error) {
			rows, err := db.Events(ctx, 0) // 0 → store's default window; ordered by last_seen DESC
			if err != nil {
				return nil, err
			}
			events := make([]domain.ClusterDataEvent, len(rows))
			for i, r := range rows {
				events[i] = toDataEvent(r)
			}
			return events, nil
		},
		func(e domain.ClusterDataEvent) string { return e.UID },
		func(t domain.ChangeType, e domain.ClusterDataEvent) domain.ClusterDataEventChange {
			return domain.ClusterDataEventChange{Type: t, Event: e, CacheID: cacheID}
		},
	), nil
}

// WatchObjects implements Data: one kind's cached objects as a delta watch
// keyed by UID. Each row carries the native body, so an in-place edit surfaces
// as Modified.
func (a dataAPI) WatchObjects(ctx context.Context, clusterID domain.ClusterID, cacheID domain.ClusterCacheID, apiVersion, resource string) (<-chan domain.ClusterDataObjectChange, error) {
	ref := domain.NewCacheRef(beehive.ObjectID(clusterID), beehive.ObjectID(cacheID))
	return cacheDeltaWatch(ctx, a.s.cacheManager, ref.CacheID, a.s.dataObjectsDebounce,
		// Keyed to (apiVersion, resource) so unrelated writes don't wake it. The
		// broker routes by plural resource, not Kind, so the key stays valid across
		// a CRD Kind remap; see docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource(apiVersion, resource)
		},
		func(ctx context.Context, db *store.ClusterDB) ([]domain.ClusterDataObject, error) {
			rows, err := db.Objects(ctx, apiVersion, resource) // ordered by (namespace, name)
			if err != nil {
				return nil, err
			}
			objects := make([]domain.ClusterDataObject, len(rows))
			for i, r := range rows {
				objects[i] = toDataObject(r)
			}
			return objects, nil
		},
		func(o domain.ClusterDataObject) string { return o.UID },
		func(t domain.ChangeType, o domain.ClusterDataObject) domain.ClusterDataObjectChange {
			return domain.ClusterDataObjectChange{Type: t, Object: o, CacheID: cacheID, APIVersion: apiVersion, Resource: resource}
		},
	), nil
}

// toDataObject maps a store ObjectRow onto ClusterDataObject 1:1 (unix-millis 0 →
// zero time, consistent across reads so the watch diff compares equal). RawJSON is
// part of the struct, so an in-place edit surfaces as Modified.
func toDataObject(r store.ObjectRow) domain.ClusterDataObject {
	return domain.ClusterDataObject{
		UID:               r.UID,
		APIVersion:        r.APIVersion,
		Kind:              r.Kind,
		Namespace:         r.Namespace,
		Name:              r.Name,
		CreationTimestamp: millisToTime(r.CreatedAt),
		RawJSON:           domain.RawJSON(r.RawJSON),
	}
}

// toDataEvent maps a store EventRow onto ClusterDataEvent 1:1 (unix-millis 0 →
// zero time).
func toDataEvent(r store.EventRow) domain.ClusterDataEvent {
	return domain.ClusterDataEvent{
		UID:               r.UID,
		Type:              r.Type,
		Reason:            r.Reason,
		Message:           r.Message,
		Count:             r.Count,
		FirstSeen:         millisToTime(r.FirstSeen),
		LastSeen:          millisToTime(r.LastSeen),
		InvolvedKind:      r.InvolvedKind,
		InvolvedNamespace: r.InvolvedNS,
		InvolvedName:      r.InvolvedName,
	}
}

// millisToTime converts unix-millis to time.Time (0 → zero Time), built so two
// reads of the same row compare equal in the watch diff.
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
