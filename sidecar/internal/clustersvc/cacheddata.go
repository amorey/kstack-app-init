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

// What a ClusterCache holds: the discovered kind catalog and the cached Kubernetes
// objects and Events read back out of it, each paired with its delta-watch frame,
// plus the CachedData implementation. Mirrors the cluster-data section of
// graph/schema.graphqls.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
)

// ClusterCachedDataKind is one entry in a cluster's discovered kind catalog — a kind the API
// server advertises, built-in or CRD. It powers the dashboard's dynamic resource nav,
// which is why it carries the plural resource name (to dedupe against the curated
// catalog) and the api group via APIVersion (to bucket the kind into a nav group).
type ClusterCachedDataKind struct {
	// APIVersion is the group/version, e.g. "apps/v1" or "v1" for the core group.
	APIVersion string
	// Kind is the Kind name, e.g. "Deployment".
	Kind string
	// Resource is the plural lowercase URL form, e.g. "deployments".
	Resource string
	// Scope is "Namespaced" or "Cluster".
	Scope string
	// IsCRD is true when the kind is backed by a CustomResourceDefinition.
	IsCRD bool
	// Count is the number of objects of this kind currently in the cache (0 for a
	// kind the API server advertises but has no cached instances of).
	Count int
}

// ClusterCachedDataKindWatchFrame is one frame on a cache's kind-catalog watch. Consumers
// key on APIVersion + Resource; a kind whose Count changes arrives as Modified.
// CacheID is provenance: a client watching the active cache uses it to reject a late
// frame from a superseded one still draining after a switch.
type ClusterCachedDataKindWatchFrame struct {
	Type DeltaFrameType
	// Kind is nil on a Bookmark.
	Kind    *ClusterCachedDataKind
	CacheID ClusterCacheID
}

// ClusterCachedDataEvent is one cached Kubernetes Event from a cluster's synced data,
// powering the dashboard's events table. The involved-object identity is flattened
// onto the record and any field of it may be empty (a name-only reference carries no
// namespace); the raw event body is not exposed.
type ClusterCachedDataEvent struct {
	// UID is the Event's own object UID — the stable identity a watch keys on.
	UID string
	// Type is the event severity — conventionally Normal or Warning, but an open string
	// (Kubernetes doesn't constrain Event.type), so it's passed through verbatim rather
	// than bound to the closed EventType enum. Empty when the source Event omitted it.
	Type string
	// Reason is the CamelCase machine reason, e.g. "BackOff" (empty if unset).
	Reason string
	// Message is the human-readable detail (empty if unset).
	Message string
	// Count is how many times the event has fired (coalesced series count; >= 1).
	Count int
	// FirstSeen/LastSeen are the first and latest occurrence times (zero when the
	// source Event carried no timestamp).
	FirstSeen time.Time
	LastSeen  time.Time
	// InvolvedKind/InvolvedNamespace/InvolvedName identify the object the event is
	// about (any may be empty).
	InvolvedKind      string
	InvolvedNamespace string
	InvolvedName      string
}

// ClusterCachedDataEventWatchFrame is one frame on a cache's events watch. Consumers key on
// UID; a re-firing event arrives as Modified (its Count/LastSeen move), and one that
// ages out of the newest-window snapshot arrives as Deleted carrying its last-known
// row. CacheID is provenance, as on ClusterCachedDataKindWatchFrame.
type ClusterCachedDataEventWatchFrame struct {
	Type DeltaFrameType
	// Event is nil on a Bookmark.
	Event   *ClusterCachedDataEvent
	CacheID ClusterCacheID
}

// ClusterCachedDataObject is one cached Kubernetes object read from the active cache. The
// typed identity fields are enough to key the watch, sort, and render Name/Namespace/
// Age without parsing the body; RawJSON carries the full native body, from which the
// frontend derives kind-specific columns. Keeping RawJSON in the struct is what makes
// an in-place edit differ across two reads and surface as Modified — and its string
// underlying type is what keeps the struct comparable for that diff.
type ClusterCachedDataObject struct {
	// UID is the object's UID — the stable identity a watch keys on.
	UID string
	// APIVersion is the group/version, e.g. "apps/v1".
	APIVersion string
	// Kind is the Kind name, e.g. "Deployment".
	Kind string
	// Namespace is the object's namespace (empty for a cluster-scoped kind).
	Namespace string
	// Name is the object's name.
	Name string
	// CreationTimestamp drives the universal Age column (zero when the source object
	// carried none; the field resolver maps zero → null).
	CreationTimestamp time.Time
	// RawJSON is the object's full native body (JSON), forwarded verbatim from the cache
	// (managedFields + the kubectl last-applied annotation stripped at write time). Empty
	// only if the store held no body; the field resolver serves it as the JSON scalar.
	RawJSON RawJSON
}

// ClusterCachedDataObjectWatchFrame is one frame on a cache's per-kind objects watch.
// Consumers key on UID. Provenance is CacheID *plus* (APIVersion, Resource): this
// watch is keyed by kind as well as cache, so a client switching resources within one
// cache needs the kind to reject a straggler from the previous subscription.
type ClusterCachedDataObjectWatchFrame struct {
	Type DeltaFrameType
	// Object is nil on a Bookmark.
	Object     *ClusterCachedDataObject
	CacheID    ClusterCacheID
	APIVersion string
	Resource   string
}

// The trailing edges each watch collapses a burst of writes onto. Three, because they do
// not carry the same load: events are the highest-volume stream and the one that storms,
// so its window is the loosest. Each is what a re-read costs at worst, and the bus's own
// coalescing is not a substitute — it merges only what a reader has not yet taken.
const (
	dataKindsDebounce   = 250 * time.Millisecond
	dataObjectsDebounce = 250 * time.Millisecond
	dataEventsDebounce  = 500 * time.Millisecond
)

// dataRetryInterval paces a re-read that failed. Needed because the change bus is keyed by
// what was written: a kind nobody writes to may not ping for hours, so one transient error
// would leave the client's table empty until something else moved.
const dataRetryInterval = 2 * time.Second

// gate resolves the (cluster, cache) pair every method is scoped by. ok false is a pair
// that will never resolve — no such cache, or one belonging to another cluster — which the
// callers answer as definitively empty rather than as an error or a wait.
func (a cachedDataAPI) gate(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (bool, error) {
	obj, err := a.s.cacheClient.Get(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cluster cache %d: %w", cacheID, err)
	}
	owner, ok, err := obj.Owner()
	if err != nil {
		return false, fmt.Errorf("read cluster cache %d owner: %w", cacheID, err)
	}
	return ok && ClusterID(owner.ID) == clusterID, nil
}

// cacheGone reports the two sentinels that mean a cache went away rather than broke: a
// clear or a teardown took it. Both are expected, so a read answers empty and a watch ends
// clean. **Every other open failure is a storage fault** — a corrupt file, a permission,
// a migration that would not run — and reporting it as an empty cache would tell a user
// their cluster serves nothing, with nothing to act on.
func cacheGone(err error) bool {
	return errors.Is(err, kubestore.ErrRemoved) || errors.Is(err, kubestore.ErrClosed)
}

func (a cachedDataAPI) ListKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) ([]ClusterCachedDataKind, error) {
	live, err := a.gate(ctx, clusterID, cacheID)
	if err != nil || !live {
		return nil, err
	}
	// Claims rather than borrowing an open file: nothing holds a cache open while it is
	// idle, so a paused one would otherwise read empty over rows still on disk. Never an
	// OpenOrCreate, which would resurrect a cache mid-teardown.
	store, ok, err := a.s.kubestoreMgr.OpenExisting(int64(cacheID))
	switch {
	case err != nil && !cacheGone(err):
		return nil, fmt.Errorf("open cache %d: %w", cacheID, err)
	case err != nil, !ok:
		// Gone under us, or no file at all — never synced. Empty, not an error.
		return nil, nil
	}
	defer store.Release()

	rows, err := store.Kinds(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cache %d kinds: %w", cacheID, err)
	}
	kinds := make([]ClusterCachedDataKind, 0, len(rows))
	for _, r := range rows {
		kinds = append(kinds, toCachedDataKind(r))
	}
	return kinds, nil
}

func (a cachedDataAPI) WatchKinds(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCachedDataKindWatchFrame], error) {
	bookmark := ClusterCachedDataKindWatchFrame{Type: DeltaFrameBookmark, CacheID: cacheID}
	live, err := a.gate(ctx, clusterID, cacheID)
	if err != nil {
		return nil, err
	}
	if !live {
		return emptyStream(ctx, bookmark), nil
	}
	return runCachedDataWatch(ctx, cachedDataWatchSpec[kubestore.KindRow, ClusterCachedDataKindWatchFrame]{
		stores:  a.s.kubestoreMgr,
		cacheID: int64(cacheID),
		// Every key: a kind's count moves on an object write, and the hardcoded
		// ('v1','Event') count on an event write, and the catalog itself on a sweep.
		debounce: dataKindsDebounce,
		retry:    dataRetryInterval,
		key:      func(r kubestore.KindRow) string { return r.APIVersion + "/" + r.Resource },
		changed:  func(a, b kubestore.KindRow) bool { return a != b },
		read: func(ctx context.Context, s *kubestore.Store) ([]kubestore.KindRow, error) {
			return s.Kinds(ctx)
		},
		frame: func(t DeltaFrameType, r kubestore.KindRow) ClusterCachedDataKindWatchFrame {
			kind := toCachedDataKind(r)
			return ClusterCachedDataKindWatchFrame{Type: t, Kind: &kind, CacheID: cacheID}
		},
		bookmark: bookmark,
	}), nil
}

func (a cachedDataAPI) WatchObjects(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID, apiVersion, resource string) (*Stream[ClusterCachedDataObjectWatchFrame], error) {
	bookmark := ClusterCachedDataObjectWatchFrame{
		Type: DeltaFrameBookmark, CacheID: cacheID, APIVersion: apiVersion, Resource: resource,
	}
	live, err := a.gate(ctx, clusterID, cacheID)
	if err != nil {
		return nil, err
	}
	if !live {
		return emptyStream(ctx, bookmark), nil
	}
	return runCachedDataWatch(ctx, cachedDataWatchSpec[kubestore.ObjectRow, ClusterCachedDataObjectWatchFrame]{
		stores:   a.s.kubestoreMgr,
		cacheID:  int64(cacheID),
		busKeys:  []string{kubestore.ObjectsKey(apiVersion, resource)},
		debounce: dataObjectsDebounce,
		retry:    dataRetryInterval,
		key:      func(r kubestore.ObjectRow) string { return r.UID },
		// The server bumps resourceVersion on every write, so it says an in-place edit
		// happened without inflating the body — which the whole-struct compare the other
		// two use would do for every row on every ping.
		changed: func(a, b kubestore.ObjectRow) bool { return a.ResourceVersion != b.ResourceVersion },
		read: func(ctx context.Context, s *kubestore.Store) ([]kubestore.ObjectRow, error) {
			return s.Objects(ctx, apiVersion, resource)
		},
		frame: func(t DeltaFrameType, r kubestore.ObjectRow) ClusterCachedDataObjectWatchFrame {
			obj := toCachedDataObject(r)
			return ClusterCachedDataObjectWatchFrame{
				Type: t, Object: &obj, CacheID: cacheID, APIVersion: apiVersion, Resource: resource,
			}
		},
		bookmark: bookmark,
	}), nil
}

func (a cachedDataAPI) WatchEvents(ctx context.Context, clusterID ClusterID, cacheID ClusterCacheID) (*Stream[ClusterCachedDataEventWatchFrame], error) {
	bookmark := ClusterCachedDataEventWatchFrame{Type: DeltaFrameBookmark, CacheID: cacheID}
	live, err := a.gate(ctx, clusterID, cacheID)
	if err != nil {
		return nil, err
	}
	if !live {
		return emptyStream(ctx, bookmark), nil
	}
	return runCachedDataWatch(ctx, cachedDataWatchSpec[kubestore.EventRow, ClusterCachedDataEventWatchFrame]{
		stores:   a.s.kubestoreMgr,
		cacheID:  int64(cacheID),
		busKeys:  []string{kubestore.EventsKey},
		debounce: dataEventsDebounce,
		retry:    dataRetryInterval,
		key:      func(r kubestore.EventRow) string { return r.UID },
		changed:  func(a, b kubestore.EventRow) bool { return a != b },
		read: func(ctx context.Context, s *kubestore.Store) ([]kubestore.EventRow, error) {
			// The window doubles as the diff window: a row that ages out of it is one the
			// watch reports as gone.
			return s.Events(ctx, kubestore.DefaultEventsLimit)
		},
		frame: func(t DeltaFrameType, r kubestore.EventRow) ClusterCachedDataEventWatchFrame {
			ev := toCachedDataEvent(r)
			return ClusterCachedDataEventWatchFrame{Type: t, Event: &ev, CacheID: cacheID}
		},
		bookmark: bookmark,
	}), nil
}

// emptyStream is the answer for a scope that can never be filled: the Bookmark, then the
// end. Never for one whose cache has merely not synced yet — that is a wait, and the watch
// serves it by binding when a file appears.
func emptyStream[F any](ctx context.Context, bookmark F) *Stream[F] {
	return NewStream(ctx, func(ctx context.Context, out chan<- F) error {
		sendFrame(ctx, out, bookmark)
		return nil
	})
}

// toCachedDataKind projects one catalog row.
func toCachedDataKind(r kubestore.KindRow) ClusterCachedDataKind {
	return ClusterCachedDataKind{
		APIVersion: r.APIVersion,
		Kind:       r.Kind,
		Resource:   r.Resource,
		Scope:      r.Scope,
		IsCRD:      r.IsCRD,
		Count:      r.Count,
	}
}

// toCachedDataEvent projects one event row; a zero stamp becomes the zero time, which the
// field resolver maps to null.
func toCachedDataEvent(r kubestore.EventRow) ClusterCachedDataEvent {
	return ClusterCachedDataEvent{
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

// toCachedDataObject projects one object row. **This is where the body is decompressed**,
// so only a row that becomes a frame pays for it — the read hands back what is stored, and
// the diff never inflates a row that did not move. A body that will not decompress is
// served empty rather than failing the watch: one malformed row must not end a collection's
// stream.
func toCachedDataObject(r kubestore.ObjectRow) ClusterCachedDataObject {
	body, err := r.Body()
	if err != nil {
		body = nil
	}
	return ClusterCachedDataObject{
		UID:               r.UID,
		APIVersion:        r.APIVersion,
		Kind:              r.Kind,
		Namespace:         r.Namespace,
		Name:              r.Name,
		CreationTimestamp: millisToTime(r.CreatedAt),
		RawJSON:           RawJSON(body),
	}
}

// millisToTime turns a stored unix-millis stamp into a time; zero stays zero, which the
// field resolvers already map to null.
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
