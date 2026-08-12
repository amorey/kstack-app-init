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

// The ClusterCache kind and the two child kinds beneath it (ClusterCacheGVRDiscovery,
// ClusterCacheGVRSync): each kind's beehive shapes, the domain record served to
// resolvers, and its delta-watch change, plus the whole-cache sync verdict folded from
// the per-kind children. Mirrors the ClusterCache section of graph/schema.graphqls.
package domain

import (
	"strconv"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// ClusterCacheName returns a ClusterCache's name, "{ClusterID}/{serverUID}":
// one cache per identity per cluster, so a UID migration yields a second
// coexisting cache. A creation/dedup key only — caches are enumerated through the
// owner edge.
func ClusterCacheName(clusterID ClusterID, serverUID string) string {
	return strconv.FormatInt(int64(clusterID), 10) + "/" + serverUID
}

// NewCacheRef builds the on-disk cache locator — the single beehive
// ObjectID→int64 conversion point, keeping the store package beehive-free.
func NewCacheRef(clusterObjID, cacheObjID beehive.ObjectID) store.CacheRef {
	return store.CacheRef{ClusterID: int64(clusterObjID), CacheID: int64(cacheObjID)}
}

// ClusterCacheGroupKind identifies the ClusterCache beehive resource kind.
var ClusterCacheGroupKind = beehive.GroupKind{Kind: "ClusterCache"}

// ClusterCacheSpec: ServerUID is the kube-system UID this cache mirrors — the
// identity a migration turns over. Written at creation by the core controller;
// the cache controller compares it against the parent's last-probed Server.UID to
// decide active-ness (it reads spec.ServerUID, never the name).
type ClusterCacheSpec struct {
	ServerUID string `json:"serverUid"`
}

// ClusterCacheStatus is empty — this kind reports through its conditions alone;
// the rollup a UI wants is served read-side by Caches().WatchSyncHealth.
// See docs/adr/2026-08-09-status-propagation-gauges.md.
type ClusterCacheStatus struct{}

// EventsKind / EventsAPIVersion / EventsResource identify the Event collection —
// an ordinary synced kind writing to its own table (eventsync). The server serves
// the same events under two spellings backed by one store, so exactly one may be
// synced: canonical `v1`; the discovery filter drops EventsAltGroup. See
// docs/adr/2026-08-09-kubesync-watch-poll.md.
const (
	EventsKind       = "Event"
	EventsAPIVersion = "v1"
	EventsResource   = "events"
	EventsAltGroup   = "events.k8s.io"
)

// ClusterCache is the domain view of one ClusterCache beehive object, streamed
// standalone via Caches().Watch and joined to its parent client-side. ID is the cache's
// own ObjectID and ClusterID its parent's — together they locate the on-disk cache and
// let the client join the cache onto its cluster. ServerUID is the kube-system UID this
// cache mirrors; the client treats the cache whose ServerUID matches the cluster's
// last-probed Server.UID as active. Active-ness is deliberately not a field: it depends
// on the parent's status, which changes with no cache event, so only the client's live
// join is correct.
type ClusterCache struct {
	ID        ClusterCacheID
	ClusterID ClusterID
	ServerUID string
	// Conditions are beehive object conditions, not part of Status — read off the
	// object rather than out of the status blob.
	Conditions []Condition
}

// ClusterCacheChange is the ClusterCache-kind counterpart of ClusterChange, carried
// on the standalone cache watch. Binds 1:1 to the GraphQL ClusterCacheChange.
type ClusterCacheChange struct {
	Type  ChangeType
	Cache *ClusterCache
}

// ClusterCacheStats reports a cluster's live on-disk cache statistics.
type ClusterCacheStats struct {
	Exists bool
	Bytes  int64
	// ObjectCount/KindCount are the whole-cache rollup read from the kind
	// catalog's trigger-maintained per-kind counts: the total cached objects and
	// the number of kinds with at least one cached object. Both exclude events
	// (events are not a catalog kind).
	ObjectCount int
	KindCount   int
}

// --- ClusterCacheGVRDiscovery kind types ---

// ClusterCacheGVRDiscoveryGroupKind identifies the discovery anchor kind: one
// object per ClusterCache, owned by it; its controller maintains one
// ClusterCacheGVRSync child per served GVR.
var ClusterCacheGVRDiscoveryGroupKind = beehive.GroupKind{Kind: "ClusterCacheGVRDiscovery"}

// ClusterCacheGVRDiscoveryName returns "gvrdiscovery/{cacheObjID}" — exactly one
// per cache, so creation is idempotent under name-uniqueness dedup. A
// creation/dedup key only; the child is enumerated through the owner edge.
func ClusterCacheGVRDiscoveryName(cacheID beehive.ObjectID) string {
	return "gvrdiscovery/" + strconv.FormatInt(int64(cacheID), 10)
}

// ClusterCacheGVRDiscoverySpec is the desired discovery for one cache. Enabled is
// the pause switch, written by ClusterCacheController (the one evaluator of the
// sync rule) and relayed into each child. Existence means "has an anchor", NOT
// "is discovering" — the object lives as long as the cache, so its subtree
// survives a pause.
type ClusterCacheGVRDiscoverySpec struct {
	Enabled bool `json:"enabled"`
}

// ClusterCacheGVRDiscoveryStatus is empty, deliberately: status is a propagation
// channel — for state a dependent reacts to, not for pass gauges, which nothing
// in the object graph reads. The gauges live in controller memory (see
// ClusterCacheGVRDiscoveryStats); the Discovered condition (a beehive object row)
// is what remains. See docs/adr/2026-08-09-status-propagation-gauges.md.
type ClusterCacheGVRDiscoveryStatus struct{}

// ClusterCacheGVRDiscoveryStats is one cache's live discovery gauges, held in
// controller memory and read on request — never stored. Absent (nil at the
// service boundary) means this process has run no pass for that cache yet.
type ClusterCacheGVRDiscoveryStats struct {
	// LastDiscoveryAt is when the last pass reached the API server (partial included).
	LastDiscoveryAt time.Time
	// ResourceCount is how many syncable GVRs that pass saw — deliberately not
	// "how many children exist" (a partial pass leaves un-pruned survivors).
	ResourceCount int
}

// ClusterCacheGVRDiscovery is the domain view of one ClusterCacheGVRDiscovery beehive
// object: a cache's kind-catalog record — which kinds the cluster serves, when that was
// last confirmed, and whether the confirmation was complete. Streamed standalone via
// Discovery().Watch and joined onto its cache client-side by CacheID, exactly as
// its sibling sync records are; why the discovery anchor is its own kind is in the
// sync-surface note in CLAUDE.md. Spec and Status are the stored values served as-is —
// no projection, the ClusterCacheStatus precedent.
type ClusterCacheGVRDiscovery struct {
	ID      ClusterCacheGVRDiscoveryID
	CacheID ClusterCacheID
	Spec    ClusterCacheGVRDiscoverySpec
	// Conditions are beehive object conditions, read off the object rather than out of
	// the status blob — `Discovered`, carrying this component's own verdict. There is no
	// Status field: the kind's status is empty by design, and the pass's gauges are
	// served out of band (ClusterCacheGVRDiscoveryStats).
	Conditions []Condition
}

// ClusterCacheGVRDiscoveryChange is one delta on the GVR-discovery watch, the third of
// the parallel object streams (clusters, caches, gvr-discoveries). Binds
// 1:1 to the GraphQL ClusterCacheGVRDiscoveryChange; consumers key on Discovery.ID.
type ClusterCacheGVRDiscoveryChange struct {
	Type      ChangeType
	Discovery *ClusterCacheGVRDiscovery
}

// --- ClusterCacheGVRSync kind types ---

// ClusterCacheGVRSyncGroupKind identifies the per-GVR sync kind: one object per
// served GVR, owned by its ClusterCacheGVRDiscovery.
var ClusterCacheGVRSyncGroupKind = beehive.GroupKind{Kind: "ClusterCacheGVRSync"}

// ClusterCacheGVRSyncName returns "gvrsync/{discoveryObjID}/{apiVersion}/{resource}" —
// deterministic, so a discovery pass is a set reconcile with no per-child
// bookkeeping. (apiVersion, resource) rather than Kind: the plural is what the
// worker's REST path needs and what the server guarantees unique per group-version.
func ClusterCacheGVRSyncName(discoveryID beehive.ObjectID, apiVersion, resource string) string {
	return "gvrsync/" + strconv.FormatInt(int64(discoveryID), 10) + "/" + apiVersion + "/" + resource
}

// ClusterCacheGVRSyncSpec is the desired sync for one GVR, written wholly by the
// discovery controller. Enabled is the pause switch relayed down the chain (the
// child never re-derives it); identity fields refresh each discovery pass, so a
// kind that changes shape converges without recreation.
type ClusterCacheGVRSyncSpec struct {
	Enabled bool `json:"enabled"`
	// APIVersion is the group/version this kind is served at, e.g. "apps/v1" — or a bare
	// version ("v1") for the core group, matching the wire form Kubernetes uses.
	APIVersion string `json:"apiVersion"`
	// Kind is the singular Kind name, e.g. "Deployment".
	Kind string `json:"kind"`
	// Resource is the lowercase plural URL segment, e.g. "deployments".
	Resource string `json:"resource"`
	// Namespaced is true when objects of this kind live in a namespace.
	Namespaced bool `json:"namespaced"`
}

// ClusterCacheGVRSyncStatus is the observed sync state for one GVR. Empty placeholder.
type ClusterCacheGVRSyncStatus struct{}

// ClusterCacheGVRSync is the domain view of one ClusterCacheGVRSync beehive object: one
// Kubernetes kind being mirrored into a cache. Shaped like its sibling sync records —
// {ID, owner, Spec, Conditions} — but streamed **cache-scoped**, because there is one per
// served kind rather than one per cache and an unscoped stream of a hundred-plus records
// would be a firehose.
type ClusterCacheGVRSync struct {
	ID ClusterCacheGVRSyncID
	// DiscoveryID is the owning ClusterCacheGVRDiscovery — this kind hangs off the
	// discovery anchor, not the cache directly, so this is the join key a client already
	// has from the discovery stream.
	DiscoveryID ClusterCacheGVRDiscoveryID
	Spec        ClusterCacheGVRSyncSpec
	// Conditions carry `Synced` — this kind's own verdict, which is the whole reason the
	// record is served: a cache's hundred kinds fail independently, and the coarse
	// cache-level condition can't say which.
	Conditions []Condition
}

// ClusterCacheGVRSyncChange is one delta on the cache-scoped per-kind sync watch.
// Consumers key on Sync.ID.
type ClusterCacheGVRSyncChange struct {
	Type ChangeType
	Sync *ClusterCacheGVRSync
}

// ClusterCacheGVRSyncStats are one kind's sync freshness stamps — measured by its worker,
// held in ClusterCacheGVRSyncController's memory, and served on request. Out of status
// for the usual reason: nothing in the object graph reacts to them, and with a hundred of
// these per cache a stored stamp would mean a hundred parent wakes every heartbeat.
type ClusterCacheGVRSyncStats struct {
	// LastUpdateAt is when an object of this kind was last written to the cache; nil if
	// never.
	LastUpdateAt *time.Time
	// LastLiveAt is when the worker's watch last proved live (a delta or a bookmark);
	// nil if never.
	LastLiveAt *time.Time
}

// ClusterCacheSyncHealth is one cache's sync verdict, folded from every kind it syncs.
//
// It is a READ-SIDE projection, not a stored condition, and that is deliberate. The
// status-is-propagation rule cuts both ways: a value belongs on an object when a
// dependent reacts to it, and nothing in the object graph reacts to this — only a UI
// does. Storing it would mean waking the cache (and every watcher) each time any of its
// hundred-plus children changed verdict, to publish a number no controller reads.
//
// It exists because the per-kind records can't serve this consumer: there is one per
// synced kind, so the always-mounted cluster registry can't carry them, and no single
// child's verdict is the cache's — a cache with ninety-nine healthy kinds and one
// forbidden CRD is not healthy, and reading any one child would call it either way at
// random.
// SyncedKindRef identifies one synced kind exactly. The plural alone is what a UI
// renders, but it does not IDENTIFY a kind — a CRD may reuse a built-in's plural under
// another api group — so anything that keys on a kind (looking its sync record up, opening
// its timeline) needs the pair.
type SyncedKindRef struct {
	APIVersion string
	Resource   string
}

type ClusterCacheSyncHealth struct {
	CacheID ClusterCacheID
	// Status/Reason mirror a Condition's shape (and its reason vocabulary), so a consumer
	// renders this exactly as it renders a per-kind verdict.
	Status ConditionStatus
	Reason string
	// UnhealthyKindRefs names the kinds behind a non-Watching verdict, sorted — the
	// identity a consumer can act on (look the kind's record up, key a subscription on
	// it), not a rendered phrase. Empty when healthy. It is a list rather than a message
	// because truncation ("+2 more") is a layout decision, and the consumer that needs the
	// first offender must not have to parse it back out of prose.
	UnhealthyKindRefs []SyncedKindRef
	// TotalKinds is how many kinds this cache syncs; UnhealthyKinds how many are not
	// currently Watching (len(UnhealthyKindRefs), which is capped by nothing).
	TotalKinds     int
	UnhealthyKinds int
	// LastUpdateAt is the most recent write across every kind — when data last arrived
	// anywhere in this cache. LastLiveAt is the OLDEST proof among the kinds that have
	// one: the weakest link, since a cache is only as verified as its least-recently
	// proven watch. Both nil until some kind has reported.
	LastUpdateAt *time.Time
	LastLiveAt   *time.Time
}
