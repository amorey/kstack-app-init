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

// Package domain is the cluster subsystem's shared vocabulary: the four beehive
// kinds with their spec/status shapes, identity (ObjectID), conditions, the
// delta-watch change types, and the records mirrored into a per-cluster cache.
// Every type the GraphQL schema binds lives here.
//
// It is a leaf: the ClusterService boundary and the controllers beneath it both
// depend on it, and it depends on neither.
//
// The four beehive kinds and their ownership chain:
//
//	Cluster                   (name: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache              (name: "{ClusterID}/{serverUID}")
//	    ↓ owns
//	ClusterCacheGVRDiscovery  (one per cache)
//	    ↓ owns
//	ClusterCacheGVRSync       (one per served kind)
//
// Cluster objects are created directly by the kubeconfig importer (one per
// kube-context); there is no separate intake kind. Each source owns a disjoint name
// namespace within the one Cluster kind, so the importer reconciles by name
// (beehive's per-kind name-uniqueness rules out duplicates), and the on-disk cache is
// keyed separately by beehive ObjectIDs so the name's arbitrary text never reaches
// the filesystem.
//
// Cluster carries connection status (Connected, Healthy conditions + server/principal
// facts); its ClusterCache child carries sync status, folded per kind from the
// ClusterCacheGVRSync records below it.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// ClusterActiveUID returns the last-probed kube-system UID, or "" if never probed. It
// selects which owned ClusterCache is active.
func ClusterActiveUID(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if obj.Status != nil && obj.Status.Server.UID != nil {
		return *obj.Status.Server.UID
	}
	return ""
}

// CacheIsActive reports whether a cache mirrors its parent's currently-active identity; one
// for an unknown identity never is. The single definition of "active cache", shared by the
// cache controller's sync gating and the service's domain join.
func CacheIsActive(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	active := ClusterActiveUID(clusterObj)
	return active != "" && cacheUID == active
}

// MaxMessageLen caps a persisted message (on a condition or an event run). Both are read
// back on every frame of a whole-fleet watch, and their sources are unbounded: a raw
// client-go error, or a /readyz?verbose=true body, which routinely runs to kilobytes.
const MaxMessageLen = 200

// TruncateMessage caps s at MaxMessageLen bytes, appending an ellipsis when it overflows.
// (Byte-bounded; error strings are effectively ASCII.)
func TruncateMessage(s string) string {
	if len(s) <= MaxMessageLen {
		return s
	}
	return s[:MaxMessageLen] + "…"
}

// ErrNotFound is returned by client helpers when no cluster with the given id
// is tracked.
var ErrNotFound = errors.New("controllers: cluster not found")

// Names are per-kind reconcile/uniqueness keys, NOT identities (identity is the
// beehive ObjectID); each source prefixes its own namespace ("kubeconfig/", future
// "cloud/"). Nothing reads a Cluster back by name.
// See docs/adr/2026-08-09-beehive-control-plane.md.
const namePrefixKubeconfig = "kubeconfig/"

// KubeconfigName returns a kubeconfig-sourced Cluster's beehive name — the
// importer's natural key for one kube-context, not an identity (see ClusterID).
func KubeconfigName(contextName string) string {
	return namePrefixKubeconfig + contextName
}

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

// --- Identity ---

// ObjectID is the identity of a persisted object — the beehive ObjectID of any
// kind, opaque on the wire (a decimal string) and bound to the one GraphQL
// ObjectID scalar. The per-kind aliases below name the same type purely to
// document which kind an id refers to.
type ObjectID int64

// ClusterID identifies a cluster record: the beehive ObjectID of its Cluster
// object — opaque, source-agnostic, stable for the record's life (changes only on
// an explicit Delete). The source's natural key lives only on the beehive name.
type ClusterID = ObjectID

// ClusterCacheID identifies one ClusterCache record. A cluster can own several,
// so the cache id is not derivable from the cluster id.
type ClusterCacheID = ObjectID

// ClusterCacheGVRDiscoveryID identifies one cache's GVR-discovery child.
type ClusterCacheGVRDiscoveryID = ObjectID

// ClusterCacheGVRSyncID identifies one synced kind's ClusterCacheGVRSync object.
type ClusterCacheGVRSyncID = ObjectID

// parseObjectID parses an ObjectID from its decimal-string wire form; a
// malformed value is a client error surfaced through UnmarshalGQL.
func parseObjectID(s string) (ObjectID, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid object id %q: %w", s, err)
	}
	return ObjectID(n), nil
}

// MarshalGQL writes the ObjectID to the GraphQL ObjectID scalar as a quoted
// decimal string (its wire form).
func (id ObjectID) MarshalGQL(w io.Writer) {
	io.WriteString(w, strconv.Quote(strconv.FormatInt(int64(id), 10)))
}

// UnmarshalGQL parses the GraphQL ObjectID scalar into a typed ObjectID. Accepts
// string, json.Number (JSON-variable number), and int64/int (inline literal).
func (id *ObjectID) UnmarshalGQL(v any) error {
	switch t := v.(type) {
	case string:
		n, err := parseObjectID(t)
		if err != nil {
			return err
		}
		*id = n
	case json.Number:
		n, err := parseObjectID(t.String())
		if err != nil {
			return err
		}
		*id = n
	case int64:
		*id = ObjectID(t)
	case int:
		*id = ObjectID(t)
	default:
		return fmt.Errorf("ObjectID must be a string or integer, got %T", v)
	}
	return nil
}

// --- Conditions ---

// ConditionStatus is a condition's three-valued verdict, Kubernetes-style.
type ConditionStatus = beehive.ConditionStatus

const (
	ConditionTrue    = beehive.ConditionTrue
	ConditionFalse   = beehive.ConditionFalse
	ConditionUnknown = beehive.ConditionUnknown
)

// ConditionType names one independently-tracked aspect of a cluster
// record's observed state. Each type is owned by exactly one controller.
type ConditionType string

const (
	// ConditionConnected reports whether the last connection probe
	// reached the cluster's API server and resolved its identity facts.
	ConditionConnected ConditionType = "Connected"
	// ConditionHealthy reports the API server's own condition (its
	// readiness checks), as distinct from our ability to reach it.
	ConditionHealthy ConditionType = "Healthy"
	// ConditionSynced reports the state of a sync. It is reported at two levels: coarse
	// on the ClusterCache (did this cache decide to sync?) and per kind on each
	// ClusterCacheGVRSync, which is the verdict a UI wants.
	ConditionSynced ConditionType = "Synced"
	// ConditionDiscovered reports whether the cache's GVR discovery pass reached the
	// API server and enumerated the kinds it serves. A separate axis from Synced: a
	// cache can have a complete, current kind list while its per-kind workers are
	// still catching up, and a discovery outage says nothing about the workers
	// already running. Owned by ClusterCacheGVRDiscoveryController.
	ConditionDiscovered ConditionType = "Discovered"
)

// Condition reason constants — CamelCase machine-readable explanations for a
// condition's status, Kubernetes-style. Human detail goes in Message.
const (
	// reasonInactive: no connection is maintained — the record is orphaned,
	// archived, deactivated, or its source has no resolvable credentials.
	ReasonInactive = "Inactive"
	// reasonConnecting: a probe is owed but none has succeeded or failed yet
	// (a freshly-minted record awaiting its first pass).
	ReasonConnecting = "Connecting"
	// reasonConnected: the last connection probe succeeded.
	ReasonConnected = "Connected"
	// reasonResolveFailed: credentials could not be resolved from the
	// record's source (e.g. the kube-context vanished from the kubeconfig).
	ReasonResolveFailed = "ResolveFailed"
	// reasonProbeFailed: credentials resolved but the dial/identity probe
	// failed.
	ReasonProbeFailed = "ProbeFailed"
	// reasonReady: the API server reports its readiness checks passing.
	ReasonReady = "Ready"
	// reasonReadyzFailed: the API server responded but named failing checks.
	ReasonReadyzFailed = "ReadyzFailed"
	// reasonUnreachable: the health probe's transport failed outright.
	ReasonUnreachable = "Unreachable"
	// reasonNoConnection: health cannot be assessed without a live
	// connection this pass.
	ReasonNoConnection = "NoConnection"
	// reasonPaused: nothing is syncing — the record is sync-disabled, deactivated,
	// orphaned, or archived.
	ReasonPaused = "Paused"
	// reasonSyncing: the sync is starting or catching up. Condition-only — the
	// event vocabulary uses SyncStart/ResyncStart instead.
	ReasonSyncing = "Syncing"
	// reasonWatching: the watch is established and proven live — caught up and
	// streaming deltas.
	ReasonWatching = "Watching"
	// reasonSyncFailed: the worker itself failed (it could not start, or its run loop
	// exited) and is retrying with backoff.
	ReasonSyncFailed = "SyncFailed"
	// reasonDiscovered: the last discovery pass enumerated every group the API
	// server serves, and the per-GVR sync children match it.
	ReasonDiscovered = "Discovered"
	// reasonDiscoveryPartial: some groups answered, others didn't — the pass adds
	// children without pruning (a group that failed to answer is not shown gone).
	ReasonDiscoveryPartial = "DiscoveryPartial"
	// reasonDiscoveryFailed: the discovery request itself failed, so nothing is
	// known about the served kinds this pass. The existing children are left alone.
	ReasonDiscoveryFailed = "DiscoveryFailed"
	// reasonDiscoveryDraining: the kind list is current but a still-served kind has
	// no live child yet — an earlier prune's child is draining and holds its name.
	ReasonDiscoveryDraining = "DiscoveryDraining"
	// reasonStale: caught up, but the watch stopped proving itself alive past the
	// threshold — the cache may be behind (a Synced=False state distinct from
	// SyncFailed, which is a hard worker failure).
	ReasonStale = "Stale"

	// Sync-EVENT reasons (the event log's transition vocabulary, distinct from the
	// Synced-condition reasons above): start/complete pairs, cold and warm. A
	// healthy steady state records no event.
	//
	// reasonSyncStart: a cold cache began its first-ever build.
	ReasonSyncStart = "SyncStart"
	// reasonSyncComplete: a cold build reached the caught-up milestone.
	ReasonSyncComplete = "SyncComplete"
	// reasonResyncStart: an already-populated cache began resuming (poke,
	// reconnect, credential restart) — its message reports the warm cache size.
	ReasonResyncStart = "ResyncStart"
	// reasonResyncComplete: a resume re-reached the caught-up milestone; its
	// message disambiguates a real catch-up (counts) from a bare liveness
	// recovery (no counts).
	ReasonResyncComplete = "ResyncComplete"
	// reasonSyncDegraded: the worker failed and is retrying with backoff. The
	// event-log parallel of the SyncFailed condition reason.
	ReasonSyncDegraded = "SyncDegraded"
	// reasonSyncStopped: the cache's syncs were stopped because the cluster became
	// sync-ineligible (sync paused/disabled, or the context departed).
	ReasonSyncStopped = "SyncStopped"
	// reasonSyncStale: a caught-up watch stopped delivering updates past the
	// threshold — the event-log parallel of the Stale condition reason.
	ReasonSyncStale = "SyncStale"
)

// Condition aliases beehive's status condition. Conditions are beehive object
// rows (not inside our status blob); the store owns TransitionedAt stamping,
// no-op suppression, and the liveness downgrade.
type Condition = beehive.Condition

// LiveCondition is the sole condition constructor. Every condition here describes
// process-scoped state, so Liveness makes beehive downgrade a previous process's
// write to Unknown until re-confirmed (docs/adr/2026-08-09-liveness-conditions.md).
// The message is capped here — the one place every condition is built — because
// messages come from unbounded sources (raw client-go errors, kilobyte /readyz
// bodies) and are re-serialized to every watcher per frame.
func LiveCondition(t ConditionType, status ConditionStatus, reason, message string) Condition {
	return Condition{
		Type: string(t), Status: status, Reason: reason, Message: TruncateMessage(message), Liveness: true,
	}
}

// FindCondition returns a pointer to the condition of the given type, or nil.
func FindCondition(conds []Condition, t ConditionType) *Condition {
	for i := range conds {
		if conds[i].Type == string(t) {
			return &conds[i]
		}
	}
	return nil
}

// --- Cluster kind types ---

// ClusterGroupKind identifies the Cluster beehive resource kind.
var ClusterGroupKind = beehive.GroupKind{Kind: "Cluster"}

// ClusterStatusSourceKubeconfig is the kubeconfig-sourced record's last-known kubeconfig
// observation: the cluster/user entry names and presence. Cached from the
// last time the context was present so it survives orphaning.
type ClusterStatusSourceKubeconfig struct {
	Cluster   string `json:"cluster"`
	User      string `json:"user"`
	IsPresent bool   `json:"isPresent"`
	IsDefault bool   `json:"isDefault"`
}

// ClusterSpecSource is the discriminated union naming where a cluster record
// comes from and how its credentials resolve.
type ClusterSpecSource struct {
	Kubeconfig *ClusterSpecSourceKubeconfig `json:"kubeconfig,omitempty"`
}

// ClusterSpecSourceKubeconfig is the kubeconfig-sourced variant of ClusterSpecSource.
type ClusterSpecSourceKubeconfig struct {
	Context string `json:"context"`
}

// ClusterStatusSource is the status-side counterpart of ClusterSpecSource.
type ClusterStatusSource struct {
	Kubeconfig *ClusterStatusSourceKubeconfig `json:"kubeconfig,omitempty"`
}

// ClusterServer holds last-known facts about the remote cluster, discovered by
// connecting. Nil fields mean never probed.
type ClusterServer struct {
	UID     *string `json:"uid,omitempty"`
	Version *string `json:"version,omitempty"`
}

// ClusterPrincipal holds last-known facts about the connecting client's
// identity on the cluster. Nil fields mean never probed.
type ClusterPrincipal struct {
	Username *string `json:"username,omitempty"`
}

// ClusterSpec is a cluster record's desired state (user/API-owned; declarative
// field names). No spec-level trigger counters — retries and resync pokes ride
// out-of-band buses, never spec writes.
type ClusterSpec struct {
	Name        *string `json:"name,omitempty"`
	Enabled     bool    `json:"enabled"`
	SyncEnabled bool    `json:"syncEnabled"`
	// Source is the reference (where this record comes from, how credentials
	// resolve). The matching observation lives on ClusterStatus.Source, written
	// live by the core controller each reconcile.
	Source ClusterSpecSource `json:"source"`
}

// ClusterStatus is both the stored status and the domain status served to
// GraphQL: connection/health observations written by the core controller. Sync
// status lives on the ClusterCache child; no merge type.
type ClusterStatus struct {
	Source          ClusterStatusSource `json:"source"`
	Server          ClusterServer       `json:"server"`
	Principal       ClusterPrincipal    `json:"principal"`
	LastConnectedAt *time.Time          `json:"lastConnectedAt,omitempty"`
}

// Probe outcomes are not stored on ClusterStatus — they ride beehive's event log,
// keeping per-probe chatter off the status watch (a repeated failure bumps a run's
// Count, no status rewrite).
const (
	// ConnectionEventCategory is the connection controller's probe-outcome category.
	ConnectionEventCategory = "connection"
	// SyncEventCategory is the sync-transition category, the sync-side parallel of
	// ConnectionEventCategory.
	SyncEventCategory = "sync"
)

// Schedule is the kind-agnostic projection of a beehive object's reconcile
// schedule (a gauge), served live via the schedule watch — a scheduling change
// fires no object WatchList.
type Schedule struct {
	// NextRequeueAt is the next reconcile time, or nil when nothing is scheduled.
	NextRequeueAt *time.Time `json:"nextRequeueAt"`
	// Probing reports whether a reconcile's network probe is in flight, asserted
	// by the core controller so the webview can show a definite "checking now".
	// Merged into this gauge because a probe start/end fires no WatchList.
	Probing bool `json:"probing"`
}

// Event is one coalesced run from a beehive object's event timeline, served under
// every kind's events surface. ID is the client's upsert key (a reason can repeat;
// a run id cannot); a repeated same-outcome occurrence bumps Count, and
// [FirstAt, LastAt] is the run's window.
type Event struct {
	ID       ObjectID          `json:"id"`
	Category string            `json:"category"`
	Type     beehive.EventType `json:"type"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
	Count    int               `json:"count"`
	FirstAt  time.Time         `json:"firstAt"`
	LastAt   time.Time         `json:"lastAt"`
}

// --- ClusterCache kind types ---

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

// --- Domain types exposed to resolvers ---

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

// Cluster is the domain record for one tracked cluster connection (one kube-context).
// Built from a single Cluster beehive object; Status binds directly to the stored
// Cluster-kind status. Owned ClusterCache records are not joined in here — they stream
// standalone via Caches().Watch and are joined client-side, so cache churn never re-emits
// a cluster.
type Cluster struct {
	ID                  ClusterID
	Generation          int64
	CreatedAt           time.Time
	DeletionRequestedAt *time.Time // beehive's soft-delete tombstone, surfaced as-is

	Spec   ClusterSpec
	Status ClusterStatus
	// Conditions are beehive object conditions, not part of Status — read off the
	// object rather than out of the status blob.
	Conditions []Condition
	// The cluster's next-reconcile time is not a field here — it is a gauge served
	// live via Clusters().WatchSchedule (a scheduling change fires no object WatchList,
	// so it can't ride this record's watch), and its probe history via the events
	// surface.
}

// ChangeType classifies a delta-watch change, mirroring a Kubernetes watch event. The
// Added/Modified/Deleted string values are identical to beehive's, so the watch pumps map
// beehive→domain with a plain conversion; it is a defined type (not an alias of
// beehive.ChangeType, which aliases into an internal package gqlgen can't import) so the
// GraphQL ChangeType enum binds straight to it — the external-enum pattern used for
// EventType/ConditionStatus.
type ChangeType string

const (
	ChangeAdded    ChangeType = "Added"
	ChangeModified ChangeType = "Modified"
	ChangeDeleted  ChangeType = "Deleted"
	// ChangeBookmark closes the on-subscribe snapshot: exactly one per stream, after the
	// last snapshot object and before the first live change. It carries no object — the
	// one case for which every change wrapper's entity is a pointer — so a consumer must
	// skip it rather than key on it.
	// See docs/adr/2026-08-09-delta-watch-protocol.md.
	ChangeBookmark ChangeType = "Bookmark"
)

// ClusterChange is one delta on the cluster list watch: what happened (Type) to
// which cluster (Cluster). On a Deleted change Cluster carries the last-known state;
// consumers key on Cluster.ID. Binds 1:1 to the GraphQL ClusterChange.
type ClusterChange struct {
	Type    ChangeType
	Cluster *Cluster
}

// ClusterCacheChange is the ClusterCache-kind counterpart of ClusterChange, carried
// on the standalone cache watch. Binds 1:1 to the GraphQL ClusterCacheChange.
type ClusterCacheChange struct {
	Type  ChangeType
	Cache *ClusterCache
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

// ClusterCacheGVRDiscoveryChange is one delta on the GVR-discovery watch, the third of
// the parallel object streams (clusters, caches, gvr-discoveries). Binds
// 1:1 to the GraphQL ClusterCacheGVRDiscoveryChange; consumers key on Discovery.ID.
type ClusterCacheGVRDiscoveryChange struct {
	Type      ChangeType
	Discovery *ClusterCacheGVRDiscovery
}

// ClusterDataKindChange is one delta on a cache's kind-catalog watch: what happened
// (Type) to which kind (Kind), from which cache (CacheID). On subscribe every catalog
// row arrives as Added (the snapshot); thereafter a new kind is Added, a kind whose
// fields change (chiefly its live Count) is Modified, and a kind leaving the catalog is
// Deleted (carrying its last-known row). Consumers key on APIVersion + Resource.
// CacheID is the frame's provenance: a client watching the active cache can reject a
// late frame from a superseded cache (one still draining after a cache/context switch).
// Binds 1:1 to the GraphQL ClusterDataKindChange.
type ClusterDataKindChange struct {
	Type ChangeType
	// Kind is nil on a Bookmark.
	Kind    *ClusterDataKind
	CacheID ClusterCacheID
}

// --- Cache statistics types (for the ClusterCache GraphQL resolver) ---

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

// ClusterDataKind is one entry in a cluster's discovered kind catalog — a kind the API
// server advertises (built-in or CRD), read from the active cache's kind_catalog. Binds
// 1:1 to the GraphQL ClusterDataKind; it powers the dashboard's dynamic resource nav,
// so it carries the plural resource name (to dedupe against the curated catalog) and
// the api group (via APIVersion) to bucket the kind into a nav group.
type ClusterDataKind struct {
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

// ClusterDataEvent is one cached Kubernetes Event from a cluster's synced data,
// read from the active cache's events table. Binds 1:1 to the GraphQL
// ClusterDataEvent; it powers the dashboard's events table. The involved-object
// identity is flattened onto the record (any field may be empty — a name-only
// reference carries no namespace, etc.); the raw event body is not exposed.
type ClusterDataEvent struct {
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

// ClusterDataEventChange is one delta on a cache's events watch: what happened
// (Type) to which event (Event), from which cache (CacheID). On subscribe the newest
// window of events arrives as Added (the snapshot); thereafter a new event is Added, an
// event whose fields change (chiefly its Count/LastSeen as it re-fires) is Modified, and
// an event leaving the watched window — dropped from the cache, or aged past the window
// as newer events arrive — is Deleted (carrying its last-known row). Consumers key on
// UID. CacheID is the frame's provenance, mirroring ClusterDataKindChange: a client
// watching the active cache can reject a late frame from a superseded cache. Binds 1:1
// to the GraphQL ClusterDataEventChange.
type ClusterDataEventChange struct {
	Type ChangeType
	// Event is nil on a Bookmark.
	Event   *ClusterDataEvent
	CacheID ClusterCacheID
}

// ClusterDataObject is one cached Kubernetes object read from the active cache. It
// carries the typed universal identity (UID/APIVersion/Kind/Namespace/Name/
// CreationTimestamp) — enough to key the watch, sort, and render the Name/Namespace/Age
// columns without parsing the body — plus RawJSON, the object's full native body. The
// frontend derives kind-specific columns from the body client-side (a resolver-gated
// field, so identity-only consumers skip the body cost). Because RawJSON is part of the
// struct, an in-place edit (a resourceVersion/spec change) differs across two reads, so
// the watch diff surfaces it as Modified. The string underlying RawJSON keeps the struct
// comparable, which the delta-watch diff requires. Distinct from ClusterDataEvent (a
// specific kind with its own typed shape). Binds 1:1 to the GraphQL ClusterDataObject.
type ClusterDataObject struct {
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

// ClusterDataObjectChange is one delta on a cache's per-kind objects watch: what happened
// (Type) to which object (Object), from which cache (CacheID) and kind (APIVersion +
// Resource). On subscribe the current object set for the kind arrives as Added (the
// snapshot); thereafter a new object is Added, an object whose fields change is Modified,
// and one removed from the cache is Deleted (carrying its last-known row). Consumers key
// on UID. CacheID + (APIVersion, Resource) are the frame's provenance: unlike the kinds/
// events watches (keyed only by cache), this watch is keyed by kind too, so a client
// switching resources within one cache uses the kind to reject a straggler from the
// previous kind's still-draining subscription. Binds 1:1 to the GraphQL
// ClusterDataObjectChange.
type ClusterDataObjectChange struct {
	Type ChangeType
	// Object is nil on a Bookmark.
	Object     *ClusterDataObject
	CacheID    ClusterCacheID
	APIVersion string
	Resource   string
}

// TimePtrEqual compares two optional timestamps: both nil is equal, and a present
// pair compares by instant, so two readings of the same stamp never look changed.
func TimePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
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
