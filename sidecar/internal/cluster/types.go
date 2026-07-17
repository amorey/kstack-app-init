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

// Package controllers is the kstack sidecar's Kubernetes logic layer: domain
// types for clusters and their caches, two beehive controller implementations
// (Cluster, ClusterCache), a kubeconfig importer, and the two cache sub-packages
// (cache/store, cache/engine) that back the per-cluster on-disk mirrors.
//
// The two beehive resource kinds and their ownership chain:
//
//	Cluster        (slug: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache   (slug: "caches/{ClusterID}")
//
// Cluster objects are created directly by the kubeconfig importer (one per
// kube-context); there is no separate intake kind. A Cluster's slug IS its
// ClusterID — a source prefix plus that source's natural key — so each source
// owns a disjoint slug namespace within the one Cluster kind, the importer
// reconciles by slug (beehive's per-kind slug-uniqueness rules out duplicates),
// and the on-disk cache is keyed separately by beehive ObjectIDs so the slug's
// arbitrary text never reaches the filesystem.
//
// Domain types here are a superset of what beehive stores: the domain Cluster
// (returned to resolvers) joins the Cluster and ClusterCache beehive objects
// into one combined status view — Cluster carries connection status (Connected,
// Healthy conditions + server/principal facts), ClusterCache carries sync
// status (Synced condition + lastSyncedAt).
package cluster

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

// ErrNotFound is returned by client helpers when no cluster with the given id
// is tracked.
var ErrNotFound = errors.New("controllers: cluster not found")

// Slug prefixes. The slug is a per-kind reconcile/uniqueness key, NOT the
// identity surfaced to consumers (that is the ClusterID, the beehive ObjectID).
//
//   - A kubeconfig-sourced Cluster is created with the slug "kubeconfig/{context}"
//     — the source's natural key, used purely by the importer so beehive's per-kind
//     slug-uniqueness rules out a duplicate for a context (race-safe). Future
//     sources add their own prefix ("cloud/", "manual/"). Nothing reads a Cluster
//     back by this slug; lookups go through the ObjectID.
//   - A ClusterCache is created with the slug "{ClusterID}/{serverUID}": its
//     parent's ObjectID plus the kube-system namespace UID of the physical cluster it
//     mirrors. beehive's per-kind UNIQUE(slug) then means "one cache per physical
//     identity per cluster" — a physical-cluster migration (the kube-context repointed
//     at a freshly-built cluster) yields a new UID and so a second, coexisting
//     ClusterCache under the same parent, rather than colliding on a one-per-cluster slug.
//     Children are enumerated via the owner edge (Client.ListOwned), not by re-deriving
//     this slug, so the UID need not be known to list a cluster's caches. There is no
//     "caches/" prefix: ClusterCache is its own beehive kind, so its slugs already sit
//     in a namespace disjoint from Cluster's — unlike the Cluster kind, whose source
//     prefix partitions multiple importers within the one kind, ClusterCache has no
//     second axis a prefix would disambiguate.
const slugPrefixKubeconfig = "kubeconfig/"

// kubeconfigSlug returns the beehive slug a kubeconfig-sourced Cluster is created
// with: the importer's natural key for one kube-context. It is not an identity —
// see ClusterID.
func kubeconfigSlug(contextName string) string {
	return slugPrefixKubeconfig + contextName
}

// ClusterCacheSlug returns the slug a ClusterCache is created with:
// "{ClusterID}/{serverUID}", where serverUID is the kube-system namespace UID of
// the physical cluster the cache mirrors. The parent-ObjectID segment scopes the UID
// to one cluster (two clusters that ever probe the same physical identity keep
// distinct caches), and the serverUID segment is the migration-turnover key that
// backs beehive's UNIQUE(slug) dedup in ensureClusterCache. The slug is a
// creation/dedup key only — a cluster's caches are enumerated through the owner edge,
// never by re-deriving this slug, so callers that lack a serverUID can still list them.
func ClusterCacheSlug(clusterID ClusterID, serverUID string) string {
	return strconv.FormatInt(int64(clusterID), 10) + "/" + serverUID
}

// newCacheRef builds the on-disk cache locator from the parent Cluster and
// ClusterCache beehive ObjectIDs. It is the single place the beehive
// ObjectID→int64 conversion happens, so the leaf store package stays
// beehive-free.
func newCacheRef(clusterObjID, cacheObjID beehive.ObjectID) store.CacheRef {
	return store.CacheRef{ClusterID: int64(clusterObjID), CacheID: int64(cacheObjID)}
}

// --- Identity ---

// ObjectID is the identity of a persisted object — the beehive ObjectID of any
// kind (a Cluster, a ClusterCache, …). It is opaque on the wire (a decimal
// string) and binds to the one GraphQL `ObjectID` scalar; its
// MarshalGQL/UnmarshalGQL are the single id (un)marshalling path every kind
// reuses, so a new kind's id needs no new scalar or parsing. Per-kind aliases
// (ClusterID below, ClusterCacheID later) name the same type purely to document
// which kind an id refers to in Go signatures.
type ObjectID int64

// ClusterID uniquely identifies a cluster record: the beehive ObjectID of its
// Cluster object. It is opaque and stable for the life of the record (a departed
// kube-context is orphaned, not deleted, so its id survives a return; the id
// changes only on an explicit Delete), and it is source-agnostic — the same
// identity regardless of which importer created the record. The source's natural
// key (e.g. a kube-context name) lives only on the beehive *slug*, an
// importer-internal reconcile/uniqueness key, never surfaced here. It is an
// alias of [ObjectID] — a documentation name, not a distinct type, so it shares
// the one GraphQL scalar and (un)marshalling machinery.
type ClusterID = ObjectID

// ClusterCacheID identifies one ClusterCache record: the beehive ObjectID of its
// ClusterCache object. Like [ClusterID] it is an alias of [ObjectID] — a
// documentation name for the cache's own id (distinct from its parent ClusterID),
// sharing the one GraphQL scalar and (un)marshalling machinery. A cluster can own
// several ClusterCache records (one per physical identity it has mirrored), so the
// cache id is not derivable from the cluster id.
type ClusterCacheID = ObjectID

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

// UnmarshalGQL parses the GraphQL ObjectID scalar into a typed ObjectID, so
// gqlgen hands resolvers the typed id with no per-resolver parsing. The scalar
// accepts a string or an integer literal: gqlgen delivers a quoted literal /
// JSON string as string, an inline integer literal as int64, and a JSON-variable
// number as json.Number.
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

// ClusterConditionType names one independently-tracked aspect of a cluster
// record's observed state. Each type is owned by exactly one controller.
type ClusterConditionType string

const (
	// ClusterConditionConnected reports whether the last connection probe
	// reached the cluster's API server and resolved its identity facts.
	ClusterConditionConnected ClusterConditionType = "Connected"
	// ClusterConditionHealthy reports the API server's own condition (its
	// readiness checks), as distinct from our ability to reach it.
	ClusterConditionHealthy ClusterConditionType = "Healthy"
	// ClusterConditionSynced reports the state of the cluster's cache-sync
	// engine. It lives in ClusterCacheStatus (the ClusterCache kind), not in
	// the Cluster kind's ClusterStatus.
	ClusterConditionSynced ClusterConditionType = "Synced"
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
	// reasonPaused: no sync engine runs — the record is sync-disabled,
	// deactivated, orphaned, or archived.
	ReasonPaused = "Paused"
	// reasonSyncing: the engine is starting or catching up (discovery walk,
	// drivers pre-first-watch). Condition-only — the event vocabulary names the
	// start transitions SyncStart/ResyncStart instead, so "Syncing" is never an
	// event reason — a same-named event reason would read ambiguously against this
	// condition state.
	ReasonSyncing = "Syncing"
	// reasonWatching: every driver reached its watch phase — the cache is
	// caught up and streaming deltas.
	ReasonWatching = "Watching"
	// reasonSyncFailed: the engine hit an engine-level failure (discovery,
	// cache open) and is retrying with backoff.
	ReasonSyncFailed = "SyncFailed"
	// reasonStale: the engine is caught up but a driver's watch stopped proving
	// itself alive past the threshold — the cache may be behind (a Synced=False
	// state distinct from SyncFailed, which is a hard engine failure).
	ReasonStale = "Stale"

	// The following are sync-EVENT reasons (the ClusterCache event log's
	// transition vocabulary), distinct from the Synced-condition reasons above
	// (which name the current state). Events are transition-oriented and carry
	// richer messages; a healthy steady state records no event at all. They come
	// in start/complete pairs, cold and warm, so the log reads symmetrically:
	// SyncStart→SyncComplete for a first-ever build, ResyncStart→ResyncComplete
	// for a resume.
	//
	// reasonSyncStart: a cold cache began its first-ever build.
	ReasonSyncStart = "SyncStart"
	// reasonSyncComplete: a cold build reached the caught-up milestone — the
	// local copy is now complete.
	ReasonSyncComplete = "SyncComplete"
	// reasonResyncStart: an already-populated cache began resuming (poke,
	// reconnect, credential restart) — its message reports the warm cache size.
	ReasonResyncStart = "ResyncStart"
	// reasonResyncComplete: a resume re-reached the caught-up milestone. Its
	// message disambiguates the two shapes a resume can take — a real catch-up
	// (carrying counts) vs a bare liveness recovery (a stale watch resuming, no
	// counts) — so no separate reason is needed for the recovery case.
	ReasonResyncComplete = "ResyncComplete"
	// reasonSyncDegraded: an engine-level failure; the engine is retrying with
	// backoff. The event-log parallel of the SyncFailed condition reason.
	ReasonSyncDegraded = "SyncDegraded"
	// reasonSyncStopped: the sync engine was torn down because the cluster became
	// sync-ineligible (sync paused/disabled, or the context departed).
	ReasonSyncStopped = "SyncStopped"
	// reasonSyncStale: a caught-up watch stopped delivering updates past the
	// threshold — the event-log parallel of the Stale condition reason.
	ReasonSyncStale = "SyncStale"
)

// ClusterCondition is one Kubernetes-style status condition on a cluster
// record. Stored as a JSON array inside the beehive status blob so that
// ObservedGeneration and LastTransitionTime survive the wire without a schema
// change. (We do not use beehive.SetCondition because the public
// beehive.Condition type elides those fields.)
type ClusterCondition struct {
	Type   ClusterConditionType `json:"type"`
	Status ConditionStatus      `json:"status"`
	// Reason is a CamelCase, machine-readable explanation of Status.
	Reason string `json:"reason"`
	// Message is the human-readable detail; empty when there is nothing to
	// explain.
	Message string `json:"message,omitempty"`
	// ObservedGeneration is the spec generation the pass that wrote this
	// condition observed; a gap to the record's generation marks the
	// condition stale.
	ObservedGeneration int64 `json:"observedGeneration"`
	// LastTransitionTime is when Status last changed — not when the condition
	// was last refreshed.
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

// SetCondition folds one condition into a condition slice, mirroring
// apimachinery's meta.SetStatusCondition semantics: a new type appends; an
// existing type updates in place, keeping LastTransitionTime unless Status
// changed (c.LastTransitionTime is used for a transition when set, else now).
// Reports whether anything changed.
func SetCondition(conds *[]ClusterCondition, c ClusterCondition) bool {
	if c.LastTransitionTime.IsZero() {
		c.LastTransitionTime = time.Now().UTC()
	}
	existing := FindCondition(*conds, c.Type)
	if existing == nil {
		*conds = append(*conds, c)
		return true
	}
	if existing.Status == c.Status {
		c.LastTransitionTime = existing.LastTransitionTime
	}
	if *existing == c {
		return false
	}
	*existing = c
	return true
}

// FindCondition returns a pointer to the condition of the given type, or nil.
func FindCondition(conds []ClusterCondition, t ClusterConditionType) *ClusterCondition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

// ConditionEqual reports whether two conditions are observably equal — hand-
// written because time.Time must compare by instant (reflect.DeepEqual trips
// over monotonic readings and locations after a persistence round-trip).
func ConditionEqual(a, b ClusterCondition) bool {
	return a.Type == b.Type && a.Status == b.Status &&
		a.Reason == b.Reason && a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration &&
		a.LastTransitionTime.Equal(b.LastTransitionTime)
}

// ConditionsEqual reports whether two condition slices are observably equal,
// element-wise via ConditionEqual.
func ConditionsEqual(a, b []ClusterCondition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !ConditionEqual(a[i], b[i]) {
			return false
		}
	}
	return true
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

// ClusterSpec is a cluster record's desired state: the user/API-owned fields.
// Field names are declarative (no "is" prefix) — spec states the desired
// condition, it does not ask a question. There is no spec-level trigger
// counter — RetryConnection forces an immediate re-probe out-of-band via the
// controller's in-process retry bus, and resync pokes likewise drive the
// controllers directly, so neither writes the spec.
type ClusterSpec struct {
	Name        *string `json:"name,omitempty"`
	Enabled     bool    `json:"enabled"`
	SyncEnabled bool    `json:"syncEnabled"`
	// Source is the reference: where this record comes from and how its
	// credentials resolve (GraphQL `spec.source`). The matching *observation*
	// (cluster/user entry names, presence, default flag) is not stored here — the
	// ClusterCoreController observes it live from the kubeconfig each reconcile and
	// writes it to ClusterStatus.Source (see ClusterStatusSource).
	Source ClusterSpecSource `json:"source"`
}

// ClusterStatus is the Cluster beehive kind's stored status AND the domain
// status surfaced to the GraphQL layer: connection/health observations written
// by the ClusterCoreController. Sync status lives separately on the ClusterCache
// child (see ClusterCacheStatus), mirroring the beehive owner chain, so there is
// no merge type — this one struct is both stored and served.
type ClusterStatus struct {
	Source          ClusterStatusSource `json:"source"`
	Server          ClusterServer       `json:"server"`
	Principal       ClusterPrincipal    `json:"principal"`
	LastConnectedAt *time.Time          `json:"lastConnectedAt,omitempty"`
	// Conditions holds the controller-written conditions (Connected, Healthy).
	Conditions []ClusterCondition `json:"conditions"`
}

// Connection-probe outcomes are not stored on ClusterStatus: the
// ClusterCoreController records them into beehive's event log (category
// ConnectionEventCategory) via RecordEvent, exposed generically through the
// cluster events surface (clusterEvents / clusterEventsWatch, category
// "connection"). This keeps per-probe chatter off the status watch — a repeated
// identical failure does not rewrite status — while beehive aggregates
// consecutive same-outcome probes into runs and bounds retention
// (WithEventRetention).
const (
	// ConnectionEventCategory is the beehive event category the connection
	// controller records probe outcomes under (its own object timeline).
	ConnectionEventCategory = "connection"
	// SyncEventCategory is the beehive event category the cache controller records
	// engine-state transitions under, on the ClusterCache object's own timeline —
	// the sync-side parallel of ConnectionEventCategory, exposed generically through
	// the events surface (clusterCacheEvents / clusterCacheEventsWatch, category
	// "sync").
	SyncEventCategory = "sync"
	// maxConnectionAttempts bounds the per-cluster connection-event retention ring.
	maxConnectionAttempts = 20
)

// defaultEventLimit bounds an events read (or watch snapshot) when the caller
// gives no explicit limit.
const defaultEventLimit = 50

// Schedule is the generic domain projection of a beehive object's reconcile
// schedule (a gauge, mapped from beehive.Schedule): when the control plane has
// queued the object's next reconcile. It is kind-agnostic like Event — the
// getter/watcher entrypoints are kind-scoped (ClusterScheduleWatch), but the
// value shape is not. A struct rather than a bare time so it can grow (mirroring
// beehive.Schedule's own extensibility). Served live via the schedule watch — a
// scheduling change fires no object WatchList, so this is the only way to observe
// the countdown move for an otherwise-idle object.
type Schedule struct {
	// NextRequeueAt is the scheduled time of the object's next reconcile (for a
	// disconnected cluster, its next backoff retry), or nil when nothing is scheduled.
	NextRequeueAt *time.Time `json:"nextRequeueAt"`
	// Probing reports whether a reconcile is *actively running its network probe*
	// right now (the controller is between "eligible, about to connect" and the
	// probe/health round-trips returning). Unlike NextRequeueAt — which the
	// scheduler drives — this is asserted by the ClusterCoreController itself, so
	// the webview can show a definite "checking now" state during the in-flight
	// window rather than inferring it from the (ambiguous) absence of a next-attempt
	// time. It is merged into this gauge (not the list watch) because a probe
	// starting/ending fires no object WatchList.
	Probing bool `json:"probing"`
}

// Event is the generic domain projection of one coalesced run from a beehive
// object's event timeline, served under every kind's events surface. It drops
// beehive.Event's ObjectID (the addressed object is the surface's subject) and
// Detail (no JSON scalar yet). ID is the run's identity, carried on the shared
// ObjectID scalar (an opaque int64) — a client's upsert key on a watch, since a
// reason can repeat across a change-and-back while the run id does not. Type
// binds straight to beehive.EventType (Normal/Warning). A repeated same-outcome
// occurrence bumps Count rather than adding a run; [FirstAt, LastAt] is the run's
// window.
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

// ClusterCacheSpec is the ClusterCache kind's spec. It carries no user-facing
// fields; the parent ClusterID is the object's owner edge, and ServerUID is the
// kube-system namespace UID of the physical cluster this cache mirrors — the
// identity a physical migration turns over (named to match the
// ClusterStatus.Server.UID it is compared against). The ClusterCoreController writes
// it at creation (once a probe has confirmed it); the ClusterCacheController reads
// it to decide whether this cache is the parent's currently-active one (ServerUID ==
// parent's last-probed Server.UID) and so should run a sync engine. It also rides
// the slug ("{ClusterID}/{ServerUID}") purely so beehive's UNIQUE(slug) dedups a
// per-identity create; spec.ServerUID, not the slug, is what the controller reads.
type ClusterCacheSpec struct {
	ServerUID string `json:"serverUid"`
}

// ClusterCacheStatus is the ClusterCache kind's stored status, written by the
// ClusterCacheController, and the domain sync-status block served under the
// Cluster's cache child. Both stored and served — there is no separate
// projection type.
type ClusterCacheStatus struct {
	// Conditions holds the sync-controller-owned condition (Synced).
	Conditions []ClusterCondition `json:"conditions"`
	// LastSyncedAt is when the cache last received fresh data; nil if never.
	LastSyncedAt *time.Time `json:"lastSyncedAt,omitempty"`
}

// --- Domain types exposed to resolvers ---

// ClusterCache is the domain view of one ClusterCache beehive object, streamed
// standalone via WatchCaches and joined to its parent client-side. ID is the cache's
// own ObjectID and ClusterID its parent's (the beehive owner) — together they locate
// the on-disk cache (the live-stats resolver builds a store.CacheRef from the pair)
// and let the client join the cache onto its cluster. ServerUID is the kube-system
// UID this cache mirrors; the client treats the cache whose ServerUID matches the
// cluster's last-probed Server.UID as the active one. Active-ness is deliberately
// *not* a field: it depends on the parent's status, which changes with no cache
// event, so it can't be kept fresh on a per-cache stream — only the client's live
// join is correct.
type ClusterCache struct {
	ID        ClusterCacheID
	ClusterID ClusterID
	ServerUID string
	Status    ClusterCacheStatus
}

// Cluster is the domain record for one tracked cluster connection (one
// kube-context): the restart-surviving facts about it. Built from a single Cluster
// beehive object. Status binds directly to the stored Cluster-kind status. A
// cluster's owned ClusterCache records are *not* joined in here — they stream
// standalone via WatchCaches and are joined client-side, so cache churn never
// re-emits a cluster.
type Cluster struct {
	ID                  ClusterID
	Generation          int64
	CreatedAt           time.Time
	DeletionRequestedAt *time.Time // beehive's soft-delete tombstone, surfaced as-is

	Spec   ClusterSpec
	Status ClusterStatus
	// The cluster's next-reconcile time is not a field here — it is a gauge served
	// live via ClusterScheduleWatch (a scheduling change fires no object WatchList,
	// so it can't ride this record's watch), and its probe history via the events
	// surface.
}

// ChangeType classifies a delta-watch change, mirroring a Kubernetes watch event
// (and beehive's own change type). Its string values are deliberately identical to
// beehive.Added/Modified/Deleted, so the watch pumps map beehive→domain with a plain
// conversion; it is a defined type (not an alias of beehive.ChangeType, which is
// itself an alias into an internal package gqlgen cannot import) so the GraphQL
// ChangeType enum can bind straight to it, the external-enum pattern used for
// EventType/ConditionStatus. Values: Added (incl. the on-subscribe snapshot),
// Modified, Deleted.
type ChangeType string

const (
	ChangeAdded    ChangeType = "Added"
	ChangeModified ChangeType = "Modified"
	ChangeDeleted  ChangeType = "Deleted"
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

// ClusterDataKindChange is one delta on a cache's kind-catalog watch: what happened
// (Type) to which kind (Kind), and which cache (CacheID) it came from. On subscribe
// every catalog row arrives as an Added change (the snapshot); thereafter a new kind
// is Added, a kind whose fields change (chiefly its live object Count) is Modified,
// and a kind that leaves the catalog is Deleted (carrying its last-known row).
// Consumers key on the kind's identity (APIVersion + Resource). CacheID carries the
// frame's provenance: a client watching the active cache can reject a late frame from
// a superseded cache (one still draining after a cache/context switch) by cache id,
// rather than inferring provenance from render state. Binds 1:1 to the GraphQL
// ClusterDataKindChange.
type ClusterDataKindChange struct {
	Type    ChangeType
	Kind    ClusterDataKind
	CacheID ClusterCacheID
}

// --- Cache statistics types (for the ClusterCache GraphQL resolver) ---

// CachedResource is the per-resource breakdown of one cluster's cache.
type CachedResource struct {
	Resource      string
	Count         int
	LastUpdatedAt *time.Time
}

// ClusterCacheStats reports a cluster's live on-disk cache statistics.
type ClusterCacheStats struct {
	Exists bool
	Bytes  int64
	// ObjectCount/KindCount are the whole-cache rollup of Resources (total
	// objects and distinct kinds), precomputed so a summary consumer need not
	// stream the full per-resource breakdown. Both include the synthetic events
	// row, matching Resources.
	ObjectCount int
	KindCount   int
	Resources   []CachedResource
}

// ClusterDataKind is one entry in a cluster's discovered kind catalog — a kind the
// cluster's API server advertises (built-in or CRD), read from the active cache's
// kind_catalog at request time. Binds 1:1 to the GraphQL ClusterDataKind; it powers
// the dashboard's dynamic resource nav, so consumers get the plural resource name
// (to dedupe against the curated catalog) and the api group (via APIVersion) to
// bucket the kind into a nav group.
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

// --- Seed conditions ---

// SeedConnectionConditions returns the initial condition set for a freshly
// minted Cluster record, before any probe has run.
func SeedConnectionConditions(gen int64, now time.Time) []ClusterCondition {
	return []ClusterCondition{
		{
			Type: ClusterConditionConnected, Status: ConditionUnknown,
			Reason: ReasonConnecting, ObservedGeneration: gen, LastTransitionTime: now,
		},
		{
			Type: ClusterConditionHealthy, Status: ConditionUnknown,
			Reason: ReasonNoConnection, ObservedGeneration: gen, LastTransitionTime: now,
		},
	}
}

// SeedSyncConditions returns the initial condition set for a freshly minted
// ClusterCache record, before any sync engine has started.
func SeedSyncConditions(gen int64, now time.Time) []ClusterCondition {
	return []ClusterCondition{
		{
			Type: ClusterConditionSynced, Status: ConditionUnknown,
			Reason: ReasonSyncing, ObservedGeneration: gen, LastTransitionTime: now,
		},
	}
}

// --- Status equality helpers (skip-the-write guards) ---

// ClusterStatusEqual reports whether two ClusterStatus blocks are observably
// equal — the ClusterCoreController's skip-the-write guard.
func ClusterStatusEqual(a, b ClusterStatus) bool {
	return ptrEqual(a.Source.Kubeconfig, b.Source.Kubeconfig) &&
		ptrEqual(a.Server.UID, b.Server.UID) &&
		ptrEqual(a.Server.Version, b.Server.Version) &&
		ptrEqual(a.Principal.Username, b.Principal.Username) &&
		timePtrEqual(a.LastConnectedAt, b.LastConnectedAt) &&
		ConditionsEqual(a.Conditions, b.Conditions)
}

// ClusterCacheStatusEqual reports whether two ClusterCacheStatus blocks are
// observably equal — the ClusterCacheController's skip-the-write guard.
func ClusterCacheStatusEqual(a, b ClusterCacheStatus) bool {
	return timePtrEqual(a.LastSyncedAt, b.LastSyncedAt) &&
		ConditionsEqual(a.Conditions, b.Conditions)
}

func ptrEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}
