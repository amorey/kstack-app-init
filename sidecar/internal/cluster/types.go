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

// Package cluster is the kstack sidecar's Kubernetes logic layer: domain types for
// clusters and their caches, two beehive controllers (Cluster, ClusterCache), a
// kubeconfig importer, and the two cache sub-packages (cache/store, cache/engine)
// that back the per-cluster on-disk mirrors.
//
// The two beehive resource kinds and their ownership chain:
//
//	Cluster        (slug: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache   (slug: "{ClusterID}/{serverUID}")
//
// Cluster objects are created directly by the kubeconfig importer (one per
// kube-context); there is no separate intake kind. Each source owns a disjoint slug
// namespace within the one Cluster kind, so the importer reconciles by slug
// (beehive's per-kind slug-uniqueness rules out duplicates), and the on-disk cache is
// keyed separately by beehive ObjectIDs so the slug's arbitrary text never reaches
// the filesystem.
//
// Cluster carries connection status (Connected, Healthy conditions + server/principal
// facts); its ClusterCache child carries sync status (Synced condition +
// lastSyncedAt).
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

// Slug prefixes. The slug is a per-kind reconcile/uniqueness key, NOT the identity
// surfaced to consumers (that is the ClusterID, the beehive ObjectID).
//
//   - A kubeconfig-sourced Cluster's slug is "kubeconfig/{context}" — the source's
//     natural key, used by the importer so beehive's per-kind slug-uniqueness rules out
//     a duplicate for a context. Future sources add their own prefix ("cloud/",
//     "manual/"). Nothing reads a Cluster back by this slug.
//   - A ClusterCache's slug is "{ClusterID}/{serverUID}": its parent's ObjectID plus
//     the kube-system UID it mirrors. beehive's UNIQUE(slug) then means "one cache per
//     identity per cluster", so a migration to a new UID yields a second, coexisting
//     cache rather than colliding on a one-per-cluster slug. Children are enumerated
//     via the owner edge (Client.ListOwned), so the UID need not be known to list a
//     cluster's caches. No "caches/" prefix is needed: ClusterCache is its own kind, so
//     its slugs already sit in a namespace disjoint from Cluster's.
const slugPrefixKubeconfig = "kubeconfig/"

// kubeconfigSlug returns the beehive slug a kubeconfig-sourced Cluster is created
// with: the importer's natural key for one kube-context. It is not an identity —
// see ClusterID.
func kubeconfigSlug(contextName string) string {
	return slugPrefixKubeconfig + contextName
}

// ClusterCacheSlug returns the slug a ClusterCache is created with:
// "{ClusterID}/{serverUID}". The parent-ObjectID segment scopes the UID to one cluster
// (two clusters probing the same identity keep distinct caches); the serverUID segment
// is the migration-turnover key backing beehive's UNIQUE(slug) dedup in
// ensureClusterCache. A creation/dedup key only — caches are enumerated through the
// owner edge, so callers lacking a serverUID can still list them.
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

// ObjectID is the identity of a persisted object — the beehive ObjectID of any kind
// (a Cluster, a ClusterCache, …). It is opaque on the wire (a decimal string) and
// binds to the one GraphQL `ObjectID` scalar; its MarshalGQL/UnmarshalGQL are the
// single id (un)marshalling path every kind reuses. Per-kind aliases (ClusterID,
// ClusterCacheID) name the same type purely to document which kind an id refers to.
type ObjectID int64

// ClusterID uniquely identifies a cluster record: the beehive ObjectID of its Cluster
// object. It is opaque and stable for the record's life (a departed kube-context is
// orphaned, not deleted, so its id survives a return; it changes only on an explicit
// Delete) and source-agnostic. The source's natural key (e.g. a kube-context name)
// lives only on the beehive slug, never surfaced here. An alias of [ObjectID] — a
// documentation name — so it shares the one GraphQL scalar and (un)marshalling.
type ClusterID = ObjectID

// ClusterCacheID identifies one ClusterCache record: the beehive ObjectID of its
// ClusterCache object. Like [ClusterID], an alias of [ObjectID] naming the cache's own
// id (distinct from its parent ClusterID). A cluster can own several ClusterCache
// records, so the cache id is not derivable from the cluster id.
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
	// ConditionSynced reports the state of the cluster's cache-sync
	// engine. It lives in ClusterCacheStatus (the ClusterCache kind), not in
	// the Cluster kind's ClusterStatus.
	ConditionSynced ConditionType = "Synced"
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
	// reasonSyncing: the engine is starting or catching up (discovery walk, drivers
	// pre-first-watch). Condition-only — the event vocabulary names the start
	// transitions SyncStart/ResyncStart instead, so a same-named event reason can't
	// read ambiguously against this condition state.
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

	// The following are sync-EVENT reasons (the ClusterCache event log's transition
	// vocabulary), distinct from the Synced-condition reasons above (which name the
	// current state). They come in start/complete pairs, cold and warm:
	// SyncStart→SyncComplete for a first-ever build, ResyncStart→ResyncComplete for a
	// resume. A healthy steady state records no event at all.
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

// Condition is one Kubernetes-style status condition on a cluster
// record. Stored as a JSON array inside the beehive status blob so that
// ObservedGeneration and LastTransitionTime survive the wire without a schema
// change. (We do not use beehive.SetCondition because the public
// beehive.Condition type elides those fields.)
type Condition struct {
	Type   ConditionType   `json:"type"`
	Status ConditionStatus `json:"status"`
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
func SetCondition(conds *[]Condition, c Condition) bool {
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
func FindCondition(conds []Condition, t ConditionType) *Condition {
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
func ConditionEqual(a, b Condition) bool {
	return a.Type == b.Type && a.Status == b.Status &&
		a.Reason == b.Reason && a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration &&
		a.LastTransitionTime.Equal(b.LastTransitionTime)
}

// ConditionsEqual reports whether two condition slices are observably equal,
// element-wise via ConditionEqual.
func ConditionsEqual(a, b []Condition) bool {
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
	Conditions []Condition `json:"conditions"`
}

// Connection-probe outcomes are not stored on ClusterStatus: the ClusterCoreController
// records them into beehive's event log (category ConnectionEventCategory), exposed
// through the cluster events surface. This keeps per-probe chatter off the status
// watch — a repeated identical failure doesn't rewrite status — while beehive
// aggregates same-outcome probes into runs and bounds retention.
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

// Schedule is the generic domain projection of a beehive object's reconcile schedule
// (a gauge): when the control plane has queued the object's next reconcile. It is
// kind-agnostic like Event — the entrypoints are kind-scoped (ClusterScheduleWatch),
// the value shape isn't. Served live via the schedule watch — a scheduling change
// fires no object WatchList, so this is the only way to observe the countdown move for
// an otherwise-idle object.
type Schedule struct {
	// NextRequeueAt is the scheduled time of the object's next reconcile (for a
	// disconnected cluster, its next backoff retry), or nil when nothing is scheduled.
	NextRequeueAt *time.Time `json:"nextRequeueAt"`
	// Probing reports whether a reconcile is actively running its network probe right
	// now (between "eligible, about to connect" and the probe/health round-trips
	// returning). Asserted by the ClusterCoreController itself, so the webview can show
	// a definite "checking now" rather than inferring it from an absent next-attempt
	// time. Merged into this gauge (not the list watch) because a probe start/end fires
	// no object WatchList.
	Probing bool `json:"probing"`
}

// Event is the generic domain projection of one coalesced run from a beehive object's
// event timeline, served under every kind's events surface. It drops beehive.Event's
// ObjectID (the addressed object is the surface's subject) and Detail. ID is the run's
// identity on the shared ObjectID scalar — a client's upsert key on a watch, since a
// reason can repeat across a change-and-back while the run id does not. A repeated
// same-outcome occurrence bumps Count rather than adding a run; [FirstAt, LastAt] is
// the run's window.
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

// ClusterCacheSpec is the ClusterCache kind's spec. It carries no user-facing fields;
// the parent ClusterID is the owner edge, and ServerUID is the kube-system UID this
// cache mirrors — the identity a migration turns over (named to match the
// ClusterStatus.Server.UID it's compared against). The ClusterCoreController writes it
// at creation (once a probe confirms it); the ClusterCacheController reads it to decide
// whether this cache is the parent's active one (ServerUID == parent's last-probed
// Server.UID) and so should run an engine. It also rides the slug so beehive's
// UNIQUE(slug) dedups a per-identity create, but the controller reads spec.ServerUID,
// not the slug.
type ClusterCacheSpec struct {
	ServerUID string `json:"serverUid"`
}

// ClusterCacheStatus is the ClusterCache kind's stored status, written by the
// ClusterCacheController, and the domain sync-status block served under the
// Cluster's cache child. Both stored and served — there is no separate
// projection type.
type ClusterCacheStatus struct {
	// Conditions holds the sync-controller-owned condition (Synced).
	Conditions []Condition `json:"conditions"`
	// LastSyncedAt is when the cache last received fresh data; nil if never.
	LastSyncedAt *time.Time `json:"lastSyncedAt,omitempty"`
}

// --- Domain types exposed to resolvers ---

// ClusterCache is the domain view of one ClusterCache beehive object, streamed
// standalone via WatchCaches and joined to its parent client-side. ID is the cache's
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
	Status    ClusterCacheStatus
}

// Cluster is the domain record for one tracked cluster connection (one kube-context).
// Built from a single Cluster beehive object; Status binds directly to the stored
// Cluster-kind status. Owned ClusterCache records are not joined in here — they stream
// standalone via WatchCaches and are joined client-side, so cache churn never re-emits
// a cluster.
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

// ChangeType classifies a delta-watch change, mirroring a Kubernetes watch event. Its
// string values are identical to beehive.Added/Modified/Deleted, so the watch pumps map
// beehive→domain with a plain conversion; it is a defined type (not an alias of
// beehive.ChangeType, which aliases into an internal package gqlgen can't import) so the
// GraphQL ChangeType enum binds straight to it — the external-enum pattern used for
// EventType/ConditionStatus. Values: Added (incl. the on-subscribe snapshot), Modified,
// Deleted.
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
// (Type) to which kind (Kind), from which cache (CacheID). On subscribe every catalog
// row arrives as Added (the snapshot); thereafter a new kind is Added, a kind whose
// fields change (chiefly its live Count) is Modified, and a kind leaving the catalog is
// Deleted (carrying its last-known row). Consumers key on APIVersion + Resource.
// CacheID is the frame's provenance: a client watching the active cache can reject a
// late frame from a superseded cache (one still draining after a cache/context switch).
// Binds 1:1 to the GraphQL ClusterDataKindChange.
type ClusterDataKindChange struct {
	Type    ChangeType
	Kind    ClusterDataKind
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
	Type    ChangeType
	Event   ClusterDataEvent
	CacheID ClusterCacheID
}

// --- Seed conditions ---

// SeedConnectionConditions returns the initial condition set for a freshly
// minted Cluster record, before any probe has run.
func SeedConnectionConditions(gen int64, now time.Time) []Condition {
	return []Condition{
		{
			Type: ConditionConnected, Status: ConditionUnknown,
			Reason: ReasonConnecting, ObservedGeneration: gen, LastTransitionTime: now,
		},
		{
			Type: ConditionHealthy, Status: ConditionUnknown,
			Reason: ReasonNoConnection, ObservedGeneration: gen, LastTransitionTime: now,
		},
	}
}

// SeedSyncConditions returns the initial condition set for a freshly minted
// ClusterCache record, before any sync engine has started.
func SeedSyncConditions(gen int64, now time.Time) []Condition {
	return []Condition{
		{
			Type: ConditionSynced, Status: ConditionUnknown,
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
