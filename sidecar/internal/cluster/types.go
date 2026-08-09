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
// kubeconfig importer, and the cache sub-package (cache/store) that backs the
// per-cluster on-disk mirrors.
//
// The two beehive resource kinds and their ownership chain:
//
//	Cluster        (name: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache   (name: "{ClusterID}/{serverUID}")
//
// Cluster objects are created directly by the kubeconfig importer (one per
// kube-context); there is no separate intake kind. Each source owns a disjoint name
// namespace within the one Cluster kind, so the importer reconciles by name
// (beehive's per-kind name-uniqueness rules out duplicates), and the on-disk cache is
// keyed separately by beehive ObjectIDs so the name's arbitrary text never reaches
// the filesystem.
//
// Cluster carries connection status (Connected, Healthy conditions + server/principal
// facts); its ClusterCache child carries sync status (Synced condition +
// lastUpdateAt/lastLiveAt).
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

// Name prefixes. The name is a per-kind reconcile/uniqueness key, NOT the identity
// surfaced to consumers (that is the ClusterID, the beehive ObjectID).
//
//   - A kubeconfig-sourced Cluster's name is "kubeconfig/{context}" — the source's
//     natural key, used by the importer so beehive's per-kind name-uniqueness rules out
//     a duplicate for a context. Future sources add their own prefix ("cloud/",
//     "manual/"). Nothing reads a Cluster back by this name.
//   - A ClusterCache's name is "{ClusterID}/{serverUID}": its parent's ObjectID plus
//     the kube-system UID it mirrors. beehive's per-kind name uniqueness then means
//     "one cache per identity per cluster", so a migration to a new UID yields a second, coexisting
//     cache rather than colliding on a one-per-cluster name. Children are enumerated
//     via the owner edge (Client.ListOwned), so the UID need not be known to list a
//     cluster's caches. No "caches/" prefix is needed: ClusterCache is its own kind, so
//     its names already sit in a namespace disjoint from Cluster's.
const namePrefixKubeconfig = "kubeconfig/"

// kubeconfigName returns the beehive name a kubeconfig-sourced Cluster is created
// with: the importer's natural key for one kube-context. It is not an identity —
// see ClusterID.
func kubeconfigName(contextName string) string {
	return namePrefixKubeconfig + contextName
}

// ClusterCacheName returns the name a ClusterCache is created with:
// "{ClusterID}/{serverUID}". The parent-ObjectID segment scopes the UID to one cluster
// (two clusters probing the same identity keep distinct caches); the serverUID segment
// is the migration-turnover key backing beehive's name-uniqueness dedup in
// ensureClusterCache. A creation/dedup key only — caches are enumerated through the
// owner edge, so callers lacking a serverUID can still list them.
func ClusterCacheName(clusterID ClusterID, serverUID string) string {
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
// lives only on the beehive name, never surfaced here. An alias of [ObjectID] — a
// documentation name — so it shares the one GraphQL scalar and (un)marshalling.
type ClusterID = ObjectID

// ClusterCacheID identifies one ClusterCache record: the beehive ObjectID of its
// ClusterCache object. Like [ClusterID], an alias of [ObjectID] naming the cache's own
// id (distinct from its parent ClusterID). A cluster can own several ClusterCache
// records, so the cache id is not derivable from the cluster id.
type ClusterCacheID = ObjectID

// ClusterCacheGVRDiscoveryID identifies one cache's GVR-discovery child: the beehive
// ObjectID of its ClusterCacheGVRDiscovery object. Like [ClusterID], an alias of
// [ObjectID] naming which kind an id refers to.
type ClusterCacheGVRDiscoveryID = ObjectID

// ClusterCacheGVRSyncID identifies one synced kind: the beehive ObjectID of its
// ClusterCacheGVRSync object. Like [ClusterID], an alias of [ObjectID] naming which kind
// an id refers to.
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
	// reasonSyncing: the sync is starting or catching up (listing, pre-first-watch).
	// Condition-only — the event vocabulary names the start
	// transitions SyncStart/ResyncStart instead, so a same-named event reason can't
	// read ambiguously against this condition state.
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
	// reasonDiscoveryPartial: some groups answered and others did not (typically an
	// unavailable aggregated APIService). The kinds that were seen are still synced,
	// but the list is known-incomplete — so the pass adds children without pruning,
	// since a group that failed to answer has not been shown to be gone.
	ReasonDiscoveryPartial = "DiscoveryPartial"
	// reasonDiscoveryFailed: the discovery request itself failed, so nothing is
	// known about the served kinds this pass. The existing children are left alone.
	ReasonDiscoveryFailed = "DiscoveryFailed"
	// reasonDiscoveryDraining: the kind list is current, but a kind the cluster still
	// serves has no live child yet — an earlier prune's child is still draining and holds
	// its name. Its own axis reason rather than the Synced-axis Syncing, so the Discovered
	// condition speaks one vocabulary and the frontend can render this state at all.
	ReasonDiscoveryDraining = "DiscoveryDraining"
	// reasonStale: caught up, but the watch stopped proving itself alive past the
	// threshold — the cache may be behind (a Synced=False state distinct from
	// SyncFailed, which is a hard worker failure).
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

// Condition is beehive's status condition, re-exported so the rest of the package and
// external consumers name it cluster.Condition. Conditions are stored by beehive as
// their own rows on the object (not inside our status blob), so the store owns the
// TransitionedAt stamping, the no-op suppression, and the liveness downgrade.
type Condition = beehive.Condition

// conditionSet accumulates the conditions one reconcile pass will report, upserting by
// type so a later observation in the same pass replaces an earlier one. The whole set is
// written once via ControllerClient.SetConditions, under a single version bump, so a
// watcher never sees half a pass.
type conditionSet []Condition

func (s *conditionSet) set(c Condition) {
	for i := range *s {
		if (*s)[i].Type == c.Type {
			(*s)[i] = c
			return
		}
	}
	*s = append(*s, c)
}

// liveCondition builds a Liveness condition. Every condition this package reports
// describes process-scoped state — a live connection, a live API-server observation, a
// running sync worker — none of which survives a restart. Marking them Liveness is what
// makes beehive downgrade a previous process's write to Unknown until this one
// re-confirms it, instead of serving a stale True.
// The message is capped here, at the one place every condition is built, rather than at
// each write: a condition is persisted and then re-serialized to every fleet-wide watcher
// on each frame, and the messages come from unbounded sources — a raw client-go error, or
// the body of a verbose /readyz listing, which routinely runs to kilobytes.
func liveCondition(t ConditionType, status ConditionStatus, reason, message string) Condition {
	return Condition{
		Type: string(t), Status: status, Reason: reason, Message: truncateMessage(message), Liveness: true,
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
	// sync transitions under, on each record's own timeline —
	// the sync-side parallel of ConnectionEventCategory, exposed generically through
	// the events surface (clusterCacheEvents / clusterCacheEventsWatch, category
	// "sync").
	SyncEventCategory = "sync"
	// maxEventRuns bounds beehive's event-retention ring. beehive keeps the newest N
	// per (object, category) timeline and counts aggregated RUNS, not occurrences — a
	// cluster that has been failing all day is one run with a high Count, not N rows —
	// so this is N state transitions of history, per timeline. It is global (set at
	// beehive.New), so it bounds the ClusterCache's "sync" timeline as well as the
	// Cluster's "connection" one.
	maxEventRuns = 20
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
// Server.UID) and so should sync. It also rides the name so beehive's
// name uniqueness dedups a per-identity create, but the controller reads spec.ServerUID,
// not the name.
type ClusterCacheSpec struct {
	ServerUID string `json:"serverUid"`
}

// ClusterCacheStatus is empty — this kind reports through its conditions alone.
//
// The cache owns no measurement of its own: the sync work happens in its children, and
// each child's freshness is that child's to report (out of band, see
// ClusterCacheGVRSyncStats). The cache-level rollup a UI wants is served read-side by
// Service.WatchCacheSyncHealth rather than stored here — nothing in the object graph acts
// on it. Status is a propagation channel; see ClusterCacheGVRDiscoveryStatus for the rule.
type ClusterCacheStatus struct{}

// eventsKind / eventsAPIVersion / eventsResource identify the cluster's Event collection,
// which the sync machinery treats like any other kind but writes to its own table (see
// eventsync).
//
// The api server serves the same events under two spellings — core `v1` and
// `events.k8s.io/v1` — and they are one underlying store, so exactly one of them may be
// synced or the two workers would fight over the same uid-keyed rows. `v1` is the
// canonical choice: it exists on every supported cluster, and it is the key the cache's
// events table and its kind_counts row are written under.
// eventsAltGroup is that other spelling's api group, which the discovery filter drops.
const (
	eventsKind       = "Event"
	eventsAPIVersion = "v1"
	eventsResource   = "events"
	eventsAltGroup   = "events.k8s.io"
)

// eventsSyncSpec is the Event kind's sync spec. It has one home because two paths produce
// it — the discovery pass, from the API server's answer, and the seed that creates the
// child before any pass has run — and a field they disagreed on would have each pass
// overwrite the other's.
func eventsSyncSpec(enabled bool) ClusterCacheGVRSyncSpec {
	return ClusterCacheGVRSyncSpec{
		Enabled:    enabled,
		APIVersion: eventsAPIVersion,
		Kind:       eventsKind,
		Resource:   eventsResource,
		Namespaced: true,
	}
}

// --- ClusterCacheGVRDiscovery kind types ---

// ClusterCacheGVRDiscoveryGroupKind identifies the ClusterCacheGVRDiscovery beehive resource
// kind: one object per ClusterCache, owned by it. It is
// the discovery layer's anchor — ClusterCacheGVRDiscoveryController reconciles it by asking
// the cluster API which GVRs it serves and maintaining one ClusterCacheGVRSync child per GVR.
var ClusterCacheGVRDiscoveryGroupKind = beehive.GroupKind{Kind: "ClusterCacheGVRDiscovery"}

// ClusterCacheGVRDiscoveryName returns the name a ClusterCacheGVRDiscovery is created with:
// "gvrdiscovery/{cacheObjID}". There is exactly one per ClusterCache, so keying the name on
// the owning cache's ObjectID makes creation idempotent under beehive's name-uniqueness dedup
// (ClusterCacheController.converge). A creation/dedup key only — the child is enumerated
// through the owner edge.
func ClusterCacheGVRDiscoveryName(cacheID beehive.ObjectID) string {
	return "gvrdiscovery/" + strconv.FormatInt(int64(cacheID), 10)
}

// ClusterCacheGVRDiscoverySpec is the desired discovery for one cache.
//
// Enabled is the pause switch, written by ClusterCacheController from the one place that
// knows whether this cache should sync, and relayed by this controller into each
// ClusterCacheGVRSync child. Existence means "this cache has a discovery anchor", NOT "it is
// discovering" — the object lives as long as the cache does, so its children, status and
// conditions survive a pause.
type ClusterCacheGVRDiscoverySpec struct {
	Enabled bool `json:"enabled"`
}

// ClusterCacheGVRDiscoveryStatus is empty, deliberately.
//
// **Status is a propagation channel**: a status write bumps resource_version, which wakes
// every dependent and pushes a frame to every watcher. So it is for state a *dependent*
// must react to — not for gauges about a pass. What this controller observes (when it last
// reached the API server, how many kinds it saw) is read by a UI and by nothing else in
// the object graph, so storing it here would wake the parent cache every pass to propagate
// something no controller reads. Those gauges live out of band, served on request from the
// controller's own memory — see ClusterCacheGVRDiscoveryStats.
//
// What remains here is the Discovered *condition* (a beehive object row, not a status
// field), which is the one part a dependent could legitimately act on.
type ClusterCacheGVRDiscoveryStatus struct{}

// ClusterCacheGVRDiscoveryStats is one cache's live discovery gauges, held in the
// controller's memory and read on request — never stored. The Kubernetes metrics-server
// relationship to the API server: current-on-read, absent when nobody has measured yet,
// and no part of the object graph's change propagation.
//
// Absent (nil at the service boundary) means this process has run no pass for that cache
// yet — after a restart, before the first pass. That is the honest reading: the kind list
// it would describe is durable and still there, but *when it was confirmed* is an
// observation, and this process has not made it.
type ClusterCacheGVRDiscoveryStats struct {
	// LastDiscoveryAt is when the last pass reached the API server (including one that
	// got a partial answer).
	LastDiscoveryAt time.Time
	// ResourceCount is how many syncable GVRs that pass saw. Deliberately not "how many
	// children exist": on a partial pass the un-pruned survivors of the groups that
	// failed to answer are still there, and conflating the two would report a drop that
	// didn't happen.
	ResourceCount int
}

// --- ClusterCacheGVRSync kind types ---

// ClusterCacheGVRSyncGroupKind identifies the ClusterCacheGVRSync beehive resource kind:
// one object per served GVR, owned by its ClusterCacheGVRDiscovery. It is created by
// ClusterCacheGVRDiscoveryController (one per GVR the API server advertises, Events
// excluded) and reconciled here.
var ClusterCacheGVRSyncGroupKind = beehive.GroupKind{Kind: "ClusterCacheGVRSync"}

// ClusterCacheGVRSyncName returns the name a ClusterCacheGVRSync is created with:
// "gvrsync/{discoveryObjID}/{apiVersion}/{resource}". The owning discovery object's id
// scopes it (names are unique per kind across every cache), and the GVR identity makes it
// deterministic — which is what lets a discovery pass be a set reconcile: the desired
// names are computed from the API server's answer and compared against the children that
// exist, with no per-child bookkeeping to keep in step.
//
// (apiVersion, resource) rather than the Kind because the resource plural is what the
// sync worker's REST path needs, and it is the discriminator the API server itself
// guarantees unique within a group-version.
func ClusterCacheGVRSyncName(discoveryID beehive.ObjectID, apiVersion, resource string) string {
	return "gvrsync/" + strconv.FormatInt(int64(discoveryID), 10) + "/" + apiVersion + "/" + resource
}

// ClusterCacheGVRSyncSpec is the desired sync for one GVR, written wholly by
// ClusterCacheGVRDiscoveryController from one entry of the API server's discovery answer.
//
// Enabled is the pause switch, pushed down the chain exactly as the discovery anchor's
// is: the cache controller decides whether this cache syncs, the discovery controller relays
// that to each child, and the child never re-derives it. The identity fields are refreshed on
// every discovery pass, so a kind that changes shape (e.g. becomes cluster-scoped across an
// upgrade) converges without the object being recreated.
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
	// Conditions are beehive object conditions, not part of Status — read off the
	// object rather than out of the status blob.
	Conditions []Condition
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
	// Conditions are beehive object conditions, not part of Status — read off the
	// object rather than out of the status blob.
	Conditions []Condition
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

// ClusterCacheGVRDiscovery is the domain view of one ClusterCacheGVRDiscovery beehive
// object: a cache's kind-catalog record — which kinds the cluster serves, when that was
// last confirmed, and whether the confirmation was complete. Streamed standalone via
// WatchGVRDiscoveries and joined onto its cache client-side by CacheID, exactly as
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
	Type       ChangeType
	Object     ClusterDataObject
	CacheID    ClusterCacheID
	APIVersion string
	Resource   string
}

// --- Status equality helpers (skip-the-write guards) ---

// ClusterStatusEqual reports whether two ClusterStatus blocks are observably
// equal — the ClusterCoreController's skip-the-write guard.
func ClusterStatusEqual(a, b ClusterStatus) bool {
	return ptrEqual(a.Source.Kubeconfig, b.Source.Kubeconfig) &&
		ptrEqual(a.Server.UID, b.Server.UID) &&
		ptrEqual(a.Server.Version, b.Server.Version) &&
		ptrEqual(a.Principal.Username, b.Principal.Username) &&
		timePtrEqual(a.LastConnectedAt, b.LastConnectedAt)
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
