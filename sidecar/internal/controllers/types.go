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
// types for clusters and their caches, three beehive controller
// implementations (ClusterSource, Cluster, ClusterCache), a kubeconfig
// importer, and the two sub-packages (clustercache, clustersync) that back
// the per-cluster on-disk mirrors.
//
// The three beehive resource kinds and their ownership chain:
//
//	ClusterSource  (slug: "sources/kubeconfig/{contextName}")
//	    ↓ owns
//	Cluster        (slug: "clusters/{uuid}")
//	    ↓ owns
//	ClusterCache   (slug: "caches/{uuid}")
//
// Domain types here are a superset of what beehive stores: the domain Cluster
// (returned to resolvers) joins the Cluster and ClusterCache beehive objects
// into one combined status view — Cluster carries connection status (Connected,
// Healthy conditions + server/principal facts), ClusterCache carries sync
// status (Synced condition + lastSyncedAt).
package controllers

import (
	"errors"
	"time"

	"github.com/amorey/beehive"
)

// ErrNotFound is returned by client helpers when no cluster with the given id
// is tracked.
var ErrNotFound = errors.New("controllers: cluster not found")

// Slug prefix constants for the three beehive resource kinds.
const (
	slugPrefixClusterSource = "sources/kubeconfig/"
	slugPrefixCluster       = "clusters/"
	slugPrefixClusterCache  = "caches/"
)

// ClusterSourceSlug returns the beehive slug for a ClusterSource object from
// its kubeconfig context name.
func ClusterSourceSlug(contextName string) string {
	return slugPrefixClusterSource + contextName
}

// ClusterSlug returns the beehive slug for a Cluster object from its UUID.
func ClusterSlug(id ClusterID) string {
	return slugPrefixCluster + string(id)
}

// ClusterCacheSlug returns the beehive slug for a ClusterCache object from the
// parent cluster UUID.
func ClusterCacheSlug(id ClusterID) string {
	return slugPrefixClusterCache + string(id)
}

// ClusterIDFromSlug extracts the ClusterID from a Cluster or ClusterCache
// slug. Returns empty string for an unrecognised prefix.
func ClusterIDFromSlug(slug string) ClusterID {
	if len(slug) > len(slugPrefixCluster) && slug[:len(slugPrefixCluster)] == slugPrefixCluster {
		return ClusterID(slug[len(slugPrefixCluster):])
	}
	if len(slug) > len(slugPrefixClusterCache) && slug[:len(slugPrefixClusterCache)] == slugPrefixClusterCache {
		return ClusterID(slug[len(slugPrefixClusterCache):])
	}
	return ""
}

// --- Identity ---

// ClusterID uniquely and stably identifies a cluster record across context
// renames and credential changes. Values are opaque to consumers (it binds to
// the GraphQL ID scalar); the ClusterSourceController assigns a random UUID at
// registration, deliberately independent of the remote cluster's UID (which is
// unknown until the first probe, and shared by two records pointing at the same
// physical cluster). Externally a ClusterID is the UUID string; internally
// beehive stores it as the slug "clusters/{uuid}".
type ClusterID string

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
	// ClusterConnectionStatus.
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
	// drivers pre-first-watch).
	ReasonSyncing = "Syncing"
	// reasonWatching: every driver reached its watch phase — the cache is
	// caught up and streaming deltas.
	ReasonWatching = "Watching"
	// reasonSyncFailed: the engine hit an engine-level failure (discovery,
	// cache open) and is retrying with backoff.
	ReasonSyncFailed = "SyncFailed"
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

// --- ClusterSource kind types ---

// ClusterSourceGroupKind identifies the ClusterSource beehive resource kind.
var ClusterSourceGroupKind = beehive.GroupKind{Kind: "ClusterSource"}

// ClusterSourceSpec is the desired-state (importer-written) half of a
// ClusterSource record: the kubeconfig observation — what the importer last
// saw for this context. IsPresent=false marks an orphaned entry (context
// departed the kubeconfig) without deleting the record (and its owned Cluster
// child).
type ClusterSourceSpec struct {
	// ContextName is the kube-context name this source tracks.
	ContextName string `json:"contextName"`
	// ClusterName is the cluster entry name the context references (from
	// kubeconfig); empty until first observed.
	ClusterName string `json:"clusterName"`
	// UserName is the user (authInfo) entry name the context references (from
	// kubeconfig); empty until first observed.
	UserName string `json:"userName"`
	// IsPresent is true while the context is present in the kubeconfig.
	IsPresent bool `json:"isPresent"`
	// IsDefault is true when the context is the kubeconfig's current-context.
	IsDefault bool `json:"isDefault"`
}

// ClusterSourceObjStatus is the controller-written half of a ClusterSource
// beehive object: what the ClusterSourceController has done in response to the
// spec. This is the stored status for the ClusterSource beehive kind — it is
// not exposed in GraphQL (only Cluster and ClusterCache are).
type ClusterSourceObjStatus struct {
	// ClusterID is the UUID of the Cluster child object the controller created
	// for this source; nil until the child is created.
	ClusterID *ClusterID `json:"clusterID,omitempty"`
}

// --- Cluster kind types ---

// ClusterGroupKind identifies the Cluster beehive resource kind.
var ClusterGroupKind = beehive.GroupKind{Kind: "Cluster"}

// KubeconfigStatus is the kubeconfig-sourced record's last-known kubeconfig
// observation: the cluster/user entry names and presence. Cached from the
// last time the context was present so it survives orphaning.
type KubeconfigStatus struct {
	Cluster   string `json:"cluster"`
	User      string `json:"user"`
	IsPresent bool   `json:"isPresent"`
	IsDefault bool   `json:"isDefault"`
}

// ClusterSource is the discriminated union naming where a cluster record
// comes from and how its credentials resolve.
type ClusterSource struct {
	Kubeconfig *ClusterSourceKubeconfig `json:"kubeconfig,omitempty"`
}

// ClusterSourceKubeconfig is the kubeconfig-sourced variant of ClusterSource.
type ClusterSourceKubeconfig struct {
	Context string `json:"context"`
}

// ClusterSourceStatus is the status-side counterpart of ClusterSource.
type ClusterSourceStatus struct {
	Kubeconfig *KubeconfigStatus `json:"kubeconfig,omitempty"`
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

// ClusterSpec is a cluster record's desired state: the user/API-owned fields
// plus internal trigger counters (RetryGeneration, PokeSyncGeneration) that
// are not exposed in the GraphQL schema but are stored in the beehive spec
// JSON and used to trigger immediate reconciles without a dedicated "enqueue"
// API.
type ClusterSpec struct {
	Name          *string       `json:"name,omitempty"`
	IsSyncEnabled bool          `json:"isSyncEnabled"`
	IsActive      bool          `json:"isActive"`
	Source        ClusterSource `json:"source"`

	// SourceObs is the kubeconfig observation written by the
	// ClusterSourceController when it detects changes in the kubeconfig
	// (cluster/user entry names, presence, default status). It is stored in
	// the spec because only the Cluster's own controller (ClusterController)
	// can write its status, and the ClusterSourceController needs a write path
	// that uses the ordinary Client.Update. The ClusterController copies
	// SourceObs into ClusterConnectionStatus.Source.Kubeconfig during its
	// reconcile, so the GraphQL layer reads from status as expected.
	SourceObs *KubeconfigStatus `json:"sourceObs,omitempty"`

	// RetryGeneration is incremented by Service.ConnectionRetry to force an
	// immediate re-probe, resetting the cluster controller's failure backoff.
	// Not in the GraphQL schema.
	RetryGeneration int64 `json:"retryGeneration,omitempty"`
	// PokeSyncGeneration is incremented by the poke handler to bounce running
	// sync engines. Detected by the ClusterCache controller via the Cluster
	// DependsOn edge. Not in the GraphQL schema.
	PokeSyncGeneration int64 `json:"pokeSyncGeneration,omitempty"`
}

// ClusterConnectionStatus is the Cluster beehive kind's stored status:
// connection/health observations from the ClusterController. Distinct from the
// domain ClusterStatus (which also includes sync status from ClusterCache).
type ClusterConnectionStatus struct {
	Source          ClusterSourceStatus `json:"source"`
	Server          ClusterServer       `json:"server"`
	Principal       ClusterPrincipal    `json:"principal"`
	LastConnectedAt *time.Time          `json:"lastConnectedAt,omitempty"`
	// Conditions holds the controller-written conditions (Connected, Healthy).
	Conditions []ClusterCondition `json:"conditions"`
}

// --- ClusterCache kind types ---

// ClusterCacheGroupKind identifies the ClusterCache beehive resource kind.
var ClusterCacheGroupKind = beehive.GroupKind{Kind: "ClusterCache"}

// ClusterCacheSpec is the ClusterCache kind's spec. It carries no user-facing
// fields; the parent cluster UUID is encoded in the object's slug
// ("caches/{uuid}"). The ClusterController creates ClusterCache objects with
// this empty spec.
type ClusterCacheSpec struct{}

// ClusterCacheStatus is the ClusterCache kind's stored status, written by the
// ClusterCacheController. It serves as both the beehive stored type and the
// domain ClusterSyncStatus.
type ClusterCacheStatus struct {
	// Conditions holds the sync-controller-owned condition (Synced).
	Conditions []ClusterCondition `json:"conditions"`
	// LastSyncedAt is when the cache last received fresh data; nil if never.
	LastSyncedAt *time.Time `json:"lastSyncedAt,omitempty"`
	// ObservedPokeSyncGeneration is the last PokeSyncGeneration value the
	// ClusterCacheController acted on; used to detect poke signals via the
	// Cluster DependsOn edge.
	ObservedPokeSyncGeneration int64 `json:"observedPokeSyncGeneration,omitempty"`
}

// --- Domain (combined) types exposed to resolvers ---

// ClusterStatus is the combined observed state of a cluster as returned by
// the resolver layer: connection/health from the Cluster beehive object plus
// sync status from its ClusterCache peer.
type ClusterStatus struct {
	Source          ClusterSourceStatus `json:"source"`
	Server          ClusterServer       `json:"server"`
	Principal       ClusterPrincipal    `json:"principal"`
	LastConnectedAt *time.Time          `json:"lastConnectedAt,omitempty"`
	// Conditions holds the cluster-controller-owned conditions (Connected,
	// Healthy).
	Conditions []ClusterCondition `json:"conditions"`
	// SyncStatus is the sync controller's status block, populated from the
	// ClusterCache peer object.
	SyncStatus ClusterCacheStatus `json:"syncStatus"`
}

// Cluster is the domain record for one tracked cluster connection (one
// kube-context): the restart-surviving facts about it. Assembled by the
// resolver layer from the Cluster + ClusterCache beehive objects.
type Cluster struct {
	ID         ClusterID
	Generation int64
	CreatedAt  time.Time
	ArchivedAt *time.Time // always nil today; reserved for future archiving
	DeletedAt  *time.Time // derived from obj.DeletionRequestedAt

	Spec   ClusterSpec
	Status ClusterStatus
}

// ClusterPatch describes a user-initiated mutation. Nil fields are unchanged.
type ClusterPatch struct {
	IsSyncEnabled *bool
}

// --- Cache statistics types (for the ClusterCache GraphQL resolver) ---

// CachedResourceStats is the per-resource breakdown of one cluster's cache.
type CachedResourceStats struct {
	Resource      string
	Count         int
	LastUpdatedAt *time.Time
}

// CacheStats reports a cluster's on-disk cache statistics.
type CacheStats struct {
	Exists    bool
	Bytes     int64
	Resources []CachedResourceStats
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

// ClusterConnectionStatusEqual reports whether two ClusterConnectionStatus
// blocks are observably equal — the ClusterController's skip-the-write guard.
func ClusterConnectionStatusEqual(a, b ClusterConnectionStatus) bool {
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
		ConditionsEqual(a.Conditions, b.Conditions) &&
		a.ObservedPokeSyncGeneration == b.ObservedPokeSyncGeneration
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
