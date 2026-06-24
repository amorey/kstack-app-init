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
//	Cluster        (slug: "clusters/{uuid}")
//	    ↓ owns
//	ClusterCache   (slug: "caches/{uuid}")
//
// Cluster objects are created directly by the kubeconfig importer (one per
// kube-context); there is no separate intake kind.
//
// Domain types here are a superset of what beehive stores: the domain Cluster
// (returned to resolvers) joins the Cluster and ClusterCache beehive objects
// into one combined status view — Cluster carries connection status (Connected,
// Healthy conditions + server/principal facts), ClusterCache carries sync
// status (Synced condition + lastSyncedAt).
package cluster

import (
	"errors"
	"time"

	"github.com/amorey/beehive"
)

// ErrNotFound is returned by client helpers when no cluster with the given id
// is tracked.
var ErrNotFound = errors.New("controllers: cluster not found")

// Slug prefix constants for the two beehive resource kinds.
const (
	slugPrefixCluster      = "clusters/"
	slugPrefixClusterCache = "caches/"
)

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
// the GraphQL ID scalar); the kubeconfig importer assigns a random UUID the
// first time it sees a context, deliberately independent of the remote
// cluster's UID (which is unknown until the first probe, and shared by two
// records pointing at the same physical cluster). Externally a ClusterID is
// the UUID string; internally
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

// --- ClusterCache kind types ---

// ClusterCacheGroupKind identifies the ClusterCache beehive resource kind.
var ClusterCacheGroupKind = beehive.GroupKind{Kind: "ClusterCache"}

// ClusterCacheSpec is the ClusterCache kind's spec. It carries no user-facing
// fields; the parent cluster UUID is encoded in the object's slug
// ("caches/{uuid}"). The ClusterCoreController creates ClusterCache objects with
// this empty spec.
type ClusterCacheSpec struct{}

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

// ClusterCache is the domain view of a cluster's owned ClusterCache child,
// mirroring the beehive owner chain: the sync Status (joined from the
// ClusterCache beehive object) plus the cluster ID, which the live-stats
// resolver needs to query the per-cluster cache. This replaces the old
// model.ClusterStatus wrapper — the ID now rides a real domain object.
type ClusterCache struct {
	ID     ClusterID
	Status ClusterCacheStatus
}

// Cluster is the domain record for one tracked cluster connection (one
// kube-context): the restart-surviving facts about it. Assembled by the service
// layer from the Cluster + ClusterCache beehive objects. Status binds directly
// to the stored Cluster-kind status; Cache carries the ClusterCache child.
type Cluster struct {
	ID         ClusterID
	Generation int64
	CreatedAt  time.Time
	DeletedAt  *time.Time // derived from obj.DeletionRequestedAt

	Spec   ClusterSpec
	Status ClusterStatus
	Cache  ClusterCache
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
	Exists    bool
	Bytes     int64
	Resources []CachedResource
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
