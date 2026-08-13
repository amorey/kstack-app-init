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

// The Cluster kind: a tracked kube-context. Its beehive spec/status shapes, the domain
// record served to resolvers, and its delta-watch change. Mirrors the Cluster section
// of graph/schema.graphqls.
package domain

import (
	"time"

	"github.com/amorey/beehive"
)

// ClusterGroupKind identifies the Cluster beehive resource kind.
var ClusterGroupKind = beehive.GroupKind{Kind: "Cluster"}

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

// ClusterWatchFrame is one frame on the cluster list watch: what happened (Type) to
// which cluster (Cluster), or the Bookmark closing the snapshot, which carries no
// cluster. On a Deleted change Cluster holds the last-known state; consumers key on
// Cluster.ID. Binds 1:1 to the GraphQL ClusterWatchFrame.
type ClusterWatchFrame struct {
	Type    FrameType
	Cluster *Cluster
}
