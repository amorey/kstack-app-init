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
// objects and Events read back out of it, each paired with its delta-watch change.
// Mirrors the cluster-data section of graph/schema.graphqls.
package types

import (
	"time"
)

// ClusterDataKind is one entry in a cluster's discovered kind catalog — a kind the API
// server advertises, built-in or CRD. It powers the dashboard's dynamic resource nav,
// which is why it carries the plural resource name (to dedupe against the curated
// catalog) and the api group via APIVersion (to bucket the kind into a nav group).
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

// ClusterDataKindWatchFrame is one frame on a cache's kind-catalog watch. Consumers
// key on APIVersion + Resource; a kind whose Count changes arrives as Modified.
// CacheID is provenance: a client watching the active cache uses it to reject a late
// frame from a superseded one still draining after a switch.
type ClusterDataKindWatchFrame struct {
	Type DeltaFrameType
	// Kind is nil on a Bookmark.
	Kind    *ClusterDataKind
	CacheID ClusterCacheID
}

// ClusterDataEvent is one cached Kubernetes Event from a cluster's synced data,
// powering the dashboard's events table. The involved-object identity is flattened
// onto the record and any field of it may be empty (a name-only reference carries no
// namespace); the raw event body is not exposed.
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

// ClusterDataEventWatchFrame is one frame on a cache's events watch. Consumers key on
// UID; a re-firing event arrives as Modified (its Count/LastSeen move), and one that
// ages out of the newest-window snapshot arrives as Deleted carrying its last-known
// row. CacheID is provenance, as on ClusterDataKindWatchFrame.
type ClusterDataEventWatchFrame struct {
	Type DeltaFrameType
	// Event is nil on a Bookmark.
	Event   *ClusterDataEvent
	CacheID ClusterCacheID
}

// ClusterDataObject is one cached Kubernetes object read from the active cache. The
// typed identity fields are enough to key the watch, sort, and render Name/Namespace/
// Age without parsing the body; RawJSON carries the full native body, from which the
// frontend derives kind-specific columns. Keeping RawJSON in the struct is what makes
// an in-place edit differ across two reads and surface as Modified — and its string
// underlying type is what keeps the struct comparable for that diff.
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

// ClusterDataObjectWatchFrame is one frame on a cache's per-kind objects watch.
// Consumers key on UID. Provenance is CacheID *plus* (APIVersion, Resource): this
// watch is keyed by kind as well as cache, so a client switching resources within one
// cache needs the kind to reject a straggler from the previous subscription.
type ClusterDataObjectWatchFrame struct {
	Type DeltaFrameType
	// Object is nil on a Bookmark.
	Object     *ClusterDataObject
	CacheID    ClusterCacheID
	APIVersion string
	Resource   string
}
