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
package domain

import (
	"time"
)

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

// ClusterDataKindWatchFrame is one frame on a cache's kind-catalog watch: what happened
// (Type) to which kind (Kind), from which cache (CacheID). On subscribe every catalog
// row arrives as Added (the snapshot); thereafter a new kind is Added, a kind whose
// fields change (chiefly its live Count) is Modified, and a kind leaving the catalog is
// Deleted (carrying its last-known row). Consumers key on APIVersion + Resource.
// CacheID is the frame's provenance: a client watching the active cache can reject a
// late frame from a superseded cache (one still draining after a cache/context switch).
// Binds 1:1 to the GraphQL ClusterDataKindWatchFrame.
type ClusterDataKindWatchFrame struct {
	Type FrameType
	// Kind is nil on a Bookmark.
	Kind    *ClusterDataKind
	CacheID ClusterCacheID
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

// ClusterDataEventWatchFrame is one frame on a cache's events watch: what happened
// (Type) to which event (Event), from which cache (CacheID). On subscribe the newest
// window of events arrives as Added (the snapshot); thereafter a new event is Added, an
// event whose fields change (chiefly its Count/LastSeen as it re-fires) is Modified, and
// an event leaving the watched window — dropped from the cache, or aged past the window
// as newer events arrive — is Deleted (carrying its last-known row). Consumers key on
// UID. CacheID is the frame's provenance, mirroring ClusterDataKindWatchFrame: a client
// watching the active cache can reject a late frame from a superseded cache. Binds 1:1
// to the GraphQL ClusterDataEventWatchFrame.
type ClusterDataEventWatchFrame struct {
	Type FrameType
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

// ClusterDataObjectWatchFrame is one frame on a cache's per-kind objects watch: what happened
// (Type) to which object (Object), from which cache (CacheID) and kind (APIVersion +
// Resource). On subscribe the current object set for the kind arrives as Added (the
// snapshot); thereafter a new object is Added, an object whose fields change is Modified,
// and one removed from the cache is Deleted (carrying its last-known row). Consumers key
// on UID. CacheID + (APIVersion, Resource) are the frame's provenance: unlike the kinds/
// events watches (keyed only by cache), this watch is keyed by kind too, so a client
// switching resources within one cache uses the kind to reject a straggler from the
// previous kind's still-draining subscription. Binds 1:1 to the GraphQL
// ClusterDataObjectWatchFrame.
type ClusterDataObjectWatchFrame struct {
	Type FrameType
	// Object is nil on a Bookmark.
	Object     *ClusterDataObject
	CacheID    ClusterCacheID
	APIVersion string
	Resource   string
}
