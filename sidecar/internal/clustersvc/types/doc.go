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

// Package types is the cluster subsystem's shared vocabulary: the four beehive
// kinds with their spec/status shapes, identity (ObjectID), conditions, the
// delta-watch change types, and the records mirrored into a per-cluster cache.
// Every type the GraphQL schema binds lives here.
//
// It is a leaf, and must stay one: the boundary and everything beneath it depend on
// it, and it depends on neither. Anything that would make it import a sibling belongs
// in that sibling instead.
//
// The four beehive kinds and their ownership chain:
//
//	Cluster                      (name: "{source}/{naturalKey}", e.g. "kubeconfig/{context}")
//	    ↓ owns
//	ClusterCache                 (name: "{ClusterID}/{serverUID}")
//	    ↓ owns
//	ClusterCachedCatalog    (name: "cachedcatalog/{CacheID}" — one per cache)
//	    ↓ owns
//	ClusterCachedResource   (name: "cachedresource/{CatalogID}/{apiVersion}/{resource}")
//
// A name is a per-kind reconcile key, never an identity. There is one Cluster kind
// and each source owns a disjoint name namespace inside it, so a source reconciles by
// name under beehive's name-uniqueness; the on-disk cache is keyed by ObjectID
// instead, so a name's arbitrary text never reaches the filesystem.
//
// A kind's GroupKind.Kind string is its Go type name, and both the Kind and the name
// prefixes above are persisted. Renaming one is a store migration the moment anything
// writes — free only while the backend is a shell.
//
// Cluster carries connection status (Connected, Healthy conditions + server/principal
// facts); its ClusterCache child carries sync status, folded per kind from the
// ClusterCachedResource records below it.
package types
