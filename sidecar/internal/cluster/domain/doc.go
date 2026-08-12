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
