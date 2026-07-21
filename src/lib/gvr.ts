// Copyright 2026 The Kubetail Authors
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

// A resource type's identity — Kubernetes' GVR, but carrying `apiVersion` (group/version
// combined, e.g. "apps/v1" or "v1" for the core group) rather than split group+version,
// because the wire, the cache, and `ServerKind` all speak apiVersion. Together with the
// plural `resource` it uniquely identifies a kind within a cache.
//
// `gvrKey` is the one canonical string form, reused as: the kinds-catalog map key
// (`useClusterDataKinds`), the kind half of the objects-watch provenance
// (`useClusterDataObjects`), and the per-kind column-registry key (ObjectsTable). One
// spelling of "which kind" everywhere, so those three can't drift apart.
export type GVR = { apiVersion: string; resource: string };

export function gvrKey(g: GVR): string {
  return `${g.apiVersion}/${g.resource}`;
}
