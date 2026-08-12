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

// Helpers used only by the ClusterService boundary: owner-edge reads for the domain
// builders, and the limits its event surface applies.
package cluster

import (
	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// ownerObjectID reads an owner id off the eager-loaded owner edge, or 0 when there is none.
// Best-effort: a hard-deleted child has no edge, but the client already dropped it on the
// soft-delete change, and consumers key removal on the object's own id.
//
// Reads the edge the watch loaded with WithLoads(LoadOwner()), resolved once per batch — a
// per-object GetOwner would be an N+1 per frame per subscriber against an edge written once
// at creation, which is why every domain builder goes through this.
func ownerObjectID[Spec, Status any](obj *beehive.Object[Spec, Status]) domain.ObjectID {
	owner, ok, err := obj.Owner()
	if err != nil || !ok {
		return 0
	}
	return domain.ObjectID(owner.ID)
}

// derefOrZero returns *p, or the zero value when nil. beehive leaves Status nil until first
// written, while domain records serve it by value — absent and zeroed say the same thing.
func derefOrZero[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// defaultEventLimit bounds an events read (or watch snapshot) when the caller
// gives no explicit limit.
const defaultEventLimit = 50

// maxEventRuns bounds retention per (object, category) timeline — counted in
// aggregated RUNS (state transitions), not occurrences. Global (set at beehive.New).
const maxEventRuns = 20
