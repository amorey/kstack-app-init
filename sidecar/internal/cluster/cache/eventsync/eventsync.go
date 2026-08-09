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

// Package eventsync writes one cluster's Kubernetes Events into that cluster's cache.
// It is a kubesync.Store, nothing more: Events are synced by the same controller, the
// same worker and the same list/watch state machine as every other kind, and diverge
// only in where the rows land.
//
// That divergence is real, which is why the store is separate: the events table has its
// own columns (involved-object identity, first/last seen, count) and an FTS index, its
// retention is server-mirrored by the relist prune rather than managed by the janitor, it
// wakes a dedicated write broker so an event burst can't drive the object watches, and
// the two api spellings the server serves events under (core/v1 and events.k8s.io/v1)
// normalise into one uid-keyed row.
package eventsync

import (
	"errors"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// NewStore is where the cluster's events land: the cache's dedicated events table.
// Composed with a source into a kubesync.Worker by the caller.
//
// Events ride the same sync machinery as every other kind and differ only here, which is
// the whole reason this is a Store and not a parallel worker: their table has its own
// columns and FTS index, their retention is server-mirrored by the relist prune rather
// than a TTL, they wake a separate write broker, and the two api spellings the server
// serves them under normalise into one uid-keyed row.
func NewStore(cdb *store.ClusterDB) (kubesync.Store, error) {
	if cdb == nil {
		return nil, errors.New("eventsync: nil cache handle")
	}
	return newEventStore(cdb), nil
}
