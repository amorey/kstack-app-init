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

// Package eventsync writes one cluster's Kubernetes Events into its cache. Events are an
// ordinary synced kind — same controller, worker and state machine — so this is a
// kubesync.Store and nothing more; it diverges only in where the rows land: own columns
// + FTS index, retention server-mirrored by the relist prune (not the janitor), a
// dedicated write broker, and both api spellings normalised into one uid-keyed row.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
package eventsync

import (
	"errors"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// NewStore returns the events-table Store, which the caller composes with a source into
// a kubesync.Worker.
func NewStore(cdb *store.ClusterDB) (kubesync.Store, error) {
	if cdb == nil {
		return nil, errors.New("eventsync: nil cache handle")
	}
	return newEventStore(cdb), nil
}
