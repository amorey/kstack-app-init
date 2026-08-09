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

// Package objectsync writes ONE Kubernetes kind's objects into a cluster's cache: a
// kubesync.Store plus Kind, with source and worker composed by
// ClusterCacheGVRSyncController.
//
// One store per kind, matching the object graph's granularity — the discovery pass
// creates a sync child per served GVR, so a new CRD or a forbidden watch is isolated to
// its own worker, conditions, and slice of the objects table.
package objectsync

import (
	"context"
	"errors"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// NewStore returns the Store for one kind's slice of the shared objects table; the root
// package's worker factory picks this or eventsync's by kind.
func NewStore(cdb *store.ClusterDB, kind Kind) (kubesync.Store, error) {
	if cdb == nil {
		return nil, errors.New("objectsync: nil cache handle")
	}
	return newObjectStore(cdb, kind), nil
}

// Forget removes every trace of one kind — objects, edges, catalog entry, resume cookie —
// when its sync child is deleted (the cluster stopped serving the kind). A package
// function, not a method, because by then the worker has drained and is gone.
func Forget(ctx context.Context, cdb *store.ClusterDB, kind Kind) error {
	return newObjectStore(cdb, kind).Forget(ctx)
}
