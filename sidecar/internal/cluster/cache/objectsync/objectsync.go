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

// Package objectsync writes ONE Kubernetes kind's objects into a cluster's cache. Like
// eventsync it is a kubesync.Store and nothing more (plus Kind, the identity a caller
// addresses a collection by); the source and the worker are composed by
// ClusterCacheGVRSyncController, which is what lets Events ride the same machinery with a
// different store.
//
// One store per kind, not one per cluster, because that is the granularity the object
// graph already has: the discovery pass creates a sync child per served GVR, so a CRD
// appearing or a single kind's watch being forbidden is isolated to its own worker, its
// own conditions, and its own slice of the objects table.
package objectsync

import (
	"context"
	"errors"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// NewStore is where one kind's objects land: its slice of the cache's shared objects
// table. The caller composes it with a source into a kubesync.Worker — see the root
// package's worker factory, which picks this or eventsync's store by kind.
func NewStore(cdb *store.ClusterDB, kind Kind) (kubesync.Store, error) {
	if cdb == nil {
		return nil, errors.New("objectsync: nil cache handle")
	}
	return newObjectStore(cdb, kind), nil
}

// Forget removes every trace of one kind from a cache — its objects and their edges, its
// catalog entry, and its resume cookie. The controller calls it when a sync child is
// deleted, which happens when the cluster stops serving the kind (an uninstalled CRD).
//
// It is a package function rather than a method on the worker because it runs when there
// is no worker: the child is being collected, so its worker has already drained.
func Forget(ctx context.Context, cdb *store.ClusterDB, kind Kind) error {
	return newObjectStore(cdb, kind).Forget(ctx)
}
