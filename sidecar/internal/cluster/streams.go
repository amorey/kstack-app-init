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

package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// watchListChan folds a beehive kind watch (snapshot + change stream) into one
// Kubernetes-style delta stream. A deletion-pending object is collapsed to Deleted
// (List/Get hide tombstones, so the watch removes it at once; the trailing hard
// Deleted repeats idempotently). beehive's terminal Failed change ends the stream
// after a log line. fn's obj is nil only on a Deleted whose final state could not
// be decoded; the removal is still reported by id. Out closes on exit.
func watchListChan[Spec, Status, Out any](
	ctx context.Context,
	kind string,
	snap beehive.ObjectListSnapshot[Spec, Status],
	src <-chan beehive.ObjectChange[Spec, Status],
	fn func(domain.ChangeType, beehive.ObjectID, *beehive.Object[Spec, Status]) Out,
) <-chan Out {
	out := make(chan Out, 1)
	go func() {
		defer close(out)
		// beehive.ChangeType and ChangeType share string values by construction.
		domainType := func(t beehive.ChangeType, obj *beehive.Object[Spec, Status]) domain.ChangeType {
			if obj != nil && obj.DeletionRequestedAt != nil {
				return domain.ChangeDeleted
			}
			return domain.ChangeType(t)
		}
		for _, obj := range snap.Objects {
			if !send(ctx, out, fn(domainType(beehive.Added, obj), obj.ID, obj)) {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-src:
				if !ok {
					return
				}
				if ev.Type == beehive.Failed {
					if ctx.Err() == nil {
						slog.Warn("clusterservice: object watch ended", "kind", kind, "err", ev.Err)
					}
					return
				}
				if !send(ctx, out, fn(domainType(ev.Type, ev.Object), ev.ID, ev.Object)) {
					return
				}
			}
		}
	}()
	return out
}

// cacheDeltaWatch is the shared machinery behind the three ClusterData*Watch
// streams: it follows one cache's db across its lifecycle (WatchDB bind/rebind),
// coalesces write pings on a trailing-edge debounce, and on each fire re-reads a
// keyed snapshot and diffs it against the last — Added/Modified/Deleted per key,
// and Deleted for every held key when the cache closes. `snapshot` must return an
// ordered slice (its order is the Added-burst order); T must be comparable so a
// changed value is detected by ==. Closes when ctx ends or the store shuts down.
func cacheDeltaWatch[T comparable, C any](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	snapshot func(context.Context, *store.ClusterDB) ([]T, error),
	keyOf func(T) string,
	mkChange func(domain.ChangeType, T) C,
) <-chan C {
	out := make(chan C, 1)
	prev := map[string]T{}

	// emit diffs a fresh snapshot against prev: Added/Modified in snapshot order,
	// then Deleted for vanished keys (map order).
	emit := func(db *store.ClusterDB) (bool, bool) {
		items, err := snapshot(ctx, db)
		if err != nil {
			if ctx.Err() != nil {
				return false, false
			}
			// Keep the stream and ask for a retry; waiting for the next write ping
			// would strand a kind nobody writes to.
			slog.Warn("clusterservice: cache watch read failed", "cache", cacheID, "err", err)
			return true, true
		}
		next := make(map[string]T, len(items))
		for _, v := range items {
			key := keyOf(v)
			next[key] = v
			old, existed := prev[key]
			switch {
			case !existed:
				if !send(ctx, out, mkChange(domain.ChangeAdded, v)) {
					return false, false
				}
			case old != v:
				if !send(ctx, out, mkChange(domain.ChangeModified, v)) {
					return false, false
				}
			}
		}
		for key, v := range prev {
			if _, ok := next[key]; !ok {
				if !send(ctx, out, mkChange(domain.ChangeDeleted, v)) {
					return false, false
				}
			}
		}
		prev = next
		return true, false
	}

	// emitEmpty sends one Deleted per held value and clears prev — run when the
	// cache closes so a never-reopened cache leaves no stale rows.
	emitEmpty := func() bool {
		for _, v := range prev {
			if !send(ctx, out, mkChange(domain.ChangeDeleted, v)) {
				return false
			}
		}
		prev = map[string]T{}
		return true
	}

	go func() {
		defer close(out)
		cacheWatchLoop(ctx, mgr, cacheID, debounceDur, cacheWatchRetryInterval, subscribe, emit, emitEmpty)
	}()
	return out
}

// cacheGaugeWatch is cacheDeltaWatch's gauge counterpart: one whole-cache
// measurement, re-read on the same cadence, emitted only when it CHANGES (the
// dedupe is what makes per-write pings affordable). `closed` supplies the value
// to emit when the cache goes away. See
// docs/adr/2026-08-09-status-propagation-gauges.md.
func cacheGaugeWatch[T comparable](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	read func(context.Context, *store.ClusterDB) (T, error),
	closed func() T,
) <-chan T {
	out := make(chan T, 1)
	var (
		last T
		sent bool
	)

	emitIfChanged := func(v T) bool {
		if sent && v == last {
			return true
		}
		if !send(ctx, out, v) {
			return false
		}
		last, sent = v, true
		return true
	}

	go func() {
		defer close(out)
		cacheWatchLoop(ctx, mgr, cacheID, debounceDur, cacheWatchRetryInterval, subscribe,
			func(db *store.ClusterDB) (bool, bool) {
				v, err := read(ctx, db)
				if err != nil {
					if ctx.Err() != nil {
						return false, false
					}
					// Keep the stream and retry; a failed read that waited for the
					// next ping would freeze the deduped value with no way to tell.
					slog.Warn("clusterservice: cache gauge read failed", "cache", cacheID, "err", err)
					return true, true
				}
				return emitIfChanged(v), false
			},
			func() bool { return emitIfChanged(closed()) },
		)
	}()
	return out
}

// mapChan streams src through fn until src closes or ctx ends (out closes on exit).
func mapChan[A, B any](ctx context.Context, src <-chan A, fn func(A) B) <-chan B {
	out := make(chan B, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
				if !ok {
					return
				}
				select {
				case out <- fn(v):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// send delivers one value on out, honoring ctx cancellation. Returns false if ctx
// ended before the send completed.
func send[T any](ctx context.Context, out chan<- T, v T) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}
