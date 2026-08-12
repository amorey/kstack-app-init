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

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// cacheDeltaWatch is the shared machinery behind the three ClusterData*Watch
// streams: it follows one cache's db across its lifecycle (WatchDB bind/rebind),
// coalesces write pings on a trailing-edge debounce, and on each fire re-reads a
// keyed snapshot and diffs it against the last — Added/Modified/Deleted per key,
// and Deleted for every held key when the cache closes. `snapshot` must return an
// ordered slice (its order is the Added-burst order); T must be comparable so a
// changed value is detected by ==. Closes when ctx ends or the store shuts down.
//
// mkChange is called with a nil entity for the single Bookmark closing the initial
// state, and must return a change carrying none.
func cacheDeltaWatch[T comparable, C any](
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	snapshot func(context.Context, *store.ClusterDB) ([]T, error),
	keyOf func(T) string,
	mkChange func(domain.ChangeType, *T) C,
) <-chan C {
	out := make(chan C, 1)
	prev := map[string]T{}

	// The initial state is complete after the first successful read — or after the
	// first bind that found no open cache, whose initial state is legitimately empty
	// (a never-synced or paused cache would otherwise spin forever). A failed read is
	// not initial state, so it waits for the retry.
	bookmarked := false
	markBookmarked := func() bool {
		if bookmarked {
			return true
		}
		bookmarked = true
		return send(ctx, out, mkChange(domain.ChangeBookmark, nil))
	}

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
				if !send(ctx, out, mkChange(domain.ChangeAdded, &v)) {
					return false, false
				}
			case old != v:
				if !send(ctx, out, mkChange(domain.ChangeModified, &v)) {
					return false, false
				}
			}
		}
		for key, v := range prev {
			if _, ok := next[key]; !ok {
				if !send(ctx, out, mkChange(domain.ChangeDeleted, &v)) {
					return false, false
				}
			}
		}
		prev = next
		return markBookmarked(), false
	}

	// emitEmpty sends one Deleted per held value and clears prev — run when the
	// cache closes so a never-reopened cache leaves no stale rows.
	emitEmpty := func() bool {
		for _, v := range prev {
			if !send(ctx, out, mkChange(domain.ChangeDeleted, &v)) {
				return false
			}
		}
		prev = map[string]T{}
		return markBookmarked()
	}

	go func() {
		defer close(out)
		cacheWatchLoop(ctx, mgr, cacheID, debounceDur, cacheWatchRetryInterval, subscribe, emit, emitEmpty)
	}()
	return out
}
