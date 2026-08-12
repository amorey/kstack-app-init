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
)

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
