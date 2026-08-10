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
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// cacheWatchRetryInterval paces re-reads after a failed one — needed because the
// write-ping that would otherwise drive recovery is resource-keyed, so a kind
// nobody writes to would never send another.
const cacheWatchRetryInterval = 2 * time.Second

// cacheWatchLoop is the shared half of every per-cache stream: it binds to the
// cache's db via WatchDB (binding when it opens, rebinding on delete+reopen) and
// coalesces write pings on a trailing-edge debounce. onFire runs once per bind and
// once per debounced burst; onClosed runs when the cache goes away; either
// returning false ends the stream.
// Both cadences are parameters, not constants, so a test picks its own timescale;
// production passes cacheWatchRetryInterval.
func cacheWatchLoop(
	ctx context.Context,
	mgr *store.Manager,
	cacheID int64,
	debounceDur time.Duration,
	retryDur time.Duration,
	subscribe func(*store.ClusterDB) (<-chan store.WriteWake, func()),
	onFire func(*store.ClusterDB) (keep bool, retry bool),
	onClosed func() bool,
) {
	handles, cancelHandles := mgr.WatchDB(cacheID)
	defer cancelHandles()

	// db/pings track the bound handle and its ping stream; nil while no cache is open.
	var (
		db        *store.ClusterDB
		pings     <-chan store.WriteWake
		cancelSub func()
	)
	defer func() {
		if cancelSub != nil {
			cancelSub()
		}
	}()

	// `armed` tracks a pending re-read; timer starts disarmed (Go timers deliver no
	// stale tick after Stop/Reset, so no manual drain).
	debounce := time.NewTimer(debounceDur)
	debounce.Stop()
	defer debounce.Stop()
	// A failed read schedules its OWN retry: the broker is resource-keyed, so a
	// static kind may not ping for hours and a transient error would otherwise show
	// an empty table indefinitely.
	retry := time.NewTimer(retryDur)
	retry.Stop()
	defer retry.Stop()
	armRetry := func() { retry.Reset(retryDur) }
	armed := false
	arm := func() {
		if !armed {
			debounce.Reset(debounceDur)
			armed = true
		}
	}
	disarm := func() {
		if armed {
			debounce.Stop()
			armed = false
		}
	}

	bind := func(next *store.ClusterDB) bool {
		disarm() // a fresh read happens below; drop any re-read pending for the old handle
		if cancelSub != nil {
			cancelSub()
			cancelSub = nil
			pings = nil
		}
		db = next
		if db == nil {
			return onClosed()
		}
		pings, cancelSub = subscribe(db)
		keep, again := onFire(db)
		if again {
			armRetry()
		}
		return keep
	}

	for {
		select {
		case <-ctx.Done():
			return
		case h, ok := <-handles:
			if !ok {
				return // store shutting down
			}
			if !bind(h) {
				return
			}
		case _, ok := <-pings:
			if !ok {
				// The bound db closed under us (e.g. Clear-cache): release the stale
				// sub and wait for WatchDB's new handle. Cancel, don't just drop —
				// a composite subscribe (catalogSubscribe) needs its goroutine stopped.
				disarm()
				if cancelSub != nil {
					cancelSub()
				}
				pings, cancelSub, db = nil, nil, nil
				continue
			}
			arm() // coalesce; the debounce fire runs the actual re-read
		case <-debounce.C:
			armed = false
			keep, again := onFire(db)
			if again {
				armRetry()
			}
			if !keep {
				return
			}
		case <-retry.C:
			// A read failed; drive the re-read from here until it succeeds.
			if db == nil {
				continue
			}
			keep, again := onFire(db)
			if again {
				armRetry()
			}
			if !keep {
				return
			}
		}
	}
}
