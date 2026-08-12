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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// The dedupe is what makes a per-write ping affordable: the gauge is re-read on every
// debounced burst, but an unchanged reading must not reach the subscriber. Without it
// a busy cache would push an identical measurement to every window on every write.
func TestCacheGaugeWatchEmitsOnlyOnChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	const cacheID = int64(3)
	cdb, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: cacheID})
	require.NoError(t, err)

	const debounce = 20 * time.Millisecond
	var value atomic.Int64
	value.Store(1)
	reads := testutil.NewProbe[struct{}](8)

	ch := cacheGaugeWatch(ctx, mgr, cacheID, debounce,
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) (int64, error) {
			reads.Fire(struct{}{})
			return value.Load(), nil
		},
		func() int64 { return 0 }, // the reading a closed cache reports
	)

	require.EqualValues(t, 1, testutil.Recv(t, ch, "the reading on bind"))

	// A write whose reading is unchanged: the re-read happens, the frame does not. A
	// negative assertion, so it needs a bounded window — sized off the debounce, and
	// tripping the instant a frame lands.
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	reads.Await(t, "the re-read the ping drives")
	select {
	case v := <-ch:
		t.Fatalf("an unchanged reading must not be re-emitted, got %v", v)
	case <-time.After(5 * debounce):
	}

	// A changed reading does reach the subscriber.
	value.Store(2)
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	require.EqualValues(t, 2, testutil.Recv(t, ch, "the changed reading"))
}

// A cache that closes reports the `closed` value rather than freezing at its last
// reading — a dashboard must not keep showing the contents of a cache that is gone.
func TestCacheGaugeWatchReportsTheClosedReading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	const cacheID = int64(4)
	_, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: cacheID})
	require.NoError(t, err)

	ch := cacheGaugeWatch(ctx, mgr, cacheID, 20*time.Millisecond,
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) (int64, error) { return 7, nil },
		func() int64 { return -1 },
	)
	require.EqualValues(t, 7, testutil.Recv(t, ch, "the reading on bind"))

	require.NoError(t, mgr.Close(ctx, cacheID))
	require.EqualValues(t, -1, testutil.Recv(t, ch, "the closed-cache reading"))
}
