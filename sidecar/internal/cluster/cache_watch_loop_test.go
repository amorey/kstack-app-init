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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// A read that fails must schedule its OWN re-read. Recovery cannot depend on the next write
// ping: the object-write broker is resource-keyed, so a kind nobody writes to — Namespaces,
// an idle CRD — may not ping again for hours, and the subscription would sit showing an
// empty table that a client cannot tell apart from a genuinely empty kind.
func TestCacheWatchLoopRetriesAFailedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	ref := store.CacheRef{ClusterID: 1, CacheID: 1}
	_, err := mgr.Open(ctx, ref)
	require.NoError(t, err)

	var fires atomic.Int32
	succeeded := testutil.NewSignal()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cacheWatchLoop(ctx, mgr, ref.CacheID, time.Millisecond,
			func(*store.ClusterDB) (<-chan store.WriteWake, func()) {
				// A bus that never pings — the static-kind case.
				return make(chan store.WriteWake), func() {}
			},
			func(*store.ClusterDB) (bool, bool) {
				// Fail the first read, then succeed. No ping ever arrives, so only the
				// retry can produce the second call.
				if fires.Add(1) == 1 {
					return true, true
				}
				succeeded.Fire()
				return true, false
			},
			func() bool { return true },
		)
	}()

	succeeded.Wait(t, "a failed read to be retried")
	assert.GreaterOrEqual(t, fires.Load(), int32(2))

	cancel()
	testutil.Wait(t, done, "the loop to unwind")
}

// The retry must stop once a read succeeds, or a healthy watch would re-read on a timer
// forever — the debounce exists precisely so re-reads are driven by writes.
func TestCacheWatchLoopStopsRetryingOnceItSucceeds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	ref := store.CacheRef{ClusterID: 1, CacheID: 1}
	_, err := mgr.Open(ctx, ref)
	require.NoError(t, err)

	var fires atomic.Int32
	go cacheWatchLoop(ctx, mgr, ref.CacheID, time.Millisecond,
		func(*store.ClusterDB) (<-chan store.WriteWake, func()) { return make(chan store.WriteWake), func() {} },
		func(*store.ClusterDB) (bool, bool) {
			fires.Add(1)
			return true, false // always succeeds
		},
		func() bool { return true },
	)

	require.Eventually(t, func() bool { return fires.Load() >= 1 }, 2*time.Second, 10*time.Millisecond)
	settled := fires.Load()

	time.Sleep(cacheWatchRetryInterval + 500*time.Millisecond)
	assert.Equal(t, settled, fires.Load(), "a successful read must not leave a retry armed")
}
