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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// Resource routing eliminates the wasted re-read, not just the wasted frame: with the
// objects watch subscribed keyed to its (apiVersion, resource), an unrelated resource's
// keyed write never wakes it, so its snapshot read never runs — proven with a counting
// snapshot fn driving cacheDeltaWatch exactly as ClusterDataObjectsWatch does. A
// matching-resource write does wake it and re-reads.

// Resource routing eliminates the wasted re-read, not just the wasted frame: with the
// objects watch subscribed keyed to its (apiVersion, resource), an unrelated resource's
// keyed write never wakes it, so its snapshot read never runs — proven with a counting
// snapshot fn driving cacheDeltaWatch exactly as ClusterDataObjectsWatch does. A
// matching-resource write does wake it and re-reads.
func TestClusterDataObjectsWatchNoReReadForOtherKind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	const cacheID = int64(7)
	cdb, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: cacheID})
	require.NoError(t, err)

	const debounce = 20 * time.Millisecond
	reads := testutil.NewProbe[struct{}](8)
	ch := cacheDeltaWatch(ctx, mgr, cacheID, debounce,
		// The keyed subscribe ClusterDataObjectsWatch uses, verbatim.
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) ([]string, error) {
			reads.Fire(struct{}{})
			return nil, nil
		},
		func(s string) string { return s },
		func(_ domain.FrameType, s *string) string {
			if s == nil {
				return "" // the Bookmark; this test only counts reads
			}
			return *s
		},
	)
	go func() { // drain so sends never block the watch goroutine
		for range ch { //nolint:revive
		}
	}()

	// Off the probe, not a sampled count that could already include a re-read.
	reads.Await(t, "the baseline snapshot read on bind")

	// An unrelated resource's keyed write must not wake the watch. A negative, so it needs a
	// window: several debounces, tripping the instant a read lands.
	cdb.ObjectsNotifyResource("v1", "pods")
	select {
	case <-reads.Chan():
		t.Fatal("an unrelated resource's write must not re-read")
	case <-time.After(5 * debounce):
	}

	// A matching-resource write wakes it — one more read after the debounce fires.
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	reads.Await(t, "a matching-resource write to re-read")
}

// The Bookmark closing the initial state is sent once per stream, not once per read:
// a client that reset on each one would treat every later re-read as a fresh
// snapshot. Driven through a snapshot fn whose contents change, so the second read
// emits a real delta and there is something for a stray Bookmark to follow.
func TestCacheDeltaWatchBookmarksOnceAcrossReReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	const cacheID = int64(11)
	cdb, err := mgr.Open(ctx, store.CacheRef{ClusterID: 1, CacheID: cacheID})
	require.NoError(t, err)

	const debounce = 20 * time.Millisecond
	items := []string{"a"}
	ch := cacheDeltaWatch(ctx, mgr, cacheID, debounce,
		func(db *store.ClusterDB) (<-chan store.WriteWake, func()) {
			return db.ObjectsSubscribeResource("apps/v1", "deployments")
		},
		func(context.Context, *store.ClusterDB) ([]string, error) { return items, nil },
		func(s string) string { return s },
		func(t domain.FrameType, s *string) change {
			if s == nil {
				return change{typ: t}
			}
			return change{typ: t, key: *s}
		},
	)

	// Initial state: the one item, then the Bookmark closing it.
	require.Equal(t, change{typ: domain.FrameAdded, key: "a"}, testutil.Recv(t, ch, "the snapshot item"))
	require.Equal(t, change{typ: domain.FrameBookmark}, testutil.Recv(t, ch, "the Bookmark"))

	// A second read delivers a plain Added — no second Bookmark ahead of it.
	items = []string{"a", "b"}
	cdb.ObjectsNotifyResource("apps/v1", "deployments")
	require.Equal(t, change{typ: domain.FrameAdded, key: "b"}, testutil.Recv(t, ch, "the live change"))
}

// change is a minimal stand-in for the ClusterData*Change wrappers: the type plus the
// entity's key, or an empty key for the entity-less Bookmark.
type change struct {
	typ domain.FrameType
	key string
}
