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

// Shared white-box test helpers for the cluster package: kubeconfig sources, a
// memory-backed beehive with the cluster kinds registered, and a no-op controller.
// In a _test.go file (package cluster) so the testing/testify/sqlite deps stay out
// of the production build while every other _test.go in the package can use them.
package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/beehive/sqlite"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// recv blocks for the next value on a stream channel. Shared by the per-object
// stream tests (schedule/event/probe watches) and the delta watches, which differ
// only in element type.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	return testutil.Recv(t, ch, "a stream value")
}

// recvRun receives the next event frame and asserts it carries a run.
func recvRun(t *testing.T, ch <-chan domain.EventWatchFrame) domain.Event {
	t.Helper()
	f := recv(t, ch)
	require.Equal(t, domain.EventFrameRun, f.Type)
	require.NotNil(t, f.Event)
	return *f.Event
}

// recvEventBookmark receives the next event frame and asserts it is the bookmark
// closing the snapshot.
func recvEventBookmark(t *testing.T, ch <-chan domain.EventWatchFrame) {
	t.Helper()
	f := recv(t, ch)
	require.Equal(t, domain.EventFrameBookmark, f.Type)
	require.Nil(t, f.Event)
}

// requireBookmark asserts a change is the one that closes a delta watch's snapshot:
// every such watch sends exactly one, after the last snapshot object and before the
// first live change. `entity` is the change's entity field — the caller reads it,
// since its name differs per kind — and must be nil.
func requireBookmark(t *testing.T, typ domain.DeltaFrameType, entity any) {
	t.Helper()
	require.Equal(t, domain.DeltaFrameBookmark, typ)
	require.Nil(t, entity)
}

// recvBy is recv with a caller-supplied deadline, so a drain loop can share one
// deadline across iterations instead of resetting it each receive.
func recvBy[T any](t *testing.T, ch <-chan T, deadline <-chan time.Time) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("stream closed unexpectedly")
		}
		return v
	case <-deadline:
		t.Fatal("timed out waiting for a stream value")
		var zero T
		return zero
	}
}

// noopController satisfies beehive.Controller without reconciling. A test gets a
// ControllerClient to write status directly (the plain Client cannot) from the
// value beehive.Register returns for the kind.
type noopController[Spec, Status any] struct{}

func (c *noopController[Spec, Status]) Reconcile(context.Context, beehive.ControllerClient[Status], *beehive.Object[Spec, Status]) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// fakeCoreController satisfies the coreController seam so the service test can assert
// the out-of-band dispatch (RetryConnection → Reprobe) without a network-touching
// ClusterCoreController. StartBackground/StopBackground are no-ops; Reprobe records the
// ids it was handed.

// fakeCoreController satisfies the coreController seam so the service test can assert
// the out-of-band dispatch (RetryConnection → Reprobe) without a network-touching
// ClusterCoreController. StartBackground/StopBackground are no-ops; Reprobe records the
// ids it was handed.
type fakeCoreController struct{ reprobed []domain.ClusterID }

func (f *fakeCoreController) StartBackground() {}

func (f *fakeCoreController) StopBackground() {}

func (f *fakeCoreController) Reprobe(id domain.ClusterID) { f.reprobed = append(f.reprobed, id) }

// WatchProbe returns a closed channel: this fake never probes, so the interface
// is satisfied without a live in-flight signal.

// WatchProbe returns a closed channel: this fake never probes, so the interface
// is satisfied without a live in-flight signal.
func (f *fakeCoreController) WatchProbe(context.Context, domain.ClusterID) <-chan bool {
	ch := make(chan bool)
	close(ch)
	return ch
}

// newServiceTest builds a started beehive with no-op controllers and returns a
// service wired to its clients plus a temp cache manager. The returned
// ControllerClients write Cluster status (core) and ClusterCache status — the
// controller-owned surfaces a white-box test stamps directly.

// newServiceTest builds a started beehive with no-op controllers and returns a
// service wired to its clients plus a temp cache manager. The returned
// ControllerClients write Cluster status (core) and ClusterCache status — the
// controller-owned surfaces a white-box test stamps directly.
func newServiceTest(t *testing.T) (*Service, beehive.ControllerClient[domain.ClusterStatus], beehive.ControllerClient[domain.ClusterCacheStatus]) {
	t.Helper()
	s, coreCC, cacheCC, _, _ := newServiceTestSync(t)
	return s, coreCC, cacheCC
}

// newServiceTestSync is newServiceTest plus the sync children's controller client, for
// the tests that write a child's status (the granular sync surface).

// newServiceTestSync is newServiceTest plus the sync children's controller client, for
// the tests that write a child's status (the granular sync surface).
func newServiceTestSync(t *testing.T) (
	*Service,
	beehive.ControllerClient[domain.ClusterStatus],
	beehive.ControllerClient[domain.ClusterCacheStatus],
	beehive.ControllerClient[domain.ClusterCacheGVRSyncStatus],
	beehive.ControllerClient[domain.ClusterCacheGVRDiscoveryStatus],
) {
	t.Helper()
	st, err := sqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bh, err := beehive.New(st)
	require.NoError(t, err)

	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	cacheClient := beehive.NewClient[domain.ClusterCacheSpec, domain.ClusterCacheStatus](bh, domain.ClusterCacheGroupKind)
	gvrDiscoveryClient := beehive.NewClient[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus](bh, domain.ClusterCacheGVRDiscoveryGroupKind)
	gvrSyncClient := beehive.NewClient[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus](bh, domain.ClusterCacheGVRSyncGroupKind)

	coreCC, err := beehive.Register(bh, domain.ClusterGroupKind, &noopController[domain.ClusterSpec, domain.ClusterStatus]{})
	require.NoError(t, err)
	cacheCC, err := beehive.Register(bh, domain.ClusterCacheGroupKind, &noopController[domain.ClusterCacheSpec, domain.ClusterCacheStatus]{})
	require.NoError(t, err)
	gvrDiscoveryCC, err := beehive.Register(bh, domain.ClusterCacheGVRDiscoveryGroupKind, &noopController[domain.ClusterCacheGVRDiscoverySpec, domain.ClusterCacheGVRDiscoveryStatus]{})
	require.NoError(t, err)
	gvrSyncCC, err := beehive.Register(bh, domain.ClusterCacheGVRSyncGroupKind, &noopController[domain.ClusterCacheGVRSyncSpec, domain.ClusterCacheGVRSyncStatus]{})
	require.NoError(t, err)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	// Shut the cache manager down before the test ends so its SQLite pools close and
	// TempDir cleanup can remove the .db files — on Windows an open file can't be
	// unlinked, so a leaked pool fails the TempDir RemoveAll.
	cacheManager := store.NewManager(t.TempDir())
	t.Cleanup(func() { _ = cacheManager.Shutdown(context.Background()) })

	return &Service{
		coreClient:         coreClient,
		cacheClient:        cacheClient,
		gvrDiscoveryClient: gvrDiscoveryClient,
		gvrSyncClient:      gvrSyncClient,
		cacheManager:       cacheManager,
		connMgr:            connections.NewManager(),
		coreCtrl:           &fakeCoreController{},
		// A short non-zero debounce keeps the watch tests fast while still exercising
		// the coalescing path (the Coalesces test overrides it to a wider window).
		dataKindsDebounce: 5 * time.Millisecond,
	}, coreCC, cacheCC, gvrSyncCC, gvrDiscoveryCC
}

// seedCluster creates a Cluster (as the importer would, with the kubeconfig
// name) and returns its ClusterID — the beehive ObjectID beehive assigned.

// seedCluster creates a Cluster (as the importer would, with the kubeconfig
// name) and returns its ClusterID — the beehive ObjectID beehive assigned.
func seedCluster(t *testing.T, s *Service, ctxName string) domain.ClusterID {
	t.Helper()
	ctx := context.Background()
	name := ctxName
	obj, err := s.coreClient.Create(ctx, domain.KubeconfigName(ctxName), domain.ClusterSpec{
		Name:        &name,
		SyncEnabled: true,
		Enabled:     true,
		Source:      domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: ctxName}},
	})
	require.NoError(t, err)
	return domain.ClusterID(obj.ID)
}

// stampActiveUID records uid as a cluster's last-probed kube-system identity by
// writing it to Status.Server.UID (as the ClusterCoreController would after a
// probe). A ClusterCache for the same uid then resolves as the cluster's active
// cache.

// stampActiveUID records uid as a cluster's last-probed kube-system identity by
// writing it to Status.Server.UID (as the ClusterCoreController would after a
// probe). A ClusterCache for the same uid then resolves as the cluster's active
// cache.
func stampActiveUID(t *testing.T, s *Service, coreCC beehive.ControllerClient[domain.ClusterStatus], id domain.ClusterID, uid string) {
	t.Helper()
	ctx := context.Background()
	obj, err := s.coreClient.Get(ctx, beehive.ObjectID(id))
	require.NoError(t, err)
	require.NoError(t, coreCC.UpdateStatus(ctx, obj.ID, obj.Generation, domain.ClusterStatus{
		Server: domain.ClusterServer{UID: &uid},
	}))
}

// seedActiveCache creates an active ClusterCache for a cluster: it stamps the
// cluster's connected UID and creates a ClusterCache (owned, UID-keyed name) for
// that identity. Returns the cache's ObjectID.

// seedActiveCache creates an active ClusterCache for a cluster: it stamps the
// cluster's connected UID and creates a ClusterCache (owned, UID-keyed name) for
// that identity. Returns the cache's ObjectID.
func seedActiveCache(t *testing.T, s *Service, coreCC beehive.ControllerClient[domain.ClusterStatus], id domain.ClusterID, uid string) beehive.ObjectID {
	t.Helper()
	ctx := context.Background()
	stampActiveUID(t, s, coreCC, id, uid)
	cacheObj, err := s.cacheClient.Create(ctx, domain.ClusterCacheName(id, uid), domain.ClusterCacheSpec{ServerUID: uid},
		beehive.WithOwner(beehive.ObjectID(id)))
	require.NoError(t, err)
	return cacheObj.ID
}

// insertCatalogKind inserts one kind_catalog row into a cache db (test helper for
// the ClusterDataKindsWatch lifecycle tests).
func insertCatalogKind(t *testing.T, ctx context.Context, cdb *store.ClusterDB, apiVersion, kind, resource, scope string) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES(?, ?, ?, ?, 0, NULL)`, apiVersion, kind, resource, scope)
	require.NoError(t, err)
}

// A subscriber that opens the stream before the cache db is opened (the common
// unsynced-cluster case) must bind to the cache when it opens and start streaming its
// catalog — not miss it forever by binding once to the initial (nil) Lookup.

// insertEvent writes one row directly into the events table (uid + the display columns),
// standing in for what the sync engine's event driver would persist.
func insertEvent(t *testing.T, ctx context.Context, cdb *store.ClusterDB, uid, evType, reason, message string, count, lastSeen int64) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO events (uid, involved_kind, involved_ns, involved_name,
		   type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
		 VALUES (?, 'Pod', 'default', 'my-pod', ?, ?, ?, ?, ?, ?, x'7b7d', ?)
		 ON CONFLICT(uid) DO UPDATE SET
		   type=excluded.type, reason=excluded.reason, message=excluded.message,
		   last_seen=excluded.last_seen, count=excluded.count, updated_at=excluded.updated_at`,
		uid, evType, reason, message, lastSeen, lastSeen, count, lastSeen)
	require.NoError(t, err)
}

// recvObjectChange reads one delta off a ClusterDataObjectsWatch stream, failing on
// close or timeout.

// insertObject writes one row directly into the objects table (the universal-identity
// columns the objects watch projects), standing in for what the sync engine's object
// driver would persist. createdAt is the object's creationTimestamp as unix-millis.
func insertObject(t *testing.T, ctx context.Context, cdb *store.ClusterDB, uid, apiVersion, kind, namespace, name, rv string, createdAt int64) {
	t.Helper()
	// The body carries the resourceVersion (as it does in a real object), so a bare rv
	// bump rewrites raw_json — which is what makes an in-place edit surface as Modified.
	// raw_json is updated on conflict too, so a re-insert with a new rv changes the body.
	body, err := store.CompressRaw([]byte(fmt.Sprintf(
		`{"metadata":{"namespace":%q,"name":%q,"resourceVersion":%q}}`, namespace, name, rv)))
	require.NoError(t, err)
	_, err = cdb.Writer().ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, namespace, name, resource_version,
		   created_at, updated_at, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(uid) DO UPDATE SET
		   namespace=excluded.namespace, name=excluded.name,
		   resource_version=excluded.resource_version, updated_at=excluded.updated_at,
		   raw_json=excluded.raw_json`,
		uid, apiVersion, kind, namespace, name, rv, createdAt, createdAt, body)
	require.NoError(t, err)
}

// emptyRawJSON is a zlib-compressed "{}" for object seeds read only via CacheStats/Kinds
// (which never decompress): store.Objects decompresses raw_json, so a seed on that read
// path must be compressed like the engine write path. (Event rows aren't decompressed by
// store.Events, so those seeds stay raw.)

// emptyRawJSON is a zlib-compressed "{}" for object seeds read only via CacheStats/Kinds
// (which never decompress): store.Objects decompresses raw_json, so a seed on that read
// path must be compressed like the engine write path. (Event rows aren't decompressed by
// store.Events, so those seeds stay raw.)
func emptyRawJSON(t *testing.T) []byte {
	t.Helper()
	b, err := store.CompressRaw([]byte(`{}`))
	require.NoError(t, err)
	return b
}

// insertObjectCatalog writes one kind_catalog row so the objects reader can translate
// the watch's plural resource to its kind.

// insertObjectCatalog writes one kind_catalog row so the objects reader can translate
// the watch's plural resource to its kind.
func insertObjectCatalog(t *testing.T, ctx context.Context, cdb *store.ClusterDB, apiVersion, kind, resource, scope string) {
	t.Helper()
	_, err := cdb.Writer().ExecContext(ctx,
		`INSERT INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
		 VALUES(?, ?, ?, ?, 0, NULL)`,
		apiVersion, kind, resource, scope)
	require.NoError(t, err)
}

// ClusterDataEventsWatch streams the active cache's cached Kubernetes Events as a delta
// watch: the newest window as an Added burst, then Added/Modified/Deleted as the sync
// engine writes events and pings the events-only store broker — what backs the dashboard
// events table.

func seedGVRDiscovery(t *testing.T, s *Service, cacheID beehive.ObjectID) beehive.ObjectID {
	t.Helper()
	obj, err := s.gvrDiscoveryClient.Create(context.Background(),
		domain.ClusterCacheGVRDiscoveryName(cacheID),
		domain.ClusterCacheGVRDiscoverySpec{Enabled: true},
		beehive.WithOwner(cacheID))
	require.NoError(t, err)
	return obj.ID
}

// seedGVRSync creates one per-kind sync child the discovery controller would.

// seedGVRSync creates one per-kind sync child the discovery controller would.
func seedGVRSync(t *testing.T, s *Service, discoveryID beehive.ObjectID, apiVersion, resource string) beehive.ObjectID {
	t.Helper()
	obj, err := s.gvrSyncClient.Create(context.Background(),
		domain.ClusterCacheGVRSyncName(discoveryID, apiVersion, resource),
		domain.ClusterCacheGVRSyncSpec{Enabled: true, APIVersion: apiVersion, Resource: resource, Kind: "Widget"},
		beehive.WithOwner(discoveryID))
	require.NoError(t, err)
	return obj.ID
}

// TestServiceWatchCacheSyncHealthSharesOneFold pins that the fold is process-wide, not
// per-subscriber. Every window computes the same verdict from the same two watches, so a
// second subscriber must attach to the running fold rather than start its own — otherwise
// each open window costs two more beehive watches, another ticker, another copy of every
// per-kind record, and another acquisition of the sync controller's writeMu per flush.

// awaitSyncHealth drains until the named cache reports want.
func awaitSyncHealth(t *testing.T, ch <-chan domain.ClusterCacheSyncHealth, cacheID domain.ClusterCacheID, want string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		h := recvBy(t, ch, deadline)
		if h.CacheID == cacheID && h.Reason == want {
			return
		}
	}
}

// TestServiceWatchCacheSyncHealthClosesOnShutdown pins the teardown. The fold outlives
// every subscriber, so nothing a subscriber does can end it — only the service can, and
// when it does every open stream has to close rather than hang on a hub nobody will
// publish to again.
