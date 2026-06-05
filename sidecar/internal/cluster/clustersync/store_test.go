package clustersync

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clustercache"
)

// migratedCDB opens a fresh, migrated per-cluster cache DB. The cache is shut
// down on cleanup.
func migratedCDB(t *testing.T) *clustercache.ClusterDB {
	t.Helper()
	ctx := context.Background()
	cache := clustercache.NewManager(t.TempDir(), nil, nil)
	cdb, err := cache.Open(ctx, "c1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Shutdown(ctx) })
	return cdb
}

// migratedWriter opens a fresh, migrated per-cluster cache DB and returns its
// writer pool. The cache is shut down on cleanup.
func migratedWriter(t *testing.T) *sql.DB {
	t.Helper()
	return migratedCDB(t).Writer()
}

func insertObject(t *testing.T, w *sql.DB, uid, apiVersion, kind string) {
	t.Helper()
	_, err := w.Exec(
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES (?, ?, ?, ?, '1', 0, 0, '{}')`,
		uid, apiVersion, kind, uid)
	require.NoError(t, err)
}

func countObjectsByKind(t *testing.T, w *sql.DB, kind, apiVersion string) int {
	t.Helper()
	var n int
	require.NoError(t, w.QueryRow(
		`SELECT COUNT(*) FROM objects WHERE kind=? AND api_version=?`, kind, apiVersion).Scan(&n))
	return n
}

func countWhere(t *testing.T, w *sql.DB, table, col, val string) int {
	t.Helper()
	var n int
	require.NoError(t, w.QueryRow(
		`SELECT COUNT(*) FROM `+table+` WHERE `+col+`=?`, val).Scan(&n))
	return n
}

// raw_json is stored zlib-compressed, not as plaintext JSON: the stored cell
// must be a zlib stream (first byte 0x78) that decompresses back to the exact
// object JSON — never the readable plaintext. Both write paths (objects and
// events) must hold this property.
func TestStoresCompressedRawJSON(t *testing.T) {
	cases := []struct {
		name  string
		table string
		uid   string
		obj   map[string]any
		write func(ctx context.Context, cdb *clustercache.ClusterDB, u *unstructured.Unstructured) error
	}{
		{
			name: "objects", table: "objects", uid: "pod-uid",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata":   map[string]any{"uid": "pod-uid", "name": "nginx", "namespace": "default"},
				"spec":       map[string]any{"nodeName": "node-1"},
			},
			write: func(ctx context.Context, cdb *clustercache.ClusterDB, u *unstructured.Unstructured) error {
				return newObjectsStore(ctx, "c1", schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, cdb.Writer(), cdb).upsert(u)
			},
		},
		{
			name: "events", table: "events", uid: "evt-uid",
			obj: map[string]any{
				"apiVersion": "v1",
				"kind":       "Event",
				"metadata":   map[string]any{"uid": "evt-uid", "name": "e1", "namespace": "default"},
				"type":       "Warning",
				"reason":     "BackOff",
				"message":    "Back-off restarting failed container",
			},
			write: func(ctx context.Context, cdb *clustercache.ClusterDB, u *unstructured.Unstructured) error {
				return newEventsStore(ctx, "c1", schema.GroupVersionKind{Version: "v1", Kind: "Event"}, cdb.Writer(), cdb).upsert(u)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cdb := migratedCDB(t)
			u := &unstructured.Unstructured{Object: tc.obj}
			require.NoError(t, tc.write(ctx, cdb, u))
			wantJSON, err := json.Marshal(u.Object)
			require.NoError(t, err)

			var cell []byte
			require.NoError(t, cdb.Writer().QueryRowContext(ctx,
				`SELECT raw_json FROM `+tc.table+` WHERE uid=?`, tc.uid).Scan(&cell))

			require.NotEqual(t, wantJSON, cell, "raw_json must not be stored as plaintext")
			require.Equal(t, byte(0x78), cell[0], "expected a zlib stream")
			got, err := clustercache.DecompressRaw(cell)
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(got))
		})
	}
}

// An uninstalled CRD's custom resources must be evicted from objects: no
// reflector runs for a kind that vanished from discovery, so the per-kind
// Replace prune never fires for it. pruneOrphanedKinds is the only thing that
// reaps them, and it must drop the cascade rows (labels, owner_refs,
// status_history) too — exactly like a per-object delete.
func TestPruneOrphanedKindsEvictsVanishedKinds(t *testing.T) {
	ctx := context.Background()
	w := migratedWriter(t)

	// Survivors: a built-in Pod and a Deployment.
	insertObject(t, w, "pod-uid", "v1", "Pod")
	insertObject(t, w, "dep-uid", "apps/v1", "Deployment")
	// Orphans: two instances of a CRD kind that discovery no longer returns.
	insertObject(t, w, "widget-1", "example.com/v1", "Widget")
	insertObject(t, w, "widget-2", "example.com/v1", "Widget")

	// Cascade rows: prove they're cleaned for the orphan and kept for survivors.
	_, err := w.Exec(`INSERT INTO labels(uid, key, value) VALUES('widget-1','app','w'),('pod-uid','app','p')`)
	require.NoError(t, err)
	_, err = w.Exec(`INSERT INTO owner_refs(child_uid, owner_uid) VALUES('widget-1','some-owner'),('child-x','widget-2')`)
	require.NoError(t, err)
	_, err = w.Exec(`INSERT INTO status_history(uid, at, summary) VALUES('widget-1', 1, 'Ready')`)
	require.NoError(t, err)

	// Discovery now returns only the built-ins (the Widget CRD was uninstalled).
	keep := map[kindKey]struct{}{
		{kind: "Pod", apiVersion: "v1"}:             {},
		{kind: "Deployment", apiVersion: "apps/v1"}: {},
	}
	n, err := pruneOrphanedKinds(ctx, w, keep)
	require.NoError(t, err)
	require.Equal(t, int64(2), n, "both Widget rows pruned")

	// Orphaned kind gone; survivors untouched.
	require.Equal(t, 0, countObjectsByKind(t, w, "Widget", "example.com/v1"))
	require.Equal(t, 1, countObjectsByKind(t, w, "Pod", "v1"))
	require.Equal(t, 1, countObjectsByKind(t, w, "Deployment", "apps/v1"))

	// Cascade rows for the orphan are gone — including the owner edge where the
	// orphan is the owner, not the child.
	require.Equal(t, 0, countWhere(t, w, "labels", "uid", "widget-1"))
	require.Equal(t, 0, countWhere(t, w, "owner_refs", "child_uid", "widget-1"))
	require.Equal(t, 0, countWhere(t, w, "owner_refs", "owner_uid", "widget-2"))
	require.Equal(t, 0, countWhere(t, w, "status_history", "uid", "widget-1"))
	// Survivor cascade rows remain.
	require.Equal(t, 1, countWhere(t, w, "labels", "uid", "pod-uid"))
}

// When every stored kind is still present in discovery, prune is a no-op — it
// must never touch live data (and must not fire on a partial discovery, which
// the caller guards by only invoking it after a complete discovery).
func TestPruneOrphanedKindsKeepsEverythingWhenNoneVanished(t *testing.T) {
	ctx := context.Background()
	w := migratedWriter(t)

	insertObject(t, w, "pod-uid", "v1", "Pod")
	insertObject(t, w, "widget-1", "example.com/v1", "Widget")

	keep := map[kindKey]struct{}{
		{kind: "Pod", apiVersion: "v1"}:                {},
		{kind: "Widget", apiVersion: "example.com/v1"}: {},
	}
	n, err := pruneOrphanedKinds(ctx, w, keep)
	require.NoError(t, err)
	require.Equal(t, int64(0), n, "nothing pruned")
	require.Equal(t, 1, countObjectsByKind(t, w, "Pod", "v1"))
	require.Equal(t, 1, countObjectsByKind(t, w, "Widget", "example.com/v1"))
}
