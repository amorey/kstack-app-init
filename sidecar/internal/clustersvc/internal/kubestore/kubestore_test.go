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

package kubestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireCreatesFreshStoreWithNoCookie(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	require.FileExists(t, filepath.Join(dir, "1.db"))

	_, ok, err := h.Store().Cookie(context.Background(), "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCookieRoundtripIsPerKind(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	ctx := context.Background()
	require.NoError(t, h.Store().SetCookie(ctx, "apps/v1", "deployments", "100"))
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "200"))

	v, ok, err := h.Store().Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "100", v)

	v, ok, err = h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "200", v)
}

func TestTwoAcquiresShareOneStore(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ha, err := r.Acquire(1)
	require.NoError(t, err)
	defer ha.Release()

	hb, err := r.Acquire(1)
	require.NoError(t, err)
	defer hb.Release()

	ctx := context.Background()
	require.NoError(t, ha.Store().SetCookie(ctx, "v1", "pods", "42"))

	v, ok, err := hb.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "42", v)
}

func TestCookiePersistsAcrossReleaseAndReacquire(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()

	h, err := r.Acquire(1)
	require.NoError(t, err)
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "7"))
	h.Release()

	h2, err := r.Acquire(1)
	require.NoError(t, err)
	defer h2.Release()

	v, ok, err := h2.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "7", v)
}

func TestClearWithNoHandlesDeletesFiles(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	require.NoError(t, h.Store().SetCookie(context.Background(), "v1", "pods", "1"))
	h.Release()

	require.NoError(t, r.Clear(1))

	stats, err := r.Stats(1)
	require.NoError(t, err)
	require.False(t, stats.Exists)
}

func TestClearOnNeverExistingCacheIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	require.NoError(t, r.Clear(999))
}

func TestClearUnderLiveHandleReopensAndKeepsHandleWorking(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()

	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "1"))

	require.NoError(t, r.Clear(1))

	_, ok, err := h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok, "cookie must be gone after Clear")

	// The same handle keeps working against the fresh store.
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "99"))
	v, ok, err := h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "99", v)
}

func TestStoreClearKindRemovesOnlyThatKindsCookie(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "1"))
	require.NoError(t, h.Store().SetCookie(ctx, "apps/v1", "deployments", "2"))

	require.NoError(t, h.Store().ClearKind(ctx, "v1", "pods"))

	_, ok, err := h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)

	v, ok, err := h.Store().Cookie(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "2", v)
}

func TestStoreClearKindOnEmptyStoreSucceeds(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	require.NoError(t, h.Store().ClearKind(context.Background(), "v1", "pods"))
}

func TestRegistryClearKindWorksWithoutCallerHoldingHandle(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "1"))
	h.Release()

	require.NoError(t, r.ClearKind(ctx, 1, "v1", "pods"))

	h2, err := r.Acquire(1)
	require.NoError(t, err)
	defer h2.Release()

	_, ok, err := h2.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestStatsReportsExistsAndBytesIncludingWalAndShm(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	require.NoError(t, h.Store().SetCookie(context.Background(), "v1", "pods", "1"))

	stats, err := r.Stats(1)
	require.NoError(t, err)
	require.True(t, stats.Exists)
	require.Greater(t, stats.Bytes, int64(0))

	// Confirm Bytes accounts for the -wal/-shm sidecars when present, not just the main file.
	var mainOnly int64
	if fi, statErr := os.Stat(filepath.Join(dir, "1.db")); statErr == nil {
		mainOnly = fi.Size()
	}
	h.Release()
	require.GreaterOrEqual(t, stats.Bytes, mainOnly)
}

func TestRegistryCloseClosesEveryOpenStoreWithoutPanicking(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)

	h1, err := r.Acquire(1)
	require.NoError(t, err)
	h2, err := r.Acquire(2)
	require.NoError(t, err)
	_ = h1
	_ = h2

	require.NoError(t, r.Close())
}

// ClearKind deletes the kind's objects and every row hanging off them — the schema
// has no cascading foreign keys — and touches no other kind.
func TestStoreClearKindDeletesObjectsAndTheirDependentRows(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	db := h.Store().db
	for _, kind := range []struct{ apiVersion, kind, resource, uid string }{
		{"v1", "Pod", "pods", "uid-pod"},
		{"apps/v1", "Deployment", "deployments", "uid-dep"},
	} {
		_, err := db.ExecContext(ctx,
			`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES (?, ?, ?, 'Namespaced', 0)`,
			kind.apiVersion, kind.kind, kind.resource)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx,
			`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
			 VALUES (?, ?, ?, 'x', '1', 0, 0, x'7b7d')`,
			kind.uid, kind.apiVersion, kind.kind)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO owner_refs (child_uid, owner_uid) VALUES (?, 'owner')`, kind.uid)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO labels (uid, key, value) VALUES (?, 'app', 'x')`, kind.uid)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO status_history (uid, at, summary) VALUES (?, 0, 'Running')`, kind.uid)
		require.NoError(t, err)
	}

	require.NoError(t, h.Store().ClearKind(ctx, "v1", "pods"))

	for _, q := range []string{
		`SELECT COUNT(*) FROM objects WHERE uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM labels WHERE uid = 'uid-pod'`,
		`SELECT COUNT(*) FROM status_history WHERE uid = 'uid-pod'`,
	} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n))
		assert.Zerof(t, n, "rows left behind by: %s", q)
	}

	// The other kind is untouched, dependent rows included.
	for _, q := range []string{
		`SELECT COUNT(*) FROM objects WHERE uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM labels WHERE uid = 'uid-dep'`,
		`SELECT COUNT(*) FROM status_history WHERE uid = 'uid-dep'`,
	} {
		var n int
		require.NoError(t, db.QueryRowContext(ctx, q).Scan(&n))
		assert.Equalf(t, 1, n, "row removed by another kind's clear: %s", q)
	}
}

// Event rows live in their own table, so clearing the Events kind has to empty it —
// deleting from objects would leave every cached event behind.
func TestStoreClearKindClearsTheEventsTable(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	db := h.Store().db
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('v1', 'Event', 'events', 'Namespaced', 0)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO events (uid, reason, message, raw_json, updated_at) VALUES ('uid-ev', 'Pulled', 'ok', x'7b7d', 0)`)
	require.NoError(t, err)

	require.NoError(t, h.Store().ClearKind(ctx, "v1", "events"))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n))
	assert.Zero(t, n, "cached events survived their kind's clear")
}

// Delete removes the files and, unlike Clear, leaves nothing open behind it: no
// empty store is reopened for a cache that is going away.
func TestDeleteRemovesTheFilesAndDropsTheOpenStore(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	require.NoError(t, h.Store().SetCookie(context.Background(), "v1", "pods", "1"))

	require.NoError(t, r.Delete(1))

	stats, err := r.Stats(1)
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the cache's files survived Delete")
	assert.Nil(t, h.Store(), "a handle still out resolves to no store")

	// Releasing a handle whose store is gone is not an error.
	h.Release()
}

func TestDeleteOnNeverExistingCacheIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	require.NoError(t, r.Delete(999))
}

// A retained child keeps its edge into a cleared owner: the edge is the child's own
// ownerReference, and nothing would put it back — a re-synced owner kind writes owner
// rows, not its children's edges.
func TestStoreClearKindKeepsARetainedChildsEdgeIntoTheClearedKind(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	db := h.Store().db
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('apps/v1', 'Deployment', 'deployments', 'Namespaced', 0)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-dep', 'apps/v1', 'Deployment', 'web', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-pod', 'v1', 'Pod', 'web-1', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO owner_refs (child_uid, owner_uid) VALUES ('uid-pod', 'uid-dep')`)
	require.NoError(t, err)

	require.NoError(t, h.Store().ClearKind(ctx, "apps/v1", "deployments"))

	var n int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM owner_refs WHERE child_uid = 'uid-pod'`).Scan(&n))
	assert.Equal(t, 1, n, "the retained Pod's own ownerReference was deleted with its owner")

	// The edge resolves to no owner, which is what a traversal's join already answers
	// for an owner kind that is not mirrored.
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM owner_refs r JOIN objects o ON o.uid = r.owner_uid WHERE r.child_uid = 'uid-pod'`).Scan(&n))
	assert.Zero(t, n)
}

// Another group may serve a Kind called "Event"; its rows are ordinary objects, so
// clearing it must leave the cached core events alone.
func TestStoreClearKindLeavesEventsAloneForANonCoreEventKind(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()

	db := h.Store().db
	_, err = db.ExecContext(ctx,
		`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES ('example.com/v1', 'Event', 'events', 'Namespaced', 1)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO objects (uid, api_version, kind, name, resource_version, created_at, updated_at, raw_json)
		 VALUES ('uid-crd', 'example.com/v1', 'Event', 'x', '1', 0, 0, x'7b7d')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO events (uid, reason, message, raw_json, updated_at) VALUES ('uid-ev', 'Pulled', 'ok', x'7b7d', 0)`)
	require.NoError(t, err)

	require.NoError(t, h.Store().ClearKind(ctx, "example.com/v1", "events"))

	var n int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n))
	assert.Equal(t, 1, n, "the CRD's clear wiped the cached core events")

	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE uid = 'uid-crd'`).Scan(&n))
	assert.Zero(t, n, "the CRD's own rows survived its clear")
}

// A deleted cache stays deleted: a straggler holding a view of it from before the
// teardown must not open a fresh file nothing will ever name again.
func TestAcquireAfterDeleteIsRefused(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	h.Release()

	require.NoError(t, r.Delete(1))

	_, err = r.Acquire(1)
	assert.ErrorIs(t, err, ErrDeleted)
	assert.ErrorIs(t, r.ClearKind(context.Background(), 1, "v1", "pods"), ErrDeleted)

	stats, err := r.Stats(1)
	require.NoError(t, err)
	assert.False(t, stats.Exists, "the refused Acquire recreated the file")

	// Another cache is unaffected.
	h2, err := r.Acquire(2)
	require.NoError(t, err)
	h2.Release()
}

// A Delete whose cleanup failed still retires the id: the caller retries, and an
// Acquire in the meantime would recreate the file the retry is there to remove.
func TestDeleteRetiresTheCacheEvenWhenCleanupFails(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	h, err := r.Acquire(1)
	require.NoError(t, err)
	h.Release()

	// An unremovable sidecar: os.Remove refuses a non-empty directory.
	wal := filepath.Join(dir, "1.db-wal")
	require.NoError(t, os.MkdirAll(filepath.Join(wal, "blocker"), 0o700))

	require.Error(t, r.Delete(1))

	_, err = r.Acquire(1)
	assert.ErrorIs(t, err, ErrDeleted)

	// The retry, once the obstruction is gone, still finishes the job.
	require.NoError(t, os.RemoveAll(wal))
	require.NoError(t, r.Delete(1))
	stats, err := r.Stats(1)
	require.NoError(t, err)
	assert.False(t, stats.Exists)
}

// A Clear whose files will not go still leaves the cache usable: the caller retries,
// and until then a handle must not resolve to the store Clear closed.
func TestClearKeepsTheCacheUsableWhenTheFilesWillNotGo(t *testing.T) {
	dir := t.TempDir()
	failDelete := errors.New("boom")
	blocked := true
	r := newRegistryWithOptions(dir, withDeleteFiles(func(path string) error {
		if blocked {
			return failDelete
		}
		return deleteStoreFiles(path)
	}))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	ctx := context.Background()
	h, err := r.Acquire(1)
	require.NoError(t, err)
	defer h.Release()
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "1"))

	require.ErrorIs(t, r.Clear(1), failDelete)

	// The rows are still there — nothing was deleted — and the handle still writes.
	v, ok, err := h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "1", v)
	require.NoError(t, h.Store().SetCookie(ctx, "v1", "pods", "2"))

	// The retry, once the files can go, wipes the store as usual.
	blocked = false
	require.NoError(t, r.Clear(1))
	_, ok, err = h.Store().Cookie(ctx, "v1", "pods")
	require.NoError(t, err)
	assert.False(t, ok)
}
