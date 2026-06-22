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

// Package clustercache owns the sidecar's per-cluster on-disk caches: one
// SQLite database file per cluster record under the host-supplied data dir.
// The package owns the lifecycle of those files (open/migrate/quarantine/
// delete) plus a per-cluster janitor goroutine; it does NOT own syncing —
// the kube package's sync controller starts an engine (internal/kube/
// clustersync) that writes through the ClusterDB this package hands out.
//
// Concurrency model: SQLite is opened in WAL mode so readers don't block a
// writer. We expose two *sql.DB handles against the same file — a single-
// connection writer pool (no SQLITE_BUSY storms) and a multi-connection
// reader pool. Callers pick the right one via Reader() / Writer().
//
// Cross-platform: uses the pure-Go modernc.org/sqlite driver so no CGO
// toolchain is required for darwin/linux/windows builds.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// dbSuffixes are the file extensions that together make up one cluster's
// on-disk SQLite cache: the main DB plus its WAL and shared-memory sidecars.
// Any operation on a cache file (quarantine, delete, size) must cover all
// three.
var dbSuffixes = []string{"", "-wal", "-shm"}

// clusterDBPath is the on-disk path of a cluster's main SQLite file. The
// sidecars live at this path + each dbSuffixes entry.
func clusterDBPath(dataDir, clusterID string) string {
	return filepath.Join(dataDir, "clusters", clusterID+".db")
}

// validClusterID guards every cluster ID that becomes a filesystem path. A
// ClusterID is a registry-minted UUID, so it never contains a path separator
// or "..". Rejecting those keeps a hostile/buggy ID (e.g. "../../foo" from a
// GraphQL mutation) from escaping the clusters dir in clusterDBPath — without
// it DeleteCacheFiles could remove "foo.db" anywhere on disk.
func validClusterID(id string) bool {
	if id == "" || strings.Contains(id, "..") || strings.ContainsAny(id, `/\`) {
		return false
	}
	return filepath.Base(id) == id
}

// readerPoolSize caps concurrent SQLite read connections per cluster.
// WAL allows many readers in parallel, but each open conn carries memory
// (page cache, prepared statements). Four is enough to keep a handful of
// GraphQL resolvers running without serializing.
const readerPoolSize = 4

// Manager owns one ClusterDB per cluster ID. Safe for concurrent use.
type Manager struct {
	dataDir string

	mu    sync.RWMutex
	dbs   map[string]*ClusterDB
	close bool
}

// NewManager returns a Manager rooted at dataDir. The clusters directory is
// created lazily on first Open.
func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir: dataDir,
		dbs:     make(map[string]*ClusterDB),
	}
}

// Open returns the ClusterDB for the given cluster ID, creating and
// migrating the on-disk SQLite file on first call (and starting its
// janitor). Subsequent calls return the same handle. After Shutdown the
// Manager refuses new opens.
func (m *Manager) Open(ctx context.Context, clusterID string) (*ClusterDB, error) {
	if !validClusterID(clusterID) {
		return nil, fmt.Errorf("invalid cluster id %q", clusterID)
	}

	m.mu.RLock()
	if m.close {
		m.mu.RUnlock()
		return nil, errors.New("cache is shut down")
	}
	if cdb, ok := m.dbs[clusterID]; ok {
		m.mu.RUnlock()
		return cdb, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.close {
		return nil, errors.New("cache is shut down")
	}
	if cdb, ok := m.dbs[clusterID]; ok {
		return cdb, nil
	}

	cdb, err := openClusterDB(ctx, m.dataDir, clusterID)
	if err != nil {
		return nil, err
	}
	cdb.startJanitor()
	m.dbs[clusterID] = cdb
	return cdb, nil
}

// Lookup returns the ClusterDB for id if it is currently open, or nil.
// Unlike Open it never creates or starts anything, so reader paths use it
// to reach an already-open handle.
func (m *Manager) Lookup(id string) *ClusterDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbs[id]
}

// CacheBytes returns the total on-disk size of a cluster's cache (main DB plus
// -wal/-shm sidecars) and whether any cache file exists. Cheap stat-only; works
// whether or not the cluster is currently open.
func (m *Manager) CacheBytes(clusterID string) (int64, bool) {
	if !validClusterID(clusterID) {
		return 0, false
	}
	path := clusterDBPath(m.dataDir, clusterID)
	var (
		total int64
		found bool
	)
	for _, suffix := range dbSuffixes {
		fi, err := os.Stat(path + suffix)
		if err != nil {
			continue
		}
		total += fi.Size()
		found = true
	}
	return total, found
}

// DeleteCacheFiles closes the cluster (releasing the file handles — required on
// Windows) and removes its cache files (main DB + sidecars). Safe for an
// unknown/closed cluster. A later Open recreates a fresh, empty cache.
func (m *Manager) DeleteCacheFiles(ctx context.Context, clusterID string) error {
	if !validClusterID(clusterID) {
		return fmt.Errorf("invalid cluster id %q", clusterID)
	}
	if err := m.Close(ctx, clusterID); err != nil {
		return err
	}
	path := clusterDBPath(m.dataDir, clusterID)
	for _, suffix := range dbSuffixes {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", path+suffix, err)
		}
	}
	return nil
}

// Close shuts down a single cluster's DB and janitor goroutine. Safe to call
// for an unknown ID (returns nil).
func (m *Manager) Close(ctx context.Context, clusterID string) error {
	m.mu.Lock()
	cdb, ok := m.dbs[clusterID]
	delete(m.dbs, clusterID)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return cdb.shutdown(ctx)
}

// Shutdown closes every open cluster DB. Subsequent Opens return an error.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.close = true
	dbs := m.dbs
	m.dbs = nil
	m.mu.Unlock()

	var firstErr error
	for id, cdb := range dbs {
		if err := cdb.shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", id, err)
		}
	}
	return firstErr
}

// ClusterDB is a single cluster's local SQLite cache. Reader() and Writer()
// hand out the appropriate pool; callers must not Close those handles.
type ClusterDB struct {
	id   string
	path string

	writeDB *sql.DB
	readDB  *sql.DB

	// janitorCancel/janitorDone bound the per-cluster TTL goroutine.
	// shutdown waits on janitorDone so a clean close doesn't leave a
	// goroutine writing into a closed DB.
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}

	subsMu sync.Mutex
	subs   map[int]chan struct{}
	nextID int
}

// Reader returns the multi-connection pool for SELECTs. Lock-free under WAL.
func (c *ClusterDB) Reader() *sql.DB { return c.readDB }

// Writer returns the single-connection pool for mutations. Wrap batches in
// `BEGIN IMMEDIATE; ... COMMIT;` for a single fsync per batch.
func (c *ClusterDB) Writer() *sql.DB { return c.writeDB }

// ID returns the cluster ID this DB is bound to.
func (c *ClusterDB) ID() string { return c.id }

// Path returns the on-disk SQLite file path. Exposed for diagnostics.
func (c *ClusterDB) Path() string { return c.path }

// ResourceStat is a per-resource cache breakdown row: how many objects of one
// kind are cached and when its rows last changed.
type ResourceStat struct {
	// Resource identifies the kind, "<group>/<Kind>" with the core group
	// elided (e.g. "Pod", "apps/Deployment"), or "events".
	Resource string
	// Count is the number of cached objects of this kind.
	Count int
	// LastUpdatedAt is the most recent sync write for this kind; nil if the
	// table carries no timestamp for it.
	LastUpdatedAt *time.Time
}

// ResourceStats reads the per-resource breakdown of the cache: one row per
// cached kind from the universal objects table, plus one for events.
func (c *ClusterDB) ResourceStats(ctx context.Context) ([]ResourceStat, error) {
	rows, err := c.readDB.QueryContext(ctx,
		`SELECT api_version, kind, COUNT(*), MAX(updated_at)
		 FROM objects GROUP BY api_version, kind ORDER BY api_version, kind`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []ResourceStat
	for rows.Next() {
		var (
			apiVersion, kind string
			count            int
			updatedAt        sql.NullInt64
		)
		if err := rows.Scan(&apiVersion, &kind, &count, &updatedAt); err != nil {
			return nil, err
		}
		stats = append(stats, ResourceStat{
			Resource:      resourceLabel(apiVersion, kind),
			Count:         count,
			LastUpdatedAt: millisPtr(updatedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var (
		eventCount  int
		eventUpdate sql.NullInt64
	)
	if err := c.readDB.QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(updated_at) FROM events`).Scan(&eventCount, &eventUpdate); err != nil {
		return nil, err
	}
	if eventCount > 0 {
		stats = append(stats, ResourceStat{
			Resource:      "events",
			Count:         eventCount,
			LastUpdatedAt: millisPtr(eventUpdate),
		})
	}
	return stats, nil
}

// resourceLabel renders an api_version + kind pair as the consumer-facing
// resource identifier: the group qualifies the kind, the version is dropped,
// and the core group is elided ("v1"+"Pod" → "Pod", "apps/v1"+"Deployment" →
// "apps/Deployment").
func resourceLabel(apiVersion, kind string) string {
	if group, _, ok := strings.Cut(apiVersion, "/"); ok {
		return group + "/" + kind
	}
	return kind
}

func millisPtr(ni sql.NullInt64) *time.Time {
	if !ni.Valid {
		return nil
	}
	t := time.UnixMilli(ni.Int64).UTC()
	return &t
}

func openClusterDB(ctx context.Context, dataDir, clusterID string) (*ClusterDB, error) {
	clusterDir := filepath.Join(dataDir, "clusters")
	if err := os.MkdirAll(clusterDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", clusterDir, err)
	}
	dbPath := clusterDBPath(dataDir, clusterID)

	writeDB, err := openPool(dbPath, true)
	if err != nil {
		return nil, fmt.Errorf("open writer: %w", err)
	}

	if err := integrityCheck(ctx, writeDB); err != nil {
		writeDB.Close()
		if quarantineErr := quarantineCorrupt(dbPath); quarantineErr != nil {
			return nil, fmt.Errorf("integrity check failed (%v) and quarantine failed: %w", err, quarantineErr)
		}
		slog.Warn("clustercache: quarantined corrupt db", "id", clusterID, "err", err)
		writeDB, err = openPool(dbPath, true)
		if err != nil {
			return nil, fmt.Errorf("reopen after quarantine: %w", err)
		}
	}

	// auto_vacuum=INCREMENTAL lets the janitor's PRAGMA incremental_vacuum return
	// DELETE'd pages to the OS. It can't live in a migration: the runner creates
	// its schema_migrations table first, and once any table exists the mode is
	// sticky. It also can't be a plain PRAGMA here — opening the pool already
	// writes the WAL header (auto_vacuum=0) before this runs. So set the mode and
	// VACUUM to rewrite the file with it. Gate on the current mode: a fresh DB
	// reports 0 and needs the one-time conversion; an already-converted DB
	// reports 2, so we skip the VACUUM — otherwise every reopen would rewrite the
	// whole file (potentially large) before sync can even start.
	const autoVacuumIncremental = 2
	var mode int
	if err := writeDB.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("read auto_vacuum: %w", err)
	}
	if mode != autoVacuumIncremental {
		if _, err := writeDB.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`); err != nil {
			writeDB.Close()
			return nil, fmt.Errorf("set auto_vacuum: %w", err)
		}
	}

	if _, err := sqlitemigrate.Apply(ctx, writeDB, migrationsFS, "migrations"); err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	readDB, err := openPool(dbPath, false)
	if err != nil {
		writeDB.Close()
		return nil, fmt.Errorf("open reader: %w", err)
	}

	return &ClusterDB{
		id:      clusterID,
		path:    dbPath,
		writeDB: writeDB,
		readDB:  readDB,
		subs:    make(map[int]chan struct{}),
	}, nil
}

// Subscribe returns a channel that receives a non-blocking signal after every
// Notify call. The channel has capacity 1 — additional pings while one is
// already buffered are coalesced (the subscriber will re-query and observe
// every change anyway). The returned cancel func unregisters the subscriber.
//
// Use this in place of polling: write paths call Notify after commit, and
// long-lived readers (e.g. the sync engine's freshness tracker, GraphQL
// subscriptions) block on the channel to know when to re-run their query.
func (c *ClusterDB) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.subsMu.Lock()
	id := c.nextID
	c.nextID++
	c.subs[id] = ch
	c.subsMu.Unlock()
	return ch, func() {
		c.subsMu.Lock()
		if existing, ok := c.subs[id]; ok {
			delete(c.subs, id)
			close(existing)
		}
		c.subsMu.Unlock()
	}
}

// Notify wakes every active subscriber. Non-blocking: if a subscriber's
// channel slot is full, the existing buffered ping subsumes this one.
// Writers should call this after committing a batch.
func (c *ClusterDB) Notify() {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for _, ch := range c.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// openPool opens a *sql.DB against path via the shared sqlitemigrate pool
// opener. writer=true caps MaxOpenConns at 1 so write transactions serialize
// at the pool level instead of fighting at the SQLite layer; readers get the
// multi-connection WAL pool.
func openPool(path string, writer bool) (*sql.DB, error) {
	maxConns := readerPoolSize
	if writer {
		maxConns = 1
	}
	return sqlitemigrate.OpenPool(path, maxConns)
}

func integrityCheck(ctx context.Context, db *sql.DB) error {
	// integrity_check returns one row with "ok" on a healthy DB. We accept
	// only that exact value — anything else means SQLite found damage.
	rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return errors.New("integrity_check returned no rows")
	}
	var v string
	if err := rows.Scan(&v); err != nil {
		return err
	}
	if v != "ok" {
		return fmt.Errorf("integrity_check: %s", v)
	}
	return nil
}

// quarantineCorrupt renames a damaged DB (and its WAL sidecars) aside so
// the next open creates a fresh file. The cluster will re-sync from
// upstream — losing the local cache is annoying but never fatal.
func quarantineCorrupt(path string) error {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	for _, suffix := range dbSuffixes {
		src := path + suffix
		if _, err := os.Stat(src); err != nil {
			continue
		}
		dst := path + ".corrupt-" + stamp + suffix
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}
	return nil
}

// startJanitor launches the per-cluster TTL goroutine. Independent of write
// activity — it runs even on an idle cluster so a long-paused app still
// trims stale rows the next time it wakes.
func (c *ClusterDB) startJanitor() {
	ctx, cancel := context.WithCancel(context.Background())
	c.janitorCancel = cancel
	c.janitorDone = make(chan struct{})
	go func() {
		defer close(c.janitorDone)
		runJanitor(ctx, c.id, c.writeDB, defaultRetention)
	}()
}

func (c *ClusterDB) shutdown(ctx context.Context) error {
	if c.janitorCancel != nil {
		c.janitorCancel()
		select {
		case <-c.janitorDone:
		case <-ctx.Done():
			// The janitor is still executing — it may be mid-write. Closing
			// writeDB now would close the connection out from under it, so bail
			// without closing rather than risk a write into a closed DB. The
			// handles leak, but a stuck goroutine plus a freed *sql.DB is worse;
			// this is the rare timeout path (the process is usually going down).
			slog.Warn("clustercache: janitor did not stop before deadline; leaving DBs open", "id", c.id)
			return ctx.Err()
		}
	}
	// Close subscriber channels so any blocked select returns.
	c.subsMu.Lock()
	for id, ch := range c.subs {
		delete(c.subs, id)
		close(ch)
	}
	c.subsMu.Unlock()
	var firstErr error
	if err := c.readDB.Close(); err != nil {
		firstErr = err
	}
	if err := c.writeDB.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
