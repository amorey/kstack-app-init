// Package clustercache is the sidecar's local cache for remote Kubernetes
// cluster state. Each cluster gets its own SQLite database file under the
// host-supplied data dir; the package owns the lifecycle of those files
// plus a long-lived sync goroutine per open cluster.
//
// Concurrency model: SQLite is opened in WAL mode so readers don't block a
// writer. We expose two *sql.DB handles against the same file — a single-
// connection writer pool (no SQLITE_BUSY storms) and a multi-connection
// reader pool. Callers pick the right one via Reader() / Writer().
//
// Cross-platform: uses the pure-Go modernc.org/sqlite driver so no CGO
// toolchain is required for darwin/linux/windows builds.
package clustercache

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
func clusterDBPath(dataDir, clusterUUID string) string {
	return filepath.Join(dataDir, "clusters", clusterUUID+".db")
}

// validClusterUUID guards every UUID that becomes a filesystem path. A cluster
// UUID is a kube-system namespace UID, so it never contains a path separator or
// "..". Rejecting those keeps a hostile/buggy UUID (e.g. "../../foo" from a
// GraphQL mutation) from escaping the clusters dir in clusterDBPath — without it
// DeleteCacheFiles could remove "foo.db" anywhere on disk.
func validClusterUUID(uuid string) bool {
	if uuid == "" || strings.Contains(uuid, "..") || strings.ContainsAny(uuid, `/\`) {
		return false
	}
	return filepath.Base(uuid) == uuid
}

// readerPoolSize caps concurrent SQLite read connections per cluster.
// WAL allows many readers in parallel, but each open conn carries memory
// (page cache, prepared statements). Four is enough to keep a handful of
// GraphQL resolvers running without serializing.
const readerPoolSize = 4

// Sync-runner retry backoff. If Upstream.Run returns while the cluster is still
// open (a startup error, or discovery transiently yielding no resources after a
// successful UID probe), the coordinator still has the cluster in its open set
// and won't re-Open it — so the runner must retry itself or the cluster stays
// unsynced until restart. Capped exponential backoff, reset to ctx-cancel on
// close. Vars (not consts) so tests can shrink them.
var (
	syncRetryInitial = 1 * time.Second
	syncRetryMax     = 30 * time.Second
)

// Upstream is the source of truth the sync runner pulls from. Kept as an
// interface so the eventual wiring (cloud GraphQL vs direct informer) is
// decoupled from this package — tests can pass a stub.
type Upstream interface {
	// Run blocks for the cluster's lifetime, applying upstream changes to
	// the writer DB. It must return when ctx is cancelled.
	Run(ctx context.Context, clusterUUID string, writer *sql.DB) error
}

// Manager owns one ClusterDB per cluster UUID. Safe for concurrent use.
type Manager struct {
	dataDir  string
	upstream Upstream

	mu    sync.RWMutex
	dbs   map[string]*ClusterDB
	close bool
}

// NewManager returns a Manager rooted at dataDir. The directory is created
// lazily on first Open. upstream may be nil to disable background sync —
// useful for tests and for the initial PR where the upstream isn't wired yet.
func NewManager(dataDir string, upstream Upstream) *Manager {
	return &Manager{
		dataDir:  dataDir,
		upstream: upstream,
		dbs:      make(map[string]*ClusterDB),
	}
}

// SetUpstream swaps the Upstream after construction. Useful when the
// Upstream needs a reference back to the Manager (e.g. clustersync.Upstream,
// which Lookups the ClusterDB to Notify) and so can't be passed at
// NewManager time. Must be called before any Open.
func (m *Manager) SetUpstream(u Upstream) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upstream = u
}

// Open returns the ClusterDB for the given cluster UUID, creating and
// migrating the on-disk SQLite file on first call. Subsequent calls return
// the same handle. After Shutdown the Manager refuses new opens.
func (m *Manager) Open(ctx context.Context, clusterUUID string) (*ClusterDB, error) {
	if !validClusterUUID(clusterUUID) {
		return nil, fmt.Errorf("invalid cluster uuid %q", clusterUUID)
	}

	m.mu.RLock()
	if m.close {
		m.mu.RUnlock()
		return nil, errors.New("cache is shut down")
	}
	if cdb, ok := m.dbs[clusterUUID]; ok {
		m.mu.RUnlock()
		return cdb, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.close {
		return nil, errors.New("cache is shut down")
	}
	if cdb, ok := m.dbs[clusterUUID]; ok {
		return cdb, nil
	}

	cdb, err := openClusterDB(ctx, m.dataDir, clusterUUID)
	if err != nil {
		return nil, err
	}

	if m.upstream != nil {
		cdb.startSync(m.upstream)
	}
	m.dbs[clusterUUID] = cdb
	return cdb, nil
}

// Lookup returns the ClusterDB for uuid if it is currently open, or nil. Unlike
// Open it never creates or starts anything, so reader paths (and Upstream.Run,
// which would deadlock calling Open during its own sync setup) use it to reach
// an already-open handle.
func (m *Manager) Lookup(uuid string) *ClusterDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbs[uuid]
}

// CacheBytes returns the total on-disk size of a cluster's cache (main DB plus
// -wal/-shm sidecars) and whether any cache file exists. Cheap stat-only; works
// whether or not the cluster is currently open.
func (m *Manager) CacheBytes(clusterUUID string) (int64, bool) {
	if !validClusterUUID(clusterUUID) {
		return 0, false
	}
	path := clusterDBPath(m.dataDir, clusterUUID)
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
func (m *Manager) DeleteCacheFiles(ctx context.Context, clusterUUID string) error {
	if !validClusterUUID(clusterUUID) {
		return fmt.Errorf("invalid cluster uuid %q", clusterUUID)
	}
	if err := m.Close(ctx, clusterUUID); err != nil {
		return err
	}
	path := clusterDBPath(m.dataDir, clusterUUID)
	for _, suffix := range dbSuffixes {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", path+suffix, err)
		}
	}
	return nil
}

// ScanCachedUUIDs lists the UUIDs of clusters that have a cache file on disk,
// ignoring quarantined `.corrupt-*` files. Used at startup to surface orphaned
// caches whose kube-context has gone. A missing clusters dir yields no UUIDs.
func (m *Manager) ScanCachedUUIDs() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(m.dataDir, "clusters"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var uuids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".db") || strings.Contains(name, ".corrupt-") {
			continue
		}
		uuids = append(uuids, strings.TrimSuffix(name, ".db"))
	}
	return uuids, nil
}

// Close shuts down a single cluster's DB and sync goroutine. Safe to call
// for an unknown UUID (returns nil).
func (m *Manager) Close(ctx context.Context, clusterUUID string) error {
	m.mu.Lock()
	cdb, ok := m.dbs[clusterUUID]
	delete(m.dbs, clusterUUID)
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
	for uuid, cdb := range dbs {
		if err := cdb.shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", uuid, err)
		}
	}
	return firstErr
}

// ClusterDB is a single cluster's local SQLite cache. Reader() and Writer()
// hand out the appropriate pool; callers must not Close those handles.
type ClusterDB struct {
	uuid string
	path string

	writeDB *sql.DB
	readDB  *sql.DB

	syncCancel context.CancelFunc
	syncDone   chan struct{}
	// janitorDone closes once the per-cluster TTL goroutine has returned.
	// shutdown waits on it alongside syncDone so a clean close doesn't
	// leave a goroutine writing into a closed DB.
	janitorDone chan struct{}

	subsMu sync.Mutex
	subs   map[int]chan struct{}
	nextID int
}

// Reader returns the multi-connection pool for SELECTs. Lock-free under WAL.
func (c *ClusterDB) Reader() *sql.DB { return c.readDB }

// Writer returns the single-connection pool for mutations. Wrap batches in
// `BEGIN IMMEDIATE; ... COMMIT;` for a single fsync per batch.
func (c *ClusterDB) Writer() *sql.DB { return c.writeDB }

// UUID returns the cluster UUID this DB is bound to.
func (c *ClusterDB) UUID() string { return c.uuid }

// Path returns the on-disk SQLite file path. Exposed for diagnostics.
func (c *ClusterDB) Path() string { return c.path }

func openClusterDB(ctx context.Context, dataDir, clusterUUID string) (*ClusterDB, error) {
	clusterDir := filepath.Join(dataDir, "clusters")
	if err := os.MkdirAll(clusterDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", clusterDir, err)
	}
	dbPath := clusterDBPath(dataDir, clusterUUID)

	writeDB, err := openPool(dbPath, true)
	if err != nil {
		return nil, fmt.Errorf("open writer: %w", err)
	}

	if err := integrityCheck(ctx, writeDB); err != nil {
		writeDB.Close()
		if quarantineErr := quarantineCorrupt(dbPath); quarantineErr != nil {
			return nil, fmt.Errorf("integrity check failed (%v) and quarantine failed: %w", err, quarantineErr)
		}
		slog.Warn("clustercache: quarantined corrupt db", "uuid", clusterUUID, "err", err)
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
		uuid:    clusterUUID,
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
// long-lived readers (e.g. GraphQL subscriptions) block on the channel to
// know when to re-run their query.
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

func (c *ClusterDB) startSync(up Upstream) {
	ctx, cancel := context.WithCancel(context.Background())
	c.syncCancel = cancel
	c.syncDone = make(chan struct{})
	go func() {
		defer close(c.syncDone)
		backoff := syncRetryInitial
		for {
			err := up.Run(ctx, c.uuid, c.writeDB)
			// A live ctx means the cluster is still open, so any return —
			// error or a premature nil (e.g. discovery found no resources) —
			// is abnormal: retry until the cluster is closed (ctx cancelled).
			if ctx.Err() != nil {
				return
			}
			slog.Error("clustercache: sync runner exited, retrying",
				"uuid", c.uuid, "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > syncRetryMax {
				backoff = syncRetryMax
			}
		}
	}()
	// Janitor lives alongside the sync runner. Shares the same ctx so a
	// cluster close shuts both down together. Independent of write
	// activity — runs even on an idle cluster so a long-paused app still
	// trims stale rows the next time it wakes.
	c.janitorDone = make(chan struct{})
	go func() {
		defer close(c.janitorDone)
		runJanitor(ctx, c.uuid, c.writeDB, defaultRetention)
	}()
}

func (c *ClusterDB) shutdown(ctx context.Context) error {
	if c.syncCancel != nil {
		c.syncCancel()
		select {
		case <-c.syncDone:
		case <-ctx.Done():
			// The sync runner is still executing — it may be mid-write. Closing
			// writeDB now would close the connection out from under it, so bail
			// without closing rather than risk a write into a closed DB. The
			// handles leak, but a stuck goroutine plus a freed *sql.DB is worse;
			// this is the rare timeout path (the process is usually going down).
			slog.Warn("clustercache: sync runner did not stop before deadline; leaving DBs open", "uuid", c.uuid)
			return ctx.Err()
		}
		if c.janitorDone != nil {
			select {
			case <-c.janitorDone:
			case <-ctx.Done():
				slog.Warn("clustercache: janitor did not stop before deadline; leaving DBs open", "uuid", c.uuid)
				return ctx.Err()
			}
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
