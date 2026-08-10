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

// Package store owns the per-cluster on-disk caches: one SQLite file per cache
// incarnation, its lifecycle (open/migrate/quarantine/delete), a janitor goroutine,
// and the reads. Syncing lives elsewhere — workers write through the ClusterDB.
//
// WAL mode, two pools over one file: single-connection writer (Writer(), no
// SQLITE_BUSY storms) and multi-connection reader (Reader()). Pure-Go
// modernc.org/sqlite driver, so no CGO.
// See docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
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
	"strconv"
	"sync"
	"time"

	"github.com/amorey/gobus"
	buswatch "github.com/amorey/gobus/watch"
	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// dbSuffixes are the three files making up one cache: main DB + WAL + shm. Every
// file operation (quarantine, delete, size) must cover all three.
var dbSuffixes = []string{"", "-wal", "-shm"}

// CacheRef identifies one on-disk cache incarnation: parent Cluster ObjectID (the
// directory) + ClusterCache ObjectID (the file). Both AUTOINCREMENT, so path-safe and
// never reused — delete+recreate yields a fresh file, no finalize-vs-recreate race.
// int64, not beehive.ObjectID, to keep this leaf package beehive-free.
type CacheRef struct {
	ClusterID int64
	CacheID   int64
}

func (r CacheRef) valid() bool { return r.ClusterID > 0 && r.CacheID > 0 }

func (r CacheRef) label() string {
	return strconv.FormatInt(r.ClusterID, 10) + "/" + strconv.FormatInt(r.CacheID, 10)
}

// clusterDir is <dataDir>/clusters/<ClusterID>/.
func clusterDir(dataDir string, ref CacheRef) string {
	return filepath.Join(dataDir, "clusters", strconv.FormatInt(ref.ClusterID, 10))
}

// clusterDBPath is <dataDir>/clusters/<ClusterID>/<CacheID>.db; sidecars live at
// this path + each dbSuffixes entry. Both segments are numeric ids, so the path
// can never escape the clusters dir.
func clusterDBPath(dataDir string, ref CacheRef) string {
	return filepath.Join(clusterDir(dataDir, ref), strconv.FormatInt(ref.CacheID, 10)+".db")
}

// readerPoolSize caps concurrent read connections per cache; each open conn costs
// memory, and four keeps a handful of resolvers from serializing.
const readerPoolSize = 4

// finishCloseTimeout bounds a retried close (see startFinishClose): long enough for a
// janitor sweep, short enough that a wedged one doesn't hold a goroutine forever.
const finishCloseTimeout = 30 * time.Second

// Manager owns one ClusterDB per open cache incarnation, keyed by CacheID. Safe for
// concurrent use.
type Manager struct {
	dataDir string

	mu    sync.RWMutex
	dbs   map[int64]*ClusterDB
	close bool

	// dbWatchers holds one latest-value handle hub per watched CacheID, created lazily
	// by WatchDB and dropped when its last subscriber cancels.
	dbWatchers map[int64]*dbWatch
	// closing holds handles that have left dbs but whose pools may still be live. An id
	// here is neither open (Open must not hand it out, nor build a second pool over the
	// same file) nor gone (Close must find it again).
	closing map[int64]*closingDB
	// deleting spans DeleteCacheFiles' whole close+unlink sequence. Closing alone doesn't
	// cover it: an Open in the gap would CREATE the file about to be unlinked and register
	// a handle onto an unnamed inode that every rebuilt worker then writes into.
	deleting map[int64]bool
	// finishing marks caches whose failed close is being retried, so retries don't stack
	// up one per Open — see startFinishClose.
	finishing map[int64]bool
}

// closingDB is a handle that has left m.dbs but is not yet fully closed. done is non-nil
// while a shutdown attempt runs, so a second Close waits rather than tearing the same
// pools down twice; an entry outliving its attempt means that shutdown failed, kept so
// the next Close re-waits on the SAME handle instead of finding nothing.
type closingDB struct {
	cdb  *ClusterDB
	done chan struct{}
	// err is the last attempt's outcome, written before done closes, so a waiter learns
	// whether the pools went away or the handle still needs finishing off.
	err error
}

// dbWatch is one CacheID's handle hub plus its live-subscriber refcount.
type dbWatch struct {
	hub  *watch.Hub[*ClusterDB]
	tx   *watch.Sender[*ClusterDB]
	refs int
}

// NewManager returns a Manager rooted at dataDir; the clusters directory is created
// on first Open.
func NewManager(dataDir string) *Manager {
	return &Manager{
		dataDir:    dataDir,
		dbs:        make(map[int64]*ClusterDB),
		dbWatchers: make(map[int64]*dbWatch),
		closing:    make(map[int64]*closingDB),
		deleting:   make(map[int64]bool),
		finishing:  make(map[int64]bool),
	}
}

// Open returns the cache's ClusterDB, creating/migrating the SQLite file and starting
// its janitor on first call. Refuses new opens after Shutdown.
func (m *Manager) Open(ctx context.Context, ref CacheRef) (*ClusterDB, error) {
	if !ref.valid() {
		return nil, fmt.Errorf("invalid cache ref %+v", ref)
	}

	m.mu.RLock()
	if m.close {
		m.mu.RUnlock()
		return nil, ErrManagerShutDown
	}
	if cdb, ok := m.dbs[ref.CacheID]; ok {
		m.mu.RUnlock()
		return cdb, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.close {
		return nil, ErrManagerShutDown
	}
	if cdb, ok := m.dbs[ref.CacheID]; ok {
		return cdb, nil
	}
	// Mid-deletion: opening here would recreate the very file about to be unlinked.
	if m.deleting[ref.CacheID] {
		return nil, fmt.Errorf("cache %d is being deleted", ref.CacheID)
	}
	// A closing cache is neither open nor gone: handing the handle back would close its
	// pools mid-query, and a second pool over the same file would race the pending
	// delete. The caller retries once the close resolves — and a FAILED close has no
	// other driver (Close is reachable only via DeleteCacheFiles, which just surfaces
	// the error), so a stranded entry gets its retry started here, on its own goroutine:
	// Open must not block on the wedged janitor that stranded it.
	if entry, ok := m.closing[ref.CacheID]; ok {
		if entry.done == nil {
			m.startFinishClose(ref.CacheID)
		}
		return nil, fmt.Errorf("cache %d is closing", ref.CacheID)
	}

	cdb, err := openClusterDB(ctx, m.dataDir, ref)
	if err != nil {
		return nil, err
	}
	cdb.startJanitor()
	m.dbs[ref.CacheID] = cdb
	m.publishDBLocked(ref.CacheID, cdb)
	return cdb, nil
}

// startFinishClose retries a failed close stranded in m.closing, on its own goroutine.
// Called under m.mu; at most one retry per cache, so retries don't pile up per Open.
func (m *Manager) startFinishClose(cacheID int64) {
	if m.finishing[cacheID] {
		return
	}
	m.finishing[cacheID] = true
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), finishCloseTimeout)
		defer cancel()
		if err := m.Close(ctx, cacheID); err != nil {
			slog.Warn("clustercache: retry of a failed close did not finish", "cache", cacheID, "err", err)
		}
		m.mu.Lock()
		delete(m.finishing, cacheID)
		m.mu.Unlock()
	}()
}

// WatchDB streams a CacheID's open handle as it changes: current handle (nil if not
// open) on subscribe, then on open, close, or replace (a Clear yields nil then the new
// handle). Latest-value; the channel closes on Shutdown. Long-lived readers bind
// through this rather than once to Lookup, so they follow a cache swap.
func (m *Manager) WatchDB(cacheID int64) (<-chan *ClusterDB, func()) {
	m.mu.Lock()
	if m.close {
		m.mu.Unlock()
		ch := make(chan *ClusterDB)
		close(ch)
		return ch, func() {}
	}
	w := m.dbWatchers[cacheID]
	if w == nil {
		// Seed with the current handle so a subscriber arriving after the open
		// still sees it.
		hub := watch.New(m.dbs[cacheID])
		w = &dbWatch{hub: hub, tx: hub.Sender()}
		m.dbWatchers[cacheID] = w
	}
	w.refs++
	rx := w.hub.Receiver()
	m.mu.Unlock()

	return rx.Chan(), func() {
		m.mu.Lock()
		if w.refs--; w.refs == 0 {
			w.hub.Close()
			delete(m.dbWatchers, cacheID)
		}
		m.mu.Unlock()
		rx.Close()
	}
}

// publishDBLocked pushes db (nil = closed) to a CacheID's hub if anyone is watching;
// a later WatchDB seeds itself from m.dbs. Must hold m.mu.
func (m *Manager) publishDBLocked(cacheID int64, db *ClusterDB) {
	if w := m.dbWatchers[cacheID]; w != nil {
		w.tx.Send(db) //nolint:errcheck // Send never blocks; a closed hub is a no-op
	}
}

// Lookup returns the CacheID's ClusterDB if currently open, else nil. It never creates
// or starts anything — teardown paths must use it, never Open, which would
// re-materialize the file. See docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
func (m *Manager) Lookup(cacheID int64) *ClusterDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbs[cacheID]
}

// CacheBytes returns a cache's total on-disk size (DB + sidecars) and whether any file
// exists. Stat-only; works whether or not the cache is open.
func (m *Manager) CacheBytes(ref CacheRef) (int64, bool) {
	if !ref.valid() {
		return 0, false
	}
	path := clusterDBPath(m.dataDir, ref)
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

// ErrManagerShutDown is returned by every entry point once Shutdown has run. Close
// returns it rather than the "unknown id" nil, so DeleteCacheFiles aborts instead of
// unlinking files behind a handle Shutdown is still closing.
var ErrManagerShutDown = errors.New("cache manager is shut down")

// DeleteCacheFiles closes the cache (releasing file handles — required on Windows) and
// removes its files. Safe for an unknown/closed cache; a later Open starts fresh.
func (m *Manager) DeleteCacheFiles(ctx context.Context, ref CacheRef) error {
	return m.deleteCacheFilesWithHook(ctx, ref, nil)
}

// deleteCacheFilesWithHook is DeleteCacheFiles with a test seam on the close→unlink
// window an Open must not slip into.
func (m *Manager) deleteCacheFilesWithHook(
	ctx context.Context, ref CacheRef, betweenCloseAndUnlink func(),
) error {
	if !ref.valid() {
		return fmt.Errorf("invalid cache ref %+v", ref)
	}
	// Un-openable across the WHOLE sequence, not just the close: an Open in the
	// close→unlink gap recreates the file and registers a handle onto an unnamed inode.
	m.mu.Lock()
	if m.deleting[ref.CacheID] {
		m.mu.Unlock()
		return fmt.Errorf("cache %d is already being deleted", ref.CacheID)
	}
	if m.deleting == nil {
		m.mu.Unlock()
		return ErrManagerShutDown
	}
	m.deleting[ref.CacheID] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.deleting, ref.CacheID)
		m.mu.Unlock()
	}()

	if err := m.Close(ctx, ref.CacheID); err != nil {
		return err
	}
	if betweenCloseAndUnlink != nil {
		betweenCloseAndUnlink()
	}
	path := clusterDBPath(m.dataDir, ref)
	for _, suffix := range dbSuffixes {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", path+suffix, err)
		}
	}
	// Reap the per-cluster dir. os.Remove only succeeds when empty, so a dir still
	// holding another incarnation survives; the error is expected and ignored.
	_ = os.Remove(clusterDir(m.dataDir, ref))
	return nil
}

// Close shuts down one cache's DB and janitor. Safe for an unknown id (returns nil).
//
// The handle is forgotten only once shutdown SUCCEEDS: a missed deadline leaves the
// pools open (a janitor mid-write must keep its connection), so the handle is still
// live and DeleteCacheFiles' retry must re-wait on it rather than find nothing, return
// nil, and unlink the .db under a live janitor.
func (m *Manager) Close(ctx context.Context, cacheID int64) error {
	for {
		m.mu.Lock()
		if m.close {
			// Shutdown took every handle out of dbs/closing before closing them, so
			// "not registered" here does NOT mean "not live" — returning nil would let
			// DeleteCacheFiles unlink the .db mid-write.
			m.mu.Unlock()
			return ErrManagerShutDown
		}
		entry, ok := m.closing[cacheID]
		switch {
		case ok && entry.done != nil:
			// Another Close owns this handle: wait for its attempt rather than tearing
			// the same pools down twice, then look again — if it failed, this becomes
			// the retry.
			done := entry.done
			m.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		case ok:
			// A previous attempt failed and left the pools open — retry the same handle.
			entry.done = make(chan struct{})
		default:
			cdb, open := m.dbs[cacheID]
			if !open {
				m.mu.Unlock()
				return nil
			}
			// Out of dbs before the shutdown (no Open may hand out a handle whose pools
			// are going away), into closing (a retry must still find it).
			delete(m.dbs, cacheID)
			m.publishDBLocked(cacheID, nil)
			entry = &closingDB{cdb: cdb, done: make(chan struct{})}
			m.closing[cacheID] = entry
		}
		attempt := entry.done
		m.mu.Unlock()

		err := entry.cdb.shutdown(ctx)

		m.mu.Lock()
		entry.err = err
		if err == nil {
			delete(m.closing, cacheID)
		}
		// A failed shutdown leaves the pools open, so the entry stays (the handle is
		// still live). Clearing done marks the attempt over so the next Close retries.
		entry.done = nil
		m.mu.Unlock()
		close(attempt)
		return err
	}
}

// Shutdown closes every open cluster DB. Subsequent Opens return an error.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.close = true
	dbs := m.dbs
	m.dbs = nil
	// Handles a Close took out of dbs but hasn't finished closing: their pools may still
	// be live, so shutdown owes them one more attempt. Each done channel is captured
	// here, under the lock guarding it — Close nils the field when its attempt ends.
	type pendingClose struct {
		id    int64
		entry *closingDB
		done  chan struct{}
	}
	pending := make([]pendingClose, 0, len(m.closing))
	for id, entry := range m.closing {
		pending = append(pending, pendingClose{id: id, entry: entry, done: entry.done})
	}
	m.closing = nil
	m.deleting = nil
	// End every WatchDB subscriber's stream.
	for _, w := range m.dbWatchers {
		w.hub.Close()
	}
	m.dbWatchers = nil
	m.mu.Unlock()

	var firstErr error
	shut := func(id int64, cdb *ClusterDB) {
		if err := cdb.shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close cache %d: %w", id, err)
		}
	}
	for id, cdb := range dbs {
		shut(id, cdb)
	}
	for _, p := range pending {
		if p.done != nil {
			// An attempt owns the handle, and m.closing is gone, so this is the last
			// chance to finish the job: wait, then take over if it failed.
			select {
			case <-p.done:
			case <-ctx.Done():
				if firstErr == nil {
					firstErr = fmt.Errorf("close cache %d: %w", p.id, ctx.Err())
				}
				continue
			}
			if p.entry.err == nil {
				continue // the concurrent Close finished it
			}
		}
		shut(p.id, p.entry.cdb)
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

	// janitorCancel/janitorDone bound the TTL goroutine; shutdown waits on janitorDone
	// so a clean close never leaves it writing into a closed DB.
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}

	// Two independent coalescing write-notify buses: writes (objects — drives both
	// the kind-catalog and objects watches) and events. Separate so an event burst
	// never wakes the unrelated object re-reads.
	writes *writeBus
	events *writeBus
}

// WriteWake is one wake from a write bus. It carries no value — the key names
// what moved and the subscriber re-reads the db — so consumers select on the
// channel and ignore what arrives.
type WriteWake = gobus.Event[string, struct{}]

// writeBus is a coalescing write-notify bus over gobus/watch: a subscriber
// registers for one routing key or across every key, and each publish leaves one
// pending wake in a matching subscriber's slot. A burst under one key collapses
// into that single slot, which is the coalescing the readers rely on — they
// re-query and see every change anyway.
//
// A keyed publish reaches that key's subscribers plus every across subscriber
// (the kind-catalog watch must wake on any write). There is deliberately no
// wake-everyone publish: every write path knows the resource it wrote.
type writeBus struct {
	hub *buswatch.Hub[string, struct{}]
	tx  *buswatch.Sender[string, struct{}]
}

func newWriteBus() *writeBus {
	hub := buswatch.New[string, struct{}]()
	return &writeBus{hub: hub, tx: hub.Sender()}
}

// subscribe registers for key, or across every key when key is "". Registration
// is the baseline and the bus never delivers it back, so a subscriber is woken
// only by writes that follow it.
func (b *writeBus) subscribe(key string) (<-chan WriteWake, func()) {
	var rx *buswatch.Receiver[string, struct{}]
	if key == "" {
		rx = b.hub.WatchAcross(struct{}{})
	} else {
		rx = b.hub.Watch(key, struct{}{})
	}
	return rx.Chan(), rx.Close
}

// notify publishes a wake under key. Never blocks.
func (b *writeBus) notify(key string) {
	_ = b.tx.Send(key, struct{}{}) //nolint:errcheck // a closed bus is a no-op
}

// close ends every subscription. Closes the SENDER, not the hub: Hub.Close is a
// hard tear-down with no drain, so a subscriber could lose a wake it had already
// been sent, and Sender.Close is the one the bus permits to run concurrently
// with a Send — nothing fences the writers against a db closing.
func (b *writeBus) close() { b.tx.Close() }

// Reader returns the multi-connection SELECT pool. Lock-free under WAL.
func (c *ClusterDB) Reader() *sql.DB { return c.readDB }

// Writer returns the single-connection mutation pool. Wrap batches in
// `BEGIN IMMEDIATE; ... COMMIT;` for one fsync per batch.
func (c *ClusterDB) Writer() *sql.DB { return c.writeDB }

// ID is the log label "<ClusterID>/<CacheID>".
func (c *ClusterDB) ID() string { return c.id }

// Path is the on-disk SQLite file path, for diagnostics.
func (c *ClusterDB) Path() string { return c.path }

// NullIfEmpty stores an absent string as SQL NULL, not "": both sync stores treat a
// reading they couldn't produce as distinct from an empty one, and query with IS NULL.
func NullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// EnsureKindCatalog upserts one kind's kind_catalog row and wakes the (keyless)
// catalog watch. Each kind's worker calls it on start; the upsert refreshes the
// descriptive columns if the kind's shape moved. Row.Count is ignored — triggers own it.
//
// The row is what makes a kind readable: Objects translates the plural resource back to
// its Kind through it, and Kinds LEFT JOINs kind_counts onto it, so a kind with rows but
// no catalog entry reads as empty.
//
// A row naming the same plural under a DIFFERENT Kind (a CRD renamed while the sidecar
// was down) is dropped first so the unique (api_version, resource) index can't refuse
// the insert; the old Kind's rows/edges/cookie are purged by objectsync's
// forgetSupersededKind. One transaction: a delete without its insert leaves the kind
// unreadable.
func EnsureKindCatalog(ctx context.Context, cdb *ClusterDB, row KindRow) error {
	tx, err := cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM kind_catalog WHERE api_version = ? AND resource = ? AND kind <> ?`,
		row.APIVersion, row.Resource, row.Kind); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(api_version, kind) DO UPDATE SET
			resource=excluded.resource,
			scope=excluded.scope,
			is_crd=excluded.is_crd`,
		row.APIVersion, row.Kind, row.Resource, row.Scope, row.IsCRD); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Under the kind's own key: a catalog row names one resource, so the keyless
	// kind-catalog watches wake and unrelated resources' watches do not.
	cdb.ObjectsNotifyResource(row.APIVersion, row.Resource)
	return nil
}

// KindRow is one kind_catalog entry: a kind the API server advertises, recorded at
// sync time from /apis discovery.
type KindRow struct {
	APIVersion string // group/version, e.g. "apps/v1" ("v1" for core)
	Kind       string // e.g. "Deployment"
	Resource   string // plural lowercase URL form, e.g. "deployments"
	Scope      string // "Namespaced" or "Cluster"
	IsCRD      bool
	// Count is the number of cached objects of this kind (0 for an advertised kind
	// with no cached instances).
	Count int
}

// Kinds reads the discovered kind catalog (built-ins + CRDs), ordered for stable
// display. Count comes from the trigger-maintained kind_counts via a point LEFT JOIN —
// O(kinds), never a scan of objects.
func (c *ClusterDB) Kinds(ctx context.Context) ([]KindRow, error) {
	rows, err := c.readDB.QueryContext(ctx,
		`SELECT kc.api_version, kc.kind, kc.resource, kc.scope, kc.is_crd, COALESCE(knt.count, 0)
		 FROM kind_catalog kc
		 LEFT JOIN kind_counts knt ON knt.api_version = kc.api_version AND knt.kind = kc.kind
		 ORDER BY kc.api_version, kc.kind`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []KindRow
	for rows.Next() {
		var (
			r     KindRow
			isCRD int64
		)
		if err := rows.Scan(&r.APIVersion, &r.Kind, &r.Resource, &r.Scope, &isCRD, &r.Count); err != nil {
			return nil, err
		}
		r.IsCRD = isCRD != 0
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// EventRow is one cached Kubernetes Event: the display projection of the events table
// (identity flattened; the compressed raw_json body is deliberately not read).
type EventRow struct {
	UID     string // the Event's own object UID, the watch key
	Type    string // "Normal" or "Warning" (empty if unset)
	Reason  string // CamelCase machine reason, e.g. "BackOff"
	Message string
	Count   int // coalesced series count, >= 1
	// FirstSeen/LastSeen are unix-millis, 0 when the source carried none.
	FirstSeen int64
	LastSeen  int64
	// The object the event is about; any may be empty.
	InvolvedKind string
	InvolvedNS   string
	InvolvedName string
}

// Events reads the newest cached events (last_seen index), bounded by limit; a
// non-positive limit means defaultEventsLimit.
//
// The uid tiebreak is load-bearing: lastTimestamp has one-second resolution, so ties
// straddle the limit, and a relist re-inserts every row with new rowids — leaving the
// order to rowid makes the delta watch emit phantom Deleted/Added for rows nothing moved.
func (c *ClusterDB) Events(ctx context.Context, limit int) ([]EventRow, error) {
	if limit <= 0 {
		limit = defaultEventsLimit
	}
	rows, err := c.readDB.QueryContext(ctx,
		`SELECT uid,
		        COALESCE(type, ''), COALESCE(reason, ''), COALESCE(message, ''),
		        COALESCE(count, 0), COALESCE(first_seen, 0), COALESCE(last_seen, 0),
		        COALESCE(involved_kind, ''), COALESCE(involved_ns, ''), COALESCE(involved_name, '')
		 FROM events
		 ORDER BY last_seen DESC, uid DESC
		 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]EventRow, 0, limit)
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(
			&r.UID, &r.Type, &r.Reason, &r.Message, &r.Count,
			&r.FirstSeen, &r.LastSeen, &r.InvolvedKind, &r.InvolvedNS, &r.InvolvedName,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// defaultEventsLimit bounds an unbounded Events read, and doubles as the events
// watch's diff window.
const defaultEventsLimit = 500

// ObjectRow is one cached object of any kind: the universal identity plus the full
// native body, from which the frontend derives kind-specific columns.
// See docs/adr/2026-08-09-rawjson-comparable-scalar.md.
type ObjectRow struct {
	UID        string
	APIVersion string
	Kind       string
	Namespace  string // empty for a cluster-scoped kind
	Name       string
	CreatedAt  int64 // creationTimestamp as unix-millis, 0 if absent
	// RawJSON is the body, decompressed from raw_json (managedFields + kubectl
	// last-applied already stripped, Secret values redacted, at write time).
	RawJSON []byte
}

// Objects reads one kind's whole cached set (no window limit), ordered by
// (namespace, name). The watch names the plural resource but the table is keyed by
// kind, so the resource is translated through kind_catalog and the query then rides
// the objects_kind_ns_name index.
func (c *ClusterDB) Objects(ctx context.Context, apiVersion, resource string) ([]ObjectRow, error) {
	rows, err := c.readDB.QueryContext(ctx,
		`SELECT uid, api_version, kind, namespace, name, created_at, raw_json
		 FROM objects
		 WHERE api_version = ?
		   AND kind = (SELECT kind FROM kind_catalog WHERE api_version = ? AND resource = ?)
		 ORDER BY namespace, name`,
		apiVersion, apiVersion, resource)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ObjectRow
	for rows.Next() {
		var r ObjectRow
		var raw []byte
		if err := rows.Scan(&r.UID, &r.APIVersion, &r.Kind, &r.Namespace, &r.Name, &r.CreatedAt, &raw); err != nil {
			return nil, err
		}
		if r.RawJSON, err = DecompressRaw(raw); err != nil {
			return nil, fmt.Errorf("decompress raw_json for %s/%s: %w", r.Namespace, r.Name, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func openClusterDB(ctx context.Context, dataDir string, ref CacheRef) (*ClusterDB, error) {
	dir := clusterDir(dataDir, ref)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	dbPath := clusterDBPath(dataDir, ref)

	writeDB, err := openPool(dbPath, true)
	if err != nil {
		return nil, fmt.Errorf("open writer: %w", err)
	}

	if err := integrityCheck(ctx, writeDB); err != nil {
		writeDB.Close()
		if quarantineErr := quarantineCorrupt(dbPath); quarantineErr != nil {
			return nil, fmt.Errorf("integrity check failed (%v) and quarantine failed: %w", err, quarantineErr)
		}
		slog.Warn("clustercache: quarantined corrupt db", "id", ref.label(), "err", err)
		writeDB, err = openPool(dbPath, true)
		if err != nil {
			return nil, fmt.Errorf("reopen after quarantine: %w", err)
		}
	}

	// auto_vacuum=INCREMENTAL lets the janitor return freed pages to the OS. It can't
	// live in a migration (the mode is sticky once any table exists) nor be a plain
	// PRAGMA here (the pool open already wrote the WAL header), so set it and VACUUM to
	// rewrite the file — gated on the current mode so only a fresh DB pays for it.
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
		id:      ref.label(),
		path:    dbPath,
		writeDB: writeDB,
		readDB:  readDB,
		writes:  newWriteBus(),
		events:  newWriteBus(),
	}, nil
}

// ObjectsSubscribe returns a keyless coalesced ping channel woken by every object write,
// plus its unsubscribe func. Use in place of polling: writers notify after commit, and
// long-lived readers block here to know when to re-query.
func (c *ClusterDB) ObjectsSubscribe() (<-chan WriteWake, func()) { return c.writes.subscribe("") }

// ObjectsSubscribeResource is ObjectsSubscribe scoped to one (apiVersion, resource) —
// plus every keyless broadcast — so a per-kind watch isn't woken by unrelated writes.
//
// The routing key is the plural resource, NOT the Kind: it survives a CRD deleted and
// recreated under the same resource with a different Kind, where a Kind-keyed
// subscription would stay bound to the dead Kind and miss the replacement's writes.
// See docs/adr/2026-08-09-per-cluster-sqlite-cache.md.
func (c *ClusterDB) ObjectsSubscribeResource(apiVersion, resource string) (<-chan WriteWake, func()) {
	return c.writes.subscribe(objectKey(apiVersion, resource))
}

// ObjectsNotifyResource wakes the (apiVersion, resource) subscribers plus every keyless
// one, leaving other resources untouched. Per-kind writers call it after commit.
//
// This is the only object-write notify: there is no wake-everyone form, because every
// write path knows the resource it wrote and an unrelated resource's watch should not
// pay a re-read for it.
func (c *ClusterDB) ObjectsNotifyResource(apiVersion, resource string) {
	c.writes.notify(objectKey(apiVersion, resource))
}

// objectKey is the broker routing key for an object write — resource, not Kind; see
// ObjectsSubscribeResource.
func objectKey(apiVersion, resource string) string { return apiVersion + "/" + resource }

// EventsSubscribe is the events table's coalesced ping channel, on its own broker so a
// high-volume event burst never wakes the object re-reads.
func (c *ClusterDB) EventsSubscribe() (<-chan WriteWake, func()) { return c.events.subscribe("") }

// EventsNotify wakes every events subscriber; the events write paths call it after
// commit. Non-blocking. The events bus has one key — every subscriber is keyless —
// so eventsKey is a label, not a route.
func (c *ClusterDB) EventsNotify() { c.events.notify(eventsKey) }

// eventsKey is the single key the events bus publishes under; see EventsNotify.
const eventsKey = "events"

// openPool opens a *sql.DB via sqlitemigrate. writer=true caps MaxOpenConns at 1 so
// writes serialize at the pool instead of fighting at the SQLite layer.
func openPool(path string, writer bool) (*sql.DB, error) {
	maxConns := readerPoolSize
	if writer {
		maxConns = 1
	}
	return sqlitemigrate.OpenPool(path, maxConns)
}

func integrityCheck(ctx context.Context, db *sql.DB) error {
	// A healthy DB returns exactly one row, "ok"; anything else is damage.
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

// quarantineCorrupt renames a damaged DB and its sidecars aside so the next open starts
// fresh; the cluster re-syncs from upstream, so losing the cache is never fatal.
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

// startJanitor launches the TTL goroutine, independent of write activity so an idle
// cluster still gets trimmed.
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
		// An already-stopped janitor is never a timeout, however stale the deadline:
		// with both arms ready select picks at random, making a clean shutdown a coin
		// flip whose loser wedges the handle in the Manager's closing set.
		select {
		case <-c.janitorDone:
		default:
			select {
			case <-c.janitorDone:
			case <-ctx.Done():
				// The janitor may be mid-write, so bail without closing rather than
				// pull its connection. The handles leak, but a stuck goroutine over a
				// freed *sql.DB is worse.
				slog.Warn("clustercache: janitor did not stop before deadline; leaving DBs open", "id", c.id)
				return ctx.Err()
			}
		}
	}
	// Close subscriber channels so any blocked select returns.
	c.writes.close()
	c.events.close()
	var firstErr error
	if err := c.readDB.Close(); err != nil {
		firstErr = err
	}
	if err := c.writeDB.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
