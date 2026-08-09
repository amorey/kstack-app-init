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

// Package store owns the sidecar's per-cluster on-disk caches: one SQLite file per
// cache incarnation under the host-supplied data dir. It owns those files'
// lifecycle (open/migrate/quarantine/delete) plus a per-cluster janitor goroutine;
// it does NOT own syncing — the cluster package's sync workers write through the
// ClusterDB handed out, and this package owns only the reads beside them.
//
// Concurrency: SQLite runs in WAL mode so readers don't block a writer. Two *sql.DB
// handles share the same file — a single-connection writer pool (no SQLITE_BUSY
// storms) and a multi-connection reader pool — picked via Writer() / Reader().
//
// Cross-platform: the pure-Go modernc.org/sqlite driver, so no CGO toolchain.
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

	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// dbSuffixes are the file extensions that together make up one cluster's
// on-disk SQLite cache: the main DB plus its WAL and shared-memory sidecars.
// Any operation on a cache file (quarantine, delete, size) must cover all
// three.
var dbSuffixes = []string{"", "-wal", "-shm"}

// CacheRef identifies one on-disk cache incarnation by the beehive ObjectIDs of the
// parent Cluster (ClusterID — the directory) and its ClusterCache child (CacheID —
// the file). Both are AUTOINCREMENT ids, so they're path-safe (digits only) and
// never reused: a delete+recreate yields a strictly-greater CacheID and thus a fresh
// file, with no finalize-vs-recreate race. The fields are int64, not beehive.ObjectID,
// to keep this leaf package beehive-free; the cluster package converts at the boundary.
type CacheRef struct {
	ClusterID int64
	CacheID   int64
}

func (r CacheRef) valid() bool { return r.ClusterID > 0 && r.CacheID > 0 }

func (r CacheRef) label() string {
	return strconv.FormatInt(r.ClusterID, 10) + "/" + strconv.FormatInt(r.CacheID, 10)
}

// clusterDir is the per-cluster directory holding one cluster's cache
// incarnations: <dataDir>/clusters/<ClusterID>/.
func clusterDir(dataDir string, ref CacheRef) string {
	return filepath.Join(dataDir, "clusters", strconv.FormatInt(ref.ClusterID, 10))
}

// clusterDBPath is the on-disk path of a cache incarnation's main SQLite file,
// <dataDir>/clusters/<ClusterID>/<CacheID>.db. The sidecars live at this path +
// each dbSuffixes entry. Both segments are AUTOINCREMENT ids, so the path can
// never escape the clusters dir — there is no string hygiene to do.
func clusterDBPath(dataDir string, ref CacheRef) string {
	return filepath.Join(clusterDir(dataDir, ref), strconv.FormatInt(ref.CacheID, 10)+".db")
}

// readerPoolSize caps concurrent SQLite read connections per cluster.
// WAL allows many readers in parallel, but each open conn carries memory
// (page cache, prepared statements). Four is enough to keep a handful of
// GraphQL resolvers running without serializing.
const readerPoolSize = 4

// finishCloseTimeout bounds the retry of a close whose first attempt missed its deadline
// (see startFinishClose). Generous enough for a janitor sweep to finish, short enough that
// a permanently-wedged one doesn't hold a goroutine for the life of the process.
const finishCloseTimeout = 30 * time.Second

// Manager owns one ClusterDB per open cache incarnation, keyed by CacheID (the
// ClusterCache ObjectID — the precise incarnation; one is live per cluster at a
// time). Safe for concurrent use.
type Manager struct {
	dataDir string

	mu    sync.RWMutex
	dbs   map[int64]*ClusterDB
	close bool

	// dbWatchers holds one latest-value hub per watched CacheID, publishing the
	// cache's open handle as it changes (open/close/replace). Created lazily on
	// the first WatchDB for a CacheID and torn down when its last subscriber
	// cancels (refs → 0), so it doesn't accumulate a hub per cache incarnation.
	dbWatchers map[int64]*dbWatch
	// closing holds handles that have left dbs but whose pools may still be live — see
	// closingDB. An id here is neither open (Open must not hand it out, nor build a second
	// pool over the same file) nor gone (Close must be able to find it again).
	closing map[int64]*closingDB
	// deleting holds the caches DeleteCacheFiles is working on, for the whole close +
	// unlink sequence. Closing alone does not cover it: between the handle going away and
	// the files being removed, an Open would CREATE the file it is about to unlink and
	// register a handle onto an inode with no name — which every worker rebuilt afterwards
	// would then write into, silently, forever.
	deleting map[int64]bool
	// finishing marks the caches whose failed close is being retried, so the retries don't
	// stack up one per Open — see startFinishClose.
	finishing map[int64]bool
}

// closingDB is a handle that has left m.dbs but is not yet fully closed. done is non-nil
// while a shutdown attempt is running, so a second Close waits for it rather than tearing
// the same pools down twice; an entry that outlives its attempt is one whose shutdown
// failed, kept so the next Close re-waits on the SAME handle instead of finding nothing.
type closingDB struct {
	cdb  *ClusterDB
	done chan struct{}
	// err is the last attempt's outcome, written before done is closed so a waiter learns
	// whether the pools actually went away or the handle still needs finishing off.
	err error
}

// dbWatch is one CacheID's handle-change hub plus a refcount of live WatchDB
// subscribers (so the Manager can drop the hub once no one is watching).
type dbWatch struct {
	hub  *watch.Hub[*ClusterDB]
	tx   *watch.Sender[*ClusterDB]
	refs int
}

// NewManager returns a Manager rooted at dataDir. The clusters directory is
// created lazily on first Open.
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

// Open returns the ClusterDB for the given cluster ID, creating and
// migrating the on-disk SQLite file on first call (and starting its
// janitor). Subsequent calls return the same handle. After Shutdown the
// Manager refuses new opens.
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
	// A cache on its way down is neither: handing back the closing handle would let the
	// pools close under the caller mid-query, and opening a second one over the same
	// file would leave two writer pools racing for the .db a deletion is about to
	// remove. The caller retries — a reconcile requeues, and by then the close has
	// resolved either way.
	//
	// "Either way" needs someone to finish a close that FAILED, and nothing else ever
	// does: Close is reachable only through DeleteCacheFiles, whose own caller (a cache
	// clear) just surfaces the error. Left alone, one missed deadline wedged the cache
	// for the life of the process — every later Open refused while the pools stayed live
	// and every watcher had already been told the cache closed. So a stranded entry gets
	// its retry driver here, off the caller's goroutine: Open must not block on a
	// janitor that is wedged mid-write, which is the very thing that stranded it.
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

// startFinishClose retries a close that failed and left its handle stranded in m.closing,
// on its own goroutine so the caller that noticed the wedge doesn't wait on it. Called
// under m.mu; at most one retry runs per cache, since a wedged janitor typically stays
// wedged and a retry per Open would just pile up waiters on the same attempt.
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

// WatchDB streams a CacheID's open ClusterDB handle as it changes over the cache's
// lifecycle: the current handle (nil if not open) on subscribe, then a fresh value
// on open (nil→handle), close (handle→nil), or replace (a Clear-cache delete+reopen
// yields nil then the new handle). Latest-value semantics — a slow consumer
// converges on the current handle. The channel closes on Manager Shutdown.
// Long-lived readers (the dashboard's kind catalog watch) use it to rebind to a
// cache that opens after they subscribe, or is swapped under them, instead of
// binding once to Lookup.
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
		// Seed the hub with the current handle so a subscriber that arrives while
		// the cache is already open sees it immediately (nil when not open).
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

// publishDBLocked pushes db (nil = closed) to a CacheID's handle hub if anyone is
// watching it. No-op when there are no subscribers — a later WatchDB seeds itself
// from the current m.dbs entry. Must be called with m.mu held.
func (m *Manager) publishDBLocked(cacheID int64, db *ClusterDB) {
	if w := m.dbWatchers[cacheID]; w != nil {
		w.tx.Send(db) //nolint:errcheck // Send never blocks; a closed hub is a no-op
	}
}

// Lookup returns the ClusterDB for the given CacheID if it is currently open, or
// nil. Unlike Open it never creates or starts anything, so reader paths use it
// to reach an already-open handle.
func (m *Manager) Lookup(cacheID int64) *ClusterDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dbs[cacheID]
}

// CacheBytes returns the total on-disk size of a cluster's cache (main DB plus
// -wal/-shm sidecars) and whether any cache file exists. Cheap stat-only; works
// whether or not the cluster is currently open.
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

// ErrManagerShutDown is returned by every entry point once Shutdown has run: the manager
// no longer holds the handles, so it can neither hand one out nor promise anything about
// one. Close returns it rather than the "unknown id" nil, which is what makes
// DeleteCacheFiles abort instead of unlinking files behind a handle Shutdown is still
// closing.
var ErrManagerShutDown = errors.New("cache manager is shut down")

// DeleteCacheFiles closes the cluster (releasing the file handles — required on
// Windows) and removes its cache files (main DB + sidecars). Safe for an
// unknown/closed cluster. A later Open recreates a fresh, empty cache.
func (m *Manager) DeleteCacheFiles(ctx context.Context, ref CacheRef) error {
	return m.deleteCacheFilesWithHook(ctx, ref, nil)
}

// deleteCacheFilesWithHook is DeleteCacheFiles with a seam this package's tests use to
// observe the window between the close and the unlink — the one an Open must not slip into.
func (m *Manager) deleteCacheFilesWithHook(
	ctx context.Context, ref CacheRef, betweenCloseAndUnlink func(),
) error {
	if !ref.valid() {
		return fmt.Errorf("invalid cache ref %+v", ref)
	}
	// Hold the cache un-openable across the WHOLE sequence, not just the close. Open
	// creates the file when it is missing, so a reconcile landing between the close and the
	// unlink would recreate it — and the handle it registered would then be an open inode
	// with no name, which the restart hands to every rebuilt worker.
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
	// Reap the now-(maybe-)empty per-cluster directory. os.Remove only succeeds
	// on an empty dir, so a directory still holding another incarnation's file is
	// left intact; "not empty"/"not exist" are both expected and ignored.
	_ = os.Remove(clusterDir(m.dataDir, ref))
	return nil
}

// Close shuts down a single cache incarnation's DB and janitor goroutine,
// addressed by CacheID. Safe to call for an unknown id (returns nil).
//
// The handle is forgotten only once shutdown has SUCCEEDED. A shutdown that misses its
// deadline deliberately leaves the pools open (a janitor mid-write must not have its
// connection closed underneath it), so the handle is still live — and forgetting it would
// break the one caller that depends on this failing loudly: DeleteCacheFiles closes before
// removing files, its error keeps the cache's finalizer, and the retry would then find
// nothing registered, return nil, and os.Remove the .db out from under the live janitor and
// pools. Keeping it registered makes the retry re-wait on the same handle.
func (m *Manager) Close(ctx context.Context, cacheID int64) error {
	for {
		m.mu.Lock()
		if m.close {
			// Shutdown owns every handle now, including any this call would have found:
			// it takes them out of dbs/closing before closing them, so "not registered"
			// here does NOT mean "not live". Returning nil would tell DeleteCacheFiles the
			// cache is closed and let it unlink the .db while Shutdown's own attempt is
			// mid-write.
			m.mu.Unlock()
			return ErrManagerShutDown
		}
		entry, ok := m.closing[cacheID]
		switch {
		case ok && entry.done != nil:
			// Another Close is tearing this handle down. Wait for its attempt rather than
			// closing the same pools twice, then look again: it either finished the job
			// (nothing left to do) or failed, and this call becomes the retry.
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
			// Out of dbs BEFORE the shutdown, so no Open can hand this handle out while
			// its pools are going away; into closing, so this call's own retry can still
			// find it if the shutdown fails.
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
		// A failed shutdown deliberately leaves the pools open (a janitor mid-write must
		// not lose its connection), so the entry stays: the handle is still live, and
		// DeleteCacheFiles must not remove files behind it. Clearing done marks the attempt
		// over, so the next Close retries it instead of waiting forever.
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
	// Handles a per-cache Close took out of dbs but has not finished closing: their pools
	// may still be live, so a process-wide shutdown owes them one more attempt. Each
	// attempt's done channel is captured HERE, under the lock that guards it — Close nils
	// the field when its attempt ends.
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
	// Hard-close every handle hub so each WatchDB subscriber's stream ends.
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
			// An attempt owns the handle right now. We must not report the process shut
			// down while its pools may still be live, and we can no longer be reached
			// through m.closing, so this is the last chance to finish the job: wait for the
			// attempt, then take over if it failed.
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

	// janitorCancel/janitorDone bound the per-cluster TTL goroutine.
	// shutdown waits on janitorDone so a clean close doesn't leave a
	// goroutine writing into a closed DB.
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}

	// writes and events are two independent coalescing write-notify brokers.
	// writes backs ObjectsSubscribe/ObjectsNotify — the object-write ping. It drives
	// both the kind-catalog watch (kinds and their counts are a read projection of
	// object writes) and the objects watch — while events backs
	// EventsSubscribe/EventsNotify — the event-write ping that drives the events
	// watch. They are kept separate so an event burst wakes only event subscribers,
	// never the (unrelated) object-write re-reads.
	writes *notifyBroker
	events *notifyBroker
}

// notifyBroker is a coalescing pub/sub over cap-1 channels: each subscriber gets a
// channel that receives a non-blocking ping on every matching notify, additional pings
// while one is already buffered being coalesced (the subscriber re-queries and observes
// every change anyway). A ClusterDB holds one broker per independent write stream.
//
// Subscribers may register interest in one key (e.g. an object kind) or subscribe
// keyless. A keyed notify wakes the matching-key subscribers PLUS every keyless
// subscriber (a keyless subscriber, like the kind-catalog watch, must wake on any
// write); a keyless notify ("" key) broadcasts to every subscriber (the fallback for a
// write a caller can't attribute to one key — the discovery catalog rewrite, an orphan
// prune). The events broker uses only the keyless forms (one consumer, no routing).
type notifyBroker struct {
	mu     sync.Mutex
	subs   map[int]brokerSub
	nextID int
}

// brokerSub is one subscriber's channel plus the key it filters on ("" = keyless, wakes
// on every notify).
type brokerSub struct {
	ch  chan struct{}
	key string
}

func newNotifyBroker() *notifyBroker {
	return &notifyBroker{subs: make(map[int]brokerSub)}
}

func (b *notifyBroker) subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = brokerSub{ch: ch, key: key}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if existing, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(existing.ch)
		}
		b.mu.Unlock()
	}
}

// notify wakes the subscribers a write concerns: a keyed notify (non-empty key) wakes the
// subscribers registered for that key plus all keyless subscribers; a keyless notify ("")
// broadcasts to every subscriber. Coalescing cap-1 semantics — a ping to a full slot is a
// no-op (the buffered ping subsumes it).
func (b *notifyBroker) notify(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		// wake on a broadcast, a keyless subscriber, or a key match.
		if key == "" || s.key == "" || s.key == key {
			select {
			case s.ch <- struct{}{}:
			default:
			}
		}
	}
}

func (b *notifyBroker) close() {
	b.mu.Lock()
	for id, s := range b.subs {
		delete(b.subs, id)
		close(s.ch)
	}
	b.mu.Unlock()
}

// Reader returns the multi-connection pool for SELECTs. Lock-free under WAL.
func (c *ClusterDB) Reader() *sql.DB { return c.readDB }

// Writer returns the single-connection pool for mutations. Wrap batches in
// `BEGIN IMMEDIATE; ... COMMIT;` for a single fsync per batch.
func (c *ClusterDB) Writer() *sql.DB { return c.writeDB }

// ID returns a human-readable label for the cache incarnation this DB is bound
// to ("<ClusterID>/<CacheID>"), used in log lines.
func (c *ClusterDB) ID() string { return c.id }

// Path returns the on-disk SQLite file path. Exposed for diagnostics.
func (c *ClusterDB) Path() string { return c.path }

// NullIfEmpty stores an absent string as SQL NULL rather than "". The two sync stores
// share it because they share the convention: a reading a store could not produce is a
// different fact from an empty one, and the columns are queried with IS NULL.
func NullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// EnsureKindCatalog registers one kind in the cache's kind_catalog and wakes the
// kind-catalog watch. Each synced kind's worker calls it on start; the upsert makes that a
// no-op after the first, and refreshes the descriptive columns if the kind's shape moved
// (a CRD re-registered under a different plural or scope).
//
// It lives here because kind_catalog is this package's table, and it is what makes a kind
// readable at all: Objects translates the plural resource a watch subscribes on back to the
// Kind the objects table is keyed by through this row, and Kinds is a kind_catalog LEFT
// JOIN over the trigger-maintained kind_counts — so a kind with rows and no catalog entry
// reads as empty and shows no count. Row.Count is ignored (the triggers own it).
//
// The ping is keyless: a catalog entry is news the dashboard nav's watch reads, and no
// object write need follow it (an empty kind stays empty).
//
// A row naming the same plural under a DIFFERENT Kind is dropped first, so the unique index
// on (api_version, resource) cannot refuse the insert. That row means a CRD's Kind was
// renamed while the sidecar was down; dropping it here only keeps the write legal, and the
// old Kind's objects, edges and cookie are purged by the caller that knows those tables (see
// objectsync's forgetSupersededKind). Both statements ride one transaction: a delete that
// landed without its insert would leave the kind unreadable.
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
	cdb.ObjectsNotify()
	return nil
}

// KindRow is one entry in the cache's kind_catalog: a kind the cluster's
// API server advertises, recorded at sync time from /apis discovery.
type KindRow struct {
	// APIVersion is the group/version, e.g. "apps/v1" or "v1" for the core group.
	APIVersion string
	// Kind is the Kind name, e.g. "Deployment".
	Kind string
	// Resource is the plural lowercase URL form, e.g. "deployments".
	Resource string
	// Scope is "Namespaced" or "Cluster".
	Scope string
	// IsCRD is true when the kind is backed by a CustomResourceDefinition.
	IsCRD bool
	// Count is the number of objects of this kind currently cached (0 for a kind
	// the API server advertises but has no cached instances of).
	Count int
}

// Kinds reads the cache's discovered kind catalog: one row per kind the
// cluster's API server advertises (built-ins + CRDs), ordered for stable display.
// Each row's Count is the number of cached objects of that kind, read from the
// trigger-maintained kind_counts aggregate (a point LEFT JOIN keyed by
// api_version+kind, so an advertised-but-empty kind counts 0) — O(kinds), never a
// scan of the objects table.
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

// EventRow is one cached Kubernetes Event, read from the events table for display.
// It is the read projection (a subset of the stored columns, involved-object
// identity flattened) that backs the dashboard's events table — the compressed
// raw_json body is deliberately not read here.
type EventRow struct {
	// UID is the Event's own object UID (the stable identity a watch keys on).
	UID string
	// Type is the event severity: "Normal" or "Warning" (empty if unset).
	Type string
	// Reason is the CamelCase machine reason, e.g. "BackOff" (empty if unset).
	Reason string
	// Message is the human-readable detail (empty if unset).
	Message string
	// Count is how many times the event has fired (coalesced series count; >= 1).
	Count int
	// FirstSeen/LastSeen are unix-millis timestamps, 0 when the source carried none.
	FirstSeen int64
	LastSeen  int64
	// InvolvedKind/InvolvedNS/InvolvedName identify the object the event is about
	// (any may be empty — a name-only reference carries no namespace, etc.).
	InvolvedKind string
	InvolvedNS   string
	InvolvedName string
}

// Events reads the most recent cached events, newest first (ordered by last_seen,
// riding the events_last_seen index), bounded by limit. A non-positive limit is
// treated as defaultEventsLimit. Empty until the Event sync has populated the
// events table.
//
// uid breaks ties, and that is not cosmetic. lastTimestamp has one-second resolution, so a
// busy cluster produces many rows sharing a last_seen — enough of them to straddle the
// limit. Without a second key SQLite decides which of the tied rows falls inside the window
// by rowid, and a relist prunes and re-inserts every row, so the same events come back with
// different rowids: the delta watch then sees rows leave and re-enter a window nothing
// actually moved in, and emits phantom Deleted/Added frames for them.
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

// defaultEventsLimit bounds an unbounded Events read. The events watch diffs a
// fixed window of the newest events, so this doubles as that window size.
const defaultEventsLimit = 500

// ObjectRow is one cached Kubernetes object read from the objects table (any kind),
// backing the dashboard's generic per-kind object tables. It carries the universal
// identity plus the object's full native body (RawJSON) — the frontend derives
// kind-specific columns from the body client-side.
type ObjectRow struct {
	// UID is the object's UID (the stable identity a watch keys on).
	UID string
	// APIVersion is the group/version, e.g. "apps/v1".
	APIVersion string
	// Kind is the Kind name, e.g. "Deployment".
	Kind string
	// Namespace is the object's namespace (empty for a cluster-scoped kind).
	Namespace string
	// Name is the object's name.
	Name string
	// CreatedAt is the object's creationTimestamp as unix-millis, 0 when the
	// source object carried none.
	CreatedAt int64
	// RawJSON is the object's full native body as JSON, decompressed from the
	// zlib-compressed raw_json column (managedFields + the kubectl last-applied
	// annotation already stripped at write time).
	RawJSON []byte
}

// Objects reads one kind's cached objects, filtered by (api_version, resource) and
// ordered by (namespace, name) for a stable table. The watch is keyed by the plural
// resource, but the objects table is keyed by kind, so the resource is translated to
// its kind through kind_catalog (a point subquery) and the query then rides the
// objects_kind_ns_name index. Unlike Events there is no window limit — a kind's whole
// cached set is returned. Each row's raw_json is decompressed into RawJSON (the write
// path always compresses). Empty until that kind's sync has populated it.
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

	// auto_vacuum=INCREMENTAL lets the janitor's PRAGMA incremental_vacuum return
	// freed pages to the OS. It can't live in a migration (the mode is sticky once
	// any table exists, and the runner creates schema_migrations first) nor be a
	// plain PRAGMA here (opening the pool already wrote the WAL header with
	// auto_vacuum=0), so we set the mode and VACUUM to rewrite the file. Gate on the
	// current mode: only a fresh DB (mode 0) needs the one-time conversion; skipping
	// an already-converted DB (mode 2) avoids rewriting the whole file on every reopen.
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
		writes:  newNotifyBroker(),
		events:  newNotifyBroker(),
	}, nil
}

// ObjectsSubscribe returns a channel that receives a non-blocking signal after
// every ObjectsNotify call. The channel has capacity 1 — additional pings while one
// is already buffered are coalesced (the subscriber will re-query and observe every
// change anyway). The returned cancel func unregisters the subscriber.
//
// Use this in place of polling: object-write paths call ObjectsNotify after commit, and
// long-lived readers (the kind-catalog and cache-stats subscriptions) block on the channel
// to know when to re-run their query.
func (c *ClusterDB) ObjectsSubscribe() (<-chan struct{}, func()) { return c.writes.subscribe("") }

// ObjectsSubscribeResource is ObjectsSubscribe scoped to one resource: the returned
// channel receives a signal only for an ObjectsNotifyResource of the same
// (apiVersion, resource) — plus every keyless ObjectsNotify broadcast — so a per-kind
// objects watch isn't woken (and doesn't re-read) for an unrelated resource's writes. The
// returned cancel func unregisters the subscriber.
//
// The routing key is the plural resource, NOT the Kind, because that is the identity the
// objects watch is opened on and it is stable across a CRD remap: a CRD deleted and
// recreated with the same (apiVersion, resource) but a different Kind keeps this key, so a
// subscription never goes stale against the replacement worker's writes (which notify by
// the same resource). Keying by Kind instead would leave the subscription bound to the
// dead Kind and miss those writes.
func (c *ClusterDB) ObjectsSubscribeResource(apiVersion, resource string) (<-chan struct{}, func()) {
	return c.writes.subscribe(objectKey(apiVersion, resource))
}

// ObjectsNotify wakes every active object-write subscriber (keyed and keyless). It's the
// keyless broadcast — the fallback for a write that isn't attributable to one resource (a
// discovery catalog rewrite, an orphan-kind prune). Non-blocking: if a subscriber's
// channel slot is full, the existing buffered ping subsumes this one.
func (c *ClusterDB) ObjectsNotify() { c.writes.notify("") }

// ObjectsNotifyResource wakes the object-write subscribers registered for
// (apiVersion, resource) plus every keyless subscriber (the kind-catalog watch), leaving
// other-resource subscribers untouched. The per-kind object writers (which know their GVR)
// call this after committing so a per-kind objects watch only wakes on its own resource's
// writes. Non-blocking.
func (c *ClusterDB) ObjectsNotifyResource(apiVersion, resource string) {
	c.writes.notify(objectKey(apiVersion, resource))
}

// objectKey is the notify-broker routing key for an object write: (apiVersion, resource) —
// the plural resource the objects watch names, chosen over the Kind because it survives a
// CRD Kind remap under a stable resource (see ObjectsSubscribeResource).
func objectKey(apiVersion, resource string) string { return apiVersion + "/" + resource }

// EventsSubscribe is Subscribe for the events table: a channel that receives a
// coalesced signal after every EventsNotify. Kept separate from Subscribe so an
// event burst (which is high-volume) wakes only the events watch, never the
// kind-catalog re-read. The returned cancel func unregisters the subscriber.
func (c *ClusterDB) EventsSubscribe() (<-chan struct{}, func()) { return c.events.subscribe("") }

// EventsNotify wakes every active events subscriber. Non-blocking, like Notify.
// The events-table write paths (incremental upsert/delete and the relist prune)
// call it after committing.
func (c *ClusterDB) EventsNotify() { c.events.notify("") }

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
		// A janitor that has ALREADY stopped is not a timeout, however long the caller's
		// deadline has been gone: with both arms ready select picks at random, so an
		// expired context turned a clean shutdown into a coin flip — and the loser left the
		// pools open and the handle wedged in the Manager's closing set.
		select {
		case <-c.janitorDone:
		default:
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
