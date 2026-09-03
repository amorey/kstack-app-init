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

// Package kubestore is the on-disk cache behind every ClusterCache: one SQLite file
// per cache under the manager's directory, holding the mirrored objects, events, and
// the sync bookkeeping the workers resume from. The schema is
// migrations/0001_init.sql; the per-kind resourceVersion cookie lives in its
// cluster_meta bag rather than a table of its own.
//
// The Manager is the only way to a Store. The kubesync writers and the boundary's
// readers must share one file per cache — the change bus is in-memory state on it —
// and Clear has to close every claim's file before deleting it: deleting under an open
// handle does not fail on POSIX, it silently forks the world, the old handle writing to
// the unlinked inode while a fresh open starts empty. Sequencing that is only possible
// where the claims are held.
// → docs/adr/2026-08-26-cache-store-per-cache.md,
// docs/adr/2026-08-26-store-change-ping-bus.md.
package kubestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

// Manager owns the caches directory and hands out refcounted Stores, keyed by the
// cache's beehive ObjectID — opaque here, and what names the file, so a beehive name's
// arbitrary text never reaches the filesystem.
type Manager struct {
	// dir is the caches directory; each cache's file is "<cacheID>.db" inside it.
	dir string
	// retention is what each open file's janitor sweeps to.
	retention Retention

	// mu guards entries and removed, and is what serializes Clear against
	// OpenOrCreate/Release: a swap must never race a Store resolving its file.
	mu      sync.Mutex
	entries map[int64]*entry
	// opens says a cache's file has come into existence, keyed by cache. A read never
	// creates one, so a watch opened before the first worker or sweep has nothing to bind
	// to and would otherwise wait out a poll.
	opens *conflate.Hub[int64, struct{}]
	// sizeLimitHub says a cache's size verdict has changed, keyed by cache. The manager's
	// rather than a file's: the reader watches every cache, and the file a verdict is about
	// is the one a Clear replaces.
	sizeLimitHub *conflate.Hub[int64, struct{}]
	// deleteFiles is the unlink step, and closeFile the close both clears go through:
	// seams, so a white-box test can drive a clear whose files will not go or whose
	// database will not close.
	deleteFiles func(path string) error
	closeFile   func(f *file) error
	// removed is the caches Remove has retired. A beehive ObjectID is never reused, so
	// refusing one forever is the whole rule — and it is what stops a straggler pass,
	// holding a view of the cache from before its teardown, from opening a fresh file
	// nothing will ever name again.
	removed map[int64]bool
}

// entry is one cache's open file and the claims on it. The file pointer is what Clear
// swaps, and a Store reads it out of the entry it claimed — which is what makes the swap
// reach live claims while a replacement entry stays theirs alone. A nil file is one
// closed for good, so a Store on a retired entry answers ErrClosed rather than reaching
// a closed *sql.DB or another claim's file.
type entry struct {
	file *file
	refs int
}

// OpenSubscription reports that a cache's file has come into existence. The value carries
// nothing — the key is the whole news, and the reader answers it by binding.
type OpenSubscription = *conflate.Receiver[int64, struct{}]

// SizeLimitNews carries the caches whose size verdict has changed. The key is the whole
// news; the reader answers it by calling Stats.
type SizeLimitNews = *conflate.Receiver[int64, struct{}]

var (
	// ErrRemoved is what OpenOrCreate answers for a cache Remove retired.
	ErrRemoved = errors.New("cache store removed")
	// ErrClosed is what a Store answers once the file under it is gone — a Remove, or a
	// Clear that could not reopen. The claim is still the caller's to Release.
	ErrClosed = errors.New("cache store is closed")
)

// NewManager returns a Manager rooted at dir, whose files hold what ret says. Nothing is
// opened until OpenOrCreate. A zero Interval runs no janitor, which is what a test about
// anything but sweeping opens with.
func NewManager(dir string, ret Retention) *Manager {
	return newManagerWithOptions(dir, withRetention(ret))
}

// withRetention is what NewManager passes; the seams below are the test-only ones.
func withRetention(ret Retention) option {
	return func(m *Manager) { m.retention = ret }
}

// option is a test seam, reachable only from white-box tests.
type option func(*Manager)

// withDeleteFiles substitutes the unlink both clears go through.
func withDeleteFiles(f func(path string) error) option {
	return func(m *Manager) { m.deleteFiles = f }
}

// withCloseFile substitutes the close both clears go through.
func withCloseFile(fn func(f *file) error) option {
	return func(m *Manager) { m.closeFile = fn }
}

// newManagerWithOptions is NewManager plus the seams.
func newManagerWithOptions(dir string, opts ...option) *Manager {
	m := &Manager{
		dir:          dir,
		entries:      map[int64]*entry{},
		opens:        conflate.New[int64, struct{}](),
		sizeLimitHub: conflate.New[int64, struct{}](),
		deleteFiles:  deleteStoreFiles,
		closeFile:    (*file).close,
		removed:      map[int64]bool{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// OpenOrCreate claims cacheID's store — creating the directory, the file, and the
// schema on first touch — or joins the open one. Release the claim.
//
// The name says the dangerous half out loud: this is the writers' door. A read goes
// through OpenExisting, which never creates.
func (m *Manager) OpenOrCreate(cacheID int64) (*Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.openOrCreateLocked(cacheID)
}

// openOrCreateLocked is OpenOrCreate for a caller already holding the lock.
func (m *Manager) openOrCreateLocked(cacheID int64) (*Store, error) {
	if m.removed[cacheID] {
		return nil, fmt.Errorf("open cache %d: %w", cacheID, ErrRemoved)
	}
	e, ok := m.entries[cacheID]
	if !ok {
		f, err := openFile(m.path(cacheID), cacheID, m.retention, m.sizeLimitHub.Sender())
		if err != nil {
			return nil, err
		}
		e = &entry{file: f}
		m.entries[cacheID] = e
		// Under the lock, so a waiter that registered before the entry landed cannot miss
		// it. Send never blocks and reaches no caller's code, so holding mu across it is
		// safe.
		_ = m.opens.Sender().Send(cacheID, struct{}{})
	}
	e.refs++
	return &Store{m: m, cacheID: cacheID, e: e}, nil
}

// WatchOpen reports when cacheID's file next comes into existence, for a reader that found
// none. **Close it when done.** A cache that already has a file never fires: the caller
// tries OpenExisting first, and this exists only for the gap before anything created one.
//
// Keyed at enqueue, so a cache opening never wakes a watch on another one — the same shape
// as Store.Subscribe, over the manager's own bus rather than a file's.
func (m *Manager) WatchOpen(cacheID int64) OpenSubscription {
	return m.opens.Receiver(m.opens.WithKey(cacheID))
}

// WatchSizeLimitNews reports every cache that crossed its size limit or came back under
// it. **Close it when done.** Edge-triggered: a reader hears that the verdict changed, and
// reads what it is now from Stats.
func (m *Manager) WatchSizeLimitNews() SizeLimitNews { return m.sizeLimitHub.Receiver() }

// Subscribe returns the change feed for a cache someone already holds open, narrowed to
// keys — or ok false, since the feed lives on the open file and there is none. **It takes
// no claim**, so a caller cannot keep a cache alive by watching it.
//
// The condition costs nothing where it belongs: an idle cache has no writer, so it cannot
// change, so there is nothing for a feed to carry. A caller that must read an idle cache's
// contents claims it through OpenExisting instead.
func (m *Manager) Subscribe(cacheID int64, keys ...string) (Subscription, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, held := m.entries[cacheID]
	if !held || e.file == nil {
		return nil, false
	}
	return e.file.subscribe(keys...), true
}

// OpenExisting claims a cache that already has a file, or reports that none does. **It
// never creates one**: the callers are reading a cache, clearing a kind, or measuring what
// is there, and a fresh empty database — schema and sidecars and all — is exactly what none
// of them wants left behind.
//
// **The claim is bound to the file it opened**, so a Clear's swap answers ErrClosed rather
// than silently redirecting to the fresh empty one. Every caller here wants that: a reader
// would otherwise report every row it holds as gone, and a per-kind clear would delete from
// a file its transaction never targeted.
//
// A cache removed between the check and the claim answers ErrRemoved, which the caller
// retries; its file is going with the record either way.
func (m *Manager) OpenExisting(cacheID int64) (*Store, bool, error) {
	// One critical section: a clear holds the lock across delete → create → migrate, and
	// a look outside it would find no file mid-window and report a cache that is very
	// much there as gone — a clear of one kind reporting success for nothing done.
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, err := os.Stat(m.path(cacheID)); err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open cache %d: %w", cacheID, err)
	}
	store, err := m.openOrCreateLocked(cacheID)
	if err != nil {
		return nil, false, err
	}
	store.bound = store.e.file
	return store, true, nil
}

// Clear wipes cacheID's store: close the open file if any, delete it (the -wal/-shm
// sidecars too), and reopen fresh for the claims still held. Callers stop the cache's
// workers first — the manager sequences claims, not writers.
func (m *Manager) Clear(cacheID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := m.path(cacheID)
	e, held := m.entries[cacheID]
	if held {
		if err := m.closeFile(e.file); err != nil {
			// The file is unusable whatever the close reported, so the entry is retired
			// rather than left installed: a claim on it must answer ErrClosed instead of
			// reaching a dead database, and the next open starts the cache fresh.
			e.file = nil
			delete(m.entries, cacheID)
			return fmt.Errorf("clear: close store: %w", err)
		}
	}
	deleteErr := m.deleteFiles(path)

	// Live claims still hold this entry — swap in a fresh file rather than dropping it,
	// or every one of them would answer ErrClosed for a cache that is meant to stay
	// usable. On a failed delete too: the caller retries the clear, and the cache has to
	// keep working until it lands.
	if held {
		f, err := openFile(path, cacheID, m.retention, m.sizeLimitHub.Sender())
		if err != nil {
			// Nothing usable to swap in, so retire the entry: a later OpenOrCreate opens
			// the cache fresh, and the claims left on this one answer ErrClosed rather
			// than reaching a file closed for good.
			e.file = nil
			delete(m.entries, cacheID)
			return errors.Join(deleteErr, fmt.Errorf("clear: reopen: %w", err))
		}
		e.file = f
	}
	if deleteErr != nil {
		return fmt.Errorf("clear: delete files: %w", deleteErr)
	}
	return nil
}

// Remove retires cacheID's store for good: close it if open, delete the files, and drop
// the entry — nothing reopens, so a claim still out answers ErrClosed and a later
// OpenOrCreate is refused with ErrRemoved. For a cache that is going away with the
// record it belongs to; Clear is the one that leaves a usable empty store behind. A
// cache that was never opened is not an error, and one whose cleanup failed stays
// retired — the caller retries, and an open in between would recreate exactly what is
// going. Callers stop the cache's workers first, the way they do for Clear.
func (m *Manager) Remove(cacheID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Retiring is the decision, not the outcome: a failed close or unlink is retried by
	// the caller's next pass, and until it lands nothing may open the file again.
	m.removed[cacheID] = true

	if e, held := m.entries[cacheID]; held {
		f := e.file
		e.file = nil
		delete(m.entries, cacheID)
		if err := m.closeFile(f); err != nil {
			return fmt.Errorf("remove: close store: %w", err)
		}
	}
	if err := m.deleteFiles(m.path(cacheID)); err != nil {
		return fmt.Errorf("remove: delete files: %w", err)
	}
	return nil
}

// Stats measures one cache: its files, and the tally of what is in it. The -wal/-shm sidecars are
// measured beside the main file rather than folded into it — a bare stat of the main file swings
// with checkpoint timing.
//
// It takes no claim. The counts come through whatever is already open, and otherwise by
// opening the file read-only: a cache whose workers are all stopped — a paused cluster —
// still holds its rows, and a gauge answering zero beside a megabyte-sized file would
// read as data loss. Read-only is what keeps this a read: the file is never created.
func (m *Manager) Stats(ctx context.Context, cacheID int64) (Stats, error) {
	// The whole measurement in one critical section, the file included: a clear holds
	// the lock across close → unlink → create → migrate, and a stat taken inside that
	// window answers that the cache does not exist — which is what the gauge in the very
	// view holding the Clear button would render as data loss.
	m.mu.Lock()
	defer m.mu.Unlock()

	path := m.path(cacheID)
	usage, err := statDiskUsage(path)
	if os.IsNotExist(err) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	out := Stats{
		Exists: true, DBBytes: usage.db, WALBytes: usage.wal, SHMBytes: usage.shm,
		SizeLimitBytes: m.retention.SizeLimit,
	}
	if e, ok := m.entries[cacheID]; ok && e.file != nil {
		out.OverSizeLimit = e.file.sizeVerdict.Load() == sizeOver
	}

	counts, err := m.countsLocked(ctx, path, cacheID)
	if err != nil {
		return Stats{}, err
	}
	out.Counts = counts
	return out, nil
}

// diskUsage is the size of the three files a cache is: the database and its -wal/-shm
// sidecars. The one measurement of a cache's size — Stats reports it and the janitor judges
// it, so the gauge and the size limit can never disagree.
type diskUsage struct{ db, wal, shm int64 }

func (u diskUsage) total() int64 { return u.db + u.wal + u.shm }

// statDiskUsage measures the cache at path. The main file's error comes back as is, for
// the caller to interpret: Stats reads os.IsNotExist as "not cached", and zeroes for it
// would report a full cache as absent.
func statDiskUsage(path string) (diskUsage, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return diskUsage{}, err
	}
	u := diskUsage{db: fi.Size()}
	if u.wal, err = sidecarSize(path + "-wal"); err != nil {
		return diskUsage{}, err
	}
	if u.shm, err = sidecarSize(path + "-shm"); err != nil {
		return diskUsage{}, err
	}
	return u, nil
}

// sidecarSize is the size of a -wal or -shm file, zero when it is absent: the pair exists
// only while the file is open. Only absence is zero. Any other failure is returned, or a
// sidecar that will not stat would undercount the cache and publish a release nothing
// released.
func sidecarSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// counts reads the per-kind tallies without taking a claim: through the open file when
// one is, and otherwise through a read-only open.
//
// A failed read through the open file falls through rather than failing the
// measurement. A Clear closes the file whatever holds it — a claim would narrow this
// window, never close it — so a read that resolved one a moment earlier can find it
// closed mid-query, and the answer is on disk either way. The fallback re-reads
// whatever is there now; a genuine failure surfaces from it instead.
func (m *Manager) countsLocked(ctx context.Context, path string, cacheID int64) (Counts, error) {
	// The entry's file directly, not through a Store: a Store resolves its file under
	// this same lock, which this call already holds.
	if e, held := m.entries[cacheID]; held && e.file != nil {
		if counts, err := countKinds(ctx, e.file.db); err == nil {
			return counts, nil
		}
	}
	return m.countsFromDiskLocked(ctx, path)
}

// countsFromDisk opens the file read-only and reads it, **under the manager's lock**.
// Clear holds that lock across close → unlink → create → migrate, and for the middle of
// it the file exists with no schema: a read slipping in there fails on a missing table
// and, through the gauge above it, ends a subscription the user is watching while
// clearing. The lock costs one point read on an indexed table, and only for a cache
// nothing has open.
func (m *Manager) countsFromDiskLocked(ctx context.Context, path string) (Counts, error) {
	db, err := openReadOnly(path)
	if err != nil {
		return Counts{}, err
	}
	defer db.Close() //nolint:errcheck // read-only, nothing to flush

	counts, err := countKinds(ctx, db)
	if err != nil {
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			// Removed between the stat above and this read: gone, not broken. SQLite
			// reports a missing file as its own error, so the file is what says so.
			return Counts{}, nil
		}
		return Counts{}, err
	}
	return counts, nil
}

// openReadOnly opens an existing cache file for reading. mode=ro is what makes this
// safe to call from a read: SQLite refuses rather than creating, so no reader can
// bring a removed cache back.
func openReadOnly(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open read-only: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// path is cacheID's db file path within the manager's directory.
func (m *Manager) path(cacheID int64) string {
	return filepath.Join(m.dir, strconv.FormatInt(cacheID, 10)+".db")
}

// deleteStoreFiles removes the main db file and its -wal/-shm sidecars; a file that
// never existed is not an error.
func deleteStoreFiles(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// openFile opens the writer pool at path, applies the schema, and starts the file's janitor.
//
// The janitor starts HERE rather than at either call site: Clear reopens a fresh file for the
// claims still held, and a start at the open alone would leave a cleared cache without a
// sweeper — the cache most likely to need one, since a clear is what frees the pages.
func openFile(
	path string, cacheID int64, ret Retention, sizeLimitSender *conflate.Sender[int64, struct{}],
) (*file, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	db, err := sqlitemigrate.OpenPool(path, 1)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}

	ctx := context.Background()
	const autoVacuumIncremental = 2
	var mode int
	if err := db.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&mode); err != nil {
		db.Close()
		return nil, fmt.Errorf("read auto_vacuum: %w", err)
	}
	// A file this build creates is already INCREMENTAL — the DSN sets it. This is for one
	// written by a build that predates that: SQLite ignores the pragma once a table exists,
	// so without the rewrite the janitor's incremental_vacuum is a no-op on it forever.
	if mode != autoVacuumIncremental {
		if _, err := db.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL; VACUUM;`); err != nil {
			db.Close()
			return nil, fmt.Errorf("set auto_vacuum: %w", err)
		}
	}

	if _, err := sqlitemigrate.Apply(ctx, db, migrationsFS, "migrations"); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	// After the migrations, so a reader never races the schema onto a fresh file.
	readDB, err := sqlitemigrate.OpenReadPool(path, readerPoolSize)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open reader pool: %w", err)
	}
	// After both pools are open, so a failure here closes both.
	writeStmts, err := prepareStatements(ctx, db, true)
	if err != nil {
		db.Close()
		readDB.Close()
		return nil, err
	}
	readStmts, err := prepareStatements(ctx, readDB, false)
	if err != nil {
		closeStatements(writeStmts)
		db.Close()
		readDB.Close()
		return nil, err
	}

	f := newFile(path, cacheID, db, readDB, writeStmts, readStmts, sizeLimitSender)
	f.startJanitor(ret)
	return f, nil
}

// Start is the lifecycle shape; the manager has no background work.
func (m *Manager) Start(ctx context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close closes every open file. Claims still out answer ErrClosed after it; Close runs
// only after everything that writes has stopped.
func (m *Manager) Close() error {
	// Ends every waiter, including one on a cache nothing ever opened — the only thing
	// that would otherwise hold it is its own context.
	m.opens.Close()
	m.sizeLimitHub.Close()

	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for id, e := range m.entries {
		if e.file != nil {
			errs = append(errs, m.closeFile(e.file))
			e.file = nil
		}
		delete(m.entries, id)
	}
	return errors.Join(errs...)
}

// Stats is one cache's measurement: its footprint on disk, and what it holds.
type Stats struct {
	Exists bool
	// The three files a cache is, measured apart. DBBytes alone is not what the cache costs:
	// it does not grow until a checkpoint lands, so it reads low — and stale — while a sync
	// fills the WAL. Apart because the split is the only way to see checkpointing fall behind.
	DBBytes  int64
	WALBytes int64
	SHMBytes int64
	// OverSizeLimit is the janitor's last verdict on this file, never a comparison made
	// here: a sweep checkpoints a WAL-heavy file before deciding and this measurement does
	// not, so recomputing would disagree with the pause that hangs off the verdict. False
	// when the cache does not exist, when nobody has it open (a closed cache cannot grow),
	// and when the manager runs no janitor because its Interval is zero.
	OverSizeLimit bool
	// SizeLimitBytes is the limit itself, 0 when unbounded, so a client can render how
	// close a cache is without hardcoding this package's default.
	SizeLimitBytes int64
	Counts
}

// Bytes is what the cache costs on disk. Derived rather than stored, so it cannot drift from
// the parts.
func (s Stats) Bytes() int64 { return s.DBBytes + s.WALBytes + s.SHMBytes }
