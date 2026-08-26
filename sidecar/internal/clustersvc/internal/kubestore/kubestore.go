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
// per cache under the registry's directory, holding the mirrored objects, events,
// and the sync bookkeeping the workers resume from. The schema is
// migrations/0001_init.sql; the per-kind resourceVersion cookie lives in its
// cluster_meta bag rather than a table of its own.
//
// The Registry is the only way to a Store. The kubesync writers and the boundary's
// readers must share one handle per cache — the change broker to come is in-memory
// state on it — and Clear has to close every handle before deleting the files:
// deleting under an open handle does not fail on POSIX, it silently forks the world,
// the old handle writing to the unlinked inode while a fresh open starts empty.
// Sequencing that is only possible where the handles are held.
// → docs/specs/cached-resource-sync.md.
package kubestore

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// The one kind whose rows live in events rather than objects: core v1 events. Any
// group may serve a Kind called "Event" — a CRD's rows are ordinary objects — so the
// events table is identified by api version and plural, never by the Kind name.
const coreEventsAPIVersion, coreEventsResource = "v1", "events"

// Registry hands out refcounted handles on each cache's store, keyed by the cache's
// beehive ObjectID — opaque here, and what names the file, so a beehive name's
// arbitrary text never reaches the filesystem.
type Registry struct {
	// dir is the caches directory; each cache's file is "<cacheID>.db" inside it.
	dir string

	// mu guards entries and deleted, and is what serializes Clear against
	// Acquire/Release: a swap must never race a handle resolving its store.
	mu      sync.Mutex
	entries map[int64]*entry
	// deleteFiles is the unlink step, a seam so a white-box test can drive a clear
	// whose files will not go.
	deleteFiles func(path string) error
	// deleted is the caches Delete has retired. A beehive ObjectID is never reused,
	// so refusing one forever is the whole rule — and it is what stops a straggler
	// pass, holding a view of the cache from before its teardown, from opening a
	// fresh file nothing will ever name again.
	deleted map[int64]bool
}

// entry is one cache's open store and the handles on it. The store pointer is what
// Clear swaps; handles read it through the registry rather than holding it, which is
// what makes the swap safe under live handles.
type entry struct {
	store *Store
	refs  int
}

// ErrDeleted is what Acquire answers for a cache whose store Delete retired.
var ErrDeleted = errors.New("cache store deleted")

// NewRegistry returns a Registry rooted at dir. Nothing is opened until Acquire.
func NewRegistry(dir string) *Registry {
	return newRegistryWithOptions(dir)
}

// option is a test seam, reachable only from white-box tests.
type option func(*Registry)

// withDeleteFiles substitutes the unlink both clears go through.
func withDeleteFiles(f func(path string) error) option {
	return func(r *Registry) { r.deleteFiles = f }
}

// newRegistryWithOptions is NewRegistry plus the seams.
func newRegistryWithOptions(dir string, opts ...option) *Registry {
	r := &Registry{
		dir:         dir,
		entries:     map[int64]*entry{},
		deleteFiles: deleteStoreFiles,
		deleted:     map[int64]bool{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Acquire opens cacheID's store — creating the directory, the file, and the schema
// on first touch — or joins the open one. Release the handle.
func (r *Registry) Acquire(cacheID int64) (*Handle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.deleted[cacheID] {
		return nil, fmt.Errorf("acquire cache %d: %w", cacheID, ErrDeleted)
	}
	e, ok := r.entries[cacheID]
	if !ok {
		store, err := openStore(r.path(cacheID))
		if err != nil {
			return nil, err
		}
		e = &entry{store: store}
		r.entries[cacheID] = e
	}
	e.refs++
	return &Handle{r: r, cacheID: cacheID}, nil
}

// Clear wipes cacheID's store: close the open store if any, delete the files (the
// -wal/-shm sidecars too), and reopen fresh for the handles still held. Callers stop
// the cache's workers first — the registry sequences handles, not writers.
func (r *Registry) Clear(cacheID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	path := r.path(cacheID)
	e, held := r.entries[cacheID]
	if held {
		if err := e.store.db.Close(); err != nil {
			return fmt.Errorf("clear: close store: %w", err)
		}
	}
	deleteErr := r.deleteFiles(path)

	// Live handles still hold this entry — swap in a fresh store rather than dropping
	// it, or Handle.Store would resolve to a closed *sql.DB. On a failed delete too:
	// the caller retries the clear, and the cache has to stay usable until it lands.
	if held {
		store, err := openStore(path)
		if err != nil {
			// Nothing usable to swap in, so drop the entry: a later Acquire opens the
			// cache fresh rather than resolving a store that is closed for good.
			delete(r.entries, cacheID)
			return errors.Join(deleteErr, fmt.Errorf("clear: reopen: %w", err))
		}
		e.store = store
	}
	if deleteErr != nil {
		return fmt.Errorf("clear: delete files: %w", deleteErr)
	}
	return nil
}

// Delete removes cacheID's store for good: close it if open, delete the files, and
// drop the entry — nothing reopens, so a handle still out is dead and a later Acquire
// is refused with ErrDeleted. For a cache that is going away with the record it
// belongs to; Clear is the one that leaves a usable empty store behind. A cache that
// was never opened is not an error, and one whose cleanup failed stays retired — the
// caller retries, and an Acquire in between would recreate exactly what is going.
// Callers stop the cache's workers first, the way they do for Clear.
func (r *Registry) Delete(cacheID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Retiring is the decision, not the outcome: a failed close or unlink is retried
	// by the caller's next pass, and until it lands nothing may open the file again.
	r.deleted[cacheID] = true

	if e, held := r.entries[cacheID]; held {
		if err := e.store.db.Close(); err != nil {
			return fmt.Errorf("delete: close store: %w", err)
		}
		delete(r.entries, cacheID)
	}
	if err := r.deleteFiles(r.path(cacheID)); err != nil {
		return fmt.Errorf("delete: delete files: %w", err)
	}
	return nil
}

// ClearKind wipes one kind from cacheID's store — a convenience over
// Acquire/Store.ClearKind/Release for a caller holding no handle.
func (r *Registry) ClearKind(ctx context.Context, cacheID int64, apiVersion, resource string) error {
	h, err := r.Acquire(cacheID)
	if err != nil {
		return err
	}
	defer h.Release()
	return h.Store().ClearKind(ctx, apiVersion, resource)
}

// Stats reports cacheID's on-disk size without opening it. Bytes counts the
// -wal/-shm sidecars alongside the main file — a bare stat of the main file swings
// with checkpoint timing.
func (r *Registry) Stats(cacheID int64) (Stats, error) {
	path := r.path(cacheID)
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, fmt.Errorf("stats: %w", err)
	}
	bytes := fi.Size()
	for _, suffix := range []string{"-wal", "-shm"} {
		if sidecarInfo, err := os.Stat(path + suffix); err == nil {
			bytes += sidecarInfo.Size()
		}
	}
	return Stats{Exists: true, Bytes: bytes}, nil
}

// path is cacheID's db file path within the registry's directory.
func (r *Registry) path(cacheID int64) string {
	return filepath.Join(r.dir, strconv.FormatInt(cacheID, 10)+".db")
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

// openStore opens the writer pool at path, sets auto_vacuum before any table exists,
// and applies the schema.
func openStore(path string) (*Store, error) {
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
	return &Store{db: db}, nil
}

// Start is the lifecycle shape; the registry has no background work.
func (r *Registry) Start(ctx context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close closes every open store. Handles still out are dead after it; Close runs
// only after everything that writes has stopped.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for id, e := range r.entries {
		if e.store != nil {
			errs = append(errs, e.store.db.Close())
		}
		delete(r.entries, id)
	}
	return errors.Join(errs...)
}

// Stats is one cache's on-disk footprint.
type Stats struct {
	Exists bool
	Bytes  int64
}

// Handle is one holder's claim on a cache's store. Store resolves through the
// registry on every call, so a Clear's swap reaches every holder.
type Handle struct {
	r       *Registry
	cacheID int64
}

// Store returns the current store behind this handle.
func (h *Handle) Store() *Store {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	e := h.r.entries[h.cacheID]
	if e == nil {
		return nil
	}
	return e.store
}

// Release gives the claim back; the last release closes the store.
func (h *Handle) Release() {
	h.r.mu.Lock()
	e := h.r.entries[h.cacheID]
	if e == nil {
		h.r.mu.Unlock()
		return
	}
	e.refs--
	if e.refs > 0 {
		h.r.mu.Unlock()
		return
	}
	delete(h.r.entries, h.cacheID)
	h.r.mu.Unlock()
	e.store.db.Close()
}

// Store is one cache's open SQLite file. The writer pool is capped at one
// connection, and auto_vacuum=INCREMENTAL is set on the fresh pool before
// migrations run — SQLite silently ignores it once any table exists.
type Store struct {
	db *sql.DB
}

// Cookie returns the watch resourceVersion recorded for one kind, and whether one
// is recorded. Keys into cluster_meta, per the schema's bookkeeping bag.
func (s *Store) Cookie(ctx context.Context, apiVersion, resource string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key = ?`, cookieKey(apiVersion, resource)).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("cookie: %w", err)
	}
	return v, true, nil
}

// SetCookie records the watch resourceVersion for one kind.
func (s *Store) SetCookie(ctx context.Context, apiVersion, resource, resourceVersion string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cluster_meta (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		cookieKey(apiVersion, resource), resourceVersion)
	if err != nil {
		return fmt.Errorf("set cookie: %w", err)
	}
	return nil
}

// ClearKind deletes one kind's rows, everything hanging off them, its kind_catalog
// row, and its cookie, in one transaction. The kind's objects are keyed by Kind while
// the caller holds the plural, so the rows are resolved through kind_catalog — a kind
// with no catalog row has no reachable rows, and only the cookie is deleted.
func (s *Store) ClearKind(ctx context.Context, apiVersion, resource string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("clear kind: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	var kind string
	err = tx.QueryRowContext(ctx,
		`SELECT kind FROM kind_catalog WHERE api_version = ? AND resource = ?`,
		apiVersion, resource).Scan(&kind)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No catalog row: nothing joins to it, so only the cookie is reachable.
	case err != nil:
		return fmt.Errorf("clear kind: resolve catalog: %w", err)
	default:
		// Core events are not in objects: they have their own table, and every cached
		// event rolls into that one catalog row, so clearing the kind clears the table.
		if apiVersion == coreEventsAPIVersion && resource == coreEventsResource {
			if _, err := tx.ExecContext(ctx, `DELETE FROM events`); err != nil {
				return fmt.Errorf("clear kind: delete events: %w", err)
			}
		}
		// The schema has no cascading foreign keys, so the rows hanging off each
		// object go here too — and before the objects they are selected through.
		// owner_refs by child_uid only: an edge is extracted from the CHILD's
		// ownerReferences, so a retained child's edge into a cleared owner is still
		// what that child says, and only rewriting the child could put it back.
		// Traversals join against objects, where a missing owner reads the same as one
		// whose kind is not mirrored at all.
		for _, stmt := range []string{
			`DELETE FROM owner_refs WHERE child_uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM labels WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM status_history WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM objects WHERE api_version = ? AND kind = ?`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, apiVersion, kind); err != nil {
				return fmt.Errorf("clear kind: delete rows: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM kind_catalog WHERE api_version = ? AND resource = ?`, apiVersion, resource); err != nil {
			return fmt.Errorf("clear kind: delete catalog: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key = ?`, cookieKey(apiVersion, resource)); err != nil {
		return fmt.Errorf("clear kind: delete cookie: %w", err)
	}
	return tx.Commit()
}

// cookieKey is the cluster_meta key one kind's watch resourceVersion is stored
// under. Never parsed back — apiVersion and resource are read from the caller's
// own arguments, not recovered from the key.
func cookieKey(apiVersion, resource string) string {
	return "cookie/" + apiVersion + "/" + resource
}
