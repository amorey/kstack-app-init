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
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// The one kind whose rows live in events rather than objects: core v1 events. Any
// group may serve a Kind called "Event" — a CRD's rows are ordinary objects — so the
// events table is identified by api version and plural, never by the Kind name. Every
// cached event rolls into the schema's hardcoded ('v1','Event') count.
const (
	coreEventsAPIVersion = "v1"
	coreEventsKind       = "Event"
	coreEventsResource   = "events"
)

// Kind identifies one synced collection: the objects table is keyed by Kind while a
// watch is opened on the plural, so a writer carries both.
type Kind struct {
	APIVersion string
	Kind       string
	Resource   string
}

// isCoreEvents reports the one kind whose rows live in events rather than objects. Any
// group may serve a Kind called "Event" — a CRD's rows are ordinary objects — so this
// asks by api version and plural, never by the Kind name.
func (k Kind) isCoreEvents() bool {
	return k.APIVersion == coreEventsAPIVersion && k.Resource == coreEventsResource
}

// execer is the *sql.DB / *sql.Tx subset a write helper needs, so a delta (direct on
// the writer) and a relist page (in a transaction) share one statement.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// querier is the same subset for a point read, so one can be served out of a read
// transaction whose other query it must agree with.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Subscription carries the store's change pings. The value is empty — the key is the
// whole news, and a reader answers it by re-reading and diffing, so an early or late
// ping costs one idempotent read rather than a wrong frame.
//
// **Close it when done.** A receiver ends with an error when the store closes, which is
// what reaches a live watch whose store was cleared or shut down.
type Subscription = *conflate.Receiver[string, struct{}]

// EventsKey is the events bus key: its own, so an event-storm cluster does not wake
// every object watch in the cache.
const EventsKey = "events"

// KindsKey is the catalog bus key — what the sweep's write pings. Its own, because what
// moves it is a kind appearing or leaving rather than any row count.
const KindsKey = "kinds"

// ObjectsKey is one kind's bus key, by the plural a reader opened its watch on.
func ObjectsKey(apiVersion, resource string) string {
	return "objects/" + apiVersion + "/" + resource
}

// Counts is the whole-cache tally behind the stats gauge, read off the
// trigger-maintained per-kind counts — O(kinds), never a scan of objects. Events are
// excluded: they are not a catalog kind.
type Counts struct {
	ObjectCount int
	KindCount   int
}

// file is one cache's open SQLite database and the change bus over it — what Clear
// swaps under the claims on it, and what the last Release closes. The writer pool is
// capped at one connection.
type file struct {
	db *sql.DB
	// readDB is the reader pool beside the writer: the watches re-read on every ping, and
	// a read must not queue behind the one write connection. Distinct from the manager's
	// openReadOnly, which opens a CLOSED cache's file per call to measure it; this serves
	// an open one for the file's life.
	readDB *sql.DB
	hub    *conflate.Hub[string, struct{}]
	// stopJanitor retires this file's sweeper. A cancel and never a wait: all three exits
	// hold m.mu across the close, and a wait there would stall Stats behind a vacuum. The
	// sweep runs on the janitor's own context, so the cancel aborts it mid-statement.
	stopJanitor context.CancelFunc
	// now is the wall clock in millis; a seam so a test can freeze it. Reads go through
	// stamp, never here.
	now func() int64
	// clockMu guards lastStamp, which forces the write stamps strictly upward.
	clockMu   sync.Mutex
	lastStamp int64
}

// Store is one holder's claim on one cache, and everything done through it — every Store
// is a claim, and owes a Release. The file under it is resolved per call, so a Clear's swap
// reaches every holder and a Remove leaves them answering ErrClosed rather than writing
// into an unlinked inode.
type Store struct {
	m       *Manager
	cacheID int64
	e       *entry
	// bound is the file this store was handed, when it must not follow a swap. Clear
	// installs a fresh empty file on the same entry, and a reader that followed it would
	// answer "no rows" for a cache that was full — which a delta watch reports as a
	// Deleted for every row it holds. Nil means "whatever the entry holds", which is what
	// a writer wants: its next write belongs in the current file.
	bound *file
}

// stamp is the updated_at every write records: the wall clock, forced strictly
// increasing. A relist prunes by comparing against its own boundary, and the clock has
// millisecond resolution — so two writes inside one tick would be indistinguishable,
// and a re-list that ran in the same millisecond as the rows it supersedes would keep
// every one of them.
func (f *file) stamp() int64 {
	f.clockMu.Lock()
	defer f.clockMu.Unlock()

	next := f.now()
	if next <= f.lastStamp {
		next = f.lastStamp + 1
	}
	f.lastStamp = next
	return next
}

// notify pings one key after a commit. Failure is a closed hub — a store shutting down,
// which the subscriber learns from its own receiver.
func (f *file) notify(key string) { _ = f.hub.Sender().Send(key, struct{}{}) }

// close closes both pools and ends every subscriber, which is how a clear or a
// shutdown reaches a live watch.
func (f *file) close() error {
	if f.stopJanitor != nil {
		f.stopJanitor()
	}
	f.hub.Close()
	return errors.Join(f.db.Close(), f.readDB.Close())
}

// startJanitor spawns this file's sweeper, or nothing when no interval is set.
func (f *file) startJanitor(cacheID int64, ret Retention) {
	if ret.Interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.stopJanitor = cancel
	go runJanitor(ctx, strconv.FormatInt(cacheID, 10), f.db, ret)
}

// newFile wraps the open pools. Nothing else builds one — openFile is the only caller.
func newFile(db, readDB *sql.DB) *file {
	return &file{
		db:     db,
		readDB: readDB,
		hub:    conflate.New[string, struct{}](),
		now:    func() int64 { return time.Now().UnixMilli() },
	}
}

// Release gives the claim back; the last release on an entry closes its file. It counts
// down the entry this store claimed, never whatever the id maps to now — a retired
// entry's stragglers must not close a fresh claim's file.
func (s *Store) Release() {
	s.m.mu.Lock()
	s.e.refs--
	if s.e.refs > 0 {
		s.m.mu.Unlock()
		return
	}
	f := s.e.file
	s.e.file = nil
	if s.m.entries[s.cacheID] == s.e {
		delete(s.m.entries, s.cacheID)
	}
	s.m.mu.Unlock()

	if f != nil {
		f.close()
	}
}

// file resolves the open file behind this store, or ErrClosed once it is gone — a
// Remove, or a Clear that could not reopen. Every method goes through it, so the check
// lives here once rather than at every call site.
func (s *Store) file() (*file, error) {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()

	if s.e.file == nil {
		return nil, fmt.Errorf("cache %d: %w", s.cacheID, ErrClosed)
	}
	// A bound store answers for its own file only: a Clear's swap ends it the way a Remove
	// does, rather than silently redirecting the read to the fresh empty one.
	if s.bound != nil && s.e.file != s.bound {
		return nil, fmt.Errorf("cache %d: %w", s.cacheID, ErrClosed)
	}
	return s.e.file, nil
}

// Subscribe returns the change feed for this cache, narrowed to keys — or every key when
// none are given, which is what a reader spanning both buses needs. It ends when the file
// closes, which is what tells a live watch that the cache was cleared or shut down.
//
// The filter runs at ENQUEUE, so a pods watch does not even hold a slot for an events
// write. Filtering in the reader's own loop would wake every open watch on every write in
// the cache, which is what the per-kind bus keys exist to prevent.
func (s *Store) Subscribe(keys ...string) (Subscription, error) {
	f, err := s.file()
	if err != nil {
		return nil, err
	}
	return f.subscribe(keys...), nil
}

// subscribe is the receiver both doors hand out: Store.Subscribe for a holder, and
// Manager.Subscribe for a caller that only borrows the feed.
func (f *file) subscribe(keys ...string) Subscription {
	if len(keys) == 0 {
		return f.hub.Receiver()
	}
	want := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		want[k] = struct{}{}
	}
	return f.hub.Receiver(f.hub.WithKeyFilter(func(k string) bool {
		_, ok := want[k]
		return ok
	}))
}

// Cookie returns the watch resourceVersion recorded for one kind, and whether one is
// recorded. Keys into cluster_meta, per the schema's bookkeeping bag.
func (s *Store) Cookie(ctx context.Context, apiVersion, resource string) (string, bool, error) {
	f, err := s.file()
	if err != nil {
		return "", false, err
	}
	var v string
	err = f.db.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key = ?`, cookieKey(apiVersion, resource)).Scan(&v)
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
	f, err := s.file()
	if err != nil {
		return err
	}
	if err := setCookie(ctx, f.db, apiVersion, resource, resourceVersion); err != nil {
		return fmt.Errorf("set cookie: %w", err)
	}
	return nil
}

// ApplyChange lands one watch delta: Added/Modified upsert the row, Deleted removes it.
// The row and the position that would replay it go in one transaction, so no restart
// resumes from a position the rows do not back.
func (s *Store) ApplyChange(ctx context.Context, k Kind, t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified, watch.Deleted:
	default:
		return nil
	}
	if u == nil || u.Object == nil {
		return fmt.Errorf("apply %s %s: empty object", k.Kind, t)
	}

	f, err := s.file()
	if err != nil {
		return err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("apply %s: begin: %w", k.Kind, err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if t == watch.Deleted {
		// An unkeyable delete errors rather than no-opping: booking progress for a delta
		// whose row never went would resume the next watch past it.
		uid := string(u.GetUID())
		if uid == "" {
			return fmt.Errorf("apply %s delete: empty UID", k.Kind)
		}
		if k.isCoreEvents() {
			_, err = tx.ExecContext(ctx, `DELETE FROM events WHERE uid=?`, uid)
		} else {
			err = deleteObjectRow(ctx, tx, uid)
		}
	} else if k.isCoreEvents() {
		err = f.writeEvent(ctx, tx, u)
	} else {
		err = f.writeObject(ctx, tx, k, u)
	}
	// A body the projection rejects is skipped, exactly as a relist page skips it. The cookie
	// still advances over it: the server replays from that position, so a run that failed here
	// would be handed the same body every time it resumed.
	if err != nil && !errors.Is(err, errUnprojectable) {
		return fmt.Errorf("apply %s %s: %w", k.Kind, t, err)
	}

	if rv := u.GetResourceVersion(); rv != "" {
		if err := setCookie(ctx, tx, k.APIVersion, k.Resource, rv); err != nil {
			return fmt.Errorf("apply %s: advance cookie: %w", k.Kind, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("apply %s: commit: %w", k.Kind, err)
	}
	f.notify(busKey(k))
	return nil
}

// writeObject projects and upserts one object body.
func (f *file) writeObject(ctx context.Context, ex execer, k Kind, u *unstructured.Unstructured) error {
	row, err := projectObject(u)
	if err != nil {
		return err
	}
	return insertObjectRow(ctx, ex, k, row, f.stamp())
}

// writeEvent projects and upserts one event body.
func (f *file) writeEvent(ctx context.Context, ex execer, u *unstructured.Unstructured) error {
	row, err := extractEvent(u)
	if err != nil {
		return err
	}
	return insertEventRow(ctx, ex, row, f.stamp())
}

// CountKind returns one kind's cached rows, off the trigger-maintained kind_counts
// rather than a scan of the shared objects table. A kind nothing has written reads 0.
func (s *Store) CountKind(ctx context.Context, k Kind) (int, error) {
	f, err := s.file()
	if err != nil {
		return 0, err
	}
	// Every cached event rolls into the schema's hardcoded ('v1','Event') tally,
	// maintained by the events triggers.
	var n int
	err = f.db.QueryRowContext(ctx,
		`SELECT count FROM kind_counts WHERE api_version=? AND kind=?`,
		k.APIVersion, k.Kind).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("count kind %s: %w", k.Kind, err)
	}
	return n, nil
}

// Counts is the whole-cache tally: total cached objects, and how many kinds hold any.
func (s *Store) Counts(ctx context.Context) (Counts, error) {
	f, err := s.file()
	if err != nil {
		return Counts{}, err
	}
	return countKinds(ctx, f.db)
}

// countKinds reads the tally off the trigger-maintained per-kind counts — O(kinds),
// never a scan of objects. A kind emptied by deletes keeps a zero row (an advertised
// but empty kind must read 0 rather than vanish), so the kind count is of kinds with
// rows. Shared with the manager's read-only path, which has a database but no Store.
func countKinds(ctx context.Context, db *sql.DB) (Counts, error) {
	var out Counts
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(count), 0), COUNT(*) FROM kind_counts
		 WHERE count > 0 AND NOT (api_version = ? AND kind = ?)`,
		coreEventsAPIVersion, coreEventsKind).Scan(&out.ObjectCount, &out.KindCount)
	if err != nil {
		return Counts{}, fmt.Errorf("counts: %w", err)
	}
	return out, nil
}

// ClearKind deletes one kind's rows, everything hanging off them, and its cookie, in one
// transaction — for a kind that has stopped being synced. The catalog row is not one of
// them: that table says what the cluster serves, and its one writer is the sweep.
//
// It takes the whole Kind because the rows are keyed by the singular while a watch is
// opened on the plural, and **the caller is what knows both**: the record carries the
// Kind, and resolving it here through kind_catalog would tie a teardown to a table the
// sweep owns, leaving every row behind for a kind no sweep has reached yet.
func (s *Store) ClearKind(ctx context.Context, k Kind) error {
	f, err := s.file()
	if err != nil {
		return err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("clear kind: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	// Core events are not in objects: they have their own table, which this collection
	// owns outright.
	if k.isCoreEvents() {
		if _, err := tx.ExecContext(ctx, `DELETE FROM events`); err != nil {
			return fmt.Errorf("clear kind: delete events: %w", err)
		}
	} else {
		// owner_refs by child_uid only: an edge is extracted from the CHILD's
		// ownerReferences, so a retained child's edge into a cleared owner is still what
		// that child says, and only rewriting the child could put it back. Traversals
		// join against objects, where a missing owner reads the same as one whose kind is
		// not mirrored at all.
		for _, stmt := range []string{
			`DELETE FROM owner_refs WHERE child_uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM labels WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM status_history WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
			`DELETE FROM objects WHERE api_version = ? AND kind = ?`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, k.APIVersion, k.Kind); err != nil {
				return fmt.Errorf("clear kind: delete rows: %w", err)
			}
		}
	}

	// The sweep above leaves the tally at 0, which is what an advertised but empty kind
	// must read. A forgotten kind is different: nothing will name it again.
	if _, err := tx.ExecContext(ctx, `DELETE FROM kind_counts WHERE api_version = ? AND kind = ?`,
		k.APIVersion, k.Kind); err != nil {
		return fmt.Errorf("clear kind: delete counts: %w", err)
	}
	// The catalog row stays: it says the CLUSTER serves this kind, which clearing the
	// cache does not change. SyncKinds' prune is what takes one out.
	if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key = ?`,
		cookieKey(k.APIVersion, k.Resource)); err != nil {
		return fmt.Errorf("clear kind: delete cookie: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	f.notify(busKey(k))
	return nil
}

// BeginReplace opens a streaming full-LIST reconcile of one kind's rows; Commit prunes
// what the LIST did not carry.
//
// Clearing the resume cookie is DEFERRED to the first written page, so a pass failing
// before any write keeps the intact snapshot's cookie, while one failing after writing
// leaves none and the next start cold-lists and prunes the leftovers.
func (s *Store) BeginReplace(k Kind) (*ReplaceSession, error) {
	f, err := s.file()
	if err != nil {
		return nil, err
	}
	// The session holds the file rather than re-resolving per page: a clear stops the
	// cache's workers before it swaps, so no session is live across one.
	return &ReplaceSession{f: f, kind: k, mark: f.stamp()}, nil
}

// ReplaceSession streams a paginated relist into the shared tables, reconciling one
// kind's rows only.
//
// It reconciles by MARK AND SWEEP: every page stamps updated_at and Commit deletes this
// kind's rows still older. That keeps the pass O(one page) in memory — a keep-set of
// every uid would defeat pagination — and prunes in a few statements rather than a
// read-back plus three per stale row. (The objects table's `generation` column is the
// object's own metadata.generation, not a sweep counter, and must not be pruned on.)
//
// Per-page commits trade whole-pass atomicity for that memory bound: a pass failing
// mid-pagination leaves committed pages visible until the next one prunes them.
type ReplaceSession struct {
	f    *file
	kind Kind
	// mark is the sweep boundary: every stamp taken before this session is strictly
	// below it, and every page this session writes strictly above.
	mark int64
	// cookieCleared makes the first page's cookie clear happen once per session.
	cookieCleared bool
}

// WritePage lands one page in its own transaction, clearing the resume cookie alongside
// the first page. A body that will not project is skipped, not fatal: one malformed
// object must not stop a collection from syncing.
//
// The first page clears the cookie **even when it carries nothing**: a cookie means a
// completed LIST landed on disk, so a relist that has begun must leave none standing —
// a pass that then fails would otherwise let the next start resume from it and skip the
// reconcile its rows still need. Later empty pages have nothing to do.
func (r *ReplaceSession) WritePage(ctx context.Context, items []*unstructured.Unstructured) error {
	if len(items) == 0 && r.cookieCleared {
		return nil
	}
	tx, err := r.f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("write page: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if !r.cookieCleared {
		if err := deleteCookie(ctx, tx, r.kind.APIVersion, r.kind.Resource); err != nil {
			return fmt.Errorf("write page: clear cookie: %w", err)
		}
	}
	now := r.f.stamp()
	for _, u := range items {
		var err error
		if r.kind.isCoreEvents() {
			err = r.writeEvent(ctx, tx, u, now)
		} else {
			err = r.writeObject(ctx, tx, u, now)
		}
		if err != nil {
			return fmt.Errorf("write page: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("write page: commit: %w", err)
	}
	r.cookieCleared = true
	if len(items) == 0 {
		// Nothing landed, so nothing to announce.
		return nil
	}
	// Per committed page, not only at Commit: a relist that commits pages and then fails
	// would otherwise leave durable rows unannounced until some later write.
	r.f.notify(busKey(r.kind))
	return nil
}

// writeObject lands one page item, skipping a body that will not project.
func (r *ReplaceSession) writeObject(ctx context.Context, ex execer, u *unstructured.Unstructured, now int64) error {
	row, err := projectObject(u)
	if errors.Is(err, errUnprojectable) {
		return nil
	} else if err != nil {
		return err
	}
	return insertObjectRow(ctx, ex, r.kind, row, now)
}

// writeEvent is writeObject for the events table.
func (r *ReplaceSession) writeEvent(ctx context.Context, ex execer, u *unstructured.Unstructured, now int64) error {
	row, err := extractEvent(u)
	if errors.Is(err, errUnprojectable) {
		return nil
	} else if err != nil {
		return err
	}
	return insertEventRow(ctx, ex, row, now)
}

// Commit sweeps the rows no page rewrote, then persists the cookie in the same
// transaction — a failed persist must not leave the cookie durably advanced, which
// would resume the next watch past the objects before it.
func (r *ReplaceSession) Commit(ctx context.Context, resourceVersion string) (int, error) {
	tx, err := r.f.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("commit relist: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	var pruned int
	if r.kind.isCoreEvents() {
		// The events table is this collection's outright, so the prune is unscoped.
		res, err := tx.ExecContext(ctx, `DELETE FROM events WHERE updated_at < ?`, r.mark)
		if err != nil {
			return 0, fmt.Errorf("commit relist: prune events: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("commit relist: prune events: %w", err)
		}
		pruned = int(n)
	} else {
		if pruned, err = sweepObjects(ctx, tx, r.kind, `updated_at < ?`, r.mark); err != nil {
			return 0, fmt.Errorf("commit relist: prune: %w", err)
		}
	}

	if resourceVersion != "" {
		if err := setCookie(ctx, tx, r.kind.APIVersion, r.kind.Resource, resourceVersion); err != nil {
			return 0, fmt.Errorf("commit relist: persist cookie: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit relist: %w", err)
	}
	r.f.notify(busKey(r.kind))
	return pruned, nil
}

// busKey is the ping key one kind's writes fan out on.
func busKey(k Kind) string {
	if k.isCoreEvents() {
		return EventsKey
	}
	return ObjectsKey(k.APIVersion, k.Resource)
}

// setMeta writes one bookkeeping value through any execer, so a caller can put it in the
// transaction whose rows it describes.
func setMeta(ctx context.Context, ex execer, key, value string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// getMeta reads one bookkeeping value, and whether it is recorded at all.
func getMeta(ctx context.Context, q querier, key string) (string, bool, error) {
	var v string
	err := q.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// setCookie writes one kind's resume position through any execer, so a delta and a
// relist commit share the statement.
func setCookie(ctx context.Context, ex execer, apiVersion, resource, resourceVersion string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		cookieKey(apiVersion, resource), resourceVersion)
	return err
}

// deleteCookie durably removes one kind's resume position, so the next start cold-lists.
func deleteCookie(ctx context.Context, ex execer, apiVersion, resource string) error {
	_, err := ex.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key = ?`, cookieKey(apiVersion, resource))
	return err
}

// cookieKey is the cluster_meta key one kind's watch resourceVersion is stored under.
// Never parsed back — apiVersion and resource are read from the caller's own arguments,
// not recovered from the key.
func cookieKey(apiVersion, resource string) string {
	return "cookie/" + apiVersion + "/" + resource
}
