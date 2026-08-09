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

package objectsync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// redactedValue replaces every Secret data value before the body is stored. The keys are
// kept so a UI can still list what a Secret holds.
const redactedValue = "[redacted]"

// objectRow is one row of the cache's objects table, flattened from an object body.
type objectRow struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
	Generation      int64
	CreatedAt       int64
	RawJSON         []byte
	OwnerRefs       []ownerRef
	Labels          map[string]string
	// status holds the materialized status columns — see status.go.
	status statusReading
}

type ownerRef struct {
	UID          string
	IsController bool
}

// objectStore is the write path into one KIND's slice of the cache's objects table — the
// kubesync.Store one ClusterCacheGVRSync worker lands its pulls in. Reads live on
// store.ClusterDB (Objects, consumed by ClusterDataObjectsWatch).
//
// Unlike the events table, which one worker owns outright, the objects table is SHARED by
// every synced kind. So every statement here is kind-scoped: the prune deletes only this
// kind's rows, the count counts only this kind's, and the resume cookie is keyed by the
// kind. Two workers writing the same table concurrently is fine — they touch disjoint rows.
type objectStore struct {
	cdb  *store.ClusterDB
	kind Kind
	// resume is THIS kind's list/watch position in cluster_meta — the shared protocol,
	// since the key prefix is the only thing that differs per collection.
	resume *store.ResumeCookie
	// now supplies the updated_at stamp. A seam only so this package's tests can freeze it
	// and pin the relist sweep's boundary, which is otherwise decided by whether the
	// millisecond happened to tick between a write and the relist that follows it.
	now func() int64
}

var (
	_ kubesync.Store             = (*objectStore)(nil)
	_ kubesync.MetadataDiffStore = (*objectStore)(nil)
)

func newObjectStore(cdb *store.ClusterDB, kind Kind) *objectStore {
	s := &objectStore{
		cdb:  cdb,
		kind: kind,
		now:  func() int64 { return time.Now().UnixMilli() },
	}
	// The cookie reads the clock through s, not a copy of it, so a test that freezes
	// s.now freezes the cookie's stamp too.
	s.resume = store.NewResumeCookie(cdb, kind.APIVersion+"/"+kind.Kind, func() int64 { return s.now() })
	return s
}

// ApplyChange lands one watch delta: Added/Modified upsert, Deleted removes by uid.
func (s *objectStore) ApplyChange(ctx context.Context, t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(ctx, u)
	case watch.Deleted:
		return s.remove(ctx, u)
	default:
		return nil
	}
}

func (s *objectStore) upsert(ctx context.Context, u *unstructured.Unstructured) error {
	return s.write(ctx, u, true)
}

// ApplyDiff lands one object the diff resync fetched, WITHOUT advancing the resume cookie.
//
// A watch delta carries its own position, so applying it and advancing the cookie together
// is exactly right. A diff's objects do not: they are fetched by GET in whatever order the
// metadata list happened to name them, each carrying its own current resourceVersion. Moving
// the cookie to one of them would leave it AHEAD of changes the pass has not applied yet, so
// a crash mid-pass would resume the next watch past them and lose them for good. The pass
// clears the cookie before its first write and persists the list's position once it has
// reconciled everything — the same "a cookie means a completed pass" invariant the full LIST
// keeps.
func (s *objectStore) ApplyDiff(ctx context.Context, u *unstructured.Unstructured) error {
	return s.write(ctx, u, false)
}

// ClearRV drops the resume cookie, so an interrupted pass leaves no position at all rather
// than one its rows don't back. See ApplyDiff.
func (s *objectStore) ClearRV(ctx context.Context) error {
	return s.resume.Delete(ctx, s.cdb.Writer())
}

// write lands one object and its edges, optionally advancing the resume cookie in the same
// transaction: a reader must never see an object beside the labels it had before the
// update, and a restart must never resume from a position the rows don't back.
func (s *objectStore) write(ctx context.Context, u *unstructured.Unstructured, advanceRV bool) error {
	row, err := s.project(u)
	if err != nil {
		return err
	}
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if err := s.insertRow(ctx, tx, row, s.now()); err != nil {
		return err
	}
	if advanceRV {
		if err := s.resume.Advance(ctx, tx, u); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

func (s *objectStore) remove(ctx context.Context, u *unstructured.Unstructured) error {
	// Same guard as project's, for the same reason: the delete path never reaches it.
	if u == nil || u.Object == nil {
		return fmt.Errorf("objectsync: %s delete carries an empty object", s.kind.Kind)
	}
	// An unkeyable delete is an ERROR, not a quiet no-op — the same answer the upsert path
	// gives an empty uid. Returning nil reported success for a delta whose cookie was never
	// advanced, so the driver booked progress and a crash in that window resumed the watch
	// from an older position and replayed. Ending the phase re-lists instead.
	uid := string(u.GetUID())
	if uid == "" {
		return fmt.Errorf("objectsync: %s delete has empty UID", s.kind.Kind)
	}
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if err := deleteOne(ctx, tx, uid); err != nil {
		return err
	}
	if err := s.resume.Advance(ctx, tx, u); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

// notify wakes this kind's objects watch. Keyed by the plural resource — the identity a
// ClusterDataObjectsWatch subscribes on — so an unrelated kind's write costs it nothing.
// Keyless subscribers (the kind-catalog watch) are woken by the same call.
func (s *objectStore) notify() {
	s.cdb.ObjectsNotifyResource(s.kind.APIVersion, s.kind.Resource)
}

// insertRow upserts one object and rewrites its edges. The single write chokepoint for
// this kind's rows — both write paths (watch delta, relist page) route through it.
//
// The edges are DELETEd before being re-inserted rather than upserted: an object that
// lost a label or an ownerReference must lose the row too, and only a delete-then-insert
// expresses that. Both tables are keyed by uid, so the delete is a point lookup.
func (s *objectStore) insertRow(ctx context.Context, ex store.Execer, row objectRow, now int64) error {
	if err := s.recordStatusTransition(ctx, ex, row, now); err != nil {
		return err
	}
	rawJSON, err := store.CompressRaw(row.RawJSON)
	if err != nil {
		return err
	}
	_, err = ex.ExecContext(ctx, `
		INSERT INTO objects (
			uid, api_version, kind, namespace, name,
			resource_version, generation, created_at, updated_at, raw_json,
			status_summary, ready_count, total_count, restart_count, host
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uid) DO UPDATE SET
			api_version=excluded.api_version,
			kind=excluded.kind,
			namespace=excluded.namespace,
			name=excluded.name,
			resource_version=excluded.resource_version,
			generation=excluded.generation,
			-- Keep the recorded creation time when the incoming body has none. It is
			-- immutable in Kubernetes, so a later write without it (a partial body, a
			-- projection that dropped it) carries no news — and project leaves it 0, which
			-- would otherwise overwrite a good value with the epoch and make the Age column
			-- read 56 years.
			-- Keep the recorded creation time when the incoming body has none. It is
			-- immutable in Kubernetes, so a later write without it (a partial body, a
			-- projection that dropped it) carries no news — and project leaves it 0, which
			-- would otherwise overwrite a good value with the epoch and make the Age column
			-- read 56 years.
			created_at=CASE WHEN excluded.created_at > 0 THEN excluded.created_at ELSE created_at END,
			updated_at=excluded.updated_at,
			raw_json=excluded.raw_json,
			status_summary=excluded.status_summary,
			ready_count=excluded.ready_count,
			total_count=excluded.total_count,
			restart_count=excluded.restart_count,
			host=excluded.host`,
		row.UID, s.kind.APIVersion, s.kind.Kind, row.Namespace, row.Name,
		row.ResourceVersion, row.Generation, row.CreatedAt, now, rawJSON,
		store.NullIfEmpty(row.status.Summary), row.status.Ready, row.status.Total,
		row.status.Restart, store.NullIfEmpty(row.status.Host),
	)
	if err != nil {
		return err
	}

	// Each edge table costs at most two statements — one DELETE, one multi-row INSERT —
	// rather than one per row. A pod carries a handful of labels, and a relist page holds
	// 500 of them inside a single transaction on a writer pool the whole cache shares, so
	// per-row statements are the difference between hundreds and thousands per page.
	if _, err := ex.ExecContext(ctx, `DELETE FROM owner_refs WHERE child_uid=?`, row.UID); err != nil {
		return err
	}
	if len(row.OwnerRefs) > 0 {
		args := make([]any, 0, len(row.OwnerRefs)*3)
		for _, ref := range row.OwnerRefs {
			args = append(args, row.UID, ref.UID, boolToInt(ref.IsController))
		}
		q := `INSERT INTO owner_refs (child_uid, owner_uid, is_controller) VALUES ` +
			valuesPlaceholders(len(row.OwnerRefs), 3) +
			` ON CONFLICT(child_uid, owner_uid) DO UPDATE SET is_controller=excluded.is_controller`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}

	if _, err := ex.ExecContext(ctx, `DELETE FROM labels WHERE uid=?`, row.UID); err != nil {
		return err
	}
	if len(row.Labels) > 0 {
		args := make([]any, 0, len(row.Labels)*3)
		for k, v := range row.Labels {
			args = append(args, row.UID, k, v)
		}
		q := `INSERT INTO labels (uid, key, value) VALUES ` + valuesPlaceholders(len(row.Labels), 3) +
			` ON CONFLICT(uid, key) DO UPDATE SET value=excluded.value`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}
	return nil
}

// valuesPlaceholders builds "(?,?,?),(?,?,?),…" for a multi-row INSERT of rows tuples of
// cols columns each.
func valuesPlaceholders(rows, cols int) string {
	tuple := "(" + strings.Repeat("?,", cols-1) + "?)"
	var b strings.Builder
	b.Grow(rows * (len(tuple) + 1))
	for i := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(tuple)
	}
	return b.String()
}

// recordStatusTransition appends to the object's status timeline, but only when its summary
// actually CHANGED — the point of the table is transitions, and a relist rewrites every row
// whether or not anything moved, so an unconditional insert would bury the real changes
// under a copy of the whole collection every resync period.
//
// One statement, not a read then an insert: the guard is a NOT EXISTS against the row's
// current summary, evaluated on the caller's transaction. That keeps it consistent with the
// upsert that follows (a separate read would run on the reader pool, outside this
// transaction) and halves the statements on the hottest path in the system — this runs per
// object write, including every item of every relist page. A row with no summary (a kind
// this package can't read, which is most CRDs) records nothing rather than a run of empties.
func (s *objectStore) recordStatusTransition(ctx context.Context, ex store.Execer, row objectRow, now int64) error {
	if row.status.Summary == "" {
		return nil
	}
	_, err := ex.ExecContext(ctx,
		`INSERT INTO status_history(uid, at, summary)
		 SELECT ?, ?, ?
		 WHERE NOT EXISTS (SELECT 1 FROM objects WHERE uid = ? AND status_summary = ?)`,
		row.UID, now, row.status.Summary, row.UID, row.status.Summary)
	return err
}

// deleteOne removes a single object and its edges. The objects delete fires the
// kind_counts trigger, keeping the dashboard's per-kind tally exact.
// cascadeTables are the per-object side tables and the uid column in each. Every deleter
// below clears these before the objects row, so the list lives here once: a new uid-keyed
// table (or a missed column) can't be added to one deleter and silently skipped by another,
// which is exactly how owner_refs.owner_uid came to be dropped from all three.
//
// owner_refs appears TWICE on purpose. A deleted object is both a child (its own references
// out) and an owner (its children's references in), and only the first is obvious. With
// --cascade=orphan the children outlive their owner, so leaving the inbound edges behind
// leaves rows pointing at a uid that no longer exists.
var cascadeTables = []struct{ table, uidCol string }{
	{"labels", "uid"},
	{"owner_refs", "child_uid"},
	{"owner_refs", "owner_uid"},
	{"status_history", "uid"},
}

func deleteOne(ctx context.Context, ex store.Execer, uid string) error {
	for _, c := range cascadeTables {
		if _, err := ex.ExecContext(ctx, `DELETE FROM `+c.table+` WHERE `+c.uidCol+`=?`, uid); err != nil {
			return err
		}
	}
	_, err := ex.ExecContext(ctx, `DELETE FROM objects WHERE uid=?`, uid)
	return err
}

// sweep deletes every object of this kind matching an extra WHERE predicate, along with
// its edges — three statements regardless of how many rows match.
//
// The edges are deleted through a subquery against the same predicate, so nothing is read
// back into Go. That matters at both call sites: a relist of a 20k-object kind would
// otherwise materialize 20k uids and issue 60k statements, all while holding the writer
// lock the whole cache shares.
func (s *objectStore) sweep(ctx context.Context, ex store.Execer, extraWhere string, extraArgs ...any) (int, error) {
	where := `api_version=? AND kind=?`
	if extraWhere != "" {
		where += ` AND ` + extraWhere
	}
	args := append([]any{s.kind.APIVersion, s.kind.Kind}, extraArgs...)
	sub := `SELECT uid FROM objects WHERE ` + where
	for _, c := range cascadeTables {
		q := `DELETE FROM ` + c.table + ` WHERE ` + c.uidCol + ` IN (` + sub + `)`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return 0, err
		}
	}
	res, err := ex.ExecContext(ctx, `DELETE FROM objects WHERE `+where, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// Count returns how many rows this kind holds — the warm-cache size the resume report
// describes itself with.
//
// It reads the trigger-maintained kind_counts aggregate rather than counting the objects
// table: this runs at every worker start, and a cache warm-resuming its hundred kinds
// would otherwise scan the shared table a hundred times. A kind with no row yet has
// written nothing, so absent reads as 0.
func (s *objectStore) Count(ctx context.Context) (int, error) {
	var n int
	err := s.cdb.Reader().QueryRowContext(ctx,
		`SELECT count FROM kind_counts WHERE api_version=? AND kind=?`,
		s.kind.APIVersion, s.kind.Kind).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// SnapshotRVs returns this kind's cached uid -> resourceVersion — what the driver's diff
// resync compares the cluster's metadata list against to decide which bodies to fetch.
//
// Kind-scoped like every other statement here: the objects table is shared, and another
// kind's rows belong to another worker's resync.
func (s *objectStore) SnapshotRVs(ctx context.Context) (map[string]string, error) {
	rows, err := s.cdb.Reader().QueryContext(ctx,
		`SELECT uid, resource_version FROM objects WHERE api_version=? AND kind=?`,
		s.kind.APIVersion, s.kind.Kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var uid string
		var rv sql.NullString
		if err := rows.Scan(&uid, &rv); err != nil {
			return nil, err
		}
		out[uid] = rv.String
	}
	return out, rows.Err()
}

// DeleteByUIDs removes the named objects and their edges — the diff resync's counterpart
// to the relist's sweep, for the objects the cluster no longer has.
//
// One transaction and one notify for the whole set, in chunked IN (…) statements, for the
// same reason sweep exists: these land on the writer connection the WHOLE cache shares, so
// a kind that lost thousands of objects between resyncs would otherwise take thousands of
// commits with every other kind's worker queued behind them — and ping the debounced
// watches thousands of times to describe one reconcile.
//
// It does not touch the resume cookie: a deletion carries no resourceVersion of its own,
// and the pass persists the list's RV once it has reconciled everything.
func (s *objectStore) DeleteByUIDs(ctx context.Context, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path

	// Chunked well under SQLite's default 999-variable statement limit.
	const chunk = 500
	for start := 0; start < len(uids); start += chunk {
		batch := uids[start:min(start+chunk, len(uids))]
		args := make([]any, len(batch))
		for i, uid := range batch {
			args[i] = uid
		}
		in := strings.Repeat("?,", len(batch)-1) + "?"
		for _, c := range cascadeTables {
			q := `DELETE FROM ` + c.table + ` WHERE ` + c.uidCol + ` IN (` + in + `)`
			if _, err := tx.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE uid IN (`+in+`)`, args...); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

// PersistRV advances this kind's resume cookie without touching rows — called on every
// watch delta so a wake resumes from the latest position.
func (s *objectStore) PersistRV(ctx context.Context, rv string) error {
	return s.resume.Persist(ctx, s.cdb.Writer(), rv)
}

// ResumeRV returns the resume cookie to seed a watch from, or "" to force a cold full
// LIST. A partial relist cleared it on its first WritePage, so a cookie means a completed
// LIST landed on disk.
func (s *objectStore) ResumeRV(ctx context.Context) (string, error) {
	return s.resume.Get(ctx)
}

// EnsureCatalog registers this kind in the cache's kind_catalog. The worker calls it on
// every start; the upsert makes that a no-op after the first.
//
// It is load-bearing for reads, not just for the nav: store.Objects translates the plural
// resource a watch subscribes on back to the Kind the objects table is keyed by through
// this table, so a kind with rows and no catalog entry reads as empty.
//
// Each kind's row is written by the worker that syncs it and removed by Forget when that
// worker's object is deleted, so the catalog says exactly "what this cache holds" — which
// is why the discovery controller writes no cache rows of its own.
func (s *objectStore) EnsureCatalog(ctx context.Context) error {
	if err := s.forgetSupersededKind(ctx); err != nil {
		return err
	}
	return store.EnsureKindCatalog(ctx, s.cdb, store.KindRow{
		APIVersion: s.kind.APIVersion,
		Kind:       s.kind.Kind,
		Resource:   s.kind.Resource,
		Scope:      s.kind.scope(),
		IsCRD:      s.kind.isCRD(),
	})
}

// forgetSupersededKind purges any OTHER Kind holding this kind's plural — the whole of it,
// not just its catalog row.
//
// Within one api group-version a plural names exactly one Kind, so a catalog row with our
// (api_version, resource) under a different Kind describes a rename we slept through: a CRD
// whose Kind changed while the sidecar was down, so no running worker was there to clean up
// after it. Everything that Kind wrote is now unreachable — nothing will ever name it
// again, so its objects and edges, its kind_counts row and its resume cookie are dead
// weight the cache file carries forever.
//
// Reusing Forget is the point: it is already the "this kind is gone" purge, so the two
// paths cannot drift over which tables a kind owns.
func (s *objectStore) forgetSupersededKind(ctx context.Context) error {
	rows, err := s.cdb.Reader().QueryContext(ctx,
		`SELECT kind FROM kind_catalog WHERE api_version=? AND resource=? AND kind<>?`,
		s.kind.APIVersion, s.kind.Resource, s.kind.Kind)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			rows.Close()
			return err
		}
		stale = append(stale, kind)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, kind := range stale {
		// Only the identity fields matter to a purge: everything Forget deletes is keyed by
		// (api_version, kind), and the resume cookie by the same pair.
		old := newObjectStore(s.cdb, Kind{
			APIVersion: s.kind.APIVersion,
			Kind:       kind,
			Resource:   s.kind.Resource,
			Namespaced: s.kind.Namespaced,
		})
		if err := old.Forget(ctx); err != nil {
			return fmt.Errorf("purge superseded kind %s/%s: %w", s.kind.APIVersion, kind, err)
		}
	}
	return nil
}

// Forget removes every trace of this kind from the cache — its rows and their edges, its
// catalog entry, and its resume cookie. The controller calls it when the sync child is
// deleted (the kind is no longer served, or the cache is going away), so the cache never
// advertises a kind whose contents are frozen at whenever its worker stopped.
func (s *objectStore) Forget(ctx context.Context) error {
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path

	if _, err := s.sweep(ctx, tx, ""); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM kind_catalog WHERE api_version=? AND kind=?`,
		s.kind.APIVersion, s.kind.Kind); err != nil {
		return err
	}
	// The sweep's deletes only decrement the tally — the trigger leaves the row at 0, since
	// a still-advertised-but-empty kind must read 0 rather than vanish. A forgotten kind is
	// not that: nothing will name it again, so its row goes with it.
	if _, err := tx.ExecContext(ctx, `DELETE FROM kind_counts WHERE api_version=? AND kind=?`,
		s.kind.APIVersion, s.kind.Kind); err != nil {
		return err
	}
	if err := s.resume.Delete(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}

// BeginReplace opens a streaming full-LIST reconcile of this kind's rows. It prunes (in
// Commit): a row absent from the LIST is gone from the server.
//
// Clearing the resume cookie is DEFERRED to the first WritePage rather than done here, so
// a pass that fails before writing anything leaves the untouched snapshot's cookie intact
// and the next start still resumes cheaply. Because a relist prunes, that clear is
// load-bearing: a pass that fails after writing pages but before Commit leaves no cookie,
// so the next start cold-LISTs and its prune reconciles the leftover rows instead of
// resuming past them.
func (s *objectStore) BeginReplace() kubesync.ReplaceSession {
	return &replaceSession{s: s, mark: s.now() + 1}
}

// replaceSession streams a paginated relist into the objects table and reconciles THIS
// KIND's rows to what the LIST returned. The prune is kind-scoped because the table is
// shared — every other kind's rows belong to another worker's relist, not this one's.
//
// It reconciles by **mark and sweep** rather than by remembering what it saw: every
// WritePage stamps `updated_at`, and Commit deletes this kind's rows still older than the
// session's start. That keeps the pass genuinely O(one page) in memory — a keep-set of
// every uid in the collection would defeat the whole point of paginating — and turns the
// prune into three statements instead of a full read-back plus three per stale row.
//
// Per-page commits trade the single-transaction atomicity of the whole pass for that
// memory bound: a pass that fails mid-pagination leaves its committed pages visible until
// the next pass's prune reconciles them.
type replaceSession struct {
	s *objectStore
	// mark is the sweep boundary: rows this pass writes get an updated_at >= mark, and
	// Commit deletes this kind's rows still below it.
	//
	// It is one millisecond PAST the session's start, not the start itself. updated_at has
	// millisecond resolution, so a row written in the same millisecond the session began
	// carries the start stamp exactly — with an inclusive boundary such a row would look
	// like one this pass wrote and survive a prune it deserved. Pushing the mark past it
	// makes "written by this pass" strictly distinguishable from "already there".
	mark int64
	// cookieCleared records whether the first WritePage has durably cleared the resume
	// cookie, so the clear happens once per session.
	cookieCleared bool
}

// WritePage lands one page in its own transaction, clearing the resume cookie alongside
// the first page's rows (see BeginReplace). A body that won't project (no UID) is skipped
// rather than failing the page — one malformed object must not wedge the pass.
func (r *replaceSession) WritePage(ctx context.Context, items []*unstructured.Unstructured) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := r.s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if !r.cookieCleared {
		if err := r.s.resume.Delete(ctx, tx); err != nil {
			return err
		}
	}
	// Stamp at or after the mark, never below it, so this page's rows survive the sweep
	// even when the whole pass runs inside one millisecond.
	now := max(r.s.now(), r.mark)
	for _, u := range items {
		row, err := r.s.project(u)
		if err != nil {
			continue
		}
		if err := r.s.insertRow(ctx, tx, row, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.cookieCleared = true
	// Notify per committed page, not only at Commit, so durable rows always reach the
	// objects watch — a relist that commits pages then fails would otherwise leave them
	// durable-but-unannounced until some later write.
	r.s.notify()
	return nil
}

// Commit sweeps this kind's rows that no page rewrote, then persists the resume cookie.
// Both share one transaction, so a failed persist can't leave the cookie durably advanced
// — which would resume the next watch past the objects before it.
func (r *replaceSession) Commit(ctx context.Context, resourceVersion string) (int, error) {
	tx, err := r.s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path

	pruned, err := r.s.sweep(ctx, tx, `updated_at < ?`, r.mark)
	if err != nil {
		return 0, err
	}
	if err := r.s.resume.Persist(ctx, tx, resourceVersion); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	r.s.notify()
	return pruned, nil
}

// project flattens an object body into the objects table's columns, redacting and
// stripping the body along the way.
func (s *objectStore) project(u *unstructured.Unstructured) (objectRow, error) {
	// A nil body, or one wrapping a nil map, would panic on GetUID — inside a worker
	// goroutine, taking the process with it. Today's driver never produces one (the watch
	// path type-asserts and the dynamic source builds from a decoded list), but the items
	// reaching WritePage/ApplyChange come from a pluggable Source, so the guard belongs on
	// the receiving side rather than in each producer.
	if u == nil || u.Object == nil {
		return objectRow{}, fmt.Errorf("objectsync: %s object is empty", s.kind.Kind)
	}
	uid := string(u.GetUID())
	if uid == "" {
		return objectRow{}, fmt.Errorf("objectsync: %s has empty UID", s.kind.Kind)
	}

	body := sanitize(u)
	rawJSON, err := json.Marshal(body.Object)
	if err != nil {
		return objectRow{}, err
	}

	row := objectRow{
		UID:             uid,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		ResourceVersion: u.GetResourceVersion(),
		Generation:      u.GetGeneration(),
		RawJSON:         rawJSON,
		Labels:          u.GetLabels(),
	}
	if ts := u.GetCreationTimestamp(); !ts.IsZero() {
		row.CreatedAt = ts.UnixMilli()
	}
	// Read from the ORIGINAL body, not the sanitized copy: sanitize redacts a Secret's
	// values, and a future strip could drop a field a reading depends on. Nothing here
	// touches the values it reads.
	row.status = extractStatus(u)
	for _, ref := range u.GetOwnerReferences() {
		if ref.UID == "" {
			continue
		}
		row.OwnerRefs = append(row.OwnerRefs, ownerRef{
			UID:          string(ref.UID),
			IsController: ref.Controller != nil && *ref.Controller,
		})
	}
	return row, nil
}

// sanitize returns a copy of the body as it should be stored: server noise removed and,
// for Secrets, values redacted. It deep-copies because the caller's body is the live
// watch object and the driver may still be reading it.
//
// Stripping is a real saving, not tidiness — managedFields and the last-applied
// annotation together are roughly half of a typical object's bytes, and nothing reads
// them (the frontend renders columns off the rest of the body). Reads are then a pure
// pass-through, which is what lets ClusterDataObject serve raw_json verbatim.
func sanitize(u *unstructured.Unstructured) *unstructured.Unstructured {
	out := u.DeepCopy()
	unstructured.RemoveNestedField(out.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(out.Object, "metadata", "annotations",
		"kubectl.kubernetes.io/last-applied-configuration")
	// An annotations map emptied by that removal is noise of its own; drop it.
	if ann, ok, _ := unstructured.NestedMap(out.Object, "metadata", "annotations"); ok && len(ann) == 0 {
		unstructured.RemoveNestedField(out.Object, "metadata", "annotations")
	}
	if isSecret(out) {
		redactSecret(out)
	}
	return out
}

// isSecret reports whether a body is a core/v1 Secret. It reads the BODY's own kind
// rather than the worker's configured one, so a Secret arriving through some other
// collection is redacted too — the check must not be bypassable by how we asked for it.
func isSecret(u *unstructured.Unstructured) bool {
	return u.GetKind() == "Secret" && u.GetAPIVersion() == "v1"
}

// redactSecret strips a Secret's values while keeping its keys, so a UI can show what a
// Secret holds without the cache file holding every credential in the cluster. stringData
// is write-only on the api server and never round-trips, so it is dropped outright.
func redactSecret(u *unstructured.Unstructured) {
	unstructured.RemoveNestedField(u.Object, "stringData")
	data, ok, _ := unstructured.NestedMap(u.Object, "data")
	if !ok {
		return
	}
	for k := range data {
		data[k] = redactedValue
	}
	_ = unstructured.SetNestedMap(u.Object, data, "data")
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
