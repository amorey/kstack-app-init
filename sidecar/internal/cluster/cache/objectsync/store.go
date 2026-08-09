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

// redactedValue replaces every Secret data value at write time; keys are kept so a UI can
// list what a Secret holds. See docs/adr/2026-08-09-rawjson-comparable-scalar.md.
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

// objectStore is one KIND's write path into the cache's objects table; reads live on
// store.ClusterDB.Objects.
//
// The objects table is SHARED by every synced kind, so every statement here is
// kind-scoped (prune, count, resume cookie). Concurrent workers touch disjoint rows.
type objectStore struct {
	cdb    *store.ClusterDB
	kind   Kind
	resume *store.ResumeCookie
	// now supplies the updated_at stamp; a seam so tests can freeze it and pin the relist
	// sweep's boundary.
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
	// Read the clock through s, not a copy, so freezing s.now freezes the cookie too.
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

// ApplyDiff lands one object the diff resync fetched, WITHOUT advancing the resume cookie:
// diff objects arrive by GET in arbitrary RV order, so advancing on one would put the
// cookie ahead of changes the pass hasn't applied and a crash would resume past them. The
// pass clears the cookie before its first write and persists the list's position at the
// end — keeping "a cookie means a completed pass".
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
func (s *objectStore) ApplyDiff(ctx context.Context, u *unstructured.Unstructured) error {
	return s.write(ctx, u, false)
}

// ClearRV drops the resume cookie, so an interrupted pass leaves no position rather than
// one its rows don't back. See ApplyDiff.
func (s *objectStore) ClearRV(ctx context.Context) error {
	return s.resume.Delete(ctx, s.cdb.Writer())
}

// write lands one object and its edges, optionally advancing the cookie, in one
// transaction: no reader may see an object beside its pre-update labels, and no restart
// may resume from a position the rows don't back.
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
	// Same guard as project's; the delete path never reaches it.
	if u == nil || u.Object == nil {
		return fmt.Errorf("objectsync: %s delete carries an empty object", s.kind.Kind)
	}
	// An unkeyable delete errors rather than no-opping: nil would book progress for a
	// delta whose cookie never advanced, so a crash there would resume older and replay.
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

// notify wakes this kind's objects watch, keyed by the plural resource (plus every
// keyless subscriber), so an unrelated kind's write costs it nothing.
func (s *objectStore) notify() {
	s.cdb.ObjectsNotifyResource(s.kind.APIVersion, s.kind.Resource)
}

// insertRow upserts one object and rewrites its edges — the single write chokepoint both
// the watch-delta and relist-page paths route through.
//
// Edges are DELETEd then re-inserted, not upserted: an object that lost a label or
// ownerReference must lose the row too. Both tables are uid-keyed, so it's a point lookup.
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
			-- creationTimestamp is immutable, so a body without it carries no news; project
			-- leaves it 0, which would otherwise overwrite a good value with the epoch.
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

	// At most two statements per edge table (one DELETE, one multi-row INSERT), not one
	// per row: a 500-object relist page runs in one transaction on the cache's shared
	// writer, where per-row statements would mean thousands.
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

// valuesPlaceholders builds "(?,?,?),(?,?,?),…" — rows tuples of cols columns.
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

// recordStatusTransition appends to the status timeline only when the summary CHANGED —
// a relist rewrites every row, so an unconditional insert would bury real transitions
// under a copy of the collection each resync period. The guard is a NOT EXISTS on the
// caller's transaction, not a separate read (which would run on the reader pool, outside
// it) — this is the system's hottest path. A summaryless kind records nothing.
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

// cascadeTables are the per-object side tables and their uid column. Every deleter clears
// these before the objects row, so the list lives here once and no deleter can silently
// skip a table.
//
// owner_refs appears TWICE on purpose: a deleted object is both a child (references out)
// and an owner (its children's references in), and with --cascade=orphan the children
// outlive it, so inbound edges left behind point at a uid that no longer exists.
var cascadeTables = []struct{ table, uidCol string }{
	{"labels", "uid"},
	{"owner_refs", "child_uid"},
	{"owner_refs", "owner_uid"},
	{"status_history", "uid"},
}

// deleteOne removes one object and its edges; the objects delete fires the kind_counts
// trigger, keeping the per-kind tally exact.
func deleteOne(ctx context.Context, ex store.Execer, uid string) error {
	for _, c := range cascadeTables {
		if _, err := ex.ExecContext(ctx, `DELETE FROM `+c.table+` WHERE `+c.uidCol+`=?`, uid); err != nil {
			return err
		}
	}
	_, err := ex.ExecContext(ctx, `DELETE FROM objects WHERE uid=?`, uid)
	return err
}

// sweep deletes this kind's objects matching an extra predicate plus their edges, in a
// fixed number of statements: the edges go through a subquery on the same predicate, so
// nothing is read back into Go (a 20k-object relist would otherwise issue 60k statements
// while holding the cache's shared writer).
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

// Count returns this kind's row count — the warm-cache size a resume reports. It reads
// the trigger-maintained kind_counts rather than scanning the shared objects table once
// per kind at every start; an absent row means nothing written, so 0.
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

// SnapshotRVs returns this kind's cached uid -> resourceVersion, which the diff resync
// compares the cluster's metadata list against. Kind-scoped like everything here.
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
// to the relist's sweep. One transaction and one notify for the whole set, in chunked
// IN (…) statements, so a kind that lost thousands of objects doesn't take thousands of
// commits on the cache's shared writer (and thousands of watch pings) for one reconcile.
//
// It leaves the cookie alone: a deletion carries no resourceVersion, and the pass
// persists the list's RV at the end.
func (s *objectStore) DeleteByUIDs(ctx context.Context, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path

	// Well under SQLite's default 999-variable statement limit.
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

// PersistRV advances this kind's cookie without touching rows — the bookmark path.
func (s *objectStore) PersistRV(ctx context.Context, rv string) error {
	return s.resume.Persist(ctx, s.cdb.Writer(), rv)
}

// ResumeRV returns the cookie to seed a watch from, or "" to force a cold full LIST. A
// partial relist cleared it on its first WritePage, so a cookie means a completed LIST
// landed on disk.
func (s *objectStore) ResumeRV(ctx context.Context) (string, error) {
	return s.resume.Get(ctx)
}

// EnsureCatalog registers this kind in kind_catalog. Load-bearing for reads, not just the
// nav: store.Objects translates the plural resource back to its Kind through this table,
// so a kind with rows and no catalog entry reads as empty. Written by the syncing worker
// and removed by Forget, so the catalog says exactly what this cache holds — which is why
// the discovery controller writes no cache rows of its own.
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

// forgetSupersededKind purges any OTHER Kind holding this kind's plural — all of it, not
// just its catalog row. A plural names one Kind per group-version, so such a row is a CRD
// Kind rename the sidecar slept through, and everything that Kind wrote is now unreachable
// dead weight. It reuses Forget so the two purges can't drift over which tables a kind
// owns.
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
		// Only identity matters: everything Forget deletes is keyed by (api_version, kind).
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

// Forget removes every trace of this kind — rows, edges, catalog entry, resume cookie —
// when its sync child is deleted, so the cache never advertises a kind whose contents are
// frozen at whenever its worker stopped.
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
	// The sweep only decrements the tally to 0 (an advertised-but-empty kind must read 0,
	// not vanish). A forgotten kind is different: nothing will name it again.
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

// BeginReplace opens a streaming full-LIST reconcile of this kind's rows; Commit prunes
// what the LIST didn't carry.
//
// Clearing the resume cookie is DEFERRED to the first WritePage, so a pass failing before
// any write keeps the intact snapshot's cookie, while one failing after writing leaves no
// cookie and the next start cold-LISTs and prunes the leftovers.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
func (s *objectStore) BeginReplace() kubesync.ReplaceSession {
	return &replaceSession{s: s, mark: s.now() + 1}
}

// replaceSession streams a paginated relist into the shared objects table, reconciling
// THIS KIND's rows only (every other kind belongs to another worker's relist).
//
// It reconciles by MARK AND SWEEP, not a keep-set: every WritePage stamps updated_at and
// Commit deletes this kind's rows still older. That keeps the pass O(one page) in memory —
// a keep-set of every uid would defeat pagination — and prunes in a few statements rather
// than a read-back plus three per stale row.
//
// Per-page commits trade whole-pass atomicity for that memory bound: a pass failing
// mid-pagination leaves committed pages visible until the next pass prunes them.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
type replaceSession struct {
	s *objectStore
	// mark is the sweep boundary, one millisecond PAST the session start: updated_at has
	// millisecond resolution, so an inclusive boundary would let a row written in the
	// start's own tick masquerade as one this pass wrote and survive a prune it deserved.
	mark int64
	// cookieCleared makes the first WritePage's cookie clear happen once per session.
	cookieCleared bool
}

// WritePage lands one page in its own transaction, clearing the resume cookie alongside
// the first page's rows (see BeginReplace). A body that won't project (no UID) is skipped,
// not fatal.
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
	// Never below the mark, so this page survives the sweep even if the whole pass runs
	// inside one millisecond.
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
	// Per committed page, not only at Commit: a relist that commits pages then fails
	// would otherwise leave durable rows unannounced until some later write.
	r.s.notify()
	return nil
}

// Commit sweeps this kind's rows no page rewrote, then persists the cookie in the same
// transaction — a failed persist must not leave the cookie durably advanced, which would
// resume the next watch past the objects before it.
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

// project flattens an object body into the objects columns, redacting and stripping the
// body along the way.
func (s *objectStore) project(u *unstructured.Unstructured) (objectRow, error) {
	// A nil body would panic on GetUID inside a worker goroutine, taking the process with
	// it. Items come from a pluggable Source, so the guard belongs on the receiving side.
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
	// Read the ORIGINAL body, not the sanitized copy, whose redaction/strips could drop a
	// field a reading depends on.
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

// sanitize returns the body as it should be stored: server noise removed, Secret values
// redacted. It deep-copies because the caller's body is the live watch object.
//
// Stripping is a real saving — managedFields plus the last-applied annotation are roughly
// half a typical object's bytes and nothing reads them. Reads are then pure pass-through,
// which is what lets ClusterDataObject serve raw_json verbatim.
// See docs/adr/2026-08-09-rawjson-comparable-scalar.md.
func sanitize(u *unstructured.Unstructured) *unstructured.Unstructured {
	out := u.DeepCopy()
	unstructured.RemoveNestedField(out.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(out.Object, "metadata", "annotations",
		"kubectl.kubernetes.io/last-applied-configuration")
	// An annotations map emptied by that removal is noise of its own.
	if ann, ok, _ := unstructured.NestedMap(out.Object, "metadata", "annotations"); ok && len(ann) == 0 {
		unstructured.RemoveNestedField(out.Object, "metadata", "annotations")
	}
	if isSecret(out) {
		redactSecret(out)
	}
	return out
}

// isSecret reads the BODY's own kind, not the worker's configured one, so redaction can't
// be bypassed by how the object was addressed.
func isSecret(u *unstructured.Unstructured) bool {
	return u.GetKind() == "Secret" && u.GetAPIVersion() == "v1"
}

// redactSecret strips a Secret's values, keeping its keys, so the cache file never holds
// the cluster's credentials. stringData is write-only server-side, so it's dropped.
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
