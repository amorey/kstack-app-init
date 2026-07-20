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

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// One store per (cluster, GVK). Two implementations live here:
//   - objectsStore — the universal `objects` table plus owner_refs, labels, and
//     status_history. Handles every kind except core/v1 and events.k8s.io/v1 Event.
//   - eventsStore — the dedicated `events` table.
//
// Both implement kindStore, driven by kindDrivers against the dynamic client;
// neither depends on a typed Kubernetes client, so CRDs flow through the same paths.

// --- objectsStore -------------------------------------------------------

type objectsStore struct {
	clusterID string
	gvk       schema.GroupVersionKind
	writer    *sql.DB
	cdb       *store.ClusterDB
	ctx       context.Context
}

func newObjectsStore(ctx context.Context, clusterID string, gvk schema.GroupVersionKind, w *sql.DB, cdb *store.ClusterDB) *objectsStore {
	return &objectsStore{clusterID: clusterID, gvk: gvk, writer: w, cdb: cdb, ctx: ctx}
}

func (s *objectsStore) upsert(u *unstructured.Unstructured) error {
	row, isEvent, err := extractObject(u)
	if err != nil {
		return err
	}
	if isEvent {
		// The driver wiring routes event GVKs to an eventsStore, so this is a
		// routing mismatch; log and skip defensively.
		slog.Warn("objectsStore got Event; routing mismatch", "uid", row.UID)
		return nil
	}
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := writeObjectRow(s.ctx, tx, row); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.ObjectsNotify()
	return nil
}

// ApplyChange applies one watch delta: Added/Modified upsert, Deleted removes.
func (s *objectsStore) ApplyChange(t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(u)
	case watch.Deleted:
		return s.DeleteByUID(s.ctx, string(u.GetUID()))
	default:
		return nil
	}
}

// BeginReplace opens a streaming full-LIST reconcile: the paginating caller
// (kindDriver.fullList) streams each page through WritePage — one page's bodies
// in memory at a time, never the whole kind — then Commit prunes and persists.
// Clearing the resume cookie (the kindStore.BeginReplace contract: the cookie means
// "a full LIST completed", so an abandoned pass leaves none and the next start
// cold-LISTs instead of resuming past the half-written rows) is DEFERRED to the first
// WritePage — the first moment partial state can exist — so a pass that fails before
// any page is written (the first LIST errors, the app quits) leaves the untouched
// snapshot's cookie intact and the next start still resumes cheaply.
func (s *objectsStore) BeginReplace(context.Context) (replaceSession, error) {
	return &objectsReplaceSession{s: s, keep: make(map[string]struct{})}, nil
}

// objectsReplaceSession streams a paginated relist into the objects table. Each
// WritePage is its own transaction (so the single writer conn isn't held across
// the network round-trips between pages); the union of every page's uids is
// accumulated in `keep` and the kind-scoped delete-missing prune is deferred to
// Commit, so a row present only in an earlier page is never wrongly pruned.
//
// Per-page commits trade single-transaction atomicity for the memory bound: on a
// warm large-churn relist a reader may briefly see a partial snapshot before Commit
// prunes, and a pass that fails mid-pagination leaves its committed pages visible
// until the next pass's prune reconciles them. The first WritePage clears the resume
// cookie (rewritten only on Commit) in the same transaction that lands the first
// rows, so once any partial state exists it can't be mistaken for a completed sync —
// the next start cold-LISTs. On a cold cache nothing observes the intermediate state
// anyway.
//
// Each committed page Notifies (like every other objects write), so durable rows
// always reach subscribers — not only at Commit. This is what keeps a pass whose
// Commit fails from leaving rows durable-but-unannounced: without it, the fullResync
// retry sees those rows (SnapshotRVs), takes the metadata-diff path, finds nothing
// changed, and would persist only the RV — never notifying — so a subscriber that
// bound before the pages landed would stay blocked despite the objects being present.
type objectsReplaceSession struct {
	s             *objectsStore
	keep          map[string]struct{}
	cookieCleared bool // whether the first WritePage has durably cleared the resume cookie
}

var _ replaceSession = (*objectsReplaceSession)(nil)

func (r *objectsReplaceSession) WritePage(items []*unstructured.Unstructured) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := r.s.writer.BeginTx(r.s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	// Clear the resume cookie in the same transaction as the first rows this pass
	// writes: partial state and a stale "sync completed" cookie can never coexist.
	if !r.cookieCleared {
		if err := deleteResumeRV(r.s.ctx, tx, r.s.gvk); err != nil {
			return err
		}
	}
	for _, u := range items {
		row, isEvent, err := extractObject(u)
		if err != nil {
			slog.Warn("objectsStore.WritePage skip", "err", err)
			continue
		}
		if isEvent {
			continue
		}
		r.keep[row.UID] = struct{}{}
		if err := writeObjectRow(r.s.ctx, tx, row); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.cookieCleared = true
	r.s.cdb.ObjectsNotify()
	return nil
}

func (r *objectsReplaceSession) Commit(resourceVersion string) error {
	s := r.s
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Delete-missing: scope only to this Kind so we don't blow away rows owned by
	// a different driver, and prune against the UNION of all pages' uids (keep).
	rows, err := tx.QueryContext(s.ctx,
		`SELECT uid FROM objects WHERE kind=? AND api_version=?`, s.gvk.Kind, s.gvk.GroupVersion().String())
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			rows.Close()
			return err
		}
		if _, ok := r.keep[uid]; !ok {
			stale = append(stale, uid)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, uid := range stale {
		if err := deleteObjectRows(s.ctx, tx, uid); err != nil {
			return err
		}
	}

	if err := persistListRVMeta(s.ctx, tx, s.gvk, resourceVersion); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.ObjectsNotify()
	return nil
}

// SnapshotRVs returns {uid: resource_version} currently cached for this kind —
// the baseline the metadata-diff resync compares the live metadata list against.
// Scoped by (kind, api_version) exactly like objectsReplaceSession.Commit's
// delete-missing prune.
func (s *objectsStore) SnapshotRVs(ctx context.Context) (map[string]string, error) {
	rows, err := s.writer.QueryContext(ctx,
		`SELECT uid, resource_version FROM objects WHERE kind=? AND api_version=?`,
		s.gvk.Kind, s.gvk.GroupVersion().String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var uid, rv string
		if err := rows.Scan(&uid, &rv); err != nil {
			return nil, err
		}
		out[uid] = rv
	}
	return out, rows.Err()
}

// PersistRV advances the kind's resume cookie (last_list_rv + last_list_at)
// without touching object rows. Called on every watch delta — no transaction:
// two single-row upserts on the serialized writer conn.
func (s *objectsStore) PersistRV(ctx context.Context, rv string) error {
	return persistListRVMeta(ctx, s.writer, s.gvk, rv)
}

// ResumeRV returns the resume cookie ONLY when the cache still holds objects of the kind
// to apply deltas onto — one indexed query gates the cookie read on object existence, so
// a cookie that outlived its objects (a kind removed then re-added) returns "" and the
// driver cold-LISTs instead of resuming past the missing initial LIST.
func (s *objectsStore) ResumeRV(ctx context.Context) (string, error) {
	var v string
	err := s.writer.QueryRowContext(ctx,
		`SELECT value FROM cluster_meta WHERE key=?
		   AND EXISTS(SELECT 1 FROM objects WHERE kind=? AND api_version=?)`,
		lastListRVKey(s.gvk), s.gvk.Kind, s.gvk.GroupVersion().String()).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil // never synced, or the cookie outlived its objects
	}
	return v, err
}

// DeleteByUID removes one object plus its cascade rows (watch deletes and the
// metadata-diff resync use it for uids that vanished from the cluster).
func (s *objectsStore) DeleteByUID(ctx context.Context, uid string) error {
	if uid == "" {
		return nil
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := deleteObjectRows(ctx, tx, uid); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.ObjectsNotify()
	return nil
}

// writeObjectRow runs the three-table atomic write for a single object. owner_refs
// and labels are deleted-then-reinserted because both can shrink across updates and
// a plain upsert wouldn't catch removals. (FKs are omitted to keep deletes cheap.)
func writeObjectRow(ctx context.Context, tx *sql.Tx, r ObjectRow) error {
	now := time.Now().UnixMilli()

	// Status history: only insert when the summary changed. Read the prior value
	// under the txn so a duplicate can't slip in.
	var prev sql.NullString
	if err := tx.QueryRowContext(ctx,
		`SELECT status_summary FROM objects WHERE uid=?`, r.UID).Scan(&prev); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if r.StatusSummary != "" && (!prev.Valid || prev.String != r.StatusSummary) {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO status_history(uid, at, summary) VALUES(?, ?, ?)`,
			r.UID, now, r.StatusSummary); err != nil {
			return err
		}
	}

	rawJSON, err := store.CompressRaw(r.RawJSON)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO objects (
			uid, api_version, kind, namespace, name,
			resource_version, generation, created_at, updated_at,
			status_summary, ready_count, total_count, restart_count, host,
			raw_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uid) DO UPDATE SET
			api_version=excluded.api_version,
			kind=excluded.kind,
			namespace=excluded.namespace,
			name=excluded.name,
			resource_version=excluded.resource_version,
			generation=excluded.generation,
			updated_at=excluded.updated_at,
			status_summary=excluded.status_summary,
			ready_count=excluded.ready_count,
			total_count=excluded.total_count,
			restart_count=excluded.restart_count,
			host=excluded.host,
			raw_json=excluded.raw_json`,
		r.UID, r.APIVersion, r.Kind, r.Namespace, r.Name,
		r.ResourceVersion, r.Generation, r.CreatedAt, now,
		nullIfEmpty(r.StatusSummary), nullableInt(r.ReadyCount), nullableInt(r.TotalCount), nullableInt(r.RestartCount), nullIfEmpty(r.Host),
		rawJSON,
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM owner_refs WHERE child_uid=?`, r.UID); err != nil {
		return err
	}
	for _, o := range r.OwnerRefs {
		ctrl := 0
		if o.IsController {
			ctrl = 1
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO owner_refs(child_uid, owner_uid, is_controller) VALUES(?, ?, ?)`,
			r.UID, o.OwnerUID, ctrl); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM labels WHERE uid=?`, r.UID); err != nil {
		return err
	}
	for k, v := range r.Labels {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO labels(uid, key, value) VALUES(?, ?, ?)`,
			r.UID, k, v); err != nil {
			return err
		}
	}
	return nil
}

// cascadeTables are the per-object child tables and the uid column in each. Both
// deleters below (deleteObjectRows, deleteKindRows) clear these before the objects
// row, so the list lives here once — a new uid-keyed table can't update one deleter
// and silently miss the other. (No FK ON DELETE CASCADE; see writeObjectRow.)
var cascadeTables = []struct{ table, uidCol string }{
	{"labels", "uid"},
	{"owner_refs", "child_uid"},
	{"owner_refs", "owner_uid"},
	{"status_history", "uid"},
}

func deleteObjectRows(ctx context.Context, tx *sql.Tx, uid string) error {
	for _, c := range cascadeTables {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+c.table+` WHERE `+c.uidCol+`=?`, uid); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE uid=?`, uid); err != nil {
		return err
	}
	return nil
}

// kindKey identifies a resource type as stored in the objects table:
// (kind, api_version) where api_version is the GroupVersion string ("v1",
// "apps/v1", "example.com/v1") — the same pair objectsReplaceSession.Commit scopes
// its per-kind delete-missing prune to.
type kindKey struct {
	kind       string
	apiVersion string
}

// pruneOrphanedKinds deletes every objects row (and its cascade rows) whose
// (kind, api_version) is absent from keep, returning the count removed. It evicts
// resource types that no longer exist on the cluster — chiefly an uninstalled CRD,
// which nothing else reaps (no driver runs for a vanished kind, and the janitor
// never sweeps objects). Caller must run this ONLY after a complete discovery, or a
// kind in a transiently-unavailable API group would be wrongly deleted.
func pruneOrphanedKinds(ctx context.Context, writer *sql.DB, keep map[kindKey]struct{}) (int64, error) {
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT kind, api_version FROM objects`)
	if err != nil {
		return 0, err
	}
	var orphans []kindKey
	for rows.Next() {
		var k kindKey
		if err := rows.Scan(&k.kind, &k.apiVersion); err != nil {
			rows.Close()
			return 0, err
		}
		if _, ok := keep[k]; !ok {
			orphans = append(orphans, k)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var total int64
	for _, k := range orphans {
		n, err := deleteKindRows(ctx, tx, k.kind, k.apiVersion)
		if err != nil {
			return 0, err
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}

// deleteKindRows removes every objects row of (kind, apiVersion) plus its
// cascade rows (labels, owner_refs both directions, status_history) in one
// shot — the whole-kind analogue of deleteObjectRows. Returns the number of
// objects rows deleted.
func deleteKindRows(ctx context.Context, tx *sql.Tx, kind, apiVersion string) (int64, error) {
	const sel = `SELECT uid FROM objects WHERE kind=? AND api_version=?`
	for _, c := range cascadeTables {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+c.table+` WHERE `+c.uidCol+` IN (`+sel+`)`, kind, apiVersion); err != nil {
			return 0, err
		}
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE kind=? AND api_version=?`, kind, apiVersion)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// --- eventsStore --------------------------------------------------------

type eventsStore struct {
	clusterID string
	gvk       schema.GroupVersionKind
	writer    *sql.DB
	cdb       *store.ClusterDB
	ctx       context.Context
}

func newEventsStore(ctx context.Context, clusterID string, gvk schema.GroupVersionKind, w *sql.DB, cdb *store.ClusterDB) *eventsStore {
	return &eventsStore{clusterID: clusterID, gvk: gvk, writer: w, cdb: cdb, ctx: ctx}
}

// execer is the subset of *sql.DB / *sql.Tx that insertEventRow needs, so the
// incremental upsert (direct on the writer) and the relist WritePage (inside
// a txn) can share one INSERT.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// insertEventRow upserts one event, compressing raw_json. The single write
// chokepoint for the events table — both event write paths route through it.
func (s *eventsStore) insertEventRow(ex execer, row EventRow, now int64) error {
	rawJSON, err := store.CompressRaw(row.RawJSON)
	if err != nil {
		return err
	}
	_, err = ex.ExecContext(s.ctx, `
		INSERT INTO events (
			uid, involved_uid, involved_kind, involved_ns, involved_name,
			type, reason, message, first_seen, last_seen, count, raw_json, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uid) DO UPDATE SET
			involved_uid=excluded.involved_uid,
			involved_kind=excluded.involved_kind,
			involved_ns=excluded.involved_ns,
			involved_name=excluded.involved_name,
			type=excluded.type,
			reason=excluded.reason,
			message=excluded.message,
			first_seen=excluded.first_seen,
			last_seen=excluded.last_seen,
			count=excluded.count,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at`,
		row.UID, nullIfEmpty(row.InvolvedUID), nullIfEmpty(row.InvolvedKind),
		nullIfEmpty(row.InvolvedNS), nullIfEmpty(row.InvolvedName),
		nullIfEmpty(row.Type), nullIfEmpty(row.Reason), nullIfEmpty(row.Message),
		nullableInt64(row.FirstSeen), nullableInt64(row.LastSeen), row.Count, rawJSON, now,
	)
	return err
}

func (s *eventsStore) upsert(u *unstructured.Unstructured) error {
	row, err := extractEvent(u)
	if err != nil {
		return err
	}
	if err := s.insertEventRow(s.writer, row, time.Now().UnixMilli()); err != nil {
		return err
	}
	// Wake the events watch on the dedicated events broker — not the object-write
	// broker — so an event burst never triggers the (unrelated) kind-catalog re-read.
	// The events watch coalesces + debounces these pings, so a burst still collapses
	// into one re-read.
	s.cdb.EventsNotify()
	return nil
}

func (s *eventsStore) delete(u *unstructured.Unstructured) error {
	uid := string(u.GetUID())
	if uid == "" {
		return nil
	}
	if _, err := s.writer.ExecContext(s.ctx, `DELETE FROM events WHERE uid=?`, uid); err != nil {
		return err
	}
	s.cdb.EventsNotify()
	return nil
}

// ApplyChange applies one watch delta: Added/Modified upsert, Deleted removes.
func (s *eventsStore) ApplyChange(t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(u)
	case watch.Deleted:
		return s.delete(u)
	default:
		return nil
	}
}

// BeginReplace opens a streaming full-LIST reconcile for the events table. Like
// objects, it runs a delete-missing prune (in Commit) so the cache mirrors the
// server's current event set — a row absent from the LIST is dropped. Retention is
// therefore whatever the server enforces (--event-ttl); the janitor no longer manages
// events. Every row's removal is a relist prune, an explicit watch Deleted
// (ApplyChange), or the two combined.
//
// Clearing the resume cookie (per the kindStore.BeginReplace contract) is DEFERRED to
// the first WritePage, matching objectsStore: a pass that fails before any page is
// written leaves the untouched snapshot's cookie intact so the next start resumes
// cheaply. Now that a relist prunes, this clear is load-bearing — a pass that fails
// after writing pages but before Commit leaves no cookie, so the next start cold-LISTs
// and its prune reconciles the leftover rows instead of resuming past them.
//
// eventsStore does NOT implement metadataDiffStore (the events table has no
// resource_version column), so fullResync's type assertion routes events to a
// plain full LIST.
func (s *eventsStore) BeginReplace(context.Context) (replaceSession, error) {
	return &eventsReplaceSession{s: s, keep: make(map[string]struct{})}, nil
}

// eventsReplaceSession streams a paginated event relist into the events table,
// mirroring the cluster: each page's uids accumulate in `keep` and Commit runs a
// delete-missing prune against their union, so a row absent from the server's LIST
// is dropped. The cache reflects the server's current event set (inheriting
// whatever server-side retention --event-ttl enforces) rather than accumulating
// history; the janitor no longer manages event retention. Unlike objectsReplaceSession
// the prune isn't scoped by (kind, api_version): Event is a single kind in its own
// table, so every row is in this session's scope.
//
// Per-page commits trade single-transaction atomicity for the memory bound (same as
// objects): a pass that fails mid-pagination leaves its committed pages visible until
// the next pass's prune reconciles them. The first WritePage clears the resume cookie
// (rewritten only on Commit) in the same transaction that lands the first rows, so
// partial state and a stale "sync completed" cookie can never coexist — a pass that
// fails before Commit leaves no cookie and the next start cold-LISTs, whose prune
// reconciles the leftover rows.
type eventsReplaceSession struct {
	s             *eventsStore
	keep          map[string]struct{}
	cookieCleared bool // whether the first WritePage has durably cleared the resume cookie
}

var _ replaceSession = (*eventsReplaceSession)(nil)

func (r *eventsReplaceSession) WritePage(items []*unstructured.Unstructured) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := r.s.writer.BeginTx(r.s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	// Clear the resume cookie in the same transaction as the first upserted page (see
	// BeginReplace), so the deferred-clear shape matches objectsStore.
	if !r.cookieCleared {
		if err := deleteResumeRV(r.s.ctx, tx, r.s.gvk); err != nil {
			return err
		}
	}
	now := time.Now().UnixMilli()
	for _, u := range items {
		row, err := extractEvent(u)
		if err != nil {
			continue
		}
		r.keep[row.UID] = struct{}{}
		if err := r.s.insertEventRow(tx, row, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.cookieCleared = true
	// Notify per committed page (like objectsReplaceSession.WritePage), so durable rows
	// always reach the events watch — not only at Commit. A relist that commits pages
	// then fails before Commit (an expired continuation token, a later page error) would
	// otherwise leave those rows durable-but-unannounced until an unrelated later event
	// write or a successful relist.
	r.s.cdb.EventsNotify()
	return nil
}

// Commit runs the delete-missing prune (against the union of all pages' uids in
// `keep`) then persists the resume cookie, so the cache mirrors the server's current
// event set. The prune's per-row deletes fire the events_kind_count triggers, keeping
// the kind_counts Event tally exact. The cookie's two cluster_meta writes (last_list_rv
// + last_list_at) go in ONE transaction so a failed persist can't leave last_list_rv
// durably advanced — which would resume the next watch from a newer RV and skip the
// Added events before it.
func (r *eventsReplaceSession) Commit(resourceVersion string) error {
	tx, err := r.s.writer.BeginTx(r.s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Delete-missing: every event row not seen in this relist is gone from the
	// server. Not scoped by kind — Event owns the whole events table.
	rows, err := tx.QueryContext(r.s.ctx, `SELECT uid FROM events`)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			rows.Close()
			return err
		}
		if _, ok := r.keep[uid]; !ok {
			stale = append(stale, uid)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, uid := range stale {
		if _, err := tx.ExecContext(r.s.ctx, `DELETE FROM events WHERE uid=?`, uid); err != nil {
			return err
		}
	}

	if err := persistListRVMeta(r.s.ctx, tx, r.s.gvk, resourceVersion); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// A relist prune adds/removes rows, so wake the events watch (dedicated broker).
	r.s.cdb.EventsNotify()
	return nil
}

func (s *eventsStore) PersistRV(ctx context.Context, rv string) error {
	return persistListRVMeta(ctx, s.writer, s.gvk, rv)
}

// ResumeRV returns the event kind's resume cookie unguarded: Event is a single,
// always-present kind in its own table, so it isn't subject to the objects-table
// remove/re-add churn the objectsStore existence-guard defends against. A completed
// relist's cookie always validly resumes (even against an empty table — the relist
// reconciled it); a partial pass cleared the cookie on its first WritePage, so the
// next start cold-LISTs and its prune reconciles the leftover rows.
func (s *eventsStore) ResumeRV(ctx context.Context) (string, error) {
	return readLastListRV(ctx, s.writer, s.gvk)
}

// --- helpers ------------------------------------------------------------

func upsertClusterMeta(ctx context.Context, ex execer, key, value string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value)
	return err
}

// lastListRVSuffix / lastListAtSuffix are the cluster_meta key suffixes marking a
// kind's resume cookie (the resume RV and its timestamp). They have a single owner
// here so the key builders and any query that detects/sweeps a resume cookie can't
// drift apart.
const (
	lastListRVSuffix = ".last_list_rv"
	lastListAtSuffix = ".last_list_at"
)

// lastListRVKey is the cluster_meta key holding a kind's resume cookie — the
// resourceVersion to seed the next watch from. readLastListRV reads it;
// persistListRVMeta writes it.
func lastListRVKey(gvk schema.GroupVersionKind) string {
	return fmt.Sprintf("%s/%s%s", gvk.GroupVersion().String(), gvk.Kind, lastListRVSuffix)
}

// lastListAtKey is the companion key holding when that resume cookie was last written.
func lastListAtKey(gvk schema.GroupVersionKind) string {
	return fmt.Sprintf("%s/%s%s", gvk.GroupVersion().String(), gvk.Kind, lastListAtSuffix)
}

// persistListRVMeta writes a kind's resume cookie (last_list_rv + last_list_at).
// It takes an execer so it works both inside a Commit transaction and directly
// on the writer for the per-delta PersistRV; the shared helper keeps the two key
// spellings from drifting.
func persistListRVMeta(ctx context.Context, ex execer, gvk schema.GroupVersionKind, rv string) error {
	if err := upsertClusterMeta(ctx, ex, lastListRVKey(gvk), rv); err != nil {
		return err
	}
	return upsertClusterMeta(ctx, ex, lastListAtKey(gvk),
		strconv.FormatInt(time.Now().UnixMilli(), 10))
}

// pruneOrphanedResumeCookies deletes the resume-cookie rows (last_list_rv + last_list_at)
// in cluster_meta whose cluster_meta key is absent from keep — the resume state of a
// kind that no longer exists on the cluster (an uninstalled CRD / removed APIService).
// Left behind, a later re-registration of that GVR against a server that kept its
// objects would make the driver resume its watch from the stale RV and SKIP the initial
// LIST, so its existing objects get no Added events and the cache stays empty for that
// kind. Caller must run this ONLY after a complete discovery (same gate as the object
// prune) and once any removed kind's driver has quiesced, so a live/transiently-absent
// kind's cookie isn't wrongly cleared. Returns the count removed.
func pruneOrphanedResumeCookies(ctx context.Context, writer *sql.DB, keep map[string]struct{}) (int64, error) {
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx, `SELECT key FROM cluster_meta`)
	if err != nil {
		return 0, err
	}
	var orphans []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return 0, err
		}
		// Only resume-cookie keys; other cluster_meta rows (e.g. cluster identity) are
		// left alone. Exact-suffix match, not SQL LIKE, since the suffix contains '_'
		// (a LIKE single-char wildcard).
		if !strings.HasSuffix(k, lastListRVSuffix) && !strings.HasSuffix(k, lastListAtSuffix) {
			continue
		}
		if _, ok := keep[k]; !ok {
			orphans = append(orphans, k)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, k := range orphans {
		if _, err := tx.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key=?`, k); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(orphans)), nil
}

// deleteResumeRV durably removes a kind's resume cookie (last_list_rv + last_list_at)
// so the next driver for it cold-LISTs. Takes an execer so it runs either directly on
// the writer (the GVR-repoint path, before relaunching the replacement — the old rows
// survive the repoint under the same GVK, so a surviving cookie would let a restart in
// the relaunch window resume the new endpoint from the stale RV) or inside a
// replaceSession's first-page transaction (BeginReplace's deferred clear).
func deleteResumeRV(ctx context.Context, ex execer, gvk schema.GroupVersionKind) error {
	_, err := ex.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key IN (?, ?)`,
		lastListRVKey(gvk), lastListAtKey(gvk))
	return err
}

// readLastListRV reads a kind's persisted resume cookie, returning "" when the
// kind has never synced (so the driver starts with a full re-sync).
func readLastListRV(ctx context.Context, db *sql.DB, gvk schema.GroupVersionKind) (string, error) {
	var v string
	err := db.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key=?`, lastListRVKey(gvk)).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return v, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// nullableInt64 writes NULL for a zero timestamp so a missing time reads as absence,
// not a bogus 1970/0001 instant: an event without any lastTimestamp/eventTime stores
// NULL rather than 0, which the read side surfaces as a null wire timestamp (via the
// ClusterDataEvent firstSeen/lastSeen resolvers) and which a COALESCE(last_seen,
// updated_at) ordering fallback can substitute ingest time for (see the events-ordering
// TODO).
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
