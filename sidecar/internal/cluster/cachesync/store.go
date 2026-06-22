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

package cachesync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/store"
)

// One store per (cluster, GVK). Two store implementations live here:
//   - objectsStore — writes to the universal `objects` table plus
//     owner_refs, labels, and status_history. Handles every kind except
//     core/v1 Event and events.k8s.io/v1 Event.
//   - eventsStore — writes to the dedicated `events` table.
//
// Both implement kindStore and are driven by kindDrivers against the dynamic
// client; nothing about either implementation depends on a typed Kubernetes
// client. CRDs flow through the same code paths.

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
		// The driver wiring should never deliver events here (the event GVKs
		// get an eventsStore). Defensive: log and skip.
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
	s.cdb.Notify()
	return nil
}

// ApplyEvent applies one watch delta: Added/Modified upsert, Deleted removes.
func (s *objectsStore) ApplyEvent(t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(u)
	case watch.Deleted:
		return s.DeleteByUID(s.ctx, string(u.GetUID()))
	default:
		return nil
	}
}

// ReplaceFull reconciles the table for this kind to exactly `items`. Atomic:
// a crash mid-reconcile leaves the previous snapshot intact.
func (s *objectsStore) ReplaceFull(items []*unstructured.Unstructured, resourceVersion string) error {
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	keep := make(map[string]struct{}, len(items))
	for _, u := range items {
		row, isEvent, err := extractObject(u)
		if err != nil {
			slog.Warn("objectsStore.ReplaceFull skip", "err", err)
			continue
		}
		if isEvent {
			continue
		}
		keep[row.UID] = struct{}{}
		if err := writeObjectRow(s.ctx, tx, row); err != nil {
			return err
		}
	}

	// Delete-missing: scope only to this Kind so we don't blow away rows
	// owned by a different driver. The kind catalog supplies the key.
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
		if _, ok := keep[uid]; !ok {
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
	s.cdb.Notify()
	return nil
}

// SnapshotRVs returns {uid: resource_version} currently cached for this kind —
// the baseline the metadata-diff resync compares the live metadata list against.
// Scoped by (kind, api_version) exactly like ReplaceFull's delete-missing prune.
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
	s.cdb.Notify()
	return nil
}

// writeObjectRow runs the three-table atomic write for a single object.
// The order matters: objects first (so owner_refs/labels FKs would be
// satisfied if we had them — we omit FKs to keep deletes cheap), then
// owner_refs and labels are deleted-then-reinserted because both can
// shrink across updates and a simple upsert wouldn't catch removals.
func writeObjectRow(ctx context.Context, tx *sql.Tx, r ObjectRow) error {
	now := time.Now().UnixMilli()

	// Status history: only insert when the summary actually changed.
	// Read the prior value under the txn so concurrent writers (there's
	// only one per cluster, but defensive) can't insert duplicates.
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

// cascadeTables are the per-object child tables and the column in each that
// holds the object's uid. Both deleters below (deleteObjectRows for one uid,
// deleteKindRows for a whole kind) delete from these before deleting the
// objects row, so the list lives here once — adding a new uid-keyed table
// can't update one deleter and silently miss the other. (No FK ON DELETE
// CASCADE does this for us; see writeObjectRow on why FKs are omitted.)
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
// "apps/v1", "example.com/v1") — the same pair objectsStore.ReplaceFull scopes
// its per-kind delete-missing prune to.
type kindKey struct {
	kind       string
	apiVersion string
}

// pruneOrphanedKinds deletes every objects row (and its cascade rows) whose
// (kind, api_version) is absent from keep. Run only after a *complete*
// discovery: it evicts resource types that no longer exist on the cluster —
// most importantly an uninstalled CRD, whose custom resources would otherwise
// linger forever. The per-kind ReplaceFull prune can't reap them because no
// driver runs for a kind that vanished from discovery, and the janitor only
// sweeps events/status_history, never objects. Returns the number of objects
// rows removed. Caller must NOT invoke this on a partial discovery, or a kind
// in a transiently-unavailable API group would be wrongly deleted.
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
// incremental upsert (direct on the writer) and the relist ReplaceFull (inside
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
	return s.insertEventRow(s.writer, row, time.Now().UnixMilli())
}

func (s *eventsStore) delete(u *unstructured.Unstructured) error {
	uid := string(u.GetUID())
	if uid == "" {
		return nil
	}
	if _, err := s.writer.ExecContext(s.ctx, `DELETE FROM events WHERE uid=?`, uid); err != nil {
		return err
	}
	// No Notify: events are read separately and notifying here would
	// hammer every subscription on every event burst.
	return nil
}

// ApplyEvent applies one watch delta: Added/Modified upsert, Deleted removes.
func (s *eventsStore) ApplyEvent(t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(u)
	case watch.Deleted:
		return s.delete(u)
	default:
		return nil
	}
}

// ReplaceFull upserts the LISTed events. Events are short-lived server-side
// (default 1h TTL) so a relist routinely covers half the table — this is
// normal, not a bug.
//
// Deliberately no delete-missing pass for events. The two drivers (core/v1
// and events.k8s.io/v1) share the events table with no cross-store
// coordination, so a "delete UIDs not in this LIST" sweep would let each
// driver delete the other's rows. Instead we rely on the per-event upsert
// plus the janitor's TTL: stale rows age out and subsequent LISTs converge.
//
// Events take resume-by-RV like any kind (the watch loop persists RV and a
// full resync just relists), but eventsStore deliberately does NOT implement
// metadataDiffStore — the events table has no resource_version column and no
// delete-missing, so the metadata-diff doesn't apply. fullResync's type
// assertion routes events to a plain full LIST instead.
func (s *eventsStore) ReplaceFull(items []*unstructured.Unstructured, resourceVersion string) error {
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UnixMilli()
	for _, u := range items {
		row, err := extractEvent(u)
		if err != nil {
			continue
		}
		if err := s.insertEventRow(tx, row, now); err != nil {
			return err
		}
	}

	if err := persistListRVMeta(s.ctx, tx, s.gvk, resourceVersion); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *eventsStore) PersistRV(ctx context.Context, rv string) error {
	return persistListRVMeta(ctx, s.writer, s.gvk, rv)
}

// --- helpers ------------------------------------------------------------

func upsertClusterMeta(ctx context.Context, ex execer, key, value string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value)
	return err
}

// lastListRVKey is the cluster_meta key holding a kind's resume cookie — the
// resourceVersion to seed the next watch from. readLastListRV reads it;
// persistListRVMeta writes it.
func lastListRVKey(gvk schema.GroupVersionKind) string {
	return fmt.Sprintf("%s/%s.last_list_rv", gvk.GroupVersion().String(), gvk.Kind)
}

// persistListRVMeta writes a kind's resume cookie (last_list_rv + last_list_at).
// It takes an execer so it works both inside the full ReplaceFull's transaction
// and directly on the writer for the per-delta PersistRV (two single-row
// upserts on the serialized writer conn — no transaction needed); the shared
// helper keeps the two key spellings from drifting.
func persistListRVMeta(ctx context.Context, ex execer, gvk schema.GroupVersionKind, rv string) error {
	if err := upsertClusterMeta(ctx, ex, lastListRVKey(gvk), rv); err != nil {
		return err
	}
	return upsertClusterMeta(ctx, ex,
		fmt.Sprintf("%s/%s.last_list_at", gvk.GroupVersion().String(), gvk.Kind),
		strconv.FormatInt(time.Now().UnixMilli(), 10))
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

// nullableInt64 writes NULL for a zero timestamp so the janitor's
// COALESCE(last_seen, updated_at) fallback fires. An event without any
// lastTimestamp/eventTime would otherwise store 0, which is non-NULL and
// older than any retention window, getting it swept on the next pass.
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
