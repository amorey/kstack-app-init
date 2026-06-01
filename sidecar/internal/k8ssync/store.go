package k8ssync

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
	"k8s.io/client-go/tools/cache"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
)

// One store per (cluster, GVK). Two store implementations live here:
//   - objectsStore — writes to the universal `objects` table plus
//     owner_refs, labels, and status_history. Handles every kind except
//     core/v1 Event and events.k8s.io/v1 Event.
//   - eventsStore — writes to the dedicated `events` table.
//
// Both implement cache.Store and are driven by raw Reflectors against
// the dynamic client; nothing about either implementation depends on a
// typed Kubernetes client. CRDs flow through the same code paths.

// --- objectsStore -------------------------------------------------------

type objectsStore struct {
	clusterUUID string
	gvk         schema.GroupVersionKind
	writer      *sql.DB
	cdb         *clustercache.ClusterDB
	ctx         context.Context
}

func newObjectsStore(ctx context.Context, uuid string, gvk schema.GroupVersionKind, w *sql.DB, cdb *clustercache.ClusterDB) *objectsStore {
	return &objectsStore{clusterUUID: uuid, gvk: gvk, writer: w, cdb: cdb, ctx: ctx}
}

func (s *objectsStore) Add(obj any) error    { return s.upsert(obj) }
func (s *objectsStore) Update(obj any) error { return s.upsert(obj) }

func (s *objectsStore) Delete(obj any) error {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("objectsStore.Delete: expected *unstructured, got %T", obj)
	}
	uid := string(u.GetUID())
	if uid == "" {
		return nil
	}
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := deleteObjectRows(s.ctx, tx, uid); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.Notify()
	return nil
}

func (s *objectsStore) upsert(obj any) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("objectsStore.upsert: expected *unstructured, got %T", obj)
	}
	row, isEvent, err := extractObject(u)
	if err != nil {
		return err
	}
	if isEvent {
		// Reflector should never deliver events here (we use eventsStore
		// for the event GVKs). Defensive: log and skip.
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

// Replace reconciles the table for this kind to exactly `items`. Atomic:
// a crash mid-reconcile leaves the previous snapshot intact.
func (s *objectsStore) Replace(items []any, resourceVersion string) error {
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	keep := make(map[string]struct{}, len(items))
	for _, it := range items {
		u, ok := it.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("objectsStore.Replace: expected *unstructured, got %T", it)
		}
		row, isEvent, err := extractObject(u)
		if err != nil {
			slog.Warn("objectsStore.Replace skip", "err", err)
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
	// owned by a different reflector. The kind catalog supplies the key.
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

	if err := upsertClusterMeta(s.ctx, tx,
		fmt.Sprintf("%s/%s.last_list_rv", s.gvk.GroupVersion().String(), s.gvk.Kind), resourceVersion); err != nil {
		return err
	}
	if err := upsertClusterMeta(s.ctx, tx,
		fmt.Sprintf("%s/%s.last_list_at", s.gvk.GroupVersion().String(), s.gvk.Kind),
		strconv.FormatInt(time.Now().UnixMilli(), 10)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.Notify()
	return nil
}

func (s *objectsStore) Resync() error                        { return nil }
func (s *objectsStore) Bookmark(string)                      {}
func (s *objectsStore) LastStoreSyncResourceVersion() string { return "" }
func (s *objectsStore) List() []any                          { return nil }
func (s *objectsStore) ListKeys() []string                   { return nil }
func (s *objectsStore) Get(any) (any, bool, error)           { return nil, false, nil }
func (s *objectsStore) GetByKey(string) (any, bool, error)   { return nil, false, nil }

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

	rawJSON, err := clustercache.CompressRaw(r.RawJSON)
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
// "apps/v1", "example.com/v1") — the same pair objectsStore.Replace scopes its
// per-kind delete-missing prune to.
type kindKey struct {
	kind       string
	apiVersion string
}

// pruneOrphanedKinds deletes every objects row (and its cascade rows) whose
// (kind, api_version) is absent from keep. Run only after a *complete*
// discovery: it evicts resource types that no longer exist on the cluster —
// most importantly an uninstalled CRD, whose custom resources would otherwise
// linger forever. The per-kind Replace prune can't reap them because no
// reflector runs for a kind that vanished from discovery, and the janitor only
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
	clusterUUID string
	gvk         schema.GroupVersionKind
	writer      *sql.DB
	cdb         *clustercache.ClusterDB
	ctx         context.Context
}

func newEventsStore(ctx context.Context, uuid string, gvk schema.GroupVersionKind, w *sql.DB, cdb *clustercache.ClusterDB) *eventsStore {
	return &eventsStore{clusterUUID: uuid, gvk: gvk, writer: w, cdb: cdb, ctx: ctx}
}

func (s *eventsStore) Add(obj any) error    { return s.upsert(obj) }
func (s *eventsStore) Update(obj any) error { return s.upsert(obj) }

func (s *eventsStore) Delete(obj any) error {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("eventsStore.Delete: %T", obj)
	}
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

// execer is the subset of *sql.DB / *sql.Tx that insertEventRow needs, so the
// incremental upsert (direct on the writer) and the relist Replace (inside a
// txn) can share one INSERT.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// insertEventRow upserts one event, compressing raw_json. The single write
// chokepoint for the events table — both event write paths route through it.
func (s *eventsStore) insertEventRow(ex execer, row EventRow, now int64) error {
	rawJSON, err := clustercache.CompressRaw(row.RawJSON)
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

func (s *eventsStore) upsert(obj any) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("eventsStore.upsert: %T", obj)
	}
	row, err := extractEvent(u)
	if err != nil {
		return err
	}
	return s.insertEventRow(s.writer, row, time.Now().UnixMilli())
}

// Replace prunes events that didn't appear in the LIST. Events are
// short-lived server-side (default 1h TTL) so a relist routinely drops
// half the table — this is normal, not a bug.
func (s *eventsStore) Replace(items []any, resourceVersion string) error {
	tx, err := s.writer.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UnixMilli()
	for _, it := range items {
		u, ok := it.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		row, err := extractEvent(u)
		if err != nil {
			continue
		}
		if err := s.insertEventRow(tx, row, now); err != nil {
			return err
		}
	}

	// Deliberately no delete-missing pass for events. The two reflectors
	// (core/v1 and events.k8s.io/v1) share the events table with no
	// cross-store coordination, so a "delete UIDs not in this LIST" sweep
	// would let each reflector delete the other's rows. Instead we rely on
	// the per-event upsert plus the janitor's TTL: stale rows age out and
	// subsequent LISTs converge.

	if err := upsertClusterMeta(s.ctx, tx,
		fmt.Sprintf("%s/%s.last_list_rv", s.gvk.GroupVersion().String(), s.gvk.Kind), resourceVersion); err != nil {
		return err
	}
	if err := upsertClusterMeta(s.ctx, tx,
		fmt.Sprintf("%s/%s.last_list_at", s.gvk.GroupVersion().String(), s.gvk.Kind),
		strconv.FormatInt(time.Now().UnixMilli(), 10)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *eventsStore) Resync() error                        { return nil }
func (s *eventsStore) Bookmark(string)                      {}
func (s *eventsStore) LastStoreSyncResourceVersion() string { return "" }
func (s *eventsStore) List() []any                          { return nil }
func (s *eventsStore) ListKeys() []string                   { return nil }
func (s *eventsStore) Get(any) (any, bool, error)           { return nil, false, nil }
func (s *eventsStore) GetByKey(string) (any, bool, error)   { return nil, false, nil }

// --- helpers ------------------------------------------------------------

func upsertClusterMeta(ctx context.Context, tx *sql.Tx, key, value string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value)
	return err
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
