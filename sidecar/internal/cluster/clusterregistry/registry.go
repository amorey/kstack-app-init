// Package clusterregistry is the durable, app-level registry of clusters the
// sidecar has discovered. It records one row per cluster (identified by its
// kube-system UUID) — name, whether the user has enabled syncing, and a few
// timestamps — so the UI can show every known cluster even after it leaves the
// kubeconfig, persist the enable/disable choice, and offer cleanup of orphaned
// caches.
//
// Its `clusters` table lives in the app-level <data-dir>/app.db, owned and
// migrated by internal/appdb; this package is a pure data-access layer over the
// *sql.DB that appdb hands it (it does not open or migrate the file). The
// clustersync.Coordinator is the sole writer; it fans change notifications to
// GraphQL subscribers itself, so this package carries no pub/sub. SQLite is the
// single source of truth — reads hit the DB directly (the table is tiny).
package clusterregistry

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

// Record is one cluster's persisted registry entry. Timestamps are unix-millis;
// 0 means "never".
type Record struct {
	UUID                   string
	Name                   string // last-known kube-context name
	Enabled                bool
	FirstSeenAt            int64
	LastSyncedAt           int64
	LastSeenInKubeconfigAt int64
}

// Registry is the cluster registry backed by the shared app.db handle. Safe for
// concurrent use (appdb's single-connection writer pool serializes mutations).
type Registry struct {
	db  *sql.DB
	now func() int64 // injectable clock (unix-millis) for tests
}

// New builds a Registry over the already-open, already-migrated app.db handle
// from internal/appdb. The handle is owned by appdb — the Registry borrows it
// and never closes it (close the appdb.DB instead).
func New(db *sql.DB) *Registry {
	return &Registry{
		db:  db,
		now: func() int64 { return time.Now().UnixMilli() },
	}
}

// withTx runs fn inside a transaction, rolling back on error and committing
// otherwise. Every mutating method funnels through it so the begin/rollback/
// commit scaffold lives in exactly one place.
func (s *Registry) withTx(fn func(context.Context, *sql.Tx) error) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// rowScanner is the row-like source scanRecord reads from (*sql.Row / *sql.Rows).
type rowScanner interface{ Scan(dest ...any) error }

func scanRecord(sc rowScanner) (Record, error) {
	var (
		r       Record
		enabled int
	)
	err := sc.Scan(&r.UUID, &r.Name, &enabled, &r.FirstSeenAt, &r.LastSyncedAt, &r.LastSeenInKubeconfigAt)
	r.Enabled = enabled != 0
	return r, err
}

const selectCols = `uuid, name, enabled, first_seen_at, last_synced_at, last_seen_in_kubeconfig_at`

// getTx reads one record within tx. ok=false (nil error) for an unknown UUID.
func getTx(ctx context.Context, tx *sql.Tx, uuid string) (Record, bool, error) {
	r, err := scanRecord(tx.QueryRowContext(ctx, `SELECT `+selectCols+` FROM clusters WHERE uuid = ?`, uuid))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	return r, err == nil, err
}

// upsertTx writes a record within tx (insert or full replace).
func upsertTx(ctx context.Context, tx *sql.Tx, r Record) error {
	enabled := 0
	if r.Enabled {
		enabled = 1
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO clusters (`+selectCols+`) VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(uuid) DO UPDATE SET
		   name = excluded.name,
		   enabled = excluded.enabled,
		   first_seen_at = excluded.first_seen_at,
		   last_synced_at = excluded.last_synced_at,
		   last_seen_in_kubeconfig_at = excluded.last_seen_in_kubeconfig_at`,
		r.UUID, r.Name, enabled, r.FirstSeenAt, r.LastSyncedAt, r.LastSeenInKubeconfigAt)
	return err
}

// List returns a snapshot of every known cluster, sorted by name then UUID. A
// query/scan error is logged and surfaces as the partial slice gathered so far;
// the no-error signature keeps callers simple (the registry is local SQLite, so
// a read failure means the DB itself is broken, not a transient miss).
func (s *Registry) List() []Record {
	rows, err := s.db.QueryContext(context.Background(),
		`SELECT `+selectCols+` FROM clusters ORDER BY name, uuid`)
	if err != nil {
		slog.Error("clusterregistry: List query failed", "err", err)
		return nil
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			slog.Error("clusterregistry: List scan failed, returning partial", "count", len(out), "err", err)
			return out
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		slog.Error("clusterregistry: List iteration failed, returning partial", "count", len(out), "err", err)
	}
	return out
}

// Get returns the record for a UUID. ok=false with a nil error means the UUID is
// genuinely unknown; a non-nil error means the read itself failed and the caller
// must not treat that as "absent" (mirrors getTx).
func (s *Registry) Get(uuid string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(context.Background(),
		`SELECT `+selectCols+` FROM clusters WHERE uuid = ?`, uuid))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

// SeenEntry is one cluster observed in the kubeconfig, for RecordSeenBatch.
type SeenEntry struct{ UUID, Name string }

// recordSeenTx applies one observation within tx: a brand-new cluster defaults
// to enabled and stamps first-seen; a non-empty name refreshes the name and
// last-seen timestamp. First-seen is sticky. Returns the resulting record.
func (s *Registry) recordSeenTx(ctx context.Context, tx *sql.Tx, uuid, name string, now int64) (Record, error) {
	r, ok, err := getTx(ctx, tx, uuid)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		r = Record{UUID: uuid, Enabled: true, FirstSeenAt: now}
	}
	if name != "" {
		r.Name = name
		r.LastSeenInKubeconfigAt = now
	}
	if err := upsertTx(ctx, tx, r); err != nil {
		return Record{}, err
	}
	return r, nil
}

// RecordSeen upserts a cluster observed in the kubeconfig: it refreshes the
// name and last-seen timestamp, and for a brand-new cluster defaults it to
// enabled and stamps first-seen. Returns the resulting record. Used both for
// live discovery and for backfilling orphaned cache files (pass name "" for an
// orphan with no known context).
func (s *Registry) RecordSeen(uuid, name string) (Record, error) {
	var r Record
	err := s.withTx(func(ctx context.Context, tx *sql.Tx) error {
		var e error
		r, e = s.recordSeenTx(ctx, tx, uuid, name, s.now())
		return e
	})
	return r, err
}

// RecordSeenBatch upserts many observed clusters in a single transaction.
// Equivalent to calling RecordSeen for each entry — used on the reconcile hot
// path, where every kubeconfig snapshot re-records every context.
func (s *Registry) RecordSeenBatch(entries []SeenEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.withTx(func(ctx context.Context, tx *sql.Tx) error {
		now := s.now()
		for _, e := range entries {
			if _, err := s.recordSeenTx(ctx, tx, e.UUID, e.Name, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetEnabled flips a cluster's enabled flag and returns the updated record.
// ok=false (with nil error) if the UUID is unknown.
func (s *Registry) SetEnabled(uuid string, enabled bool) (Record, bool, error) {
	var (
		r  Record
		ok bool
	)
	err := s.withTx(func(ctx context.Context, tx *sql.Tx) error {
		rec, found, e := getTx(ctx, tx, uuid)
		if e != nil || !found {
			ok = found
			return e
		}
		rec.Enabled = enabled
		if e := upsertTx(ctx, tx, rec); e != nil {
			return e
		}
		r, ok = rec, true
		return nil
	})
	return r, ok, err
}

// SetLastSyncedAtBatch advances the last-synced timestamp for each given cluster
// whose value moved forward (ignoring unknown UUIDs and non-advancing values),
// in a single transaction. Returns whether anything changed. The "only if
// advanced" check lives here so the freshness flusher can hand over its whole
// snapshot without pre-filtering.
func (s *Registry) SetLastSyncedAtBatch(updates map[string]int64) (bool, error) {
	if len(updates) == 0 {
		return false, nil
	}
	changed := false
	err := s.withTx(func(ctx context.Context, tx *sql.Tx) error {
		for uuid, ts := range updates {
			res, err := tx.ExecContext(ctx,
				`UPDATE clusters SET last_synced_at = ? WHERE uuid = ? AND ? > last_synced_at`,
				ts, uuid, ts)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				changed = true
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

// Delete forgets a cluster entirely. No-op for an unknown UUID.
func (s *Registry) Delete(uuid string) error {
	_, err := s.db.ExecContext(context.Background(), `DELETE FROM clusters WHERE uuid = ?`, uuid)
	return err
}
