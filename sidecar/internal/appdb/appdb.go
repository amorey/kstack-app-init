// Package appdb owns the sidecar's single app-level SQLite database,
// <data-dir>/app.db. It is the one place that opens the file, holds the one
// forward-only migration sequence for it (embedded migrations/*.sql, applied by
// the shared internal/sqlitemigrate runner), and hands the resulting *sql.DB to
// the feature packages that store app-level data in it.
//
// Why a dedicated owner: a SQLite file has exactly one schema_migrations table
// with a single monotonic version sequence, so its schema cannot be co-owned by
// several feature packages each embedding their own migrations. appdb holds that
// sequence centrally; consumers (today internal/kube's registry, tomorrow
// others) are pure data-access layers over the shared handle and add their
// tables as new numbered migrations here. The per-cluster caches under clusters/
// are a different story — one file per cluster, each its own sequence — and stay
// owned by internal/kube.
//
// app.db deliberately lives outside the clusters/ dir so the per-cluster cache
// scan never mistakes it for a <uuid>.db cache. The pool is a single-connection
// WAL writer (the app-level tables are tiny and low-traffic, so one connection
// sidesteps SQLITE_BUSY entirely).
package appdb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kubetail-org/kstack-app/sidecar/internal/sqlitemigrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Open opens (creating + migrating as needed) app.db at path. A missing parent
// dir is created; a missing file yields an empty, freshly-migrated database.
func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	// Single-connection pool: the app-level tables are low-traffic and
	// single-writer, so one connection avoids SQLITE_BUSY entirely.
	db, err := sqlitemigrate.OpenPool(path, 1)
	if err != nil {
		return nil, err
	}
	if _, err := sqlitemigrate.Apply(context.Background(), db, migrationsFS, "migrations"); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}
