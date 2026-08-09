// Package appdb owns <data-dir>/app.db: the only place that opens the file, holding its
// single forward-only migration sequence and handing the *sql.DB to consumers.
//
// A SQLite file has ONE schema_migrations sequence, so its schema can't be co-owned by
// packages each embedding their own migrations — consumers add tables as new numbered
// migrations here. (The per-cluster caches are a different story: one file each, owned by
// internal/cluster.) app.db lives outside clusters/ so the cache scan never mistakes it
// for one.
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

// Open opens app.db, creating the parent dir and migrating as needed.
func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	// One connection: the app-level tables are tiny and single-writer, so this
	// sidesteps SQLITE_BUSY entirely.
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
