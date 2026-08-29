package sqlitemigrate

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

// openDB opens a fresh on-disk SQLite database in a temp dir. A file (not
// :memory:) so the single *sql.DB can span multiple connections without losing
// the schema, matching how the runner is used in production.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// fsWith writes name->sql pairs into a temp "migrations" dir and returns an
// fs.FS rooted at the temp dir, so Apply(..., fsys, "migrations") reads them.
func fsWith(t *testing.T, files map[string]string) fs.FS {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	for name, body := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}
	return os.DirFS(root)
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&n)
	require.NoError(t, err)
	return n > 0
}

func TestApplyRunsInOrderAndRecords(t *testing.T) {
	db := openDB(t)
	fsys := fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	})

	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.True(t, tableExists(t, db, "alpha"))
	require.True(t, tableExists(t, db, "beta"))

	var versions string
	require.NoError(t, db.QueryRow(
		`SELECT group_concat(version, ',') FROM (SELECT version FROM schema_migrations ORDER BY version)`,
	).Scan(&versions))
	require.Equal(t, "1,2", versions)
}

func TestApplyIsIdempotent(t *testing.T) {
	db := openDB(t)
	fsys := fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	})

	v, err := Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// A second call must be a no-op: it would error ("table alpha already
	// exists") if it re-ran the migration instead of skipping it.
	v, err = Apply(context.Background(), db, fsys, "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)
}

func TestApplyResumesAfterPartial(t *testing.T) {
	db := openDB(t)

	v, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// Now a newer binary ships an additional migration; only v2 should run.
	v, err = Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.True(t, tableExists(t, db, "beta"))
}

func TestApplyRejectsNewerThanBinary(t *testing.T) {
	db := openDB(t)

	// Simulate a DB written by a newer binary: schema at v2 already.
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql":  `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0002_second.sql": `CREATE TABLE beta (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.NoError(t, err)

	// This (older) binary only knows v1 — refuse rather than truncate.
	_, err = Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err)
}

func TestApplyRejectsVersionGap(t *testing.T) {
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"0001_first.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
		"0003_third.sql": `CREATE TABLE gamma (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err, "a gap in version numbers must be caught at startup")
}

func TestApplyRejectsBadFilename(t *testing.T) {
	db := openDB(t)
	_, err := Apply(context.Background(), db, fsWith(t, map[string]string{
		"init.sql": `CREATE TABLE alpha (id INTEGER PRIMARY KEY);`,
	}), "migrations")
	require.Error(t, err, "a file without a numeric version prefix is a packaging bug")
}

// A reader pool that inherits the writer's DSN carries _txlock=immediate, so a read
// transaction that omits ReadOnly takes the WAL write lock — the one thing the reader pool
// exists to avoid, and a latency mystery rather than an error. query_only is what refuses it.
func TestOpenReadPoolRefusesWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	w, err := OpenPool(path, 1)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })
	_, err = w.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1);`)
	require.NoError(t, err)

	r, err := OpenReadPool(path, 2)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	_, err = r.Exec(`INSERT INTO t VALUES (2)`)
	require.Error(t, err, "a write on the reader pool")

	var n int
	require.NoError(t, r.QueryRow(`SELECT count(*) FROM t`).Scan(&n))
	require.Equal(t, 1, n)

	// The trap: under query_only an _txlock=immediate BEGIN fails outright, so this goes
	// red the moment the writer's DSN is copied back onto this pool.
	tx, err := r.BeginTx(context.Background(), nil)
	require.NoError(t, err, "a transaction that did not ask for ReadOnly")
	require.NoError(t, tx.Rollback())
}

// SQLite ignores auto_vacuum once any table exists, so the only place it can be set is the
// connection that creates the file — a migration that set it would be a silent no-op.
func TestOpenPoolCreatesAnIncrementalFile(t *testing.T) {
	db, err := OpenPool(filepath.Join(t.TempDir(), "test.db"), 1)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	const incremental = 2
	var mode int
	require.NoError(t, db.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode))
	require.Equal(t, incremental, mode)
}

// database/sql keeps 2 idle connections by default, so a pool sized above that closes
// exactly the connections it was sized to open — and the next read reopens them, re-running
// the DSN's pragmas, on a path a watch walks every 250ms.
func TestOpenPoolKeepsEveryConnectionItOpens(t *testing.T) {
	const n = 4
	db, err := OpenReadPool(filepath.Join(t.TempDir(), "test.db"), n)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// Held all at once: a Conn is exclusive until it is put back, so each checkout has to
	// open a new one. Connections released one at a time would be served by the same one,
	// and Idle would fall short for a reason unrelated to MaxIdleConns. No clock is
	// involved — a connection returns to the pool the moment it is released.
	conns := make([]*sql.Conn, n)
	for i := range conns {
		conns[i], err = db.Conn(context.Background())
		require.NoError(t, err)
	}
	for _, c := range conns {
		require.NoError(t, c.Close())
	}

	require.Equal(t, n, db.Stats().Idle)
	require.Zero(t, db.Stats().MaxIdleClosed)
}
