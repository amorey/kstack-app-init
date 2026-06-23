package appdb

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Open creates a missing parent dir, runs the embedded migrations (recording
// the sequence in schema_migrations), and the file survives a close/reopen
// cycle. app.db holds no app-level tables yet, so the assertion is that the
// migration runner reached version 1.
func TestOpenCreatesMigratesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "app.db")

	db, err := Open(path)
	require.NoError(t, err)
	require.FileExists(t, path)

	// Migrations ran: the schema_migrations sequence is at version 1.
	var v int
	require.NoError(t, db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v))
	require.Equal(t, 1, v)
	require.NoError(t, db.Close())

	// Reopening the same file is idempotent — migrations are not re-applied.
	db2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	require.NoError(t, db2.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v))
	require.Equal(t, 1, v)
}
