package appdb

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Open creates a missing parent dir, runs the embedded migrations (so the
// clusters table exists), and the file survives a close/reopen cycle.
func TestOpenCreatesMigratesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "app.db")

	db, err := Open(path)
	require.NoError(t, err)
	require.FileExists(t, path)

	// Migrations ran: the clusters table is queryable.
	var n int
	require.NoError(t, db.SQL().QueryRow(`SELECT count(*) FROM clusters`).Scan(&n))
	require.Equal(t, 0, n)
	require.NoError(t, db.Close())

	// Reopening the same file is idempotent — migrations are not re-applied.
	db2, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	require.NoError(t, db2.SQL().QueryRow(`SELECT count(*) FROM clusters`).Scan(&n))
	require.Equal(t, 0, n)
}
