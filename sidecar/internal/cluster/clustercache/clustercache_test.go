package clustercache

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenMigrateClose(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()

	cdb, err := r.Open(ctx, "cluster-a")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(dir, "clusters", "cluster-a.db"))

	var n int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&n))
	require.Greater(t, n, 0, "expected at least one migration recorded")

	require.NoError(t, r.Shutdown(ctx))
}

// auto_vacuum must end up INCREMENTAL (2) so the janitor's incremental_vacuum
// can return trimmed pages to the OS. It has to be set before any table exists,
// including the migration runner's schema_migrations table — a regression here
// is silent (the DB just grows), so guard the resulting mode explicitly.
func TestAutoVacuumIsIncremental(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()

	cdb, err := r.Open(ctx, "cluster-a")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	var mode int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&mode))
	require.Equal(t, 2, mode, "auto_vacuum should be INCREMENTAL (2)")
}

// status_history is intentionally a plain rowid table with no (uid, at) primary
// key, so two distinct status transitions for the same object that land in the
// same millisecond both survive. Re-adding a unique constraint would silently
// drop the second transition — guard the schema directly.
func TestStatusHistoryKeepsSameMillisecondTransitions(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()

	cdb, err := r.Open(ctx, "cluster-a")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	const uid, at = "pod-1", int64(1700000000000)
	for _, summary := range []string{"Pending", "Running"} {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO status_history(uid, at, summary) VALUES(?, ?, ?)`, uid, at, summary)
		require.NoError(t, err, "same (uid, at) must not collide")
	}

	var n int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM status_history WHERE uid=? AND at=?`, uid, at).Scan(&n))
	require.Equal(t, 2, n, "both same-millisecond transitions should persist")
}

// A cluster UUID becomes a filesystem path, so DeleteCacheFiles (and Open) must
// reject path-traversal values — otherwise "../foo" would delete foo.db
// outside the clusters dir.
func TestDeleteCacheFilesRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()

	// A sentinel sibling of the clusters dir that must survive a traversal attempt.
	sentinel := filepath.Join(dir, "foo.db")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0o600))

	require.Error(t, r.DeleteCacheFiles(ctx, "../foo"))
	require.FileExists(t, sentinel, "file outside clusters dir must not be deleted")

	_, err := r.Open(ctx, "../foo")
	require.Error(t, err, "Open rejects a traversal UUID")
}

func TestOpenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()

	a, err := r.Open(ctx, "cluster-a")
	require.NoError(t, err)
	b, err := r.Open(ctx, "cluster-a")
	require.NoError(t, err)
	require.Same(t, a, b, "second Open should return the same handle")

	require.NoError(t, r.Shutdown(ctx))
}

func TestReopenRunsNoMigrations(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	r1 := NewManager(dir, nil)
	cdb1, err := r1.Open(ctx, "c")
	require.NoError(t, err)
	var firstCount int
	require.NoError(t, cdb1.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&firstCount))
	require.NoError(t, r1.Shutdown(ctx))

	r2 := NewManager(dir, nil)
	cdb2, err := r2.Open(ctx, "c")
	require.NoError(t, err)
	var secondCount int
	require.NoError(t, cdb2.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schema_migrations`).Scan(&secondCount))
	require.Equal(t, firstCount, secondCount, "reopen should not re-apply migrations")
	require.NoError(t, r2.Shutdown(ctx))
}

func TestConcurrentReadersDuringWriter(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()
	cdb, err := r.Open(ctx, "c")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	// Writer: 1000 pod upserts in a single transaction.
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- func() error {
			tx, err := cdb.Writer().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for i := range 1000 {
				if _, err := tx.ExecContext(ctx,
					`INSERT OR REPLACE INTO objects
					 (uid, api_version, kind, namespace, name, resource_version, generation,
					  created_at, updated_at, status_summary, raw_json)
					 VALUES (?, 'v1', 'Pod', 'default', ?, '1', 1, ?, ?, 'Running', '{}')`,
					"uid-"+itoa(i), "pod-"+itoa(i), time.Now().UnixMilli(), time.Now().UnixMilli(),
				); err != nil {
					return err
				}
			}
			return tx.Commit()
		}()
	}()

	// Readers run concurrently. They should see either pre- or post-commit
	// state — never an error.
	var wg sync.WaitGroup
	readerErr := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				var n int
				if err := cdb.Reader().QueryRowContext(ctx,
					`SELECT COUNT(*) FROM objects WHERE kind='Pod'`).Scan(&n); err != nil {
					readerErr <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(readerErr)
	for err := range readerErr {
		require.NoError(t, err, "reader saw SQLITE_BUSY or similar during writer txn")
	}
	require.NoError(t, <-writerDone)

	var final int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM objects WHERE kind='Pod'`).Scan(&final))
	require.Equal(t, 1000, final)
}

func TestCorruptFileQuarantined(t *testing.T) {
	dir := t.TempDir()
	clustersDir := filepath.Join(dir, "clusters")
	require.NoError(t, os.MkdirAll(clustersDir, 0o700))
	dbPath := filepath.Join(clustersDir, "broken.db")
	// SQLite files start with a magic string; arbitrary bytes are an
	// invalid header and will fail integrity_check on first query.
	require.NoError(t, os.WriteFile(dbPath, []byte("not a sqlite file"), 0o600))

	r := NewManager(dir, nil)
	ctx := context.Background()
	cdb, err := r.Open(ctx, "broken")
	require.NoError(t, err, "open should recover from a corrupt file by quarantining it")
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	matches, err := filepath.Glob(filepath.Join(clustersDir, "broken.db.corrupt-*"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "expected a quarantined .corrupt-* file")

	// New DB is usable.
	require.NoError(t, cdb.Reader().PingContext(ctx))
}

func TestJanitorTrimsStaleEvents(t *testing.T) {
	dir := t.TempDir()
	r := NewManager(dir, nil)
	ctx := context.Background()
	cdb, err := r.Open(ctx, "c")
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown(ctx) })

	now := time.Now().UnixMilli()
	hourAgo := now - (60 * 60 * 1000)
	twoDaysAgo := now - (48 * 60 * 60 * 1000)

	// Two events: one within 24h, one beyond.
	for i, last := range []int64{hourAgo, twoDaysAgo} {
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO events(uid, type, reason, message, first_seen, last_seen, count, raw_json, updated_at)
			 VALUES(?, 'Normal', 'Test', 'hello', ?, ?, 1, '{}', ?)`,
			"ev-"+itoa(i), last, last, now,
		)
		require.NoError(t, err)
	}

	sweep(ctx, "c", cdb.Writer(), Retention{
		EventsTTL:        24 * time.Hour,
		StatusHistoryTTL: 7 * 24 * time.Hour,
		Interval:         time.Minute,
	})

	var count int
	require.NoError(t, cdb.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count))
	require.Equal(t, 1, count, "expected only the recent event to remain")
}

func TestSyncRunnerLifecycle(t *testing.T) {
	dir := t.TempDir()
	up := &fakeUpstream{started: make(chan struct{}, 1)}
	r := NewManager(dir, up)
	ctx := context.Background()

	_, err := r.Open(ctx, "c")
	require.NoError(t, err)
	select {
	case <-up.started:
	case <-time.After(2 * time.Second):
		t.Fatal("sync runner did not start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(shutdownCtx))
	require.True(t, up.ctxObservedDone(), "sync runner ctx should be cancelled on shutdown")
}

// A sync runner that exits abnormally while the cluster is still open (an
// error, or a premature nil from transient empty discovery) must be retried —
// the coordinator keeps the cluster in its open set and won't re-Open it, so
// without self-retry the cluster would stay unsynced until restart.
func TestSyncRunnerRetriesAfterError(t *testing.T) {
	orig := syncRetryInitial
	syncRetryInitial = time.Millisecond
	t.Cleanup(func() { syncRetryInitial = orig })

	dir := t.TempDir()
	up := &flakyUpstream{calls: make(chan struct{}, 8)}
	r := NewManager(dir, up)
	ctx := context.Background()

	_, err := r.Open(ctx, "c")
	require.NoError(t, err)

	// First two Run returns are abnormal (error then premature nil); both must
	// be followed by another call.
	for i := 0; i < 3; i++ {
		select {
		case <-up.calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected sync runner call %d after a prior abnormal exit", i+1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(shutdownCtx))
}

type fakeUpstream struct {
	started chan struct{}

	mu      sync.Mutex
	ctxDone bool
}

func (f *fakeUpstream) Run(ctx context.Context, uuid string, w *sql.DB) error {
	f.started <- struct{}{}
	<-ctx.Done()
	f.mu.Lock()
	f.ctxDone = true
	f.mu.Unlock()
	return ctx.Err()
}

// flakyUpstream returns abnormally on the first two calls (an error, then a
// premature nil) and blocks on ctx thereafter, modelling a transient discovery
// failure that resolves on retry.
type flakyUpstream struct {
	calls chan struct{}
	n     atomic.Int32
}

func (f *flakyUpstream) Run(ctx context.Context, uuid string, w *sql.DB) error {
	f.calls <- struct{}{}
	switch f.n.Add(1) {
	case 1:
		return fmt.Errorf("transient startup error")
	case 2:
		return nil // premature success — discovery found nothing
	default:
		<-ctx.Done()
		return ctx.Err()
	}
}

func (f *fakeUpstream) ctxObservedDone() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ctxDone
}

func itoa(i int) string {
	// fmt.Sprintf would pull fmt into the hot loop; this is tight and
	// deterministic.
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
