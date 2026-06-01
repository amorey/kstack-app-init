package clusterregistry

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/appdb"
)

// withClock injects a deterministic clock starting at `start` and incrementing
// by 1 on each call, so timestamp assertions are exact.
func withClock(s *Registry, start int64) *Registry {
	tick := start
	s.now = func() int64 { n := tick; tick++; return n }
	return s
}

// openAt builds a store over a fresh app.db in a temp dir, with a deterministic
// clock. The appdb handle is closed on cleanup.
func openAt(t *testing.T, start int64) *Registry {
	t.Helper()
	db, err := appdb.Open(filepath.Join(t.TempDir(), "app.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return withClock(New(db.SQL()), start)
}

func TestRecordSeenDefaultsEnabledAndStampsFirstSeen(t *testing.T) {
	s := openAt(t, 1000)

	r, err := s.RecordSeen("uid-a", "ctx-a")
	require.NoError(t, err)
	require.True(t, r.Enabled, "new cluster defaults to enabled")
	require.Equal(t, int64(1000), r.FirstSeenAt)
	require.Equal(t, int64(1000), r.LastSeenInKubeconfigAt)
	require.Equal(t, "ctx-a", r.Name)

	// Seeing it again refreshes last-seen but keeps first-seen + enabled.
	r2, err := s.RecordSeen("uid-a", "ctx-a")
	require.NoError(t, err)
	require.Equal(t, int64(1000), r2.FirstSeenAt, "first-seen is sticky")
	require.Equal(t, int64(1001), r2.LastSeenInKubeconfigAt)
}

func TestSetEnabledAndLastSynced(t *testing.T) {
	s := openAt(t, 1)
	_, err := s.RecordSeen("uid-a", "ctx-a")
	require.NoError(t, err)

	r, ok, err := s.SetEnabled("uid-a", false)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, r.Enabled)

	_, ok, err = s.SetEnabled("missing", true)
	require.NoError(t, err)
	require.False(t, ok, "unknown UUID is a no-op")

	changed, err := s.SetLastSyncedAtBatch(map[string]int64{"uid-a": 4242, "missing": 1})
	require.NoError(t, err)
	require.True(t, changed)
	got, _, err := s.Get("uid-a")
	require.NoError(t, err)
	require.Equal(t, int64(4242), got.LastSyncedAt)

	// A non-advancing value (and unknown UUIDs) are ignored.
	changed, err = s.SetLastSyncedAtBatch(map[string]int64{"uid-a": 100})
	require.NoError(t, err)
	require.False(t, changed)
}

func TestDeleteAndList(t *testing.T) {
	s := openAt(t, 1)
	_, _ = s.RecordSeen("uid-b", "beta")
	_, _ = s.RecordSeen("uid-a", "alpha")

	list := s.List()
	require.Len(t, list, 2)
	require.Equal(t, "alpha", list[0].Name, "sorted by name")
	require.Equal(t, "beta", list[1].Name)

	require.NoError(t, s.Delete("uid-a"))
	require.Len(t, s.List(), 1)
	_, ok, err := s.Get("uid-a")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")

	db, err := appdb.Open(path)
	require.NoError(t, err)
	s := withClock(New(db.SQL()), 7)
	_, err = s.RecordSeen("uid-a", "ctx-a")
	require.NoError(t, err)
	_, err = s.SetLastSyncedAtBatch(map[string]int64{"uid-a": 99})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopening the same file recovers the persisted state from disk.
	db2, err := appdb.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	s2 := New(db2.SQL())
	got, ok, err := s2.Get("uid-a")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(99), got.LastSyncedAt)
	require.True(t, got.Enabled)
}

func TestEmptyRegistry(t *testing.T) {
	s := openAt(t, 1)
	require.Empty(t, s.List())
}
