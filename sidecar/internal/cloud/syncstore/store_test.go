package syncstore_test

import (
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/syncstore"
)

// payload is a minimal stand-in for a real synced type; syncstore is
// generic and must not depend on any concrete payload package.
type payload struct {
	Name string `json:"name"`
}

// C1: Load on a missing file is a normal "no synced state yet" condition —
// not an error. The engine relies on this so it can Load() on startup and
// treat the zero envelope as "nothing reconciled yet, do a full resync".
func TestLoadMissingFileReturnsZeroEnvelope(t *testing.T) {
	s := syncstore.NewStore[payload](filepath.Join(t.TempDir(), "state.json"))
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load on missing file: %v", err)
	}
	if got != (syncstore.Envelope[payload]{}) {
		t.Fatalf("want zero Envelope, got %+v", got)
	}
}

// C2: Save then Load round-trips every envelope field (payload + sync
// metadata). This is the whole happy-path contract for the reconciled-state
// cache.
func TestSaveLoadRoundTrip(t *testing.T) {
	s := syncstore.NewStore[payload](filepath.Join(t.TempDir(), "state.json"))
	want := syncstore.Envelope[payload]{
		Data:         payload{Name: "hello"},
		Version:      "v3",
		LastSyncedAt: 1717000000000,
		LastEventAt:  1717000001234,
	}
	if err := s.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Fatalf("round-trip: want %+v, got %+v", want, got)
	}
}

// Concurrent Saves must not corrupt the file. We don't care which writer
// wins — only that a later Load returns a parseable envelope whose Version
// is one of the values we wrote (proving no torn document).
func TestConcurrentSavesDoNotCorrupt(t *testing.T) {
	s := syncstore.NewStore[payload](filepath.Join(t.TempDir(), "state.json"))

	versions := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	var wg sync.WaitGroup
	for _, v := range versions {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			if err := s.Save(syncstore.Envelope[payload]{Version: v}); err != nil {
				t.Errorf("Save(%q): %v", v, err)
			}
		}(v)
	}
	wg.Wait()

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load after concurrent saves: %v", err)
	}
	if !slices.Contains(versions, got.Version) {
		t.Fatalf("Version=%q is not one of the written values", got.Version)
	}
}

// A successful Save leaves no tmp files behind: the write goes to a tmp
// file in the same dir and is atomically renamed into place.
func TestSaveLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	s := syncstore.NewStore[payload](filepath.Join(dir, "state.json"))
	if err := s.Save(syncstore.Envelope[payload]{Version: "v1"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "state.json" {
			t.Fatalf("unexpected leftover file in store dir: %q", e.Name())
		}
	}
}
