package prefs_test

import (
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// Load on a missing file is a normal "no cache yet" condition — not an error.
// The resolver layer relies on this so it can do `s, _ := store.Load()` on
// startup and treat the zero value as "ask the cloud".
func TestLoadMissingFileReturnsZero(t *testing.T) {
	s := prefs.NewStore(filepath.Join(t.TempDir(), "preferences.json"))
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load on missing file: %v", err)
	}
	if got != (prefs.Settings{}) {
		t.Fatalf("want zero Settings, got %+v", got)
	}
}

// Save then Load round-trips the placeholder field. This is the entire
// happy-path contract for the local cache.
func TestSaveLoadRoundTrip(t *testing.T) {
	s := prefs.NewStore(filepath.Join(t.TempDir(), "preferences.json"))
	want := prefs.Settings{Placeholder: "hello"}
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
// wins — only that a subsequent Load returns a parseable Settings whose
// Placeholder is one of the values we wrote.
func TestConcurrentSavesDoNotCorrupt(t *testing.T) {
	s := prefs.NewStore(filepath.Join(t.TempDir(), "preferences.json"))

	values := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	var wg sync.WaitGroup
	for _, v := range values {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			if err := s.Save(prefs.Settings{Placeholder: v}); err != nil {
				t.Errorf("Save(%q): %v", v, err)
			}
		}(v)
	}
	wg.Wait()

	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load after concurrent saves: %v", err)
	}
	if !slices.Contains(values, got.Placeholder) {
		t.Fatalf("Placeholder=%q is not one of the written values", got.Placeholder)
	}
}
