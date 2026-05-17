package atomicjson_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
)

type doc struct {
	A string `json:"a"`
	N int    `json:"n"`
}

// A missing file is the "nothing written yet" state: zero value, no error.
// Both store packages depend on this so a first Load before any Save works.
func TestLoadMissingFileReturnsZero(t *testing.T) {
	got, err := atomicjson.Load[doc](filepath.Join(t.TempDir(), "x.json"))
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if got != (doc{}) {
		t.Fatalf("want zero doc, got %+v", got)
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.json")
	want := doc{A: "hello", N: 7}
	if err := atomicjson.Save(p, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := atomicjson.Load[doc](p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != want {
		t.Fatalf("round-trip: want %+v, got %+v", want, got)
	}
}

// A second Save fully replaces the document (atomic rename, not append).
func TestSaveOverwrites(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.json")
	if err := atomicjson.Save(p, doc{A: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := atomicjson.Save(p, doc{A: "second"}); err != nil {
		t.Fatal(err)
	}
	got, _ := atomicjson.Load[doc](p)
	if got.A != "second" {
		t.Fatalf("want second, got %+v", got)
	}
}

// Corrupt content surfaces as an error with the zero value — callers must
// not silently treat garbage as valid state.
func TestLoadInvalidJSONReturnsError(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.json")
	if err := os.WriteFile(p, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := atomicjson.Load[doc](p)
	if err == nil {
		t.Fatal("want unmarshal error, got nil")
	}
	if got != (doc{}) {
		t.Fatalf("want zero doc on error, got %+v", got)
	}
}

// A successful Save leaves no temp file behind: the write goes to a temp
// file in the same dir and is atomically renamed into place.
func TestSaveLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x.json")
	if err := atomicjson.Save(p, doc{A: "v"}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "x.json" {
			t.Fatalf("unexpected leftover file: %q", e.Name())
		}
	}
}

// The parent directory is created if absent (0700) and the file lands 0600.
func TestSaveCreatesDirAndTightPerms(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nested", "deeper", "x.json")
	if err := atomicjson.Save(p, doc{A: "v"}); err != nil {
		t.Fatalf("Save into missing dirs: %v", err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	// Windows has no POSIX permission bits; Go's Chmod is a best-effort
	// no-op there and Stat reports 0666 for any writable file. The 0600
	// guarantee only holds on Unix.
	if runtime.GOOS == "windows" {
		return
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("file perm = %o, want 600", perm)
	}
}
