//go:build !windows

package atomicjson_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
)

// host.json holds the user's settings, so Save writes the file 0600 and any directory
// it has to create 0700 — whatever umask the process was started with. Windows has no
// POSIX permission bits; the per-user profile ACL carries it there.
func TestSaveIsOwnerOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "deeper")
	p := filepath.Join(dir, "x.json")
	if err := atomicjson.Save(p, doc{A: "v"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("file perm = %o, want 600", perm)
	}

	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Fatalf("dir perm = %o, want 700", perm)
	}
}
