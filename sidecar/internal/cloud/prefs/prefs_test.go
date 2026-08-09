package prefs_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

//go:fix inline
func strptr(s string) *string { return &s }

// C3: a zero Settings marshals with its fields absent (omitempty on
// nil pointers) — distinct from a field explicitly set to the empty
// string. This is what lets the cloud distinguish "field not present"
// from "field cleared" under deep-merge.
func TestSettingsZeroValueOmitsAbsentFields(t *testing.T) {
	b, err := json.Marshal(prefs.Settings{})
	if err != nil {
		t.Fatalf("marshal zero: %v", err)
	}
	if string(b) != "{}" {
		t.Fatalf("zero Settings: want {}, got %s", b)
	}

	b2, err := json.Marshal(prefs.Settings{Theme: strptr("")})
	if err != nil {
		t.Fatalf("marshal empty-string: %v", err)
	}
	if string(b2) != `{"theme":""}` {
		t.Fatalf("empty-string Theme: want {\"theme\":\"\"}, got %s", b2)
	}
}

// C4: Get after Set returns the value, and it survives a fresh Store
// constructed over the same path (persistence).
func TestStoreSetGetPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	s, err := prefs.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	want := prefs.Settings{Theme: strptr("dark")}
	changed, err := s.Set(want)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !changed {
		t.Fatalf("Set of a new value: want changed=true")
	}
	if got := s.Get(); *got.Theme != "dark" {
		t.Fatalf("Get: want dark, got %+v", got)
	}

	reopened, err := prefs.NewStore(path)
	if err != nil {
		t.Fatalf("reopen NewStore: %v", err)
	}
	if got := reopened.Get(); got.Theme == nil || *got.Theme != "dark" {
		t.Fatalf("reopened Get: want dark, got %+v", got)
	}
}

// C5: a subscriber receives the current snapshot on subscribe, without
// waiting for a change (watch latest-value semantics).
func TestStoreSubscribeDeliversCurrentSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	s, err := prefs.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := s.Set(prefs.Settings{Theme: strptr("light")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	sub := s.Subscribe()
	defer sub.Close()
	if got := testutil.Recv(t, sub.Chan(), "the current snapshot"); got.Theme == nil || *got.Theme != "light" {
		t.Fatalf("snapshot: want light, got %+v", got)
	}
}

// C6: setting an unchanged value reports changed=false and does not
// publish to subscribers (the second Recv blocks because nothing new
// was sent).
func TestStoreSetUnchangedDoesNotPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	s, err := prefs.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	val := prefs.Settings{Theme: strptr("dark")}
	if _, err := s.Set(val); err != nil {
		t.Fatalf("first Set: %v", err)
	}

	sub := s.Subscribe()
	defer sub.Close()
	<-sub.Chan() // consume current snapshot

	changed, err := s.Set(prefs.Settings{Theme: strptr("dark")})
	if err != nil {
		t.Fatalf("second Set: %v", err)
	}
	if changed {
		t.Fatalf("Set of an equal value: want changed=false")
	}

	select {
	case got := <-sub.Chan():
		t.Fatalf("unexpected publish on unchanged Set: %+v", got)
	case <-time.After(200 * time.Millisecond):
		// good: nothing published
	}
}

// Set stores a deep copy: mutating the string a caller passed in must not
// change the store's in-memory value (which would desync it from disk).
func TestSetClonesInputAgainstAliasing(t *testing.T) {
	store, err := prefs.NewStore(filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	theme := "dark"
	if _, err := store.Set(prefs.Settings{Theme: &theme}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	theme = "mutated-after-set" // caller mutates the variable it passed in

	if got := store.Get(); got.Theme == nil || *got.Theme != "dark" {
		t.Fatalf("store aliased caller's pointer: %+v", got)
	}
}

// Get returns a deep copy: mutating it must not reach into the store's state.
func TestGetReturnsCopy(t *testing.T) {
	store, err := prefs.NewStore(filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.Set(prefs.Settings{Theme: strptr("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got := store.Get()
	*got.Theme = "mutated-via-get"

	if again := store.Get(); again.Theme == nil || *again.Theme != "dark" {
		t.Fatalf("Get exposed the store's pointer: %+v", again)
	}
}

// Set publishes a clone: a subscriber mutating the snapshot it receives must
// not write through to the store's in-memory current value.
func TestSetPublishesClone(t *testing.T) {
	s, err := prefs.NewStore(filepath.Join(t.TempDir(), "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	sub := s.Subscribe()
	defer sub.Close()
	<-sub.Chan() // consume current snapshot

	if _, err := s.Set(prefs.Settings{Theme: strptr("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got := <-sub.Chan()
	*got.Theme = "mutated-by-subscriber"

	if cur := s.Get(); cur.Theme == nil || *cur.Theme != "dark" {
		t.Fatalf("subscriber mutation leaked into store: %+v", cur)
	}
}
