package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/zalando/go-keyring"
)

// The keyring-backed CredentialsStore round-trips a Token through the OS secret
// store (here go-keyring's in-memory mock): an empty store loads the zero Token,
// a saved token loads back identically, and saving the zero Token (Clear)
// erases the entry back to signed-out.
func TestKeyringStoreRoundTrip(t *testing.T) {
	keyring.MockInit()
	store := newKeyringStore("kstack-app-test")

	// Nothing stored yet → zero Token (signed-out), not an error.
	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if got != (Token{}) {
		t.Fatalf("empty store should load the zero Token, got %+v", got)
	}

	want := Token{
		AccessToken:  "access",
		RefreshToken: "refresh",
		IDToken:      "id",
		Expiry:       time.Now().Add(time.Hour).Truncate(time.Second).UTC(),
	}
	if err := store.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err = store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.AccessToken != want.AccessToken || got.RefreshToken != want.RefreshToken ||
		got.IDToken != want.IDToken || !got.Expiry.Equal(want.Expiry) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}

	// Saving the zero Token (sign-out / Clear) erases the entry.
	if err := store.Save(Token{}); err != nil {
		t.Fatalf("Save zero: %v", err)
	}
	got, err = store.Load()
	if err != nil {
		t.Fatalf("Load after clear: %v", err)
	}
	if got != (Token{}) {
		t.Fatalf("after clearing, want the zero Token, got %+v", got)
	}
}

// An unreadable keyring is an error, not an empty store: only a genuinely absent entry
// means signed out, and the two must not be confused into discarding a live session.
func TestKeyringStoreSurfacesBackendErrors(t *testing.T) {
	boom := errors.New("keyring unavailable")
	keyring.MockInitWithError(boom)
	store := newKeyringStore("kstack-app-test")

	if _, err := store.Load(); !errors.Is(err, boom) {
		t.Fatalf("Load err = %v, want %v", err, boom)
	}
	if err := store.Save(Token{RefreshToken: "r"}); !errors.Is(err, boom) {
		t.Fatalf("Save err = %v, want %v", err, boom)
	}
	if err := store.Save(Token{}); !errors.Is(err, boom) {
		t.Fatalf("Save(zero) err = %v, want %v", err, boom)
	}
}

// A stored entry that is not the Token JSON is an error rather than a zero Token, so a
// corrupted keyring cannot silently read as signed out.
func TestKeyringStoreRejectsAMalformedEntry(t *testing.T) {
	keyring.MockInit()
	if err := keyring.Set("kstack-app-test", keyringUser, "not json"); err != nil {
		t.Fatalf("seed keyring: %v", err)
	}

	if _, err := newKeyringStore("kstack-app-test").Load(); err == nil {
		t.Fatal("Load should reject an entry that is not a Token")
	}
}

// Clearing a store that holds nothing is a no-op: sign-out runs the same path whether or
// not a credential was ever written.
func TestKeyringStoreClearIsIdempotent(t *testing.T) {
	keyring.MockInit()

	if err := newKeyringStore("kstack-app-test").Save(Token{}); err != nil {
		t.Fatalf("Save(zero) on an empty store: %v", err)
	}
}
