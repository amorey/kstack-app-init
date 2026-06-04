package auth

import (
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
