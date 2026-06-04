package auth

import (
	"encoding/json"
	"errors"

	"github.com/zalando/go-keyring"
)

// keyringUser is the fixed account name under which the token JSON is stored
// in the OS keyring for a given service.
const keyringUser = "auth-token"

// keyringStore implements CredentialsStore over the OS keyring — macOS Keychain,
// Windows Credential Manager, Linux Secret Service — via go-keyring. The whole
// Token is serialized to JSON under a single (service, user) entry: the refresh
// token is the long-lived secret, the rest ride along so a restart restores the
// full credential without a network round-trip.
//
// The sidecar owns the keyring directly (no host round-trip): loopback OAuth
// already pins the browser and the listener to this machine, so persistence
// belongs here too.
type keyringStore struct {
	service string
}

// newKeyringStore returns a CredentialsStore persisting under the given keyring
// service name (e.g. "Kstack").
func newKeyringStore(service string) *keyringStore {
	return &keyringStore{service: service}
}

// Load returns the persisted Token, or the zero Token when nothing is stored
// (an absent entry is "signed out", not an error — matching the contract).
func (k *keyringStore) Load() (Token, error) {
	s, err := keyring.Get(k.service, keyringUser)
	if errors.Is(err, keyring.ErrNotFound) {
		return Token{}, nil
	}
	if err != nil {
		return Token{}, err
	}
	var tok Token
	if err := json.Unmarshal([]byte(s), &tok); err != nil {
		return Token{}, err
	}
	return tok, nil
}

// Save persists the Token, or deletes the entry when handed the zero Token (so a
// sign-out / Clear leaves no empty credential behind rather than an entry full of
// blanks). Deleting an already-absent entry is not an error.
func (k *keyringStore) Save(tok Token) error {
	if tok == (Token{}) {
		err := keyring.Delete(k.service, keyringUser)
		if errors.Is(err, keyring.ErrNotFound) {
			return nil
		}
		return err
	}
	b, err := json.Marshal(tok)
	if err != nil {
		return err
	}
	return keyring.Set(k.service, keyringUser, string(b))
}
