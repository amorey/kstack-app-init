package auth

import (
	"encoding/json"
	"errors"

	"github.com/zalando/go-keyring"
)

// keyringUser is the account name the token JSON is stored under.
const keyringUser = "auth-token"

// keyringStore implements CredentialsStore over the OS keyring via go-keyring. The whole
// Token rides one JSON entry, so a restart restores the full credential without a network
// round-trip. The sidecar owns the keyring directly — loopback OAuth already pins the
// flow to this machine.
type keyringStore struct {
	service string
}

// newKeyringStore persists under the given keyring service name (e.g. "Kstack").
func newKeyringStore(service string) *keyringStore {
	return &keyringStore{service: service}
}

// Load returns the persisted Token; an absent entry is "signed out", not an error.
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

// Save persists the Token, or deletes the entry for the zero Token, so a sign-out leaves
// no blank credential behind. Deleting an absent entry is not an error.
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
