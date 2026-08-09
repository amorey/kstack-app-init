// Package prefs holds the cloud-synced preferences: the Settings type, a JSON-backed
// Store, and a watch hub. Settings fields are POINTERS so absent is distinct from zero —
// the cloud deep-merges patches, so "not present" must not read as "cleared". The Store
// wraps syncstore so the engine's reconcile metadata rides the same file; this package
// touches only the payload.
package prefs

import (
	"reflect"
	"sync"

	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/syncstore"
)

// Settings is the preferences payload; pointers + omitempty give deep-merge semantics.
type Settings struct {
	Theme  *string `json:"theme,omitempty"`
	Locale *string `json:"locale,omitempty"`
}

// Merge overlays patch's non-nil fields onto base — the local-first reconcile primitive,
// re-layering pending local patches over an incoming snapshot so a stale one can't
// clobber them. The result aliases neither argument.
func Merge(base, patch Settings) Settings {
	out := base
	if patch.Theme != nil {
		out.Theme = patch.Theme
	}
	if patch.Locale != nil {
		out.Locale = patch.Locale
	}
	return Clone(out)
}

// Clone deep-copies s. The Store and the mutation queue clone at their boundaries, so a
// caller mutating what it passed in (or got back) can't desync memory from disk.
func Clone(s Settings) Settings {
	return Settings{
		Theme:  clonePtr(s.Theme),
		Locale: clonePtr(s.Locale),
	}
}

func clonePtr(p *string) *string {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// Subscription is a latest-value receiver of Settings snapshots.
type Subscription = *watch.Receiver[Settings]

// Store persists Settings to a JSON file and publishes the current value to
// subscribers. Safe for concurrent use.
type Store struct {
	store *syncstore.Store[Settings]
	hub   *watch.Hub[Settings]
	tx    *watch.Sender[Settings]

	mu sync.Mutex
	// env is the cached on-disk envelope. We own the file exclusively, so Set updates
	// Data in place — no per-write read to preserve the engine's metadata.
	env syncstore.Envelope[Settings]
	cur Settings
}

// NewStore opens (or lazily creates) the Settings file and seeds the hub from it.
func NewStore(path string) (*Store, error) {
	st := syncstore.NewStore[Settings](path)
	env, err := st.Load()
	if err != nil {
		return nil, err
	}
	// Seed with a clone so a current-on-subscribe delivery shares no pointers with
	// cur/env.
	hub := watch.New(Clone(env.Data))
	return &Store{
		store: st,
		hub:   hub,
		tx:    hub.Sender(),
		env:   env,
		cur:   env.Data,
	}, nil
}

// Get deep-copies the current Settings, so a caller can't reach into store state.
func (s *Store) Get() Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Clone(s.cur)
}

// Set persists v and publishes it, preserving the envelope's sync metadata, and reports
// whether it changed — an equal Set is a no-op, so only real changes reach subscribers
// and the cloud.
func (s *Store) Set(v Settings) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if reflect.DeepEqual(s.cur, v) {
		return false, nil
	}

	// Clone so we own the stored value.
	v = Clone(v)

	// Update the cached payload in place (keeping the metadata); roll back on failure.
	prev := s.env.Data
	s.env.Data = v
	if err := s.store.Save(s.env); err != nil {
		s.env.Data = prev
		return false, err
	}
	s.cur = v
	// A separate clone, so a subscriber can't write through to s.cur.
	s.tx.Send(Clone(v)) //nolint:errcheck // watch.Send never blocks; closed hub is a no-op
	return true, nil
}

// Subscribe returns a current-on-subscribe receiver; close it when done.
func (s *Store) Subscribe() Subscription {
	return s.hub.Receiver()
}
