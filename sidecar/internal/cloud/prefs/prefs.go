// Package prefs holds the user's cloud-synced preferences: the Settings type, a
// JSON-backed Store, and a watch hub fanning the current Settings to subscribers.
//
// Settings uses pointer fields so absent (nil) is distinct from zero — the cloud
// deep-merges patches, so "not present" must not read as "cleared". The Store
// wraps internal/cloud/syncstore so the engine can carry its reconcile metadata
// in the same file; this package reads/writes only the payload (Data).
package prefs

import (
	"reflect"
	"sync"

	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/syncstore"
)

// Settings is the user's preferences payload. Fields are pointers + omitempty so
// the JSON form distinguishes absent from zero (deep-merge semantics).
type Settings struct {
	Theme  *string `json:"theme,omitempty"`
	Locale *string `json:"locale,omitempty"`
}

// Merge deep-merges patch onto base: each non-nil patch field overrides base, nil
// fields leave base untouched. The local-first reconcile primitive — a pending
// local patch is re-layered over an incoming cloud snapshot so a stale snapshot
// can't clobber it. The result owns fresh copies, never aliasing base or patch.
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

// Clone returns a deep copy of s with fresh pointer storage. The Store and the
// mutation queue clone at their boundaries, so a caller mutating a string it
// passed in (or got back) can't desync in-memory settings from disk.
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
	// env is the cached on-disk envelope. We own the file exclusively, so we hold
	// the full envelope in memory and update Data in place on Set — no per-write
	// disk read needed to preserve the engine's metadata.
	env syncstore.Envelope[Settings]
	cur Settings
}

// NewStore opens (or lazily creates) the Settings file at path and seeds the
// watch hub with whatever is currently persisted.
func NewStore(path string) (*Store, error) {
	st := syncstore.NewStore[Settings](path)
	env, err := st.Load()
	if err != nil {
		return nil, err
	}
	// Seed with a clone so a current-on-subscribe delivery doesn't share the
	// pointers held in cur/env (a subscriber mutating it would desync from disk).
	hub := watch.New(Clone(env.Data))
	return &Store{
		store: st,
		hub:   hub,
		tx:    hub.Sender(),
		env:   env,
		cur:   env.Data,
	}, nil
}

// Get returns a deep copy of the current Settings, so a caller mutating its
// fields can't reach into the store's in-memory state.
func (s *Store) Get() Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Clone(s.cur)
}

// Set persists v and publishes it to subscribers, preserving the engine's sync
// metadata in the envelope. It reports whether the value changed: an equal Set is
// a no-op (no write, no publish) so subscribers and the cloud see only real changes.
func (s *Store) Set(v Settings) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if reflect.DeepEqual(s.cur, v) {
		return false, nil
	}

	// Clone so we own the stored/published value; a caller mutating what it passed
	// in can't desync our in-memory copy from disk.
	v = Clone(v)

	// Update the cached envelope's payload in place (preserving the engine's
	// metadata) and persist. Roll the cache back on write failure.
	prev := s.env.Data
	s.env.Data = v
	if err := s.store.Save(s.env); err != nil {
		s.env.Data = prev
		return false, err
	}
	s.cur = v
	// Publish a separate clone so a subscriber mutating its snapshot can't write
	// through to s.cur.
	s.tx.Send(Clone(v)) //nolint:errcheck // watch.Send never blocks; closed hub is a no-op
	return true, nil
}

// Subscribe returns a receiver whose first Recv yields the current Settings,
// then each subsequent change. Close it when done.
func (s *Store) Subscribe() Subscription {
	return s.hub.Receiver()
}
