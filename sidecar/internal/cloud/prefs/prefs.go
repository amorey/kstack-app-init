// Package prefs holds the user's cloud-synced preferences: the Settings
// type, a persistent Store backed by a JSON file, and a watch hub that
// fans the current Settings to subscribers.
//
// Settings uses pointer fields so an absent field (nil) is distinct from a
// field explicitly set to its zero value — the cloud deep-merges patches,
// so "field not present" must not be confused with "field cleared". The
// Store wraps internal/cloud/syncstore so the sync engine can carry its
// reconcile metadata (version/timestamps) in the same file; this package
// reads and writes only the payload (Data) and leaves the metadata to the
// engine.
package prefs

import (
	"reflect"
	"sync"

	"github.com/amorey/gochan/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/syncstore"
)

// Settings is the user's preferences payload. Fields are pointers + omitempty
// so the JSON form distinguishes absent from zero (deep-merge semantics).
// The concrete field set stays minimal until the cloud Settings schema is
// finalized.
type Settings struct {
	Theme  *string `json:"theme,omitempty"`
	Locale *string `json:"locale,omitempty"`
}

// Merge deep-merges patch onto base: each non-nil field in patch overrides
// base; nil (absent) fields leave base untouched. This is the local-first
// reconcile primitive — a still-pending local patch is re-layered over an
// incoming cloud snapshot so a snapshot that predates the edit can't clobber
// it. The returned Settings owns fresh copies of every field, so it never
// aliases base's or patch's pointers.
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

// Clone returns a deep copy of s: the pointer fields point at fresh storage, so
// the result and s never alias. The Store and the mutation queue clone at their
// boundaries, so a caller mutating the string it passed to Set/Enqueue (or one
// returned by Get/Pending) can't desync in-memory settings from disk.
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
	// env is the cached on-disk envelope. We own the file exclusively, so we
	// keep the full envelope in memory and update Data in place on Set —
	// avoiding a disk read on every write to preserve the engine's metadata.
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
	// Seed the hub with a clone so a current-on-subscribe delivery doesn't hand
	// out the same pointers held in cur/env (a subscriber mutating it would
	// desync memory from disk).
	hub := watch.New(Clone(env.Data))
	return &Store{
		store: st,
		hub:   hub,
		tx:    hub.Sender(),
		env:   env,
		cur:   env.Data,
	}, nil
}

// Get returns the current Settings. The result is a deep copy, so a caller
// mutating its pointer fields can't reach into the store's in-memory state.
func (s *Store) Get() Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Clone(s.cur)
}

// Set persists v as the current Settings and publishes it to subscribers,
// preserving the engine's sync metadata in the envelope. It reports whether
// the value actually changed: an equal Set is a no-op (no disk write, no
// publish) so subscribers and the cloud only see real changes.
func (s *Store) Set(v Settings) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if reflect.DeepEqual(s.cur, v) {
		return false, nil
	}

	// Clone so we own the stored/published value: a caller mutating the string
	// it passed in can't later desync our in-memory copy from the persisted JSON.
	v = Clone(v)

	// Update the cached envelope's payload in place (preserving the engine's
	// metadata) and persist — no disk read needed since we own the file. Roll
	// the cache back if the write fails so it stays consistent with disk.
	prev := s.env.Data
	s.env.Data = v
	if err := s.store.Save(s.env); err != nil {
		s.env.Data = prev
		return false, err
	}
	s.cur = v
	// Publish a separate clone: a subscriber mutating the snapshot it receives
	// must not write through to s.cur (which would desync memory from disk).
	s.tx.Send(Clone(v)) //nolint:errcheck // watch.Send never blocks; closed hub is a no-op
	return true, nil
}

// Subscribe returns a receiver whose first Recv yields the current Settings,
// then each subsequent change. Close it when done.
func (s *Store) Subscribe() Subscription {
	return s.hub.Receiver()
}
