package graph_test

// Test-only mocks shared by the resolver tests.

import (
	"context"
	"sync"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// fakeAuth is a hand-written auth.Service for the resolver tests. The resolver
// depends on the auth.Service interface, so its tests fake that interface rather
// than standing up the real service (whose own package owns the real Login /
// persist / refresh coverage). It models only what the resolver uses:
// Current/Subscribe snapshots, and Login/Logout flipping the session and
// publishing the resulting State — mirroring the real service's derived state
// (Authenticated + Identity present only while signed in) and its latest-value,
// current-on-subscribe session stream.
type fakeAuth struct {
	mu       sync.Mutex
	signedIn bool
	identity auth.Identity // current identity (set while signed in)
	loginAs  auth.Identity // who a Login signs in as

	loginErr error // when set, Login fails synchronously (setup error) without signing in

	subs   map[int]chan auth.State
	nextID int
}

// newFakeAuth returns a signed-out fake whose Login signs in as loginAs (the
// resolver's login flow).
func newFakeAuth(loginAs auth.Identity) *fakeAuth {
	return &fakeAuth{loginAs: loginAs, subs: map[int]chan auth.State{}}
}

// signedInFakeAuth returns a fake already signed in as id.
func signedInFakeAuth(id auth.Identity) *fakeAuth {
	return &fakeAuth{signedIn: true, identity: id, loginAs: id, subs: map[int]chan auth.State{}}
}

func (f *fakeAuth) Current(context.Context) (auth.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stateLocked(), nil
}

func (f *fakeAuth) StartLogin(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loginErr != nil {
		return f.loginErr // synchronous setup failure: no sign-in, no publish
	}
	f.signedIn = true
	f.identity = f.loginAs
	f.publishLocked()
	return nil
}

func (f *fakeAuth) Logout(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signedIn = false
	f.identity = auth.Identity{}
	f.publishLocked()
	return nil
}

// TokenSource is unused by the graph resolvers (only the cloud client reads it),
// so the fake returns nil.
func (f *fakeAuth) TokenSource(context.Context) oauth2.TokenSource { return nil }

// Subscribe mirrors the real latest-value stream: it delivers the current State
// on subscribe, then each subsequent change.
func (f *fakeAuth) Subscribe() (<-chan auth.State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	ch := make(chan auth.State, 8)
	ch <- f.stateLocked() // current-on-subscribe
	f.subs[id] = ch
	return ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if c, ok := f.subs[id]; ok {
			delete(f.subs, id)
			close(c)
		}
	}
}

// stateLocked derives the public State from the session, mirroring the real
// grant: Identity is present only while signed in. Caller holds f.mu.
func (f *fakeAuth) stateLocked() auth.State {
	st := auth.State{Authenticated: f.signedIn}
	if f.signedIn {
		id := f.identity
		st.Identity = &id
	}
	return st
}

// publishLocked publishes the current State to subscribers. Caller holds f.mu.
// The send is non-blocking (drops on a full buffer, like the real watch hub) so
// a Login goroutine can't wedge on a slow reader.
func (f *fakeAuth) publishLocked() {
	st := f.stateLocked()
	for _, ch := range f.subs {
		select {
		case ch <- st:
		default:
		}
	}
}
