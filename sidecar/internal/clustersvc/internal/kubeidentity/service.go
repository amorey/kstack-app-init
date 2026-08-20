// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kubeidentity reads the identity of the API server behind a set of
// credentials, and keeps what it read fresh for callers that must not dial.
//
// The split kubeconfig.Service keeps: Get reads a map, background workers do the
// network. What is worth probing is decided by who asks — see Get.
//
// The dial is not written. A loop runs the cadence and probes nothing, so Get answers
// "nothing known" for every context and Subscribe never fires — a caller sees a fleet
// awaiting its first probe rather than a panic, which is the state it already renders.
// What resolves the credentials and dials them is still to come.
package kubeidentity

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
)

// Budget is the pacing, taken from the caller rather than read from a constant here, so
// a test never has to outwait production's numbers.
type Budget struct {
	// Interval is how long a loop waits between probes of its context.
	Interval time.Duration
	// Jitter is the spread added to each wait. A startup pass registers the whole fleet
	// at once, and without it they would dial in lockstep forever after.
	Jitter time.Duration
}

// DefaultBudget is the pacing production runs at.
var DefaultBudget = Budget{
	Interval: 60 * time.Second,
	Jitter:   15 * time.Second,
}

// ErrProbe marks a failure to reach the server, as opposed to one resolving the
// credentials to reach it with. A caller reports the two differently — one is a cluster
// that is down, the other a kubeconfig that needs fixing.
var ErrProbe = errors.New("probe failed")

// Identity is what one probe learned: which cluster answered, and as whom.
//
// A field whose own request was refused stays empty rather than failing the probe: a
// namespace-scoped user refused kube-system reached a cluster that is up, which a
// caller reports differently from one it never reached.
type Identity struct {
	ServerUID     string
	ServerVersion string
	Username      string
}

// State is what is known about one context's server: the identity its last probe read,
// or why there is none.
type State struct {
	Identity Identity
	// Err is why the last attempt produced nothing new — the context would not resolve,
	// or the server would not answer. Wrapped so a caller can tell those apart with
	// errors.Is, against kubeconfig's sentinels or ErrProbe.
	Err error
}

// sameAs reports whether this state says what other says. Field by field, since Err
// makes State uncomparable.
//
// Err compares by its text: connection-refused becoming a 401 is a different answer, and
// a caller left reporting the old reason indefinitely is the failure that hides. Error
// identity cannot stand in for it — two separately built wraps of one sentinel are not
// errors.Is each other, so a cluster that stays down would publish every interval.
//
// **Whoever builds these errors owes the text one thing**: a change of class must show in
// it. A caller tells the classes apart with errors.Is (a context that left the file, a
// server that would not answer, a file that would not resolve) and acts differently on
// each, so two classes sharing a message would leave it on the wrong one until its own
// resync. Naming the sentinels here instead would put that caller's switch in this leaf,
// where nothing keeps the two in step.
func (s State) sameAs(other State) bool {
	return s.Identity == other.Identity && errText(s.Err) == errText(other.Err)
}

// errText is err's message, or "" for no error.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Subscription reports the contexts whose State moved, one event per context.
//
// A keyed bus, not a fan-out ring: it holds a slot per context, so a fleet answering at
// once neither loses a context behind a busier one nor bounds what is remembered by a
// buffer length. The value carries nothing — the key is the news, and the reader
// re-reads Get for what it now says.
type Subscription = *conflate.Receiver[string, struct{}]

// entry is one registered context: the loop probing it, and what that loop last learned.
// A value in the map, not a pointer, so every read copies it out from under the lock and
// nothing that escapes Get is mutated by a loop mid-read.
//
// **An entry and its loop are the same fact** — one exists exactly while the other does,
// which is why nothing tracks whether a loop is running.
type entry struct {
	state State
	// known separates a context that has been answered from one only registered.
	// Presence cannot say it: an entry exists from the moment a Get asks, which is
	// before its first probe.
	known bool
	// stop ends this entry's loop. Held on the row it belongs to so Forget can end the
	// two together, and so a context registered again gets a loop of its own rather
	// than inheriting one already unwinding.
	stop context.CancelFunc
}

// Service keeps what is known about each context's server, and dials off its callers'
// goroutines.
type Service struct {
	budget Budget
	hub    *conflate.Hub[string, struct{}]

	// mu guards entries, which Get reads on a caller's goroutine.
	mu      sync.Mutex
	entries map[string]entry

	// ctx bounds every loop and stopLoops ends them. Live from New, so a context
	// registered before Start still gets a loop.
	ctx       context.Context
	stopLoops context.CancelFunc
	// stopped closes the door on new loops, set by whichever of the stop func and Close
	// runs first. A loop starts on a caller's Get, so without this one arriving
	// mid-shutdown would add a goroutine to a WaitGroup already being waited on — or,
	// past Close, an entry whose loop ends on the cancelled context it was handed.
	stopped bool
	wg      sync.WaitGroup
}

// New returns a Service with nothing known, probing at the pace budget sets.
func New(budget Budget) *Service {
	// Not Start's context, which bounds startup: this one bounds every loop, so it
	// lives until the stop func cancels it.
	ctx, stopLoops := context.WithCancel(context.Background())

	return &Service{
		budget: budget,
		// Nothing to merge: two signals for one context say the same thing, which is
		// that Get is worth re-reading.
		hub:       conflate.New[string](func(_, next struct{}) (struct{}, bool) { return next, true }),
		entries:   map[string]entry{},
		ctx:       ctx,
		stopLoops: stopLoops,
	}
}

// Get returns what is known about the context's server, and whether anything is known
// at all. It never dials.
//
// **Asking is what starts a probe.** The first Get registers the context and starts the
// loop that probes it, which is what leaves the policy — which clusters are worth
// connecting — with the caller rather than with a declaration API here. A registered
// context is then probed until the caller Forgets it, or the service stops.
//
// A newly registered context reads as nothing known: registering is not answering, and
// the loop's first probe is what fills it in.
func (s *Service) Get(contextName string) (State, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[contextName]
	if !ok {
		// Refused during shutdown rather than registered: no loop would run for it, so
		// the entry would sit unprobed and outlive the drain.
		if s.stopped {
			return State{}, false
		}
		// Bounded by the service's context, so a stop ends every loop, and by its own,
		// so Forget ends just this one.
		loopCtx, stop := context.WithCancel(s.ctx)
		e.stop = stop
		s.entries[contextName] = e
		s.wg.Go(func() { s.run(loopCtx, contextName) })
	}
	return e.state, e.known
}

// Forget discards what is known about a context and ends its loop. It is how a caller
// says it will not ask again — a cluster deleted, disabled, or pointed at another
// context — since asking is the only thing that ever started the work.
//
// Idempotent, and forgetting a context nothing registered does nothing. The loop unwinds
// on its own; the stop func is still what waits for it.
func (s *Service) Forget(contextName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[contextName]
	if !ok {
		return
	}
	delete(s.entries, contextName)
	e.stop()
}

// Subscribe reports the contexts whose State moved. Close the subscription when done.
// Nothing sends while a probe learns nothing, so a subscriber parks until the dial
// lands.
func (s *Service) Subscribe() Subscription { return s.hub.Receiver() }

// Start returns the func that ends every loop. The loops themselves start with the Get
// that registers their context, which can be before this. Nothing here can fail.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(ctx context.Context) error {
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()

		s.stopLoops()
		return drain.WithContext(ctx, s.wg.Wait)
	}, nil
}

// Close releases the bus. The stop func is what waits for the loops; this only cancels
// them, so a Close on its own still leaves nothing running — a loop unwinding past it
// finds the bus closed and its send refused.
func (s *Service) Close() error {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	s.stopLoops()
	s.hub.Close()
	return nil
}

// run paces one context's probes until the service shuts down. The dial is not written,
// so a pass does nothing yet.
func (s *Service) run(ctx context.Context, contextName string) {
	// Fires immediately. Registering is a Get that got nothing back, and waiting out an
	// interval first would leave that caller unanswered for as long as the fleet's
	// steady-state cadence.
	next := time.NewTimer(0)
	defer next.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-next.C:
		}

		// Jittered per pass, so loops that a shared wake — a startup pass, a resume —
		// lined up drift apart again instead of dialing in lockstep from then on.
		next.Reset(s.budget.Interval + time.Duration(rand.Int63n(int64(s.budget.Jitter)+1)))
	}
}

// store records what a probe learned, and publishes when the answer moved.
//
// **Only on a move.** A signal wakes the caller's pass, which wakes everything depending
// on what that pass reports — so publishing an unchanged answer costs the fleet a round
// of work that can only conclude nothing changed, once per context per interval, forever.
func (s *Service) store(contextName string, state State) {
	s.mu.Lock()
	e, ok := s.entries[contextName]
	if !ok {
		// Forgotten while the probe ran. Storing would put the row back without the loop
		// that was deleted with it, leaving one nothing probes and nothing forgets.
		s.mu.Unlock()
		return
	}
	moved := !e.known || !state.sameAs(e.state)
	e.state, e.known = state, true
	s.entries[contextName] = e
	s.mu.Unlock()

	if moved {
		// Fails only against a closed bus, which is shutdown: nobody is left to tell.
		_ = s.hub.Sender().Send(contextName, struct{}{})
	}
}
