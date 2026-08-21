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

// Package kubeidentity answers which server a kube-context reaches, and as whom.
//
// The split kubeconfig.Service keeps: Get reads memory, the network happens elsewhere.
//
// **What it holds is keyed by credentials.** Every Get re-resolves the context and compares
// the key against the one the stored answer was learned under, so an edit that re-points a
// context or rotates its token makes that answer unreadable at the instant it lands rather
// than at the next probe. The caller reads "nothing known" and reports connecting — never a
// stale identity as a current one. Nothing here subscribes to the kubeconfig: the key check
// is what keeps the answer honest, and promptness already arrives from above, since the pass
// that asks is itself woken when the file moves.
//
// **The probe is not written.** Nothing fills the store, so every context that resolves reads
// as nothing known and Subscribe never fires — a caller sees a fleet awaiting its first probe,
// which is the state it already renders. What lands is a dialer calling record, and the
// cadence it re-dials on: a server whose identity changes under unchanged credentials moves
// nothing in the kubeconfig, so that cadence is the only thing that can detect it.
package kubeidentity

import (
	"context"
	"errors"
	"sync"

	"github.com/amorey/gobus/conflate"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// Identity is what one probe learned: which cluster answered, and as whom.
//
// Comparable by value, which is what will let a probe tell an unchanged answer from one worth
// publishing. A field that breaks that puts every probe back on the wake path.
//
// Every field is optional: a probe that reached the API server succeeded, and reports whatever
// this user was allowed to read.
type Identity struct {
	ServerUID     string
	ServerVersion string
	Username      string
}

// State is what is known about one context's server: the identity its last probe read, or why
// there is none.
type State struct {
	Identity Identity
	// Err is why there is nothing to report — today only that the context would not resolve,
	// which wraps one of kubeconfig's sentinels for a caller to match with errors.Is.
	Err error
}

// Subscription reports the contexts whose State moved, one event per context.
//
// A keyed bus, not a fan-out ring: it holds a slot per context, so a fleet answering at once
// neither loses a context behind a busier one nor bounds what is remembered by a buffer
// length. The value carries nothing — the key is the news, and the reader re-reads Get for
// what it now says.
type Subscription = *conflate.Receiver[string, struct{}]

// kubeconfigService resolves one context to credentials and the key naming them. The key
// excludes the context name, so two contexts aimed at one server as one user will be one
// probe's worth of work.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
}

// entry is one probe's answer and the credentials it was learned under. The key is what
// makes the answer refutable: without it a stored identity outlives the credentials that
// produced it, and nothing in the process could tell.
type entry struct {
	key   string
	state State
}

// Service answers what is known about each context's server.
type Service struct {
	kubecfg kubeconfigService
	hub     *conflate.Hub[string, struct{}]

	mu    sync.Mutex
	known map[string]entry
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfg kubeconfigService) *Service {
	return &Service{
		kubecfg: kubecfg,
		// Nothing to merge: two signals for one context say the same thing, which is that Get
		// is worth re-reading.
		hub:   conflate.New[string](func(_, next struct{}) (struct{}, bool) { return next, true }),
		known: map[string]entry{},
	}
}

// Get returns what is known about the context's server, and whether anything is known at all.
// It never dials.
//
// The resolve is what makes the answer current: it yields the credentials the context names
// *now*, and a stored answer is returned only if it was learned under those. Credentials that
// moved therefore read as nothing known, which is the same thing a context nobody has probed
// reads as — both are "connecting", and neither is a claim about a server.
//
// A context whose kubeconfig has not been read reads as nothing known too. The unread config
// is empty, so every context looks departed, and reporting that would record a live cluster as
// gone.
func (s *Service) Get(contextName string) (State, bool) {
	_, key, err := s.kubecfg.RESTConfig(contextName)
	if err != nil {
		if errors.Is(err, kubeconfig.ErrNotRead) {
			return State{}, false
		}
		return State{Err: err}, true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.known[contextName]
	if !ok || e.key != key {
		return State{}, false
	}
	return e.state, true
}

// record stores what a probe learned, publishing the context when the answer moved. key is
// the credentials fingerprint the probe resolved before dialing — passed in rather than
// re-resolved here, so what is stored is what was actually dialed even if the file moved
// while the probe was in flight.
//
// The publish is conditional because a signal wakes the cluster's pass and re-emits its
// record to every watcher: sending on an unchanged answer would cost the fleet a round of
// work per context per cadence.
func (s *Service) record(contextName, key string, state State) {
	s.mu.Lock()
	prev, had := s.known[contextName]
	s.known[contextName] = entry{key: key, state: state}
	s.mu.Unlock()

	if had && prev.key == key && sameState(prev.state, state) {
		return
	}
	s.hub.Sender().Send(contextName, struct{}{}) //nolint:errcheck // a closed bus means shutdown
}

// sameState compares two answers for the purpose of suppressing a publish. Errors compare by
// message rather than by value: a probe failure is built afresh on every attempt, so two
// attempts that failed the same way are never equal and would publish every cadence.
func sameState(a, b State) bool {
	if a.Identity != b.Identity {
		return false
	}
	if (a.Err == nil) != (b.Err == nil) {
		return false
	}
	return a.Err == nil || a.Err.Error() == b.Err.Error()
}

// Subscribe reports the contexts whose State moved. Close the subscription when done. Nothing
// sends until there is a probe to send about, so a subscriber parks.
func (s *Service) Subscribe() Subscription { return s.hub.Receiver() }

// Start is the lifecycle shape this wears for the composition root. Nothing runs in the
// background, so its stop func has nothing to end.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close releases the bus.
func (s *Service) Close() error {
	s.hub.Close()
	return nil
}
