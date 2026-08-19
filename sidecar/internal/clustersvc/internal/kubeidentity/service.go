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
// Neither the workers nor the dial they make are written. Get answers "nothing known"
// for every context and Subscribe never fires, so a caller sees a fleet awaiting its
// first probe rather than a panic — the state it already renders. What resolves and
// dials for it is the kubeconn rework's to settle.
package kubeidentity

import (
	"context"
	"errors"

	"github.com/amorey/gobus/conflate"
)

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
	// UIDErr is why ServerUID is empty, when a response came back saying so. An error
	// rather than a bool because a caller tells the cases apart: no RBAC on kube-system
	// (403) and a server with no such namespace (404) are different news.
	UIDErr error
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

// Subscription reports the contexts whose State moved, one event per context.
//
// A keyed bus, not a fan-out ring: it holds a slot per context, so a fleet answering at
// once neither loses a context behind a busier one nor bounds what is remembered by a
// buffer length. The value carries nothing — the key is the news, and the reader
// re-reads Get for what it now says.
type Subscription = *conflate.Receiver[string, struct{}]

// Service keeps what is known about each context's server, and dials off its callers'
// goroutines.
type Service struct {
	hub *conflate.Hub[string, struct{}]
}

// New returns a Service with nothing known. It takes no arguments: what the workers will
// resolve and dial through is the kubeconn rework's to settle, and a dependency nothing
// reads is one the wiring would have to keep true for no one.
func New() *Service {
	return &Service{
		// Nothing to merge: two signals for one context say the same thing, which is
		// that Get is worth re-reading.
		hub: conflate.New[string](func(_, next struct{}) (struct{}, bool) { return next, true }),
	}
}

// Get returns what is known about the context's server, and whether anything is known
// at all. It never dials.
//
// **Asking is what keeps a context probed.** A caller that stops asking stops the work,
// which is what leaves the policy — which clusters are worth connecting — with the
// caller rather than with a declaration API here. A context with no probe in flight is
// queued for one, so a caller's own cadence is the probe's cadence.
//
// Nothing is queued yet, and nothing is known.
func (s *Service) Get(contextName string) (State, bool) { return State{}, false }

// Subscribe reports the contexts whose State moved. Close the subscription when done.
// Nothing sends yet, so a subscriber parks until the workers exist.
func (s *Service) Subscribe() Subscription { return s.hub.Receiver() }

// Start launches the workers that dial, and returns the func that ends them.
//
// There are none yet, so this is the shape without the work: the stop func is what a
// caller composes into lifecycle.StartAll, and it must stay callable once workers arrive
// behind it. Called once — a second call would otherwise start a worker set sharing the
// first's drain.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close releases what the workers hold. Nothing yet: a dial's client is built per probe
// and released with it.
func (s *Service) Close() error { return nil }
