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
// **It holds nothing.** Every Get resolves the context afresh, so a context re-pointed at
// another server is described correctly by the next ask — there is no cached credential to
// invalidate, and no window in which one is stale. It is also why nothing here subscribes to
// the kubeconfig: a notification would have nothing to update.
//
// **The probe is not written.** Get reports whether a context resolves and nothing beyond
// that, so one that does reads as "nothing known" and Subscribe never fires — a caller sees a
// fleet awaiting its first probe, which is the state it already renders.
package kubeidentity

import (
	"context"
	"errors"

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

// Service answers what is known about each context's server.
type Service struct {
	kubecfg kubeconfigService
	hub     *conflate.Hub[string, struct{}]
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfg kubeconfigService) *Service {
	return &Service{
		kubecfg: kubecfg,
		// Nothing to merge: two signals for one context say the same thing, which is that Get
		// is worth re-reading.
		hub: conflate.New[string](func(_, next struct{}) (struct{}, bool) { return next, true }),
	}
}

// Get returns what is known about the context's server, and whether anything is known at all.
// It never dials.
//
// A context that resolves reads as nothing known: what would fill that in is the probe, and
// there is none yet. So does one whose kubeconfig has not been read — the unread config is
// empty, so every context looks departed, and reporting that would record a live cluster as
// gone.
func (s *Service) Get(contextName string) (State, bool) {
	// Only the error is read. What the credentials are is the probe's question, and it is the
	// probe that is missing.
	_, _, err := s.kubecfg.RESTConfig(contextName)
	if err != nil && !errors.Is(err, kubeconfig.ErrNotRead) {
		return State{Err: err}, true
	}
	return State{}, false
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
