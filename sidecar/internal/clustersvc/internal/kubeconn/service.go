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

// Package kubeconn hands out leases on kube-contexts and reports what probing the server behind
// one found.
//
// **Nothing dials yet**, so Lease.Conn reports ErrNoConnection and the only answers State carries
// are the ones a probe can reach without a server: a context that left the kubeconfig, and one
// that will not resolve. The probes live in probe.go; the scheduling around them is the probe
// engine's (internal/probe). This file is the rest: leases, and publishing what the engine
// observes.
//
// Three rules shape it:
//
//   - One context, one entry. Contexts resolving to the same credentials are not merged, so
//     what is learned about one belongs to exactly one context.
//   - A claim outlives what it is a claim on. The kubeconfig can stop naming a context while a
//     holder still holds it, and the entry stays: the file may name it again, and the claim is
//     how the holder hears about that. Only releasing drops an entry.
//   - It never learns what a cluster is. Callers name kube-contexts; whether the server behind
//     one is the cluster the caller meant is the caller's to decide.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// kubeconfigService is the reader this package asks whether a context still resolves, and which
// tells it when the file changed. The fingerprint RESTConfig returns beside the config is unused
// until connections come back.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

// Subscription reports the contexts whose news changed, one event per context, for a reader
// whose reaction to any of them is the same: re-read that claim. A keyed, coalescing bus, so a
// fleet changing at once neither loses a context behind a busier one nor overflows a buffer.
// The value carries nothing — the key is the news.
type Subscription = *conflate.Receiver[string, struct{}]

// StateSubscription carries each State published for one claim. Close it when done — an
// abandoned one keeps its slot.
//
// Keyed by context, not by the credentials behind it: a receiver is bound to its key for life,
// and credentials move under a context that never does.
//
// Nothing is delivered on attach — a watcher reads Lease.State for what is known now and this
// for what comes after. Every value is a level, never an edge: the hub keeps only the latest
// per key, so a reader that falls behind skips what came between.
type StateSubscription = *watch.Receiver[string, State]

// Lease is a claim on one kube-context: what keeps the pool tracking it, and how a holder hears
// about it. An interface because it crosses out of this package, where a caller's test has to
// stand in for a live cluster.
type Lease interface {
	// Conn is the connection to the context's server.
	//
	// Nothing builds one yet, so this always reports ErrNoConnection.
	Conn(ctx context.Context) (*Connection, error)
	// State is what the probes last found, probe by probe. It never dials.
	//
	// Nothing dials yet, so only the answers that need no server are real.
	State() State
	// WatchState carries every State published for this claim. It delivers nothing on attach,
	// so a watcher pairs it with State for what is known now.
	//
	// **The caller closes it**, as with Subscribe. Release does not: the receiver is keyed by
	// context, so one that outlives its claim keeps a hub slot and goes on reporting whatever
	// claims that name next.
	WatchState() StateSubscription
	// Departed reports that the kubeconfig no longer names this claim's context.
	//
	// Not an error and not final: the file may name it again, and this claim is how the
	// holder hears about that. Subscribe and WatchState both fire when this flips.
	Departed() bool
	// Release drops the claim, and the entry with it once nothing else holds the context.
	// Idempotent, so it is safe to defer.
	Release()
}

// Service is the pool the cluster service leases contexts from. The probe engine tracks one
// subject per claimed context; this type owns who holds the claims and what the fleet is told.
type Service struct {
	kubecfgSvc kubeconfigService
	// engine runs the five probes of probe.go over the claimed contexts; probes is what
	// registering them returned.
	engine *probe.Engine
	// signalHub names the contexts whose news changed; stateHub carries what the probes read.
	// Both keyed by context, both fed by publish.
	signalHub *conflate.Hub[string, struct{}]
	stateHub  *watch.Hub[string, State]

	// mu guards claimed and published together: what publish announces is measured against
	// who still holds the claim, and nothing may see one without the other.
	mu sync.Mutex
	// claimed holds one entry per claimed context — a key is here exactly while someone holds
	// that context, mirrored by a subject in the engine — and is also the key both hubs
	// publish under.
	claimed map[string]*entry
	// published is the news the fleet was last told per context, compared against so a pass
	// that moved only timing wakes nobody.
	published map[string]news

	wg sync.WaitGroup
}

// entry is a claimed context's holder count. A pointer per context on purpose: a claim carries
// the entry it was given, which is what stops a release that outlived Close from touching
// whatever claims the name next.
type entry struct {
	holders int
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	s := &Service{
		kubecfgSvc: kubecfgSvc,
		engine:     probe.New(),
		signalHub:  conflate.New[string, struct{}](),
		stateHub:   watch.New[string, State](),
		claimed:    map[string]*entry{},
		published:  map[string]news{},
	}
	registerProbes(s.engine, kubecfgSvc)
	s.engine.OnChange(s.publish)
	return s
}

// Acquire claims contextName and hands back the lease on it. It never fails and never waits: a
// context the kubeconfig does not name yet is claimable, because the file may name it later and
// the claim is how the holder finds out.
//
// The first holder adds the context to the engine, whose first pass dispatches the connection
// probe — not work to do on the caller's thread. A later holder joins what the probes found.
func (s *Service) Acquire(contextName string) Lease {
	s.mu.Lock()
	e, held := s.claimed[contextName]
	if !held {
		e = &entry{}
		s.claimed[contextName] = e
	}
	e.holders++
	s.mu.Unlock()

	if !held {
		s.engine.Add(contextName)
	}
	return &claim{svc: s, contextName: contextName, entry: e}
}

// Subscribe reports every context whose news changed, for a reader whose reaction to any of them
// is the same. A holder that cares about one claim watches that claim instead.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Start runs the engine and the kubeconfig watch. The kubeconfig subscription is taken before
// Start returns, so nothing it says in between is dropped; the engine's queues need no such
// care, since they hold what a claim taken before Start asked for until its workers run.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	stopEngine := s.engine.Start(ctx)

	// Not Start's context, which bounds startup: this one bounds the watch, so it lives until
	// the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	cfgs := s.kubecfgSvc.Subscribe()
	s.wg.Go(func() {
		defer cfgs.Close()
		s.watchKubeconfig(loopCtx, cfgs)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return errors.Join(drain.WithContext(ctx, s.wg.Wait), stopEngine(ctx))
	}, nil
}

// Close drops what the pool holds, the engine's subjects included. Claims are not released for
// their holders: a claim outliving the pool is the holder's bug, and reading as nothing known is
// what a dropped entry already does.
func (s *Service) Close() error {
	err := s.engine.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.claimed)
	clear(s.published)
	return err
}

// watchKubeconfig wakes every claimed context's connection probe on every change, until stopped.
// Every context rather than the ones that moved, because the feed carries a whole config and
// working out which contexts changed is what the probe does anyway. Only the connection probe:
// resolving owns the context's lifecycle, and the four behind it hang off its answer.
//
// Cancellation ends the wait: the service behind the feed is the app's and is not required to
// close its channel when released.
func (s *Service) watchKubeconfig(ctx context.Context, cfgs kubeconfig.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-cfgs.Chan():
			if !ok {
				return
			}
			s.engine.WakeAll(nameConnection)
		}
	}
}

// publish is the engine's OnChange: project the pass into State, tell the claim watchers, and
// signal the fleet when the news moved. The engine serializes it per context, and the order
// holds — a reader the signal wakes finds the state already published.
func (s *Service) publish(contextName string, v probe.Snapshot) {
	st := s.stateOf(v)
	n := s.newsOf(v, st)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, held := s.claimed[contextName]; !held {
		// The pass raced the last release; watchers on this name hear about whatever claims
		// it next.
		return
	}
	changed := s.published[contextName] != n
	s.published[contextName] = n

	s.stateHub.Sender().Send(contextName, st)
	if changed {
		s.signalHub.Sender().Send(contextName, struct{}{})
	}
}

// news is the part of a pass a fleet reader reacts to: what the probes concluded, never when.
// The timing moves every run, so signalling on it would wake every cluster's reconcile on every
// cadence to find nothing changed.
type news struct {
	departed bool
	phase    Phase
	identity Identity
	ok       [len(probeNames)]bool
}

func (s *Service) newsOf(v probe.Snapshot, st State) news {
	n := news{
		departed: keyConnection.From(v).Value.departed,
		phase:    st.Phase(),
		identity: st.Identity(),
	}
	for i, name := range probeNames {
		n.ok[i] = v.Attempts(name).OK()
	}
	return n
}

// stateOf projects the engine's observables into State. The connection's observable bundles the
// context's standing with the endpoint; only the endpoint is the answer State carries.
func (s *Service) stateOf(v probe.Snapshot) State {
	ci := keyConnection.From(v)
	return State{
		Connection:    Observation[string]{Value: ci.Value.endpoint, LastSeen: ci.LastSeen, Attempts: ci.Attempts},
		Readiness:     probe.Get[ComponentStatus](v, nameReadiness),
		ServerUID:     probe.Get[string](v, nameServerUID),
		ServerVersion: probe.Get[VersionInfo](v, nameServerVersion),
		Principal:     probe.Get[Principal](v, namePrincipal),
	}
}

// claim is one holder's claim on the context it named, and on the entry it was given. It carries
// both because a context outlives its entries: what a claim may read and release is the one it
// was made for, never whatever has the name now.
type claim struct {
	svc         *Service
	contextName string
	entry       *entry

	released atomic.Bool
}

func (c *claim) Conn(context.Context) (*Connection, error) {
	return nil, fmt.Errorf("%w: %q", ErrNoConnection, c.contextName)
}

func (c *claim) State() State {
	state, _ := c.svc.read(c.contextName, c.entry)
	return state
}

func (c *claim) Departed() bool {
	_, departed := c.svc.read(c.contextName, c.entry)
	return departed
}

// read answers a claim from the engine, for the entry the claim was made for. Once the pool no
// longer holds that entry — released, or dropped by Close — the claim reads as departed with
// nothing known.
//
// The identity check comes after the engine read: the last holder can release and another
// caller re-claim the name mid-read, and the state that came back is then the new claim's — the
// check catches it, where one taken before the read would not.
func (s *Service) read(contextName string, held *entry) (State, bool) {
	v, tracked := s.engine.Read(contextName)

	s.mu.Lock()
	valid := s.claimed[contextName] == held
	s.mu.Unlock()

	if !valid || !tracked {
		return State{}, true
	}
	return s.stateOf(v), keyConnection.From(v).Value.departed
}

// WatchState takes no baseline: the hub compares against one only through an Accept, and this
// one has none, so every value is delivered. What a baseline is for — reading and registering in
// one critical section, so a publish landing between the two is not skipped — is worth having
// once a probe can land at all.
//
// Not tracked for the caller. The receiver is the caller's to close, and closing it here on
// Release would take one out from under a caller that is still reading it.
func (c *claim) WatchState() StateSubscription {
	return c.svc.stateHub.Watch(c.contextName)
}

// Release gives the claim back, dropping the entry — and the engine's subject with it — once
// the last holder goes: an entry nobody holds is one nothing probes. The CAS makes it
// idempotent while other holders remain, when the entry check below cannot tell a second
// release from a first.
func (c *claim) Release() {
	if !c.released.CompareAndSwap(false, true) {
		return
	}

	s := c.svc
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only the entry the claim was made for: Close drops entries out from under the leases
	// still holding them, so a stale release must not decrement — or delete — whatever has
	// claimed the name since.
	if s.claimed[c.contextName] != c.entry {
		return
	}
	c.entry.holders--
	if c.entry.holders == 0 {
		delete(s.claimed, c.contextName)
		delete(s.published, c.contextName)
		// Under the lock, so a release racing a fresh Acquire of the same name cannot remove
		// the subject the new claim just added.
		s.engine.Remove(c.contextName)
	}
}
